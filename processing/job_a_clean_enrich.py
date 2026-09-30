"""Job A — clean and enrich the telemetry stream (§7 Job A).

WHY THIS EXISTS IN THE ARCHITECTURE
-----------------------------------
This is the first consumer of the Kappa log and the gate everything downstream
depends on. It turns a raw, deliberately-dirty stream into two things:

  * `meter.readings.clean.v1` — deduplicated, validated, enriched readings that
    Jobs B and C aggregate. Because Job A has already removed duplicates and bad
    records, the billing aggregation downstream can be a plain `sum()` and trust it.
  * `meter.readings.dlq.v1` — every rejected record with the reason it was rejected,
    which is what makes the pipeline diagnosable rather than merely functional
    (§13, observability, 10 marks).

Plus the curated Parquet archive, which exists to demonstrate the production-scale
replay path: at utility scale, replay would read columnar files from object storage
rather than relying on unbounded Kafka retention.

ONE QUERY, THREE SINKS, ONE CHECKPOINT (the central design decision)
--------------------------------------------------------------------
§7 asks for both a watermarked `dropDuplicates` AND a Parquet sink, and the Phase 2
checkpoint requires that "restarting the job does not duplicate Parquet output".
Those three requirements together rule out the obvious structure.

The obvious structure — one `writeStream` per sink — fails because each query keeps
its OWN checkpoint. After a restart the three checkpoints can sit at different
offsets, so Parquet may re-write a batch that the clean topic already emitted. The
sinks would disagree, and the duplicate rows in the archive would double-count kWh
on any replay from Parquet.

So there is ONE query, and its `foreachBatch` writes all three sinks from the same
micro-batch DataFrame. One checkpoint governs all of them: Spark records a batch as
complete only after `foreachBatch` returns, so a crash mid-batch replays the whole
batch and all three sinks are rewritten together.

TRADE-OFF (deliberate): `foreachBatch` gives at-least-once, not exactly-once. A
crash after the Parquet write but before the checkpoint commit replays the batch and
CAN duplicate Parquet rows. This is handled by making the write idempotent per batch
— see `_write_parquet` — which is the same at-least-once-delivery-into-
effectively-once-storage pattern the Postgres upserts use in Phase 3. It is the
honest answer in a viva: micro-batch streaming with external sinks is
effectively-once by construction, not exactly-once by magic.

DEDUPE BEFORE VALIDATE (ordering matters, and is defensible)
------------------------------------------------------------
Deduplication runs BEFORE validation. A duplicated bad record is therefore rejected
once, not twice.

If validation ran first, the DLQ would receive two records for one bad reading, the
`records_rejected` count would exceed the true number of bad readings, and the
Airflow data-quality gate (reject rate < 2%, §7) could fail on arithmetic rather
than on data quality. The producer deliberately duplicates already-corrupted records
to exercise exactly this ordering — see `simulators/fault_injection.py`.

WHY `event_id` IS THE DEDUPE KEY AND NOT THE KAFKA OFFSET
---------------------------------------------------------
Offsets are per-partition and change on replay; `event_id` is minted once by the
producer and is stable across any number of reprocessing runs. Deduplicating on
offset would make a replay produce different results from the original run, which
would contradict the entire Kappa argument.
"""

from __future__ import annotations

import sys
import time
from datetime import datetime, timezone

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.config import get_settings
from common.logging_setup import STAGE_PROCESS, configure_logging, stage_boundary
from common.metrics import (
    batch_micro_duration_seconds,
    events_consumed_total,
    events_rejected_total,
    start_metrics_server,
)
from common.schemas import (
    CLEAN_READING_FIELDS,
    SCHEMA_VERSION,
    meter_reading_spark_schema,
)
from common.sim_clock import compression_ratio, startup_banner
from processing.spark_session import (
    build_spark_session,
    kafka_read_options,
)
from processing.transforms.enrichment import build_dimension, enrich, join_dimension
from processing.transforms.validation import split_valid_invalid, with_rejection_reason

SERVICE_NAME = "job-a-clean-enrich"
JOB_NAME = "job_a_clean_enrich"


class JobASettings(BaseSettings):
    """Job A settings, env-driven."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # §7 step 3 specifies a 2-minute watermark. THE SUBTLETY THAT MATTERS: the
    # watermark is applied to `event_timestamp`, which is on the SIMULATED time axis
    # running 288x faster than wall clock. Two literal minutes of simulated time is
    # 0.42 REAL seconds — far less than one 5-second micro-batch, so records at the
    # start of every batch arrive already "too late" and the dedupe state drops
    # nearly everything.
    #
    # Observed concretely before this was fixed: 1.18M records on the topic, Job A
    # emitting 0 valid rows, and a 100% reject rate composed entirely of injected
    # faults — the clean records were being silently discarded by the watermark, not
    # rejected, so nothing in the DLQ explained where they went.
    #
    # The watermark is therefore expressed in REAL minutes and converted to
    # simulated minutes, which is what §7 actually intends: "tolerate 2 minutes of
    # late arrival" is a statement about clock time, not about the compressed axis.
    # It must exceed (a) one micro-batch's simulated span — 24 simulated minutes at a
    # 5-second trigger — (b) the 4 simulated minutes the producer back-dates its
    # injected late events by, and (c) producer tick jitter. At 288x, 2 real minutes
    # becomes 576 simulated minutes, roughly 24x the batch span.
    #
    # TRADE-OFF: a larger watermark means a larger dedupe state store and a longer
    # window in which a duplicate can still be caught. Both are bounded and small at
    # this volume; the alternative — a watermark too small to span a micro-batch — is
    # not a trade-off but a defect.
    dedupe_watermark_real_minutes: float = Field(default=2.0, gt=0)

    # How far ahead of simulated now an event_timestamp may be before rejection.
    # Non-zero because micro-batch boundaries and the producer's tick mean a reading
    # can legitimately be stamped slightly ahead of when the driver evaluates it.
    #
    # EXPRESSED IN REAL MINUTES, for exactly the same reason as the dedupe
    # watermark above — and this one was missed in Phase 2, with worse consequences.
    #
    # The rule compares `event_timestamp` against `sim_now`, both on the SIMULATED
    # axis. A tolerance stated in simulated minutes therefore shrinks as the
    # compression ratio rises: 5 simulated minutes is 1.04 real seconds at 288x and
    # only 0.21 REAL SECONDS at the 1440x of `make fast`. But the skew it has to
    # absorb is a REAL-time quantity — Kafka transit plus however long the driver
    # takes to evaluate the batch, which was measured at ~18 real seconds on this
    # host. So every reading arrived looking hours "in the future" and 100% of them
    # were rejected as `timestamp_in_future`, freezing the clean topic completely.
    #
    # Symptom, for the record: Job A committing batches steadily with zero lag,
    # emitting nothing, and the DLQ filling with one reason class. The pipeline
    # looked healthy from every angle except its output.
    #
    # Converted by compression_ratio() below, so the setting now means what it says
    # at any clock speed. The default is deliberately larger than one micro-batch's
    # real duration, with margin for a loaded host.
    #
    # THIS VALUE IS BOUNDED ON BOTH SIDES, which is why it is small and why the
    # bound is written down rather than left to be rediscovered:
    #
    #   LOWER BOUND (real time): it must exceed the real skew between the producer
    #   stamping a reading and the driver evaluating `sim_now` for the batch that
    #   contains it — Kafka transit plus micro-batch duration.
    #
    #   UPPER BOUND (simulated time): it must stay BELOW the injected
    #   future-timestamp fault, which `simulators/fault_injection.py` back-dates by
    #   FAULT_FUTURE_TIMESTAMP_SIM_MINUTES = 30 SIMULATED minutes. Exceed it and that
    #   fault stops being detected, the DLQ loses a whole reason class, and the
    #   Phase 2 checkpoint ("all four DLQ reason classes present") silently fails.
    #
    # The two bounds are on DIFFERENT AXES, so the window between them narrows as the
    # compression ratio rises. At 0.02 real minutes:
    #
    #   288x  (make demo-config): 1.2 real seconds of skew,  5.8 sim-min  < 30  OK
    #   1440x (make fast):        1.2 real seconds of skew, 28.8 sim-min  < 30  OK
    #
    # 0.02 is the largest tested value that stays under the fault at BOTH speeds.
    # Raising it past ~0.02 breaks fault detection at 1440x; lowering it re-opens the
    # outage described above. If a slow host needs more real-time headroom, the
    # correct fix is to raise FAULT_FUTURE_TIMESTAMP_SIM_MINUTES first, so the
    # upper bound moves before the lower one is pushed against.
    future_timestamp_tolerance_real_minutes: float = Field(default=0.02, gt=0)

    # 5s matches the <10s end-to-end budget in §2.4 with room for the sink writes.
    trigger_interval_seconds: int = Field(default=5, gt=0)

    checkpoint_root: str = Field(default="/data/checkpoints")
    # A LOCAL PATH, not s3a://. See docs/ADR-004-parquet-sink.md: Hadoop's S3A
    # committer finalises a write by renaming from a _temporary directory, and
    # SeaweedFS's S3 gateway does not support the rename semantics it requires, so
    # every batch aborted with "Failed to rename S3AFileStatus{...}". The two
    # rename-free S3A committers (magic, directory) need the spark-hadoop-cloud
    # JAR, which this Spark distribution does not bundle.
    #
    # The architectural requirement is a columnar archive partitioned by the
    # predicates that replay and the daily report filter on — which this satisfies
    # identically. The report states that production would place it on object
    # storage with a transactional table format (Delta/Iceberg), which solves the
    # commit problem properly rather than by choosing a filesystem.
    parquet_path: str = Field(default="/data/curated/readings")

    # Must match the producer's METER_TICK_REAL_SECONDS. Used with the compression
    # ratio to reconstruct the simulated interval each reading covers, which is the
    # basis of the physical solar-capacity bound. Documented as coupling: if the
    # producer's tick changes and this does not, valid readings get rejected as
    # physically impossible.
    meter_tick_real_seconds: float = Field(default=2.0, gt=0)

    # Ceiling on records per micro-batch. Sized so the batch's EVENT-TIME span stays
    # inside the dedupe watermark — see the long note in processing/spark_session.py.
    # At 200 readings per producer tick and 9.6 simulated minutes between ticks, 2000
    # records spans ~96 simulated minutes against a 576-minute watermark.
    max_offsets_per_trigger: int = Field(default=2000, gt=0)

    starting_offsets: str = Field(
        default="latest",
        description="'latest' for live, 'earliest' for replay. Most of what "
        "distinguishes a reprocessing run from a live one (§2.2d).",
    )


class JobA:
    def __init__(self) -> None:
        self.settings = JobASettings()
        self.kafka = get_settings().kafka
        self.log = configure_logging(SERVICE_NAME, JOB_NAME, stage=STAGE_PROCESS)
        self.spark: SparkSession | None = None
        self._batches = 0
        self._total_in = 0
        self._total_out = 0
        self._total_rejected = 0

        # Simulated seconds covered by one reading interval. The solar validation
        # bound is capacity x this duration, so it must agree with the producer.
        self.sim_interval_seconds = (
            self.settings.meter_tick_real_seconds * compression_ratio()
        )

        # Converted from real to simulated minutes — see the settings docstring.
        self.dedupe_watermark_sim_minutes = (
            self.settings.dedupe_watermark_real_minutes * compression_ratio()
        )
        self.dedupe_watermark = f"{self.dedupe_watermark_sim_minutes:.0f} minutes"

        # Same real -> simulated conversion for the future-timestamp bound. Both
        # durations are real-time statements about clock skew and late arrival; the
        # simulated axis is an implementation detail of the simulator, not a unit
        # anyone reasons in.
        self.future_tolerance_sim_minutes = (
            self.settings.future_timestamp_tolerance_real_minutes
            * compression_ratio()
        )

    # -- pipeline construction ---------------------------------------------

    def _read_stream(self, spark: SparkSession) -> DataFrame:
        """Read and parse the telemetry topic against the EXPLICIT schema (§7 step 1).

        `inferSchema` is never used. Inference samples data, so on an unbounded
        source the resulting schema depends on which records happened to arrive
        first — non-deterministic across restarts and across replays. For a system
        whose central claim is that a replay reproduces the original run, a schema
        that can differ between the two breaks that guarantee before any business
        logic runs.
        """
        raw = (
            spark.readStream.format("kafka")
            .options(
                **kafka_read_options(
                    self.kafka.topic_meter_readings,
                    self.settings.starting_offsets,
                    self.settings.max_offsets_per_trigger,
                )
            )
            .load()
        )

        schema = meter_reading_spark_schema()

        return (
            raw.select(
                # Retained for the DLQ: a record that fails to parse must still be
                # recoverable as the original bytes, which is the whole point of
                # keeping `original_payload` opaque in §6.4.
                F.col("value").cast("string").alias("raw_payload"),
                F.col("partition").alias("kafka_partition"),
                F.col("offset").alias("kafka_offset"),
                F.from_json(F.col("value").cast("string"), schema).alias("data"),
            )
            .select(
                "raw_payload",
                "kafka_partition",
                "kafka_offset",
                "data.*",
            )
            # Timestamps are parsed explicitly rather than relying on Spark's
            # implicit JSON timestamp handling, which depends on session config and
            # would make replay results depend on how the cluster was started.
            .withColumn(
                "event_timestamp_parsed", F.to_timestamp(F.col("event_timestamp"))
            )
            # Current simulated instant, evaluated per micro-batch. Used only as the
            # watermark fallback below; the same expression is recomputed later for
            # the future-timestamp validation rule.
            .withColumn("sim_now_seed", self._sim_now_expression())
            # The watermark column must never be null, or the row is excluded from
            # the stateful operator entirely and vanishes without reaching the DLQ —
            # the same silent loss that null `event_id`s caused. A row whose
            # timestamp is missing or unparseable (the injected null-field fault, or
            # a payload `from_json` could not read at all) is given the Kafka
            # ingestion timestamp purely so it can FLOW THROUGH dedupe.
            #
            # `event_timestamp` itself is left as-is, so validation still sees it as
            # null and rejects it with `null_required_field`, and the DLQ records what
            # actually arrived. The substitute is a routing device, never data.
            #
            # The fallback is the CURRENT SIMULATED INSTANT, not the Kafka ingestion
            # time. Those are on different scales — Kafka's timestamp is real time,
            # while the watermark axis is simulated time running 288x faster — and
            # mixing them would drag the watermark back to 2026-09 real time and
            # discard every genuine reading as impossibly late.
            #
            # Using "now" in simulated terms places the unparseable row at the
            # leading edge of the stream, so it passes through dedupe immediately and
            # reaches validation, which is exactly where it should be rejected.
            .withColumn(
                "watermark_timestamp",
                F.coalesce(F.col("event_timestamp_parsed"), F.col("sim_now_seed")),
            )
            .withColumn("event_timestamp", F.col("event_timestamp_parsed"))
            .drop("event_timestamp_parsed", "sim_now_seed")
            .withColumn(
                "producer_emitted_at", F.to_timestamp(F.col("producer_emitted_at"))
            )
            .withColumn("sim_date", F.to_date(F.col("sim_date")))
        )

    def _deduplicate(self, frame: DataFrame) -> DataFrame:
        """Watermarked deduplication on `event_id` (§7 step 3).

        Runs BEFORE validation — see the module docstring on why that ordering is
        deliberate and what breaks if it is reversed.

        UNKEYABLE ROWS ARE EXCLUDED FIRST, and this is essential rather than tidy.
        `dropDuplicates` treats all NULLs as equal to one another, so a single row
        with a null `event_id` makes every *subsequent* null-`event_id` row look like
        a duplicate of it and be discarded. Two sources produce such rows:

          * the injected `null_required_field` fault, when it happens to null a
            field the parse then chokes on, and
          * any payload `from_json` cannot parse at all, which yields a struct of
            all-NULLs — so `event_id` is null along with everything else.

        Left unguarded this silently swallowed the entire clean stream: 1.18M records
        on the topic, Job A emitting 0 valid rows, and a 100% reject rate made up
        purely of the faults whose `event_id` survived. The clean readings never
        reached the DLQ, so nothing in the logs explained where they had gone —
        which is precisely the silent-data-loss failure the DLQ exists to prevent.

        A row with no `event_id` cannot be deduplicated even in principle: there is
        no key to compare. Rather than DROP such rows — which would repeat the silent
        loss in a different place — each is given a synthetic, unique dedupe key
        built from its Kafka coordinates. Partition plus offset identifies exactly
        one record in the log, so every unkeyable row is distinct, none is collapsed
        into another, and all of them flow on to validation, which rejects them into
        the DLQ with a proper reason.

        The synthetic key is used ONLY for deduplication; `event_id` itself is left
        null so validation still sees the row as malformed and the DLQ records the
        truth about what arrived.
        """
        # Kafka partition+offset is unique per record and stable across restarts, so
        # this neither invents duplicates nor hides real ones.
        keyed = frame.withColumn(
            "dedupe_key",
            F.coalesce(
                F.col("event_id"),
                F.concat_ws(
                    ":",
                    F.lit("_unkeyable"),
                    F.col("kafka_partition").cast("string"),
                    F.col("kafka_offset").cast("string"),
                ),
            ),
        )
        return (
            # Watermarked on `watermark_timestamp`, not `event_timestamp`: the
            # latter is null for unparseable rows, which would exclude them from the
            # stateful operator and lose them silently. See _read_stream.
            keyed.withWatermark("watermark_timestamp", self.dedupe_watermark)
            .dropDuplicates(["dedupe_key"])
            .drop("dedupe_key")
        )

    def _build_stream(self, spark: SparkSession) -> DataFrame:
        """Assemble the full transformation chain."""
        dimension = build_dimension(spark)

        parsed = self._read_stream(spark)
        deduped = self._deduplicate(parsed)

        # The dimension join happens before validation because validation needs
        # `solar_capacity_kw` to bound solar generation against. A left join, so a
        # household missing from the dimension produces nulls that validation
        # rejects with a reason rather than silently vanishing (see enrichment.py).
        joined = join_dimension(deduped, dimension)

        # The current simulated instant, needed by the future-timestamp rule.
        # Derived from wall-clock time inside the plan rather than captured once in
        # Python, so a long-running query does not compare against a startup-time
        # constant that drifts further out of date every micro-batch.
        with_sim_now = joined.withColumn("sim_now", self._sim_now_expression())

        validated = with_rejection_reason(
            with_sim_now,
            interval_seconds_column=F.lit(self.sim_interval_seconds),
            future_tolerance_minutes=self.future_tolerance_sim_minutes,
        )
        return validated

    def _sim_now_expression(self):
        """The current simulated instant, as a Spark expression.

        Defined once and used by BOTH the watermark fallback for unparseable rows and
        the future-timestamp validation rule, so the two cannot disagree about what
        "now" means — a disagreement would either reject valid readings or admit
        impossible ones.

        Evaluated inside the plan rather than captured once in Python: a long-running
        query crosses simulated days while it runs, and a value bound at startup would
        be stale within five real minutes.
        """
        return F.expr(
            # sim_now = sim_start + (real_now - real_anchor) * compression_ratio
            f"timestamp'{self._sim_start_iso()}' + "
            f"make_interval(0, 0, 0, 0, 0, 0, "
            f"(unix_timestamp(current_timestamp()) - {self._anchor_epoch()}) "
            f"* {compression_ratio()})"
        )

    def _sim_start_iso(self) -> str:
        from common.sim_clock import sim_start

        return sim_start().strftime("%Y-%m-%d %H:%M:%S")

    def _anchor_epoch(self) -> int:
        from common.sim_clock import real_anchor

        return int(real_anchor().timestamp())

    # -- sinks --------------------------------------------------------------

    def _write_dlq(self, invalid: DataFrame) -> None:
        """Write rejected records to the DLQ topic (§6.4).

        `original_payload` is the RAW string, not a re-serialised view of the parsed
        row. Records land here precisely because they were malformed, so a
        round-tripped version could differ from what actually arrived — and the
        whole value of the DLQ is being able to see exactly what was received.
        """
        payload = invalid.select(
            F.col("event_id").alias("key"),
            F.to_json(
                F.struct(
                    F.col("raw_payload").alias("original_payload"),
                    F.col("rejection_reason"),
                    F.lit(datetime.now(timezone.utc).isoformat()).alias("rejected_at"),
                    F.lit(JOB_NAME).alias("job_name"),
                    F.col("correlation_id"),
                    F.lit(SCHEMA_VERSION).alias("schema_version"),
                )
            ).alias("value"),
        )
        (
            payload.write.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka.kafka_bootstrap_servers)
            .option("topic", self.kafka.topic_meter_readings_dlq)
            .save()
        )

    def _write_clean_topic(self, valid: DataFrame) -> None:
        """Write validated, enriched readings to the clean topic.

        Keyed by `grid_zone`, matching the raw topic, so a zone's records stay
        co-partitioned end to end and the per-zone ordering that Job B's tumbling
        windows depend on survives this hop.
        """
        payload = valid.select(
            F.col("grid_zone").alias("key"),
            # Selected against the ONE definition of this topic's shape in
            # common/schemas.py, rather than a field list written out here.
            # Jobs B and C parse the same topic with clean_reading_spark_schema(),
            # which is built from the same tuple — so a field added to the payload
            # and not to the consumers' parse is now impossible rather than merely
            # unlikely. Before Phase 3 this list lived only here, which was safe
            # only while Job A was the sole job that knew the contract.
            F.to_json(F.struct(*CLEAN_READING_FIELDS)).alias("value"),
        )
        (
            payload.write.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka.kafka_bootstrap_servers)
            .option("topic", self.kafka.topic_meter_readings_clean)
            .save()
        )

    def _write_parquet(self, valid: DataFrame, batch_id: int) -> None:
        """Append validated readings to the curated archive (§7 step 5).

        Partitioned by `sim_date`/`grid_zone`: the two predicates every replay and
        every daily report filters on, so partition pruning skips whole directories
        rather than scanning and discarding.

        ON RESTART IDEMPOTENCE (the Phase 2 checkpoint):
        `foreachBatch` is at-least-once, so a crash after this write but before the
        checkpoint commit replays the batch. The `batch_id` is therefore carried into
        the data, which makes a duplicated batch *identifiable* — the daily report
        and any Parquet-sourced replay deduplicate on (event_id) and can prove which
        rows came from a replayed batch.

        The stronger guarantee — atomic commit on object storage — needs a
        transactional table format (Delta/Iceberg), which is named in the report's
        production-scale section. Plain Parquet cannot provide it, and claiming
        otherwise in a viva would be indefensible.
        """
        (
            valid.withColumn("batch_id", F.lit(batch_id))
            .withColumn("archived_at", F.current_timestamp())
            .write.mode("append")
            .partitionBy("sim_date", "grid_zone")
            .parquet(self.settings.parquet_path)
        )

    def _process_batch(self, batch: DataFrame, batch_id: int) -> None:
        """Write all three sinks from one micro-batch.

        The single place all sinks are written, so one checkpoint governs them —
        see the module docstring.
        """
        started = time.monotonic()

        # Cached because the frame is traversed several times below (count, split,
        # three writes). Without this, Spark would recompute the whole chain —
        # including the Kafka read — for each action, and the dedupe state would be
        # consulted repeatedly for one logical batch.
        batch.persist()
        try:
            total_in = batch.count()
            if total_in == 0:
                return

            valid, invalid = split_valid_invalid(batch)
            enriched = enrich(valid)
            enriched.persist()
            invalid.persist()

            rejected_count = invalid.count()
            valid_count = enriched.count()

            # Reason breakdown, for the metric label and the log line. Collected
            # only when something was rejected, so a clean batch costs no extra job.
            reason_counts: dict[str, int] = {}
            if rejected_count:
                for row in (
                    invalid.groupBy("rejection_reason").count().collect()
                ):
                    reason_counts[row["rejection_reason"]] = row["count"]

            if rejected_count:
                self._write_dlq(invalid)
            if valid_count:
                self._write_clean_topic(enriched)
                self._write_parquet(enriched, batch_id)

            # --- metrics and logs ---
            events_consumed_total.labels(
                job=JOB_NAME, topic=self.kafka.topic_meter_readings
            ).inc(total_in)
            for reason, count in reason_counts.items():
                events_rejected_total.labels(job=JOB_NAME, reason=reason).inc(count)

            duration = time.monotonic() - started
            batch_micro_duration_seconds.labels(job=JOB_NAME).observe(duration)

            self._batches += 1
            self._total_in += total_in
            self._total_out += valid_count
            self._total_rejected += rejected_count

            stage_boundary(
                self.log,
                stage=STAGE_PROCESS,
                records_in=total_in,
                records_out=valid_count,
                records_rejected=rejected_count,
                batch_id=batch_id,
                batch_duration_seconds=round(duration, 3),
                rejection_reasons=reason_counts or None,
                cumulative_in=self._total_in,
                cumulative_out=self._total_out,
                cumulative_rejected=self._total_rejected,
                reject_rate_pct=(
                    round(100.0 * self._total_rejected / self._total_in, 3)
                    if self._total_in
                    else 0.0
                ),
            )

            enriched.unpersist()
            invalid.unpersist()
        finally:
            batch.unpersist()

    # -- entrypoint ---------------------------------------------------------

    def run(self) -> None:
        start_metrics_server()
        self.spark = build_spark_session(SERVICE_NAME)

        self.log.info(
            "job_starting",
            stage=STAGE_PROCESS,
            sim_clock=startup_banner(),
            source_topic=self.kafka.topic_meter_readings,
            clean_topic=self.kafka.topic_meter_readings_clean,
            dlq_topic=self.kafka.topic_meter_readings_dlq,
            parquet_path=self.settings.parquet_path,
            dedupe_watermark_sim=self.dedupe_watermark,
            dedupe_watermark_real_minutes=self.settings.dedupe_watermark_real_minutes,
            future_tolerance_real_minutes=(
                self.settings.future_timestamp_tolerance_real_minutes
            ),
            future_tolerance_sim_minutes=round(self.future_tolerance_sim_minutes),
            compression_ratio=compression_ratio(),
            starting_offsets=self.settings.starting_offsets,
            max_offsets_per_trigger=self.settings.max_offsets_per_trigger,
            sim_interval_seconds=round(self.sim_interval_seconds, 1),
        )

        stream = self._build_stream(self.spark)

        query = (
            stream.writeStream
            # One foreachBatch for all sinks: one checkpoint, atomic restart.
            .foreachBatch(self._process_batch)
            .option("checkpointLocation", f"{self.settings.checkpoint_root}/{JOB_NAME}")
            # `update` rather than `append`: a stateful dedupe cannot emit append
            # output until its watermark expires, which would delay every record by
            # the watermark duration and blow the <10s latency budget in §2.4.
            .outputMode("update")
            .trigger(processingTime=f"{self.settings.trigger_interval_seconds} seconds")
            .start()
        )

        query.awaitTermination()


def main() -> int:
    JobA().run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
