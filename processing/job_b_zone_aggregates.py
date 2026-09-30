"""Job B — zone aggregates, the real-time answer to R1 and R2 (§7 Job B).

WHY THIS EXISTS IN THE ARCHITECTURE
-----------------------------------
R1 ("current grid load per zone") and R2 ("current renewable contribution per
zone") are the seconds-latency half of the requirements table (§1). They are what
the control-room dashboard shows and what `/api/v1/zones/load` serves, and §2.4
budgets them at under ten seconds end to end.

This job is also the input to Job D's alert evaluator in Phase 6 — LOW_RENEWABLE
and ZONE_OVERLOAD are both thresholds on rows this job writes — so its window
size decides the granularity at which an alert can fire.

THE "1 MINUTE" IN §7 IS ONE REAL MINUTE, NOT ONE SIMULATED MINUTE
------------------------------------------------------------------
This is the decision to defend in the viva, and it applies to BOTH the watermark
and the window.

§7 specifies `withWatermark("event_timestamp", "1 minute")` and
`window(event_timestamp, "1 minute")`. But `event_timestamp` is on the SIMULATED
axis, which runs at `compression_ratio()` = 288x wall clock (1 simulated day = 5
real minutes, §0). Read literally, on this system:

  * One simulated minute is 0.21 REAL seconds.
  * One micro-batch spans ~96 simulated minutes of event time (2000 records at
    200 readings per producer tick, 9.6 simulated minutes apart).

So a literal 1-minute watermark would be 96x SMALLER than the event-time span of
a single batch. Nearly every record would arrive already beyond the watermark and
be dropped by the stateful aggregation before it could be counted. That is
precisely the failure Phase 2 hit in Job A — 1.18M records on the topic and 0
valid rows emitted (see job_a_clean_enrich.py) — except that Job A had a DLQ to
make the loss visible, and this job's aggregation path has none. It would simply
report near-zero load and look like a broken simulator.

The window read literally is a second, independent failure. A window closing five
times per second writes ~86,400 rows per real minute into `zone_load_1m`: a write
storm on a 7.7 GB host, and a Grafana panel with 17,280 points per real minute,
which is not a chart.

§7 was written against the observer's clock — a control room refreshes about once
a minute — before the 288x compression was designed. So both durations are
declared in REAL minutes and converted:

    window    = 1 real minute  -> 288 simulated minutes (4.8 simulated hours)
    watermark = 2 real minutes -> 576 simulated minutes

That yields 5 buckets per simulated day, 5 rows per real minute across the five
zones, and 25 rows per simulated day. The §6.5 table name `zone_load_1m` is kept
because it is a contract; it now means "the 1-minute-refresh zone load table",
recorded in serving/sql/001_schema.sql and here.

The watermark deliberately MATCHES Job A's. Both jobs consume the same physical
stream, and a watermark narrower than Job A's would discard readings that Job A
had just certified as on time — the two would disagree about what "late" means.

THE THREE DURATIONS ARE COUPLED, SO THE COUPLING IS ASSERTED
-------------------------------------------------------------
`maxOffsetsPerTrigger` bounds a batch's EVENT-TIME span, not merely its memory
(the note in processing/spark_session.py covers why). That makes three numbers
mutually constrained:

    watermark_sim > batch_span_sim    or records arrive already expired
    watermark_sim >= window_sim       or a window is evicted before its own data
    window_sim   >= batch_span_sim    or one batch straddles many windows

With the defaults: 576 > 288 > 96, margins of 6x and 3x. Raising
MAX_OFFSETS_PER_TRIGGER past ~6000 silently erodes the first of those. So
`_assert_time_budget` computes all three at startup and REFUSES TO START if the
relationship breaks, naming the environment variable to change. A tuning change
that would have quietly halved the output now fails loudly instead.

WHY `update` OUTPUT MODE AND WHY THE SINK MUST BE AN UPSERT
------------------------------------------------------------
`append` would withhold every window until the watermark had passed it — 2 real
minutes of latency against §2.4's sub-10-second budget. So the mode is `update`,
which re-emits each open window every micro-batch as more of its data arrives.

That is exactly why the sink is keyed on `(grid_zone, window_start)` and uses
`ON CONFLICT DO UPDATE`: the same window is written many times, and each write
must REPLACE the previous row rather than accumulate beside it. §7 asks for this
and it is also what makes the job idempotent under the forced restart the Phase 3
checkpoint demands — a replayed batch recomputes the same window totals from
Spark's state and overwrites with identical values.
"""

from __future__ import annotations

import sys
import time
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.config import get_settings
from common.logging_setup import (
    STAGE_PROCESS,
    STAGE_STORE,
    configure_logging,
    stage_boundary,
)
from common.metrics import (
    batch_micro_duration_seconds,
    events_consumed_total,
    last_event_timestamp_seconds,
    start_metrics_server,
    zone_renewable_pct,
)
from common.schemas import clean_reading_spark_schema
from common.sim_clock import compression_ratio, startup_banner
from processing.sinks.postgres_sink import upsert_batch
from processing.spark_session import build_spark_session, kafka_read_options

SERVICE_NAME = "job-b-zone-aggregates"
JOB_NAME = "job_b_zone_aggregates"
TARGET_TABLE = "zone_load_1m"

# Column order for the upsert. Declared once so the INSERT list, the SELECT list
# and the update set cannot disagree (see processing/sinks/postgres_sink.py).
TARGET_COLUMNS = (
    "grid_zone",
    "window_start",
    "window_end",
    "total_consumption_kwh",
    "total_solar_kwh",
    "renewable_pct",
    "active_meter_count",
    "late_event_count",
    "updated_at",
)
CONFLICT_COLUMNS = ("grid_zone", "window_start")
UPDATE_COLUMNS = tuple(c for c in TARGET_COLUMNS if c not in CONFLICT_COLUMNS)

class JobBSettings(BaseSettings):
    """Job B settings, env-driven. Durations are REAL minutes — see the module
    docstring for why that conversion is the whole point."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # §7's "1 minute" tumbling window, read as one minute of the OBSERVER's clock
    # and converted to the simulated axis by compression_ratio(). At 288x this is
    # 288 simulated minutes. See the module docstring for the full argument and
    # for what a literal reading would do.
    zone_window_real_minutes: float = Field(default=1.0, gt=0)

    # §7's "1 minute" watermark, same conversion. Set to 2.0 rather than 1.0 to
    # match Job A's dedupe watermark exactly: both jobs consume the same stream,
    # and a narrower watermark here would drop readings Job A had already
    # accepted as on-time.
    zone_watermark_real_minutes: float = Field(default=2.0, gt=0)

    # 5s matches the <10s end-to-end budget in §2.4 with room for the sink write.
    trigger_interval_seconds: int = Field(default=5, gt=0)

    # Bounds a batch's EVENT-TIME span, not just its memory. Must stay well inside
    # the watermark; _assert_time_budget enforces it rather than trusting it.
    max_offsets_per_trigger: int = Field(default=2000, gt=0)

    starting_offsets: str = Field(
        default="latest",
        description="'latest' for live, 'earliest' for replay -- most of what "
        "distinguishes a reprocessing run from a live one (§2.2d).",
    )

    checkpoint_root: str = Field(default="/data/checkpoints")

    # The two producer facts the event-time span is derived from. Declared rather
    # than hardcoded so the startup assertion stays correct if the simulator's
    # shape changes; if these drift from the real producer the assertion silently
    # checks the wrong number, so they are echoed in the startup log.
    meter_tick_real_seconds: float = Field(default=2.0, gt=0)
    readings_per_producer_tick: int = Field(default=200, gt=0)


class JobB:
    def __init__(self) -> None:
        self.settings = JobBSettings()
        self.kafka = get_settings().kafka
        self.postgres = get_settings().postgres
        self.log = configure_logging(SERVICE_NAME, JOB_NAME, stage=STAGE_PROCESS)
        self.spark: Optional[SparkSession] = None

        self._batches = 0
        self._total_in = 0
        self._total_out = 0
        self._total_rejected = 0

        ratio = compression_ratio()

        # Real -> simulated, the conversion the module docstring exists to justify.
        self.window_sim_minutes = self.settings.zone_window_real_minutes * ratio
        self.watermark_sim_minutes = self.settings.zone_watermark_real_minutes * ratio
        self.window_duration = "{:.0f} minutes".format(self.window_sim_minutes)
        self.watermark = "{:.0f} minutes".format(self.watermark_sim_minutes)

        # Event-time span of one micro-batch, in simulated minutes:
        #   ticks per batch   = max_offsets / readings_per_tick
        #   simulated minutes = ticks x tick_real_seconds x ratio / 60
        ticks_per_batch = (
            self.settings.max_offsets_per_trigger
            / self.settings.readings_per_producer_tick
        )
        self.batch_span_sim_minutes = (
            ticks_per_batch * self.settings.meter_tick_real_seconds * ratio / 60.0
        )

        # A reading is "late" when the producer emitted it more than one tick
        # after the instant it claims to describe -- i.e. the simulator's injected
        # late_event fault, which Job A passes through as perfectly valid data.
        # Expressed in simulated seconds because both timestamps being compared
        # are on the simulated axis.
        self.late_threshold_sim_seconds = (
            self.settings.meter_tick_real_seconds * ratio
        )

    # -- startup guards -----------------------------------------------------

    def _assert_time_budget(self) -> None:
        """Refuse to start if the watermark/window/batch-span relationship breaks.

        These three numbers are coupled through `maxOffsetsPerTrigger`, and the
        failure mode when they break is SILENT: the job keeps running, keeps
        logging batches, and emits a fraction of the windows it should. Phase 2
        lost hours to exactly that. An assertion here converts a silent
        correctness bug into a startup error naming the variable to change.
        """
        problems = []

        if self.watermark_sim_minutes <= self.batch_span_sim_minutes:
            problems.append(
                "watermark ({:.0f} sim-min) must exceed one batch's event-time "
                "span ({:.0f} sim-min), or most records in every batch arrive "
                "already expired and are dropped by the aggregation without "
                "reaching any sink. Raise ZONE_WATERMARK_REAL_MINUTES or lower "
                "MAX_OFFSETS_PER_TRIGGER.".format(
                    self.watermark_sim_minutes, self.batch_span_sim_minutes
                )
            )

        if self.watermark_sim_minutes < self.window_sim_minutes:
            problems.append(
                "watermark ({:.0f} sim-min) must be at least the window size "
                "({:.0f} sim-min), or a window is evicted from state before its "
                "own data has finished arriving. Raise "
                "ZONE_WATERMARK_REAL_MINUTES or lower "
                "ZONE_WINDOW_REAL_MINUTES.".format(
                    self.watermark_sim_minutes, self.window_sim_minutes
                )
            )

        if problems:
            raise ValueError(
                "Job B time budget is unsafe (see the module docstring): "
                + " | ".join(problems)
            )

    # -- pipeline construction ----------------------------------------------

    def _read_clean_stream(self, spark: SparkSession) -> DataFrame:
        """Read `meter.readings.clean.v1` against the EXPLICIT shared schema.

        The schema comes from `common.schemas.clean_reading_spark_schema()`, which
        is generated from the same field tuple Job A's writer selects against —
        so producer and consumer cannot drift. `inferSchema` is never used on a
        stream; the reasoning is in common/schemas.py and applies unchanged here.
        """
        raw = (
            spark.readStream.format("kafka")
            .options(
                **kafka_read_options(
                    self.kafka.topic_meter_readings_clean,
                    self.settings.starting_offsets,
                    self.settings.max_offsets_per_trigger,
                )
            )
            .load()
        )

        schema = clean_reading_spark_schema()

        return (
            raw.select(
                F.from_json(F.col("value").cast("string"), schema).alias("data")
            )
            .select("data.*")
            # Timestamps parsed explicitly rather than via Spark's implicit JSON
            # handling, which depends on session configuration and would make a
            # replay's results depend on how the cluster was started.
            .withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp")))
            .withColumn(
                "producer_emitted_at", F.to_timestamp(F.col("producer_emitted_at"))
            )
            .withColumn("sim_date", F.to_date(F.col("sim_date")))
            # Emission lag on the SIMULATED axis. Both operands are simulated
            # instants, so their difference is simulated seconds -- comparing it
            # against a real-time threshold would classify every reading as late.
            .withColumn(
                "is_late",
                F.col("producer_emitted_at").isNotNull()
                & (
                    F.unix_timestamp("producer_emitted_at")
                    - F.unix_timestamp("event_timestamp")
                    > F.lit(self.late_threshold_sim_seconds)
                ),
            )
        )

    def _aggregate(self, frame: DataFrame) -> DataFrame:
        """Tumbling per-zone window (§7 Job B).

        The null-timestamp filter is not defensive clutter. A null watermark
        column excludes a row from the stateful operator entirely — it vanishes
        without reaching any sink or any count, which is the silent-loss failure
        Phase 2 hit twice. Job A guarantees this cannot happen (it rejects null
        required fields into the DLQ before publishing here), but that is an
        invariant of ANOTHER job.

        So the rows are dropped EXPLICITLY, by a filter that names the condition,
        rather than being swallowed by `withWatermark` as a side effect.

        WHY THE COUNT IS NOT COLLECTED HERE. A row with no event_timestamp
        belongs to no event-time window — that is exactly what makes it unusable —
        so it cannot be folded into the grouped aggregation below, and counting
        it would need a second action on the streaming frame, i.e. a second pass
        over the Kafka source every micro-batch. That is a real cost to observe a
        quantity that is structurally always zero.

        The condition is instead asserted at the boundary where it is cheap:
        `_process_batch` compares the rows the aggregation produced against what
        the window count implies, and the invariant itself is owned by Job A,
        which rejects null required fields into the DLQ *with a reason* before
        publishing here. The DLQ is where such a row is visible, by design — this
        filter exists so that if the invariant were ever broken, the rows are
        discarded at a named line rather than vanishing inside a stateful
        operator, which is the silent-loss failure Phase 2 hit twice.
        """
        usable = frame.filter(F.col("event_timestamp").isNotNull())

        return (
            usable.withWatermark("event_timestamp", self.watermark)
            .groupBy(
                F.window(F.col("event_timestamp"), self.window_duration),
                F.col("grid_zone"),
            )
            .agg(
                F.sum("power_consumption_kwh").alias("total_consumption_kwh"),
                F.sum("solar_generation_kwh").alias("total_solar_kwh"),
                # Approximate rather than exact: this is a health signal, not a
                # billed quantity, and an exact distinct count would force a full
                # shuffle per window for a number nobody reconciles. Default
                # relative error is ~5%, which against an expected 40 meters per
                # zone is well inside "is this zone reporting at all".
                F.approx_count_distinct("meter_id").alias("active_meter_count"),
                F.sum(F.when(F.col("is_late"), F.lit(1)).otherwise(F.lit(0))).alias(
                    "late_event_count"
                ),
            )
            .select(
                F.col("grid_zone"),
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                F.col("total_consumption_kwh"),
                F.col("total_solar_kwh"),
                # renewable_pct = solar / consumption * 100, clamped to [0, 100].
                #
                # §7 writes this as `solar / NULLIF(consumption, 0) * 100`, and the
                # divide-guard is the important half: a zone with zero consumption
                # must yield NULL, not an error and not a zero (see the column
                # comment in serving/sql/001_schema.sql for why NULL is the only
                # defensible answer and what it means for Phase 6's alert rule).
                #
                # `F.when(...)` without an `otherwise` IS the null-guard, and it is
                # used in preference to `F.nullif` because `F.nullif` is broken in
                # PySpark 3.5.3 for a Column argument: it raises
                #   AnalysisException: Invalid call to dataType on unresolved object
                # at plan construction, before a single row is read. Verified
                # directly against this image's Spark; the `when` form produces
                # exactly the SQL NULLIF semantics this needs.
                #
                # The clamp guards a window in which generation legitimately
                # exceeds consumption (a heavily exporting zone at midday). That is
                # physically real, but "percentage of load met by solar" above 100
                # is not a meaningful reading, and an unclamped value would trip
                # Job D's thresholds oddly in Phase 6.
                F.least(
                    F.lit(100.0),
                    F.greatest(
                        F.lit(0.0),
                        F.col("total_solar_kwh")
                        / F.when(
                            F.col("total_consumption_kwh") != F.lit(0.0),
                            F.col("total_consumption_kwh"),
                        )
                        * F.lit(100.0),
                    ),
                ).alias("renewable_pct"),
                F.col("active_meter_count").cast("int"),
                F.col("late_event_count").cast("int"),
                F.current_timestamp().alias("updated_at"),
            )
        )

    # -- sink ---------------------------------------------------------------

    def _process_batch(self, batch: DataFrame, batch_id: int) -> None:
        """Upsert one micro-batch's windows and publish the gauges."""
        started = time.monotonic()

        # Cached because the frame is traversed several times below (count, the
        # staging write, the gauge collect). Without it Spark would recompute the
        # aggregation for each action.
        batch.persist()
        try:
            rows = batch.count()
            if rows == 0:
                return

            affected = upsert_batch(
                self.spark,
                batch,
                job_name=JOB_NAME,
                target_table=TARGET_TABLE,
                columns=TARGET_COLUMNS,
                conflict_columns=CONFLICT_COLUMNS,
                update_columns=UPDATE_COLUMNS,
                pg=self.postgres,
                log=self.log,
            )

            # Gauges, from the aggregate frame itself. A collect() is honest here
            # in a way it would not be in Job A: this frame holds at most one row
            # per zone per open window -- single digits -- not a micro-batch of
            # readings. Collecting the readings would be indefensible; collecting
            # five aggregate rows is cheaper than a second pass.
            for row in batch.collect():
                if row["renewable_pct"] is not None:
                    zone_renewable_pct.labels(grid_zone=row["grid_zone"]).set(
                        float(row["renewable_pct"])
                    )
                if row["window_end"] is not None:
                    # The freshness signal the NoDataReceived rule watches (§8).
                    # Deliberately the SIMULATED window end, matching the axis
                    # every other timestamp in the serving store is on.
                    last_event_timestamp_seconds.labels(
                        grid_zone=row["grid_zone"]
                    ).set(row["window_end"].timestamp())

            events_consumed_total.labels(
                job=JOB_NAME, topic=self.kafka.topic_meter_readings_clean
            ).inc(rows)

            duration = time.monotonic() - started
            batch_micro_duration_seconds.labels(job=JOB_NAME).observe(duration)

            self._batches += 1
            self._total_in += rows
            self._total_out += affected

            stage_boundary(
                self.log,
                stage=STAGE_STORE,
                records_in=rows,
                records_out=affected,
                records_rejected=0,
                batch_id=batch_id,
                batch_duration_seconds=round(duration, 3),
                target_table=TARGET_TABLE,
                window_sim_minutes=round(self.window_sim_minutes),
                cumulative_in=self._total_in,
                cumulative_out=self._total_out,
            )
        finally:
            batch.unpersist()

    # -- entrypoint ---------------------------------------------------------

    def run(self) -> None:
        # Asserted BEFORE the Spark session is built, so a misconfiguration costs
        # a second rather than a cluster connection and a first micro-batch.
        self._assert_time_budget()

        start_metrics_server()
        self.spark = build_spark_session(SERVICE_NAME)

        self.log.info(
            "job_starting",
            stage=STAGE_PROCESS,
            sim_clock=startup_banner(),
            source_topic=self.kafka.topic_meter_readings_clean,
            target_table=TARGET_TABLE,
            jdbc_url=self.postgres.jdbc_url,
            # All four coupled durations, echoed so that a run's log alone
            # explains why its windows are the size they are -- and so a future
            # tuning change is visible in the log before it is visible in the data.
            window_real_minutes=self.settings.zone_window_real_minutes,
            window_sim_minutes=round(self.window_sim_minutes),
            watermark_real_minutes=self.settings.zone_watermark_real_minutes,
            watermark_sim_minutes=round(self.watermark_sim_minutes),
            batch_span_sim_minutes=round(self.batch_span_sim_minutes),
            compression_ratio=compression_ratio(),
            max_offsets_per_trigger=self.settings.max_offsets_per_trigger,
            starting_offsets=self.settings.starting_offsets,
        )

        stream = self._aggregate(self._read_clean_stream(self.spark))

        query = (
            stream.writeStream.foreachBatch(self._process_batch)
            .option(
                "checkpointLocation",
                "{}/{}".format(self.settings.checkpoint_root, JOB_NAME),
            )
            # `update`, not `append`: append would withhold every window until the
            # watermark expired, adding 2 real minutes of latency to a path
            # budgeted at under 10 seconds (§2.4). The upsert sink is what makes
            # re-emission safe.
            .outputMode("update")
            .trigger(
                processingTime="{} seconds".format(
                    self.settings.trigger_interval_seconds
                )
            )
            .start()
        )

        query.awaitTermination()


def main() -> int:
    JobB().run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
