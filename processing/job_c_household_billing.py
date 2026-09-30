"""Job C — household billing via a stream-static join (§7 Job C, R5/R6).

WHY THIS EXISTS IN THE ARCHITECTURE
-----------------------------------
This is the job the architecture decision was made for. §2.2a argues that Kappa
fits this use case *because* the daily tariff feed is a small, keyed,
slowly-changing DIMENSION rather than a high-volume fact extract — which makes it
a log-compacted Kafka topic and a stream-static join, not a reason to run a second
batch engine. This job is that join. If it needed a batch layer, the Kappa
argument would be weaker than the report claims.

It answers R5 (per-household bill for the simulated day) and R6 (solar
contribution and self-consumption ratio), and it applies
`processing/transforms/billing.py` — the single implementation of the billing
maths that §2.2d cites as the concrete reason not to maintain two codebases.

WHAT THIS JOB DELIBERATELY DOES NOT DO
--------------------------------------
It writes `household_billing_running` and NEVER `household_billing_daily`.

That separation is the R7 mechanism. The running table is a LIVE ESTIMATE,
recomputed every micro-batch and allowed to be incomplete — it exists before the
day has closed and often before the day's tariff has arrived at all. The daily
table is the ISSUED bill: versioned, `is_current`-flagged, and materialised by
Airflow (Phase 5) once the day is sealed and priced. If this job wrote the daily
table, "which version of the bill is in force, and who authored it" would stop
having a clean answer, and the restatement demo would lose its meaning.

So a missing tariff here delays nothing that matters: the estimate simply carries
no cost yet, and no incorrect invoice is ever issued.

THE STREAM-STATIC JOIN, AND THE TRADE-OFF §7 ASKS TO BE DOCUMENTED
-------------------------------------------------------------------
§7: "Read the compacted `tariff.reference.v1` into a broadcast dimension,
refreshed per micro-batch inside `foreachBatch` (simplest correct approach;
document the trade-off vs. a true stream-stream join with state)."

Concretely, that is a BATCH read (`spark.read`, earliest to latest) of the whole
tariff topic on every micro-batch. Because the topic is compacted on
`household_id`, "the whole topic" is one record per household — about 200 rows —
so it costs a few hundred milliseconds against a 5-second trigger.

  * THIS APPROACH: no join state, no second watermark to reason about, and the
    join always sees the newest published tariff. Its cost is O(households) per
    batch, re-reading the entire topic each time, which would become the dominant
    expense at utility scale. It is also NOT point-in-time correct: a batch
    replayed tomorrow joins against tomorrow's tariff, not the one that was
    current when the batch originally ran.

  * A STREAM-STREAM JOIN would be point-in-time correct and would read each tariff
    record once. But the tariff for day D arrives during day D+1 (§14), a full
    simulated day AFTER the readings it prices. The join watermark would therefore
    have to span 24 simulated hours on both sides, holding every reading for every
    household in state before a single row could be emitted — and nothing could be
    served until the tariff landed. That is flatly incompatible with §7's own
    requirement that running kWh be written immediately with a NULL rate. The
    correct-sounding option is the one that cannot meet the requirement.

  * WHY THE NON-CORRECTNESS IS ACCEPTABLE: the authoritative bill is
    `household_billing_daily`, produced after the tariff has landed and versioned
    so that a restatement is an explicit new version rather than a silent change.
    The running table is documented as an estimate. The imprecision is confined to
    a table that is defined to be imprecise.

COMPACTION IS EVENTUAL, SO THE READ REDUCES TO THE LATEST ROW ITSELF
---------------------------------------------------------------------
Kafka's log cleaner only compacts CLOSED segments, so a batch read of a compacted
topic can still return several versions of the same household's tariff. Taking
whichever row happened to come back would make the bill depend on broker
background timing — exactly the non-determinism the Kappa replay argument cannot
tolerate. So the read explicitly keeps the latest row per
`(household_id, sim_date)` by `effective_from`, which is correct whether or not
the cleaner has run.

BOUNDED STATE, AND WHY THE AGGREGATION IS WATERMARKED
------------------------------------------------------
The running total is a streaming aggregation grouped by
`(household_id, sim_date)`. Without a watermark, Spark would keep state for every
household for every simulated day forever — and at 288x compression the simulated
days accumulate fast. The watermark bounds it: state for a day is released once
the watermark has passed beyond it.

The watermark is expressed in REAL minutes and converted by `compression_ratio()`,
for the same reason as Job A's and Job B's — a literal reading on the simulated
axis would be a fraction of a real second and would drop nearly every record. It
matches Job A's value deliberately, since both consume the same physical stream.

WHY THE SINK MUST NOT ACCUMULATE
--------------------------------
Spark's state holds the running total; the sink publishes its CURRENT VALUE with
`ON CONFLICT DO UPDATE SET x = EXCLUDED.x`. An accumulating upsert
(`SET x = target.x + EXCLUDED.x`) would look natural for something called a
"running" total and would double-count on every replayed batch — failing the
Phase 3 checkpoint. That constraint is enforced in
`processing/sinks/postgres_sink.py` and asserted in its tests.
"""

from __future__ import annotations

import sys
import time
from decimal import Decimal
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common.config import get_settings
from common.logging_setup import (
    STAGE_PROCESS,
    STAGE_STORE,
    configure_logging,
    stage_boundary,
)
from common.metrics import (
    batch_micro_duration_seconds,
    billing_unpriced_households,
    events_consumed_total,
    start_metrics_server,
)
from common.schemas import clean_reading_spark_schema, tariff_reference_spark_schema
from common.sim_clock import compression_ratio, startup_banner
from processing.sinks.postgres_sink import upsert_batch
from processing.spark_session import build_spark_session, kafka_read_options
from processing.transforms.billing import (
    BillingSettings,
    BillInputs,
    compute_bill,
    to_money,
)

SERVICE_NAME = "job-c-household-billing"
JOB_NAME = "job_c_household_billing"
TARGET_TABLE = "household_billing_running"

TARGET_COLUMNS = (
    "household_id",
    "sim_date",
    "consumption_kwh",
    "solar_kwh",
    "net_grid_kwh",
    "self_consumption_ratio",
    "running_cost",
    "tariff_missing",
    "updated_at",
)
CONFLICT_COLUMNS = ("household_id", "sim_date")
UPDATE_COLUMNS = tuple(c for c in TARGET_COLUMNS if c not in CONFLICT_COLUMNS)


class JobCSettings(BaseSettings):
    """Job C settings, env-driven."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # REAL minutes, converted by compression_ratio() — see the module docstring.
    # Matches Job A's dedupe watermark: both jobs consume the same physical
    # stream, so a narrower watermark here would discard readings Job A had
    # already certified as on-time.
    billing_watermark_real_minutes: float = Field(default=2.0, gt=0)

    trigger_interval_seconds: int = Field(default=5, gt=0)
    max_offsets_per_trigger: int = Field(default=2000, gt=0)
    starting_offsets: str = Field(default="latest")
    checkpoint_root: str = Field(default="/data/checkpoints")


class JobC:
    def __init__(self) -> None:
        self.settings = JobCSettings()
        self.billing = BillingSettings()
        self.kafka = get_settings().kafka
        self.postgres = get_settings().postgres
        self.log = configure_logging(SERVICE_NAME, JOB_NAME, stage=STAGE_PROCESS)
        self.spark: Optional[SparkSession] = None

        self._batches = 0
        self._total_in = 0
        self._total_out = 0

        self.watermark_sim_minutes = (
            self.settings.billing_watermark_real_minutes * compression_ratio()
        )
        self.watermark = "{:.0f} minutes".format(self.watermark_sim_minutes)

    # -- pipeline construction ----------------------------------------------

    def _read_clean_stream(self, spark: SparkSession) -> DataFrame:
        """Read `meter.readings.clean.v1` against the EXPLICIT shared schema.

        The schema is generated from the same field tuple Job A's writer selects
        against (common/schemas.py), so producer and consumer cannot drift.
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

        return (
            raw.select(
                F.from_json(
                    F.col("value").cast("string"), clean_reading_spark_schema()
                ).alias("data")
            )
            .select("data.*")
            .withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp")))
            .withColumn("sim_date", F.to_date(F.col("sim_date")))
            # Dropped explicitly rather than left to vanish inside the stateful
            # operator. Job A rejects null required fields into the DLQ before
            # publishing here, so this filters nothing in practice — but a null
            # watermark column silently excludes a row from the aggregation, and
            # that silent loss is the failure Phase 2 hit twice.
            .filter(F.col("event_timestamp").isNotNull())
            .filter(F.col("household_id").isNotNull())
        )

    def _aggregate(self, frame: DataFrame) -> DataFrame:
        """Running per-household totals for the simulated day (§7 Job C).

        Grouped by `(household_id, sim_date)` rather than by household alone: a
        bill belongs to a day, and §14 fixes the day as the billing period. The
        `sim_date` in the group key is also what lets the watermark release a
        completed day's state.
        """
        return (
            frame.withWatermark("event_timestamp", self.watermark)
            .groupBy(F.col("household_id"), F.col("sim_date"))
            .agg(
                F.sum("power_consumption_kwh").alias("consumption_kwh"),
                F.sum("solar_generation_kwh").alias("solar_kwh"),
                # Summing the SIGNED per-interval net preserves the distinction
                # between importing and exporting across the day: a household that
                # exports at midday and draws at night nets out correctly, which
                # summing clamped values would not.
                F.sum("net_grid_kwh").alias("net_grid_kwh"),
                F.sum("self_consumption_kwh").alias("self_consumed_kwh"),
            )
        )

    def _load_tariff_dimension(self, spark: SparkSession) -> DataFrame:
        """Batch-read the compacted tariff topic and broadcast it.

        A BATCH read on every micro-batch — see the module docstring for the cost
        and the trade-off against a stream-stream join.

        The reduction to the latest row per `(household_id, sim_date)` is not
        belt-and-braces: Kafka compacts only closed segments, so several versions
        of one household's tariff can legitimately still be on the topic. Relying
        on compaction alone would make the bill depend on when the log cleaner
        last ran, which is precisely the non-determinism that would break the
        replay guarantee.
        """
        raw = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka.kafka_bootstrap_servers)
            .option("subscribe", self.kafka.topic_tariff_reference)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )

        parsed = (
            raw.select(
                F.from_json(
                    F.col("value").cast("string"), tariff_reference_spark_schema()
                ).alias("t")
            )
            .select("t.*")
            .withColumn("sim_date", F.to_date(F.col("sim_date")))
            .withColumn("effective_from", F.to_timestamp(F.col("effective_from")))
            .filter(F.col("household_id").isNotNull())
        )

        latest = (
            parsed.withColumn(
                "_rn",
                F.row_number().over(
                    Window.partitionBy("household_id", "sim_date").orderBy(
                        F.col("effective_from").desc()
                    )
                ),
            )
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .select("household_id", "sim_date", "tariff_rate", "billing_tier",
                    "subsidy_flag")
        )

        # Marked explicitly rather than left to the size heuristic: the dimension
        # is ~200 rows and must never shuffle. Same argument as enrichment.py.
        return F.broadcast(latest)

    # -- pricing -------------------------------------------------------------

    def _price(self, joined: DataFrame) -> DataFrame:
        """Apply the billing maths and shape the rows for the serving table.

        The pricing itself runs in a Python UDF over `processing/transforms/
        billing.py`, rather than being reimplemented as Spark column expressions.
        That is a deliberate performance-for-correctness trade:

          * A column-expression version would run in the JVM and avoid the
            Python round trip — but it would be a SECOND implementation of the
            block ladder, in a language where `Decimal` is not available, which
            is exactly the dual-logic bug class §2.2d rejects Lambda over. The
            unit tests would then cover the wrong one of the two.

          * The UDF returns Decimal via DecimalType, so the exactness holds all
            the way into Postgres' NUMERIC columns. A DoubleType return would
            quietly undo everything billing.py does.

        The cost is real but small at this scale: one Python call per household
        per micro-batch, i.e. ~200, not one per reading.
        """
        billing = self.billing

        result_type = T.StructType(
            [
                T.StructField("running_cost", T.DecimalType(14, 2), True),
                T.StructField("tariff_missing", T.BooleanType(), False),
            ]
        )

        def price_row(net_grid_kwh, consumption_kwh, solar_kwh, tariff_rate,
                      billing_tier, subsidy_flag):
            outputs = compute_bill(
                BillInputs(
                    consumption_kwh=to_money(consumption_kwh or 0.0),
                    solar_kwh=to_money(solar_kwh or 0.0),
                    net_grid_kwh=to_money(net_grid_kwh or 0.0),
                    tariff_rate=(
                        to_money(tariff_rate) if tariff_rate is not None else None
                    ),
                    billing_tier=billing_tier,
                    subsidy_flag=bool(subsidy_flag),
                ),
                billing,
            )
            return (outputs.final_bill, outputs.tariff_missing)

        price_udf = F.udf(price_row, result_type)

        priced = joined.withColumn(
            "_bill",
            price_udf(
                F.col("net_grid_kwh"),
                F.col("consumption_kwh"),
                F.col("solar_kwh"),
                F.col("tariff_rate"),
                F.col("billing_tier"),
                F.col("subsidy_flag"),
            ),
        )

        return priced.select(
            F.col("household_id"),
            F.col("sim_date"),
            F.col("consumption_kwh").cast(T.DecimalType(18, 6)),
            F.col("solar_kwh").cast(T.DecimalType(18, 6)),
            F.col("net_grid_kwh").cast(T.DecimalType(18, 6)),
            # self_consumed / generated. NULL rather than 0 when the household
            # generated nothing: a house with no panels has no self-consumption
            # ratio, which is a different statement from a ratio of zero. The
            # `when` guard is also the divide-by-zero guard (F.nullif is broken
            # in PySpark 3.5.3 — see the note in job_b_zone_aggregates.py).
            (
                F.col("self_consumed_kwh")
                / F.when(F.col("solar_kwh") != F.lit(0.0), F.col("solar_kwh"))
            )
            .cast(T.DecimalType(6, 4))
            .alias("self_consumption_ratio"),
            F.col("_bill.running_cost").alias("running_cost"),
            F.col("_bill.tariff_missing").alias("tariff_missing"),
            F.current_timestamp().alias("updated_at"),
        )

    # -- sink ----------------------------------------------------------------

    def _process_batch(self, batch: DataFrame, batch_id: int) -> None:
        """Join against the tariff dimension, price, and upsert."""
        started = time.monotonic()

        batch.persist()
        try:
            rows = batch.count()
            if rows == 0:
                return

            tariff = self._load_tariff_dimension(self.spark)
            tariff_rows = tariff.count()

            # LEFT join, for the same reason enrichment.py gives: an inner join
            # would make every household without a tariff VANISH, which is exactly
            # the silent loss §7's missing-tariff rule exists to prevent. The
            # un-priced rows must reach the serving table carrying their kWh.
            joined = batch.join(tariff, on=["household_id", "sim_date"], how="left")

            priced = self._price(joined)
            priced.persist()
            try:
                unpriced = priced.filter(F.col("tariff_missing")).count()

                affected = upsert_batch(
                    self.spark,
                    priced,
                    job_name=JOB_NAME,
                    target_table=TARGET_TABLE,
                    columns=TARGET_COLUMNS,
                    conflict_columns=CONFLICT_COLUMNS,
                    update_columns=UPDATE_COLUMNS,
                    pg=self.postgres,
                    log=self.log,
                )

                # Labelled by the simulated day the un-priced households belong
                # to, so the expected sawtooth is visible per day rather than
                # smeared across the boundary.
                sim_dates = [
                    r["sim_date"]
                    for r in priced.select("sim_date").distinct().collect()
                    if r["sim_date"] is not None
                ]
                for sim_date in sim_dates:
                    count = priced.filter(
                        F.col("tariff_missing") & (F.col("sim_date") == F.lit(sim_date))
                    ).count()
                    billing_unpriced_households.labels(
                        sim_date=sim_date.isoformat()
                    ).set(count)
            finally:
                priced.unpersist()

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
                # The two numbers that explain an un-priced spike without needing
                # the data: how many households had no tariff, and how many rows
                # the dimension actually held when they were joined. A spike with
                # tariff_dimension_rows=0 means the loader has stalled; a spike
                # with a healthy dimension means the sim-day has just rolled over.
                unpriced_households=unpriced,
                tariff_dimension_rows=tariff_rows,
                cumulative_in=self._total_in,
                cumulative_out=self._total_out,
            )
        finally:
            batch.unpersist()

    # -- entrypoint ----------------------------------------------------------

    def run(self) -> None:
        start_metrics_server()
        self.spark = build_spark_session(SERVICE_NAME)

        self.log.info(
            "job_starting",
            stage=STAGE_PROCESS,
            sim_clock=startup_banner(),
            source_topic=self.kafka.topic_meter_readings_clean,
            tariff_topic=self.kafka.topic_tariff_reference,
            target_table=TARGET_TABLE,
            jdbc_url=self.postgres.jdbc_url,
            watermark_real_minutes=self.settings.billing_watermark_real_minutes,
            watermark_sim_minutes=round(self.watermark_sim_minutes),
            compression_ratio=compression_ratio(),
            starting_offsets=self.settings.starting_offsets,
            max_offsets_per_trigger=self.settings.max_offsets_per_trigger,
            # The billing configuration, echoed so a bill can be explained from
            # the log of the run that produced it — which is what makes a
            # restatement auditable rather than merely possible.
            block_kwh=str(self.billing.billing_block_kwh),
            tier_rates=[
                str(self.billing.billing_tier_1_rate),
                str(self.billing.billing_tier_2_rate),
                str(self.billing.billing_tier_3_rate),
                str(self.billing.billing_tier_4_rate),
            ],
            export_credit_fraction=str(self.billing.billing_export_credit_fraction),
            subsidy_discount_pct=str(self.billing.billing_subsidy_discount_pct),
        )

        stream = self._aggregate(self._read_clean_stream(self.spark))

        query = (
            stream.writeStream.foreachBatch(self._process_batch)
            .option(
                "checkpointLocation",
                "{}/{}".format(self.settings.checkpoint_root, JOB_NAME),
            )
            # `update`: the running total is re-emitted for every household seen
            # in the batch, and the upsert replaces the previous row.
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
    JobC().run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
