"""Data contracts (§6) — one definition of every record shape in the platform.

WHY THIS EXISTS IN THE ARCHITECTURE
-----------------------------------
Three different runtimes touch the same records: the simulators (which produce
them), the Spark jobs (which parse and transform them) and the API (which serves
derived results). If each declares its own view of `meter.readings.v1`, a field
added on the producer side is silently dropped on the consumer side and the
first symptom is a wrong bill. This module is the single source of truth, in
both representations the platform needs:

  * Pydantic models — used by the producers to VALIDATE BEFORE PUBLISH, so a
    malformed record never enters the log. The Kafka log is immutable; garbage
    written into it is garbage forever, replayed on every reprocessing run.
  * Spark StructTypes — used by the streaming jobs to parse the JSON payload.

The two are declared adjacently and deliberately field-for-field identical, so a
contract change that updates one and not the other is visible in a single diff.

THE `inferSchema` RULE (§7 Job A, step 1)
-----------------------------------------
Schemas here are EXPLICIT StructTypes, and `inferSchema` is never used on a
stream. Inference works by sampling data, which on an unbounded source means the
schema depends on whichever records happened to arrive first — it is
non-deterministic across restarts and across replays. For a Kappa system that is
disqualifying: the whole architecture rests on a replay producing identical
results to the original run, and a schema that can differ between those two runs
breaks that guarantee before any business logic executes. A secondary benefit is
that an explicit schema turns an unexpected field into a visible null rather
than a silent type change.

TRADE-OFF (deliberate)
----------------------
Hand-maintaining two representations is duplication, and the honest alternative
is a schema registry with Avro, which generates both and enforces compatibility
on publish. That is named in the report's production-scale section. It is not
used here because it adds a container and a build step for a project whose
contracts are five small records that change only when the author changes them;
the `schema_version` integer on each payload is the cheap stand-in, letting a
future `.v2` topic coexist with `.v1` during a migration.

PySpark is imported lazily: the simulators, loader and API images do not install
Spark, and they must still be able to import the pydantic half of this module.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

# Bumped when a contract changes shape. Carried on every payload so a consumer
# can route or reject records it does not understand rather than misreading them.
SCHEMA_VERSION = 1


# ---------------------------------------------------------------------------
# Pydantic models — producer-side validation
# ---------------------------------------------------------------------------


class _Contract(BaseModel):
    """Base for all wire contracts.

    `extra="forbid"` is intentional: an unexpected field means producer and
    consumer disagree about the contract, and failing loudly at publish time is
    far cheaper than discovering it after the field has been sitting in the
    immutable log for a day.
    """

    model_config = ConfigDict(extra="forbid")


class MeterReading(_Contract):
    """A single smart-meter interval reading — `meter.readings.v1` (§6.1).

    Kafka key is `grid_zone`, giving per-zone ordering across the 6 partitions,
    which the 1-minute tumbling zone aggregates depend on.
    """

    event_id: str = Field(description="UUID4; the deduplication key in Job A.")
    meter_id: str
    household_id: str
    grid_zone: str

    # CRITICAL SEMANTIC (§6.1): these are PER-INTERVAL kWh, not cumulative meter
    # counters. Cumulative readings would require differencing against the
    # previous reading per meter — stateful, and wrong whenever a reading is
    # lost or replayed. Per-interval values are additive, which is what makes
    # the billing aggregation a plain sum and makes replay safe.
    power_consumption_kwh: float = Field(ge=0)
    solar_generation_kwh: float = Field(ge=0)

    # Event time, in SIMULATED time — the axis all windowing and watermarking
    # uses. Distinct from producer_emitted_at on purpose: the gap between them
    # is how late-arrival is measured and how injected late events are detected.
    event_timestamp: datetime
    sim_date: date

    # Real wall-clock publish time. Used for the end-to-end latency histogram
    # and to prove that a "late" event was genuinely late rather than reordered.
    producer_emitted_at: datetime

    correlation_id: str = Field(
        description="Minted by the producer, carried on the Kafka header and "
        "through every stage; the thread the DLQ trace demo follows."
    )
    schema_version: int = SCHEMA_VERSION


class TariffReference(_Contract):
    """Daily billing reference — `tariff.reference.v1`, LOG-COMPACTED (§6.2).

    Keyed by `household_id`, so compaction retains the latest tariff per
    household forever. That is precisely the broadcast dimension Job C needs for
    its stream-static join, obtained without a database or a separate batch
    layer — one of the concrete reasons Kappa fits this use case (§2.2a).

    It is also the mechanism behind R7: correcting a tariff means publishing a
    new record to this topic and replaying the affected day. No mutation, no
    reconciliation between two engines.
    """

    household_id: str
    sim_date: date
    tariff_rate: float = Field(gt=0, description="Base rate, LKR per kWh.")
    billing_tier: str
    subsidy_flag: bool
    effective_from: datetime
    schema_version: int = SCHEMA_VERSION


class WeatherForecast(_Contract):
    """Per-zone daily forecast — `weather.forecast.v1`, log-compacted (§6.3).

    Key is `grid_zone|sim_date` rather than `grid_zone` alone, because the demo
    needs to retain several days' forecasts to replay a past day. Compacting on
    `grid_zone` alone would discard the forecast that a replayed day's solar
    figures should be interpreted against.
    """

    grid_zone: str
    sim_date: date
    cloud_cover_pct: float = Field(ge=0, le=100)
    expected_solar_index: float = Field(
        ge=0, le=1, description="0-1 multiplier applied to the clear-sky solar curve."
    )
    forecast_issued_at: datetime
    schema_version: int = SCHEMA_VERSION

    @property
    def compaction_key(self) -> str:
        """Composite Kafka key; see the class docstring for why sim_date is in it."""
        return f"{self.grid_zone}|{self.sim_date.isoformat()}"


class DlqRecord(_Contract):
    """Dead-letter envelope — `meter.readings.dlq.v1` (§6.4).

    The original payload is kept as an OPAQUE STRING, not as a parsed model:
    records land here precisely because they failed to parse or validate, so a
    typed field would be unfillable for the very cases the DLQ exists to
    capture. Keeping the raw text also means the record can be corrected and
    republished once the underlying bug is fixed.
    """

    original_payload: str
    rejection_reason: str
    rejected_at: datetime
    job_name: str
    correlation_id: str
    schema_version: int = SCHEMA_VERSION


# ---------------------------------------------------------------------------
# Spark StructTypes — consumer-side parsing
# ---------------------------------------------------------------------------


def _spark_types() -> Any:
    """Import PySpark's type module only when a Spark job actually asks for it.

    Keeps this module importable in the simulator, loader and API images, which
    deliberately do not ship Spark.
    """
    try:
        from pyspark.sql import types as T
    except ImportError as exc:  # pragma: no cover - only hit outside Spark images
        raise ImportError(
            "PySpark is required for the Spark schema definitions in "
            "common.schemas; the pydantic contracts import without it."
        ) from exc
    return T


def meter_reading_spark_schema() -> Any:
    """Explicit StructType for `meter.readings.v1`.

    Every field is nullable at the PARSE layer even though the contract requires
    it. This is deliberate: a missing required field must arrive as a null that
    Job A's validation rejects into the DLQ *with a reason*, rather than killing
    the micro-batch. A streaming job that dies on one malformed record has
    turned a data-quality problem into an availability problem.

    Timestamps are parsed as strings and cast explicitly downstream, because
    Spark's implicit JSON timestamp parsing depends on session configuration,
    which would make replay results depend on how the cluster was started.
    """
    T = _spark_types()
    return T.StructType(
        [
            T.StructField("event_id", T.StringType(), True),
            T.StructField("meter_id", T.StringType(), True),
            T.StructField("household_id", T.StringType(), True),
            T.StructField("grid_zone", T.StringType(), True),
            # DoubleType, not DecimalType: these are physical measurements where
            # float error is orders of magnitude below meter precision. Money is
            # a different matter — the billing tables use NUMERIC in Postgres.
            T.StructField("power_consumption_kwh", T.DoubleType(), True),
            T.StructField("solar_generation_kwh", T.DoubleType(), True),
            T.StructField("event_timestamp", T.StringType(), True),
            T.StructField("sim_date", T.StringType(), True),
            T.StructField("producer_emitted_at", T.StringType(), True),
            T.StructField("correlation_id", T.StringType(), True),
            T.StructField("schema_version", T.IntegerType(), True),
        ]
    )


def tariff_reference_spark_schema() -> Any:
    """Explicit StructType for the compacted `tariff.reference.v1` dimension."""
    T = _spark_types()
    return T.StructType(
        [
            T.StructField("household_id", T.StringType(), True),
            T.StructField("sim_date", T.StringType(), True),
            T.StructField("tariff_rate", T.DoubleType(), True),
            T.StructField("billing_tier", T.StringType(), True),
            T.StructField("subsidy_flag", T.BooleanType(), True),
            T.StructField("effective_from", T.StringType(), True),
            T.StructField("schema_version", T.IntegerType(), True),
        ]
    )


def weather_forecast_spark_schema() -> Any:
    """Explicit StructType for the compacted `weather.forecast.v1` dimension."""
    T = _spark_types()
    return T.StructType(
        [
            T.StructField("grid_zone", T.StringType(), True),
            T.StructField("sim_date", T.StringType(), True),
            T.StructField("cloud_cover_pct", T.DoubleType(), True),
            T.StructField("expected_solar_index", T.DoubleType(), True),
            T.StructField("forecast_issued_at", T.StringType(), True),
            T.StructField("schema_version", T.IntegerType(), True),
        ]
    )


def dlq_spark_schema() -> Any:
    """Explicit StructType for `meter.readings.dlq.v1`."""
    T = _spark_types()
    return T.StructType(
        [
            T.StructField("original_payload", T.StringType(), True),
            T.StructField("rejection_reason", T.StringType(), True),
            T.StructField("rejected_at", T.StringType(), True),
            T.StructField("job_name", T.StringType(), True),
            T.StructField("correlation_id", T.StringType(), True),
            T.StructField("schema_version", T.IntegerType(), True),
        ]
    )
