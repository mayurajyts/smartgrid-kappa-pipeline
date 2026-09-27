"""Enrichment for Job A — the stream-static dimension join and derived columns.

WHY THIS EXISTS IN THE ARCHITECTURE
-----------------------------------
§7 is explicit that transformations must be "meaningful, not pass-through", and
enrichment is where a raw reading becomes a record the rest of the system can answer
questions from. Three things happen here, each serving a specific downstream need:

  1. THE DIMENSION JOIN brings in household attributes (`has_solar`,
     `solar_capacity_kw`) and zone metadata (`name`, `capacity_kw`). Without it,
     validation has no physical bound to check solar against and Job D's
     ZONE_OVERLOAD alert has no capacity to compare zone load to.

  2. `net_grid_kwh = consumption - solar` is the number the bill is actually
     computed from. A household generating more than it consumes draws nothing from
     the grid and is owed an export credit, so this signed value — not gross
     consumption — is what Phase 3's billing maths sums.

  3. `is_exporting` and `time_of_day_bucket` are the dimensions the daily report
     slices by ("solar contribution league table", peak-hour analysis).

WHY BROADCAST, AND WHAT IT COSTS
--------------------------------
The dimension is ~200 households and 5 zones, read from the CSVs committed in
`simulators/reference/`. At that size a broadcast join sends the whole dimension to
every executor and the join becomes a hash lookup with no shuffle — the reading
stream is never repartitioned, so per-zone ordering survives the join.

TRADE-OFF (deliberate, and named in the report's limitations section §12):
broadcasting does not scale. At a few hundred thousand households the dimension
stops fitting comfortably in executor memory and this becomes a genuine stream-static
join against a partitioned store, or a stateful stream-stream join. At 200 rows,
paying that complexity now would be speculative.

READING THE DIMENSION FROM THE SAME CSVs AS THE SIMULATOR
---------------------------------------------------------
This is the load-bearing detail. `solar_capacity_kw` here is the identical value the
meter simulator generated from, because both read `simulators/reference_data.py`.
Validation's "solar above physical capacity" rule is therefore checking against the
same bound the producer respected. If Job A had its own copy of the dimension, the
two could drift and valid readings would be rejected as physically impossible — a
data-quality error caused entirely by a duplicated source of truth.

A LEFT JOIN, DELIBERATELY
-------------------------
The join is a LEFT join, not inner. An inner join would silently DROP any reading
whose household is absent from the dimension — the reading would vanish with no
error, no DLQ record and no count discrepancy anywhere. A left join keeps the record
with null dimension columns, and validation then rejects it into the DLQ with a
reason. Losing data quietly is the worse failure; this converts it into a visible
one.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from simulators.load_profile import time_of_day_bucket
from simulators.reference_data import load_households, load_zones


def build_dimension(spark: SparkSession) -> DataFrame:
    """Build the broadcast dimension: households joined to their zones.

    Built from the committed CSVs via `simulators.reference_data`, which validates
    them and cross-checks every household's `grid_zone` against a real zone. That
    validation happening at load time means a malformed dimension fails the job at
    startup rather than producing nulls in the middle of a stream.

    Returned already marked with `broadcast()`, so a caller cannot forget to and
    accidentally trigger a shuffle join that would destroy per-zone ordering.
    """
    households = load_households()
    zones = load_zones()

    rows = []
    for household in households:
        zone = zones[household.grid_zone]
        rows.append(
            {
                "household_id": household.household_id,
                "dim_meter_id": household.meter_id,
                "dim_grid_zone": household.grid_zone,
                "has_solar": household.has_solar,
                "solar_capacity_kw": float(household.solar_capacity_kw),
                "base_load_kw": float(household.base_load_kw),
                "zone_name": zone.name,
                "zone_capacity_kw": float(zone.capacity_kw),
            }
        )

    # createDataFrame from Python objects rather than spark.read.csv: the CSVs have
    # already been parsed and validated by the pydantic models, so re-reading them
    # through Spark would mean a second, unvalidated parse that could disagree with
    # the first.
    return F.broadcast(spark.createDataFrame(rows))


def join_dimension(readings: DataFrame, dimension: DataFrame) -> DataFrame:
    """Left-join readings to the broadcast dimension on `household_id`.

    See the module docstring on why this is a left join. Dimension columns are
    prefixed `dim_` where they duplicate a reading field, so the reading's own
    (possibly corrupted) value stays distinguishable from the authoritative one —
    which matters when the null-field fault has removed the reading's `grid_zone`.
    """
    return readings.join(dimension, on="household_id", how="left")


def net_grid_kwh() -> Column:
    """Signed net flow: positive means drawn from the grid, negative means exported.

    Deliberately signed and NOT clamped at zero. The sign is the whole point: it is
    what `is_exporting` tests and what the export-credit branch of the billing maths
    keys off. Clamping here would make solar households look like they simply consumed
    less, and the export credit could never be computed.
    """
    return F.col("power_consumption_kwh") - F.col("solar_generation_kwh")


def time_of_day_bucket_column() -> Column:
    """Map `event_timestamp` to a coarse period label.

    The boundaries are read from `simulators.load_profile.time_of_day_bucket` rather
    than written again here, so the label a reading carries corresponds to the load
    shape that actually produced it. Re-declaring the hours would let the producer's
    "evening peak" and the report's "EVENING" bucket drift apart, and the daily
    report's peak-hour analysis would be quietly describing the wrong window.

    Built as a CASE over the hour rather than as a UDF: the boundaries are derived
    once in Python at plan time, then the comparison runs natively in the JVM.
    """
    from datetime import datetime, timezone

    hour = F.hour(F.col("event_timestamp"))

    # Derive the boundaries by asking the shared function what each hour maps to.
    # This keeps a single source of truth without paying a per-row Python call.
    boundaries = {
        h: time_of_day_bucket(datetime(2026, 1, 1, h, tzinfo=timezone.utc))
        for h in range(24)
    }

    expression = F.when(hour == F.lit(0), F.lit(boundaries[0]))
    for h in range(1, 24):
        expression = expression.when(hour == F.lit(h), F.lit(boundaries[h]))
    return expression.otherwise(F.lit("UNKNOWN"))


def enrich(readings: DataFrame) -> DataFrame:
    """Add the §7 step 4 derived columns to a dimension-joined frame.

    Args:
        readings: validated readings already joined to the dimension.

    Returns:
        The frame with `net_grid_kwh`, `is_exporting`, `time_of_day_bucket` and
        `self_consumption_kwh` added.
    """
    enriched = readings.withColumn("net_grid_kwh", net_grid_kwh())

    return (
        enriched
        # Strictly less than zero: a household exactly in balance is not exporting.
        # The boundary matters because `is_exporting` gates the export credit, and
        # crediting a household for zero exported energy would be wrong.
        .withColumn("is_exporting", F.col("net_grid_kwh") < F.lit(0))
        .withColumn("time_of_day_bucket", time_of_day_bucket_column())
        # Solar actually used on site rather than exported — the numerator of R6's
        # self-consumption ratio. Computed here, once, so Phase 3's billing job and
        # the daily report cannot derive it two slightly different ways.
        .withColumn(
            "self_consumption_kwh",
            F.least(F.col("solar_generation_kwh"), F.col("power_consumption_kwh")),
        )
    )
