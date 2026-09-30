"""Validation rules for Job A — pure Spark Column expressions, no I/O.

WHY THIS EXISTS IN THE ARCHITECTURE
-----------------------------------
This is the gate between the raw log and everything downstream. §7 step 2 requires
four rejections: null required fields, negative kWh, solar above the physical panel
cap, and an event timestamp too far in the future. Rejected records go to the DLQ
with a reason rather than being silently dropped or, worse, being allowed through to
corrupt a bill.

The reason is the deliverable, not just the rejection. §11 step 7 — the demo the
10-mark observability criterion is worded around — takes one DLQ record and traces
its `correlation_id` back through the logs. That is only meaningful if the reason on
the DLQ record is the *correct* one: a DLQ full of records all labelled
"unknown_error" would pass a count check while proving nothing.

REASON STRINGS ARE A CONTRACT WITH THE PRODUCER
-----------------------------------------------
The constants below are imported from `simulators.fault_injection` rather than
re-declared. This is deliberate and load-bearing: the producer records which fault
it injected, and Job A records why it rejected. If those two vocabularies could
drift, an injected `negative_kwh` might arrive in the DLQ as `null_required_field`
and the trace demo would be asserting a coincidence rather than a causal chain.
Importing the same constants makes a mismatch a NameError at startup instead of a
subtly wrong story in a viva.

`tests/unit/test_validation.py` asserts this correspondence explicitly.

WHY PURE COLUMN EXPRESSIONS AND NOT A UDF
-----------------------------------------
Every rule is built from Spark's native `Column` API, so the whole validation pass
executes inside the JVM with no Python serialisation per row. A Python UDF would
round-trip every record through the interpreter — at 200 readings per 2 seconds that
is survivable, but it would also make the rules opaque to Catalyst, losing predicate
pushdown and any chance of the optimiser reordering them.

The more important reason is testability: a Column expression can be asserted
against a small hand-built DataFrame in `tests/unit/`, which is how these rules get
tested before the job that calls them exists.

ONE REASON PER RECORD, IN A FIXED PRECEDENCE
--------------------------------------------
A record can violate several rules at once (the producer can null a field *and* the
row could be otherwise odd). The rules are therefore evaluated in a fixed order and
collapse to exactly one `rejection_reason`, because:

  * the DLQ schema (§6.4) has a single `rejection_reason` field, and
  * `smartgrid_events_rejected_total` is labelled by one reason, so a record counted
    under two reasons would inflate the reject rate above the true number of bad
    readings and could fail the 2% data-quality gate on arithmetic rather than on
    data.

Null checks come first, because a null field makes the other predicates
unevaluable — a null kWh is neither negative nor non-negative, and SQL three-valued
logic would return NULL rather than true, letting the record pass.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# Imported, never re-declared — see the module docstring.
from simulators.fault_injection import (
    REASON_NEGATIVE_KWH,
    REASON_NULL_FIELD,
    REASON_SOLAR_ABOVE_CAPACITY,
    REASON_TIMESTAMP_IN_FUTURE,
)

# Column added by `with_rejection_reason`. Null means the record is valid.
REJECTION_REASON_COLUMN = "rejection_reason"

# Fields without which a reading cannot be processed at all. Each one breaks a
# different downstream consumer, which is why all of them are required rather than
# defaulted:
#   event_id        - the deduplication key; without it dedupe cannot work
#   household_id    - the billing unit; an unattributable reading cannot be billed
#   grid_zone       - the partition and aggregation key
#   event_timestamp - the windowing and watermarking axis
#   the two kWh     - the measurements themselves
REQUIRED_FIELDS = (
    "event_id",
    "meter_id",
    "household_id",
    "grid_zone",
    "power_consumption_kwh",
    "solar_generation_kwh",
    "event_timestamp",
)


def any_required_field_is_null() -> Column:
    """True when any required field is missing.

    Checked FIRST. A null field makes every other predicate unevaluable: under
    SQL's three-valued logic `NULL < 0` is NULL, not true, so a null kWh would slip
    past the negative-kWh rule and reach the billing aggregation as a missing value.
    """
    condition = F.col(REQUIRED_FIELDS[0]).isNull()
    for field in REQUIRED_FIELDS[1:]:
        condition = condition | F.col(field).isNull()
    return condition


def has_negative_kwh() -> Column:
    """True when either energy measurement is negative.

    Physically impossible for per-interval readings: a meter cannot consume or
    generate negative energy over an interval. (Net *grid* flow can be negative —
    that is export — which is why `net_grid_kwh` is derived during enrichment and is
    deliberately not subject to this rule.)
    """
    return (F.col("power_consumption_kwh") < 0) | (F.col("solar_generation_kwh") < 0)


def solar_exceeds_physical_capacity(interval_seconds_column: Column) -> Column:
    """True when solar generation exceeds what the panels could physically produce.

    The bound is the household's installed capacity times the interval length, taken
    from the broadcast dimension — the SAME `solar_capacity_kw` the simulator
    generated from. That shared bound is why this rule cannot reject a legitimate
    reading: producer and validator agree on what is physically possible.

    Note the bound uses full nameplate capacity, not the derated peak the generator
    models. The derating factor describes typical output; this rule is a hard
    physical ceiling. Validating against the derated figure would reject a
    legitimately exceptional reading as impossible.

    A household with no panels has capacity 0, so ANY positive generation is
    impossible for it — which is correct, and is what catches the injected
    solar-spike fault on a non-solar household.
    """
    max_possible = F.col("solar_capacity_kw") * (interval_seconds_column / F.lit(3600.0))
    # A tiny tolerance absorbs float rounding in the producer's own kWh arithmetic,
    # so a reading exactly at capacity is not rejected by representation error.
    return F.col("solar_generation_kwh") > (max_possible + F.lit(1e-6))


def timestamp_too_far_in_future(tolerance_minutes: float) -> Column:
    """True when `event_timestamp` is implausibly ahead of the current simulated time.

    A future timestamp is not merely odd — it is actively damaging to a streaming
    system. Spark's watermark tracks the MAXIMUM event time seen, so one record
    stamped an hour ahead advances the watermark by an hour and causes every
    subsequent genuine reading to be discarded as late. A single bad clock on one
    meter could silence a whole zone.

    Compared against the simulated clock rather than wall-clock time, because
    `event_timestamp` is on the simulated axis. Comparing against `current_timestamp()`
    would reject every reading, since simulated time and real time are different
    scales entirely.

    The threshold is passed in (not read from config here) so this stays a pure
    function of its arguments and is directly testable.
    """
    # Seconds rather than minutes, and a float rather than an int: the caller
    # converts a REAL-minute tolerance onto the simulated axis by multiplying by the
    # compression ratio (see job_a_clean_enrich.py), which rarely yields a whole
    # number of minutes. `INTERVAL 288.0 MINUTES` is a SQL parse error, so the value
    # is carried as seconds and rounded once, here.
    tolerance_seconds = int(round(tolerance_minutes * 60))
    return F.col("event_timestamp") > (
        F.col("sim_now") + F.expr(f"INTERVAL {tolerance_seconds} SECONDS")
    )


def with_rejection_reason(
    frame: DataFrame,
    interval_seconds_column: Column,
    future_tolerance_minutes: float,
) -> DataFrame:
    """Add a `rejection_reason` column: the single reason, or null when valid.

    Args:
        frame: parsed readings, already joined to the broadcast dimension so
            `solar_capacity_kw` is available, and carrying a `sim_now` column with
            the current simulated instant.
        interval_seconds_column: simulated seconds each reading covers, used for the
            physical solar bound.
        future_tolerance_minutes: how far ahead of simulated now a timestamp may
            be, IN SIMULATED MINUTES. The caller is responsible for converting a
            real-time tolerance onto this axis; see job_a_clean_enrich.py.

    Returns:
        The frame with `rejection_reason` added. Callers split on
        `col("rejection_reason").isNull()`.

    Rules are applied as a chained CASE (`when`/`otherwise`), so Spark evaluates them
    in the written order and the FIRST match wins — which is what enforces one
    reason per record. The order is the precedence documented in the module
    docstring: nulls first, because they make the later predicates unevaluable.
    """
    reason = (
        F.when(any_required_field_is_null(), F.lit(REASON_NULL_FIELD))
        .when(has_negative_kwh(), F.lit(REASON_NEGATIVE_KWH))
        .when(
            solar_exceeds_physical_capacity(interval_seconds_column),
            F.lit(REASON_SOLAR_ABOVE_CAPACITY),
        )
        .when(
            timestamp_too_far_in_future(future_tolerance_minutes),
            F.lit(REASON_TIMESTAMP_IN_FUTURE),
        )
        .otherwise(F.lit(None).cast("string"))
    )
    return frame.withColumn(REJECTION_REASON_COLUMN, reason)


def split_valid_invalid(frame: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Partition a validated frame into (valid, invalid).

    Both halves are returned rather than filtering in place, because Job A must write
    the invalid half to the DLQ. Dropping it would make the reject count
    unverifiable and would leave the observability demo with nothing to trace —
    silently discarding bad data is the failure mode the DLQ exists to prevent.
    """
    valid = frame.filter(F.col(REJECTION_REASON_COLUMN).isNull())
    invalid = frame.filter(F.col(REJECTION_REASON_COLUMN).isNotNull())
    return valid, invalid
