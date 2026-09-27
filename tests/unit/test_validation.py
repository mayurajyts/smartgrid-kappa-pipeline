"""Tests for Job A's validation rules.

WHY THIS SUITE MATTERS
----------------------
Two properties are being protected, and both are easy to break silently.

FIRST: the rejection REASON must be right, not just the rejection. §11 step 7 — the
demo the 10-mark observability criterion is worded around — takes one DLQ record and
traces its correlation_id back to the moment the fault was injected. If validation
labelled a negative-kWh record as `null_required_field`, the DLQ would still have the
right number of records and a count check would pass, while the trace demo would be
asserting a coincidence rather than a causal chain.

`TestReasonContractWithProducer` therefore asserts the reason strings against the
producer's own constants, imported from `simulators.fault_injection`. A rename on
either side becomes a test failure instead of a subtly wrong story in a viva.

SECOND: exactly ONE reason per record. The DLQ schema has a single
`rejection_reason` field and the rejected-events metric carries one `reason` label,
so a record counted under two reasons would inflate the reject rate above the true
number of bad readings — and could fail the 2% Airflow data-quality gate on
arithmetic rather than on data.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from processing.transforms.validation import (  # noqa: E402
    REJECTION_REASON_COLUMN,
    REQUIRED_FIELDS,
    split_valid_invalid,
    with_rejection_reason,
)
from simulators.fault_injection import (  # noqa: E402
    REASON_NEGATIVE_KWH,
    REASON_NULL_FIELD,
    REASON_SOLAR_ABOVE_CAPACITY,
    REASON_TIMESTAMP_IN_FUTURE,
)

# One simulated hour per reading, so kWh equals kW and the capacity arithmetic is
# readable in the assertions below.
INTERVAL_SECONDS = 3600.0
SIM_NOW = datetime(2026, 1, 14, 12, 0, 0, tzinfo=timezone.utc)
FUTURE_TOLERANCE_MINUTES = 5

SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("meter_id", StringType(), True),
        StructField("household_id", StringType(), True),
        StructField("grid_zone", StringType(), True),
        StructField("power_consumption_kwh", DoubleType(), True),
        StructField("solar_generation_kwh", DoubleType(), True),
        StructField("event_timestamp", TimestampType(), True),
        StructField("solar_capacity_kw", DoubleType(), True),
        StructField("has_solar", BooleanType(), True),
        StructField("sim_now", TimestampType(), True),
    ]
)


def row(**overrides):
    """A valid reading, with named fields overridden."""
    base = {
        "event_id": "evt-1",
        "meter_id": "MTR-0001",
        "household_id": "HH-0001",
        "grid_zone": "ZONE-A",
        "power_consumption_kwh": 0.5,
        "solar_generation_kwh": 0.2,
        "event_timestamp": SIM_NOW - timedelta(minutes=1),
        "solar_capacity_kw": 4.0,
        "has_solar": True,
        "sim_now": SIM_NOW,
    }
    base.update(overrides)
    return tuple(base[f.name] for f in SCHEMA.fields)


def validate(spark, *rows):
    """Run the validation rules over the given rows and return them as dicts."""
    frame = spark.createDataFrame(list(rows), SCHEMA)
    validated = with_rejection_reason(
        frame,
        interval_seconds_column=F.lit(INTERVAL_SECONDS),
        future_tolerance_minutes=FUTURE_TOLERANCE_MINUTES,
    )
    return [r.asDict() for r in validated.collect()]


class TestValidRecordsPass:
    def test_a_clean_reading_is_not_rejected(self, spark):
        """If this fails, every rejection test below is meaningless."""
        result = validate(spark, row())[0]
        assert result[REJECTION_REASON_COLUMN] is None

    def test_zero_consumption_is_valid(self, spark):
        """Zero is not negative. A meter reporting no consumption for an interval is
        unusual but physically possible, and rejecting it would discard real data."""
        result = validate(spark, row(power_consumption_kwh=0.0))[0]
        assert result[REJECTION_REASON_COLUMN] is None

    def test_zero_solar_at_night_is_valid(self, spark):
        result = validate(spark, row(solar_generation_kwh=0.0))[0]
        assert result[REJECTION_REASON_COLUMN] is None

    def test_solar_exactly_at_capacity_is_valid(self, spark):
        """The bound is inclusive: a reading at exactly the physical maximum is
        remarkable, not impossible. Rejecting it would treat a legitimate peak as
        corrupt data."""
        result = validate(spark, row(solar_generation_kwh=4.0))[0]
        assert result[REJECTION_REASON_COLUMN] is None

    def test_a_household_exporting_is_valid(self, spark):
        """Solar above consumption means export, which is a normal condition and the
        basis of R6's self-consumption ratio — not a validation failure."""
        result = validate(
            spark, row(power_consumption_kwh=0.1, solar_generation_kwh=3.0)
        )[0]
        assert result[REJECTION_REASON_COLUMN] is None

    def test_a_slightly_future_timestamp_is_tolerated(self, spark):
        """Micro-batch timing means a reading can legitimately be stamped a little
        ahead of when the driver evaluates it."""
        result = validate(
            spark, row(event_timestamp=SIM_NOW + timedelta(minutes=2))
        )[0]
        assert result[REJECTION_REASON_COLUMN] is None


class TestNullRequiredFields:
    @pytest.mark.parametrize("field", REQUIRED_FIELDS)
    def test_each_required_field_null_is_rejected(self, spark, field):
        """Every field in REQUIRED_FIELDS must actually be enforced. Without this
        parametrised check, adding a field to the tuple and forgetting to include it
        in the predicate would go unnoticed."""
        result = validate(spark, row(**{field: None}))[0]
        assert result[REJECTION_REASON_COLUMN] == REASON_NULL_FIELD

    def test_null_is_checked_before_the_numeric_rules(self, spark):
        """A null kWh must be reported as a null, not slip through the negative
        check. Under SQL three-valued logic `NULL < 0` is NULL rather than true, so
        without the ordering a null measurement would pass validation entirely and
        reach the billing aggregation as a missing value."""
        result = validate(spark, row(power_consumption_kwh=None))[0]
        assert result[REJECTION_REASON_COLUMN] == REASON_NULL_FIELD

    def test_a_missing_dimension_match_is_rejected(self, spark):
        """The left dimension join leaves nulls when a household is unknown.
        Enrichment uses a left join specifically so this becomes a visible DLQ
        record rather than a silently dropped reading."""
        result = validate(spark, row(household_id=None, solar_capacity_kw=None))[0]
        assert result[REJECTION_REASON_COLUMN] == REASON_NULL_FIELD


class TestNegativeKwh:
    def test_negative_consumption_is_rejected(self, spark):
        result = validate(spark, row(power_consumption_kwh=-0.5))[0]
        assert result[REJECTION_REASON_COLUMN] == REASON_NEGATIVE_KWH

    def test_negative_solar_is_rejected(self, spark):
        result = validate(spark, row(solar_generation_kwh=-0.2))[0]
        assert result[REJECTION_REASON_COLUMN] == REASON_NEGATIVE_KWH

    def test_a_tiny_negative_is_still_rejected(self, spark):
        """No tolerance on the sign: per-interval energy is never negative, so even
        a rounding-scale negative indicates a broken meter or a corrupted record."""
        result = validate(spark, row(power_consumption_kwh=-0.000001))[0]
        assert result[REJECTION_REASON_COLUMN] == REASON_NEGATIVE_KWH


class TestSolarAboveCapacity:
    def test_generation_above_the_panel_cap_is_rejected(self, spark):
        """4 kW panels over one simulated hour cannot exceed 4 kWh."""
        result = validate(spark, row(solar_generation_kwh=9.0))[0]
        assert result[REJECTION_REASON_COLUMN] == REASON_SOLAR_ABOVE_CAPACITY

    def test_any_generation_from_a_household_without_panels_is_rejected(self, spark):
        """Capacity 0 makes any positive generation impossible. This is the case the
        injected solar-spike fault hits on a non-solar household, and it is why the
        fault multiplies capacity rather than the current reading."""
        result = validate(
            spark, row(has_solar=False, solar_capacity_kw=0.0, solar_generation_kwh=5.0)
        )[0]
        assert result[REJECTION_REASON_COLUMN] == REASON_SOLAR_ABOVE_CAPACITY

    def test_the_bound_scales_with_the_interval(self, spark):
        """Energy is power x time. Over a half-hour interval, 4 kW panels cannot
        exceed 2 kWh, so a 3 kWh reading is impossible even though it would be fine
        over a full hour."""
        frame = spark.createDataFrame([row(solar_generation_kwh=3.0)], SCHEMA)
        validated = with_rejection_reason(
            frame,
            interval_seconds_column=F.lit(1800.0),  # half a simulated hour
            future_tolerance_minutes=FUTURE_TOLERANCE_MINUTES,
        )
        assert (
            validated.collect()[0][REJECTION_REASON_COLUMN]
            == REASON_SOLAR_ABOVE_CAPACITY
        )

    def test_float_rounding_does_not_cause_a_false_rejection(self, spark):
        """The producer rounds kWh to 6 places, so a reading can land a hair above
        the computed bound through representation error alone. A false rejection here
        would put valid data in the DLQ and inflate the reject rate."""
        result = validate(spark, row(solar_generation_kwh=4.0000001))[0]
        assert result[REJECTION_REASON_COLUMN] is None


class TestFutureTimestamp:
    def test_a_far_future_timestamp_is_rejected(self, spark):
        result = validate(
            spark, row(event_timestamp=SIM_NOW + timedelta(minutes=30))
        )[0]
        assert result[REJECTION_REASON_COLUMN] == REASON_TIMESTAMP_IN_FUTURE

    def test_the_boundary_is_respected(self, spark):
        """Just inside the tolerance passes, just outside is rejected. This rule
        protects the watermark: Spark tracks the MAXIMUM event time seen, so one
        record stamped far ahead would advance the watermark and cause every
        subsequent genuine reading to be discarded as late — a single bad clock
        could silence an entire zone."""
        inside = validate(
            spark,
            row(event_timestamp=SIM_NOW + timedelta(minutes=FUTURE_TOLERANCE_MINUTES - 1)),
        )[0]
        outside = validate(
            spark,
            row(event_timestamp=SIM_NOW + timedelta(minutes=FUTURE_TOLERANCE_MINUTES + 1)),
        )[0]
        assert inside[REJECTION_REASON_COLUMN] is None
        assert outside[REJECTION_REASON_COLUMN] == REASON_TIMESTAMP_IN_FUTURE

    def test_a_past_timestamp_is_never_rejected_by_this_rule(self, spark):
        """Late events are handled by the watermark, not by validation. A late
        reading is well-formed and must not go to the DLQ — the producer back-dates
        events deliberately to exercise the watermark, and treating those as
        rejections would conflate two different mechanisms."""
        result = validate(spark, row(event_timestamp=SIM_NOW - timedelta(hours=6)))[0]
        assert result[REJECTION_REASON_COLUMN] is None


class TestOneReasonPerRecord:
    def test_multiple_violations_collapse_to_a_single_reason(self, spark):
        """See the module docstring: two reasons on one record would inflate the
        reject rate above the true number of bad readings."""
        result = validate(
            spark,
            row(
                household_id=None,
                power_consumption_kwh=-1.0,
                solar_generation_kwh=99.0,
                event_timestamp=SIM_NOW + timedelta(hours=1),
            ),
        )[0]
        assert result[REJECTION_REASON_COLUMN] == REASON_NULL_FIELD

    def test_precedence_is_null_then_negative_then_capacity(self, spark):
        negative_and_spiked = validate(
            spark, row(power_consumption_kwh=-1.0, solar_generation_kwh=99.0)
        )[0]
        assert negative_and_spiked[REJECTION_REASON_COLUMN] == REASON_NEGATIVE_KWH

        spiked_and_future = validate(
            spark,
            row(solar_generation_kwh=99.0, event_timestamp=SIM_NOW + timedelta(hours=1)),
        )[0]
        assert (
            spiked_and_future[REJECTION_REASON_COLUMN] == REASON_SOLAR_ABOVE_CAPACITY
        )


class TestSplit:
    def test_valid_and_invalid_are_separated_without_loss(self, spark):
        """Every input must appear in exactly one output. A record in neither would
        be silently dropped — the failure the DLQ exists to prevent."""
        frame = spark.createDataFrame(
            [
                row(event_id="ok-1"),
                row(event_id="bad-1", power_consumption_kwh=-1.0),
                row(event_id="ok-2"),
                row(event_id="bad-2", household_id=None),
            ],
            SCHEMA,
        )
        validated = with_rejection_reason(
            frame,
            interval_seconds_column=F.lit(INTERVAL_SECONDS),
            future_tolerance_minutes=FUTURE_TOLERANCE_MINUTES,
        )
        valid, invalid = split_valid_invalid(validated)

        assert valid.count() == 2
        assert invalid.count() == 2
        assert valid.count() + invalid.count() == frame.count()

    def test_every_invalid_record_carries_a_reason(self, spark):
        """A DLQ record without a reason is untraceable, which defeats the point."""
        frame = spark.createDataFrame(
            [row(event_id="bad", solar_generation_kwh=99.0)], SCHEMA
        )
        validated = with_rejection_reason(
            frame,
            interval_seconds_column=F.lit(INTERVAL_SECONDS),
            future_tolerance_minutes=FUTURE_TOLERANCE_MINUTES,
        )
        _, invalid = split_valid_invalid(validated)
        for record in invalid.collect():
            assert record[REJECTION_REASON_COLUMN]


class TestReasonContractWithProducer:
    """The reason strings are a contract with `simulators/fault_injection.py`.

    Imported from there rather than duplicated, so an injected fault and its DLQ
    record are provably the same event. See the module docstring.
    """

    def test_every_producer_rejection_fault_has_a_matching_validation_rule(self, spark):
        """Each fault the producer injects with intent-to-be-rejected must actually
        be rejected, with the same reason string the producer recorded."""
        cases = {
            REASON_NULL_FIELD: row(grid_zone=None),
            REASON_NEGATIVE_KWH: row(power_consumption_kwh=-0.3),
            REASON_SOLAR_ABOVE_CAPACITY: row(solar_generation_kwh=20.0),
            REASON_TIMESTAMP_IN_FUTURE: row(
                event_timestamp=SIM_NOW + timedelta(minutes=30)
            ),
        }
        for expected_reason, bad_row in cases.items():
            actual = validate(spark, bad_row)[0][REJECTION_REASON_COLUMN]
            assert actual == expected_reason, (
                f"producer injects {expected_reason!r} but validation reported "
                f"{actual!r}; the DLQ trace demo depends on these matching"
            )
