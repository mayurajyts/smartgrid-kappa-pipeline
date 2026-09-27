"""Tests for Job A's enrichment step.

WHY THIS SUITE MATTERS
----------------------
Enrichment produces `net_grid_kwh`, which is the number the bill is computed from.
Everything here protects that value or the dimension join that feeds it:

  * THE LEFT JOIN. An inner join would silently drop readings whose household is
    absent from the dimension — no error, no DLQ record, no count discrepancy
    anywhere. `TestDimensionJoin` asserts the reading survives with null dimension
    columns so validation can reject it visibly instead.

  * THE `is_exporting` BOUNDARY. It gates the export credit in Phase 3's billing
    maths. A household exactly in balance must not be treated as exporting, or it
    would be credited for zero exported energy.

  * `time_of_day_bucket` AGREEING WITH THE PRODUCER. The boundaries come from
    `simulators.load_profile` rather than being re-declared, so the label a reading
    carries corresponds to the load shape that produced it. If they drifted, the
    daily report's peak-hour analysis would be describing the wrong window.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

pytest.importorskip("pyspark")

from pyspark.sql.types import (  # noqa: E402
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from processing.transforms.enrichment import (  # noqa: E402
    build_dimension,
    enrich,
    join_dimension,
)
from simulators.load_profile import time_of_day_bucket  # noqa: E402
from simulators.reference_data import load_households  # noqa: E402

READING_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("household_id", StringType(), True),
        StructField("grid_zone", StringType(), True),
        StructField("power_consumption_kwh", DoubleType(), True),
        StructField("solar_generation_kwh", DoubleType(), True),
        StructField("event_timestamp", TimestampType(), True),
    ]
)


def at(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 1, 14, hour, minute, tzinfo=timezone.utc)


def reading(**overrides):
    base = {
        "event_id": "evt-1",
        "household_id": "HH-0001",
        "grid_zone": "ZONE-A",
        "power_consumption_kwh": 0.5,
        "solar_generation_kwh": 0.2,
        "event_timestamp": at(12),
    }
    base.update(overrides)
    return tuple(base[f.name] for f in READING_SCHEMA.fields)


def enriched_rows(spark, *rows):
    frame = spark.createDataFrame(list(rows), READING_SCHEMA)
    return [r.asDict() for r in enrich(frame).collect()]


class TestDimension:
    def test_dimension_covers_every_household(self, spark):
        dimension = build_dimension(spark)
        assert dimension.count() == len(load_households())

    def test_dimension_carries_the_validation_bound(self, spark):
        """`solar_capacity_kw` is what validation bounds solar generation against.
        It must come from the same CSVs the producer generated from, or valid
        readings would be rejected as physically impossible."""
        dimension = build_dimension(spark)
        assert "solar_capacity_kw" in dimension.columns
        assert dimension.filter("solar_capacity_kw IS NULL").count() == 0

    def test_dimension_carries_zone_capacity_for_the_overload_alert(self, spark):
        """Job D's ZONE_OVERLOAD compares zone load against this."""
        dimension = build_dimension(spark)
        assert "zone_capacity_kw" in dimension.columns
        assert dimension.filter("zone_capacity_kw <= 0").count() == 0

    def test_solar_flag_and_capacity_agree(self, spark):
        """A household flagged as having panels but with zero capacity would have
        every solar reading rejected as above-capacity."""
        dimension = build_dimension(spark)
        assert dimension.filter("has_solar AND solar_capacity_kw <= 0").count() == 0
        assert dimension.filter("NOT has_solar AND solar_capacity_kw > 0").count() == 0


class TestDimensionJoin:
    def test_a_known_household_gains_its_attributes(self, spark):
        known = load_households()[0]
        frame = spark.createDataFrame(
            [reading(household_id=known.household_id)], READING_SCHEMA
        )
        result = join_dimension(frame, build_dimension(spark)).collect()[0]
        assert result["solar_capacity_kw"] == pytest.approx(known.solar_capacity_kw)
        assert result["has_solar"] == known.has_solar
        assert result["zone_capacity_kw"] > 0

    def test_an_unknown_household_survives_with_null_attributes(self, spark):
        """THE critical property. An inner join would drop this record silently;
        the left join keeps it so validation rejects it into the DLQ with a reason.
        Losing data quietly is the worse failure."""
        frame = spark.createDataFrame(
            [reading(household_id="HH-DOES-NOT-EXIST")], READING_SCHEMA
        )
        joined = join_dimension(frame, build_dimension(spark))
        assert joined.count() == 1
        assert joined.collect()[0]["solar_capacity_kw"] is None

    def test_the_join_does_not_multiply_rows(self, spark):
        """The dimension has one row per household, so the join must be 1:1. A
        duplicated dimension row would double-count that household's consumption in
        every zone aggregate and every bill."""
        frame = spark.createDataFrame(
            [reading(event_id=f"evt-{i}") for i in range(10)], READING_SCHEMA
        )
        assert join_dimension(frame, build_dimension(spark)).count() == 10


class TestNetGridKwh:
    def test_consumption_above_solar_gives_a_positive_draw(self, spark):
        result = enriched_rows(
            spark, reading(power_consumption_kwh=0.5, solar_generation_kwh=0.2)
        )[0]
        assert result["net_grid_kwh"] == pytest.approx(0.3)

    def test_solar_above_consumption_gives_a_negative_net(self, spark):
        """Deliberately signed and not clamped: the sign is what `is_exporting`
        tests and what the export-credit branch of the billing maths keys off."""
        result = enriched_rows(
            spark, reading(power_consumption_kwh=0.2, solar_generation_kwh=0.9)
        )[0]
        assert result["net_grid_kwh"] == pytest.approx(-0.7)

    def test_no_solar_means_net_equals_consumption(self, spark):
        result = enriched_rows(
            spark, reading(power_consumption_kwh=0.42, solar_generation_kwh=0.0)
        )[0]
        assert result["net_grid_kwh"] == pytest.approx(0.42)


class TestIsExporting:
    def test_exporting_when_net_is_negative(self, spark):
        result = enriched_rows(
            spark, reading(power_consumption_kwh=0.1, solar_generation_kwh=0.5)
        )[0]
        assert result["is_exporting"] is True

    def test_not_exporting_when_net_is_positive(self, spark):
        result = enriched_rows(
            spark, reading(power_consumption_kwh=0.5, solar_generation_kwh=0.1)
        )[0]
        assert result["is_exporting"] is False

    def test_exactly_in_balance_is_not_exporting(self, spark):
        """THE boundary case. `is_exporting` gates the export credit, so a household
        that exported nothing must not be credited."""
        result = enriched_rows(
            spark, reading(power_consumption_kwh=0.3, solar_generation_kwh=0.3)
        )[0]
        assert result["is_exporting"] is False
        assert result["net_grid_kwh"] == pytest.approx(0.0)


class TestSelfConsumption:
    def test_capped_at_consumption_when_solar_exceeds_it(self, spark):
        """Self-consumption is solar actually USED on site. A household cannot
        consume more of its own generation than it consumed in total — without the
        cap, R6's self-consumption ratio could exceed 100%."""
        result = enriched_rows(
            spark, reading(power_consumption_kwh=0.2, solar_generation_kwh=0.9)
        )[0]
        assert result["self_consumption_kwh"] == pytest.approx(0.2)

    def test_equals_solar_when_consumption_exceeds_it(self, spark):
        result = enriched_rows(
            spark, reading(power_consumption_kwh=1.0, solar_generation_kwh=0.3)
        )[0]
        assert result["self_consumption_kwh"] == pytest.approx(0.3)

    def test_zero_without_solar(self, spark):
        result = enriched_rows(spark, reading(solar_generation_kwh=0.0))[0]
        assert result["self_consumption_kwh"] == pytest.approx(0.0)

    def test_never_exceeds_either_input(self, spark):
        """The invariant that keeps the ratio in [0, 1]."""
        rows = [
            reading(event_id=f"e{i}", power_consumption_kwh=c, solar_generation_kwh=s)
            for i, (c, s) in enumerate(
                [(0.1, 0.9), (0.9, 0.1), (0.5, 0.5), (0.0, 0.3), (0.3, 0.0)]
            )
        ]
        for result in enriched_rows(spark, *rows):
            assert result["self_consumption_kwh"] <= result["power_consumption_kwh"]
            assert result["self_consumption_kwh"] <= result["solar_generation_kwh"]


class TestTimeOfDayBucket:
    @pytest.mark.parametrize(
        "hour,expected",
        [
            (0, "OVERNIGHT"), (3, "OVERNIGHT"),
            (7, "MORNING"), (10, "MORNING"),
            (13, "AFTERNOON"), (16, "AFTERNOON"),
            (18, "EVENING"), (21, "EVENING"),
            (23, "OVERNIGHT"),
        ],
    )
    def test_buckets(self, spark, hour, expected):
        result = enriched_rows(spark, reading(event_timestamp=at(hour)))[0]
        assert result["time_of_day_bucket"] == expected

    def test_agrees_with_the_producer_for_every_hour(self, spark):
        """The boundaries are imported from simulators.load_profile rather than
        re-declared. This asserts they genuinely match, so the label a reading
        carries corresponds to the load shape that produced it."""
        rows = [reading(event_id=f"e{h}", event_timestamp=at(h)) for h in range(24)]
        results = enriched_rows(spark, *rows)
        by_hour = {r["event_timestamp"].hour: r["time_of_day_bucket"] for r in results}
        for hour in range(24):
            assert by_hour[hour] == time_of_day_bucket(at(hour))

    def test_no_hour_falls_through_to_unknown(self, spark):
        """An unmapped hour would put a null-ish dimension value in the Parquet
        archive and break the report's grouping."""
        rows = [reading(event_id=f"e{h}", event_timestamp=at(h)) for h in range(24)]
        for result in enriched_rows(spark, *rows):
            assert result["time_of_day_bucket"] != "UNKNOWN"


class TestEnrichmentPreservesInput:
    def test_no_rows_are_added_or_lost(self, spark):
        """Enrichment adds columns, never rows. A change in count would mean the
        derivations introduced a join or an explode."""
        rows = [reading(event_id=f"e{i}") for i in range(7)]
        frame = spark.createDataFrame(rows, READING_SCHEMA)
        assert enrich(frame).count() == 7

    def test_original_measurements_are_untouched(self, spark):
        """Downstream billing sums the original kWh values; enrichment must not
        modify them."""
        result = enriched_rows(
            spark, reading(power_consumption_kwh=0.1234, solar_generation_kwh=0.0567)
        )[0]
        assert result["power_consumption_kwh"] == pytest.approx(0.1234)
        assert result["solar_generation_kwh"] == pytest.approx(0.0567)
