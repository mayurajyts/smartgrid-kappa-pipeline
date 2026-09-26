"""Tests for the household consumption profile.

WHY THESE PROPERTIES
--------------------
Two of these tests guard against failures that would sabotage other parts of the
project rather than merely making the data look odd:

  * STRICTLY POSITIVE consumption. A zero or negative reading on the clean path
    would be indistinguishable from the deliberately injected `negative_kwh`
    fault, making the DLQ evidence unfalsifiable; a zero would also mimic meter
    dropout and could trip the staleness alert with no meter actually silent.
  * DETERMINISM per (meter, simulated instant). The §11 step 8 replay demo shows
    a bill recomputing to the same value. If consumption were drawn from a global
    RNG, a re-run would produce different readings for the same simulated time and
    a changed bill would be indistinguishable from a real pipeline bug.

The peak-shape tests exist because the duck-curve coincidence — peak demand in the
evening, when solar has stopped — is the most defensible thing on the business
dashboard, and it only emerges if the peaks are actually where they are claimed.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from simulators.load_profile import (
    _NOISE_FRACTION,
    consumption_kwh,
    demand_multiplier,
    time_of_day_bucket,
)

ONE_SIM_HOUR = 3600.0
BASE_LOAD_KW = 0.8
METER = "MTR-0042"


def at(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 1, 1, hour, minute, tzinfo=timezone.utc)


class TestDemandMultiplier:
    def test_never_zero(self):
        """Households draw standby power overnight. A zero would empty the
        1-minute zone windows at night, which looks identical to an outage."""
        for hour in range(24):
            assert demand_multiplier(at(hour)) > 0.0

    def test_morning_peak_is_above_overnight(self):
        assert demand_multiplier(at(7, 30)) > demand_multiplier(at(3))

    def test_evening_peak_is_above_overnight(self):
        assert demand_multiplier(at(19, 30)) > demand_multiplier(at(3))

    def test_evening_peak_dominates_the_morning_peak(self):
        """Real residential demand peaks in the evening. This ordering is what
        makes the renewable-contribution dip meaningful: demand is highest exactly
        when solar has stopped."""
        assert demand_multiplier(at(19, 30)) > demand_multiplier(at(7, 30))

    def test_midday_dip_between_the_peaks(self):
        """The house is empty in the early afternoon."""
        midday = demand_multiplier(at(13))
        assert midday < demand_multiplier(at(7, 30))
        assert midday < demand_multiplier(at(19, 30))

    def test_deepest_trough_is_in_the_small_hours(self):
        """The daily minimum sits around midnight-to-03:00, between the decaying
        tail of the previous evening peak and the rise of the morning peak.

        Note the trough is NOT flat across the whole overnight bucket: at 03:00
        the evening peak's Gaussian tail is still measurably above the 00:00
        floor. That is a property of summing two bells rather than switching
        between discrete levels, and it is why this asserts a minimum region
        rather than a single overnight constant."""
        trough = min(demand_multiplier(at(h)) for h in (0, 1, 2, 3))
        for hour in range(5, 24):
            assert demand_multiplier(at(hour)) >= trough


class TestTimeOfDayBucket:
    @pytest.mark.parametrize(
        "hour,expected",
        [
            (0, "OVERNIGHT"), (3, "OVERNIGHT"), (5, "OVERNIGHT"),
            (6, "MORNING"), (9, "MORNING"), (11, "MORNING"),
            (12, "AFTERNOON"), (15, "AFTERNOON"), (16, "AFTERNOON"),
            (17, "EVENING"), (19, "EVENING"), (21, "EVENING"),
            (22, "OVERNIGHT"), (23, "OVERNIGHT"),
        ],
    )
    def test_buckets(self, hour, expected):
        assert time_of_day_bucket(at(hour)) == expected

    def test_every_hour_maps_to_a_known_bucket(self):
        """Job A enriches each reading with this label; an unmapped hour would
        produce a null dimension value in the Parquet archive."""
        known = {"OVERNIGHT", "MORNING", "AFTERNOON", "EVENING"}
        for hour in range(24):
            assert time_of_day_bucket(at(hour)) in known


class TestConsumption:
    def test_strictly_positive_across_the_whole_day(self):
        """THE critical invariant — see the module docstring."""
        for hour in range(24):
            for minute in (0, 15, 30, 45):
                value = consumption_kwh(
                    METER, at(hour, minute), BASE_LOAD_KW, ONE_SIM_HOUR
                )
                assert value > 0.0

    def test_positive_for_a_wide_range_of_base_loads(self):
        for base in (0.05, 0.25, 1.0, 2.66, 10.0):
            for hour in (0, 7, 13, 19):
                assert consumption_kwh(METER, at(hour), base, ONE_SIM_HOUR) > 0.0

    def test_positive_for_very_short_intervals(self):
        """A short interval must not round down to zero and mimic a silent
        meter — the guard in consumption_kwh exists for this case."""
        for interval in (1.0, 5.0, 60.0):
            assert consumption_kwh(METER, at(12), BASE_LOAD_KW, interval) > 0.0

    def test_scales_with_base_load(self):
        """Household ranking must be stable, or the billing report's top-10
        consumers list would be arbitrary."""
        small = consumption_kwh(METER, at(12), 0.5, ONE_SIM_HOUR)
        large = consumption_kwh(METER, at(12), 5.0, ONE_SIM_HOUR)
        assert large > small

    def test_scales_with_interval_length(self):
        full = consumption_kwh(METER, at(12), BASE_LOAD_KW, ONE_SIM_HOUR)
        half = consumption_kwh(METER, at(12), BASE_LOAD_KW, ONE_SIM_HOUR / 2)
        assert half == pytest.approx(full / 2, rel=1e-6)

    def test_noise_stays_within_its_declared_bound(self):
        """Noise must be bounded well below 1.0 so it can never drive a reading
        to zero or negative.

        The expected value is recomputed per instant: the demand multiplier
        varies continuously through the day, so comparing a 12:45 reading against
        the 12:00 shape would be measuring the curve's slope, not the noise."""
        for hour in (0, 7, 12, 19, 23):
            for minute in range(0, 60, 5):
                instant = at(hour, minute)
                expected = (
                    BASE_LOAD_KW * demand_multiplier(instant) * ONE_SIM_HOUR / 3600.0
                )
                actual = consumption_kwh(METER, instant, BASE_LOAD_KW, ONE_SIM_HOUR)
                assert actual == pytest.approx(expected, rel=_NOISE_FRACTION + 1e-6)

    def test_noise_actually_varies_between_readings(self):
        """Without variation the stream would be a repeating pattern rather than
        telemetry, and deduplication would have nothing to distinguish."""
        values = {
            consumption_kwh(METER, at(12, m), BASE_LOAD_KW, ONE_SIM_HOUR)
            for m in range(0, 60, 5)
        }
        assert len(values) > 1


class TestDeterminism:
    def test_same_meter_and_instant_give_the_same_value(self):
        """The property the replay demo rests on."""
        first = consumption_kwh(METER, at(14, 22), BASE_LOAD_KW, ONE_SIM_HOUR)
        second = consumption_kwh(METER, at(14, 22), BASE_LOAD_KW, ONE_SIM_HOUR)
        assert first == second

    def test_different_meters_differ_at_the_same_instant(self):
        """Otherwise every household in a zone would report identically and the
        consumer ranking would be meaningless."""
        a = consumption_kwh("MTR-0001", at(12), BASE_LOAD_KW, ONE_SIM_HOUR)
        b = consumption_kwh("MTR-0002", at(12), BASE_LOAD_KW, ONE_SIM_HOUR)
        assert a != b

    def test_not_affected_by_global_random_state(self):
        """Explicitly guards against a regression to a shared RNG: reseeding the
        global generator must not change the output."""
        import random

        random.seed(1)
        first = consumption_kwh(METER, at(9, 9), BASE_LOAD_KW, ONE_SIM_HOUR)
        random.seed(999)
        second = consumption_kwh(METER, at(9, 9), BASE_LOAD_KW, ONE_SIM_HOUR)
        assert first == second
