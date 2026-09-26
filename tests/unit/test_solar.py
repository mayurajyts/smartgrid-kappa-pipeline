"""Tests for the diurnal solar curve.

WHY THESE PROPERTIES AND NOT OTHERS
-----------------------------------
The solar model is an approximation, so testing it against a physical reference
would be testing the wrong thing. What matters is the small set of properties the
rest of the pipeline actually depends on:

  * Zero at night — otherwise Job D's "during simulated daylight hours"
    qualifier on LOW_RENEWABLE is meaningless.
  * Never above panel capacity — this is the invariant Job A's validation rejects
    violations of. If the clean path could breach it by accident, the DLQ would
    fill with simulator bugs that look identical to injected faults, and the
    fault-injection evidence would be worthless.
  * Responds to cloud cover — this is the lever the §11 step 4 demo pulls to make
    LOW_RENEWABLE fire.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from simulators.solar import (
    PEAK_CAPACITY_FACTOR,
    SUNRISE_HOUR,
    SUNSET_HOUR,
    is_daylight,
    max_possible_generation_kwh,
    solar_elevation_factor,
    solar_generation_kwh,
)

# One simulated hour per reading interval keeps the kWh arithmetic easy to read:
# at a 1-hour interval, kWh equals kW.
ONE_SIM_HOUR = 3600.0
CAPACITY_KW = 4.0


def at(hour: int, minute: int = 0) -> datetime:
    """A simulated instant on the fixed simulated start date."""
    return datetime(2026, 1, 1, hour, minute, tzinfo=timezone.utc)


class TestElevationFactor:
    def test_zero_before_sunrise(self):
        for hour in (0, 3, 5):
            assert solar_elevation_factor(at(hour)) == 0.0

    def test_zero_after_sunset(self):
        for hour in (18, 20, 23):
            assert solar_elevation_factor(at(hour)) == 0.0

    def test_exactly_zero_at_the_boundaries(self):
        """Sunrise and sunset are the edges of the half-open daylight window."""
        assert solar_elevation_factor(at(int(SUNRISE_HOUR))) == 0.0
        assert solar_elevation_factor(at(int(SUNSET_HOUR))) == 0.0

    def test_peaks_at_solar_noon(self):
        """Midpoint of the daylight window reaches full elevation."""
        noon_hour = int((SUNRISE_HOUR + SUNSET_HOUR) / 2)
        assert solar_elevation_factor(at(noon_hour)) == pytest.approx(1.0)

    def test_monotonic_increase_to_noon(self):
        """No dips on the way up — a non-monotonic morning would look like a
        cloud transient the model does not claim to simulate."""
        values = [solar_elevation_factor(at(h)) for h in range(6, 13)]
        assert values == sorted(values)

    def test_symmetric_about_noon(self):
        """09:00 and 15:00 are equidistant from noon and must match."""
        assert solar_elevation_factor(at(9)) == pytest.approx(
            solar_elevation_factor(at(15))
        )

    def test_never_exceeds_one(self):
        for hour in range(24):
            for minute in (0, 15, 30, 45):
                assert 0.0 <= solar_elevation_factor(at(hour, minute)) <= 1.0


class TestIsDaylight:
    def test_night_is_not_daylight(self):
        for hour in (0, 3, 5, 19, 22):
            assert not is_daylight(at(hour))

    def test_midday_is_daylight(self):
        for hour in (7, 10, 12, 15, 17):
            assert is_daylight(at(hour))

    def test_agrees_with_the_generation_curve(self):
        """Job D's alert qualifier and the generator must use one definition of
        daylight, or the alert fires on hours the simulator considers dark."""
        for hour in range(24):
            instant = at(hour)
            generating = solar_generation_kwh(instant, CAPACITY_KW, ONE_SIM_HOUR) > 0
            assert generating == is_daylight(instant)


class TestGeneration:
    def test_no_panels_generates_nothing(self):
        assert solar_generation_kwh(at(12), 0.0, ONE_SIM_HOUR) == 0.0

    def test_night_generates_nothing_even_with_panels(self):
        assert solar_generation_kwh(at(2), CAPACITY_KW, ONE_SIM_HOUR) == 0.0

    def test_midday_generates_the_derated_peak(self):
        """At solar noon with a 1-hour interval, output is capacity x derating."""
        generated = solar_generation_kwh(at(12), CAPACITY_KW, ONE_SIM_HOUR)
        assert generated == pytest.approx(CAPACITY_KW * PEAK_CAPACITY_FACTOR, rel=1e-3)

    def test_never_negative(self):
        for hour in range(24):
            assert solar_generation_kwh(at(hour), CAPACITY_KW, ONE_SIM_HOUR) >= 0.0

    def test_never_exceeds_the_physical_cap(self):
        """THE critical invariant: Job A rejects readings above the panel cap, so
        the clean path must never produce one. A violation here would be
        indistinguishable from the injected solar-spike fault."""
        cap = max_possible_generation_kwh(CAPACITY_KW, ONE_SIM_HOUR)
        for hour in range(24):
            for minute in (0, 20, 40):
                generated = solar_generation_kwh(
                    at(hour, minute), CAPACITY_KW, ONE_SIM_HOUR
                )
                assert generated <= cap

    def test_cap_holds_across_capacities_and_intervals(self):
        for capacity in (0.5, 2.0, 5.0, 12.0):
            for interval in (60.0, 900.0, ONE_SIM_HOUR):
                cap = max_possible_generation_kwh(capacity, interval)
                assert solar_generation_kwh(at(12), capacity, interval) <= cap

    def test_energy_scales_with_interval_length(self):
        """Energy is power x time: a half-hour interval yields half the energy."""
        full = solar_generation_kwh(at(12), CAPACITY_KW, ONE_SIM_HOUR)
        half = solar_generation_kwh(at(12), CAPACITY_KW, ONE_SIM_HOUR / 2)
        assert half == pytest.approx(full / 2, rel=1e-3)

    def test_scales_linearly_with_capacity(self):
        small = solar_generation_kwh(at(12), 2.0, ONE_SIM_HOUR)
        large = solar_generation_kwh(at(12), 4.0, ONE_SIM_HOUR)
        assert large == pytest.approx(2 * small, rel=1e-3)


class TestWeatherSensitivity:
    def test_clear_sky_is_the_maximum(self):
        clear = solar_generation_kwh(at(12), CAPACITY_KW, ONE_SIM_HOUR, solar_index=1.0)
        cloudy = solar_generation_kwh(at(12), CAPACITY_KW, ONE_SIM_HOUR, solar_index=0.3)
        assert cloudy < clear

    def test_total_overcast_generates_nothing(self):
        """The lever the §11 step 4 demo pulls to force LOW_RENEWABLE."""
        assert solar_generation_kwh(
            at(12), CAPACITY_KW, ONE_SIM_HOUR, solar_index=0.0
        ) == 0.0

    def test_index_scales_output_proportionally(self):
        clear = solar_generation_kwh(at(12), CAPACITY_KW, ONE_SIM_HOUR, solar_index=1.0)
        half = solar_generation_kwh(at(12), CAPACITY_KW, ONE_SIM_HOUR, solar_index=0.5)
        assert half == pytest.approx(clear / 2, rel=1e-3)

    def test_out_of_range_index_is_clamped(self):
        """solar_index arrives from the weather topic, i.e. from outside this
        module. An index above 1 must not push generation past the physical cap
        and turn a data-quality problem into a validation failure."""
        cap = max_possible_generation_kwh(CAPACITY_KW, ONE_SIM_HOUR)
        assert solar_generation_kwh(
            at(12), CAPACITY_KW, ONE_SIM_HOUR, solar_index=5.0
        ) <= cap
        assert solar_generation_kwh(
            at(12), CAPACITY_KW, ONE_SIM_HOUR, solar_index=-1.0
        ) == 0.0


class TestDeterminism:
    def test_same_inputs_give_same_output(self):
        """Solar has no noise term, so it must be exactly reproducible — part of
        what makes a replayed simulated day comparable to the original."""
        first = solar_generation_kwh(at(12, 30), CAPACITY_KW, ONE_SIM_HOUR, 0.7)
        second = solar_generation_kwh(at(12, 30), CAPACITY_KW, ONE_SIM_HOUR, 0.7)
        assert first == second
