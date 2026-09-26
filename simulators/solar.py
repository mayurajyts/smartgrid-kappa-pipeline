"""Diurnal solar generation curve — pure functions, no I/O.

WHY THIS EXISTS IN THE ARCHITECTURE
-----------------------------------
Requirement R2 is "current renewable (solar) contribution % per zone", and R3 is
an alert when that contribution drops below a threshold. Neither is observable
unless solar generation actually *varies* in a way the pipeline can measure. A
constant solar value would make `renewable_pct` a flat line and the
`LOW_RENEWABLE` alert unfireable, so this module is what gives the downstream
aggregates and alerts something real to detect.

It produces three behaviours the rest of the system depends on:

  1. Zero at night. This is what makes the `LOW_RENEWABLE` alert's "during
     simulated daylight hours" qualifier (§7 Job D) necessary rather than
     decorative — without a real night, the alert would fire every night and
     mean nothing.
  2. A midday peak. Gives the Grafana business dashboard a recognisable shape,
     so a marker can see at a glance that the data is physically plausible.
  3. Sensitivity to the weather forecast. This is the demo lever: §11 step 4
     triggers low-solar weather and watches `LOW_RENEWABLE` fire. That only works
     because `expected_solar_index` from the compacted weather topic scales this
     curve.

SEPARATED FROM THE PRODUCER ON PURPOSE
--------------------------------------
These are pure functions of (time, capacity, weather) with no Kafka, no clock and
no config reads, so they are unit-testable directly. The invariant that matters
most — generation never exceeds panel capacity — is asserted in
tests/unit/test_solar.py, because Job A rejects any reading that violates it
(§7 step 2). If this module could emit above-capacity values by accident, the DLQ
would fill with records that look like injected faults but are really simulator
bugs, and the fault-injection demo would be unfalsifiable.

TRADE-OFF (deliberate)
----------------------
This is a geometric approximation — a sine bell between sunrise and sunset — not
a physical irradiance model. A real model would account for solar declination,
atmospheric air mass, panel tilt and azimuth, and temperature derating.

Rejected because none of that is observable through the pipeline. Every
downstream consumer sees only the kWh number; it cannot tell whether that number
came from a sine curve or from a full irradiance calculation. What the pipeline
*can* observe is zero-at-night, peak-at-midday and weather-sensitivity, and those
are exactly what this produces. The added complexity would buy realism that
nothing in the system measures, while adding failure modes to a module whose
correctness the DLQ story depends on.

This limitation belongs in the report's "simulated data lacks real meter
pathologies" limitation (§12 item 9): real arrays show cloud-edge transients,
soiling, shading and inverter clipping, none of which appear here.
"""

from __future__ import annotations

import math
from datetime import datetime

# Daylight window in simulated local hours. Fixed rather than computed from
# latitude and date because these zones sit near 6degN, where day length varies by
# only about 45 minutes across the entire year — a seasonal model would add
# arithmetic whose effect is smaller than the noise already in the load profile.
SUNRISE_HOUR = 6.0
SUNSET_HOUR = 18.0

# Fraction of nameplate capacity reached at solar noon under a clear sky. Real
# arrays rarely exceed ~80% of nameplate because of temperature derating and
# inverter losses, so generating at 100% would be the physically implausible
# choice here.
PEAK_CAPACITY_FACTOR = 0.80


def solar_elevation_factor(sim_time: datetime) -> float:
    """Clear-sky generation as a 0-1 fraction of peak, from the time of day.

    A half-sine over the daylight window: 0 at sunrise and sunset, 1 at solar
    noon. Returns exactly 0.0 outside the window, so night is genuinely dark
    rather than merely dim.
    """
    hour = sim_time.hour + sim_time.minute / 60.0 + sim_time.second / 3600.0

    if hour <= SUNRISE_HOUR or hour >= SUNSET_HOUR:
        return 0.0

    # Map [sunrise, sunset] onto [0, pi] and take the sine: zero at both ends,
    # peaking at the midpoint.
    day_fraction = (hour - SUNRISE_HOUR) / (SUNSET_HOUR - SUNRISE_HOUR)
    return math.sin(math.pi * day_fraction)


def is_daylight(sim_time: datetime) -> bool:
    """Whether a simulated instant falls in daylight.

    Job D's `LOW_RENEWABLE` rule is qualified to daylight hours, and the alert
    evaluator must use the same definition the generator does — otherwise the
    alert would fire during a night the simulator considers dark, or fail to fire
    during a dawn hour it considers lit.
    """
    return SUNRISE_HOUR < (sim_time.hour + sim_time.minute / 60.0) < SUNSET_HOUR


def solar_generation_kwh(
    sim_time: datetime,
    solar_capacity_kw: float,
    interval_seconds: float,
    solar_index: float = 1.0,
) -> float:
    """Solar energy generated in one reading interval, in kWh.

    Args:
        sim_time: simulated instant at the end of the interval.
        solar_capacity_kw: installed panel capacity; 0 for a household without
            panels, which returns 0.0.
        interval_seconds: length of the interval in SIMULATED seconds. Energy is
            power x time, so this must be simulated rather than real seconds —
            using the 2-second real tick would under-report by the 288x
            compression factor and make every bill wrong by that ratio.
        solar_index: 0-1 weather multiplier, from `expected_solar_index` on the
            compacted weather topic. 1.0 is clear sky.

    Returns:
        kWh for the interval, never negative and never above what the panel could
        physically produce in that interval.
    """
    if solar_capacity_kw <= 0:
        return 0.0

    # Clamped rather than trusted: solar_index arrives from the weather topic,
    # i.e. from outside this module. A value above 1 would produce generation
    # above the panel's physical maximum, which Job A would then reject as a
    # validation failure — a data-quality error masquerading as a physics one.
    solar_index = min(max(solar_index, 0.0), 1.0)

    elevation = solar_elevation_factor(sim_time)
    if elevation == 0.0:
        return 0.0

    power_kw = solar_capacity_kw * PEAK_CAPACITY_FACTOR * elevation * solar_index
    energy_kwh = power_kw * (interval_seconds / 3600.0)

    # Round to Wh. Meters do not report unbounded float precision, and rounding
    # here keeps the JSON payloads readable in the console-consumer output that
    # forms the Phase 1 checkpoint evidence.
    return round(energy_kwh, 6)


def max_possible_generation_kwh(
    solar_capacity_kw: float, interval_seconds: float
) -> float:
    """The physical cap for one interval: full nameplate for the whole interval.

    This is the bound Job A's "solar above panel cap" validation compares
    against (§7 step 2). It is defined here, next to the generator, so the
    producer and the validator cannot disagree about what "physically possible"
    means — a disagreement would either reject valid readings or admit impossible
    ones.

    Note it uses full nameplate, not PEAK_CAPACITY_FACTOR: the derating factor is
    a modelling choice about typical output, whereas this is a hard physical
    ceiling. Validating against the derated figure would reject a legitimately
    exceptional reading.
    """
    return round(solar_capacity_kw * (interval_seconds / 3600.0), 6)
