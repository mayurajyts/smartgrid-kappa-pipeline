"""Household consumption profile — pure functions, no I/O.

WHY THIS EXISTS IN THE ARCHITECTURE
-----------------------------------
Requirement R1 is "current grid load (kWh) per zone". If every household consumed
a constant amount, zone load would be a flat line, and nothing in the system would
be worth monitoring: the dashboard would show a horizontal trace, the
`ZONE_OVERLOAD` alert could never fire, and the daily billing report's "top-10
consumers" would be an arbitrary ordering of identical numbers.

This module shapes consumption so that the aggregates downstream carry
information. Three properties matter to the rest of the system:

  1. A morning and an evening peak. This is the shape of real residential demand,
     and it is what makes the evening peak — when solar has stopped and demand is
     highest — the interesting moment for `renewable_pct`. That coincidence is
     the most defensible thing on the business dashboard: it is a real grid
     phenomenon (the "duck curve") emerging from the simulation rather than being
     hardcoded.
  2. Per-household variation. Households differ by `base_load_kw`, so the billing
     report's consumer ranking is stable and meaningful across days.
  3. Bounded noise. Consecutive readings differ, so the stream looks like
     telemetry rather than a repeating pattern — but the noise is bounded, so it
     can never produce a negative value that Job A would reject as a fault the
     simulator did not intend to inject.

DETERMINISM: SEEDED PER (METER, INTERVAL)
-----------------------------------------
The noise is drawn from a generator seeded on the meter id and the interval's
simulated timestamp, NOT from a shared global RNG. This makes consumption a pure
function of (household, simulated time): replaying the same simulated day
produces the same readings.

That is not a convenience — it is what makes the Kappa demo verifiable. §11 step 8
replays a simulated day and shows the bill recomputing to the same value. With a
global RNG, a restarted or re-run simulator would produce different consumption
for the same simulated instant, and a bill that changed on replay would be
indistinguishable from a genuine pipeline bug.

(Note this determinism is a property of the *simulator*, not of the pipeline. The
pipeline's replay guarantee comes from reading the same immutable Kafka log. The
simulator's determinism is what lets us tell the two apart while debugging.)

TRADE-OFF (deliberate)
----------------------
This is a shaped analytic profile, not a model derived from real metering data.
Real load has appliance-level structure — a kettle is a 2kW spike lasting 90
seconds, an air conditioner cycles — which produces the short bursts and duty
cycles that make real meter data hard to aggregate cleanly.

Rejected for the same reason the solar model is geometric: the pipeline observes
only the kWh number. Appliance-level realism would make the data look better in a
plot without changing anything the windowed aggregation, the watermarking or the
billing maths has to handle. It belongs in the report's limitations section
(§12 item 9) as a stated simplification rather than being silently glossed over.
"""

from __future__ import annotations

import hashlib
import random
from datetime import datetime

# Multipliers applied to a household's base load through the simulated day.
# Residential shape: an overnight trough, a morning peak as the household wakes,
# a midday dip while the house is empty, and the dominant evening peak.
_MORNING_PEAK_HOUR = 7.5
_EVENING_PEAK_HOUR = 19.5
_MORNING_PEAK_MULTIPLIER = 1.7
_EVENING_PEAK_MULTIPLIER = 2.3   # evening dominates, as in real demand curves
_OVERNIGHT_MULTIPLIER = 0.45     # standby load: fridge, router, chargers
_PEAK_WIDTH_HOURS = 2.5          # standard deviation of each peak's bell

# Noise is +/- this fraction of the shaped value. Bounded strictly below 1.0 so the
# result can never reach zero, let alone go negative: a negative consumption
# reading is one of the faults `fault_injection.py` injects deliberately, and the
# clean path must never produce one by accident or the DLQ evidence is worthless.
_NOISE_FRACTION = 0.18


def _bell(hour: float, centre: float, width: float) -> float:
    """Unnormalised Gaussian bump, 1.0 at the centre."""
    return pow(2.718281828459045, -((hour - centre) ** 2) / (2 * width**2))


def demand_multiplier(sim_time: datetime) -> float:
    """Time-of-day multiplier applied to a household's base load.

    Never returns zero: households draw standby power overnight, and a zero would
    make the 1-minute zone windows empty at night, which would be
    indistinguishable from the meter dropout fault and from a genuine outage.
    """
    hour = sim_time.hour + sim_time.minute / 60.0 + sim_time.second / 3600.0

    morning = (_MORNING_PEAK_MULTIPLIER - _OVERNIGHT_MULTIPLIER) * _bell(
        hour, _MORNING_PEAK_HOUR, _PEAK_WIDTH_HOURS
    )
    evening = (_EVENING_PEAK_MULTIPLIER - _OVERNIGHT_MULTIPLIER) * _bell(
        hour, _EVENING_PEAK_HOUR, _PEAK_WIDTH_HOURS
    )
    return _OVERNIGHT_MULTIPLIER + morning + evening


def time_of_day_bucket(sim_time: datetime) -> str:
    """Coarse period label, derived here so producer and enrichment agree.

    Job A derives `time_of_day_bucket` during enrichment (§7 step 4). The
    boundaries are defined once, in this module, next to the peaks they describe,
    so the label a reading carries actually corresponds to the load shape that
    produced it.
    """
    hour = sim_time.hour + sim_time.minute / 60.0
    if hour < 6.0:
        return "OVERNIGHT"
    if hour < 12.0:
        return "MORNING"
    if hour < 17.0:
        return "AFTERNOON"
    if hour < 22.0:
        return "EVENING"
    return "OVERNIGHT"


def _noise_for(meter_id: str, sim_time: datetime) -> float:
    """Deterministic noise in [-1, 1] for one meter at one simulated instant.

    Seeded from a hash of (meter_id, timestamp) rather than from a shared RNG, so
    the value depends only on its inputs. See the module docstring for why
    reproducibility matters to the replay demo.

    blake2b rather than Python's `hash()`: `hash()` on a string is randomised per
    process by PYTHONHASHSEED, so it would give different readings on every
    restart — the exact non-determinism this is designed to avoid.
    """
    digest = hashlib.blake2b(
        f"{meter_id}|{sim_time.isoformat()}".encode(), digest_size=8
    ).digest()
    return random.Random(int.from_bytes(digest, "big")).uniform(-1.0, 1.0)


def consumption_kwh(
    meter_id: str,
    sim_time: datetime,
    base_load_kw: float,
    interval_seconds: float,
) -> float:
    """Energy consumed in one reading interval, in kWh.

    Args:
        meter_id: identifies the meter; part of the noise seed.
        sim_time: simulated instant at the end of the interval.
        base_load_kw: the household's average draw, from households.csv.
        interval_seconds: interval length in SIMULATED seconds. Energy is power x
            time, so this must be simulated seconds; using the real 2-second tick
            would under-report every bill by the 288x compression factor.

    Returns:
        A strictly positive kWh value for the interval.
    """
    shaped_kw = base_load_kw * demand_multiplier(sim_time)
    noisy_kw = shaped_kw * (1.0 + _NOISE_FRACTION * _noise_for(meter_id, sim_time))

    energy_kwh = noisy_kw * (interval_seconds / 3600.0)

    # Rounded to Wh for readable payloads in the console-consumer output that
    # forms the Phase 1 checkpoint. max() guards the rounding itself: a very small
    # interval could otherwise round to exactly 0.0 and produce the "silent meter"
    # signature without the meter actually being silent.
    return max(round(energy_kwh, 6), 0.000001)
