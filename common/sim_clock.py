"""Simulated-time helpers: 1 simulated day = 5 real minutes.

WHY THIS EXISTS IN THE ARCHITECTURE
-----------------------------------
The use case is inherently multi-day: tariff data for day D arrives at the start
of day D+1, bills are issued per completed day, and the headline demo restates a
*previous* day's bill from the log (§11 step 8). None of that is observable in a
10-minute viva at real time, so wall-clock time is compressed: every simulated
day takes SIM_DAY_REAL_SECONDS (300) real seconds.

This module is the ONLY place that conversion is implemented. Every other
component — the meter simulator stamping event_timestamp, the tariff simulator
deciding when to drop a file, the Airflow DAG sealing a day, the Spark jobs
deriving sim_date — calls in here. A second implementation would mean two
definitions of "which day is it", and a reading attributed to the wrong sim_date
is billed on the wrong day's tariff.

TWO ANCHORS, NOT ONE
--------------------
The mapping needs two separate anchors, and conflating them is a real bug:

  SIM_START_DATE  (default 2026-01-01) — the simulated date the demo BEGINS on.
                  Fixed, and asserted in §14 of the plan.
  SIM_ANCHOR_REAL — the real instant that simulated date began, i.e. when this
                  run of the stack started.

An earlier version of this module used a single fixed epoch for both. Because
simulated time advances 288x faster than real time, elapsed real time since a
date in the past gets multiplied by 288: by late 2026 the clock read
`sim_date=2237`, and a demo would have shown bills dated in the 23rd century.
Internally consistent, but it contradicted §14 and would have been indefensible
in a viva. Separating the anchors means day 1 of every run is SIM_START_DATE.

TRADE-OFF (deliberate)
----------------------
SIM_ANCHOR_REAL is the one piece of startup state in the system. It is set once
per stack run (by `make up`, into `.env`) and then read identically by every
container, which keeps the conversion itself a pure function: no service
increments a counter, and any container can restart or start late and
immediately agree with the others on the current simulated day, with nothing to
recover. Had the anchor instead been a shared mutable "current sim day" counter,
a restart would have needed to recover it from somewhere, and two services
disagreeing about the date would silently bill readings against the wrong day's
tariff.

The residual cost is that the clock still cannot be paused or stepped: slowing
the demo means restarting with a larger SIM_DAY_REAL_SECONDS. At this scale,
restart-safety is worth more than pausability.

If SIM_ANCHOR_REAL is unset, it falls back to process start. That keeps a bare
`python -c` or a unit test working without orchestration, at the cost that two
independently-launched processes would disagree — which is why `make up` sets it
explicitly and every long-lived service logs the resolved value at startup.

NOTE ON TIMEZONES: every datetime crossing this boundary is timezone-aware UTC.
Naive datetimes are rejected rather than assumed, because a silent local-time
assumption would shift day boundaries by the host's offset and misattribute
readings near midnight.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from common.config import get_settings

# A simulated day always contains 24 simulated hours; only its real-time
# duration is compressed. Keeping this explicit avoids magic 86400s downstream.
SECONDS_PER_SIM_DAY = 86_400

# Fallback real anchor for processes started without SIM_ANCHOR_REAL set (a unit
# test, or `make clock` on the host). Captured at import so repeated calls within
# one process are consistent rather than drifting with each invocation.
_PROCESS_START_REAL = datetime.now(timezone.utc)


def _require_utc(value: datetime, name: str) -> datetime:
    """Reject naive datetimes; normalise aware ones to UTC."""
    if value.tzinfo is None:
        raise ValueError(
            f"{name} must be timezone-aware; naive datetimes would silently "
            "shift simulated day boundaries by the host's UTC offset"
        )
    return value.astimezone(timezone.utc)


def compression_ratio() -> float:
    """Simulated seconds elapsed per real second. 86400/300 = 288x."""
    return SECONDS_PER_SIM_DAY / get_settings().sim.sim_day_real_seconds


def real_anchor() -> datetime:
    """The real instant at which simulated time started for this run.

    Read from SIM_ANCHOR_REAL so that every container in the stack shares one
    anchor. Falls back to this process's start time when unset, which keeps
    tests and ad-hoc commands working without orchestration.
    """
    configured = get_settings().sim.sim_anchor_real
    if configured is None:
        return _PROCESS_START_REAL
    return _require_utc(configured, "sim_anchor_real")


def sim_start() -> datetime:
    """Midnight of the simulated date the demo begins on (default 2026-01-01)."""
    start_date = get_settings().sim.sim_start_date
    return datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc)


def real_to_sim(real_now: datetime | None = None) -> datetime:
    """Map a wall-clock instant to its simulated instant.

    Elapsed real time since the anchor is multiplied by the compression ratio
    and added to the simulated start. Note that both anchors are needed: using
    the simulated start as the real anchor too is what produced 23rd-century
    dates in the earlier version of this module.
    """
    real_now = _require_utc(real_now or datetime.now(timezone.utc), "real_now")

    real_elapsed = (real_now - real_anchor()).total_seconds()
    return sim_start() + timedelta(seconds=real_elapsed * compression_ratio())


def sim_to_real(sim_instant: datetime) -> datetime:
    """Inverse of `real_to_sim`: when does this simulated instant occur?

    Used by the tariff simulator and the Airflow sim-day sensor, which need to
    know the real moment a simulated day boundary will be crossed.
    """
    sim_instant = _require_utc(sim_instant, "sim_instant")

    sim_elapsed = (sim_instant - sim_start()).total_seconds()
    return real_anchor() + timedelta(seconds=sim_elapsed / compression_ratio())


def current_sim_datetime(real_now: datetime | None = None) -> datetime:
    """The current simulated instant. Stamped onto every meter reading."""
    return real_to_sim(real_now)


def current_sim_date(real_now: datetime | None = None) -> date:
    """The current simulated calendar date — the `sim_date` partition key used
    by the billing aggregates, the Parquet layout and the Airflow DAGs."""
    return current_sim_datetime(real_now).date()


def sim_day_index(real_now: datetime | None = None) -> int:
    """How many whole simulated days have elapsed since the simulated start.
    Exposed as the `smartgrid_sim_day_current` gauge (§8)."""
    return (current_sim_date(real_now) - get_settings().sim.sim_start_date).days


def sim_day_bounds(sim_date: date) -> tuple[datetime, datetime]:
    """Half-open simulated-time bounds [start, end) of a simulated day.

    Half-open by design: a reading at exactly midnight belongs to the day that
    is starting, never to both. Closed bounds are how readings get double-billed
    at day boundaries.
    """
    start = datetime.combine(sim_date, datetime.min.time(), tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


def real_day_bounds(sim_date: date) -> tuple[datetime, datetime]:
    """The real wall-clock window during which a simulated day plays out.

    Airflow needs this to schedule the sealing of a day against real time.
    """
    sim_start, sim_end = sim_day_bounds(sim_date)
    return sim_to_real(sim_start), sim_to_real(sim_end)


def real_seconds_until_next_sim_day(real_now: datetime | None = None) -> float:
    """Real seconds remaining before the simulated date rolls over.

    The tariff simulator sleeps on this to drop day D's file at the start of
    day D+1, modelling a real end-of-day utility feed (§14).
    """
    real_now = _require_utc(real_now or datetime.now(timezone.utc), "real_now")
    _, next_sim_start = sim_day_bounds(current_sim_date(real_now))
    return max(0.0, (sim_to_real(next_sim_start) - real_now).total_seconds())


def startup_banner() -> str:
    """One-line statement of the simulated clock.

    §0 of the plan requires this to be printed in the logs at startup, stated in
    the README and repeated in the report, so that nobody reading a dashboard
    mistakes 288x-compressed time for real time.
    """
    settings = get_settings().sim
    return (
        f"SIMULATED CLOCK: 1 simulated day = {settings.sim_day_real_seconds}s real "
        f"({compression_ratio():.0f}x compression) "
        f"| sim_start={settings.sim_start_date.isoformat()} "
        f"| real_anchor={real_anchor().isoformat()} "
        f"| now sim_date={current_sim_date().isoformat()} "
        f"sim_time={current_sim_datetime().strftime('%H:%M:%S')} "
        f"(sim day {sim_day_index()})"
    )
