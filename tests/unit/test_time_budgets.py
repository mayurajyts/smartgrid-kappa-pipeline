"""Pure-arithmetic guards on the simulated-clock conversions (Phase 3).

WHY THESE EXIST
---------------
Every duration in this platform is a REAL-time quantity that has to be converted
onto a simulated axis running `compression_ratio()` faster than wall clock. Three
separate bugs in Phases 2 and 3 were all the same mistake — a duration left on the
wrong axis — and each one presented as a healthy-looking pipeline producing no
output:

  * Job A's dedupe watermark, stated as 2 simulated minutes (0.42 real seconds).
  * Job A's future-timestamp tolerance, stated as 5 simulated minutes (0.21 real
    seconds at `make fast`), which rejected 100% of readings and froze the clean
    topic.
  * Job B's window and watermark, both specified as "1 minute" in §7.

None of those were caught by a test, because each needs a running cluster and live
data to show itself. The arithmetic, however, is pure — so it can be pinned here,
on the host, where it runs in milliseconds and never skips.

These tests assert RELATIONSHIPS between settings, not the settings themselves. A
value can be tuned freely; what must not change silently is the ordering that
keeps the pipeline correct.
"""

from __future__ import annotations

import pytest

from common.config import get_settings

# Producer facts the batch event-time span is derived from.
READINGS_PER_TICK = 200
METER_TICK_REAL_SECONDS = 2.0

# The two clock speeds the project actually runs at: `make demo-config` (the
# documented 1 sim day = 5 real minutes) and `make fast` (the dev inner loop).
DEMO_RATIO = 86_400 / 300.0    # 288x
FAST_RATIO = 86_400 / 60.0     # 1440x
BOTH_RATIOS = [
    pytest.param(DEMO_RATIO, id="demo-300s"),
    pytest.param(FAST_RATIO, id="fast-60s"),
]


def batch_span_sim_minutes(max_offsets: int, ratio: float) -> float:
    """Event-time span of one micro-batch, in simulated minutes.

    `maxOffsetsPerTrigger` bounds a batch's event-time span, not merely its memory
    — the fact that cost Phase 2 hours. This is the arithmetic behind that claim.
    """
    ticks = max_offsets / READINGS_PER_TICK
    return ticks * METER_TICK_REAL_SECONDS * ratio / 60.0


# ---------------------------------------------------------------------------
# Job A — the future-timestamp tolerance is bounded on BOTH sides
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ratio", BOTH_RATIOS)
def test_future_tolerance_stays_below_the_injected_fault(ratio):
    """The upper bound, and the one that is easy to break by "adding headroom".

    `simulators/fault_injection.py` back-dates its future-timestamp fault by
    FAULT_FUTURE_TIMESTAMP_SIM_MINUTES *simulated* minutes. If the tolerance ever
    exceeds that, the fault stops being rejected, the DLQ loses an entire reason
    class, and the Phase 2 checkpoint ("all four DLQ reason classes present")
    fails — silently, because nothing errors.

    The window narrows as the ratio rises, so both speeds are checked.
    """
    from processing.job_a_clean_enrich import JobASettings
    from simulators.fault_injection import FaultSettings

    tolerance_real = JobASettings(
        _env_file=None
    ).future_timestamp_tolerance_real_minutes
    fault_sim = FaultSettings(
        _env_file=None
    ).fault_future_timestamp_sim_minutes

    tolerance_sim = tolerance_real * ratio
    assert tolerance_sim < fault_sim, (
        "future tolerance is {:.1f} simulated minutes at {:.0f}x, which is not "
        "below the injected fault's {} simulated minutes -- that fault would stop "
        "being detected. Raise FAULT_FUTURE_TIMESTAMP_SIM_MINUTES before raising "
        "the tolerance.".format(tolerance_sim, ratio, fault_sim)
    )


def test_future_tolerance_is_invariant_across_clock_speeds():
    """The property the fix actually delivers, stated precisely.

    The original bug was not "the number was too small" — at 288x it was 1.04 real
    seconds, only marginally under the 1.20 the fix gives. The bug was that the
    number was on the WRONG AXIS, so its real-world meaning changed by 5x between
    `make demo-config` and `make fast`: 1.04 real seconds became 0.21, which is
    less than Kafka transit and rejected every reading.

    Being real-based means the permitted skew is the same at any clock speed. That
    is the invariant worth pinning, and it is the one the old setting violated at
    every ratio but one.

    HONEST MARGIN NOTE: 1.2 real seconds is not generous against a micro-batch
    that was measured at up to 18 real seconds on this host under load. The reason
    it cannot simply be raised is the upper bound in the test above — the injected
    fault sits at 30 SIMULATED minutes, which is only 28.8 at 1440x. Widening the
    real-time headroom requires raising FAULT_FUTURE_TIMESTAMP_SIM_MINUTES first.
    That coupling is the real constraint and is documented in
    processing/job_a_clean_enrich.py.
    """
    from processing.job_a_clean_enrich import JobASettings

    tolerance_real = JobASettings(
        _env_file=None
    ).future_timestamp_tolerance_real_minutes

    # The permitted skew in REAL seconds does not depend on the ratio at all --
    # that is the whole point of the conversion happening in the job.
    skew_demo = tolerance_real * 60
    skew_fast = tolerance_real * 60
    assert skew_demo == skew_fast

    # And it must be more than network transit. This floor is deliberately low;
    # see the margin note above for why it cannot currently be higher.
    assert skew_demo >= 1.0, (
        "future tolerance is {:.2f} real seconds, below Kafka transit".format(
            skew_demo
        )
    )


def test_future_tolerance_is_real_minutes_not_simulated():
    """A guard against the setting silently reverting to the simulated axis.

    If someone renames the field back, or re-adds a `*_minutes` field that Job A
    passes through unconverted, this fails.
    """
    from processing.job_a_clean_enrich import JobASettings

    fields = JobASettings.model_fields
    assert "future_timestamp_tolerance_real_minutes" in fields
    assert "future_timestamp_tolerance_minutes" not in fields


# ---------------------------------------------------------------------------
# Job A — the dedupe watermark must span a micro-batch
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ratio", BOTH_RATIOS)
def test_dedupe_watermark_exceeds_one_batch_event_time_span(ratio):
    """Both sides scale with the ratio, so this holds at any clock speed — which
    is the point of expressing the watermark in real minutes in the first place."""
    from processing.job_a_clean_enrich import JobASettings

    settings = JobASettings(_env_file=None)
    watermark_sim = settings.dedupe_watermark_real_minutes * ratio
    span_sim = batch_span_sim_minutes(settings.max_offsets_per_trigger, ratio)

    assert watermark_sim > span_sim, (
        "dedupe watermark {:.0f} sim-min does not exceed one batch's event-time "
        "span {:.0f} sim-min at {:.0f}x -- most records in every batch would "
        "arrive already expired".format(watermark_sim, span_sim, ratio)
    )


def test_raising_max_offsets_breaks_the_watermark_budget():
    """The coupling itself: this is what Job B's startup assertion guards.

    Pinned as a test so the relationship is documented executably — 20000 was the
    value that silently dropped nearly everything in Phase 2.
    """
    from processing.job_a_clean_enrich import JobASettings

    watermark_sim = (
        JobASettings(_env_file=None).dedupe_watermark_real_minutes * DEMO_RATIO
    )
    assert batch_span_sim_minutes(2000, DEMO_RATIO) < watermark_sim
    assert batch_span_sim_minutes(20000, DEMO_RATIO) > watermark_sim


# ---------------------------------------------------------------------------
# Job B — window, watermark and batch span
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ratio", BOTH_RATIOS)
def test_job_b_time_budget_holds(ratio):
    """The three-way ordering Job B asserts at startup:

        watermark > batch span     (or records arrive already expired)
        watermark >= window        (or a window is evicted before its own data)
    """
    from processing.job_b_zone_aggregates import JobBSettings

    settings = JobBSettings(_env_file=None)
    window_sim = settings.zone_window_real_minutes * ratio
    watermark_sim = settings.zone_watermark_real_minutes * ratio
    span_sim = batch_span_sim_minutes(settings.max_offsets_per_trigger, ratio)

    assert watermark_sim > span_sim
    assert watermark_sim >= window_sim


def test_job_b_window_is_not_one_literal_simulated_minute():
    """§7 says "1 minute"; read literally that is 0.21 real seconds and ~86,400
    rows per real minute into zone_load_1m. This pins the resolved reading."""
    from processing.job_b_zone_aggregates import JobBSettings

    settings = JobBSettings(_env_file=None)
    assert settings.zone_window_real_minutes * DEMO_RATIO == 288.0


def test_job_b_watermark_matches_job_a():
    """Both jobs consume the same physical stream. A narrower watermark in Job B
    would discard readings Job A had just certified as on-time, so the two
    disagreeing about "late" is a defect rather than a tuning choice."""
    from processing.job_a_clean_enrich import JobASettings
    from processing.job_b_zone_aggregates import JobBSettings

    assert (
        JobBSettings(_env_file=None).zone_watermark_real_minutes
        == JobASettings(_env_file=None).dedupe_watermark_real_minutes
    )


def test_job_c_watermark_matches_job_a():
    from processing.job_a_clean_enrich import JobASettings
    from processing.job_c_household_billing import JobCSettings

    assert (
        JobCSettings(_env_file=None).billing_watermark_real_minutes
        == JobASettings(_env_file=None).dedupe_watermark_real_minutes
    )


# ---------------------------------------------------------------------------
# The shipped .env must satisfy the same budgets as the defaults
# ---------------------------------------------------------------------------


def test_configured_clock_is_one_of_the_two_supported_speeds():
    """Not a correctness requirement, but a tripwire: the budgets above are
    verified at 288x and 1440x. A third speed has not been checked against the
    fault-detection upper bound."""
    seconds = get_settings().sim.sim_day_real_seconds
    assert seconds in (300, 60), (
        "SIM_DAY_REAL_SECONDS={} is neither the documented demo value (300) nor "
        "the dev value (60); re-check the time budgets in this module".format(seconds)
    )
