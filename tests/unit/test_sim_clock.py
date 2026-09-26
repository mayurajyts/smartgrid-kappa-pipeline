"""Tests for the simulated clock.

WHY THIS MODULE IS TESTED IN PHASE 0, BEFORE ANY PIPELINE CODE EXISTS
---------------------------------------------------------------------
Every later component derives `sim_date` from this module: the meter simulator
stamps it on each reading, the tariff simulator uses it to decide when to drop a
file, Job C groups billing aggregates by it, and Airflow seals days on it. A
day-boundary error here does not surface as a crash — it surfaces as readings
billed against the wrong day's tariff, which is the single most expensive class
of bug in this system and the hardest to spot by eye.

The boundary conditions (exact midnight, the last microsecond of a day) are
tested explicitly because those are where an off-by-one puts a reading on the
wrong side of a billing period.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from common.config import get_settings
from common.sim_clock import (
    SECONDS_PER_SIM_DAY,
    compression_ratio,
    current_sim_date,
    current_sim_datetime,
    real_anchor,
    real_day_bounds,
    real_seconds_until_next_sim_day,
    real_to_sim,
    sim_day_bounds,
    sim_day_index,
    sim_start,
    sim_to_real,
    startup_banner,
)

# The REAL anchor used by the tests. Deliberately a different value from the
# simulated start date, so any test that still conflates the two fails.
ANCHOR = datetime(2026, 9, 26, 12, 0, 0, tzinfo=timezone.utc)

# Midnight of the simulated start date.
SIM_START = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _default_clock_settings(monkeypatch):
    """Pin the clock to the documented defaults for every test.

    Settings are cached process-wide (see get_settings), so the cache is cleared
    both before and after: before, so a value cached by an earlier test does not
    leak in; after, so these test values do not leak out.
    """
    monkeypatch.setenv("SIM_START_DATE", "2026-01-01")
    monkeypatch.setenv("SIM_ANCHOR_REAL", ANCHOR.isoformat())
    monkeypatch.setenv("SIM_DAY_REAL_SECONDS", "300")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


class TestCompressionRatio:
    def test_five_minute_sim_day_is_288x(self):
        """86400 simulated seconds in 300 real seconds = 288x. This is the
        headline number stated in the README, the logs and the report."""
        assert compression_ratio() == pytest.approx(288.0)

    def test_ratio_follows_configuration(self, monkeypatch):
        """The ratio must be derived from config, not hardcoded — the demo may
        be slowed down for the viva."""
        monkeypatch.setenv("SIM_DAY_REAL_SECONDS", "600")
        get_settings.cache_clear()
        assert compression_ratio() == pytest.approx(144.0)


class TestTwoAnchors:
    """The bug this separation fixes: a single fixed epoch multiplied all elapsed
    real time since 2026-01-01 by 288, so the clock read a 23rd-century date."""

    def test_anchor_and_sim_start_are_independent(self):
        assert real_anchor() == ANCHOR
        assert sim_start() == SIM_START
        assert real_anchor() != sim_start()

    def test_simulated_time_starts_at_sim_start_not_at_the_anchor(self):
        """At the anchor instant, simulated time is exactly SIM_START — not the
        anchor date, and not centuries later."""
        assert real_to_sim(ANCHOR) == SIM_START

    def test_a_late_anchor_does_not_push_simulated_time_forward(self):
        """Anchoring years after the simulated start date must still begin the
        demo on the simulated start date. This is the regression test for the
        23rd-century bug."""
        assert real_to_sim(ANCHOR).year == 2026
        assert current_sim_date(ANCHOR) == date(2026, 1, 1)

    def test_unset_anchor_falls_back_to_process_start(self, monkeypatch):
        """A bare `python -c` or a unit test must work without orchestration."""
        monkeypatch.delenv("SIM_ANCHOR_REAL", raising=False)
        get_settings.cache_clear()
        assert real_anchor() is not None
        # Still begins at the simulated start date, just anchored to "now".
        assert current_sim_date(real_anchor()) == date(2026, 1, 1)


class TestRealToSim:
    def test_anchor_maps_to_simulated_start(self):
        assert real_to_sim(ANCHOR) == SIM_START

    def test_one_real_sim_day_advances_exactly_one_sim_day(self):
        """300 real seconds must advance simulated time by exactly 24 hours.
        Drift here accumulates across days and eventually misplaces a boundary."""
        assert real_to_sim(ANCHOR + timedelta(seconds=300)) == SIM_START + timedelta(days=1)

    def test_half_a_sim_day_is_simulated_noon(self):
        result = real_to_sim(ANCHOR + timedelta(seconds=150))
        assert result == SIM_START + timedelta(hours=12)

    def test_round_trip_through_sim_and_back_is_identity(self):
        """sim_to_real must invert real_to_sim exactly; the tariff simulator and
        the Airflow sensor depend on converting in both directions."""
        real = ANCHOR + timedelta(seconds=1234.5)
        assert sim_to_real(real_to_sim(real)) == pytest.approx(
            real, abs=timedelta(microseconds=1)
        )

    def test_naive_datetime_is_rejected(self):
        """A naive datetime would be interpreted in the host's local time,
        shifting every day boundary by the host's UTC offset. Refuse it."""
        with pytest.raises(ValueError, match="timezone-aware"):
            real_to_sim(datetime(2026, 1, 1, 0, 0, 0))

    def test_non_utc_input_is_normalised(self):
        """An aware datetime in another zone is valid; it must be converted,
        not rejected."""
        plus_five_thirty = timezone(timedelta(hours=5, minutes=30))
        same_instant = ANCHOR.astimezone(plus_five_thirty)
        assert real_to_sim(same_instant) == SIM_START


class TestSimDate:
    def test_date_at_the_anchor_is_the_simulated_start_date(self):
        assert current_sim_date(ANCHOR) == date(2026, 1, 1)

    def test_date_rolls_over_after_one_real_sim_day(self):
        assert current_sim_date(ANCHOR + timedelta(seconds=300)) == date(2026, 1, 2)

    def test_date_is_stable_within_a_sim_day(self):
        """Every instant strictly inside a 300s window is the same sim date."""
        for offset in (1, 75, 150, 299):
            assert current_sim_date(ANCHOR + timedelta(seconds=offset)) == date(2026, 1, 1)

    def test_boundary_instant_belongs_to_the_new_day(self):
        """At exactly 300s the new day has started. Half-open semantics: the
        boundary instant belongs to the day beginning, never to both."""
        assert current_sim_date(ANCHOR + timedelta(seconds=299.999)) == date(2026, 1, 1)
        assert current_sim_date(ANCHOR + timedelta(seconds=300.0)) == date(2026, 1, 2)

    def test_sim_day_index_counts_from_epoch(self):
        assert sim_day_index(ANCHOR) == 0
        assert sim_day_index(ANCHOR + timedelta(seconds=300)) == 1
        assert sim_day_index(ANCHOR + timedelta(seconds=1500)) == 5


class TestSimDayBounds:
    def test_bounds_span_exactly_one_day(self):
        start, end = sim_day_bounds(date(2026, 1, 14))
        assert start == datetime(2026, 1, 14, tzinfo=timezone.utc)
        assert end == datetime(2026, 1, 15, tzinfo=timezone.utc)
        assert (end - start).total_seconds() == SECONDS_PER_SIM_DAY

    def test_bounds_are_half_open_and_do_not_overlap(self):
        """Consecutive days must abut without overlapping: day D's end is day
        D+1's start, and no instant falls in both. Overlapping bounds are how
        readings get counted twice at a billing boundary."""
        _, end_of_first = sim_day_bounds(date(2026, 1, 14))
        start_of_second, _ = sim_day_bounds(date(2026, 1, 15))
        assert end_of_first == start_of_second

    def test_bounds_round_trip_with_current_sim_date(self):
        """A day's own start instant must resolve back to that same day."""
        target = date(2026, 1, 20)
        start, _ = sim_day_bounds(target)
        assert start.date() == target

    def test_real_day_bounds_span_one_real_sim_day(self):
        """A simulated day occupies exactly SIM_DAY_REAL_SECONDS of real time —
        the window Airflow schedules the sealing task against."""
        real_start, real_end = real_day_bounds(date(2026, 1, 3))
        assert (real_end - real_start).total_seconds() == pytest.approx(300.0)


class TestTimeUntilNextSimDay:
    def test_full_day_remaining_at_a_boundary(self):
        assert real_seconds_until_next_sim_day(ANCHOR) == pytest.approx(300.0)

    def test_counts_down_within_a_day(self):
        remaining = real_seconds_until_next_sim_day(ANCHOR + timedelta(seconds=120))
        assert remaining == pytest.approx(180.0)

    def test_never_negative(self):
        """The tariff simulator sleeps on this value; a negative sleep would
        raise, taking the producer down at a day boundary."""
        for offset in (0, 299.9999, 300, 1234.5):
            assert real_seconds_until_next_sim_day(ANCHOR + timedelta(seconds=offset)) >= 0


class TestStartupBanner:
    def test_banner_states_the_compression(self):
        """§0 requires the clock ratio to appear in the logs at startup, so
        nobody mistakes 288x-compressed time for real time."""
        banner = startup_banner()
        assert "300s real" in banner
        assert "288x" in banner
        assert "sim_start=2026-01-01" in banner
        # Both anchors must be visible, so an operator reading the logs can tell
        # which real moment the simulated timeline was pinned to.
        assert "real_anchor=" in banner

    def test_banner_reports_the_live_clock(self):
        """Sanity check that the banner reflects the real clock rather than a
        frozen constant."""
        assert current_sim_datetime().tzinfo is not None
        assert "sim_date=" in startup_banner()
