"""Tests for the committed reference data.

WHY TEST COMMITTED DATA
-----------------------
These CSVs are checked into the repository, so they cannot change between runs —
which raises the fair question of why they need tests at all.

Two reasons. First, the §14 assumptions ("~200 households across 5 grid zones;
~60% have solar") are stated in the report and defended in the viva; a test is how
the claim stays true if the data is ever regenerated or hand-edited. Second, the
loader enforces cross-file integrity — a household must reference a real zone, ids
must be unique, the solar flag must agree with the capacity — and those guards are
worth testing because the failure they prevent is silent. A household in a
mistyped zone does not error anywhere; its consumption simply never appears in any
zone aggregate, and the grid totals are quietly wrong on a dashboard that looks
perfectly healthy.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from simulators.reference_data import (
    Household,
    Zone,
    households_by_zone,
    load_households,
    load_zones,
    reference_summary,
)


class TestZones:
    def test_five_zones_per_the_assumptions(self):
        assert len(load_zones()) == 5

    def test_keyed_by_grid_zone(self):
        for key, zone in load_zones().items():
            assert key == zone.grid_zone

    def test_every_zone_has_positive_capacity(self):
        """capacity_kw is the ZONE_OVERLOAD alert threshold; zero or negative
        would make the alert fire constantly or never."""
        for zone in load_zones().values():
            assert zone.capacity_kw > 0

    def test_zones_are_in_southern_sri_lanka(self):
        """The fixed 06:00-18:00 daylight window in solar.py is justified by
        these zones sitting near the equator, where day length barely varies."""
        for zone in load_zones().values():
            assert 5.0 < zone.latitude_deg < 10.0

    def test_every_zone_is_named(self):
        for zone in load_zones().values():
            assert zone.name.strip()


class TestHouseholds:
    def test_two_hundred_households_per_the_assumptions(self):
        assert len(load_households()) == 200

    def test_roughly_sixty_percent_have_solar(self):
        """§14 states ~60%. Asserted as a band, not an exact count, since the
        assumption is approximate — but tight enough to catch a regeneration
        that changed the mix materially."""
        households = load_households()
        share = sum(1 for h in households if h.has_solar) / len(households)
        assert 0.55 <= share <= 0.65

    def test_household_ids_are_unique(self):
        """household_id is the primary key of household_billing_daily."""
        ids = [h.household_id for h in load_households()]
        assert len(ids) == len(set(ids))

    def test_meter_ids_are_unique(self):
        """meter_id identifies the entity in the METER_SILENT alert."""
        ids = [h.meter_id for h in load_households()]
        assert len(ids) == len(set(ids))

    def test_every_household_references_a_real_zone(self):
        """The silent-data-loss guard: a household in an unknown zone would
        vanish from zone aggregates with no error raised anywhere."""
        zones = load_zones()
        for household in load_households():
            assert household.grid_zone in zones

    def test_every_household_has_positive_base_load(self):
        for household in load_households():
            assert household.base_load_kw > 0

    def test_solar_flag_agrees_with_capacity(self):
        """Read by different parts of the system — has_solar by the enrichment
        join, solar_capacity_kw by the validation rule — so a mismatch would make
        a household's readings rejected as above-capacity while the dimension
        insisted it had no panels."""
        for household in load_households():
            if household.has_solar:
                assert household.solar_capacity_kw > 0
            else:
                assert household.solar_capacity_kw == 0


class TestGrouping:
    def test_every_zone_appears_in_the_grouping(self):
        assert set(households_by_zone()) == set(load_zones())

    def test_grouping_preserves_every_household(self):
        grouped = households_by_zone()
        assert sum(len(members) for members in grouped.values()) == len(load_households())

    def test_no_zone_is_empty(self):
        """An empty zone would produce no 1-minute windows and would look
        permanently silent to the staleness alert."""
        for zone, members in households_by_zone().items():
            assert members, f"{zone} has no households"

    def test_zones_have_different_sizes(self):
        """Deliberately uneven, so zone aggregates differ visibly on the
        dashboard rather than all tracking the same line."""
        sizes = {len(m) for m in households_by_zone().values()}
        assert len(sizes) > 1


class TestSummary:
    def test_summary_reports_the_documented_population(self):
        """Logged at simulator startup so a run's logs record which population
        produced its data — the evidence that a replay ran against the same
        world as the original."""
        summary = reference_summary()
        assert summary["households"] == 200
        assert summary["zones"] == 5
        assert 55.0 <= summary["solar_pct"] <= 65.0
        assert summary["total_solar_capacity_kw"] > 0
        assert summary["total_zone_capacity_kw"] > 0


class TestCaching:
    def test_loaders_are_cached(self):
        """The meter simulator iterates households every tick; re-parsing the
        CSV each time would be pure waste."""
        assert load_households() is load_households()
        assert load_zones() is load_zones()


class TestValidationGuards:
    """The model-level guards, exercised directly rather than via the CSVs."""

    def test_solar_flag_true_without_capacity_is_rejected(self):
        with pytest.raises(ValidationError):
            Household(
                household_id="HH-9999",
                meter_id="MTR-9999",
                grid_zone="ZONE-A",
                has_solar=True,
                base_load_kw=1.0,
                solar_capacity_kw=0.0,
            )

    def test_solar_flag_false_with_capacity_is_rejected(self):
        with pytest.raises(ValidationError):
            Household(
                household_id="HH-9999",
                meter_id="MTR-9999",
                grid_zone="ZONE-A",
                has_solar=False,
                base_load_kw=1.0,
                solar_capacity_kw=3.0,
            )

    def test_non_positive_base_load_is_rejected(self):
        with pytest.raises(ValidationError):
            Household(
                household_id="HH-9999",
                meter_id="MTR-9999",
                grid_zone="ZONE-A",
                has_solar=False,
                base_load_kw=0.0,
                solar_capacity_kw=0.0,
            )

    def test_unexpected_field_is_rejected(self):
        """extra='forbid' — an added CSV column must fail loudly rather than
        being silently ignored."""
        with pytest.raises(ValidationError):
            Zone(
                grid_zone="ZONE-Z",
                name="Test",
                capacity_kw=100.0,
                latitude_deg=6.0,
                unexpected="value",
            )

    def test_records_are_immutable(self):
        """frozen=True: the cached dimension is shared across the process, so one
        caller mutating it would change what every other caller sees."""
        household = load_households()[0]
        with pytest.raises(ValidationError):
            household.base_load_kw = 99.0
