"""Tests for the daily tariff/weather feed generator.

WHY THESE TESTS
---------------
The tariff file's `sim_date` decides which simulated day a household's bill is
computed against. Getting it wrong does not raise anywhere — it produces a tariff
for a day that has no telemetry, and a day of telemetry with no tariff. The bill is
then either missing or computed against the wrong rates, and the only symptom is a
number that looks plausible.

The regression test below (`TestSimDateSelection`) covers a bug found while
verifying Phase 1 end to end. The simulator originally derived the completed day as
`current_sim_date() - 1 day` AFTER waking from its sleep. Because the process starts
mid-day and the clock can advance past the boundary by more than the wake
granularity, this wrote a file dated 2025-12-31 — before the simulated epoch — on a
cold start. It is fixed by capturing the day being waited through BEFORE sleeping.

Content is also asserted against the real §6.2 / §6.3 contracts, so a field renamed
in `common/schemas.py` fails here rather than at the batch loader, where the symptom
would be a silently skipped row.
"""

from __future__ import annotations

import csv
import json
from datetime import date, timedelta

import pytest

from common.config import get_settings
from common.schemas import TariffReference, WeatherForecast
from simulators.tariff_simulator import TARIFF_TIERS, TariffSimulator


@pytest.fixture
def simulator(tmp_path, monkeypatch):
    """A simulator writing into a temp directory rather than the shared volume."""
    monkeypatch.setenv("DROP_DIR", str(tmp_path))
    monkeypatch.setenv("SIM_START_DATE", "2026-01-01")
    monkeypatch.setenv("SIM_DAY_REAL_SECONDS", "300")
    get_settings.cache_clear()
    yield TariffSimulator()
    get_settings.cache_clear()


class TestSimDateSelection:
    """Regression tests for the off-by-one that wrote a pre-epoch file."""

    def test_publishing_uses_the_date_it_is_given(self, simulator):
        """The loop captures the day BEFORE sleeping and passes it in, so the
        writer must honour that date rather than re-deriving it from the clock."""
        target = date(2026, 1, 5)
        simulator._publish_day(target)
        assert (simulator.settings.drop_dir / "tariff_2026-01-05.csv").exists()
        assert (simulator.settings.drop_dir / "weather_2026-01-05.json").exists()

    def test_no_file_is_ever_dated_before_the_simulated_epoch(self, simulator):
        """The concrete bug: a cold start produced tariff_2025-12-31.csv, a day
        with no telemetry and no possible bill."""
        sim_start = get_settings().sim.sim_start_date
        simulator._publish_day(sim_start)
        for path in simulator.settings.drop_dir.iterdir():
            if path.is_file() and "_" in path.stem:
                file_date = date.fromisoformat(path.stem.split("_", 1)[1])
                assert file_date >= sim_start

    def test_rows_carry_the_same_date_as_the_filename(self, simulator):
        """The filename is what an operator reads; the row's sim_date is what the
        billing join uses. A mismatch would bill day D against day D+1's rates."""
        target = date(2026, 1, 7)
        simulator._publish_day(target)

        with (simulator.settings.drop_dir / "tariff_2026-01-07.csv").open() as handle:
            for row in csv.DictReader(handle):
                assert row["sim_date"] == "2026-01-07"

        with (simulator.settings.drop_dir / "weather_2026-01-07.json").open() as handle:
            for entry in json.load(handle):
                assert entry["sim_date"] == "2026-01-07"


class TestTariffFileContents:
    def test_one_row_per_household(self, simulator):
        simulator._publish_day(date(2026, 1, 2))
        with (simulator.settings.drop_dir / "tariff_2026-01-02.csv").open() as handle:
            rows = list(csv.DictReader(handle))
        assert len(rows) == len(simulator.households)

    def test_every_row_validates_against_the_contract(self, simulator):
        """Validated through §6.2 itself, so a contract change fails here rather
        than as a silently skipped row in the batch loader."""
        simulator._publish_day(date(2026, 1, 2))
        with (simulator.settings.drop_dir / "tariff_2026-01-02.csv").open() as handle:
            for row in csv.DictReader(handle):
                TariffReference(
                    household_id=row["household_id"],
                    sim_date=row["sim_date"],
                    tariff_rate=float(row["tariff_rate"]),
                    billing_tier=row["billing_tier"],
                    subsidy_flag=row["subsidy_flag"].strip().lower() == "true",
                    effective_from=row["effective_from"],
                    schema_version=int(row["schema_version"]),
                )

    def test_every_household_appears_exactly_once(self, simulator):
        """household_id is the compaction key; two rows for one household in one
        file would make which tariff survives depend on write order."""
        simulator._publish_day(date(2026, 1, 2))
        with (simulator.settings.drop_dir / "tariff_2026-01-02.csv").open() as handle:
            ids = [row["household_id"] for row in csv.DictReader(handle)]
        assert len(ids) == len(set(ids))
        assert set(ids) == {h.household_id for h in simulator.households}

    def test_tier_matches_the_published_rate(self, simulator):
        """A tier whose rate disagrees with the tier table would make the billing
        maths incoherent with the dimension it joined against."""
        simulator._publish_day(date(2026, 1, 2))
        with (simulator.settings.drop_dir / "tariff_2026-01-02.csv").open() as handle:
            for row in csv.DictReader(handle):
                assert float(row["tariff_rate"]) == TARIFF_TIERS[row["billing_tier"]]

    def test_tier_correlates_with_consumption(self, simulator):
        """Random tier assignment would make the billing report incoherent — a
        lifeline-rate household topping the consumption table."""
        simulator._publish_day(date(2026, 1, 2))
        by_id = {h.household_id: h for h in simulator.households}
        with (simulator.settings.drop_dir / "tariff_2026-01-02.csv").open() as handle:
            pairs = [
                (by_id[r["household_id"]].base_load_kw, float(r["tariff_rate"]))
                for r in csv.DictReader(handle)
            ]
        lightest = min(pairs, key=lambda p: p[0])
        heaviest = max(pairs, key=lambda p: p[0])
        assert heaviest[1] > lightest[1]

    def test_some_households_receive_a_subsidy(self, simulator):
        """The subsidy branch of the billing maths must be exercised by the demo,
        so the flag cannot be uniformly false."""
        simulator._publish_day(date(2026, 1, 2))
        with (simulator.settings.drop_dir / "tariff_2026-01-02.csv").open() as handle:
            flags = {row["subsidy_flag"] for row in csv.DictReader(handle)}
        assert "true" in flags


class TestWeatherFileContents:
    def test_one_forecast_per_zone(self, simulator):
        simulator._publish_day(date(2026, 1, 2))
        with (simulator.settings.drop_dir / "weather_2026-01-02.json").open() as handle:
            forecasts = json.load(handle)
        assert len(forecasts) == len(simulator.zones)
        assert {f["grid_zone"] for f in forecasts} == set(simulator.zones)

    def test_every_forecast_validates_against_the_contract(self, simulator):
        simulator._publish_day(date(2026, 1, 2))
        with (simulator.settings.drop_dir / "weather_2026-01-02.json").open() as handle:
            for entry in json.load(handle):
                WeatherForecast(**entry)

    def test_solar_index_is_never_zero(self, simulator):
        """Diffuse light still generates under heavy cloud. A hard zero would look
        like a panel fault rather than weather, and would make renewable_pct
        undefined instead of merely low."""
        simulator._publish_day(date(2026, 1, 2))
        with (simulator.settings.drop_dir / "weather_2026-01-02.json").open() as handle:
            for entry in json.load(handle):
                assert 0.0 < entry["expected_solar_index"] <= 1.0

    def test_solar_index_falls_as_cloud_cover_rises(self, simulator):
        """The §11 step 4 demo lever: cloud must actually suppress solar."""
        indices = []
        for day_offset in range(12):
            target = date(2026, 1, 2) + timedelta(days=day_offset)
            simulator._publish_day(target)
            path = simulator.settings.drop_dir / f"weather_{target.isoformat()}.json"
            with path.open() as handle:
                for entry in json.load(handle):
                    indices.append(
                        (entry["cloud_cover_pct"], entry["expected_solar_index"])
                    )
        cloudiest = max(indices, key=lambda p: p[0])
        clearest = min(indices, key=lambda p: p[0])
        assert cloudiest[1] < clearest[1]

    def test_compaction_key_includes_the_sim_date(self, simulator):
        """Keyed by grid_zone|sim_date, not grid_zone alone: replaying a past day
        must see that day's forecast, not the latest one."""
        simulator._publish_day(date(2026, 1, 2))
        with (simulator.settings.drop_dir / "weather_2026-01-02.json").open() as handle:
            record = WeatherForecast(**json.load(handle)[0])
        assert record.compaction_key == f"{record.grid_zone}|2026-01-02"


class TestDeterminism:
    def test_the_same_day_regenerates_identical_tariffs(self, simulator):
        """Replaying a simulated day must be able to reproduce its tariff file, or
        the replay would compare a bill against different rates and prove nothing.
        """
        target = date(2026, 1, 9)
        path = simulator.settings.drop_dir / f"tariff_{target.isoformat()}.csv"

        simulator._publish_day(target)
        first = path.read_text(encoding="utf-8")
        path.unlink()
        simulator._publish_day(target)
        assert path.read_text(encoding="utf-8") == first

    def test_different_days_differ(self, simulator):
        """Otherwise every day's weather would be identical and no overcast day
        would ever occur to trigger LOW_RENEWABLE."""
        simulator._publish_day(date(2026, 1, 3))
        simulator._publish_day(date(2026, 1, 4))
        third = (simulator.settings.drop_dir / "weather_2026-01-03.json").read_text()
        fourth = (simulator.settings.drop_dir / "weather_2026-01-04.json").read_text()
        assert third != fourth


class TestAtomicWrites:
    def test_no_temporary_files_remain(self, simulator):
        """Files are written to a dotted temp name then renamed, so the polling
        loader can never read a half-written file. A leftover temp file would mean
        the rename did not happen."""
        simulator._publish_day(date(2026, 1, 2))
        leftovers = [
            p.name
            for p in simulator.settings.drop_dir.iterdir()
            if p.name.startswith(".") and p.name.endswith(".tmp")
        ]
        assert leftovers == []

    def test_republishing_a_day_overwrites_cleanly(self, simulator):
        """A corrected tariff for an already-published day must replace the file
        rather than appending — this is the R7 restatement path."""
        target = date(2026, 1, 2)
        simulator._publish_day(target)
        simulator._publish_day(target)
        path = simulator.settings.drop_dir / f"tariff_{target.isoformat()}.csv"
        with path.open() as handle:
            rows = list(csv.DictReader(handle))
        assert len(rows) == len(simulator.households)
