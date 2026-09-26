"""The simulated world: households and grid zones, loaded from committed CSVs.

WHY THIS EXISTS IN THE ARCHITECTURE
-----------------------------------
This is the static dimension the whole system is described in terms of. The meter
simulator iterates over these households, Job A enriches readings by joining
against them (§7 step 4), Job B's zone aggregates are grouped by their
`grid_zone`, and the billing report's "top-10 consumers" and "solar league table"
are lists of these households.

WHY THE CSVs ARE COMMITTED, NOT GENERATED AT STARTUP
----------------------------------------------------
This is the load-bearing decision in this module, and it is what makes the
headline demo (§11 step 8) mean anything.

The Kappa claim is demonstrated by replaying a simulated day and showing the bill
recomputes to the same value. If the household population were randomly generated
at container startup, a replay would run against a *different* set of households
with different base loads, and "the bill came out the same" would be either
impossible or meaningless. A fixed, committed population makes the replay a
genuine comparison: same inputs, same code, same answer.

It also makes the project reproducible for a marker. `HH-0042` in a screenshot in
the report is the same household on their machine as on mine.

TRADE-OFF (deliberate)
----------------------
Committed data is inflexible: changing the population means editing a CSV and
re-running the demo rather than tweaking an env var. The alternative — a seeded
generator — would give the same determinism from a seed. It was rejected because
a CSV is directly inspectable: in the viva the population can be opened and read,
whereas a seed requires trusting that the generator is deterministic across Python
versions (which, for `random`, is not actually guaranteed across releases).

VALIDATION IS STRICT AND FAIL-FAST
----------------------------------
A malformed row, a duplicate id, or a household referencing a non-existent zone
raises at load time and the container refuses to start. The failure mode being
avoided is silent: a household in a typo'd zone would simply never appear in any
zone aggregate — its consumption would vanish from the grid totals with no error
anywhere, and the numbers would be quietly wrong on a dashboard that looks fine.
"""

from __future__ import annotations

import csv
from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, model_validator

# The CSVs live beside this module, so the package is self-contained and the
# path does not depend on the working directory a container happens to use.
REFERENCE_DIR = Path(__file__).parent / "reference"
HOUSEHOLDS_CSV = REFERENCE_DIR / "households.csv"
ZONES_CSV = REFERENCE_DIR / "zones.csv"


class Zone(BaseModel):
    """A grid zone — the aggregation unit for R1/R2 and the Kafka partition key."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    grid_zone: str
    name: str
    # Used by the ZONE_OVERLOAD alert (§7 Job D) as the threshold to compare
    # summed zone consumption against.
    capacity_kw: float = Field(gt=0)
    # Shapes sunrise/sunset in the solar curve. These zones are in southern Sri
    # Lanka (~6degN), where day length barely varies across the year — which is
    # why the solar model can use a fixed daylight window without being wrong.
    latitude_deg: float = Field(ge=-90, le=90)


class Household(BaseModel):
    """A metered household — the billing unit for R5/R6."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    household_id: str
    meter_id: str
    grid_zone: str
    has_solar: bool
    # Average real power draw in kW. The load profile shapes this across the day;
    # it is not itself the per-interval reading.
    base_load_kw: float = Field(gt=0)
    # Installed panel capacity in kW. Job A rejects any reading whose solar
    # generation exceeds this (§7 step 2, "above the physical panel cap"), so
    # this value is the physical bound that validation rule is defined against.
    solar_capacity_kw: float = Field(ge=0)

    @model_validator(mode="after")
    def _solar_flag_is_consistent(self) -> "Household":
        """A household with panels must have capacity, and vice versa.

        Checked because these two fields are read by different parts of the
        system — `has_solar` by the enrichment join, `solar_capacity_kw` by the
        validation rule — and an inconsistency between them would make a
        household's readings rejected as "solar above capacity" while the
        dimension insisted it had no panels at all.

        A model validator, not a field validator: a field validator on
        `has_solar` would run before `solar_capacity_kw` had been populated
        (pydantic validates in declaration order), so the cross-field check would
        silently never fire. Validating after the whole model is built means both
        values are always present.
        """
        if self.has_solar and self.solar_capacity_kw <= 0:
            raise ValueError("has_solar is true but solar_capacity_kw is not positive")
        if not self.has_solar and self.solar_capacity_kw > 0:
            raise ValueError("has_solar is false but solar_capacity_kw is positive")
        return self


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(
            f"reference data missing: {path}. These CSVs are committed to the "
            "repository; a missing file means an incomplete checkout, not a "
            "configuration problem."
        )
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


@lru_cache(maxsize=1)
def load_zones() -> dict[str, Zone]:
    """Load zones, keyed by `grid_zone`. Cached: read once per process."""
    zones: dict[str, Zone] = {}
    for line_no, row in enumerate(_read_csv(ZONES_CSV), start=2):
        try:
            zone = Zone(
                grid_zone=row["grid_zone"],
                name=row["name"],
                capacity_kw=float(row["capacity_kw"]),
                latitude_deg=float(row["latitude_deg"]),
            )
        except Exception as exc:
            raise ValueError(f"{ZONES_CSV.name} line {line_no}: {exc}") from exc
        if zone.grid_zone in zones:
            raise ValueError(
                f"{ZONES_CSV.name} line {line_no}: duplicate grid_zone "
                f"{zone.grid_zone!r}; zone ids are the Kafka partition key and "
                "must be unique"
            )
        zones[zone.grid_zone] = zone
    if not zones:
        raise ValueError(f"{ZONES_CSV.name} contains no zones")
    return zones


@lru_cache(maxsize=1)
def load_households() -> tuple[Household, ...]:
    """Load households in file order, cross-checked against the zones.

    Returns a tuple rather than a list so the cached value cannot be mutated by
    one caller and observed changed by another — the meter simulator iterates
    this every tick.
    """
    zones = load_zones()
    households: list[Household] = []
    seen_households: set[str] = set()
    seen_meters: set[str] = set()

    for line_no, row in enumerate(_read_csv(HOUSEHOLDS_CSV), start=2):
        try:
            household = Household(
                household_id=row["household_id"],
                meter_id=row["meter_id"],
                grid_zone=row["grid_zone"],
                has_solar=row["has_solar"].strip().lower() == "true",
                base_load_kw=float(row["base_load_kw"]),
                solar_capacity_kw=float(row["solar_capacity_kw"]),
            )
        except Exception as exc:
            raise ValueError(f"{HOUSEHOLDS_CSV.name} line {line_no}: {exc}") from exc

        # The silent-data-loss check described in the module docstring.
        if household.grid_zone not in zones:
            raise ValueError(
                f"{HOUSEHOLDS_CSV.name} line {line_no}: household "
                f"{household.household_id} references unknown zone "
                f"{household.grid_zone!r}. Known zones: {sorted(zones)}"
            )
        # Both ids are primary keys downstream: household_id in
        # household_billing_daily, meter_id in the METER_SILENT alert.
        if household.household_id in seen_households:
            raise ValueError(
                f"{HOUSEHOLDS_CSV.name} line {line_no}: duplicate household_id "
                f"{household.household_id!r}"
            )
        if household.meter_id in seen_meters:
            raise ValueError(
                f"{HOUSEHOLDS_CSV.name} line {line_no}: duplicate meter_id "
                f"{household.meter_id!r}"
            )
        seen_households.add(household.household_id)
        seen_meters.add(household.meter_id)
        households.append(household)

    if not households:
        raise ValueError(f"{HOUSEHOLDS_CSV.name} contains no households")
    return tuple(households)


def households_by_zone() -> dict[str, tuple[Household, ...]]:
    """Group households by zone — used when reporting per-zone meter counts."""
    grouped: dict[str, list[Household]] = {zone: [] for zone in load_zones()}
    for household in load_households():
        grouped[household.grid_zone].append(household)
    return {zone: tuple(members) for zone, members in grouped.items()}


def reference_summary() -> dict[str, object]:
    """One-line-able summary, logged at simulator startup.

    Printed at boot so the log records which population produced a given run's
    data. When a replay is compared against the original run, this is the
    evidence that both ran against the same world.
    """
    households = load_households()
    zones = load_zones()
    with_solar = sum(1 for h in households if h.has_solar)
    return {
        "households": len(households),
        "zones": len(zones),
        "households_with_solar": with_solar,
        "solar_pct": round(100.0 * with_solar / len(households), 1),
        "total_solar_capacity_kw": round(
            sum(h.solar_capacity_kw for h in households), 2
        ),
        "total_zone_capacity_kw": round(sum(z.capacity_kw for z in zones.values()), 2),
    }
