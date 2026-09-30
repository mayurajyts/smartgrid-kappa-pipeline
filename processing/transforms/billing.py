"""Billing maths — the single implementation of what a household owes (§7 Job C).

WHY THIS EXISTS IN THE ARCHITECTURE
-----------------------------------
This module is the strongest single piece of evidence for the Kappa decision.

§2.2d rejects Lambda primarily because Lambda requires the same business logic in
two runtimes — and names *this* calculation as the example: "The tiered billing
rule here (`billing_tier` + `subsidy_flag` + block rates) is exactly the kind of
fiddly logic that silently diverges between a Spark batch job and a streaming
job." That argument is only worth making if the project then actually writes it
once. So this is the one implementation. Job C calls it live; Airflow's daily
report (Phase 5) calls it to materialise the issued bill; a replay calls it again
and must get the identical answer. Three consumers, one definition.

It is also the module that makes R7 (bill restatement) meaningful. A restated
bill is only defensible if recomputing from a corrected tariff is *deterministic*
— if the same inputs give the same money, always. That is a property of this
file, not of Spark.

WHY IT IS PURE PYTHON WITH NO SPARK IMPORT
------------------------------------------
Nothing here touches a DataFrame. That is deliberate and has a concrete payoff:
these tests run on the host in milliseconds and NEVER SKIP. The Spark-dependent
transform tests skip when no JVM is available (see tests/unit/conftest.py), which
is the right call for them — but it would be indefensible for the money. The most
important module in the project is the one whose tests always actually run.

§10 additionally requires the tests for this module to be written BEFORE the job
that calls it. They were.

WHY `Decimal` AND NEVER `float`
-------------------------------
Binary floating point cannot represent 22.50, 32.50 or 0.1 exactly. The error per
operation is ~1e-16, which sounds negligible until you count the operations: a
household accumulates ~288 readings per simulated day, each summed into a running
total, then split across up to four blocks, multiplied by a rate, discounted, and
credited. The accumulated error is small — and that is exactly the problem. A
replay would produce a bill that differs in the last decimal place: *nearly*
identical, not identical.

"Nearly identical" is worse than "clearly different". It would quietly falsify the
claim the whole architecture rests on — that a replay reproduces the original run
— while every test that compares to 2 decimal places still passes. So the money is
`Decimal` here and `NUMERIC` in Postgres (see serving/sql/001_schema.sql); either
one alone would only hold the guarantee up to the weaker of them.

Note the deliberate asymmetry with `common/schemas.py`, where kWh are `DoubleType`:
those are physical measurements whose float error is orders of magnitude below
meter precision. Measurements are floats; money is not.

THE BLOCK-RATE MODEL (a resolved ambiguity in §7 — read this before the viva)
-----------------------------------------------------------------------------
§7 asks for a "block/tiered rate by `billing_tier`", but the tariff feed (§6.2)
gives each household ONE `tariff_rate` and ONE `billing_tier`. Those two
descriptions are not the same scheme, and the spec does not reconcile them:

  * Read as a FLAT rate, `billing_tier` is decorative — the bill is
    `kwh x tariff_rate` and the word "block" means nothing.
  * Read as BLOCKS, `tariff_rate` is not the rate most of the bill is charged at.

This implementation takes the second reading, because §7 says "block/tiered" and
because it is what real utility tariffs (including Sri Lanka's) actually do:

  Consumption is split across blocks cheapest-first. The first
  BILLING_BLOCK_KWH are charged at TIER_1's rate, the next block at TIER_2's, and
  so on. `billing_tier` from the feed names the household's highest applicable
  block, and therefore CAPS the ladder rather than selecting a single rate.

Two consequences worth stating plainly, because both will be asked about:

1. The cap makes the final block UNBOUNDED. A TIER_2 household drawing 200 kWh
   pays TIER_1 on the first 30 and TIER_2 on the remaining 170 — it never reaches
   TIER_3's rate however much it consumes. That is what "highest applicable block"
   means, and it is why a lifeline-rate household stays on the lifeline rate.

2. `tariff_rate` from the feed is the household's MARGINAL (top-block) rate, not
   its effective rate. A TIER_3 household drawing 75 kWh has a published
   `tariff_rate` of 45.00 but pays an effective 31.00 LKR/kWh. Both numbers are
   stored on the issued bill so a customer can reconcile them; publishing only the
   first would be an audit problem. This is recorded as a genuine wart in §6.2
   rather than hidden.

THE ROUNDING RULE, STATED ONCE
------------------------------
  Every monetary quantity is quantized to 2 decimal places (LKR cents) using
  ROUND_HALF_UP, exactly once, at the moment it becomes a reportable number.
  Intermediate products and sums are carried at full Decimal precision.

Two choices inside that sentence are load-bearing:

  * ROUND_HALF_UP, not Python's default ROUND_HALF_EVEN. Banker's rounding is
    statistically better and is not what a utility bill does. A customer who sees
    1.125 become 1.12 on one line and 1.135 become 1.14 on the next has a
    legitimate complaint that the rule is not a rule.

  * "Exactly once, at the end." `gross_cost` is the quantization of the UNROUNDED
    sum of the blocks, not the sum of the quantized blocks. With four blocks each
    rounding up, the latter could exceed the true cost by two cents — small, but
    it would be a systematic over-charge, which is the kind of defect that is
    indefensible regardless of magnitude.

WHY A MISSING TARIFF RETURNS A SENTINEL RATHER THAN RAISING
-----------------------------------------------------------
§7 requires that when the tariff for a `sim_date` has not arrived, the running kWh
are written with a NULL rate and flagged — never guessed. That is the normal,
expected state during the first simulated day and at every day boundary (the feed
arrives at D+1 by §14), not an error.

So `compute_bill` returns `BillOutputs(tariff_missing=True, ...)` with every money
field `None`. Raising would force Job C to wrap every call in a try/except and
would turn a routine availability condition into an exception path — the usual way
a "never guess" rule erodes into a silent default.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from typing import Optional, Tuple, Union

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Money is reported to the cent. Quantizing anywhere other than the reporting
# boundary is what this constant exists to make visible in a diff.
MONEY_QUANT = Decimal("0.01")

# kWh are quantized far finer than any block boundary, so a reading can never be
# nudged across a boundary by rounding. Six places matches NUMERIC(18,6) in the
# serving schema; the two must agree or Postgres would round after this module
# had already decided the answer.
KWH_QUANT = Decimal("0.000001")

# Rates are held to 4 places: LKR/kWh values like 22.50 are exact, but an
# effective rate is a quotient and needs somewhere to land.
RATE_QUANT = Decimal("0.0001")

# Ascending order IS the block order. Index i is the (i+1)-th block, and a
# household's `billing_tier` is a cap expressed as a position in this tuple.
TIER_ORDER: Tuple[str, ...] = ("TIER_1", "TIER_2", "TIER_3", "TIER_4")

Number = Union[Decimal, float, int, str]


class BillingSettings(BaseSettings):
    """Block widths, rates and discounts — env-driven, never hardcoded.

    Declared as `Decimal` fields so pydantic parses the environment string
    straight to `Decimal`. That matters: a `float` field would parse "22.50" into
    the nearest binary double and the exactness would already be lost before any
    arithmetic ran, which would defeat the entire point of this module.
    """

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # Width of each block. 30 kWh is illustrative, in the same spirit as the rates
    # (§14: "rates are illustrative and not real CEB tariffs").
    billing_block_kwh: Decimal = Field(default=Decimal("30"), gt=0)

    # MUST agree with simulators/tariff_simulator.py's TARIFF_TIERS, which is what
    # the feed publishes as each household's `tariff_rate`. If the two drift, the
    # feed would advertise one rate while the bill charged from another and
    # nothing would surface it — so tests/unit/test_billing.py asserts they match.
    # See the module docstring for why that duplication exists at all.
    billing_tier_1_rate: Decimal = Field(default=Decimal("22.50"), gt=0)
    billing_tier_2_rate: Decimal = Field(default=Decimal("32.50"), gt=0)
    billing_tier_3_rate: Decimal = Field(default=Decimal("45.00"), gt=0)
    billing_tier_4_rate: Decimal = Field(default=Decimal("62.00"), gt=0)

    # Feed-in tariff, as a fraction of the import rate. 0.5 models the usual shape:
    # the utility still bears network and balancing costs on exported energy, so it
    # does not buy back at the retail price.
    #
    # Deliberately tied to the tariff ladder rather than being an absolute LKR
    # figure, so that a RESTATED tariff also restates the credit. The R7 demo
    # depends on that: publishing a corrected tariff must change every number on
    # the bill, not just the import half.
    billing_export_credit_fraction: Decimal = Field(
        default=Decimal("0.5"), ge=0, le=1
    )

    # Percentage discount on gross cost for subsidised households (§6.2's
    # `subsidy_flag`). Non-zero by default so the branch is exercised in the demo.
    billing_subsidy_discount_pct: Decimal = Field(
        default=Decimal("20.0"), ge=0, le=100
    )


@dataclass(frozen=True)
class BillInputs:
    """Everything needed to price one household for one simulated day.

    Frozen because a bill's inputs must not be mutated after the fact — if a
    number changes, that is a restatement (a new version, §6.5), not an edit.

    `tariff_rate` and `billing_tier` are optional because the tariff feed arrives
    at D+1 (§14) and may legitimately not have landed yet.
    """

    consumption_kwh: Decimal
    solar_kwh: Decimal

    # SIGNED. Negative means the household exported more than it drew over the
    # period. The sign is the only thing that distinguishes a bill from a credit,
    # so it must never be clamped upstream.
    net_grid_kwh: Decimal

    tariff_rate: Optional[Decimal] = None
    billing_tier: Optional[str] = None
    subsidy_flag: bool = False


@dataclass(frozen=True)
class BillOutputs:
    """The priced result. Every money field is None when the tariff is missing.

    `None` rather than `Decimal("0.00")` throughout, and the distinction is the
    whole point: zero is a price, absence is not. Collapsing them would make a
    household that drew nothing indistinguishable from one whose tariff had not
    arrived, which is precisely what `household_billing_running.tariff_missing`
    exists to keep apart.
    """

    gross_cost: Optional[Decimal]
    subsidy_amount: Optional[Decimal]
    export_credit: Optional[Decimal]
    final_bill: Optional[Decimal]

    # gross_cost / billable kWh — what the household actually paid per unit, as
    # opposed to the marginal `tariff_rate` the feed publishes. None when nothing
    # was billable (a quotient with a zero denominator is not zero).
    effective_rate: Optional[Decimal]

    tariff_missing: bool
    billing_tier_applied: Optional[str]

    # (tier, kwh, cost) per block. Carried so the daily report can show a customer
    # *why* the bill is what it is; also what makes the unit tests able to pin the
    # split independently of the total.
    block_breakdown: Tuple[Tuple[str, Decimal, Decimal], ...]


def to_money(value: Number) -> Decimal:
    """Convert any numeric input to an exact `Decimal`.

    ALWAYS via `str()`, never `Decimal(float)`. This is the single most common way
    Decimal code silently reverts to float precision:

        Decimal(0.1)      -> 0.1000000000000000055511151231257827021181583404541015625
        Decimal(str(0.1)) -> 0.1

    It matters here because the kWh arriving from Spark are `DoubleType` (they are
    measurements, correctly), so every value entering this module crosses a float
    boundary exactly once — at this function. Getting it wrong here would make
    every downstream `Decimal` merely decorative.
    """
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _quantize_money(value: Decimal) -> Decimal:
    """Apply the module's one rounding rule. See the module docstring."""
    return value.quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)


def block_rates(settings: BillingSettings) -> "OrderedDict[str, Decimal]":
    """The rate ladder, in block order.

    An OrderedDict keyed by tier name rather than a bare list, so that a caller
    reading `rates["TIER_2"]` cannot accidentally depend on positional indexing
    that a future fifth tier would silently shift.
    """
    return OrderedDict(
        (
            ("TIER_1", settings.billing_tier_1_rate),
            ("TIER_2", settings.billing_tier_2_rate),
            ("TIER_3", settings.billing_tier_3_rate),
            ("TIER_4", settings.billing_tier_4_rate),
        )
    )


def _cap_index(tier_cap: str) -> int:
    """Position of `tier_cap` in the ladder, or raise.

    An unknown tier RAISES rather than defaulting. The feed is validated by
    `TariffReference` (common/schemas.py) before publication, so an unrecognised
    tier arriving here means the contract has been violated somewhere upstream.
    Coercing it to TIER_1 would under-bill silently; coercing it to TIER_4 would
    over-bill silently. Both are worse than a loud failure, because the whole
    reason this module exists is that billing errors must not be quiet.
    """
    try:
        return TIER_ORDER.index(tier_cap)
    except ValueError:
        raise ValueError(
            "unknown billing_tier {!r}; expected one of {}".format(
                tier_cap, ", ".join(TIER_ORDER)
            )
        ) from None


def split_into_blocks(
    kwh: Decimal, tier_cap: str, settings: BillingSettings
) -> Tuple[Tuple[str, Decimal], ...]:
    """Split `kwh` across the block ladder, cheapest first, capped at `tier_cap`.

    Returns `(tier_name, kwh_in_that_block)` pairs, omitting empty blocks — so 30
    kWh yields ONE pair, not two with a zero second. That keeps the breakdown a
    faithful description of the bill rather than a padded table.

    THE CAP MAKES THE LAST BLOCK UNBOUNDED. Every block below the cap is exactly
    `billing_block_kwh` wide; the block AT the cap absorbs everything remaining.
    A TIER_2 household drawing 200 kWh therefore gets `[(TIER_1, 30), (TIER_2,
    170)]`. Without that, a loop over four fixed-width blocks would silently drop
    every kWh past 120 — which is why the tests pin a 500 kWh case explicitly.
    """
    if kwh <= 0:
        return ()

    cap = _cap_index(tier_cap)
    width = settings.billing_block_kwh

    out = []
    remaining = kwh
    for i in range(cap + 1):
        if i < cap:
            take = width if remaining > width else remaining
        else:
            # The capped block: unbounded by construction.
            take = remaining

        if take > 0:
            out.append((TIER_ORDER[i], take))
            remaining -= take

        if remaining <= 0:
            break

    return tuple(out)


def gross_cost_for(
    kwh: Decimal, tier_cap: str, settings: BillingSettings
) -> Tuple[Decimal, Tuple[Tuple[str, Decimal, Decimal], ...]]:
    """Cost of `kwh` under the capped block ladder, plus the per-block breakdown.

    The returned total is the quantization of the UNROUNDED sum — not the sum of
    the quantized per-block costs. See the rounding rule in the module docstring:
    four blocks each rounding up would systematically over-charge by up to two
    cents, and a systematic over-charge is indefensible at any magnitude.

    The per-block costs in the breakdown ARE quantized, because they are display
    values. They may therefore not add up to the total exactly, by design; the
    daily report shows the total as authoritative.
    """
    rates = block_rates(settings)
    blocks = split_into_blocks(kwh, tier_cap, settings)

    exact_total = Decimal("0")
    breakdown = []
    for tier, block_kwh in blocks:
        block_cost = block_kwh * rates[tier]
        exact_total += block_cost
        breakdown.append((tier, block_kwh, _quantize_money(block_cost)))

    return _quantize_money(exact_total), tuple(breakdown)


def export_credit_for(
    net_grid_kwh: Decimal, tier_cap: str, settings: BillingSettings
) -> Decimal:
    """Credit for energy exported to the grid, as a fraction of the import rate.

    Zero unless `net_grid_kwh` is strictly negative. A household exactly in
    balance is not credited — there is no exported kWh to pay for — which mirrors
    `enrichment.py`'s `is_exporting = net_grid_kwh < 0`.

    WHICH IMPORT RATE? TIER_1's, and the reasoning is worth having ready.

    The decision recorded for this phase is "a fraction of the import rate", but
    under a block ladder "the import rate" is itself ambiguous — a household has
    up to four of them. TIER_1 is chosen because a household with negative
    `net_grid_kwh` has, by definition, zero billable draw: it is in the lowest
    block. Crediting at a higher block's rate would pay it for energy it would
    never have bought at that rate, which would make exporting more profitable the
    higher the tier — the opposite of how a feed-in tariff is meant to work.

    `tier_cap` is accepted and validated even though TIER_1 is always used, so
    that an invalid tier fails here too rather than only on the import path, and
    so the signature does not have to change if a future scheme credits by tier.
    """
    _cap_index(tier_cap)  # validate, for the reason in the docstring

    if net_grid_kwh >= 0:
        return _quantize_money(Decimal("0"))

    exported = -net_grid_kwh
    rate = block_rates(settings)[TIER_ORDER[0]]
    return _quantize_money(exported * rate * settings.billing_export_credit_fraction)


def subsidy_for(
    gross_cost: Decimal, subsidy_flag: bool, settings: BillingSettings
) -> Decimal:
    """Percentage discount on gross cost for a subsidised household.

    Applied to GROSS COST ONLY, and before the export credit is subtracted. The
    ordering is deliberate: a subsidy is a discount on what you bought. Applying
    it to the net figure would shrink a subsidised household's export credit —
    i.e. penalise a subsidised household for generating — which is backwards, and
    is the kind of ordering bug that is invisible unless it is named.
    """
    if not subsidy_flag:
        return _quantize_money(Decimal("0"))
    return _quantize_money(
        gross_cost * settings.billing_subsidy_discount_pct / Decimal("100")
    )


def compute_bill(
    inputs: BillInputs, settings: Optional[BillingSettings] = None
) -> BillOutputs:
    """Price one household for one simulated day. The only entry point Job C uses.

    ORDER OF OPERATIONS (load-bearing — each step depends on the previous):

      1. Billable kWh is `max(net_grid_kwh, 0)`. The bill is for energy DRAWN FROM
         THE GRID, not for gross consumption: a household that self-consumed 20 of
         its 30 kWh pays for 10. Billing gross consumption would charge customers
         for their own solar output.

      2. No tariff -> short-circuit with every money field None (see the module
         docstring). Checked before any arithmetic so there is no code path on
         which a partial figure could escape.

      3. Gross cost from the capped block ladder.

      4. Subsidy on gross, before the credit (see `subsidy_for`).

      5. Export credit, if net is negative (see `export_credit_for`). Note that
         steps 3 and 5 are MUTUALLY EXCLUSIVE: `net_grid_kwh` is a single signed
         sum, so a household is either a net importer or a net exporter over the
         period, never both. A reader will wonder; it is stated here so they do
         not have to work it out.

      6. `final_bill = gross - subsidy - credit`, NOT clamped at zero. A net
         exporter's bill is negative and that is a credit the utility owes.
         Clamping would silently confiscate it — and would do so invisibly,
         because the row would still look perfectly well-formed.

      7. `effective_rate` for the report, None when nothing was billable.
    """
    if settings is None:
        settings = BillingSettings()

    net = to_money(inputs.net_grid_kwh)
    billable = net if net > 0 else Decimal("0")

    # Step 2: never guess a rate (§7).
    if inputs.tariff_rate is None or inputs.billing_tier is None:
        return BillOutputs(
            gross_cost=None,
            subsidy_amount=None,
            export_credit=None,
            final_bill=None,
            effective_rate=None,
            tariff_missing=True,
            billing_tier_applied=None,
            block_breakdown=(),
        )

    tier = inputs.billing_tier
    gross_cost, breakdown = gross_cost_for(billable, tier, settings)
    subsidy_amount = subsidy_for(gross_cost, inputs.subsidy_flag, settings)
    export_credit = export_credit_for(net, tier, settings)

    final_bill = _quantize_money(gross_cost - subsidy_amount - export_credit)

    if billable > 0:
        effective_rate = (gross_cost / billable).quantize(
            RATE_QUANT, rounding=ROUND_HALF_UP
        )
    else:
        effective_rate = None

    return BillOutputs(
        gross_cost=gross_cost,
        subsidy_amount=subsidy_amount,
        export_credit=export_credit,
        final_bill=final_bill,
        effective_rate=effective_rate,
        tariff_missing=False,
        billing_tier_applied=tier,
        block_breakdown=breakdown,
    )
