"""Unit tests for the billing maths (§10: written BEFORE the job that calls it).

WHY THESE EXIST AND WHY THEY NEVER SKIP
---------------------------------------
§7 says of `transforms/billing.py`: "Unit-test this module hard. It is the most
defensible thing you can show in a viva." §10 additionally fixes the ordering —
these tests precede `job_c_household_billing.py`, so the maths is pinned before
any Spark plumbing exists to distract from whether it is right.

Unlike the validation and enrichment suites, nothing here imports PySpark, so
none of it is skipped by the JVM guard in conftest.py. That is deliberate: a test
suite that silently skips is indistinguishable from one that passes, and the one
module where that would be unacceptable is the one that computes money.

WHAT IS ACTUALLY BEING TESTED
-----------------------------
Not "does it run" — the genuine boundary conditions the block structure creates:
the exact block edges, the tier cap biting below actual consumption, the
unbounded final block, the interaction of subsidy and export credit, the rounding
mode, and the distinction between a zero bill and an absent one.

Every test builds its own `BillingSettings` with explicit values rather than
reading the ambient `.env`. If these read the environment, a developer running
`make fast` or editing a rate locally would change what the tests assert — and
the suite would stop being a specification of the maths and become a description
of one machine's configuration.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from processing.transforms.billing import (
    TIER_ORDER,
    BillingSettings,
    BillInputs,
    block_rates,
    compute_bill,
    export_credit_for,
    gross_cost_for,
    split_into_blocks,
    subsidy_for,
    to_money,
)


def settings(**overrides) -> BillingSettings:
    """A `BillingSettings` with the documented defaults, pinned explicitly.

    `_env_file=None` stops pydantic-settings reading `.env`, so these assertions
    describe the billing rules rather than the current machine's configuration.
    """
    base = dict(
        billing_block_kwh=Decimal("30"),
        billing_tier_1_rate=Decimal("22.50"),
        billing_tier_2_rate=Decimal("32.50"),
        billing_tier_3_rate=Decimal("45.00"),
        billing_tier_4_rate=Decimal("62.00"),
        billing_export_credit_fraction=Decimal("0.5"),
        billing_subsidy_discount_pct=Decimal("20.0"),
    )
    base.update(overrides)
    return BillingSettings(_env_file=None, **base)


def inputs(net, tier="TIER_4", subsidy=False, consumption=None, solar=Decimal("0")):
    """A `BillInputs` for a household whose net grid draw is `net`.

    `consumption` defaults to `net` because most cases here are about the billed
    quantity, which is derived from net grid draw rather than gross consumption.
    """
    net = to_money(net)
    return BillInputs(
        consumption_kwh=to_money(consumption if consumption is not None else net),
        solar_kwh=to_money(solar),
        net_grid_kwh=net,
        tariff_rate=Decimal("45.00"),
        billing_tier=tier,
        subsidy_flag=subsidy,
    )


# ---------------------------------------------------------------------------
# Block boundaries
#
# The block ladder's edges are where an off-by-one costs a customer money, so
# each boundary is pinned from both sides rather than sampled in the middle.
# ---------------------------------------------------------------------------


def test_zero_consumption_has_no_blocks():
    cfg = settings()
    assert split_into_blocks(Decimal("0"), "TIER_4", cfg) == ()

    total, breakdown = gross_cost_for(Decimal("0"), "TIER_4", cfg)
    assert total == Decimal("0.00")
    assert breakdown == ()


def test_negative_kwh_has_no_blocks():
    """Defensive: `compute_bill` clamps before calling, but the split must not
    invent a negative block if it is ever called directly."""
    assert split_into_blocks(Decimal("-5"), "TIER_4", settings()) == ()


def test_exactly_one_block_produces_one_entry():
    """30 kWh is ONE block, not two with an empty second.

    An off-by-one in the loop bound shows up here and nowhere else: a padded
    empty block costs nothing, so the total would still be right while the
    customer-facing breakdown had a spurious line.
    """
    cfg = settings()
    assert split_into_blocks(Decimal("30"), "TIER_4", cfg) == (
        ("TIER_1", Decimal("30")),
    )
    total, _ = gross_cost_for(Decimal("30"), "TIER_4", cfg)
    assert total == Decimal("675.00")  # 30 x 22.50


def test_just_under_the_first_boundary():
    cfg = settings()
    blocks = split_into_blocks(Decimal("29.999999"), "TIER_4", cfg)
    assert blocks == (("TIER_1", Decimal("29.999999")),)


def test_just_over_the_first_boundary():
    """The smallest representable step past 30 must open the second block."""
    cfg = settings()
    blocks = split_into_blocks(Decimal("30.000001"), "TIER_4", cfg)
    assert blocks == (
        ("TIER_1", Decimal("30")),
        ("TIER_2", Decimal("0.000001")),
    )


def test_exactly_sixty():
    cfg = settings()
    assert split_into_blocks(Decimal("60"), "TIER_4", cfg) == (
        ("TIER_1", Decimal("30")),
        ("TIER_2", Decimal("30")),
    )
    total, _ = gross_cost_for(Decimal("60"), "TIER_4", cfg)
    assert total == Decimal("1650.00")  # 675 + 975


def test_exactly_ninety():
    cfg = settings()
    total, breakdown = gross_cost_for(Decimal("90"), "TIER_4", cfg)
    assert len(breakdown) == 3
    assert total == Decimal("3000.00")  # 675 + 975 + 1350


def test_above_ninety_reaches_tier_4():
    cfg = settings()
    total, breakdown = gross_cost_for(Decimal("120"), "TIER_4", cfg)
    assert [tier for tier, _, _ in breakdown] == list(TIER_ORDER)
    assert breakdown[-1][1] == Decimal("30")
    assert total == Decimal("4860.00")  # 3000 + 30 x 62.00


def test_final_block_is_unbounded():
    """500 kWh must be FOUR blocks with a 410 kWh tail, not four blocks of 30.

    This is the test that catches a loop which iterates fixed-width blocks and
    stops at the top tier: it would silently drop 380 kWh and under-bill by
    roughly 23,000 LKR while every smaller case still passed.
    """
    cfg = settings()
    blocks = split_into_blocks(Decimal("500"), "TIER_4", cfg)

    assert len(blocks) == 4
    assert blocks[-1] == ("TIER_4", Decimal("410"))
    assert sum(kwh for _, kwh in blocks) == Decimal("500")

    total, _ = gross_cost_for(Decimal("500"), "TIER_4", cfg)
    # 675 + 975 + 1350 + 410 x 62.00
    assert total == Decimal("28420.00")


def test_all_kwh_are_always_allocated():
    """No kWh may be lost or invented by the split, at any magnitude or cap."""
    cfg = settings()
    for kwh in ["0.000001", "1", "29.999999", "30", "45", "60", "90", "120", "500"]:
        for tier in TIER_ORDER:
            blocks = split_into_blocks(Decimal(kwh), tier, cfg)
            assert sum(k for _, k in blocks) == Decimal(kwh), (kwh, tier)


# ---------------------------------------------------------------------------
# The tier cap
#
# `billing_tier` names the household's HIGHEST APPLICABLE block, so it is a
# ceiling on the ladder rather than a rate selector. These are the tests that
# distinguish that reading from a flat-rate one.
# ---------------------------------------------------------------------------


def test_cap_below_consumption_never_reaches_higher_rates():
    """A TIER_2 household drawing 120 kWh pays TIER_2 on everything past 30."""
    cfg = settings()
    blocks = split_into_blocks(Decimal("120"), "TIER_2", cfg)

    assert blocks == (
        ("TIER_1", Decimal("30")),
        ("TIER_2", Decimal("90")),
    )
    assert "TIER_3" not in [t for t, _ in blocks]
    assert "TIER_4" not in [t for t, _ in blocks]

    total, _ = gross_cost_for(Decimal("120"), "TIER_2", cfg)
    assert total == Decimal("3600.00")  # 675 + 90 x 32.50


def test_tier_1_cap_charges_everything_at_the_lifeline_rate():
    """The lifeline rate is protective: it must not erode at high consumption."""
    cfg = settings()
    blocks = split_into_blocks(Decimal("500"), "TIER_1", cfg)
    assert blocks == (("TIER_1", Decimal("500")),)

    total, _ = gross_cost_for(Decimal("500"), "TIER_1", cfg)
    assert total == Decimal("11250.00")  # 500 x 22.50


def test_cap_above_consumption_is_a_noop():
    """A cap the consumption never reaches must not change the answer."""
    cfg = settings()
    at_tier_3, _ = gross_cost_for(Decimal("75"), "TIER_3", cfg)
    at_tier_4, _ = gross_cost_for(Decimal("75"), "TIER_4", cfg)
    assert at_tier_3 == at_tier_4 == Decimal("2325.00")


def test_unknown_tier_raises_rather_than_defaulting():
    """Coercing an unknown tier would mis-bill silently in one direction or the
    other; both are worse than failing loudly. See `_cap_index`."""
    cfg = settings()
    with pytest.raises(ValueError, match="unknown billing_tier"):
        split_into_blocks(Decimal("50"), "TIER_9", cfg)

    with pytest.raises(ValueError, match="unknown billing_tier"):
        export_credit_for(Decimal("-10"), "PLATINUM", cfg)


# ---------------------------------------------------------------------------
# Subsidy, export credit, and their interaction
# ---------------------------------------------------------------------------


def test_subsidy_applies_to_gross_only():
    """The §3 worked example, end to end."""
    out = compute_bill(inputs(Decimal("75"), tier="TIER_3", subsidy=True), settings())

    assert out.gross_cost == Decimal("2325.00")
    assert out.subsidy_amount == Decimal("465.00")  # 20% of gross
    assert out.export_credit == Decimal("0.00")
    assert out.final_bill == Decimal("1860.00")
    assert out.effective_rate == Decimal("31.0000")
    assert out.tariff_missing is False


def test_effective_rate_differs_from_the_published_tariff_rate():
    """The block ladder's whole point, and a wart worth pinning.

    The feed publishes tariff_rate=45.00 for this household, but it pays 31.00.
    If this assertion ever starts failing because the two converged, the block
    structure has quietly been replaced by a flat rate.
    """
    bill_inputs = inputs(Decimal("75"), tier="TIER_3")
    out = compute_bill(bill_inputs, settings())
    assert bill_inputs.tariff_rate == Decimal("45.00")
    assert out.effective_rate == Decimal("31.0000")
    assert out.effective_rate < bill_inputs.tariff_rate


def test_no_subsidy_leaves_gross_intact():
    out = compute_bill(inputs(Decimal("75"), tier="TIER_3", subsidy=False), settings())
    assert out.subsidy_amount == Decimal("0.00")
    assert out.final_bill == out.gross_cost


def test_pure_export_produces_a_negative_bill():
    """A net exporter is owed money, and `final_bill` must be allowed to say so."""
    cfg = settings()
    out = compute_bill(
        inputs(Decimal("-20"), tier="TIER_1", consumption=Decimal("5"),
               solar=Decimal("25")),
        cfg,
    )

    assert out.gross_cost == Decimal("0.00")
    assert out.export_credit == Decimal("225.00")  # 20 x 22.50 x 0.5
    assert out.final_bill == Decimal("-225.00")
    assert out.effective_rate is None  # nothing was billable


def test_exact_balance_is_not_credited():
    """net == 0 means nothing was exported. Mirrors `is_exporting = net < 0`."""
    out = compute_bill(inputs(Decimal("0"), tier="TIER_1"), settings())
    assert out.gross_cost == Decimal("0.00")
    assert out.export_credit == Decimal("0.00")
    assert out.final_bill == Decimal("0.00")


def test_subsidy_does_not_reduce_the_export_credit():
    """The ordering test: subsidy applies to gross, never to the credit.

    If subsidy were applied to the net figure, a subsidised exporter would be
    credited 20% less than an unsubsidised one — i.e. penalised for holding a
    subsidy. This is the assertion that makes that bug impossible to reintroduce.
    """
    cfg = settings()
    args = dict(tier="TIER_1", consumption=Decimal("5"), solar=Decimal("25"))

    subsidised = compute_bill(inputs(Decimal("-20"), subsidy=True, **args), cfg)
    plain = compute_bill(inputs(Decimal("-20"), subsidy=False, **args), cfg)

    assert subsidised.export_credit == plain.export_credit == Decimal("225.00")
    assert subsidised.subsidy_amount == Decimal("0.00")  # gross was zero
    assert subsidised.final_bill == plain.final_bill == Decimal("-225.00")


def test_export_credit_uses_tier_1_even_under_a_higher_cap():
    """An exporter is in the lowest block by definition; see `export_credit_for`."""
    cfg = settings()
    at_tier_1 = export_credit_for(Decimal("-20"), "TIER_1", cfg)
    at_tier_4 = export_credit_for(Decimal("-20"), "TIER_4", cfg)

    assert at_tier_1 == at_tier_4 == Decimal("225.00")
    assert at_tier_4 != Decimal("620.00")  # NOT 20 x 62.00 x 0.5


def test_export_credit_fraction_is_configurable():
    """R7 depends on the credit tracking the tariff, so the lever must work."""
    half = export_credit_for(Decimal("-20"), "TIER_1", settings())
    quarter = export_credit_for(
        Decimal("-20"), "TIER_1",
        settings(billing_export_credit_fraction=Decimal("0.25")),
    )
    assert half == Decimal("225.00")
    assert quarter == Decimal("112.50")
    assert quarter * 2 == half


def test_zero_export_fraction_disables_the_credit():
    credit = export_credit_for(
        Decimal("-20"), "TIER_1",
        settings(billing_export_credit_fraction=Decimal("0")),
    )
    assert credit == Decimal("0.00")


def test_subsidy_percentage_is_configurable():
    gross = Decimal("1000.00")
    assert subsidy_for(gross, True, settings()) == Decimal("200.00")
    assert subsidy_for(
        gross, True, settings(billing_subsidy_discount_pct=Decimal("35"))
    ) == Decimal("350.00")
    assert subsidy_for(gross, False, settings()) == Decimal("0.00")


def test_block_width_is_configurable():
    """The width is config, not a constant baked into the loop."""
    cfg = settings(billing_block_kwh=Decimal("50"))
    assert split_into_blocks(Decimal("75"), "TIER_4", cfg) == (
        ("TIER_1", Decimal("50")),
        ("TIER_2", Decimal("25")),
    )


# ---------------------------------------------------------------------------
# Decimal discipline and the rounding rule
# ---------------------------------------------------------------------------


def test_rounding_is_half_up_not_bankers():
    """ROUND_HALF_UP specifically, pinned with values that actually discriminate.

    Python's Decimal default is ROUND_HALF_EVEN (banker's), which breaks an exact
    half towards the nearest EVEN digit. Most half-cent values round identically
    under both modes, so a test has to pick the ones where they differ:

        11.245 -> half-up 11.25, bankers 11.24   (4 is even, stays)
        11.265 -> half-up 11.27, bankers 11.26   (6 is even, stays)

    Both assertions below would fail under banker's rounding. A customer seeing
    one half-cent go up and the next go down has a legitimate complaint that the
    rounding rule is not a rule, which is why a utility bill does not use it.
    """
    # 0.5 kWh x 22.49 = 11.245 exactly.
    cfg_down = settings(billing_tier_1_rate=Decimal("22.49"))
    total_down, _ = gross_cost_for(Decimal("0.5"), "TIER_1", cfg_down)
    assert total_down == Decimal("11.25")

    # 0.5 kWh x 22.53 = 11.265 exactly.
    cfg_up = settings(billing_tier_1_rate=Decimal("22.53"))
    total_up, _ = gross_cost_for(Decimal("0.5"), "TIER_1", cfg_up)
    assert total_up == Decimal("11.27")


def test_gross_is_the_quantized_sum_not_the_sum_of_quantized_blocks():
    """Four blocks each with a half-cent tail must not each round up into the
    total. Summing rounded blocks would over-charge systematically."""
    # A rate with a third-decimal tail, so every block's exact cost ends in .xx5
    cfg = settings(
        billing_block_kwh=Decimal("1"),
        billing_tier_1_rate=Decimal("0.005"),
        billing_tier_2_rate=Decimal("0.005"),
        billing_tier_3_rate=Decimal("0.005"),
        billing_tier_4_rate=Decimal("0.005"),
    )
    total, breakdown = gross_cost_for(Decimal("4"), "TIER_4", cfg)

    # Exact: 4 x 0.005 = 0.020 -> 0.02
    assert total == Decimal("0.02")
    # Each block rounds up to 0.01 on its own, so the naive sum would be 0.04.
    assert sum(cost for _, _, cost in breakdown) == Decimal("0.04")
    assert total != sum(cost for _, _, cost in breakdown)


def test_to_money_converts_floats_via_str():
    """Decimal(0.1) is the 55-digit binary expansion; Decimal(str(0.1)) is 0.1.

    Every kWh entering this module crosses a float boundary exactly once, here.
    """
    assert to_money(0.1) == Decimal("0.1")
    assert to_money(0.1) != Decimal(0.1)
    assert to_money(22.50) == Decimal("22.5")
    assert to_money(Decimal("0.1")) == Decimal("0.1")
    assert to_money("0.1") == Decimal("0.1")
    assert to_money(3) == Decimal("3")


def test_every_money_field_is_a_decimal_and_not_a_float():
    out = compute_bill(inputs(Decimal("75"), tier="TIER_3", subsidy=True), settings())
    for name in (
        "gross_cost",
        "subsidy_amount",
        "export_credit",
        "final_bill",
        "effective_rate",
    ):
        value = getattr(out, name)
        assert isinstance(value, Decimal), name
        assert not isinstance(value, float), name

    for tier, kwh, cost in out.block_breakdown:
        assert isinstance(kwh, Decimal)
        assert isinstance(cost, Decimal)


def test_money_is_quantized_to_two_places():
    out = compute_bill(inputs(Decimal("75.123456"), tier="TIER_3"), settings())
    assert out.gross_cost.as_tuple().exponent == -2
    assert out.final_bill.as_tuple().exponent == -2


def test_float_inputs_do_not_leak_float_error():
    """Job C hands over Spark doubles; the result must still be exact."""
    from_float = compute_bill(
        BillInputs(
            consumption_kwh=to_money(75.0),
            solar_kwh=to_money(0.0),
            net_grid_kwh=to_money(75.0),
            tariff_rate=Decimal("45.00"),
            billing_tier="TIER_3",
            subsidy_flag=True,
        ),
        settings(),
    )
    assert from_float.gross_cost == Decimal("2325.00")
    assert from_float.final_bill == Decimal("1860.00")


def test_bill_is_reproducible():
    """The unit-level statement of the replay guarantee (§10's checkpoint).

    If this ever fails, no amount of Kafka replay machinery can make bills
    recompute identically — the non-determinism would be in the maths itself.
    """
    args = inputs(Decimal("75.123456"), tier="TIER_3", subsidy=True)
    first = compute_bill(args, settings())
    second = compute_bill(args, settings())
    assert first == second


# ---------------------------------------------------------------------------
# Missing tariff — §7's explicit availability-vs-correctness choice
# ---------------------------------------------------------------------------


def test_missing_tariff_rate_prices_nothing():
    out = compute_bill(
        BillInputs(
            consumption_kwh=Decimal("50"),
            solar_kwh=Decimal("0"),
            net_grid_kwh=Decimal("50"),
            tariff_rate=None,
            billing_tier="TIER_2",
        ),
        settings(),
    )
    assert out.tariff_missing is True
    assert out.gross_cost is None
    assert out.subsidy_amount is None
    assert out.export_credit is None
    assert out.final_bill is None
    assert out.effective_rate is None
    assert out.billing_tier_applied is None
    assert out.block_breakdown == ()


def test_missing_billing_tier_prices_nothing_even_with_a_rate():
    """A rate without a tier cannot be applied: the ladder has no ceiling."""
    out = compute_bill(
        BillInputs(
            consumption_kwh=Decimal("50"),
            solar_kwh=Decimal("0"),
            net_grid_kwh=Decimal("50"),
            tariff_rate=Decimal("32.50"),
            billing_tier=None,
        ),
        settings(),
    )
    assert out.tariff_missing is True
    assert out.final_bill is None


def test_missing_tariff_is_not_a_zero_bill():
    """The distinction `household_billing_running.tariff_missing` exists to keep.

    An un-priced household and a household that owes nothing must never be
    conflated — one is waiting on the D+1 feed, the other has been billed.
    """
    unpriced = compute_bill(
        BillInputs(
            consumption_kwh=Decimal("50"),
            solar_kwh=Decimal("0"),
            net_grid_kwh=Decimal("50"),
            tariff_rate=None,
            billing_tier=None,
        ),
        settings(),
    )
    genuinely_zero = compute_bill(inputs(Decimal("0"), tier="TIER_1"), settings())

    assert unpriced.final_bill is None
    assert unpriced.final_bill != Decimal("0.00")
    assert genuinely_zero.final_bill == Decimal("0.00")
    assert genuinely_zero.tariff_missing is False


def test_missing_tariff_does_not_raise():
    """It is the expected steady state on sim-day 0, not an error path (§14)."""
    compute_bill(
        BillInputs(
            consumption_kwh=Decimal("1"),
            solar_kwh=Decimal("0"),
            net_grid_kwh=Decimal("1"),
        ),
        settings(),
    )


# ---------------------------------------------------------------------------
# Consistency with the feed
# ---------------------------------------------------------------------------


def test_block_rates_match_the_tariff_simulator():
    """Guards the one piece of duplicated truth in the billing path.

    `simulators/tariff_simulator.py` publishes each household's `tariff_rate`
    from its own TARIFF_TIERS dict, while the bill is charged from
    BillingSettings. The simulator cannot import this module (its image does not
    ship `processing/`), so the duplication is structural — but a silent
    divergence would mean the feed advertising one rate while the bill charged
    another, with nothing to surface it. This test is that surface.
    """
    from simulators.tariff_simulator import TARIFF_TIERS

    rates = block_rates(settings())
    assert set(rates) == set(TARIFF_TIERS)
    for tier, rate in TARIFF_TIERS.items():
        assert rates[tier] == Decimal(str(rate)), tier


def test_tier_order_matches_the_rate_ladder():
    """The ladder must be ascending, or "cheapest kWh first" is a false claim."""
    rates = block_rates(settings())
    assert tuple(rates) == TIER_ORDER
    values = list(rates.values())
    assert values == sorted(values)
