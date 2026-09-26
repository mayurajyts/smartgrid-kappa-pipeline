"""Tests for deliberate fault injection.

WHY THIS IS THE MOST IMPORTANT SUITE IN PHASE 1
-----------------------------------------------
Fault injection is what makes Phase 2's DLQ, Phase 3's deduplication and the §11
step 7 correlation-id trace demo demonstrable. But an injected "fault" that fails
to actually corrupt the record is worse than no fault at all: the pipeline would
report a clean stream, the DLQ would be empty, and that would look exactly like a
working system. The bug would be invisible precisely where it matters.

So the central assertion in this file is the round trip through the real contract:

    a CLEAN payload validates against MeterReading
    a CORRUPTED payload does NOT

That is what proves each fault produces a record Job A's validation will reject,
rather than one that quietly passes. Testing against `common.schemas.MeterReading`
itself — not a copy of its rules — means these tests keep working if the contract
tightens, and start failing if a fault stops being a fault.

`TestReasonsMatchTheContract` then pins the reason strings, because a DLQ record
whose `rejection_reason` does not match what the injector recorded cannot be
traced back to its origin, and the trace demo is the deliverable.

The RNG is injected throughout, so faults fire deterministically instead of the
suite looping until probability obliges.
"""

from __future__ import annotations

import random
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from common.schemas import MeterReading
from simulators.fault_injection import (
    ALL_FAULTS,
    FAULT_DUPLICATE,
    FAULT_LATE_EVENT,
    NULLABLE_TARGET_FIELDS,
    REASON_NEGATIVE_KWH,
    REASON_NULL_FIELD,
    REASON_SOLAR_ABOVE_CAPACITY,
    REASON_TIMESTAMP_IN_FUTURE,
    FaultInjector,
    FaultSettings,
)

CAPACITY_KW = 4.0


def clean_payload() -> dict:
    """A valid reading, built and validated through the real contract.

    Going through MeterReading rather than hand-writing a dict means the baseline
    is known-good by the same definition the pipeline uses.
    """
    reading = MeterReading(
        event_id="11111111-2222-3333-4444-555555555555",
        meter_id="MTR-0042",
        household_id="HH-0042",
        grid_zone="ZONE-A",
        power_consumption_kwh=0.0412,
        solar_generation_kwh=0.0170,
        event_timestamp=datetime(2026, 1, 14, 9, 31, 4, tzinfo=timezone.utc),
        sim_date=datetime(2026, 1, 14, tzinfo=timezone.utc).date(),
        producer_emitted_at=datetime(2026, 9, 26, 8, 12, 44, tzinfo=timezone.utc),
        correlation_id="cid-abc-123",
    )
    return reading.model_dump(mode="json")


def only(fault_field: str, **overrides) -> FaultSettings:
    """Settings with every fault off except the named one, forced to 1.0."""
    disabled = {
        "fault_rate_null_field": 0.0,
        "fault_rate_negative_kwh": 0.0,
        "fault_rate_solar_spike": 0.0,
        "fault_rate_future_timestamp": 0.0,
        "fault_rate_duplicate": 0.0,
        "fault_rate_late_event": 0.0,
        "fault_rate_meter_dropout": 0.0,
    }
    disabled[fault_field] = 1.0
    disabled.update(overrides)
    return FaultSettings(**disabled)


def injector(fault_field: str, **overrides) -> FaultInjector:
    return FaultInjector(settings=only(fault_field, **overrides), rng=random.Random(7))


class TestCleanBaseline:
    def test_the_baseline_payload_is_valid(self):
        """If this fails, every corruption test below is meaningless."""
        MeterReading.model_validate(clean_payload())

    def test_injection_disabled_leaves_the_payload_untouched(self):
        """The master switch — used to isolate a real bug from an injected one."""
        inj = FaultInjector(
            settings=FaultSettings(fault_injection_enabled=False),
            rng=random.Random(1),
        )
        original = clean_payload()
        payload, duplicates, faults = inj.apply(dict(original), CAPACITY_KW)
        assert payload == original
        assert duplicates == []
        assert faults == []
        MeterReading.model_validate(payload)

    def test_all_rates_zero_leaves_the_payload_untouched(self):
        inj = FaultInjector(settings=only("fault_rate_null_field", **{
            "fault_rate_null_field": 0.0
        }), rng=random.Random(1))
        original = clean_payload()
        payload, duplicates, faults = inj.apply(dict(original), CAPACITY_KW)
        assert payload == original
        assert faults == []
        MeterReading.model_validate(payload)


class TestRejectionFaultsBreakTheContract:
    """Each of these must produce a record MeterReading REJECTS.

    This is the property that guarantees the fault reaches the DLQ.
    """

    def test_null_field_is_rejected_by_the_contract(self):
        payload, _, faults = injector("fault_rate_null_field").apply(
            clean_payload(), CAPACITY_KW
        )
        assert [f["reason"] for f in faults] == [REASON_NULL_FIELD]
        nulled = [k for k in NULLABLE_TARGET_FIELDS if payload[k] is None]
        assert len(nulled) == 1
        with pytest.raises(ValidationError):
            MeterReading.model_validate(payload)

    def test_negative_kwh_is_rejected_by_the_contract(self):
        payload, _, faults = injector("fault_rate_negative_kwh").apply(
            clean_payload(), CAPACITY_KW
        )
        assert [f["reason"] for f in faults] == [REASON_NEGATIVE_KWH]
        assert payload["power_consumption_kwh"] < 0
        with pytest.raises(ValidationError):
            MeterReading.model_validate(payload)

    def test_solar_spike_exceeds_panel_capacity(self):
        """The contract's ge=0 bound cannot catch this one — it is Job A's
        business rule, compared against the household's capacity — so the
        assertion is on the physical relationship instead."""
        payload, _, faults = injector("fault_rate_solar_spike").apply(
            clean_payload(), CAPACITY_KW
        )
        assert [f["reason"] for f in faults] == [REASON_SOLAR_ABOVE_CAPACITY]
        assert payload["solar_generation_kwh"] > CAPACITY_KW
        # Still schema-valid: this is a semantic violation, not a structural one,
        # which is exactly why Job A needs a capacity rule beyond the StructType.
        MeterReading.model_validate(payload)

    def test_solar_spike_fires_even_at_night(self):
        """At night generation is 0.0; a multiplicative spike would no-op and the
        fault would silently never appear in the DLQ."""
        night = clean_payload()
        night["solar_generation_kwh"] = 0.0
        payload, _, faults = injector("fault_rate_solar_spike").apply(night, CAPACITY_KW)
        assert faults[0]["reason"] == REASON_SOLAR_ABOVE_CAPACITY
        assert payload["solar_generation_kwh"] > CAPACITY_KW

    def test_solar_spike_fires_for_a_household_with_no_panels(self):
        """capacity 0 must still produce a detectably impossible value rather
        than 0 * 5 == 0."""
        payload, _, faults = injector("fault_rate_solar_spike").apply(
            clean_payload(), 0.0
        )
        assert faults[0]["reason"] == REASON_SOLAR_ABOVE_CAPACITY
        assert payload["solar_generation_kwh"] > 0.0

    def test_future_timestamp_moves_the_event_forward(self):
        original = clean_payload()
        payload, _, faults = injector("fault_rate_future_timestamp").apply(
            dict(original), CAPACITY_KW
        )
        assert [f["reason"] for f in faults] == [REASON_TIMESTAMP_IN_FUTURE]
        assert _parse(payload["event_timestamp"]) > _parse(original["event_timestamp"])
        # Structurally valid — Job A's rule compares against wall-clock time.
        MeterReading.model_validate(payload)


class TestStatefulFaults:
    """Faults producing VALID records that stress stateful processing."""

    def test_late_event_is_back_dated_beyond_the_watermark(self):
        """Must exceed Job A's 2-minute watermark, or the event is merely
        out-of-order — handled correctly, and therefore not a test of anything."""
        original = clean_payload()
        inj = injector("fault_rate_late_event", fault_late_event_sim_minutes=4)
        payload, _, faults = inj.apply(dict(original), CAPACITY_KW)

        assert [f["reason"] for f in faults] == [FAULT_LATE_EVENT]
        delta = _parse(original["event_timestamp"]) - _parse(payload["event_timestamp"])
        assert delta.total_seconds() == pytest.approx(4 * 60)
        assert delta.total_seconds() > 2 * 60, "must exceed the 2-minute watermark"
        MeterReading.model_validate(payload)

    def test_duplicate_repeats_the_same_event_id(self):
        """dropDuplicates keys on event_id, so the copy must share it."""
        payload, duplicates, faults = injector("fault_rate_duplicate").apply(
            clean_payload(), CAPACITY_KW
        )
        assert [f["reason"] for f in faults] == [FAULT_DUPLICATE]
        assert len(duplicates) == 1
        assert duplicates[0]["event_id"] == payload["event_id"]
        assert duplicates[0] == payload
        MeterReading.model_validate(duplicates[0])

    def test_duplicate_is_an_independent_copy(self):
        """Mutating the published record must not retroactively change the
        duplicate — they are published as two separate Kafka records."""
        payload, duplicates, _ = injector("fault_rate_duplicate").apply(
            clean_payload(), CAPACITY_KW
        )
        payload["power_consumption_kwh"] = 999.0
        assert duplicates[0]["power_consumption_kwh"] != 999.0


class TestMutualExclusion:
    def test_at_most_one_rejection_fault_per_record(self):
        """A record that is both null and negative gives the DLQ an ambiguous
        rejection_reason, breaking the trace demo's chain of evidence."""
        settings = FaultSettings(
            fault_rate_null_field=1.0,
            fault_rate_negative_kwh=1.0,
            fault_rate_solar_spike=1.0,
            fault_rate_future_timestamp=1.0,
            fault_rate_duplicate=0.0,
            fault_rate_late_event=0.0,
            fault_rate_meter_dropout=0.0,
        )
        inj = FaultInjector(settings=settings, rng=random.Random(3))
        _, _, faults = inj.apply(clean_payload(), CAPACITY_KW)
        rejection_reasons = {
            REASON_NULL_FIELD,
            REASON_NEGATIVE_KWH,
            REASON_SOLAR_ABOVE_CAPACITY,
            REASON_TIMESTAMP_IN_FUTURE,
        }
        applied = [f for f in faults if f["reason"] in rejection_reasons]
        assert len(applied) == 1

    def test_late_event_does_not_fight_the_future_timestamp_fault(self):
        """Both mutate event_timestamp; applying them together would leave the
        direction of the shift undefined."""
        settings = FaultSettings(
            fault_rate_null_field=0.0,
            fault_rate_negative_kwh=0.0,
            fault_rate_solar_spike=0.0,
            fault_rate_future_timestamp=1.0,
            fault_rate_late_event=1.0,
            fault_rate_duplicate=0.0,
            fault_rate_meter_dropout=0.0,
        )
        inj = FaultInjector(settings=settings, rng=random.Random(5))
        original = clean_payload()
        payload, _, faults = inj.apply(dict(original), CAPACITY_KW)
        reasons = [f["reason"] for f in faults]
        assert REASON_TIMESTAMP_IN_FUTURE in reasons
        assert FAULT_LATE_EVENT not in reasons
        assert _parse(payload["event_timestamp"]) > _parse(original["event_timestamp"])


class TestMeterDropout:
    def test_a_meter_is_not_silent_by_default(self):
        inj = injector("fault_rate_meter_dropout", fault_rate_meter_dropout=0.0)
        assert not inj.is_meter_silent("MTR-0042", _now())

    def test_dropout_starts_and_reports_its_duration(self):
        inj = injector("fault_rate_meter_dropout", fault_meter_dropout_real_seconds=90)
        assert inj.maybe_start_dropout("MTR-0042", _now()) == 90
        assert inj.is_meter_silent("MTR-0042", _now())
        assert inj.silent_meter_count == 1

    def test_dropout_expires_after_its_window(self):
        """It must recover: §11 step 6 restarts the simulator and shows the
        NoDataReceived alert clearing and consumer lag draining."""
        from datetime import timedelta

        inj = injector("fault_rate_meter_dropout", fault_meter_dropout_real_seconds=90)
        start = _now()
        inj.maybe_start_dropout("MTR-0042", start)
        assert inj.is_meter_silent("MTR-0042", start + timedelta(seconds=45))
        assert not inj.is_meter_silent("MTR-0042", start + timedelta(seconds=91))
        assert inj.silent_meter_count == 0

    def test_dropout_does_not_restart_while_already_silent(self):
        inj = injector("fault_rate_meter_dropout")
        assert inj.maybe_start_dropout("MTR-0042", _now()) is not None
        assert inj.maybe_start_dropout("MTR-0042", _now()) is None

    def test_dropout_affects_only_the_chosen_meter(self):
        inj = injector("fault_rate_meter_dropout")
        inj.maybe_start_dropout("MTR-0042", _now())
        assert not inj.is_meter_silent("MTR-0099", _now())

    def test_disabled_injection_never_drops_a_meter(self):
        inj = FaultInjector(
            settings=FaultSettings(fault_injection_enabled=False),
            rng=random.Random(1),
        )
        assert inj.maybe_start_dropout("MTR-0042", _now()) is None


class TestReasonsMatchTheContract:
    """The reason strings are a contract with Job A's DLQ and the metrics labels.

    A DLQ record whose rejection_reason does not match what the injector logged
    cannot be traced back to its origin — and that trace is the deliverable.
    """

    def test_reason_strings_are_stable(self):
        assert REASON_NULL_FIELD == "null_required_field"
        assert REASON_NEGATIVE_KWH == "negative_kwh"
        assert REASON_SOLAR_ABOVE_CAPACITY == "solar_above_capacity"
        assert REASON_TIMESTAMP_IN_FUTURE == "timestamp_in_future"

    def test_all_faults_are_enumerated(self):
        assert set(ALL_FAULTS) == {
            REASON_NULL_FIELD,
            REASON_NEGATIVE_KWH,
            REASON_SOLAR_ABOVE_CAPACITY,
            REASON_TIMESTAMP_IN_FUTURE,
            FAULT_DUPLICATE,
            FAULT_LATE_EVENT,
            "meter_dropout",
        }

    def test_every_fault_records_a_detail_for_the_log(self):
        """The detail is what makes a log line traceable to a specific mutation
        rather than just naming a category."""
        for field in (
            "fault_rate_null_field",
            "fault_rate_negative_kwh",
            "fault_rate_solar_spike",
            "fault_rate_future_timestamp",
            "fault_rate_late_event",
            "fault_rate_duplicate",
        ):
            _, _, faults = injector(field).apply(clean_payload(), CAPACITY_KW)
            assert faults, f"{field} produced no fault"
            for fault in faults:
                assert fault["detail"], f"{field} recorded no detail"


class TestRateValidation:
    def test_rates_outside_zero_to_one_are_rejected(self):
        with pytest.raises(ValidationError):
            FaultSettings(fault_rate_null_field=1.5)
        with pytest.raises(ValidationError):
            FaultSettings(fault_rate_duplicate=-0.1)

    def test_default_rejection_rates_stay_under_the_quality_gate(self):
        """Airflow fails the daily report if the reject rate exceeds 2% (§7).
        Defaults that breached it would make the pipeline fail its own gate by
        design, which would look like a bug rather than a deliberate choice."""
        s = FaultSettings()
        total = (
            s.fault_rate_null_field
            + s.fault_rate_negative_kwh
            + s.fault_rate_solar_spike
            + s.fault_rate_future_timestamp
        )
        assert total < 0.02

    def test_defaults_are_non_zero_so_the_dlq_is_never_empty(self):
        """A demo with an empty DLQ cannot show the trace walkthrough."""
        s = FaultSettings()
        assert s.fault_injection_enabled
        for rate in (
            s.fault_rate_null_field,
            s.fault_rate_negative_kwh,
            s.fault_rate_solar_spike,
            s.fault_rate_future_timestamp,
            s.fault_rate_duplicate,
            s.fault_rate_late_event,
            s.fault_rate_meter_dropout,
        ):
            assert rate > 0.0

    def test_late_event_default_exceeds_the_watermark(self):
        """4 simulated minutes against Job A's 2-minute watermark."""
        assert FaultSettings().fault_late_event_sim_minutes > 2


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _now() -> datetime:
    return datetime(2026, 9, 26, 12, 0, 0, tzinfo=timezone.utc)
