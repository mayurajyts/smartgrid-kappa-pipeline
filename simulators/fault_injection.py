"""Deliberate data-quality faults — the test fixtures for Jobs A and D.

WHY THIS EXISTS IN THE ARCHITECTURE
-----------------------------------
This module is not decoration, and it is not a "chaos testing" flourish. It is the
only thing that makes three graded deliverables demonstrable:

  * §7 Job A's validation and DLQ routing. Rules that reject nulls, negative kWh,
    above-capacity solar and future timestamps cannot be shown to work unless
    something produces records that violate them. Without injected faults the DLQ
    is empty at demo time, and "my validation works" is an unsupported claim.
  * §7 Job A's watermarked deduplication. `dropDuplicates(["event_id"])` with a
    2-minute watermark is only interesting if duplicates and late events actually
    arrive.
  * §11 step 7, the correlation-id trace demo, which the observability criterion
    (§13, 10 marks) is explicitly worded around: "pick one rejected record from
    the DLQ and show its full path through the logs". That demo needs a rejected
    record to exist, and needs to be able to prove the rejection was expected.

Every injected fault is therefore LOGGED with its correlation_id and its reason.
In the viva, a DLQ record can be traced backwards to the log line where it was
deliberately created — which turns "a record failed validation" into "this exact
record was corrupted this way at this moment, and the pipeline caught it for the
right reason".

THE CONTRACT BOUNDARY (the load-bearing design decision here)
-------------------------------------------------------------
`common/schemas.py` deliberately makes `MeterReading` strict: `extra="forbid"` and
`ge=0` bounds. A corrupted reading therefore CANNOT be constructed through the
model — which is correct, because the model's job is to stop malformed data
entering the immutable log.

So the flow is:

    build MeterReading  ->  validate  ->  model_dump()  ->  corrupt the dict  ->  publish

Faults are applied to the *dict*, after validation, immediately before publish.
This has three properties worth defending:

  1. The contract remains the single source of truth for what a valid reading is.
     There is no second, permissive code path that could drift from it.
  2. Every corruption is one explicit, named mutation of one field. In the viva
     the exact bytes that differ from a valid record can be pointed at.
  3. Corrupted records are, by construction, records the contract rejects. The
     test suite asserts this directly (test_fault_injection.py): a clean payload
     validates, a corrupted one raises. That is what proves an injected fault will
     actually reach the DLQ rather than slipping through validation unnoticed —
     without it, a fault that failed to corrupt anything would look like a working
     pipeline.

TRADE-OFF (deliberate)
----------------------
Faults are independent Bernoulli draws per reading, at fixed rates. Real data
quality problems are correlated and bursty: a failing concentrator drops a whole
zone at once, a firmware bug corrupts one model of meter, a network partition
delivers a thousand late events together.

Independent draws were chosen because each rule can then be exercised in
isolation and its rejection reason checked without waiting for a burst to happen
to occur. The cost is that the pipeline is never tested against a correlated
failure — a stated limitation in the report, and the honest answer to "how would
this behave in production" is that bursty late arrival would stress the watermark
far harder than this does.

All rates are env-configurable and default to small but NON-ZERO values, so the
DLQ always has content at demo time without the reject rate breaching the 2% data
quality gate the Airflow DAG enforces (§7).
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Rejection reasons. These strings are the contract between this module and Job
# A's validation: the DLQ `rejection_reason` Job A writes should match the reason
# recorded here, which is what makes a rejection verifiable end to end rather
# than merely plausible. They are also the `reason` label on the
# `smartgrid_events_rejected_total` metric.
REASON_NULL_FIELD = "null_required_field"
REASON_NEGATIVE_KWH = "negative_kwh"
REASON_SOLAR_ABOVE_CAPACITY = "solar_above_capacity"
REASON_TIMESTAMP_IN_FUTURE = "timestamp_in_future"

# Non-rejection faults: these produce records that are individually VALID but
# stress the stream's stateful handling rather than its validation.
FAULT_DUPLICATE = "duplicate"
FAULT_LATE_EVENT = "late_event"
FAULT_METER_DROPOUT = "meter_dropout"

ALL_FAULTS = (
    REASON_NULL_FIELD,
    REASON_NEGATIVE_KWH,
    REASON_SOLAR_ABOVE_CAPACITY,
    REASON_TIMESTAMP_IN_FUTURE,
    FAULT_DUPLICATE,
    FAULT_LATE_EVENT,
    FAULT_METER_DROPOUT,
)

# Fields whose absence Job A must reject. Chosen because each one breaks a
# different downstream consumer: no household_id means the reading cannot be
# billed, no grid_zone means it cannot be aggregated or even partitioned
# correctly, and no event_timestamp means it cannot be windowed or watermarked.
NULLABLE_TARGET_FIELDS = ("household_id", "grid_zone", "event_timestamp")


class FaultSettings(BaseSettings):
    """Per-fault probabilities, all env-driven (§ config rule).

    Rates are per reading, applied independently. Defaults are deliberately
    small: the Airflow data-quality gate fails the daily report if the reject
    rate exceeds 2% (§7), so the four rejection faults must sum to comfortably
    less than that or the pipeline would fail its own quality check by design.
    Current defaults sum to 0.8%.
    """

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    fault_injection_enabled: bool = Field(
        default=True,
        description="Master switch. Disabling it produces a perfectly clean "
        "stream, which is useful for isolating a real bug from an injected one.",
    )

    # --- Faults that Job A must REJECT to the DLQ ---
    fault_rate_null_field: float = Field(default=0.002, ge=0.0, le=1.0)
    fault_rate_negative_kwh: float = Field(default=0.002, ge=0.0, le=1.0)
    fault_rate_solar_spike: float = Field(default=0.002, ge=0.0, le=1.0)
    fault_rate_future_timestamp: float = Field(default=0.002, ge=0.0, le=1.0)

    # --- Faults that produce VALID records but stress stateful processing ---
    # Higher rate than the rejection faults: duplicates are deduplicated rather
    # than rejected, so they do not count against the data-quality gate, and a
    # visible duplicate count makes the dedupe step's effect obvious in the logs.
    fault_rate_duplicate: float = Field(default=0.01, ge=0.0, le=1.0)
    fault_rate_late_event: float = Field(default=0.01, ge=0.0, le=1.0)

    # How far back a late event is stamped. MUST exceed Job A's 2-minute
    # watermark (§7 step 3) in SIMULATED time, or the event would arrive merely
    # out-of-order — correctly handled and therefore not a test of anything. The
    # point is to produce events the watermark actually drops.
    fault_late_event_sim_minutes: int = Field(default=4, gt=0)

    # How far into the future a bad timestamp is stamped, in simulated minutes.
    fault_future_timestamp_sim_minutes: int = Field(default=30, gt=0)

    # Meter dropout: a meter stops reporting entirely for a while. Drives the
    # METER_SILENT alert (§7 Job D) and the NoDataReceived Prometheus rule.
    # Expressed in REAL seconds because the alert rules are written against real
    # time ("no reading in the last N minutes"), so the two must agree.
    fault_rate_meter_dropout: float = Field(default=0.001, ge=0.0, le=1.0)
    fault_meter_dropout_real_seconds: int = Field(default=90, gt=0)


class FaultInjector:
    """Applies faults to validated reading dicts.

    Stateful in two narrow ways, both necessary:
      * `_dropped_until` tracks which meters are currently silent.
      * duplicates are returned as extra payloads for the caller to publish.

    Takes an injectable RNG so tests can force a fault deterministically instead
    of looping until probability obliges.
    """

    def __init__(
        self,
        settings: FaultSettings | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self.settings = settings or FaultSettings()
        self._rng = rng or random.Random()
        # meter_id -> real monotonic-ish deadline until which the meter is silent
        self._dropped_until: dict[str, datetime] = {}

    # -- dropout ------------------------------------------------------------

    def is_meter_silent(self, meter_id: str, real_now: datetime) -> bool:
        """Whether this meter is currently in a dropout window.

        Checked by the producer BEFORE generating a reading: a silent meter emits
        nothing at all, which is what distinguishes dropout from the other faults.
        The others corrupt a record; this one removes it, and only absence
        triggers the staleness detection that §8's `NoDataReceived` rule covers.
        """
        deadline = self._dropped_until.get(meter_id)
        if deadline is None:
            return False
        if real_now >= deadline:
            del self._dropped_until[meter_id]
            return False
        return True

    def maybe_start_dropout(self, meter_id: str, real_now: datetime) -> int | None:
        """Possibly begin a dropout for this meter; returns its duration if so."""
        if not self.settings.fault_injection_enabled:
            return None
        if meter_id in self._dropped_until:
            return None
        if self._rng.random() >= self.settings.fault_rate_meter_dropout:
            return None

        seconds = self.settings.fault_meter_dropout_real_seconds
        self._dropped_until[meter_id] = real_now + timedelta(seconds=seconds)
        return seconds

    @property
    def silent_meter_count(self) -> int:
        """Number of meters currently silent, reported at stage boundaries."""
        return len(self._dropped_until)

    # -- payload corruption -------------------------------------------------

    def apply(
        self, payload: dict[str, Any], solar_capacity_kw: float
    ) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, str]]]:
        """Apply faults to one validated payload.

        Args:
            payload: a dict from `MeterReading.model_dump(mode="json")` — already
                validated, so it is known-good before this method touches it.
            solar_capacity_kw: the household's panel capacity, needed to push
                solar generation above the physical cap by a meaningful margin.

        Returns:
            (payload, duplicates, faults)
              payload:    the possibly-corrupted primary record
              duplicates: extra copies to publish, same event_id (may be empty)
              faults:     one {"fault","reason","detail"} per injected fault, for
                          logging and metrics. Empty means the record is clean.

        At most ONE corrupting fault is applied per record. Stacking two would
        make the DLQ's single `rejection_reason` ambiguous: a record that is both
        negative and null tells us nothing about which rule caught it, and the
        trace demo depends on reason being unambiguous.
        """
        faults: list[dict[str, str]] = []
        duplicates: list[dict[str, Any]] = []

        if not self.settings.fault_injection_enabled:
            return payload, duplicates, faults

        s = self.settings

        # --- Corrupting faults: mutually exclusive, checked in a fixed order ---
        if self._rng.random() < s.fault_rate_null_field:
            field = self._rng.choice(NULLABLE_TARGET_FIELDS)
            # Set to None rather than deleting the key: this models a meter that
            # reported the field as empty, which is what a real feed does. It also
            # keeps the JSON shape stable, so Job A's explicit StructType parses
            # the record successfully and the VALIDATION rule rejects it — rather
            # than the parse failing and producing a less specific reason.
            payload[field] = None
            faults.append(
                {
                    "fault": REASON_NULL_FIELD,
                    "reason": REASON_NULL_FIELD,
                    "detail": f"field={field}",
                }
            )

        elif self._rng.random() < s.fault_rate_negative_kwh:
            original = payload["power_consumption_kwh"]
            payload["power_consumption_kwh"] = -abs(original)
            faults.append(
                {
                    "fault": REASON_NEGATIVE_KWH,
                    "reason": REASON_NEGATIVE_KWH,
                    "detail": f"was={original}",
                }
            )

        elif self._rng.random() < s.fault_rate_solar_spike:
            # Exceed the panel's physical capacity outright. Uses capacity (not
            # the current generation value) as the base so the fault fires even at
            # night, when generation is 0 and multiplying it would change nothing
            # — a fault that silently no-ops is worse than no fault.
            spiked = round(max(solar_capacity_kw, 1.0) * 5.0, 6)
            original = payload["solar_generation_kwh"]
            payload["solar_generation_kwh"] = spiked
            faults.append(
                {
                    "fault": REASON_SOLAR_ABOVE_CAPACITY,
                    "reason": REASON_SOLAR_ABOVE_CAPACITY,
                    "detail": f"was={original} now={spiked} capacity_kw={solar_capacity_kw}",
                }
            )

        elif self._rng.random() < s.fault_rate_future_timestamp:
            shifted = _shift_iso_timestamp(
                payload["event_timestamp"], s.fault_future_timestamp_sim_minutes
            )
            original = payload["event_timestamp"]
            payload["event_timestamp"] = shifted
            faults.append(
                {
                    "fault": REASON_TIMESTAMP_IN_FUTURE,
                    "reason": REASON_TIMESTAMP_IN_FUTURE,
                    "detail": f"was={original} now={shifted}",
                }
            )

        # --- Late event: VALID but back-dated past the watermark ---
        # Checked independently of the corrupting faults, because lateness is
        # orthogonal to validity: a late record is well-formed and should be
        # dropped by the watermark, not sent to the DLQ. Skipped if the timestamp
        # was already corrupted above, so the two do not fight over one field.
        if not any(f["fault"] == REASON_TIMESTAMP_IN_FUTURE for f in faults):
            if self._rng.random() < s.fault_rate_late_event:
                shifted = _shift_iso_timestamp(
                    payload["event_timestamp"], -s.fault_late_event_sim_minutes
                )
                payload["event_timestamp"] = shifted
                faults.append(
                    {
                        "fault": FAULT_LATE_EVENT,
                        "reason": FAULT_LATE_EVENT,
                        "detail": f"back_dated_sim_minutes={s.fault_late_event_sim_minutes}",
                    }
                )

        # --- Duplicate: republish the SAME event_id ---
        # Deliberately a copy of whatever the payload now is, including any
        # corruption: a duplicated bad record should be deduplicated first and
        # rejected once, not rejected twice. Dedupe runs before validation in
        # Job A, and this exercises that ordering.
        if self._rng.random() < s.fault_rate_duplicate:
            duplicates.append(dict(payload))
            faults.append(
                {
                    "fault": FAULT_DUPLICATE,
                    "reason": FAULT_DUPLICATE,
                    "detail": f"event_id={payload.get('event_id')}",
                }
            )

        return payload, duplicates, faults


def _shift_iso_timestamp(value: str, minutes: int) -> str:
    """Shift an ISO-8601 timestamp by a number of simulated minutes.

    Operates on the string form because by this point the payload is a JSON-mode
    dict, not a model. Normalises a trailing 'Z' to '+00:00' for `fromisoformat`,
    which does not accept 'Z' before Python 3.11, then re-emits in the same
    'Z'-suffixed form the contract uses so the corrupted record stays
    byte-comparable with a clean one.
    """
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    shifted = parsed + timedelta(minutes=minutes)
    return (
        shifted.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
