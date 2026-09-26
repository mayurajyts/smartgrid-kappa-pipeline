"""Smart-meter telemetry source — the streaming input to the Kappa pipeline.

WHY THIS EXISTS IN THE ARCHITECTURE
-----------------------------------
This is the continuous, unbounded source that makes the system a streaming system.
Every downstream guarantee is defined relative to what this produces: the event-time
windows in Job B window the `event_timestamp` set here, the watermark is calibrated
against the lateness injected here, the billing aggregation sums the per-interval
kWh emitted here, and the replay demo re-reads exactly these records back out of
the log.

It is a *producer only*. It writes to Kafka and never to Postgres, never to the
object store, and never reads back. That one-directional flow is what makes the log
the single source of truth: there is no path by which processed state could leak
back into the source and make a replay non-reproducible.

PER-TICK STRUCTURE
------------------
Each tick emits at most one reading per meter:

    for each household:
        if the meter is in an injected dropout window -> emit nothing
        else: build MeterReading -> validate -> dump -> inject faults -> publish

Validation happens BEFORE fault injection, always. The contract is what defines a
valid reading, so the clean path must satisfy it; corruption is then applied
deliberately to the dumped dict. This ordering is what lets the test suite assert
that a corrupted payload genuinely fails `MeterReading` — see
`simulators/fault_injection.py` and `tests/unit/test_fault_injection.py`.

SIMULATED VS REAL TIME (the subtle part)
----------------------------------------
The loop sleeps in REAL seconds (`METER_TICK_REAL_SECONDS`, default 2), but each
reading covers an interval measured in SIMULATED seconds:

    sim_interval = real_tick x compression_ratio          (2s x 288 = 576 sim s)

Energy is power x time, so the kWh values must use the simulated interval. Using
the real 2-second tick would under-report every reading — and therefore every bill
— by the 288x compression factor. This is the single easiest arithmetic error to
make in the whole project, which is why the interval is computed in one place and
passed explicitly to both generators.

OBSERVABILITY (§8)
------------------
  * The sim-clock banner and the reference-population summary are logged at
    startup, so a run's logs record both the clock it used and the world it
    described — the evidence a replay is comparable to the original.
  * Counts are logged at TICK BOUNDARIES with in/out/rejected totals, never per
    record. At 200 meters every 2 seconds, per-record logging would produce more
    log volume than data.
  * `events_produced_total` and `last_event_timestamp_seconds` are updated per
    tick. The freshness gauge is what the `NoDataReceived` alert rule (§8 rule 1)
    fires on, and it is set per zone so a single silent zone is distinguishable
    from a fully stopped simulator.
"""

from __future__ import annotations

import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from common.config import get_settings
from common.logging_setup import (
    STAGE_INGEST,
    configure_logging,
    new_correlation_id,
    stage_boundary,
)
from common.metrics import (
    events_produced_total,
    last_event_timestamp_seconds,
    sim_day_current,
    start_metrics_server,
)
from common.schemas import MeterReading
from common.sim_clock import (
    compression_ratio,
    current_sim_date,
    current_sim_datetime,
    sim_day_index,
    startup_banner,
)
from simulators.fault_injection import FaultInjector, FaultSettings
from simulators.kafka_producer import KafkaPublisher
from simulators.load_profile import consumption_kwh
from simulators.reference_data import load_households, load_zones, reference_summary
from simulators.solar import solar_generation_kwh

SERVICE_NAME = "meter-simulator"


class MeterSimulatorSettings(BaseSettings):
    """Producer settings, env-driven."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # Real seconds between ticks. 2s x 288 compression = one reading per meter per
    # ~9.6 simulated minutes, giving ~150 readings per meter per simulated day —
    # enough to shape a recognisable daily curve without flooding the broker.
    meter_tick_real_seconds: float = Field(default=2.0, gt=0)

    # Caps the number of meters, for a lighter demo on a constrained machine.
    # 0 means "all households in the reference data".
    meter_limit: int = Field(default=0, ge=0)

    # Default weather assumption used until the weather topic has been consumed.
    # The producer does NOT read the weather topic: that would make the source
    # depend on its own downstream, and a source that reads back from the pipeline
    # cannot be replayed independently. Instead the clear-sky default is used, and
    # the weather forecast is applied downstream where it belongs. Documented in
    # the report as a simplification of §6.1's "scaled by the zone's weather
    # forecast cloud cover".
    default_solar_index: float = Field(default=0.85, ge=0.0, le=1.0)


class MeterSimulator:
    def __init__(self) -> None:
        self.settings = MeterSimulatorSettings()
        self.kafka = get_settings().kafka
        self.log = configure_logging(SERVICE_NAME, stage=STAGE_INGEST)
        self.faults = FaultInjector(FaultSettings())
        self.publisher = KafkaPublisher(
            client_id=SERVICE_NAME,
            logger=self.log,
            on_delivery_failure=lambda topic: None,
        )

        households = load_households()
        if self.settings.meter_limit:
            households = households[: self.settings.meter_limit]
        self.households = households
        self.zones = load_zones()

        # Simulated seconds covered by one tick. Computed once: see the module
        # docstring on why this must be simulated, not real, seconds.
        self.sim_interval_seconds = (
            self.settings.meter_tick_real_seconds * compression_ratio()
        )

        self._running = True
        self._total_published = 0
        self._total_faults = 0

    def _handle_shutdown(self, signum, _frame) -> None:
        """Stop cleanly so queued records are flushed rather than dropped.

        §11 step 6 kills and restarts this container to demonstrate the staleness
        alert firing and recovering. A clean stop means the restart shows recovery
        rather than a gap caused by lost records.
        """
        self.log.info("shutdown_requested", stage=STAGE_INGEST, signal=signum)
        self._running = False

    def _build_reading(
        self, household, sim_now: datetime, real_now: datetime, correlation_id: str
    ) -> MeterReading:
        """Build and validate one reading. Raises if the contract is violated."""
        consumption = consumption_kwh(
            meter_id=household.meter_id,
            sim_time=sim_now,
            base_load_kw=household.base_load_kw,
            interval_seconds=self.sim_interval_seconds,
        )
        solar = solar_generation_kwh(
            sim_time=sim_now,
            solar_capacity_kw=household.solar_capacity_kw,
            interval_seconds=self.sim_interval_seconds,
            solar_index=self.settings.default_solar_index,
        )
        return MeterReading(
            event_id=str(uuid.uuid4()),
            meter_id=household.meter_id,
            household_id=household.household_id,
            grid_zone=household.grid_zone,
            power_consumption_kwh=consumption,
            solar_generation_kwh=solar,
            event_timestamp=sim_now,
            sim_date=sim_now.date(),
            producer_emitted_at=real_now,
            correlation_id=correlation_id,
        )

    def _tick(self) -> tuple[int, int, int]:
        """Emit one reading per active meter. Returns (in, out, skipped)."""
        real_now = datetime.now(timezone.utc)
        sim_now = current_sim_datetime(real_now)

        attempted = 0
        published = 0
        skipped_silent = 0
        fault_counts: dict[str, int] = {}
        latest_by_zone: dict[str, float] = {}

        for household in self.households:
            attempted += 1

            # Dropout is checked FIRST and emits nothing at all. Unlike the other
            # faults, which corrupt a record, this one removes it — and only
            # absence exercises the staleness detection in §8's NoDataReceived
            # rule and Job D's METER_SILENT alert.
            if self.faults.is_meter_silent(household.meter_id, real_now):
                skipped_silent += 1
                continue
            dropout_seconds = self.faults.maybe_start_dropout(
                household.meter_id, real_now
            )
            if dropout_seconds is not None:
                self.log.warning(
                    "fault_injected",
                    stage=STAGE_INGEST,
                    fault="meter_dropout",
                    meter_id=household.meter_id,
                    grid_zone=household.grid_zone,
                    detail=f"silent_for_real_seconds={dropout_seconds}",
                )
                fault_counts["meter_dropout"] = fault_counts.get("meter_dropout", 0) + 1
                skipped_silent += 1
                continue

            correlation_id = new_correlation_id()
            reading = self._build_reading(household, sim_now, real_now, correlation_id)

            # Validated clean, now deliberately corruptible. See the module
            # docstring on why this ordering matters.
            payload = reading.model_dump(mode="json")
            payload, duplicates, faults = self.faults.apply(
                payload, household.solar_capacity_kw
            )

            for fault in faults:
                fault_counts[fault["fault"]] = fault_counts.get(fault["fault"], 0) + 1
                # Logged per injected fault, not per record: injected faults are
                # rare by design (well under 2%), so this stays low-volume while
                # giving the trace demo its starting point. The correlation_id
                # here is what links this line to the DLQ record Job A will write.
                self.log.warning(
                    "fault_injected",
                    stage=STAGE_INGEST,
                    fault=fault["fault"],
                    reason=fault["reason"],
                    detail=fault["detail"],
                    correlation_id=correlation_id,
                    event_id=payload.get("event_id"),
                    meter_id=household.meter_id,
                )

            for record in [payload, *duplicates]:
                self.publisher.publish(
                    topic=self.kafka.topic_meter_readings,
                    # Keyed by grid_zone for per-zone ordering. Falls back to the
                    # household id only when the null-field fault has removed the
                    # zone: a keyless publish would round-robin the record and
                    # break ordering for the whole partition, so a corrupted
                    # record must not be allowed to damage the clean ones.
                    key=payload.get("grid_zone") or household.grid_zone,
                    payload=record,
                    correlation_id=correlation_id,
                )
                published += 1

            events_produced_total.labels(
                source=SERVICE_NAME, grid_zone=household.grid_zone
            ).inc(1 + len(duplicates))
            latest_by_zone[household.grid_zone] = real_now.timestamp()

        # Freshness gauge per zone, driving the NoDataReceived alert rule.
        for zone, seen_at in latest_by_zone.items():
            last_event_timestamp_seconds.labels(grid_zone=zone).set(seen_at)
        sim_day_current.set(sim_day_index(real_now))

        # Serve delivery callbacks so failures surface promptly.
        self.publisher.poll(0.0)

        self._total_published += published
        self._total_faults += sum(fault_counts.values())

        stage_boundary(
            self.log,
            stage=STAGE_INGEST,
            records_in=attempted,
            records_out=published,
            records_rejected=0,  # this stage rejects nothing; Job A does that
            sim_date=sim_now.date().isoformat(),
            sim_time=sim_now.strftime("%H:%M:%S"),
            meters_silent=skipped_silent,
            faults_injected=sum(fault_counts.values()) or None,
            fault_breakdown=fault_counts or None,
            cumulative_published=self._total_published,
        )
        return attempted, published, skipped_silent

    def run(self) -> None:
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

        start_metrics_server()

        # §0 requires the simulated clock to be printed at startup. Logged
        # alongside the population summary so the logs record both the clock and
        # the world this run used.
        self.log.info(
            "simulator_starting",
            stage=STAGE_INGEST,
            sim_clock=startup_banner(),
            topic=self.kafka.topic_meter_readings,
            tick_real_seconds=self.settings.meter_tick_real_seconds,
            sim_interval_seconds=round(self.sim_interval_seconds, 1),
            meters=len(self.households),
            zones=len(self.zones),
            reference=reference_summary(),
            fault_injection_enabled=self.faults.settings.fault_injection_enabled,
        )

        tick = self.settings.meter_tick_real_seconds
        while self._running:
            started = time.monotonic()
            try:
                self._tick()
            except Exception:
                # A tick must never kill the source. An exception here is logged
                # with a traceback and the loop continues: a data source that dies
                # on one bad tick turns a transient problem into total data loss,
                # and the staleness alert would then report an outage that is
                # really a crash.
                self.log.exception("tick_failed", stage=STAGE_INGEST)

            # Sleep for the remainder of the tick, so the emission rate stays
            # constant regardless of how long generation took. A fixed sleep would
            # let the effective rate drift with machine load, and the simulated
            # clock — which is derived from real time — would then disagree with
            # the density of events on the log.
            elapsed = time.monotonic() - started
            remaining = tick - elapsed
            if remaining > 0:
                time.sleep(remaining)
            else:
                self.log.warning(
                    "tick_overran",
                    stage=STAGE_INGEST,
                    elapsed_seconds=round(elapsed, 3),
                    tick_seconds=tick,
                )

        undelivered = self.publisher.flush()
        self.log.info(
            "simulator_stopped",
            stage=STAGE_INGEST,
            sim_date=current_sim_date().isoformat(),
            total_published=self._total_published,
            total_faults_injected=self._total_faults,
            undelivered_on_exit=undelivered,
        )


def main() -> int:
    MeterSimulator().run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
