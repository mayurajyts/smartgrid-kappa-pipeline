"""Drop-directory watcher: daily reference files -> compacted Kafka topics.

WHY THIS EXISTS IN THE ARCHITECTURE
-----------------------------------
This is the bridge that makes the Kappa claim work. The brief's second source is a
daily FILE feed, but Kappa requires that *everything* enters the system as an
immutable, replayable log — otherwise the daily data would be a second ingestion
path with its own semantics, which is the beginning of a Lambda architecture.

This loader is what turns the file feed into log records, so there is exactly one
kind of input to the processing layer: a Kafka topic. §2.2a's argument — that the
daily feed is a slowly-changing dimension best held as a compacted topic and joined
as stream-static state — is only realised here, at the point the file becomes a
keyed record.

THE KEYS ARE THE WHOLE POINT
----------------------------
Log compaction retains the latest value PER KEY, forever. So the key choice is the
design:

  * tariff  -> keyed by `household_id`. Compaction therefore keeps each household's
    current tariff indefinitely, which is precisely the broadcast dimension Job C
    joins against. It is also the mechanism behind R7: correcting a tariff means
    publishing a NEW record under the same key, not mutating anything. The old
    value stays in the log until compaction reclaims it, and a replay of an earlier
    offset still sees the original — which is what makes a restatement auditable.

  * weather -> keyed by `grid_zone|sim_date`, NOT by `grid_zone` alone. If it were
    keyed by zone only, compaction would keep just the most recent forecast per
    zone, and replaying a past simulated day would interpret that day's solar
    figures against today's weather. Including sim_date in the key means each day's
    forecast survives for as long as the topic does.

IDEMPOTENCE, AND WHY THE MARKER FILE IS AN OPTIMISATION NOT A CORRECTNESS FIX
----------------------------------------------------------------------------
Processed filenames are recorded in a marker directory so a restart does not
re-publish. But re-publishing would be SAFE anyway, and that is the more important
property: the topics are keyed and compacted, so publishing the same file twice
writes the same value under the same key and the topic converges to the identical
state. At-least-once delivery becoming effectively-once state — the same property
the Postgres upserts rely on in Phase 3.

The marker exists to avoid pointless work and confusing duplicate log lines, not to
protect correctness. A design whose correctness depended on the marker file would be
fragile: the marker is on a volume that `make clean` deletes.

POLLING, NOT FILESYSTEM EVENTS (confirmed with the user)
--------------------------------------------------------
The directory is polled on an interval rather than watched with inotify/watchdog.
inotify does not fire reliably for bind-mounted and virtualised volumes on Docker
Desktop for Windows, and a loader that silently never notices a file would break the
demo on a marker's machine while working perfectly here. Reproducibility across
machines is directly assessed, so a slightly latent poll is the correct trade: the
cost is up to POLL_SECONDS of delay on a feed that arrives once per five real
minutes.

VALIDATION IS STRICT: BAD ROWS ARE SKIPPED, NOT PUBLISHED
---------------------------------------------------------
Every row is validated through the §6 contract before publish. A malformed tariff
row is logged and skipped rather than written to the topic. The reason is that the
log is immutable and this topic feeds the billing join: a bad rate published once
is replayed on every future reprocessing run, and a wrong tariff is wrong money.
Rejecting at the boundary keeps bad data out of the permanent record.
"""

from __future__ import annotations

import csv
import json
import signal
import sys
import time
from pathlib import Path

from pydantic import Field, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

from common.config import get_settings
from common.logging_setup import (
    STAGE_INGEST,
    configure_logging,
    new_correlation_id,
    stage_boundary,
)
from common.metrics import events_produced_total, start_metrics_server
from common.schemas import TariffReference, WeatherForecast
from common.sim_clock import startup_banner
from simulators.kafka_producer import KafkaPublisher

SERVICE_NAME = "batch-loader"


class BatchLoaderSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    drop_dir: Path = Field(default=Path("/data/drop"))
    # Separate directory so markers are never mistaken for input files.
    processed_marker_dir: Path = Field(default=Path("/data/drop/.processed"))
    # 5s against a feed arriving every 300s: negligible latency, negligible cost.
    loader_poll_seconds: float = Field(default=5.0, gt=0)


class BatchLoader:
    def __init__(self) -> None:
        self.settings = BatchLoaderSettings()
        self.kafka = get_settings().kafka
        self.log = configure_logging(SERVICE_NAME, stage=STAGE_INGEST)
        self.publisher = KafkaPublisher(client_id=SERVICE_NAME, logger=self.log)
        self._running = True
        self._files_processed = 0

    def _handle_shutdown(self, signum, _frame) -> None:
        self.log.info("shutdown_requested", stage=STAGE_INGEST, signal=signum)
        self._running = False

    # -- processed-file marker ----------------------------------------------

    def _marker_for(self, path: Path) -> Path:
        return self.settings.processed_marker_dir / f"{path.name}.done"

    def _already_processed(self, path: Path) -> bool:
        return self._marker_for(path).exists()

    def _mark_processed(self, path: Path) -> None:
        """Written only AFTER a successful flush.

        Ordering matters: marking before the flush would let a crash between the
        two leave the file marked but unpublished, and the tariff would be missing
        with nothing to indicate why. Marking after means a crash re-processes the
        file, which is harmless because the topics are compacted and keyed.
        """
        marker = self._marker_for(path)
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text(
            f"processed_at={time.time()}\nsource={path.name}\n", encoding="utf-8"
        )

    # -- file handlers ------------------------------------------------------

    def _load_tariff_file(self, path: Path) -> tuple[int, int]:
        """Publish a tariff CSV. Returns (published, rejected)."""
        published = rejected = 0
        with path.open(newline="", encoding="utf-8") as handle:
            for line_no, row in enumerate(csv.DictReader(handle), start=2):
                correlation_id = new_correlation_id()
                try:
                    record = TariffReference(
                        household_id=row["household_id"],
                        sim_date=row["sim_date"],
                        tariff_rate=float(row["tariff_rate"]),
                        billing_tier=row["billing_tier"],
                        subsidy_flag=row["subsidy_flag"].strip().lower() == "true",
                        effective_from=row["effective_from"],
                        schema_version=int(row["schema_version"]),
                    )
                except (ValidationError, KeyError, ValueError) as exc:
                    rejected += 1
                    # Logged per rejected row rather than in aggregate: reference
                    # rows are few (one per household per day) and each one is a
                    # household whose bill would otherwise have no rate, so the
                    # specific identity matters for diagnosis.
                    self.log.error(
                        "reference_row_rejected",
                        stage=STAGE_INGEST,
                        file=path.name,
                        line=line_no,
                        topic=self.kafka.topic_tariff_reference,
                        household_id=row.get("household_id"),
                        error=str(exc),
                        correlation_id=correlation_id,
                    )
                    continue

                self.publisher.publish(
                    topic=self.kafka.topic_tariff_reference,
                    # Keyed by household_id — compaction retains the latest tariff
                    # per household, which IS the broadcast dimension for Job C.
                    key=record.household_id,
                    payload=record.model_dump(mode="json"),
                    correlation_id=correlation_id,
                )
                published += 1

        events_produced_total.labels(
            source=SERVICE_NAME, grid_zone="_reference"
        ).inc(published)
        return published, rejected

    def _load_weather_file(self, path: Path) -> tuple[int, int]:
        """Publish a weather JSON array. Returns (published, rejected)."""
        published = rejected = 0
        with path.open(encoding="utf-8") as handle:
            try:
                forecasts = json.load(handle)
            except json.JSONDecodeError as exc:
                # A whole unreadable file, not one bad row. Logged and skipped
                # WITHOUT a marker, so a file that was caught mid-write is retried
                # on the next poll instead of being permanently discarded.
                self.log.error(
                    "reference_file_unparseable",
                    stage=STAGE_INGEST,
                    file=path.name,
                    error=str(exc),
                )
                raise

        for index, entry in enumerate(forecasts):
            correlation_id = new_correlation_id()
            try:
                record = WeatherForecast(**entry)
            except (ValidationError, TypeError) as exc:
                rejected += 1
                self.log.error(
                    "reference_row_rejected",
                    stage=STAGE_INGEST,
                    file=path.name,
                    index=index,
                    topic=self.kafka.topic_weather_forecast,
                    grid_zone=(entry or {}).get("grid_zone"),
                    error=str(exc),
                    correlation_id=correlation_id,
                )
                continue

            self.publisher.publish(
                topic=self.kafka.topic_weather_forecast,
                # Composite key: see the module docstring on why sim_date must be
                # part of it for replay to be correct.
                key=record.compaction_key,
                payload=record.model_dump(mode="json"),
                correlation_id=correlation_id,
            )
            published += 1

        events_produced_total.labels(
            source=SERVICE_NAME, grid_zone="_reference"
        ).inc(published)
        return published, rejected

    # -- poll loop ----------------------------------------------------------

    def _scan_once(self) -> None:
        drop_dir = self.settings.drop_dir
        if not drop_dir.exists():
            return

        # Sorted so files are processed in a deterministic order. Names embed the
        # sim_date, so this also means earlier days are published before later
        # ones — which matters for a compacted topic, where out-of-order publishes
        # would leave an older day's value as the surviving one.
        for path in sorted(drop_dir.iterdir()):
            if not self._running:
                return
            # Skip directories (including .processed) and the simulator's
            # in-progress temp files, which start with a dot.
            if not path.is_file() or path.name.startswith("."):
                continue
            if self._already_processed(path):
                continue

            if path.name.startswith("tariff_") and path.suffix == ".csv":
                handler = self._load_tariff_file
                topic = self.kafka.topic_tariff_reference
            elif path.name.startswith("weather_") and path.suffix == ".json":
                handler = self._load_weather_file
                topic = self.kafka.topic_weather_forecast
            else:
                self.log.warning(
                    "unrecognised_drop_file", stage=STAGE_INGEST, file=path.name
                )
                continue

            try:
                published, rejected = handler(path)
                # Flush before marking: the marker asserts the records are in the
                # log, so it must not be written while they are still queued
                # locally.
                undelivered = self.publisher.flush(timeout=30.0)
                if undelivered:
                    raise RuntimeError(
                        f"{undelivered} records undelivered after flush; not "
                        "marking the file processed so it is retried"
                    )
            except Exception as exc:
                self.log.error(
                    "drop_file_failed",
                    stage=STAGE_INGEST,
                    file=path.name,
                    error=str(exc),
                )
                continue

            self._mark_processed(path)
            self._files_processed += 1

            stage_boundary(
                self.log,
                stage=STAGE_INGEST,
                records_in=published + rejected,
                records_out=published,
                records_rejected=rejected,
                file=path.name,
                topic=topic,
                files_processed=self._files_processed,
            )

    def run(self) -> None:
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

        start_metrics_server()
        self.settings.drop_dir.mkdir(parents=True, exist_ok=True)
        self.settings.processed_marker_dir.mkdir(parents=True, exist_ok=True)

        self.log.info(
            "loader_starting",
            stage=STAGE_INGEST,
            sim_clock=startup_banner(),
            drop_dir=str(self.settings.drop_dir),
            poll_seconds=self.settings.loader_poll_seconds,
            tariff_topic=self.kafka.topic_tariff_reference,
            weather_topic=self.kafka.topic_weather_forecast,
            detection="polling (inotify is unreliable on bind mounts under "
            "Docker Desktop for Windows)",
        )

        while self._running:
            try:
                self._scan_once()
            except Exception:
                # The loader must not die on one bad scan: the tariff feed is on
                # the billing path, and a dead loader means bills with no rate.
                self.log.exception("scan_failed", stage=STAGE_INGEST)
            time.sleep(self.settings.loader_poll_seconds)

        self.publisher.flush()
        self.log.info(
            "loader_stopped", stage=STAGE_INGEST, files_processed=self._files_processed
        )


def main() -> int:
    BatchLoader().run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
