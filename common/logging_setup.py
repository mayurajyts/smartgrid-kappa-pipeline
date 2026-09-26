"""Structured JSON logging — the substrate for the correlation-id trace demo.

WHY THIS EXISTS IN THE ARCHITECTURE
-----------------------------------
The observability criterion (§13, 10 marks) is worded around *detecting and
diagnosing pipeline failures*, and the concrete demonstration of that (§8, §11
step 7) is: take one rejected record out of the DLQ, and follow its
correlation_id across ingestion, processing and storage. That demo is only
possible if every service emits machine-parseable lines carrying the same field
names. Free-text f-string logs cannot be grepped that way — "reading 42 was
rejected" in one service and "dropped event id=42" in another do not join.

So this module fixes the contract from §8. Every line carries:
    timestamp, level, service, job_name, stage, sim_date, correlation_id, event
plus whatever stage-specific counts the caller adds.

`service`, `job_name` and `stage` are bound once at configure time, so callers
cannot forget them and cannot spell them differently.

TRADE-OFFS (deliberate)
-----------------------
1. JSON to stdout, not to files or a log shipper. Docker captures stdout, so
   `docker compose logs` is the query interface and no service needs a volume,
   a rotation policy or a sidecar. The cost is that logs are unindexed — fine
   for `grep` over one demo run, and the report notes that production would ship
   these to Loki or Elasticsearch.
2. `sim_date` is captured per log call rather than bound once, because a
   long-lived streaming job crosses simulated day boundaries while running; a
   value bound at startup would be stale within 5 real minutes.
3. Logging happens at STAGE BOUNDARIES ONLY (§8), never per record inside a hot
   loop. At ~200 meters emitting every 2s, per-record logging would produce more
   log volume than data volume and would itself become the bottleneck. This is
   why `stage_boundary` reports aggregate counts (in/out/rejected).
"""

from __future__ import annotations

import logging
import sys
import uuid
from typing import Any

import structlog

from common.config import get_settings

# Pipeline stages from §8. A closed vocabulary, because the whole point is that
# a filter like `.stage == "process"` works identically across services.
STAGE_INGEST = "ingest"
STAGE_PROCESS = "process"
STAGE_STORE = "store"
STAGE_SERVE = "serve"
STAGE_ORCHESTRATE = "orchestrate"

VALID_STAGES = {
    STAGE_INGEST,
    STAGE_PROCESS,
    STAGE_STORE,
    STAGE_SERVE,
    STAGE_ORCHESTRATE,
}


def new_correlation_id() -> str:
    """Mint a correlation id.

    Generated once by the producer, carried on the Kafka record header, logged
    at every stage and stored on DLQ records — this is the thread the §11 step 7
    trace demo pulls on.
    """
    return str(uuid.uuid4())


def _add_sim_date(_logger: Any, _method: str, event_dict: dict) -> dict:
    """Stamp the current simulated date on every line unless the caller set one.

    A caller processing a *past* sim_date (a replay run, or a late-arriving
    event) passes its own; otherwise we record the clock's view.
    """
    if "sim_date" not in event_dict:
        # Imported lazily: sim_clock imports config, and binding this at module
        # import time would make logging setup depend on clock validity.
        from common.sim_clock import current_sim_date

        event_dict["sim_date"] = current_sim_date().isoformat()
    return event_dict


def configure_logging(
    service: str,
    job_name: str | None = None,
    stage: str | None = None,
) -> structlog.stdlib.BoundLogger:
    """Configure process-wide structured logging and return a bound logger.

    Args:
        service: container/process identity, e.g. "meter-simulator".
        job_name: the pipeline job, e.g. "job_a_clean_enrich". Defaults to
            `service` for single-purpose containers.
        stage: default pipeline stage for this process; individual calls may
            override it (an ingest service still logs a "store" line when it
            writes a checkpoint).
    """
    settings = get_settings().logging

    if stage is not None and stage not in VALID_STAGES:
        raise ValueError(f"stage must be one of {sorted(VALID_STAGES)}, got {stage!r}")

    # Route stdlib logging (used by kafka-python, psycopg, uvicorn) through the
    # same renderer, so third-party lines do not break JSON parsing of the stream.
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, settings.log_level),
        force=True,
    )

    renderer = (
        structlog.processors.JSONRenderer()
        if settings.log_format == "json"
        else structlog.dev.ConsoleRenderer()
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            # ISO-8601 UTC: sortable, unambiguous, and directly comparable
            # across containers when tracing one record end to end.
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            _add_sim_date,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, settings.log_level)
        ),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    bound = structlog.get_logger().bind(
        service=service,
        job_name=job_name or service,
    )
    if stage is not None:
        bound = bound.bind(stage=stage)
    return bound


def bind_correlation_id(correlation_id: str) -> None:
    """Attach a correlation id to every subsequent log line on this task.

    Uses contextvars, so an async FastAPI request handler or a per-record
    processing scope carries its own id without threading it through every
    function signature.
    """
    structlog.contextvars.bind_contextvars(correlation_id=correlation_id)


def clear_correlation_id() -> None:
    """Drop the bound correlation id at the end of a processing scope, so the
    next record does not inherit the previous one's identity."""
    structlog.contextvars.unbind_contextvars("correlation_id")


def stage_boundary(
    logger: structlog.stdlib.BoundLogger,
    stage: str,
    records_in: int,
    records_out: int,
    records_rejected: int = 0,
    **extra: Any,
) -> None:
    """Log the mandatory in/out/rejected counts at a stage boundary (§8).

    Called once per micro-batch or per processing pass — never per record.
    These counts are the log-side counterpart of the Prometheus counters and of
    the `pipeline_run_audit` table, giving three independent views of the same
    numbers for the viva.
    """
    if stage not in VALID_STAGES:
        raise ValueError(f"stage must be one of {sorted(VALID_STAGES)}, got {stage!r}")

    logger.info(
        "stage_boundary",
        stage=stage,
        records_in=records_in,
        records_out=records_out,
        records_rejected=records_rejected,
        # Surfaced explicitly so the "is anything being dropped" question is
        # answerable from a single log line without arithmetic.
        records_dropped=records_in - records_out - records_rejected,
        **extra,
    )
