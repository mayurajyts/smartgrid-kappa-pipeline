"""Shared helpers for the four DAGs (§7, Phase 5).

WHY THIS EXISTS IN THE ARCHITECTURE
-----------------------------------
All four DAGs need the same three things: a Postgres connection to the serving
store, the simulated clock, and the sim_day_state lifecycle. Without a shared
module each DAG would carry its own copy, and the copies would drift — which is
the same single-source-of-truth argument that puts the billing maths in one file
and the contracts in `common/schemas.py`.

WHY RAW psycopg AND NOT AIRFLOW'S PostgresHook
----------------------------------------------
The Hook would work. It is not used because it reads its connection from
Airflow's own Connections store, which is configured through the UI or a CLI
call — state that lives in Airflow's metadata DB rather than in this repository.
That would mean `make up` produced a stack whose DAGs failed until someone
clicked through a form, which breaks reproducibility from a clean clone (§13).

Reading `POSTGRES_*` from the environment via `common/config.py` means the DAGs
use the same configuration as every other service and need no manual setup.

WHY THE DAGs COMPUTE THE SIMULATED DAY THEMSELVES
-------------------------------------------------
Airflow schedules on real time. The business day here is SIMULATED, running 288x
faster, so `logical_date` is unrelated to the `sim_date` a run is about. Every
DAG therefore derives its target day from `common/sim_clock.py` rather than from
the Airflow context — and takes the day BEFORE the current one, because the
current simulated day is still in progress and its totals are still moving.

This is worth stating plainly in the viva: Airflow here orchestrates jobs AROUND
the stream (§3). It is not a Lambda batch layer, and it never recomputes what the
stream already computed.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import psycopg

from common.config import get_settings
from common.sim_clock import current_sim_date


def pg_connect() -> psycopg.Connection:
    """Open a connection to the serving store."""
    return psycopg.connect(get_settings().postgres.dsn, autocommit=False)


def query(sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    from psycopg.rows import dict_row

    with pg_connect() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params)
            return cur.fetchall() if cur.description else []


def query_one(sql: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
    rows = query(sql, params)
    return rows[0] if rows else None


def execute(sql: str, params: tuple = ()) -> int:
    """Run a write statement in its own transaction. Returns rows affected."""
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            affected = cur.rowcount
        conn.commit()
    return affected


def target_sim_date() -> date:
    """The simulated day these DAGs operate on: the one BEFORE the current one.

    The current simulated day is still accumulating readings, so its totals are
    not final and an invoice issued against them would be wrong. Taking D-1 is
    what makes "seal the day, then bill it" meaningful.
    """
    return current_sim_date() - timedelta(days=1)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# sim_day_state lifecycle
# ---------------------------------------------------------------------------


def upsert_sim_day(sim_date: date, **fields: Any) -> None:
    """Idempotent insert-or-update of one day's lifecycle row.

    COALESCE on the timestamp columns so that a re-run never CLEARS a timestamp
    already set, and never moves one backwards. Sealing is a fact about the day,
    not about the run that noticed it — so the first run to seal a day records
    the time, and later runs leave it alone.
    """
    allowed = (
        "opened_at",
        "sealed_at",
        "tariff_received_at",
        "weather_received_at",
        "report_generated_at",
        "status",
    )
    sets = {k: v for k, v in fields.items() if k in allowed and v is not None}
    if not sets:
        return

    cols = list(sets)
    placeholders = ", ".join(["%s"] * len(cols))
    # `status` is overwritten (it is a current-state field); timestamps are
    # preserved if already present.
    updates = ", ".join(
        f"{c} = EXCLUDED.{c}" if c == "status"
        else f"{c} = COALESCE(sim_day_state.{c}, EXCLUDED.{c})"
        for c in cols
    )
    execute(
        f"""
        INSERT INTO sim_day_state (sim_date, {', '.join(cols)}, updated_at)
        VALUES (%s, {placeholders}, now())
        ON CONFLICT (sim_date) DO UPDATE
          SET {updates}, updated_at = now()
        """,
        (sim_date, *[sets[c] for c in cols]),
    )


def sim_day_row(sim_date: date) -> Optional[Dict[str, Any]]:
    return query_one("SELECT * FROM sim_day_state WHERE sim_date = %s", (sim_date,))


# ---------------------------------------------------------------------------
# Audit
# ---------------------------------------------------------------------------


def write_audit(
    run_id: str,
    job_name: str,
    stage: str,
    sim_date: Optional[date],
    records_in: int = 0,
    records_out: int = 0,
    records_rejected: int = 0,
    status: str = "success",
    notes: Optional[str] = None,
    started_at: Optional[datetime] = None,
) -> None:
    """Record one task's outcome in `pipeline_run_audit` (§6.5).

    Exists because "the pipeline must be diagnosable when it breaks, not just
    when it works" (§1) needs somewhere durable and QUERYABLE. Logs answer the
    same question but roll over and cannot be joined against the bills whose
    provenance they explain.

    Upserted on (run_id, job_name, stage) so an Airflow task retry updates its
    row rather than inserting a second one — a retried task is one attempt at one
    piece of work, not two pieces.
    """
    execute(
        """
        INSERT INTO pipeline_run_audit
            (run_id, job_name, stage, sim_date, records_in, records_out,
             records_rejected, started_at, finished_at, status, notes)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,now(),%s,%s)
        ON CONFLICT (run_id, job_name, stage) DO UPDATE
          SET records_in = EXCLUDED.records_in,
              records_out = EXCLUDED.records_out,
              records_rejected = EXCLUDED.records_rejected,
              finished_at = now(),
              status = EXCLUDED.status,
              notes = EXCLUDED.notes
        """,
        (
            run_id,
            job_name,
            stage,
            sim_date,
            records_in,
            records_out,
            records_rejected,
            started_at or utcnow(),
            status,
            notes,
        ),
    )
