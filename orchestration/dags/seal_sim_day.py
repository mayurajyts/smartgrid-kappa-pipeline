"""DAG: seal_sim_day — declare a simulated day complete (§7, Phase 5).

WHY THIS EXISTS IN THE ARCHITECTURE
-----------------------------------
§3 justifies Airflow on exactly this task: "the simulated day boundary is a
scheduling concern, not a streaming one: something must *seal* a sim-day".

A stream has no notion of a day being finished. Job C keeps a running total per
`(household_id, sim_date)` and keeps updating it for as long as readings for that
day keep arriving. Somebody has to decide that no more are coming, and that
decision is what turns a moving estimate into a billable fact.

This DAG makes that decision and records it in `sim_day_state`. Nothing else in
the system is allowed to issue an invoice for a day this DAG has not sealed —
`daily_billing_report` gates on it.

WHY IT ALSO RECORDS WHEN THE REFERENCE FEEDS ARRIVED
----------------------------------------------------
The tariff for day D arrives during day D+1 (§14). "Sealed" and "priceable" are
therefore different states, and conflating them would mean either billing
without a tariff (guessing a rate, which §7 forbids) or never billing at all.
Recording both timestamps separately lets the billing DAG wait for the second
one while the first has already happened.

SCHEDULE: every 2 real minutes, not once per simulated day.
One simulated day is 5 real minutes (§0), so a 5-minute schedule would drift in
and out of phase with the boundary and could miss a day entirely. Running more
often than needed is free because the work is idempotent — sealing an
already-sealed day is a no-op (see `upsert_sim_day`).
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

# The DAG files are mounted at /opt/airflow/dags; the project root is where
# `common/` and `orchestration/` live. Added explicitly because Airflow does not
# put the repository on the path for us.
sys.path.insert(0, "/opt/project")

import pendulum
from airflow.decorators import dag, task

from common.sim_clock import current_sim_date, sim_day_bounds
from orchestration.dags.common_tasks import (
    query,
    target_sim_date,
    upsert_sim_day,
    utcnow,
    write_audit,
    query_one,
)

DEFAULT_ARGS = {
    "owner": "smartgrid",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


@dag(
    dag_id="seal_sim_day",
    description="Mark a completed simulated day as sealed; record feed arrival.",
    default_args=DEFAULT_ARGS,
    # Every 2 real minutes. See the module docstring on why this is deliberately
    # more frequent than the 5-real-minute simulated day.
    schedule=timedelta(minutes=2),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    # Sealing is not parallelisable and a backlog of concurrent runs would all
    # try to seal the same day.
    max_active_runs=1,
    tags=["smartgrid", "phase5"],
)
def seal_sim_day():
    @task
    def open_current_day() -> str:
        """Record that the CURRENT simulated day exists and is in progress.

        Without this, a day only appears in `sim_day_state` once it is sealed, so
        the dashboard and `/api/v1/pipeline/status` could not show a day in
        progress — and "no row" would be indistinguishable from "no data".
        """
        today = current_sim_date()
        start, _ = sim_day_bounds(today)
        upsert_sim_day(today, opened_at=start, status="open")
        return today.isoformat()

    @task
    def seal_previous_day() -> dict:
        """Seal D-1 and record whether its reference feeds have landed.

        The feed arrival check reads the serving store rather than Kafka: the
        batch loader has already published the tariff, and Job C's join has
        already used it, so the observable fact is that priced rows exist. That
        is a stronger signal than "a message is on a topic" — it means the data
        actually reached the place a bill is computed from.
        """
        sim_date = target_sim_date()
        _, end = sim_day_bounds(sim_date)

        priced = query_one(
            """
            SELECT count(*) AS total,
                   count(running_cost) AS priced
            FROM household_billing_running
            WHERE sim_date = %s
            """,
            (sim_date,),
        ) or {"total": 0, "priced": 0}

        tariff_at = utcnow() if (priced.get("priced") or 0) > 0 else None

        upsert_sim_day(
            sim_date,
            opened_at=None,
            sealed_at=end,
            tariff_received_at=tariff_at,
            status="sealed",
        )

        # Seal any EARLIER day that has readings but was never sealed.
        #
        # Needed because this DAG only started running in Phase 5, while the
        # stream has been producing since Phase 2 -- so there is a backlog of
        # completed days with running totals and no lifecycle row. Without this
        # they could never be billed, and the demo would have exactly one
        # billable day however long the stack had been up.
        #
        # Every such day IS complete by definition: it is earlier than the one
        # just sealed, so no further readings for it can arrive. Sealing it is a
        # statement of fact, not a guess.
        backlog = query(
            """
            SELECT DISTINCT r.sim_date
            FROM household_billing_running r
            LEFT JOIN sim_day_state s ON s.sim_date = r.sim_date
            WHERE r.sim_date < %s
              AND (s.sim_date IS NULL OR s.sealed_at IS NULL)
            ORDER BY r.sim_date DESC
            LIMIT 10
            """,
            (sim_date,),
        )
        for row in backlog:
            day = row["sim_date"]
            _, day_end = sim_day_bounds(day)
            upsert_sim_day(day, sealed_at=day_end, status="sealed")

        return {
            "sim_date": sim_date.isoformat(),
            "households": priced.get("total") or 0,
            "priced": priced.get("priced") or 0,
        }

    @task
    def audit(result: dict, **context) -> dict:
        write_audit(
            run_id=context["run_id"],
            job_name="seal_sim_day",
            stage="orchestrate",
            sim_date=None,
            records_in=result["households"],
            records_out=result["priced"],
            status="success",
            notes=f"sealed {result['sim_date']}",
        )
        return result

    open_current_day() >> audit(seal_previous_day())


seal_sim_day()
