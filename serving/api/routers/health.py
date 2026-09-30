"""Health, alerts and pipeline status (§9).

WHY /health SEPARATES "THE API IS UP" FROM "THE PIPELINE IS UP"
---------------------------------------------------------------
These are different questions and conflating them is how a dead pipeline behind
a healthy API goes unnoticed for an hour.

`/health` reports the API process and its one hard dependency (Postgres). It
returns 200 whenever the API can serve, even if no data is arriving — because
that IS the truth about the API, and a container orchestrator restarting the API
would not fix a stopped simulator.

Data freshness lives in `/api/v1/pipeline/status` instead, which is what the
demo and the Prometheus staleness rule (§8) look at.
"""

from __future__ import annotations

from fastapi import APIRouter, Query, Response

from common.sim_clock import current_sim_date, current_sim_datetime, sim_day_index
from serving.api import repositories as repo

router = APIRouter(tags=["ops"])


@router.get("/health")
def health(response: Response):
    """Liveness plus downstream dependency status.

    503 when Postgres is unreachable: the API genuinely cannot serve any endpoint
    without it, so reporting 200 would be a lie that a load balancer would act on.
    """
    db_ok = repo.ping()
    if not db_ok:
        response.status_code = 503
    return {
        "status": "ok" if db_ok else "degraded",
        "dependencies": {"postgres": "up" if db_ok else "down"},
        # The simulated clock, on every health check, because almost every
        # confusing observation in this system traces back to simulated time
        # being somewhere other than where the reader assumed.
        "sim_date": current_sim_date().isoformat(),
        "sim_datetime": current_sim_datetime().isoformat(),
        "sim_day_index": sim_day_index(),
    }


@router.get("/api/v1/alerts")
def alerts(active: bool = Query(default=True)):
    """Current alerts (R3, R4).

    Returns an empty list with a note until Job D exists (Phase 6). The endpoint
    is part of the §9 contract, so it is present and correct now rather than
    404ing — but it does not pretend to have data it cannot have.
    """
    present = repo.alerts_table_exists()
    return {
        "active_only": active,
        "alerts": repo.active_alerts(active),
        "note": None if present else "alerts table not created yet (Phase 6)",
    }


@router.get("/api/v1/pipeline/status")
def pipeline_status():
    """Sim-day state and serving-store freshness.

    The one endpoint to check when something looks wrong: it shows what the clock
    thinks, when each table was last written, and how many rows exist.
    """
    return {
        "sim_date": current_sim_date().isoformat(),
        "sim_datetime": current_sim_datetime().isoformat(),
        "sim_day_index": sim_day_index(),
        "freshness": repo.pipeline_freshness(),
    }
