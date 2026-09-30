"""Zone endpoints — the real-time answers to R1 and R2 (§9).

These are the seconds-latency half of the requirements table: what the control
room watches and what the Grafana business dashboard polls. §2.4 budgets the path
at under 10 seconds end to end, and every query here is a small indexed read so
that the budget is spent in the pipeline rather than in the API.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from serving.api import repositories as repo

router = APIRouter(prefix="/api/v1", tags=["zones"])


@router.get("/zones/load")
def zones_load():
    """Current load and renewable mix per zone (R1, R2).

    One row per zone, its most recent 1-minute-refresh window. Note the window is
    288 SIMULATED minutes wide — see serving/sql/001_schema.sql on why "1m" in
    `zone_load_1m` means one real minute of the observer's clock.
    """
    return {"zones": repo.latest_zone_load()}


@router.get("/zones/{grid_zone}/timeseries")
def zone_timeseries(
    grid_zone: str,
    # 60 windows is an hour of real time at one window per real minute, which is
    # what a dashboard panel shows without being truncated or unreadable.
    limit: int = Query(default=60, ge=1, le=1000),
):
    """The 1-minute series for one zone, oldest first, for charting."""
    rows = repo.zone_timeseries(grid_zone, limit)
    if not rows:
        # 404 rather than an empty list: a zone id that has never reported is
        # almost certainly a typo, and returning [] would make it look like a
        # healthy zone with no load.
        raise HTTPException(
            status_code=404,
            detail=f"no windows recorded for zone {grid_zone!r}",
        )
    return {"grid_zone": grid_zone, "windows": rows}


@router.get("/grid/summary")
def grid_summary():
    """System-wide load, renewable share and active meters.

    renewable_pct is recomputed from summed kWh rather than averaged across
    zones — see repositories.grid_summary for why averaging percentages would
    overstate it.
    """
    summary = repo.grid_summary()
    if not summary or summary.get("zone_count") == 0:
        raise HTTPException(
            status_code=503,
            detail="no zone aggregates yet; Job B has not written a window",
        )
    return summary
