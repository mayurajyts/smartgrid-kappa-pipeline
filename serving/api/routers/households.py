"""Household billing endpoints — R5, R6 and the R7 evidence (§9).

WHY THE BILL ENDPOINT DISTINGUISHES ITS SOURCE
----------------------------------------------
`/households/{id}/bill` can answer from two tables, and says which one it used:

  * `household_billing_daily` — the ISSUED bill. Versioned, `is_current`-flagged,
    written by Airflow once a simulated day is sealed and priced (Phase 5).
  * `household_billing_running` — a LIVE ESTIMATE from Job C, which may have no
    tariff yet (the feed arrives at D+1, §14).

The `source` field is not decoration. A customer reading an estimate as an
invoice is exactly the failure the two-table split exists to prevent, and an API
that returned the same shape for both would undo that separation at the last
hop. `tariff_missing` is surfaced for the same reason.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from serving.api import repositories as repo

router = APIRouter(prefix="/api/v1", tags=["households"])


@router.get("/households/{household_id}/bill")
def household_bill(
    household_id: str,
    sim_date: Optional[date] = Query(
        default=None,
        description="Simulated date. Omit for the most recent day on record.",
    ),
):
    """The current bill or running estimate for a household (R5, R6)."""
    bill = repo.household_bill(household_id, sim_date)
    if not bill:
        raise HTTPException(
            status_code=404,
            detail=f"no billing record for household {household_id!r}"
            + (f" on {sim_date}" if sim_date else ""),
        )
    return bill


@router.get("/households/{household_id}/bill/versions")
def household_bill_versions(household_id: str):
    """Restatement history — the concrete evidence for R7 (§11 step 8).

    An empty list is a valid answer and NOT a 404: until Airflow has issued a
    bill (Phase 5) no household has versions, and the endpoint's job is to show
    the supersession chain once it exists. `count` is returned so a caller can
    tell "no versions yet" from "one version, never restated".
    """
    versions = repo.household_bill_versions(household_id)
    return {
        "household_id": household_id,
        "count": len(versions),
        "versions": versions,
    }


@router.get("/households/top")
def top_consumers(
    sim_date: Optional[date] = Query(default=None),
    limit: int = Query(default=10, ge=1, le=200),
):
    """Highest grid draw for a simulated day. Ranked by NET grid kWh, not gross
    consumption, so a household that self-consumed its own solar is not billed —
    or ranked — for energy it never drew."""
    return {"sim_date": sim_date, "households": repo.top_consumers(sim_date, limit)}


@router.get("/reports/daily/{sim_date}")
def daily_report(sim_date: date):
    """The consolidated daily payload (§9).

    Composed from the zone aggregates and the billing summary rather than
    recomputed: the report must agree with what the dashboard shows, and the only
    way to guarantee that is to read the same rows.
    """
    summary = repo.billing_summary(sim_date)
    if not summary:
        raise HTTPException(
            status_code=404, detail=f"no billing data for sim_date {sim_date}"
        )
    return {
        "sim_date": sim_date,
        "billing": summary,
        "top_consumers": repo.top_consumers(sim_date, 10),
        "zones": repo.latest_zone_load(),
    }
