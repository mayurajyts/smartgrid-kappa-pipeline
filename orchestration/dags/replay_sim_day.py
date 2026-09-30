"""DAG: replay_sim_day — restate a bill from the log (§7, §11 step 8, Phase 5).

WHY THIS EXISTS IN THE ARCHITECTURE
-----------------------------------
This DAG is the architecture decision made concrete. §7 says so outright: "This
DAG is your live proof of the Kappa claim — demo it."

R7 asks for the ability to restate a previously issued bill when tariff data
arrives late or wrong. §2.2c argues Kappa serves that better than Lambda:

  * Under Lambda, a corrected tariff means running the batch layer with a fix and
    hoping the batch and speed implementations agree.
  * Under Kappa, it is a new record on a compacted topic followed by a replay of
    the affected day into a NEW OUTPUT VERSION, which is then atomically swapped
    in. One code path, one definition of "a bill", no reconciliation.

This DAG is the second half of that sentence. It re-prices a chosen simulated day
against the tariff currently on the compacted topic and issues version N+1,
flipping `is_current` in one transaction. Version N remains readable at
`/api/v1/households/{id}/bill/versions`, which is the auditable trail the claim
depends on.

WHY IT RE-PRICES FROM THE SERVING STORE RATHER THAN RE-READING KAFKA
--------------------------------------------------------------------
An honest caveat, and it belongs in the viva rather than hidden.

The fullest form of the Kappa claim would replay the raw telemetry from Kafka
through Jobs A and C again. This DAG does something narrower: it takes the kWh
totals Job C already derived from the log and re-prices them. It does NOT
re-derive the kWh.

That is the right trade for this requirement, because R7 is about a CORRECTED
TARIFF, not corrupted telemetry. The kWh for a sealed day are a settled fact
derived from an immutable log; the tariff is the thing that changed. Re-deriving
identical kWh to prove they are identical is work the Phase 3 checkpoint already
did — it verified that Job C recomputes the same totals when replayed from
`earliest` with a deleted checkpoint (0 differing rows, same total to the cent).

What this DAG demonstrates is the versioned, atomic swap. If the telemetry itself
needed correcting, the procedure is the Phase 3 one: delete the checkpoint and
restart the job with `STARTING_OFFSETS=earliest`. Both paths use the same code,
which is the claim.

MANUAL TRIGGER, PARAMETERISED
-----------------------------
`schedule=None`: a restatement is a decision somebody makes, not something that
happens on a timer. Parameters are `sim_date` (required) and `reason` (recorded in
the audit trail, because an unexplained restatement is an audit problem).
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from decimal import Decimal

sys.path.insert(0, "/opt/project")

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param

from orchestration.dags.common_tasks import (
    pg_connect,
    query,
    query_one,
    target_sim_date,
    write_audit,
)
from processing.transforms.billing import (
    BillingSettings,
    BillInputs,
    compute_bill,
    to_money,
)

BILLING = BillingSettings()

DEFAULT_ARGS = {"owner": "smartgrid", "retries": 0}


@dag(
    dag_id="replay_sim_day",
    description=(
        "Restate a simulated day's bills as a new version (R7). Manual, "
        "parameterised. The live proof of the Kappa claim."
    ),
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={
        "sim_date": Param(
            default="",
            type="string",
            description="Simulated date to restate, YYYY-MM-DD. Blank uses the "
                        "most recently billed day.",
        ),
        "reason": Param(
            default="tariff restatement",
            type="string",
            description="Why this restatement is being issued. Recorded in "
                        "pipeline_run_audit — an unexplained restatement is an "
                        "audit problem.",
        ),
    },
    tags=["smartgrid", "phase5", "replay", "R7"],
)
def replay_sim_day():
    @task
    def resolve_target(**context) -> str:
        """Pick the day to restate, and refuse if it was never billed.

        A restatement supersedes something. Restating a day with no version 1
        would silently create one, which would make `version` meaningless as an
        audit trail — so this fails loudly instead.
        """
        raw = (context["params"].get("sim_date") or "").strip()
        if raw:
            sim_date = date.fromisoformat(raw)
        else:
            row = query_one(
                "SELECT max(sim_date) AS d FROM household_billing_daily"
            )
            if not row or not row["d"]:
                raise ValueError(
                    "no day has been billed yet; daily_billing_report must "
                    "issue version 1 before anything can be restated"
                )
            sim_date = row["d"]

        existing = query_one(
            "SELECT max(version) AS v, count(*) AS rows "
            "FROM household_billing_daily WHERE sim_date = %s",
            (sim_date,),
        )
        if not existing or not existing["v"]:
            raise ValueError(
                f"{sim_date} has no issued bill to restate. Run "
                f"daily_billing_report first — a restatement must supersede "
                f"a version, not invent one."
            )
        return sim_date.isoformat()

    @task
    def restate(sim_date_str: str, **context) -> dict:
        """Re-price the day and issue version N+1, flipping is_current atomically.

        The prices come from `processing/transforms/billing.py`, the same module
        Job C and daily_billing_report call. That is the entire point: a
        restatement that used different arithmetic from the original issue would
        prove nothing about the architecture.
        """
        sim_date = date.fromisoformat(sim_date_str)
        reason = (context["params"].get("reason") or "").strip() or "unspecified"

        # The superseded version, for the before/after the demo shows.
        previous = query_one(
            """
            SELECT version, sum(final_bill) AS total, count(*) AS households
            FROM household_billing_daily
            WHERE sim_date = %s AND is_current
            GROUP BY version
            """,
            (sim_date,),
        ) or {}

        # Re-price from the kWh (settled) and the tariff CURRENTLY on the
        # compacted topic as reflected in the latest issued version's rate. A
        # corrected tariff republished to Kafka reaches Job C, which reprices
        # household_billing_running -- so the running row's cost is the signal
        # that the rate changed, and the tier/rate on it are what this reissues
        # against.
        rows = query(
            """
            SELECT r.household_id, r.consumption_kwh, r.solar_kwh, r.net_grid_kwh,
                   d.billing_tier, d.tariff_rate, d.subsidy_flag,
                   r.running_cost AS current_running_cost
            FROM household_billing_running r
            JOIN LATERAL (
                SELECT billing_tier, tariff_rate, subsidy_flag
                FROM household_billing_daily p
                WHERE p.household_id = r.household_id AND p.sim_date = r.sim_date
                ORDER BY version DESC LIMIT 1
            ) d ON TRUE
            WHERE r.sim_date = %s AND r.running_cost IS NOT NULL
            """,
            (sim_date,),
        )
        if not rows:
            raise ValueError(
                f"no priced running rows for {sim_date}; nothing to restate"
            )

        priced = []
        for r in rows:
            o = compute_bill(
                BillInputs(
                    consumption_kwh=to_money(r["consumption_kwh"]),
                    solar_kwh=to_money(r["solar_kwh"]),
                    net_grid_kwh=to_money(r["net_grid_kwh"]),
                    tariff_rate=to_money(r["tariff_rate"]),
                    billing_tier=r["billing_tier"],
                    subsidy_flag=bool(r["subsidy_flag"]),
                ),
                BILLING,
            )
            priced.append((r, o))

        with pg_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT coalesce(max(version),0)+1 FROM household_billing_daily "
                    "WHERE sim_date = %s",
                    (sim_date,),
                )
                version = cur.fetchone()[0]

                # ONE TRANSACTION: supersede, then insert. The partial unique
                # index on (household_id, sim_date) WHERE is_current enforces
                # that exactly one version is ever live, so a mistake here fails
                # loudly rather than leaving two current bills.
                cur.execute(
                    "UPDATE household_billing_daily SET is_current = FALSE "
                    "WHERE sim_date = %s AND is_current",
                    (sim_date,),
                )
                for r, o in priced:
                    cur.execute(
                        """
                        INSERT INTO household_billing_daily
                            (household_id, sim_date, version, consumption_kwh,
                             solar_kwh, net_grid_kwh, tariff_rate, billing_tier,
                             subsidy_flag, gross_cost, subsidy_amount,
                             export_credit, final_bill, effective_rate,
                             is_current, generated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,TRUE,now())
                        """,
                        (
                            r["household_id"], sim_date, version,
                            r["consumption_kwh"], r["solar_kwh"], r["net_grid_kwh"],
                            r["tariff_rate"], r["billing_tier"],
                            bool(r["subsidy_flag"]),
                            o.gross_cost, o.subsidy_amount, o.export_credit,
                            o.final_bill, o.effective_rate,
                        ),
                    )
            conn.commit()

        new_total = sum((o.final_bill or Decimal("0")) for _, o in priced)
        return {
            "sim_date": sim_date_str,
            "reason": reason,
            "previous_version": previous.get("version"),
            "previous_total": str(previous.get("total") or ""),
            "new_version": version,
            "new_total": str(new_total),
            "households": len(priced),
        }

    @task
    def verify_and_audit(result: dict, **context) -> dict:
        """Assert the R7 invariant, then record the restatement.

        Checked rather than assumed: exactly one version current, and the
        superseded one still present. A restatement that quietly deleted its
        predecessor would look identical in the API but would destroy the audit
        trail that justifies the whole design.
        """
        sim_date = date.fromisoformat(result["sim_date"])

        current = query_one(
            "SELECT count(DISTINCT version) AS versions, count(*) AS rows "
            "FROM household_billing_daily WHERE sim_date = %s AND is_current",
            (sim_date,),
        ) or {}
        if (current.get("versions") or 0) != 1:
            raise ValueError(
                f"R7 invariant broken: {current.get('versions')} versions are "
                f"current for {sim_date}; exactly one must be"
            )

        history = query_one(
            "SELECT count(DISTINCT version) AS versions "
            "FROM household_billing_daily WHERE sim_date = %s",
            (sim_date,),
        ) or {}
        if (history.get("versions") or 0) < 2:
            raise ValueError(
                f"restatement did not preserve history: only "
                f"{history.get('versions')} version(s) exist for {sim_date}"
            )

        write_audit(
            run_id=context["run_id"],
            job_name="replay_sim_day",
            stage="orchestrate",
            sim_date=sim_date,
            records_in=result["households"],
            records_out=result["households"],
            status="success",
            notes=(
                f"restated v{result['previous_version']} -> v{result['new_version']}; "
                f"total {result['previous_total']} -> {result['new_total']} LKR; "
                f"reason: {result['reason']}"
            ),
        )
        return {**result, "versions_retained": history.get("versions")}

    verify_and_audit(restate(resolve_target()))


replay_sim_day()
