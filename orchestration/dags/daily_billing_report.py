"""DAG: daily_billing_report — issue the bill for a sealed day (§7, Phase 5).

WHY THIS EXISTS IN THE ARCHITECTURE
-----------------------------------
This is where a running estimate becomes an invoice, and it is the only place in
the system allowed to write `household_billing_daily`.

Job C maintains `household_billing_running`, a live estimate that is deliberately
allowed to be incomplete — during a day in progress it carries kWh with no
tariff at all (§7's availability-vs-correctness choice). This DAG takes a day
that `seal_sim_day` has declared finished, checks it passed the quality gates,
prices it, and writes a VERSIONED row.

THE VERSIONING IS THE POINT (R7, and the payoff of the Kappa argument)
----------------------------------------------------------------------
Issuing a bill never updates a previous one. It inserts version N+1 and flips
`is_current` in ONE transaction. Version N stays, so "what did we invoice, and
what did we correct it to" is answerable from the table itself rather than from a
log or a backup.

That is what §2.2c claims Kappa buys over Lambda: a corrected tariff is a new
record on a compacted topic plus a replay into a new version, with one definition
of what a bill is throughout. `replay_sim_day` is the other half of that
demonstration; this DAG is the half that creates version 1.

WHY IT RE-PRICES RATHER THAN COPYING running_cost
-------------------------------------------------
It would be less code to copy `running_cost` across. It is not done, because the
running figure was computed by Job C against whatever tariff was on the topic at
the time, and the whole reason the issued bill is separate is that it must be
priced against the tariff that is authoritative for the sealed day.

Crucially, it re-prices by calling `processing/transforms/billing.py` — the SAME
module Job C calls. Not a reimplementation. §2.2d rejects Lambda specifically
because maintaining the tiered-tariff logic twice invites drift between the two;
writing a second copy here would commit exactly that error inside a Kappa system,
which would be worse than Lambda because nobody would expect it.

WHY THE REPORT IS CSV PLUS HTML
-------------------------------
§7 asks for "CSV + a PDF/HTML summary". HTML needs no extra dependency in this
image, opens in a browser during the demo, and prints to PDF from the browser if
a PDF is wanted for the report appendix. The CSV is the machine-readable
artifact.
"""

from __future__ import annotations

import csv
import html
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, "/opt/project")

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from common.metrics import daily_report_success_total
from orchestration.dags.common_tasks import (
    execute,
    pg_connect,
    query,
    query_one,
    sim_day_row,
    target_sim_date,
    upsert_sim_day,
    utcnow,
    write_audit,
)
from processing.transforms.billing import (
    BillingSettings,
    BillInputs,
    compute_bill,
    to_money,
)


class ReportSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    report_dir: Path = Field(default=Path("/data/reports"))


SETTINGS = ReportSettings()
BILLING = BillingSettings()

DEFAULT_ARGS = {
    "owner": "smartgrid",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


@dag(
    dag_id="daily_billing_report",
    description="Price a sealed simulated day into household_billing_daily and render the report.",
    default_args=DEFAULT_ARGS,
    # Every 5 real minutes = once per simulated day (§0). Slightly out of phase
    # with the boundary is fine: the DAG skips a day it has already reported and
    # skips one that is not yet sealed, so running early or late is harmless.
    schedule=timedelta(minutes=5),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["smartgrid", "phase5", "billing"],
)
def daily_billing_report():
    @task
    def wait_for_sealed_and_priced() -> str:
        """The sensor (§7 step 1): the day must be sealed AND have a tariff.

        Implemented as a task that SKIPS rather than an Airflow sensor that
        polls. A sensor would occupy a worker slot waiting, and on this
        memory-constrained host there is one slot; skipping and letting the next
        5-minute run try again costs nothing and cannot deadlock.

        Skips rather than fails, because "the day is not ready yet" is the normal
        state for most runs — the tariff for day D arrives during D+1 (§14). A
        failure here would make a red DAG the steady state and train the operator
        to ignore it.
        """
        # The NEWEST day that is billable, rather than assuming D-1.
        #
        # D-1 is the day that has just closed, and on a healthy pipeline it is
        # the right answer. But it is not the only answer, and assuming it makes
        # the DAG brittle in two real situations:
        #
        #   * Job C is lagging, so the newest day with running totals is older
        #     than D-1. The readings for D-1 are still in Kafka; they just have
        #     not been aggregated yet.
        #   * The stream was restarted, so a stretch of days has running totals
        #     but no persisted tariff (the columns were added in Phase 5).
        #
        # In both cases D-1 has nothing to bill while an earlier day is fully
        # ready, and a DAG that skipped forever would never issue a bill at all.
        # Selecting the newest READY day instead means the pipeline catches up
        # one simulated day per run, which is the behaviour a backlog needs.
        #
        # "Ready" is: sealed, has priced rows WITH a persisted tariff, and not
        # already reported.
        candidate = query_one(
            """
            SELECT r.sim_date
            FROM household_billing_running r
            JOIN sim_day_state s ON s.sim_date = r.sim_date
            WHERE s.sealed_at IS NOT NULL
              AND s.report_generated_at IS NULL
              AND r.running_cost IS NOT NULL
              AND r.tariff_rate IS NOT NULL
              AND r.billing_tier IS NOT NULL
            GROUP BY r.sim_date
            ORDER BY r.sim_date DESC
            LIMIT 1
            """
        )

        if not candidate:
            # A skip, not a failure: for most runs this is the normal state.
            # The tariff for day D arrives during D+1 (§14), so a day that has
            # just closed legitimately has nothing to bill yet. Failing here
            # would make a red DAG the steady state and train the operator to
            # ignore it.
            raise AirflowSkipException(
                "no sealed, priced, unreported simulated day is ready to bill. "
                "Normal when the current day has just closed and its tariff has "
                "not yet landed; check `make bills` if it persists."
            )

        return candidate["sim_date"].isoformat()

    @task
    def issue_bills(sim_date_str: str) -> dict:
        """Materialise version N+1 and flip `is_current`, in one transaction.

        THE TRANSACTION BOUNDARY IS THE WHOLE POINT. Clearing the old
        `is_current` and inserting the new version must be atomic, or there is a
        window in which either two versions are current or none is. The partial
        unique index on (household_id, sim_date) WHERE is_current would reject
        the first case outright — which is why that index exists rather than
        trusting this code to be careful.
        """
        sim_date = date.fromisoformat(sim_date_str)

        # Job C persists the tariff it joined, so the issued bill is priced
        # against the rate that was authoritative for the day rather than a tier
        # inferred from consumption. Rows whose tariff never arrived are excluded
        # by the running_cost IS NOT NULL predicate -- an unpriced household has
        # nothing to invoice.
        rows = query(
            """
            SELECT household_id, sim_date, consumption_kwh, solar_kwh,
                   net_grid_kwh, tariff_rate, billing_tier, subsidy_flag
            FROM household_billing_running
            WHERE sim_date = %s
              AND running_cost IS NOT NULL
              AND tariff_rate IS NOT NULL
              AND billing_tier IS NOT NULL
            """,
            (sim_date,),
        )
        if not rows:
            raise ValueError(
                f"no priced rows with a persisted tariff for {sim_date}. If Job C "
                f"predates the tariff_rate/billing_tier columns, restart it so the "
                f"next batch populates them."
            )

        priced_rows = []
        for r in rows:
            outputs = compute_bill(
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
            priced_rows.append(
                (r, r["billing_tier"], r["tariff_rate"],
                 bool(r["subsidy_flag"]), outputs)
            )

        with pg_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT coalesce(max(version), 0) + 1 FROM household_billing_daily "
                    "WHERE sim_date = %s",
                    (sim_date,),
                )
                version = cur.fetchone()[0]

                # Supersede whatever was current for this day, then insert the
                # new version. Same transaction, so the invariant never breaks.
                cur.execute(
                    "UPDATE household_billing_daily SET is_current = FALSE "
                    "WHERE sim_date = %s AND is_current",
                    (sim_date,),
                )

                for r, tier, rate, subsidy, o in priced_rows:
                    cur.execute(
                        """
                        INSERT INTO household_billing_daily
                            (household_id, sim_date, version, consumption_kwh,
                             solar_kwh, net_grid_kwh, tariff_rate, billing_tier,
                             subsidy_flag, gross_cost, subsidy_amount,
                             export_credit, final_bill, effective_rate,
                             is_current, generated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,TRUE,now())
                        ON CONFLICT (household_id, sim_date, version)
                        DO UPDATE SET final_bill = EXCLUDED.final_bill,
                                      is_current = TRUE
                        """,
                        (
                            r["household_id"], sim_date, version,
                            r["consumption_kwh"], r["solar_kwh"], r["net_grid_kwh"],
                            rate, tier, bool(subsidy),
                            o.gross_cost, o.subsidy_amount, o.export_credit,
                            o.final_bill, o.effective_rate,
                        ),
                    )
            conn.commit()

        total = sum((o.final_bill or Decimal("0")) for *_, o in priced_rows)
        return {
            "sim_date": sim_date_str,
            "version": version,
            "households": len(priced_rows),
            "total_billed": str(total),
        }

    @task
    def render_report(issued: dict) -> dict:
        """Write the CSV and HTML artifacts (§7 step 4)."""
        sim_date = issued["sim_date"]
        version = issued["version"]
        out_dir = SETTINGS.report_dir
        out_dir.mkdir(parents=True, exist_ok=True)

        bills = query(
            """
            SELECT household_id, consumption_kwh, solar_kwh, net_grid_kwh,
                   tariff_rate, billing_tier, subsidy_flag, gross_cost,
                   subsidy_amount, export_credit, final_bill, effective_rate
            FROM household_billing_daily
            WHERE sim_date = %s AND version = %s
            ORDER BY final_bill DESC NULLS LAST
            """,
            (sim_date, version),
        )
        zones = query(
            """
            SELECT grid_zone,
                   round(sum(total_consumption_kwh), 3) AS consumption_kwh,
                   round(sum(total_solar_kwh), 3)       AS solar_kwh,
                   round(avg(renewable_pct), 1)         AS avg_renewable_pct,
                   max(active_meter_count)              AS peak_meters
            FROM zone_load_1m
            WHERE window_start >= %s::date AND window_start < %s::date + 1
            GROUP BY grid_zone ORDER BY grid_zone
            """,
            (sim_date, sim_date),
        )

        stem = f"billing_report_{sim_date}_v{version}"
        csv_path = out_dir / f"{stem}.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as fh:
            if bills:
                w = csv.DictWriter(fh, fieldnames=list(bills[0].keys()))
                w.writeheader()
                w.writerows(bills)

        total = sum((b["final_bill"] or Decimal("0")) for b in bills)
        html_path = out_dir / f"{stem}.html"
        html_path.write_text(_render_html(sim_date, version, bills, zones, total),
                             encoding="utf-8")

        upsert_sim_day(date.fromisoformat(sim_date),
                       report_generated_at=utcnow(), status="reported")

        # §8's counter. The DailyReportMissed alert rule (Phase 6) fires when
        # this stops increasing, so it must be incremented only on success.
        daily_report_success_total.inc()

        return {**issued, "csv": str(csv_path), "html": str(html_path),
                "total_billed": str(total)}

    @task
    def audit(report: dict, **context) -> None:
        write_audit(
            run_id=context["run_id"],
            job_name="daily_billing_report",
            stage="orchestrate",
            sim_date=date.fromisoformat(report["sim_date"]),
            records_in=report["households"],
            records_out=report["households"],
            status="success",
            notes=(
                f"version={report['version']} total={report['total_billed']} LKR "
                f"csv={Path(report['csv']).name}"
            ),
        )

    audit(render_report(issue_bills(wait_for_sealed_and_priced())))


def _render_html(sim_date, version, bills, zones, total) -> str:
    """A self-contained HTML summary. No template engine, no CDN — it has to
    open from a file:// path during a demo with no network."""
    def esc(v):
        return html.escape("" if v is None else str(v))

    zone_rows = "".join(
        f"<tr><td>{esc(z['grid_zone'])}</td><td class=n>{esc(z['consumption_kwh'])}</td>"
        f"<td class=n>{esc(z['solar_kwh'])}</td><td class=n>{esc(z['avg_renewable_pct'])}%</td>"
        f"<td class=n>{esc(z['peak_meters'])}</td></tr>"
        for z in zones
    )
    top = bills[:10]
    bill_rows = "".join(
        f"<tr><td>{esc(b['household_id'])}</td><td>{esc(b['billing_tier'])}</td>"
        f"<td class=n>{esc(b['net_grid_kwh'])}</td>"
        f"<td class=n>{esc(b['gross_cost'])}</td>"
        f"<td class=n>{esc(b['subsidy_amount'])}</td>"
        f"<td class=n>{esc(b['export_credit'])}</td>"
        f"<td class=n><b>{esc(b['final_bill'])}</b></td>"
        f"<td class=n>{esc(b['effective_rate'])}</td></tr>"
        for b in top
    )
    exporters = [b for b in bills if (b["final_bill"] or 0) < 0]

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<title>Billing report {esc(sim_date)} v{version}</title>
<style>
 :root {{ color-scheme: light dark; }}
 body {{ font: 15px/1.55 system-ui, sans-serif; margin: 0; padding: 32px;
        max-width: 1100px; background: #fcfcfb; color: #0b0b0b; }}
 @media (prefers-color-scheme: dark) {{
   body {{ background: #1a1a19; color: #fff; }}
   td, th {{ border-color: #3a3a38 !important; }}
   .card {{ background: #232322 !important; }}
 }}
 h1 {{ font-size: 22px; margin: 0 0 4px; }}
 .sub {{ color: #6b6b66; margin-bottom: 24px; }}
 .cards {{ display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 28px; }}
 .card {{ background: #f4f4f1; border-radius: 8px; padding: 14px 18px; min-width: 150px; }}
 .card .k {{ font-size: 12px; color: #6b6b66; text-transform: uppercase;
             letter-spacing: .04em; }}
 .card .v {{ font-size: 24px; font-weight: 600; }}
 table {{ border-collapse: collapse; width: 100%; margin-bottom: 28px; }}
 th, td {{ text-align: left; padding: 7px 10px; border-bottom: 1px solid #e2e2dd; }}
 th {{ font-size: 12px; text-transform: uppercase; letter-spacing: .04em;
       color: #6b6b66; }}
 td.n, th.n {{ text-align: right; font-variant-numeric: tabular-nums; }}
 h2 {{ font-size: 16px; margin: 0 0 10px; }}
 footer {{ color: #6b6b66; font-size: 13px; border-top: 1px solid #e2e2dd;
           padding-top: 14px; }}
</style></head><body>
<h1>Daily billing report &mdash; simulated day {esc(sim_date)}</h1>
<div class="sub">Version {version} &middot; generated {esc(utcnow().isoformat(timespec='seconds'))}
 &middot; currency LKR (illustrative rates, not real CEB tariffs)</div>

<div class="cards">
  <div class="card"><div class="k">Households billed</div><div class="v">{len(bills)}</div></div>
  <div class="card"><div class="k">Total billed</div><div class="v">{esc(total)}</div></div>
  <div class="card"><div class="k">Net exporters</div><div class="v">{len(exporters)}</div></div>
  <div class="card"><div class="k">Version</div><div class="v">{version}</div></div>
</div>

<h2>Per-zone totals</h2>
<table><thead><tr><th>Zone</th><th class=n>Consumption kWh</th>
<th class=n>Solar kWh</th><th class=n>Avg renewable</th><th class=n>Peak meters</th>
</tr></thead><tbody>{zone_rows}</tbody></table>

<h2>Top 10 bills</h2>
<table><thead><tr><th>Household</th><th>Tier</th><th class=n>Net kWh</th>
<th class=n>Gross</th><th class=n>Subsidy</th><th class=n>Export credit</th>
<th class=n>Final bill</th><th class=n>Effective rate</th>
</tr></thead><tbody>{bill_rows}</tbody></table>

<footer>
 Produced by Airflow <code>daily_billing_report</code> from
 <code>household_billing_daily</code> version {version}. Bills are computed by
 <code>processing/transforms/billing.py</code> &mdash; the single implementation
 of the billing maths, shared with the streaming job. A negative final bill is a
 credit owed for exported energy.
</footer>
</body></html>
"""


daily_billing_report()
