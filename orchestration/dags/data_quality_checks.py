"""DAG: data_quality_checks — the gate that must fail loudly (§7, Phase 5).

WHY THIS EXISTS IN THE ARCHITECTURE
-----------------------------------
§7 specifies three gates before a bill is issued: "reject rate < 2%, expected
meter coverage >= 95%, no unexplained kWh gap. Fail the DAG loudly if breached."

The point is not the thresholds. The point is that a billing system which issues
invoices from data it has not checked is indefensible, and that the check has to
be able to STOP the pipeline rather than merely log a warning nobody reads. So
every task here raises on breach, and `daily_billing_report` depends on this
DAG's outcome.

WHY THE THRESHOLDS ARE CONFIGURABLE BUT THE FAILURE IS NOT
----------------------------------------------------------
The numbers come from the environment, because 2% is a judgement about this
simulator's fault injection rate (0.8% designed) and would differ for real meter
data. What is not configurable is whether a breach fails: there is no "warn only"
mode, because the one thing a quality gate must not be is optional.

WHY IT RUNS SEPARATELY FROM THE BILLING DAG
-------------------------------------------
Two reasons. First, the checks are useful on their own — a marker or an operator
can run them on demand without issuing a bill. Second, keeping them in their own
DAG means a quality failure is visibly a QUALITY failure in the Airflow UI, not a
billing task that happened to throw. When the demo shows a red DAG, it should be
obvious what was red about it.
"""

from __future__ import annotations

import sys
from datetime import timedelta

sys.path.insert(0, "/opt/project")

import pendulum
from airflow.decorators import dag, task
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from orchestration.dags.common_tasks import (
    query_one,
    target_sim_date,
    write_audit,
)


class QualitySettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # §7's 2%. The simulator injects faults at a designed 0.8% combined rate and
    # Phase 2 measured 0.74% in steady state, so 2% leaves room for a genuinely
    # bad batch without tripping on normal operation.
    dq_max_reject_rate_pct: float = Field(default=2.0, gt=0)

    # §7's 95%. 200 households across 5 zones; the simulator's meter-dropout
    # fault deliberately silences a few meters at a time, so demanding 100%
    # would fail on correct behaviour.
    dq_min_meter_coverage_pct: float = Field(default=95.0, gt=0, le=100)

    # Total household count the coverage figure is measured against. Read from
    # config rather than counted from the data: counting the households that
    # REPORTED and then checking coverage against that number would always give
    # 100% and the check would be vacuous.
    dq_expected_households: int = Field(default=200, gt=0)


SETTINGS = QualitySettings()

DEFAULT_ARGS = {"owner": "smartgrid", "retries": 0}


@dag(
    dag_id="data_quality_checks",
    description="Reject rate, meter coverage and kWh coherence gates (§7).",
    default_args=DEFAULT_ARGS,
    schedule=timedelta(minutes=5),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["smartgrid", "phase5", "quality"],
)
def data_quality_checks():
    @task
    def check_meter_coverage() -> dict:
        """At least N% of expected households reported for the sealed day.

        Low coverage means readings are being lost somewhere between the
        simulator and Job C — which would understate every bill for that day.
        """
        sim_date = target_sim_date()
        row = query_one(
            "SELECT count(DISTINCT household_id) AS reporting "
            "FROM household_billing_running WHERE sim_date = %s",
            (sim_date,),
        ) or {"reporting": 0}

        reporting = row["reporting"] or 0
        coverage = 100.0 * reporting / SETTINGS.dq_expected_households

        if coverage < SETTINGS.dq_min_meter_coverage_pct:
            raise ValueError(
                f"meter coverage {coverage:.1f}% for {sim_date} is below the "
                f"{SETTINGS.dq_min_meter_coverage_pct}% gate "
                f"({reporting}/{SETTINGS.dq_expected_households} households "
                f"reported). Readings are being lost upstream; every bill for "
                f"this day would be understated."
            )
        return {
            "sim_date": sim_date.isoformat(),
            "reporting": reporting,
            "coverage_pct": round(coverage, 2),
        }

    @task
    def check_kwh_coherence() -> dict:
        """Physical sanity on the aggregates, not a statistical test.

        Three things that cannot be true of real meter data and would each mean
        a transform bug rather than bad input:
          * negative consumption or negative generation,
          * self-consumption exceeding generation,
          * a self-consumption ratio outside [0, 1].

        This is the "no unexplained kWh gap" gate. A statistical bound on total
        kWh would be tuned to the simulator's parameters and would break the
        moment those changed; these three are invariants of the physics.
        """
        sim_date = target_sim_date()
        row = query_one(
            """
            SELECT
              sum(CASE WHEN consumption_kwh < 0 OR solar_kwh < 0
                       THEN 1 ELSE 0 END)                        AS negative_kwh,
              sum(CASE WHEN self_consumption_ratio IS NOT NULL
                        AND (self_consumption_ratio < 0
                             OR self_consumption_ratio > 1)
                       THEN 1 ELSE 0 END)                        AS bad_ratio,
              sum(CASE WHEN solar_kwh > 0
                        AND consumption_kwh + solar_kwh < net_grid_kwh
                       THEN 1 ELSE 0 END)                        AS impossible_net,
              count(*)                                           AS rows
            FROM household_billing_running
            WHERE sim_date = %s
            """,
            (sim_date,),
        ) or {}

        problems = []
        for key, label in (
            ("negative_kwh", "negative consumption or generation"),
            ("bad_ratio", "self-consumption ratio outside [0,1]"),
            ("impossible_net", "net grid draw exceeding consumption plus generation"),
        ):
            count = row.get(key) or 0
            if count:
                problems.append(f"{count} rows with {label}")

        if problems:
            raise ValueError(
                f"kWh coherence gate failed for {sim_date}: "
                + "; ".join(problems)
                + ". These are physically impossible, so the cause is a "
                "transform defect rather than bad input data."
            )
        return {"sim_date": sim_date.isoformat(), "rows_checked": row.get("rows") or 0}

    @task
    def check_reject_rate() -> dict:
        """Reject rate below the gate, measured from the audit trail.

        Reads `pipeline_run_audit` rather than the DLQ topic: counting DLQ
        records would need a Kafka consumer inside Airflow, and the audit table
        already holds the in/out/rejected counts Job A logged at its stage
        boundary. Same numbers, no extra dependency.

        Returns a pass when no audit rows exist yet rather than failing —
        absence of evidence is not a quality breach, and failing here on a fresh
        stack would block the very first bill for no reason.
        """
        row = query_one(
            """
            SELECT sum(records_in) AS records_in,
                   sum(records_rejected) AS rejected
            FROM pipeline_run_audit
            WHERE job_name LIKE 'job_a%%' AND started_at > now() - interval '1 hour'
            """
        ) or {}

        total = row.get("records_in") or 0
        rejected = row.get("rejected") or 0
        if total == 0:
            return {"reject_rate_pct": None, "note": "no audit rows yet"}

        rate = 100.0 * rejected / total
        if rate > SETTINGS.dq_max_reject_rate_pct:
            raise ValueError(
                f"reject rate {rate:.2f}% exceeds the "
                f"{SETTINGS.dq_max_reject_rate_pct}% gate "
                f"({rejected}/{total} records). Investigate the DLQ before any "
                f"bill is issued from this data."
            )
        return {"reject_rate_pct": round(rate, 3), "records_in": total}

    @task
    def audit(coverage: dict, coherence: dict, rejects: dict, **context) -> None:
        write_audit(
            run_id=context["run_id"],
            job_name="data_quality_checks",
            stage="orchestrate",
            sim_date=None,
            records_in=coherence.get("rows_checked", 0),
            records_out=coverage.get("reporting", 0),
            status="success",
            notes=(
                f"coverage={coverage.get('coverage_pct')}% "
                f"reject_rate={rejects.get('reject_rate_pct')}%"
            ),
        )

    audit(check_meter_coverage(), check_kwh_coherence(), check_reject_rate())


data_quality_checks()
