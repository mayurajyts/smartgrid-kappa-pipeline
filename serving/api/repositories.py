"""SQL access for the serving API (§9).

WHY THIS EXISTS IN THE ARCHITECTURE
-----------------------------------
Every query the API can run lives here, in one module, separated from the HTTP
layer. Two concrete reasons rather than habit:

  * The routers stay about HTTP — status codes, query parameters, response
    shapes — and this stays about SQL. When a dashboard panel is slow, there is
    one file to look in.
  * §7's claim is that the serving store is a *projection* of the Kafka log, and
    that the API never computes business logic. Keeping the SQL here makes that
    checkable: there is no aggregation in this file that the streaming jobs did
    not already do. The bills are read, not recalculated — the billing maths has
    exactly one implementation (processing/transforms/billing.py), and an API
    that re-derived a total would be a second one.

WHY psycopg WITH A CONNECTION POOL AND NOT AN ORM
-------------------------------------------------
The queries are a handful of small reads over two tables, several with window
functions. An ORM would add a dependency and a mapping layer to express SQL that
is already the clearest statement of what is wanted. `row_factory=dict_row`
gives dictionaries straight out, which FastAPI serialises directly.

The pool matters more than the driver choice: the dashboard polls several panels
every few seconds, and opening a TCP connection and authenticating per request
would dominate the latency budget (§2.4 allows under 10 s end to end, and a
Grafana panel refreshing at 5 s has far less than that).

NUMERIC COMES BACK AS Decimal, AND THAT IS DELIBERATE
-----------------------------------------------------
The money and kWh columns are NUMERIC (see serving/sql/001_schema.sql), so
psycopg returns `Decimal`. These are passed straight through to the response
models, which declare them as `Decimal` too, so the exactness that
billing.py and the schema jointly guarantee survives all the way to the HTTP
response. Casting to float here would silently reintroduce the error the whole
billing design exists to avoid.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row

from common.config import get_settings

_pool: Optional[ConnectionPool] = None


def init_pool() -> ConnectionPool:
    """Open the connection pool. Called once from the app's lifespan hook.

    `open=True` connects eagerly so that a misconfigured database fails at
    startup rather than on the first request — a container that starts and then
    500s on every call is harder to diagnose than one that refuses to start.
    """
    global _pool
    if _pool is None:
        pg = get_settings().postgres
        _pool = ConnectionPool(
            pg.dsn,
            min_size=1,
            # Small: this is one API container serving a dashboard and a demo, and
            # Postgres shares its instance with Airflow's metadata DB (see
            # docker/postgres/init/01_create_databases.sh), so connection slots
            # are a shared resource rather than free.
            max_size=8,
            open=True,
            timeout=10.0,
            kwargs={"row_factory": dict_row},
        )
    return _pool


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


def _query(sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    pool = init_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def _query_one(sql: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
    rows = _query(sql, params)
    return rows[0] if rows else None


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


def ping() -> bool:
    """True if the serving store answers. Backs /health's dependency check."""
    try:
        return _query_one("SELECT 1 AS ok") is not None
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Zones — R1 (grid load) and R2 (renewable contribution)
# ---------------------------------------------------------------------------

# The latest window per zone. DISTINCT ON is Postgres-specific and is the right
# tool here: it returns one row per grid_zone with no self-join and no window
# function, which keeps the dashboard's headline query trivial to read.
_LATEST_PER_ZONE = """
    SELECT DISTINCT ON (grid_zone)
           grid_zone, window_start, window_end,
           total_consumption_kwh, total_solar_kwh, renewable_pct,
           active_meter_count, late_event_count, updated_at
    FROM zone_load_1m
    ORDER BY grid_zone, window_start DESC
"""


def latest_zone_load() -> List[Dict[str, Any]]:
    """Current load and renewable mix per zone (R1, R2)."""
    return _query(_LATEST_PER_ZONE)


def zone_timeseries(grid_zone: str, limit: int) -> List[Dict[str, Any]]:
    """Recent windows for one zone, oldest first so a chart plots left to right.

    The inner ORDER BY ... DESC + LIMIT selects the newest N rows using the
    `zone_load_1m_window_start_idx` index; the outer one reverses them for
    display. Sorting ascending and limiting would return the OLDEST N, which is
    the opposite of what a live chart wants.
    """
    return _query(
        """
        SELECT * FROM (
            SELECT grid_zone, window_start, window_end,
                   total_consumption_kwh, total_solar_kwh, renewable_pct,
                   active_meter_count, late_event_count
            FROM zone_load_1m
            WHERE grid_zone = %s
            ORDER BY window_start DESC
            LIMIT %s
        ) recent
        ORDER BY window_start ASC
        """,
        (grid_zone, limit),
    )


def grid_summary() -> Optional[Dict[str, Any]]:
    """System-wide totals across the latest window of every zone.

    Summed over `latest_zone_load`'s rows rather than over the whole table: the
    question is "what is the grid doing NOW", so including historical windows
    would report a cumulative figure and grow without bound.

    renewable_pct is recomputed from the summed kWh, NOT averaged across zones.
    Averaging percentages weights a tiny zone equally with a large one and would
    overstate the renewable share whenever the small zones happen to be sunny.
    """
    return _query_one(
        f"""
        WITH latest AS ({_LATEST_PER_ZONE})
        SELECT count(*)                         AS zone_count,
               sum(total_consumption_kwh)       AS total_consumption_kwh,
               sum(total_solar_kwh)             AS total_solar_kwh,
               sum(active_meter_count)          AS active_meter_count,
               sum(late_event_count)            AS late_event_count,
               CASE WHEN sum(total_consumption_kwh) > 0
                    THEN least(100, greatest(0,
                         sum(total_solar_kwh) / sum(total_consumption_kwh) * 100))
                    ELSE NULL END               AS renewable_pct,
               max(window_start)                AS window_start,
               max(updated_at)                  AS updated_at
        FROM latest
        """
    )


# ---------------------------------------------------------------------------
# Households — R5 (daily bill) and R6 (solar contribution)
# ---------------------------------------------------------------------------


def household_bill(household_id: str, sim_date: Optional[date]) -> Optional[Dict[str, Any]]:
    """The current bill for a household.

    Reads `household_billing_daily` where a version has been ISSUED (Phase 5),
    and falls back to the running estimate otherwise. The fallback is explicit in
    the response via `source`, because the difference matters: one is an invoice,
    the other is a live estimate that may not yet have a tariff (§7). Collapsing
    them would let a customer read an estimate as a bill.
    """
    issued = _query_one(
        """
        SELECT household_id, sim_date, version, consumption_kwh, solar_kwh,
               net_grid_kwh, tariff_rate, billing_tier, subsidy_flag,
               gross_cost, subsidy_amount, export_credit, final_bill,
               effective_rate, is_current, generated_at
        FROM household_billing_daily
        WHERE household_id = %s
          AND (%s::date IS NULL OR sim_date = %s::date)
          AND is_current
        ORDER BY sim_date DESC
        LIMIT 1
        """,
        (household_id, sim_date, sim_date),
    )
    if issued:
        issued["source"] = "issued"
        return issued

    running = _query_one(
        """
        SELECT household_id, sim_date, consumption_kwh, solar_kwh, net_grid_kwh,
               self_consumption_ratio, running_cost, tariff_missing, updated_at
        FROM household_billing_running
        WHERE household_id = %s
          AND (%s::date IS NULL OR sim_date = %s::date)
        ORDER BY sim_date DESC
        LIMIT 1
        """,
        (household_id, sim_date, sim_date),
    )
    if running:
        running["source"] = "running_estimate"
    return running


def household_bill_versions(household_id: str) -> List[Dict[str, Any]]:
    """Every issued version for a household — the R7 restatement evidence.

    Ordered newest-version-first per day so the supersession chain reads top-down.
    This endpoint is the demo's proof that a restated bill did not overwrite its
    predecessor (§11 step 8).
    """
    return _query(
        """
        SELECT household_id, sim_date, version, final_bill, gross_cost,
               subsidy_amount, export_credit, tariff_rate, billing_tier,
               effective_rate, is_current, generated_at
        FROM household_billing_daily
        WHERE household_id = %s
        ORDER BY sim_date DESC, version DESC
        """,
        (household_id,),
    )


def top_consumers(sim_date: Optional[date], limit: int) -> List[Dict[str, Any]]:
    """Highest grid draw for a simulated day. Feeds the dashboard and the report."""
    return _query(
        """
        SELECT household_id, sim_date, consumption_kwh, solar_kwh, net_grid_kwh,
               self_consumption_ratio, running_cost, tariff_missing
        FROM household_billing_running
        WHERE (%s::date IS NULL OR sim_date = %s::date)
        ORDER BY net_grid_kwh DESC
        LIMIT %s
        """,
        (sim_date, sim_date, limit),
    )


def billing_summary(sim_date: Optional[date]) -> Optional[Dict[str, Any]]:
    """Per-day billing totals, including how many households are still unpriced.

    `unpriced` is surfaced deliberately: during the current simulated day it
    should equal the household count (the tariff arrives at D+1, §14), and it
    falling to zero is the visible signal that the daily feed landed. A summary
    that hid it would make a normal state look like missing data.
    """
    return _query_one(
        """
        SELECT sim_date,
               count(*)                                            AS households,
               count(running_cost)                                 AS priced,
               sum(CASE WHEN tariff_missing THEN 1 ELSE 0 END)      AS unpriced,
               sum(consumption_kwh)                                AS total_consumption_kwh,
               sum(solar_kwh)                                      AS total_solar_kwh,
               sum(net_grid_kwh)                                   AS total_net_grid_kwh,
               sum(running_cost)                                   AS total_running_cost
        FROM household_billing_running
        WHERE (%s::date IS NULL OR sim_date = %s::date)
        GROUP BY sim_date
        ORDER BY sim_date DESC
        LIMIT 1
        """,
        (sim_date, sim_date),
    )


# ---------------------------------------------------------------------------
# Alerts (R3, R4) — the table arrives in Phase 6
# ---------------------------------------------------------------------------


def alerts_table_exists() -> bool:
    """Whether Job D's table has been created yet.

    Checked rather than assumed so that /api/v1/alerts returns an empty list with
    a note during Phases 4-5, instead of a 500 that looks like a broken API. The
    endpoint is in §9 and is part of the contract; the data behind it is not
    written until Phase 6.
    """
    row = _query_one(
        "SELECT to_regclass('public.alerts') IS NOT NULL AS present"
    )
    return bool(row and row["present"])


def active_alerts(only_active: bool) -> List[Dict[str, Any]]:
    if not alerts_table_exists():
        return []
    clause = "WHERE resolved_at IS NULL" if only_active else ""
    return _query(
        f"""
        SELECT alert_id, alert_type, severity, entity_type, entity_id, sim_date,
               window_start, message, metric_value, threshold_value,
               raised_at, resolved_at
        FROM alerts
        {clause}
        ORDER BY raised_at DESC
        LIMIT 200
        """
    )


# ---------------------------------------------------------------------------
# Pipeline status
# ---------------------------------------------------------------------------


def pipeline_freshness() -> Dict[str, Any]:
    """How recently each serving table was written.

    The single most useful diagnostic the API exposes: it answers "is the
    pipeline alive" without reading a log or opening Grafana, and it is what
    /health uses to distinguish "the API is up" from "the pipeline is up". Those
    are different questions and conflating them is how a dead pipeline behind a
    healthy API goes unnoticed.
    """
    zones = _query_one(
        """
        SELECT max(window_start) AS last_window,
               max(updated_at)   AS last_write,
               count(*)          AS rows
        FROM zone_load_1m
        """
    ) or {}
    bills = _query_one(
        """
        SELECT max(sim_date)   AS last_sim_date,
               max(updated_at) AS last_write,
               count(*)        AS rows
        FROM household_billing_running
        """
    ) or {}
    return {
        "zone_load_1m": zones,
        "household_billing_running": bills,
        "alerts_table_present": alerts_table_exists(),
    }
