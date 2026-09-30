"""Unit tests for the Postgres upsert sink (Phase 3).

WHY THESE RUN ON THE HOST WITH NO SPARK AND NO DATABASE
-------------------------------------------------------
The idempotence guarantee of Jobs B and C is one SQL statement. Everything else
in the sink — the JDBC writer, the Py4J connection — is plumbing around it. So the
statement itself is composed by a pure function (`build_merge_sql`) that can be
asserted without a JVM or a running Postgres, and these tests pin its shape.

That matters because the alternative is only discovering that the MERGE is wrong
from a Phase 3 checkpoint run, which takes eleven real minutes to set up and
reports the symptom (rows differ after a restart) a long way from the cause.

The staging-table NAME is tested here too, because it has to agree with the
CREATE TABLE statements in serving/sql/001_schema.sql — two files that are edited
at different times and would otherwise drift silently, surfacing as a runtime
"relation does not exist" mid-demo.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from processing.sinks.postgres_sink import (
    build_merge_sql,
    jdbc_properties,
    staging_table_name,
)

SCHEMA_SQL = Path(__file__).resolve().parents[2] / "serving" / "sql" / "001_schema.sql"


# ---------------------------------------------------------------------------
# Staging table naming
# ---------------------------------------------------------------------------


def test_staging_name_is_deterministic():
    """A crashed batch must overwrite the SAME staging table on replay; a random
    suffix would leak one table per restart. See the function docstring."""
    first = staging_table_name("job_b_zone_aggregates", "zone_load_1m")
    second = staging_table_name("job_b_zone_aggregates", "zone_load_1m")
    assert first == second == "stg_job_b_zone_aggregates_zone_load_1m"


def test_staging_names_do_not_collide_between_jobs():
    b = staging_table_name("job_b_zone_aggregates", "zone_load_1m")
    c = staging_table_name("job_c_household_billing", "household_billing_running")
    assert b != c


def test_staging_names_match_the_shipped_schema():
    """The names this module generates must be the ones 001_schema.sql creates.

    If they diverge, Spark's overwrite would CREATE the missing table from the
    DataFrame's schema — with float8 money columns — and the exactness guarantee
    would be gone with no error anywhere. That is precisely the silent failure
    `truncate=true` exists to prevent, so the two files are pinned together here.
    """
    ddl = SCHEMA_SQL.read_text(encoding="utf-8")
    for job, table in (
        ("job_b_zone_aggregates", "zone_load_1m"),
        ("job_c_household_billing", "household_billing_running"),
    ):
        name = staging_table_name(job, table)
        assert "CREATE TABLE IF NOT EXISTS {}".format(name) in ddl, name


def test_staging_name_sanitises_unusual_characters():
    assert staging_table_name("job-b", "zone.load") == "stg_job_b_zone_load"


# ---------------------------------------------------------------------------
# The MERGE statement — the idempotence guarantee itself
# ---------------------------------------------------------------------------


ZONE_COLUMNS = [
    "grid_zone",
    "window_start",
    "window_end",
    "total_consumption_kwh",
    "renewable_pct",
    "updated_at",
]


def zone_sql(**overrides) -> str:
    kwargs = dict(
        staging_table="stg_job_b_zone_aggregates_zone_load_1m",
        target_table="zone_load_1m",
        columns=ZONE_COLUMNS,
        conflict_columns=["grid_zone", "window_start"],
        update_columns=[c for c in ZONE_COLUMNS if c not in ("grid_zone", "window_start")],
    )
    kwargs.update(overrides)
    return build_merge_sql(**kwargs)


def test_merge_is_an_upsert_on_the_primary_key():
    sql = zone_sql()
    assert sql.startswith("INSERT INTO zone_load_1m (")
    assert "FROM stg_job_b_zone_aggregates_zone_load_1m" in sql
    assert "ON CONFLICT (grid_zone, window_start) DO UPDATE SET" in sql


def test_update_assigns_from_excluded_not_from_the_target():
    """The assignment must be a TOTAL OVERWRITE.

    This is the single most important assertion in the file. An accumulating
    upsert (`SET x = zone_load_1m.x + EXCLUDED.x`) would double-count every
    replayed batch and fail the Phase 3 checkpoint — while looking entirely
    plausible in review, because accumulation is what a "running total" sounds
    like it should do. The running total lives in Spark's state; this sink only
    ever publishes its current value.
    """
    sql = zone_sql()
    assert "total_consumption_kwh = EXCLUDED.total_consumption_kwh" in sql

    # No assignment may reference the target table on the right-hand side.
    assignments = sql.split("DO UPDATE SET", 1)[1]
    assert "zone_load_1m." not in assignments


def test_conflict_columns_are_never_updated():
    """Updating a key column would change the row's identity mid-merge."""
    assignments = zone_sql().split("DO UPDATE SET", 1)[1]
    assert "grid_zone =" not in assignments
    assert "window_start =" not in assignments


def test_every_value_column_is_updated():
    """A column present on insert but absent from the update set would go stale
    on every re-emission of an open window — silently, and only for rows that
    have been seen more than once."""
    assignments = zone_sql().split("DO UPDATE SET", 1)[1]
    for column in ("window_end", "total_consumption_kwh", "renewable_pct", "updated_at"):
        assert "{0} = EXCLUDED.{0}".format(column) in assignments, column


def test_insert_and_select_column_lists_are_identical_and_ordered():
    """Positional mismatch between the two lists would write values into the
    wrong columns wherever their types happened to be compatible."""
    sql = zone_sql()
    insert_cols = re.search(r"INSERT INTO zone_load_1m \(([^)]+)\)", sql).group(1)
    select_cols = re.search(r"SELECT (.+?) FROM ", sql).group(1)
    assert insert_cols == select_cols == ", ".join(ZONE_COLUMNS)


def test_optional_where_clause_is_applied():
    sql = zone_sql(where="sim_date = '2026-01-02'")
    assert "FROM stg_job_b_zone_aggregates_zone_load_1m WHERE sim_date = '2026-01-02' " in sql


def test_no_where_clause_by_default():
    assert " WHERE " not in zone_sql()


# ---------------------------------------------------------------------------
# Identifier safety
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad",
    ["grid_zone; DROP TABLE zone_load_1m", "grid zone", "", "col--", "a'b"],
)
def test_unsafe_identifiers_are_rejected(bad):
    """Table and column names cannot be bind parameters, so they are
    interpolated. Every caller passes repository literals — this guard exists so
    that a future caller passing something data-derived fails here rather than
    composing a statement whose shape depends on a Kafka payload."""
    with pytest.raises(ValueError, match="unsafe SQL identifier"):
        build_merge_sql(
            staging_table="stg_x",
            target_table="zone_load_1m",
            columns=[bad],
            conflict_columns=["grid_zone"],
            update_columns=["total_consumption_kwh"],
        )


def test_unsafe_table_names_are_rejected():
    with pytest.raises(ValueError, match="unsafe SQL identifier"):
        build_merge_sql(
            staging_table="stg_x; DELETE FROM zone_load_1m",
            target_table="zone_load_1m",
            columns=["grid_zone"],
            conflict_columns=["grid_zone"],
            update_columns=["grid_zone"],
        )


# ---------------------------------------------------------------------------
# JDBC properties
# ---------------------------------------------------------------------------


def test_jdbc_properties_name_the_driver_explicitly():
    """Naming the class turns a missing JAR into a ClassNotFoundException that
    says which class, rather than "No suitable driver found" which reads as a
    malformed URL."""
    from common.config import PostgresSettings

    props = jdbc_properties(PostgresSettings(_env_file=None))
    assert props["driver"] == "org.postgresql.Driver"
    assert props["stringtype"] == "unspecified"
    assert props["user"] == "smartgrid"


def test_jdbc_url_is_the_one_from_settings():
    """The sink and the API must not drift onto different stores."""
    from common.config import PostgresSettings

    pg = PostgresSettings(_env_file=None)
    assert pg.jdbc_url == "jdbc:postgresql://postgres:5432/smartgrid"
