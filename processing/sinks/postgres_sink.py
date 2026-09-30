"""Idempotent Postgres upsert for the streaming sinks (§7 Jobs B and C).

WHY THIS EXISTS IN THE ARCHITECTURE
-----------------------------------
This module is where at-least-once delivery becomes effectively-once storage —
the claim §7 asks to be stated explicitly in the report, and the mechanism behind
half of the Phase 3 checkpoint ("upserts are idempotent under forced restart").

Spark's `foreachBatch` sink is at-least-once by construction: the checkpoint is
committed only AFTER the function returns, so a crash anywhere inside it replays
the entire batch on restart. Nothing in Spark prevents that replay from writing
the same rows twice. The guarantee has to come from the sink, and it comes from
the target tables' primary keys plus `ON CONFLICT DO UPDATE`: a replayed batch
overwrites the same keys with the same values, so the second write is a no-op on
content. That is why every table in serving/sql/001_schema.sql that a streaming
job writes has a PRIMARY KEY on its business identity.

It lives in one shared module rather than as a method on each job because Jobs B
and C — and Phase 5's replay and daily report — all need that guarantee. Two
implementations would mean two guarantees, and the weaker one would be the real
one.

WHY JDBC + Py4J RATHER THAN psycopg2
------------------------------------
`df.write.jdbc()` can only append or overwrite; it has no `ON CONFLICT`. Two ways
out of that, and the trade-off is worth having ready for the viva:

  * Collect the batch to the driver and upsert with psycopg2. Simpler and easy to
    unit-test, but it pulls every row through the driver process. At this scale
    (5 zone rows, ~200 household rows) that is genuinely fine — and it is fine for
    reasons that stop being true the moment the system grows, which makes it a
    design that cannot be defended as written.

  * Stage the batch with the distributed JDBC writer, then issue ONE SQL statement
    that merges staging into the target. The write stays on the executors; only a
    single `INSERT ... SELECT` crosses the driver. That is this module.

The connection for that statement is obtained through Py4J from the JVM the
driver already runs, using the same `postgresql-42.7.4.jar` the DataFrame writer
uses. No second driver, no second connection configuration, nothing to drift.

WHY A STAGING TABLE AND NOT ROW-BY-ROW `foreachPartition`
---------------------------------------------------------
A per-row upsert from each partition would also be idempotent, and would be one
transaction per row — ~200 transactions per micro-batch for Job C, each with its
own round trip and its own chance to fail halfway. The staging approach is one
bulk write plus one transaction, and the merge is atomic: the target is never
observed partially updated from a single batch.

THE TRANSACTION BOUNDARY, STATED HONESTLY
-----------------------------------------
The staging write and the MERGE are two separate transactions, not one. The
possible crash points and what each leaves behind:

  * Crash BETWEEN them — staging is populated, the target is not yet merged.
    Spark replays the whole batch: staging is overwritten with identical content
    and the merge runs. Net effect: correct.
  * Crash DURING the merge — the merge's transaction rolls back atomically. The
    target is unchanged, and the replay repeats it. Net effect: correct.
  * Crash AFTER the merge but before the checkpoint commits — the replay re-runs
    both, and `ON CONFLICT DO UPDATE` rewrites the same values. Net effect:
    correct, because the update is a total overwrite of the value columns rather
    than an accumulation.

That last point is the one that matters and the one that constrains Job C's
design: the merge must never do `SET x = target.x + excluded.x`. An accumulating
upsert would double-count on every replayed batch, which would fail the Phase 3
checkpoint outright. Spark's stateful aggregation holds the running total, and
this sink only ever publishes the current value of it.

ONE DELIBERATE EXCEPTION TO BIT-IDENTICAL REPLAY: `updated_at` is in the update
set, so a replayed batch writes a new timestamp. That is intended — it records
when the write happened, not what was computed — and the idempotence check must
exclude it from its comparison. Saying so here so that a correct system does not
look broken during verification.
"""

from __future__ import annotations

import time
from typing import Any, Optional, Sequence

from common.config import PostgresSettings
from common.logging_setup import STAGE_STORE
from common.metrics import (
    serving_rows_upserted_total,
    serving_upsert_duration_seconds,
)

# Set on the JDBC connection so Postgres parses ambiguous literals by the target
# column's type rather than by the string's shape. Without it a timestamp handed
# over as text can be rejected with "column is of type timestamptz but expression
# is of type character varying" on some driver/server combinations.
_STRINGTYPE = "unspecified"

_DRIVER_CLASS = "org.postgresql.Driver"


def jdbc_properties(pg: PostgresSettings) -> dict:
    """Connection properties for the Spark JDBC writer.

    The driver class is named explicitly rather than left to auto-detection: a
    missing JAR then fails with a clear ClassNotFoundException naming the class,
    instead of the far more confusing "No suitable driver found for
    jdbc:postgresql://..." which reads like a malformed URL.
    """
    return {
        "user": pg.postgres_user,
        "password": pg.postgres_password,
        "driver": _DRIVER_CLASS,
        "stringtype": _STRINGTYPE,
    }


def staging_table_name(job_name: str, target_table: str) -> str:
    """Deterministic staging table name for one job writing one target.

    DETERMINISTIC, NOT RANDOM, and this is a correctness property rather than
    tidiness. A batch that crashed after its staging write must, on replay,
    overwrite the SAME staging table — a random suffix would leave the half-
    written one behind and leak a table per restart, which on a long-running demo
    becomes thousands of orphaned relations.

    Collision safety: the name embeds both the job and the target, so Jobs B and
    C never share one. Two instances of the SAME job would collide, but cannot
    run concurrently anyway — compose gives each a fixed container name, and the
    second would in any case fail on the checkpoint lock before reaching a write.
    Documented here rather than defended with a suffix, because the suffix would
    trade a real property (crash-replay) for a hypothetical one.

    Must agree with the CREATE TABLE statements in serving/sql/001_schema.sql;
    the tables are created there so they exist with the target's column TYPES
    before the first write (see `write_staging` on why that matters).
    """
    safe = "".join(c if c.isalnum() else "_" for c in f"{job_name}_{target_table}")
    return f"stg_{safe}".lower()


def write_staging(df: Any, table: str, pg: PostgresSettings) -> None:
    """Overwrite `table` with this micro-batch's rows, from the executors.

    `truncate=true` IS NOT OPTIONAL. Spark's default `overwrite` behaviour is to
    DROP the table and recreate it from the DataFrame's schema — which would
    replace the `NUMERIC(18,6)` and `NUMERIC(14,2)` columns declared in
    001_schema.sql with `float8`, silently ending the exactness guarantee that
    both the schema and processing/transforms/billing.py are built on. The bills
    would still look right to two decimal places, and would stop being
    reproducible.

    It also makes the operation a TRUNCATE inside a transaction rather than DDL,
    so a concurrent reader never observes the table as missing.
    """
    (
        df.write.mode("overwrite")
        .option("truncate", "true")
        .option("isolationLevel", "READ_COMMITTED")
        .jdbc(pg.jdbc_url, table, properties=jdbc_properties(pg))
    )


def build_merge_sql(
    *,
    staging_table: str,
    target_table: str,
    columns: Sequence[str],
    conflict_columns: Sequence[str],
    update_columns: Sequence[str],
    where: Optional[str] = None,
) -> str:
    """Compose the `INSERT ... SELECT ... ON CONFLICT DO UPDATE` statement.

    Split out from `merge_from_staging` so the SQL can be asserted in a unit test
    without a JVM, a Spark session or a database. The statement is the entire
    idempotence guarantee, so it is worth being able to read it in a test failure.

    Identifiers are interpolated rather than parameterised because SQL does not
    accept bind parameters for table or column names. They are safe here because
    every caller passes literals defined in this repository — never user input,
    never anything read from Kafka. `_assert_identifier` enforces that rather than
    trusting it.
    """
    for name in list(columns) + list(conflict_columns) + list(update_columns):
        _assert_identifier(name)
    _assert_identifier(staging_table)
    _assert_identifier(target_table)

    col_list = ", ".join(columns)
    conflict = ", ".join(conflict_columns)

    # EXCLUDED is the row proposed by the INSERT. Assigning from it makes the
    # update a TOTAL OVERWRITE of the value columns, which is what makes a
    # replayed batch a no-op on content. Never `target.col + EXCLUDED.col` — see
    # the module docstring.
    assignments = ", ".join("{0} = EXCLUDED.{0}".format(c) for c in update_columns)
    predicate = " WHERE {0}".format(where) if where else ""

    return (
        "INSERT INTO {target} ({cols}) "
        "SELECT {cols} FROM {staging}{predicate} "
        "ON CONFLICT ({conflict}) DO UPDATE SET {assignments}"
    ).format(
        target=target_table,
        cols=col_list,
        staging=staging_table,
        predicate=predicate,
        conflict=conflict,
        assignments=assignments,
    )


def _assert_identifier(name: str) -> None:
    """Reject anything that is not a plain SQL identifier.

    These names are all repository literals, so this can never fire in normal
    operation. It exists so that a future caller which passes a column name
    derived from data fails here, loudly, rather than composing a statement whose
    shape depends on a Kafka payload.
    """
    if not name or not all(c.isalnum() or c == "_" for c in name):
        raise ValueError("unsafe SQL identifier: {!r}".format(name))


def merge_from_staging(
    spark: Any,
    *,
    staging_table: str,
    target_table: str,
    columns: Sequence[str],
    conflict_columns: Sequence[str],
    update_columns: Sequence[str],
    pg: PostgresSettings,
    where: Optional[str] = None,
) -> int:
    """Merge staging into the target in ONE transaction. Returns rows affected.

    Runs on the driver, through the JVM's own `java.sql.DriverManager` via Py4J —
    the same JVM and the same JAR the DataFrame writer used, so there is no second
    connection configuration to drift.
    """
    sql = build_merge_sql(
        staging_table=staging_table,
        target_table=target_table,
        columns=columns,
        conflict_columns=conflict_columns,
        update_columns=update_columns,
        where=where,
    )

    jvm = spark._jvm

    # Force driver registration before asking for a connection. The JAR is on the
    # classpath, but DriverManager's ServiceLoader discovery does not reliably run
    # under Spark's classloader arrangement, and when it does not the failure is
    #   java.sql.SQLException: No suitable driver found for jdbc:postgresql://...
    # which reads as a missing JAR or a malformed URL rather than as a
    # registration-order problem. One line here removes a genuinely misleading
    # failure mode.
    jvm.Class.forName(_DRIVER_CLASS)

    conn = jvm.java.sql.DriverManager.getConnection(
        pg.jdbc_url, pg.postgres_user, pg.postgres_password
    )
    try:
        # Explicit transaction. With autocommit left on, a multi-statement future
        # version of this merge could commit halfway.
        conn.setAutoCommit(False)
        stmt = conn.createStatement()
        try:
            affected = stmt.executeUpdate(sql)
        finally:
            stmt.close()
        conn.commit()
        return int(affected)
    except Exception:
        # Roll back explicitly rather than relying on the close. Postgres does
        # discard an open transaction on disconnect — but the staging write's
        # TRUNCATE has already COMMITTED by this point, so a failure swallowed
        # here would leave the target stale while staging looked perfectly
        # correct. That is the hardest possible state to diagnose, and the reason
        # this branch exists at all.
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def upsert_batch(
    spark: Any,
    df: Any,
    *,
    job_name: str,
    target_table: str,
    columns: Sequence[str],
    conflict_columns: Sequence[str],
    update_columns: Sequence[str],
    pg: PostgresSettings,
    log: Any = None,
) -> int:
    """Stage, then merge. The single call a `foreachBatch` makes.

    EXCEPTIONS ARE RE-RAISED, NEVER SWALLOWED. A failed upsert must fail the
    micro-batch so that Spark does not advance the checkpoint: swallowing it would
    commit offsets for data that never reached Postgres, and those rows would be
    unrecoverable short of a full replay from the start of the topic. A job that
    dies loudly is recoverable; one that silently skips a batch is not.
    """
    staging = staging_table_name(job_name, target_table)
    started = time.monotonic()

    try:
        write_staging(df.select(*columns), staging, pg)
        affected = merge_from_staging(
            spark,
            staging_table=staging,
            target_table=target_table,
            columns=columns,
            conflict_columns=conflict_columns,
            update_columns=update_columns,
            pg=pg,
        )
    except Exception as exc:
        if log is not None:
            message = str(exc)
            # The schema-missing case is worth naming explicitly. It is the most
            # likely first-run failure — the Postgres init hook only fires on an
            # empty data volume, so anyone whose volume predates Phase 3 has no
            # serving tables — and the raw JDBC error says nothing about the fix.
            hint = None
            if "does not exist" in message and target_table in message:
                hint = (
                    "the serving schema is missing; run `make serving-schema` "
                    "(the postgres init hook only runs on an empty data volume)"
                )
            log.error(
                "serving_upsert_failed",
                stage=STAGE_STORE,
                job_name=job_name,
                target_table=target_table,
                staging_table=staging,
                hint=hint,
                error=message.splitlines()[0][:500],
            )
        raise

    duration = time.monotonic() - started
    serving_upsert_duration_seconds.labels(
        job=job_name, table=target_table
    ).observe(duration)
    serving_rows_upserted_total.labels(
        job=job_name, table=target_table
    ).inc(affected)

    return affected
