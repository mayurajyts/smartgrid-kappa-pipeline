#!/bin/bash
# =============================================================================
# Creates the two databases this platform needs, on one Postgres instance.
#
# WHY TWO DATABASES, ONE INSTANCE (plan §4.2):
#   `smartgrid` holds the serving store: zone aggregates, bills, alerts, audit.
#   `airflow`   holds Airflow's own metadata (DAG runs, task instances, XComs).
#
# These are isolated by DATABASE, not by instance. Separate databases give
# separate schemas, separate roles and no accidental cross-joins, while costing
# one container instead of two.
#
# TRADE-OFF (deliberate): they still share one instance's shared_buffers,
# connection slots and WAL. A runaway Airflow scheduler can therefore contend
# with a serving query — real coupling, not eliminated by this split. It is
# acceptable at demo scale, where Airflow runs four small DAGs. At production
# scale these belong on separate instances, since the serving store is on the
# latency path for the API and Airflow's metadata DB is not.
#
# Runs automatically via the postgres image's docker-entrypoint-initdb.d hook,
# which executes ONLY when the data volume is empty. After `make clean` it runs
# again; on an ordinary restart it does not, which is what makes it safe.
# =============================================================================
set -euo pipefail

echo "[postgres-init] creating serving and airflow databases"

# --set + :'var' passes values as properly quoted SQL literals/identifiers,
# so a password containing shell metacharacters cannot break out.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  --set=sg_db="$SMARTGRID_DB" \
  --set=sg_user="$SMARTGRID_USER" \
  --set=sg_password="$SMARTGRID_PASSWORD" \
  --set=af_db="$AIRFLOW_DB" \
  --set=af_user="$AIRFLOW_USER" \
  --set=af_password="$AIRFLOW_PASSWORD" <<'SQL'
CREATE ROLE :"sg_user" WITH LOGIN PASSWORD :'sg_password';
CREATE DATABASE :"sg_db" OWNER :"sg_user";
GRANT ALL PRIVILEGES ON DATABASE :"sg_db" TO :"sg_user";

CREATE ROLE :"af_user" WITH LOGIN PASSWORD :'af_password';
CREATE DATABASE :"af_db" OWNER :"af_user";
GRANT ALL PRIVILEGES ON DATABASE :"af_db" TO :"af_user";
SQL

# Postgres 15+ revokes CREATE on the public schema from non-owners, so the
# owning role must be granted it explicitly or the Phase 4 schema migration
# fails with a permission error that looks unrelated to this file.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$SMARTGRID_DB" \
  --set=sg_user="$SMARTGRID_USER" <<'SQL'
GRANT ALL ON SCHEMA public TO :"sg_user";
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$AIRFLOW_DB" \
  --set=af_user="$AIRFLOW_USER" <<'SQL'
GRANT ALL ON SCHEMA public TO :"af_user";
SQL

echo "[postgres-init] done: $SMARTGRID_DB and $AIRFLOW_DB ready"
