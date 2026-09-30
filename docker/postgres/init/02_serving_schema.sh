#!/bin/bash
# =============================================================================
# Applies the serving schema (serving/sql/001_schema.sql) to the smartgrid
# database, on a FRESH data volume.
#
# WHY A SHELL WRAPPER AND NOT A PLAIN .sql FILE
# ---------------------------------------------
# The postgres image's init hook does run bare .sql files -- but it runs them as
# the superuser against POSTGRES_DB, which is `postgres` here, NOT `smartgrid`.
# (See docker-compose.yml: POSTGRES_DB is deliberately `postgres` so that
# 01_create_databases.sh has a database to connect to while creating the real
# ones.) A .sql file dropped in this directory would therefore create every table
# in the wrong database, and the failure would be silent -- the tables would
# exist, just nowhere the jobs look.
#
# This wrapper picks the database explicitly, which is the only reason it exists.
#
# WHY THE DDL IS NOT INLINE HERE
# ------------------------------
# The SQL lives in serving/sql/001_schema.sql because Phase 4's migrations
# (002_indexes.sql, 003_views.sql) and any future replay tooling read it from
# there. Duplicating it into this script would create a second definition of the
# serving schema, which is the same failure this project avoids everywhere else.
# Compose mounts ./serving/sql read-only at /opt/serving for this script.
#
# WHY THIS IS NOT THE ONLY WAY THE SCHEMA GETS APPLIED
# ----------------------------------------------------
# This hook runs ONLY when the data volume is empty -- i.e. on a first start or
# after `make clean`. Anyone who already has a postgres-data volume (everyone who
# ran Phases 0-2) would never see these tables, and Job B would die on its first
# upsert with a bare JDBC "relation does not exist".
#
# So `make serving-schema` applies the same file to a RUNNING instance by piping
# it in on stdin, and `make up` invokes it unconditionally. That is safe, and in
# fact required, because every statement in 001_schema.sql is IF NOT EXISTS.
# Between the two paths, the schema is present regardless of the volume's history
# -- which removes "did the volume already exist?" from the marker's path.
# =============================================================================
set -euo pipefail

SCHEMA_FILE=/opt/serving/001_schema.sql

if [ ! -f "$SCHEMA_FILE" ]; then
  # Fail loudly rather than starting a Postgres with no serving tables. A missing
  # mount here surfaces two minutes later as an unexplained JDBC error inside a
  # Spark executor log, which is a far worse place to diagnose it from.
  echo "[postgres-init] ERROR: $SCHEMA_FILE not found."
  echo "[postgres-init] docker-compose.yml must mount ./serving/sql at /opt/serving."
  exit 1
fi

echo "[postgres-init] applying serving schema to $SMARTGRID_DB"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$SMARTGRID_DB" \
  -f "$SCHEMA_FILE"

# The tables are created by the SUPERUSER (this hook's only available identity),
# so the smartgrid role that the Spark jobs and the API connect as owns none of
# them and cannot write to them. Granting after the fact is therefore not
# housekeeping -- without it every upsert fails with a permission error.
#
# ALTER DEFAULT PRIVILEGES covers the tables Phase 4/5 migrations add later, so
# this does not have to be remembered again.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$SMARTGRID_DB" \
  --set=sg_user="$SMARTGRID_USER" <<'SQL'
GRANT ALL ON ALL TABLES IN SCHEMA public TO :"sg_user";
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO :"sg_user";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO :"sg_user";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO :"sg_user";
SQL

echo "[postgres-init] serving schema ready in $SMARTGRID_DB"
