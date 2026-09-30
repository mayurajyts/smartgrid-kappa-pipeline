# =============================================================================
# Airflow image for the four orchestration DAGs (Phase 5).
#
# WHY NOT BUILT ON smartgrid-base: Airflow pins a large, specific dependency set
# and ships its own constraints file. Installing Airflow on top of our base would
# mean resolving its pins against ours, and a conflict there surfaces as an
# obscure runtime import error rather than a build failure. Starting from the
# official image and adding only what the DAGs need is the safer direction.
#
# The project itself is BIND-MOUNTED at /opt/project rather than copied in, so
# `common/` and `processing/` are the same files the other services run. That
# matters more here than anywhere else: the billing DAG calls
# processing/transforms/billing.py, and the entire architecture claim is that it
# is the SAME implementation the streaming job uses -- not a copy that happens to
# agree today.
# =============================================================================
FROM apache/airflow:2.10.3-python3.11

USER root

# psycopg 3 for the DAGs' own serving-store access (the DAGs read and write
# household_billing_daily and sim_day_state directly). Airflow's bundled
# psycopg2 is used by Airflow for its METADATA database; the DAG code uses
# psycopg 3 via orchestration/dags/common_tasks.py, matching the API.
# Pre-create the report directory and hand it to the airflow user BEFORE the
# named volume mounts over it.
#
# Docker creates a named volume's mountpoint as root:root, so a container running
# as a non-root user cannot write into a freshly created volume -- the report
# renderer failed with "PermissionError: /data/reports/...". When a volume is
# mounted onto a path that ALREADY exists in the image, Docker seeds it from that
# path INCLUDING ownership, which is what fixes it.
#
# The same trap bit Phase 1 (the drop directory) and Phase 2 (the Spark
# checkpoint directory); docker/base/Dockerfile carries the same fix.
RUN mkdir -p /data/reports && chown -R airflow:root /data

USER airflow
RUN pip install --no-cache-dir \
        "psycopg[binary]~=3.2" \
        "pydantic~=2.9" \
        "pydantic-settings~=2.6" \
        "structlog~=24.4" \
        "prometheus-client~=0.21"

# /opt/project is where docker-compose bind-mounts the repository. On PYTHONPATH
# so `import common...` and `import processing...` work inside a DAG without each
# DAG file manipulating sys.path -- they still do so defensively, because a DAG
# that fails to import is invisible in the Airflow UI rather than loudly broken.
ENV PYTHONPATH=/opt/project \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
