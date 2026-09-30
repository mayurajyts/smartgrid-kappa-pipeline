# =============================================================================
# Serving API image (Phase 4, §9).
#
# Thin layer on the shared Python base, for the reason that base exists: the API
# imports `common/` — the same config, contracts and simulated clock the
# simulators and loader use — and building from one base guarantees it runs the
# identical version of that shared code rather than a stale copy.
#
# The API deliberately does NOT ship `processing/`. It contains no business
# logic: it reads rows the Spark jobs wrote and shapes JSON (see
# serving/api/main.py). Not having the transforms on its path makes that
# structural rather than merely intended — an accidental import of the billing
# maths here would fail at build time instead of quietly becoming a second
# implementation of it.
# =============================================================================
FROM smartgrid-base:latest

USER root

# `serving/` only. `common/` is already in the base image.
COPY serving/ /app/serving/

RUN chown -R appuser:appuser /app

USER appuser

EXPOSE 8000

# --host 0.0.0.0 so the port is reachable from outside the container; the default
# 127.0.0.1 would bind inside the namespace only and the published port would
# connect to nothing.
#
# One worker on purpose: the connection pool lives in the process, and multiple
# uvicorn workers would each open their own pool against a Postgres instance
# shared with Airflow's metadata DB. At demo request volume one worker is ample,
# and the honest production answer is a process manager plus a shared pooler
# (pgbouncer), which is noted in the report rather than faked here.
CMD ["uvicorn", "serving.api.main:app", \
     "--host", "0.0.0.0", "--port", "8000", \
     "--workers", "1", "--no-access-log"]
