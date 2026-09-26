# =============================================================================
# Shared Python base image for the non-Spark services.
#
# WHY ONE BASE IMAGE: the simulators, batch loader and API all import `common/`
# — the same config, logging, schemas and clock. Building them from one base
# guarantees they run identical versions of that shared code. If each service
# installed its own, a stale image could publish records against an older view
# of the contract, which is precisely the drift this project's single-source-of-
# truth design exists to prevent.
#
# python:3.11-slim: slim keeps the image small; the services need no compiler
# because every dependency ships wheels.
# =============================================================================
FROM python:3.11-slim

# Fail fast and log promptly inside containers:
#   PYTHONUNBUFFERED    - stdout is the log transport (§8); buffering would
#                         delay or lose lines when a container is killed, which
#                         is exactly the moment the logs matter most.
#   PYTHONDONTWRITEBYTECODE - no .pyc clutter in a layer that is rebuilt anyway.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

# Dependencies first, in their own layer: requirements.txt changes rarely while
# application code changes constantly, so this caches across most rebuilds.
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# The shared library. Service code is added by the per-service images and, in
# development, bind-mounted over the top for fast iteration.
COPY common/ /app/common/

# Run as a non-root user. Nothing here needs root, and it keeps files written to
# mounted volumes from landing on the host owned by root.
RUN useradd --create-home --uid 1000 appuser && chown -R appuser:appuser /app

# Pre-create the drop directory and hand it to appuser BEFORE the volume is
# mounted over it.
#
# Docker creates a named volume's mountpoint as root:root mode 755. A container
# running as a non-root user therefore cannot write into a freshly created volume
# — the tariff simulator could not drop its files and the batch loader could not
# create its marker directory. When a named volume is mounted onto a path that
# ALREADY exists in the image, Docker seeds the volume from that path, including
# its ownership, so creating it here with the right owner fixes both services.
#
# The alternative — running these containers as root — would work but would mean
# every file the daily feed writes is root-owned, which is the thing the non-root
# user is here to avoid.
RUN mkdir -p /data/drop && chown -R appuser:appuser /data

USER appuser

CMD ["python", "-c", "from common.sim_clock import startup_banner; print(startup_banner())"]
