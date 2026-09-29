# =============================================================================
# Application image for the Spark jobs — a thin layer over smartgrid-spark-base.
#
# Contains ONLY the code. Everything expensive (pip install, ~500 MB of connector
# JARs) lives in docker/spark/Dockerfile.base and is built once via
# `make spark-base`, so a code change rebuilds in seconds rather than minutes.
#
# If the base image is missing, `make up` builds it automatically.
# =============================================================================
FROM smartgrid-spark-base:latest

USER root
WORKDIR /app

# --- Application code --------------------------------------------------------
# `simulators/` is included because the Spark jobs import the reference dimension
# and the fault-reason constants from it. That sharing is intentional: the
# validator bounds solar against the SAME solar_capacity_kw the producer generated
# from, and Job A's DLQ reasons are the SAME strings the producer records, so an
# injected fault and its rejection are provably the same event.
COPY common/ /app/common/
COPY processing/ /app/processing/
COPY simulators/ /app/simulators/

# The transform tests ship in the image so `make test-spark` can run them HERE,
# inside the environment the jobs actually run in. PySpark's local mode needs a
# Hadoop native environment that `pip install pyspark` does not provide (winutils
# on Windows), so on some hosts those tests skip on the host and this is the only
# place they genuinely execute.
COPY tests/ /app/tests/
COPY pyproject.toml /app/pyproject.toml

# Checkpoint root. Mounted as a volume in compose so checkpoints survive a
# container restart — which is precisely what makes the Phase 2 checkpoint
# ("restarting the job does not duplicate Parquet output") testable.
# Pre-create BOTH volume mountpoints and hand them to the spark user BEFORE the
# volumes are mounted over them.
#
# Docker creates a named volume's mountpoint as root:root, so a container running as
# a non-root user cannot write into a freshly created volume. When a volume is
# mounted onto a path that ALREADY exists in the image, Docker seeds the volume from
# that path including its ownership — so creating them here with the right owner is
# what makes both writable. (The same trap bit the drop-directory volume in Phase 1;
# it is easy to miss because the failure surfaces as "Mkdirs failed to create" deep
# in the Hadoop committer rather than as a permission error.)
RUN mkdir -p /data/checkpoints /data/curated && chown -R spark:spark /data /app

USER spark
