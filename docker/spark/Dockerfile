# =============================================================================
# Spark image for the streaming jobs (A-D) and the replay entrypoint.
#
# Built on apache/spark:3.5.3-python3. Note bitnami/spark is no longer pullable
# without authentication, the same problem that moved the object store off MinIO
# (see docs/ADR-003-object-store.md) — the official Apache image avoids that class
# of dependency entirely.
#
# THE PYTHON 3.8 CONSTRAINT (non-obvious, so recorded here)
# --------------------------------------------------------
# This image ships Python 3.8, while the simulator and API images run 3.11. That
# asymmetry is deliberate: the PySpark version MUST match the cluster exactly or
# driver/executor communication fails with opaque serialisation errors, so the
# image's own Python is used rather than being upgraded underneath Spark.
#
# The consequence is that `common/` — shared with the 3.11 services — must remain
# importable on 3.8. It already uses `from __future__ import annotations`, which
# makes the `X | None` syntax parse. But pydantic must EVALUATE those annotations
# at runtime to build its validators, and `datetime | None` is not evaluable on
# 3.8 no matter how it was parsed. Hence `eval_type_backport` below: without it,
# importing common.config raises
#   TypeError: Unable to evaluate type annotation 'datetime | None'
# which looks like a code bug rather than a runtime-version mismatch.
#
# The alternative — rewriting every annotation as typing.Optional[...] — was
# rejected because it would degrade the 3.11 codebase to satisfy one image.
#
# CONNECTOR JARS
# --------------
# Fetched at build time rather than resolved by `--packages` at submit time, so a
# container start needs no network access and cannot fail on a Maven outage mid-
# demo. Versions are pinned to match: spark-sql-kafka 3.5.3 for Spark 3.5.3, and
# hadoop-aws 3.3.4 against the hadoop-client 3.3.4 already in the image. A
# mismatched hadoop-aws is the usual cause of NoSuchMethodError on the s3a path.
# =============================================================================
FROM apache/spark:3.5.3-python3

# Root for the install steps; the image's own spark user is restored at the end.
USER root

# PySpark ships inside this image as zip archives under $SPARK_HOME/python/lib,
# which only `spark-submit` adds to sys.path. Putting them on PYTHONPATH makes
# `import pyspark` work for a plain `python3` too, which is what lets pytest run
# the transform tests here. Without it the Spark tests silently SKIP inside the very
# container built to run them — the worst outcome, since a suite that only ever
# skips is indistinguishable from one that passed.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

WORKDIR /app

# --- Python dependencies -----------------------------------------------------
# Deliberately NOT requirements.txt, for two reasons:
#   * that file pins pyspark for the host test run, and installing a second
#     PySpark here could shadow the cluster's own version;
#   * it includes confluent-kafka, which has no CPython 3.8 wheel for this
#     platform and fails to build here.
#
# Dropping confluent-kafka is correct rather than merely convenient: the Spark
# jobs reach Kafka through the JVM spark-sql-kafka connector installed below, not
# through a Python client. Nothing under processing/ or common/ imports it. The
# Python client is only used by the simulators and the batch loader, which run on
# the 3.11 base image.
RUN pip install --no-cache-dir \
        "pydantic~=2.9" \
        "pydantic-settings~=2.6" \
        "structlog~=24.4" \
        "prometheus-client~=0.21" \
        eval_type_backport

# --- Connector JARs ----------------------------------------------------------
ARG SPARK_VERSION=3.5.3
ARG SCALA_VERSION=2.12
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

RUN set -eux; \
    MAVEN=https://repo1.maven.org/maven2; \
    JARS=/opt/spark/jars; \
    # Kafka source/sink, plus the clients and token provider it needs at runtime.
    curl -fsSL -o "$JARS/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
        "$MAVEN/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"; \
    curl -fsSL -o "$JARS/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
        "$MAVEN/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"; \
    curl -fsSL -o "$JARS/kafka-clients-3.4.1.jar" \
        "$MAVEN/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar"; \
    # commons-pool2 is required by the Kafka source's consumer pool; omitting it
    # produces a ClassNotFoundException only once a stream actually starts.
    curl -fsSL -o "$JARS/commons-pool2-2.11.1.jar" \
        "$MAVEN/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"; \
    # s3a filesystem for the curated Parquet archive.
    curl -fsSL -o "$JARS/hadoop-aws-${HADOOP_VERSION}.jar" \
        "$MAVEN/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar"; \
    curl -fsSL -o "$JARS/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
        "$MAVEN/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

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
