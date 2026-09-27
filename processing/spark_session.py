"""SparkSession construction, shared by every streaming job and by replay.

WHY THIS EXISTS IN THE ARCHITECTURE
-----------------------------------
Jobs A-D and `replay.py` all need the same Kafka and S3A configuration. Building it
once here is not merely tidiness — two of the settings below are correctness
requirements that would fail silently if a job got them wrong:

  * THE S3A ENDPOINT. If Job A wrote the curated archive to one endpoint and the
    replay entrypoint read from another, replay would silently find no data and
    report a successful run over zero rows. A bill recomputed from nothing is not a
    bill that failed — it is a bill that is quietly wrong.

  * THE CHECKPOINT ROOT. Checkpoints are what make the sinks idempotent across
    restarts (the Phase 2 checkpoint requirement). A job that defaulted its
    checkpoint to a container-local path would lose it on restart and reprocess
    from the beginning of the topic, duplicating everything it had already written.

This module also centralises the "one codebase, live and replay" claim from §2.2:
the same builder serves both, differing only in the settings passed to it.

PATH-STYLE S3 ACCESS (a SeaweedFS/MinIO requirement)
----------------------------------------------------
`fs.s3a.path.style.access=true` is mandatory for any S3-compatible store that is not
AWS. The default is virtual-host style, which resolves
`https://bucket.endpoint/key` — a hostname that does not exist for a local object
store. The failure surfaces as an UnknownHostException deep inside a Spark task,
which reads as a networking problem rather than a configuration one.

COMMITTER CHOICE, AND WHY IT MATTERS HERE
-----------------------------------------
The default `FileOutputCommitter` v1 writes to a temporary directory and renames on
commit. Rename is atomic on HDFS but is a COPY on object stores, which is slow and,
on S3, not atomic at all. We therefore set the v2 algorithm and disable the
`_SUCCESS` marker.

TRADE-OFF (deliberate): committer v2 makes partial output visible if a task fails
mid-commit, whereas v1 hides it until the whole job commits. That is acceptable here
because the Parquet archive is NOT the serving path — nothing queries it live, and
Job A's checkpoint means a failed micro-batch is replayed in full. The report's
production-scale section names the proper fix: a transactional table format
(Delta/Iceberg) which gives atomic commits on object storage without the rename.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from common.config import get_settings


def build_spark_session(app_name: str, shuffle_partitions: int = 6) -> SparkSession:
    """Build a configured SparkSession.

    Args:
        app_name: identifies the job in the Spark UI and in driver logs. Every job
            passes its own, so a running query is attributable to a source file.
        shuffle_partitions: default 6, matching the telemetry topic's partition
            count. Spark's default is 200, which on this data volume would create
            200 near-empty tasks per aggregation — scheduling overhead far exceeding
            the work itself, and micro-batch durations dominated by task setup.

    Returns:
        An active SparkSession.
    """
    settings = get_settings()
    store = settings.objectstore

    builder = (
        SparkSession.builder.appName(app_name)
        # --- Streaming behaviour ---
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        # Adaptive query execution is disabled for streaming: it coalesces
        # partitions based on runtime statistics, which changes the physical plan
        # between micro-batches. For a system whose central claim is that a replay
        # reproduces the original run, a plan that varies with observed data volume
        # is an unnecessary source of non-determinism.
        .config("spark.sql.adaptive.enabled", "false")
        # Fail fast on ambiguous datetime parsing rather than silently correcting
        # it. A timestamp quietly shifted by a timezone assumption would put a
        # reading in the wrong sim_date and bill it against the wrong day's tariff.
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.session.timeZone", "UTC")
        # --- S3A for the curated Parquet archive ---
        .config("spark.hadoop.fs.s3a.endpoint", store.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", store.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", store.s3_secret_key)
        # Mandatory for non-AWS S3 — see the module docstring.
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        # The object store has no IAM; a simple static credential provider avoids
        # the default chain spending its timeout probing EC2 instance metadata that
        # does not exist, which otherwise adds seconds to every first write.
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # --- Output committer: see the module docstring on the trade-off ---
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config(
            "spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
        )
        # Parquet is written and read by this codebase only, so the newer writer
        # is safe and produces better statistics for partition pruning.
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    )

    spark = builder.getOrCreate()
    # WARN, not INFO: at a 5-second trigger, Spark's INFO logging emits tens of
    # lines per micro-batch and would bury this project's own structured JSON logs,
    # which are the evidence for the observability criterion.
    spark.sparkContext.setLogLevel("WARN")
    return spark


def kafka_read_options(
    topic: str,
    starting_offsets: str = "latest",
    max_offsets_per_trigger: int = 2000,
) -> dict[str, str]:
    """Kafka source options for a streaming read.

    Args:
        topic: topic to subscribe to.
        starting_offsets: "latest" for a live job, "earliest" for replay. This single
            parameter is most of what separates a live run from a reprocessing run —
            concrete evidence for the single-codebase claim in §2.2d.
        max_offsets_per_trigger: ceiling on records consumed per micro-batch. This
            is NOT just a memory guard — it bounds the EVENT-TIME SPAN of a batch,
            which must stay inside the watermark. See the note below.

    Returns:
        Options for `spark.readStream.format("kafka").options(**...)`.
    """
    settings = get_settings()
    return {
        "kafka.bootstrap.servers": settings.kafka.kafka_bootstrap_servers,
        "subscribe": topic,
        "startingOffsets": starting_offsets,
        # false, deliberately. The default true KILLS the query if data it needs has
        # aged out of the topic. Since Kafka retention here is configured to cover
        # the entire simulated history (the premise of the replay argument), losing
        # data means something is genuinely wrong, and a loud failure is correct —
        # silently skipping a gap would produce bills missing hours of consumption
        # with nothing to indicate it.
        "failOnDataLoss": "true",
        # Bounds how much one micro-batch may consume. Two reasons, and the second
        # is the one that actually bit:
        #
        # 1. Without any limit, a job restarting after downtime tries to process the
        #    entire backlog in one batch and dies on executor memory — the classic
        #    streaming-restart failure.
        #
        # 2. IT BOUNDS THE EVENT-TIME SPAN OF A BATCH, which must stay inside the
        #    watermark. The producer stamps 200 readings per tick and ticks are 9.6
        #    SIMULATED minutes apart under 288x compression, so consuming N records
        #    spans (N/200) x 9.6 simulated minutes. At 20000 the span was 960
        #    simulated minutes — 16 simulated hours — far beyond a 576-minute
        #    watermark, so the oldest records in every batch were already "too late"
        #    and `dropDuplicates` discarded them.
        #
        #    The symptom was badly misleading: 1.18M records on the topic, Job A
        #    emitting 0 valid rows, and a 100% reject rate composed entirely of
        #    injected faults. The clean records were not rejected (nothing appeared in
        #    the DLQ to explain them) — they were silently dropped by the watermark.
        #
        #    At 2000 the span is 96 simulated minutes, comfortably inside the
        #    watermark with ~6x margin.
        #
        # TRADE-OFF: a lower ceiling means more, smaller batches and therefore a
        # longer drain after downtime. That is the right way round: a slow catch-up is
        # visible and recoverable, whereas silently dropped readings are neither.
        "maxOffsetsPerTrigger": str(max_offsets_per_trigger),
        # Surfaces the correlation-id header so it can be traced without parsing the
        # payload (§8).
        "includeHeaders": "true",
    }


def kafka_write_options(topic: str, checkpoint_location: str) -> dict[str, str]:
    """Kafka sink options for a streaming write."""
    settings = get_settings()
    return {
        "kafka.bootstrap.servers": settings.kafka.kafka_bootstrap_servers,
        "topic": topic,
        "checkpointLocation": checkpoint_location,
    }
