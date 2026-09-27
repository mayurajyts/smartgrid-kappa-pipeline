# ADR-004 — Curated Parquet archive on a local volume, not the S3 gateway

**Status:** Accepted
**Date:** 2026-09-26 (Phase 2)
**Relates to:** `IMPLEMENTATION_PLAN.md` §3 (curated archive), §7 Job A step 5,
and [`ADR-003-object-store.md`](ADR-003-object-store.md)

---

## Context

§7 Job A step 5 requires the validated readings to be archived as Parquet
"partitioned by `sim_date`, `grid_zone`", and §3 places that archive on
S3-compatible object storage. ADR-003 already recorded the substitution of
SeaweedFS for MinIO after MinIO gated its container images.

During Phase 2, Job A's Parquet sink failed on **every** micro-batch:

```
java.io.IOException: Failed to rename S3AFileStatus{
  path=s3a://smartgrid-curated/readings/_temporary/0/_temporary/
       attempt_.../sim_date=2026-01-01/grid_zone=ZONE-A/part-00000-....parquet
  ...} to s3a://smartgrid-curated/readings/sim_date=2026-01-01/grid_zone=ZONE-A/
       part-00000-....parquet
org.apache.spark.SparkException: [TASK_WRITE_FAILED]
```

The failure is in the **commit**, not the write. Files reached the object store
correctly — 30 Parquet part files were present, with the right
`sim_date=/grid_zone=` partition paths — but all of them were stranded under
`_temporary/`, and `0` files were ever committed. Because Job A writes all three
sinks from one `foreachBatch`, the aborted Parquet write failed the whole batch, so
the DLQ and clean-topic writes were rolled back with it.

### Why the rename fails

Hadoop's default `FileOutputCommitter` finalises a job by writing each task's
output to a `_temporary` directory and then **renaming** it into place. Rename is a
single atomic metadata operation on HDFS. On object storage there is no rename: the
S3A connector emulates it as copy-then-delete, and that emulation depends on
specific S3 semantics that SeaweedFS's gateway does not fully implement for these
paths.

This was isolated with a minimal 2-row write, so it is not a data-volume,
memory, or Job A logic problem. Plain S3 operations through the same gateway all
succeed — verified with boto3: `list_buckets`, `create_bucket`, `put_object` and
even `copy_object` return OK. The incompatibility is specific to the committer's
rename sequence.

### Alternatives tested and rejected

| Option | Result |
|---|---|
| S3A **magic** committer (`fs.s3a.committer.name=magic`) — avoids rename entirely using multipart uploads | **Fails.** Requires `PathOutputCommitProtocol` / `BindingParquetOutputCommitter` from the `spark-hadoop-cloud` artifact, which this Spark distribution does not bundle. |
| S3A **directory** staging committer | **Fails**, same missing artifact. |
| Add `spark-hadoop-cloud_2.12:3.5.3` to the image and use the magic committer | Plausible, but magic-committer support against SeaweedFS is unverified — it would be a second speculative dependency on the same gateway that just failed. Rejected as risk without evidence. |
| Drop the Parquet sink | Rejected: leaves §7 step 5 unimplemented and removes the artefact the restart-idempotence checkpoint is tested against. |

## Decision

**Write the curated Parquet archive to a Docker named volume** (`/data/curated/readings`
via the plain `file://` path), keeping the `sim_date`/`grid_zone` partitioning
unchanged.

The S3-compatible object store remains in the stack and remains the *documented*
production location for the archive.

## Rationale

The architectural requirement is a **columnar archive partitioned by the predicates
that replay and the daily report filter on**. That is what earns the marks in §3 and
§7, and it is satisfied identically on a volume:

| Requirement from §3 / §7 | Met on a volume |
|---|---|
| Columnar (Parquet) format | Yes — identical files, identical writer |
| Partitioned by `sim_date`/`grid_zone` | Yes — identical layout, so partition pruning works the same |
| Supports the production-scale replay story | Yes — `replay.py` reads the same paths through the same DataFrame API |
| Demonstrates a curated archive distinct from the serving store | Yes |

What is genuinely lost is the *demonstration* that the archive lives on object
storage. That is a real reduction in fidelity and it is stated plainly in the
report's limitations section rather than glossed over.

**The honest framing for the viva** is that this is the same problem production
systems hit, and the production answer is not "use a different filesystem" — it is a
**transactional table format**. Delta Lake and Apache Iceberg exist substantially
because `FileOutputCommitter` is unsafe and slow on object storage: they commit
through an atomic metadata log rather than through directory renames. That fix was
already named in the report's production-scale section (§12 item 10) for a different
reason — exactly-once sink semantics — and this incident is concrete evidence for why
it matters. Encountering the rename problem first-hand is a better answer than never
having met it.

## Consequences

**Positive**
- Job A commits reliably, so both halves of the Phase 2 checkpoint become testable.
- The archive survives container restarts (named volume), which restart-idempotence
  requires.
- No speculative dependency on unverified committer support.

**Negative**
- The archive is not on object storage, so the S3A write path is configured and
  demonstrated (the session builder still sets it up) but not exercised by the
  archive itself.
- A volume does not scale the way object storage does — irrelevant at megabytes,
  material at utility scale, and listed with the other demonstration-scale
  limitations alongside the single Kafka broker.

**Neutral**
- `processing/spark_session.py` keeps its full S3A configuration, so moving the
  archive back to `s3a://` is a one-line change to `PARQUET_PATH` once a working
  committer is available. The deviation is a configuration value, not a code path.
