# ADR-003 — S3-compatible object store for the curated archive

**Status:** Accepted
**Date:** 2026-09-26 (Phase 0)
**Supersedes:** the specific vendor named in `IMPLEMENTATION_PLAN.md` §3; the
architectural requirement itself is unchanged.

---

## Context

The implementation plan (§3, §5) specifies **MinIO** as the S3-compatible object
store holding the curated Parquet archive of validated readings, partitioned by
`sim_date`/`grid_zone`. The archive supports the production-scale replay story:
at utility scale, replay would read columnar files from object storage rather
than relying on unbounded Kafka retention.

During Phase 0, MinIO's container images were found to require **registry
authentication** on both Docker Hub (`minio/minio`, `minio/mc`) and quay.io
(`quay.io/minio/minio`), returning `401 UNAUTHORIZED` on an unauthenticated
pull. This was verified directly:

```
$ docker pull quay.io/minio/minio:RELEASE.2024-10-13T13-34-11Z
Error response from daemon: unknown: failed to resolve reference ...
  unexpected status from HEAD request ...: 401 Unauthorized
```

This conflicts with a property the project is **directly assessed on**:
reproducibility from a clean clone (§3, §13 — Code Quality & Documentation).
A marker running `make up` on a fresh machine would have the stack fail at the
object store unless they first obtained MinIO credentials and ran `docker login`.

## Decision

**Substitute SeaweedFS (`chrislusf/seaweedfs:3.80`) for MinIO**, running its
S3 gateway (`server -s3`).

The variables and settings class are renamed from vendor-specific
(`MINIO_*`, `MinioSettings`) to API-specific (`S3_*`, `ObjectStoreSettings`), to
make it explicit in the code that the dependency is on the S3 API, not on any
particular implementation.

## Rationale

**The architectural requirement was never "MinIO".** §3 justifies the choice as
"Parquet on MinIO (S3-compatible)" — a *columnar archive on S3-compatible
object storage*, rejected against HDFS on the grounds that HDFS is "heavier to
run in Compose for identical benefit at this scale". Every clause of that
justification holds for SeaweedFS:

| Requirement from §3 | Met by SeaweedFS |
|---|---|
| S3-compatible API | Yes — a real S3 gateway; `ListBuckets`, `PutObject`, `GetObject` verified returning correct S3 XML |
| Columnar Parquet archive, partitioned | Yes — object storage; partitioning is a key-prefix concern, not a store concern |
| Lighter than HDFS in Compose | Yes — one container, one process |
| Realistic production write path | Yes — Spark writes via `s3a://` identically |

**Spark's write path is unchanged.** Phase 2 configures the `s3a` connector with
an endpoint, access key and secret key. Those values now point at a different
container; no code differs.

**It is a real object store, not a mock.** `adobe/s3mock` and
`localstack/localstack` were also confirmed to pull without authentication, but
both are testing fixtures. SeaweedFS is a production distributed object store
with persistent volume storage, which keeps the archive's demonstration honest —
the report claims a curated archive on object storage, and that is what exists.

**The report is unaffected in substance.** §3's row should now read
"Parquet on an S3-compatible object store (SeaweedFS)", with the same
justification and the same rejected alternative (HDFS). If asked in the viva why
not MinIO, the answer is this ADR: MinIO gated its images, and depending on a
credentialed registry would have traded an assessed property (reproducibility)
for a vendor name that the architecture does not actually require.

## Consequences

**Positive**
- `make up` works from a clean clone with no registry credentials.
- Naming now reflects the true dependency (the S3 API), so a future swap needs
  no code change — only `.env`.
- The bucket-init script uses a plain S3 `PUT` rather than a vendor CLI (`mc`),
  so it too is implementation-independent.

**Negative**
- SeaweedFS's browser UI is a filer view, less polished than MinIO's console.
  Cosmetic; the demo shows the archive through the S3 API and Spark regardless.
- SeaweedFS is less widely recognised than MinIO, so the substitution needs
  explaining — which is what this ADR is for.
- `server -s3` runs master, volume, filer and gateway in one process. Acceptable
  for the same reason the single Kafka broker is (§14): this demonstrates an
  architecture, it is not a highly-available deployment. Listed alongside the
  other single-point-of-failure limitations in the report.

**Neutral**
- Deviating from the plan is itself recorded rather than silent. The plan is the
  specification; where reality contradicts it, the contradiction and its
  resolution are documented, which is the behaviour the plan asks for.
