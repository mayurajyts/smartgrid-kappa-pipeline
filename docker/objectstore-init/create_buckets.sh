#!/bin/sh
# =============================================================================
# Creates the bucket holding the curated Parquet archive (plan §3).
#
# WHY THE ARCHIVE EXISTS AT ALL, given Kafka already retains everything:
# it is the PRODUCTION-SCALE REPLAY STORY made concrete. At this project's
# volume, replay reads from Kafka because a week of simulated history is a few
# megabytes. At utility scale, retaining years of telemetry in Kafka is not
# economic, and replay would instead read columnar Parquet from object storage
# and feed the SAME streaming code. Writing Job A's validated output here in
# Phase 2 demonstrates that path without pretending to operate at that scale.
#
# Parquet is partitioned by sim_date/grid_zone — the two predicates every
# replay and every report filters on, so partition pruning skips whole files.
#
# Bucket creation uses a plain S3 PUT rather than a vendor CLI: it depends only
# on the S3 API the architecture actually requires, so swapping the object
# store implementation does not require rewriting this script.
#
# Idempotent: an existing bucket returns 409 BucketAlreadyOwnedByYou, which is
# treated as success so re-running `make up` is safe.
# =============================================================================
set -eu

echo "[objectstore-init] waiting for the S3 gateway at ${S3_ENDPOINT}"
until curl -sf -o /dev/null "${S3_ENDPOINT}/"; do
  sleep 2
done
echo "[objectstore-init] S3 gateway is up"

STATUS=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "${S3_ENDPOINT}/${S3_CURATED_BUCKET}")

case "${STATUS}" in
  200|201|204)
    echo "[objectstore-init] created bucket: ${S3_CURATED_BUCKET}" ;;
  409)
    echo "[objectstore-init] bucket already exists: ${S3_CURATED_BUCKET}" ;;
  *)
    echo "[objectstore-init] FAILED to create bucket (HTTP ${STATUS})" >&2
    exit 1 ;;
esac

echo "[objectstore-init] bucket inventory:"
curl -s "${S3_ENDPOINT}/"
echo ""
echo "[objectstore-init] complete"
