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
# WHY IT DOES NOT SIGN THE REQUEST:
# the object store's identity config grants the `anonymous` identity Read and
# List only, so an unsigned PUT is refused with 403. Rather than implement AWS
# SigV4 in shell (which needs an HMAC chain over a canonical request - a lot of
# fragile code for one call), this script treats 403 on an EXISTING bucket as
# success: the bucket is created on the first run, when the store has no identity
# config loaded yet, and thereafter the GET below proves it is present. A 403 with
# the bucket absent is still a hard failure.
#
# Idempotent: an existing bucket returns 409 BucketAlreadyOwnedByYou (or 403 once
# authentication is enforced), both treated as success so `make up` is re-runnable.
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
  403)
    # Anonymous writes are denied once the identity config is loaded. Verify the
    # bucket exists anyway; if it does, there is nothing to do and this is not an
    # error. If it does not, fail loudly - Job A would otherwise start and fail on
    # its first Parquet write with a much less obvious message.
    if curl -sf -o /dev/null "${S3_ENDPOINT}/${S3_CURATED_BUCKET}/"; then
      echo "[objectstore-init] bucket exists: ${S3_CURATED_BUCKET} (anonymous writes denied, which is expected)"
    else
      echo "[objectstore-init] FAILED: bucket ${S3_CURATED_BUCKET} is absent and anonymous creation is denied (HTTP 403)." >&2
      echo "[objectstore-init] Create it with credentials, or run 'make clean' to reinitialise the store." >&2
      exit 1
    fi ;;
  *)
    echo "[objectstore-init] FAILED to create bucket (HTTP ${STATUS})" >&2
    exit 1 ;;
esac

echo "[objectstore-init] bucket inventory:"
curl -s "${S3_ENDPOINT}/"
echo ""
echo "[objectstore-init] complete"
