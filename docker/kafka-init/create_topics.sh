#!/bin/bash
# =============================================================================
# Creates the Kafka topics the architecture depends on (plan §6).
#
# WHY THIS IS A CONTAINER AND NOT A README INSTRUCTION:
# The topic CONFIGURATION is load-bearing, not incidental. Two settings in
# particular are architectural decisions, not tuning:
#
#   1. cleanup.policy=compact on the two reference topics. Compaction is what
#      gives "latest tariff per household, retained forever" for free. That
#      compacted topic IS the broadcast dimension for Job C's stream-static
#      join (§2.2a). Without it the tariff topic ages out and the join silently
#      starts producing nulls -> bills with no tariff.
#
#   2. 6 partitions on the telemetry topic, keyed by grid_zone. Kafka guarantees
#      ordering only WITHIN a partition, so keying by zone is what gives the
#      per-zone ordering the 1-minute tumbling aggregates rely on.
#
# If a marker had to set these by hand, a clean clone would not reproduce the
# system. Auto-creation would be worse still: Kafka's defaults are 1 partition
# and delete-retention, i.e. exactly wrong on both counts, and the failure would
# be silent.
#
# Idempotent (--if-not-exists), so `make up` is safe to re-run.
# =============================================================================
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS}"

echo "[kafka-init] waiting for broker at ${BOOTSTRAP}"
until kafka-topics --bootstrap-server "${BOOTSTRAP}" --list >/dev/null 2>&1; do
  sleep 2
done
echo "[kafka-init] broker is up"

# --- Telemetry: the Kappa log ------------------------------------------------
# retention.ms must cover the FULL simulated history. Replay is the
# architecture, not a recovery feature: if the log ages out, restating a past
# day's bill (R7) becomes impossible and the Kappa claim is unsupportable.
kafka-topics --bootstrap-server "${BOOTSTRAP}" --create --if-not-exists \
  --topic "${TOPIC_METER_READINGS}" \
  --partitions "${TOPIC_METER_PARTITIONS}" \
  --replication-factor "${TOPIC_REPLICATION_FACTOR}" \
  --config retention.ms="${TOPIC_METER_RETENTION_MS}" \
  --config cleanup.policy=delete

# Validated output of Job A. Same partitioning as the raw topic so a zone's
# records stay co-partitioned end to end and downstream ordering is preserved.
kafka-topics --bootstrap-server "${BOOTSTRAP}" --create --if-not-exists \
  --topic "${TOPIC_METER_READINGS_CLEAN}" \
  --partitions "${TOPIC_METER_PARTITIONS}" \
  --replication-factor "${TOPIC_REPLICATION_FACTOR}" \
  --config retention.ms="${TOPIC_METER_RETENTION_MS}" \
  --config cleanup.policy=delete

# Dead-letter queue. Same retention as the main topic: a rejected record is
# evidence, and the §11 step 7 trace demo needs it to still be there at
# demo time.
kafka-topics --bootstrap-server "${BOOTSTRAP}" --create --if-not-exists \
  --topic "${TOPIC_METER_READINGS_DLQ}" \
  --partitions 1 \
  --replication-factor "${TOPIC_REPLICATION_FACTOR}" \
  --config retention.ms="${TOPIC_METER_RETENTION_MS}" \
  --config cleanup.policy=delete

# --- Reference dimensions: COMPACTED ----------------------------------------
# These are slowly-changing dimensions, not fact feeds (§2.2a). Compaction
# retains the latest value per key indefinitely, which is what lets the stream
# job hold them as broadcast state.
#
# The segment/cleanup settings below exist because Kafka only compacts CLOSED
# segments. With defaults (1GB segments, 7-day roll) the active segment would
# never close at this tiny data volume, so compaction would never visibly
# happen and the checkpoint could not be demonstrated. Small segments and an
# aggressive cleanup ratio make compaction observable within a demo.
# TRADE-OFF: more, smaller segments means more file handles and more frequent
# cleaner work. Irrelevant at hundreds of records; would be retuned at scale.
for REFERENCE_TOPIC in "${TOPIC_TARIFF_REFERENCE}" "${TOPIC_WEATHER_FORECAST}"; do
  kafka-topics --bootstrap-server "${BOOTSTRAP}" --create --if-not-exists \
    --topic "${REFERENCE_TOPIC}" \
    --partitions "${TOPIC_REFERENCE_PARTITIONS}" \
    --replication-factor "${TOPIC_REPLICATION_FACTOR}" \
    --config cleanup.policy=compact \
    --config min.cleanable.dirty.ratio=0.01 \
    --config segment.ms=60000 \
    --config delete.retention.ms=60000 \
    --config min.compaction.lag.ms=0
done

# --- Checkpoint evidence -----------------------------------------------------
# Printing the effective config here is what makes the Phase 0 checkpoint
# ("topics created with correct compaction settings") self-evidencing: the
# proof is in `docker compose logs kafka-init`, not in a manual check.
echo ""
echo "=================================================================="
echo "[kafka-init] TOPIC INVENTORY"
echo "=================================================================="
kafka-topics --bootstrap-server "${BOOTSTRAP}" --list
echo ""
for TOPIC in "${TOPIC_METER_READINGS}" "${TOPIC_METER_READINGS_CLEAN}" \
             "${TOPIC_METER_READINGS_DLQ}" "${TOPIC_TARIFF_REFERENCE}" \
             "${TOPIC_WEATHER_FORECAST}"; do
  echo "------------------------------------------------------------------"
  kafka-topics --bootstrap-server "${BOOTSTRAP}" --describe --topic "${TOPIC}"
done
echo "=================================================================="
echo "[kafka-init] complete"
