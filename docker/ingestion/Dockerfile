# =============================================================================
# Image for the batch loader: drop-directory watcher -> compacted Kafka topics.
#
# Depends on `simulators/kafka_producer.py` for the shared publisher, so that the
# key and correlation-id header conventions are identical to the meter
# simulator's. A separate publisher implementation here could set a different key
# and silently break log compaction.
# =============================================================================
FROM smartgrid-base:latest

COPY ingestion/ /app/ingestion/
COPY simulators/ /app/simulators/

CMD ["python", "-m", "ingestion.batch_loader"]
