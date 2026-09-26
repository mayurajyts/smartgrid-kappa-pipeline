"""Kafka producer wrapper shared by the simulators and the batch loader.

WHY THIS EXISTS IN THE ARCHITECTURE
-----------------------------------
Three processes publish to the log — the meter simulator, and the batch loader for
each of the two reference topics. Wrapping the client once means they cannot
disagree about the two things that are architecturally load-bearing:

  1. THE KEY. `meter.readings.v1` is keyed by `grid_zone` because Kafka guarantees
     ordering only within a partition, and the 1-minute tumbling zone aggregates
     depend on per-zone ordering. The reference topics are keyed by
     `household_id` and `grid_zone|sim_date` because those keys are what log
     compaction retains the latest value per. A producer that forgot to set a key
     would round-robin its records across partitions, destroying both properties
     silently — the data would all still be there, just unusable for the join and
     unordered for the windows.

  2. THE CORRELATION ID HEADER. §8 requires the id to be "carried on the Kafka
     record header", so a consumer can trace a record without deserialising its
     payload. Setting it in one place means it cannot be omitted by one producer.

DELIVERY GUARANTEE: at-least-once, deliberately
-----------------------------------------------
Configured with `acks=all` and `enable.idempotence=true`. That gives per-partition
exactly-once *produce* semantics — no duplicates from a broker-side retry — while
the overall pipeline remains at-least-once end to end, which the idempotent
Postgres upserts in Phase 3 then turn into effectively-once storage.

Note this sits alongside the DELIBERATE duplicates from fault injection. Those are
application-level duplicates with the same `event_id`, published as separate
records on purpose to exercise Job A's `dropDuplicates`. Producer idempotence
prevents accidental protocol-level duplicates; it does not and must not suppress
the intentional ones.

TRADE-OFF (deliberate)
----------------------
`confluent-kafka` (librdkafka) over the pure-Python `kafka-python`: it is the
client matching the Confluent broker in use, and it exposes per-record delivery
callbacks and record headers, both of which this design needs. The cost is a
compiled dependency — mitigated by the fact that prebuilt wheels exist for
CPython 3.11 on Windows, macOS and Linux, so no compiler is needed on a marker's
machine.

Delivery is ASYNCHRONOUS with a callback, not a blocking flush per record. At ~200
meters every 2 seconds a synchronous round trip per record would dominate the tick
budget. The cost is that a delivery failure is reported slightly after the fact,
which is why failures increment a counter and log rather than raising into the
producer loop — a transient broker hiccup must not kill the data source.
"""

from __future__ import annotations

import json
from typing import Any, Callable

import structlog
from confluent_kafka import Producer

from common.config import get_settings

# Header name carrying the correlation id. A constant rather than a literal, so
# the producers that set it and the consumers that read it cannot drift.
CORRELATION_ID_HEADER = "correlation_id"


class KafkaPublisher:
    """Thin publisher: JSON value, string key, correlation-id header."""

    def __init__(
        self,
        client_id: str,
        logger: structlog.stdlib.BoundLogger,
        on_delivery_failure: Callable[[str], None] | None = None,
    ) -> None:
        """
        Args:
            client_id: identifies this producer in Kafka's own metrics and logs,
                which is what makes broker-side diagnostics attributable to a
                specific container.
            logger: a configured structlog logger; delivery failures are logged
                through it so they carry the mandatory §8 fields.
            on_delivery_failure: optional hook, called with the topic name on a
                failed delivery, so the caller can increment a metric without this
                module importing the metric registry.
        """
        settings = get_settings()
        self._logger = logger
        self._on_delivery_failure = on_delivery_failure

        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka.kafka_bootstrap_servers,
                "client.id": client_id,
                # acks=all: the record is acknowledged only once every in-sync
                # replica has it. With a single broker this is equivalent to
                # acks=1, but it is written explicitly so the configuration stays
                # correct if the cluster is ever given replicas — the production
                # change named in the report.
                "acks": "all",
                # Idempotent produce: the broker deduplicates retried records by
                # producer id and sequence number, so a network retry cannot
                # create an accidental duplicate. See the module docstring on why
                # this does not interfere with injected duplicates.
                "enable.idempotence": True,
                # Small linger to batch records within a tick. At 200 records per
                # tick this materially reduces request count; 5ms is far below the
                # <10s end-to-end latency budget in §2.4, so it costs nothing
                # observable.
                "linger.ms": 5,
                "compression.type": "snappy",
            }
        )

    def _delivery_report(self, err: Any, msg: Any) -> None:
        """Delivery callback: log failures, stay silent on success.

        Silent on success deliberately — §8 requires logging at stage boundaries
        with aggregate counts, not per record. At 200 records every 2 seconds,
        per-record success logging would produce more log volume than data volume
        and would itself become the bottleneck.
        """
        if err is None:
            return
        topic = msg.topic() if msg is not None else "unknown"
        self._logger.error(
            "kafka_delivery_failed",
            stage="ingest",
            topic=topic,
            error=str(err),
        )
        if self._on_delivery_failure is not None:
            self._on_delivery_failure(topic)

    def publish(
        self,
        topic: str,
        key: str,
        payload: dict[str, Any],
        correlation_id: str | None = None,
    ) -> None:
        """Queue one record for asynchronous delivery.

        Args:
            topic: destination topic.
            key: partition/compaction key. Required, not optional — see the module
                docstring: a keyless record silently breaks both zone ordering and
                compaction.
            payload: JSON-serialisable dict.
            correlation_id: set on the record header; defaults to the payload's own
                `correlation_id` when present.
        """
        if not key:
            raise ValueError(
                f"a key is required when publishing to {topic}: an unkeyed record "
                "round-robins across partitions, which breaks per-zone ordering "
                "and log compaction"
            )

        headers = None
        cid = correlation_id or payload.get("correlation_id")
        if cid:
            headers = [(CORRELATION_ID_HEADER, str(cid).encode("utf-8"))]

        try:
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
                headers=headers,
                on_delivery=self._delivery_report,
            )
        except BufferError:
            # The local queue is full: the broker is not keeping up. Serve the
            # delivery callbacks to drain it, then retry once. Dropping the record
            # instead would create a silent gap in the log that no downstream
            # count could explain.
            self._logger.warning(
                "kafka_queue_full_draining", stage="ingest", topic=topic
            )
            self._producer.poll(1.0)
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
                headers=headers,
                on_delivery=self._delivery_report,
            )

    def poll(self, timeout: float = 0.0) -> int:
        """Serve delivery callbacks. Called once per tick by the producer loop."""
        return self._producer.poll(timeout)

    def flush(self, timeout: float = 10.0) -> int:
        """Block until the queue drains; returns the number still undelivered.

        Called on shutdown so a stopped container does not silently lose records
        it had already accepted and reported.
        """
        return self._producer.flush(timeout)
