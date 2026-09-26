"""Prometheus collectors — defined once so metric names and labels cannot drift.

WHY THIS EXISTS IN THE ARCHITECTURE
-----------------------------------
The Prometheus alert rules in §8 are written against specific metric names and
label sets. `HighRejectRate` divides a rejected counter by a consumed counter;
that division silently returns nothing if two services spell a label `job` vs
`job_name`, or if one omits it. Alerting rules that quietly never fire are worse
than no alerting, because they look like health.

So every counter, gauge and histogram in the §8 table is declared here, in one
registry, and services import them rather than constructing their own. A typo
becomes an ImportError at startup instead of an alert that never fires.

This module DECLARES the instruments; the code that increments them arrives with
the jobs in later phases. Declaring them up front also means a freshly started
service exposes all its series at zero, so a Grafana panel shows "0" rather than
"No data" before the first event — a meaningful distinction when the thing you
are diagnosing is whether data is flowing at all.

TRADE-OFF (deliberate)
----------------------
Spark's executors run in separate JVM/Python processes that Prometheus cannot
usefully scrape individually. We therefore instrument in the DRIVER, inside
`foreachBatch` and a `StreamingQueryListener`, and expose one endpoint per job
from the driver process (§8 asks us to pick one approach and document it). The
cost is that these are per-micro-batch aggregates, not per-executor detail; we
lose the ability to spot a single straggling executor. For this workload —
200 meters, a handful of partitions — driver-side aggregates are sufficient, and
the alternative (a Pushgateway) adds a component whose stale-metric semantics
would mislead the staleness alerts that matter most here.
"""

from __future__ import annotations

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)

# An explicit registry rather than the global default: the Spark driver and the
# FastAPI app both run inside processes that other libraries may instrument, and
# an explicit registry keeps our series separable and re-creatable under test.
REGISTRY = CollectorRegistry()

# Latency buckets tuned to the §2.4 budget: the zone path targets <10s
# end-to-end and alerts <15s, so resolution matters most below ~15s. Default
# prometheus_client buckets top out too low to show a stalled micro-batch.
_LATENCY_BUCKETS = (0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 15.0, 30.0, 60.0, 120.0)


# --- Ingestion -------------------------------------------------------------

events_produced_total = Counter(
    "smartgrid_events_produced_total",
    "Events published to Kafka by a source.",
    ["source", "grid_zone"],
    registry=REGISTRY,
)

events_consumed_total = Counter(
    "smartgrid_events_consumed_total",
    "Events read from a Kafka topic by a job.",
    ["job", "topic"],
    registry=REGISTRY,
)

events_rejected_total = Counter(
    "smartgrid_events_rejected_total",
    "Events routed to the DLQ, labelled by rejection reason.",
    ["job", "reason"],
    registry=REGISTRY,
)


# --- Processing ------------------------------------------------------------

processing_latency_seconds = Histogram(
    "smartgrid_processing_latency_seconds",
    "End-to-end latency from event_timestamp to processing completion.",
    ["job"],
    buckets=_LATENCY_BUCKETS,
    registry=REGISTRY,
)

batch_micro_duration_seconds = Histogram(
    "smartgrid_batch_micro_duration_seconds",
    "Wall-clock duration of one Structured Streaming micro-batch.",
    ["job"],
    buckets=_LATENCY_BUCKETS,
    registry=REGISTRY,
)

# Freshness gauge. `NoDataReceived` (§8 rule 1) alerts on
# `time() - this > 120`, which is why it must be a unix timestamp in seconds
# and must be set even when a zone reports zero consumption. A zone that is
# quiet is healthy; a zone that is silent is not, and only this gauge
# distinguishes them.
last_event_timestamp_seconds = Gauge(
    "smartgrid_last_event_timestamp_seconds",
    "Unix timestamp of the most recent event seen for a zone.",
    ["grid_zone"],
    registry=REGISTRY,
)

kafka_consumer_lag = Gauge(
    "smartgrid_kafka_consumer_lag",
    "Uncommitted records between a consumer group's offset and the log end.",
    ["topic", "group"],
    registry=REGISTRY,
)


# --- Business-level --------------------------------------------------------

zone_renewable_pct = Gauge(
    "smartgrid_zone_renewable_pct",
    "Share of a zone's consumption met by solar generation, percent.",
    ["grid_zone"],
    registry=REGISTRY,
)

active_alerts = Gauge(
    "smartgrid_active_alerts",
    "Currently unresolved alerts.",
    ["alert_type", "severity"],
    registry=REGISTRY,
)

daily_report_success_total = Counter(
    "smartgrid_daily_report_success_total",
    "Daily billing reports generated successfully.",
    registry=REGISTRY,
)

# Unlabelled gauge of the current simulated day index. Lets any dashboard panel
# be read against simulated rather than real time, and backs the
# `DailyReportMissed` rule's notion of a day having passed.
sim_day_current = Gauge(
    "smartgrid_sim_day_current",
    "Simulated day index currently in progress, counted from the sim epoch.",
    registry=REGISTRY,
)


def start_metrics_server(port: int | None = None) -> int:
    """Expose /metrics for Prometheus to scrape, returning the bound port.

    Every long-lived service calls this at startup. Short-lived processes (the
    one-shot init containers, Airflow tasks) do not: Prometheus is a pull-based
    system and cannot scrape a process that has already exited, which is why
    Airflow's outcomes are recorded as counters on a long-lived exporter rather
    than scraped from the task itself.
    """
    from common.config import get_settings

    resolved = port if port is not None else get_settings().logging.metrics_port
    start_http_server(resolved, registry=REGISTRY)
    return resolved
