# EC8202 — Applied Big Data Engineering Mini-Project
## Implementation Plan: Smart Grid Energy Monitoring & Billing (Use Case 3)

**Module:** EC8202 Big Data Analytics, Dept. of Electrical & Information Engineering, University of Ruhuna
**Submission type:** Individual
**Weighting:** 25% of module grade (End Semester: 40% demo/viva + 10% report)
**Duration:** 2 weeks
**Architecture decision:** Kappa (justified in §2, with rejected Lambda alternative)

---

## 0. Executive Summary

Build a Dockerised, end-to-end streaming data platform that:

1. Ingests **smart-meter telemetry** continuously (Python simulator → Kafka).
2. Ingests **daily tariff/billing reference data** and a **daily weather forecast** once per simulated day (Python simulator → file drop → Kafka via a loader).
3. Processes everything through **one** Spark Structured Streaming codebase (Kappa), performing validation, deduplication, enrichment, stream-static joins, and windowed aggregation.
4. Serves results from **PostgreSQL** via a **FastAPI** real-time API and a **Grafana** dashboard.
5. Uses **Apache Airflow** to orchestrate the daily billing report, data-quality checks, and replay/reprocessing jobs.
6. Is **observable**: structured JSON logging, Prometheus metrics at every stage, Grafana dashboards, and Alertmanager rules covering staleness, error rate, and low renewable contribution.

**Simulated clock: 1 simulated day = 5 real minutes.** This must be stated in the README, in the report, and printed in the logs at startup.

---

## 1. Business Requirements (interpreted from the brief)

The utility company needs two distinct classes of answer, at two distinct latencies:

| # | Requirement | Latency class | Consumer |
|---|---|---|---|
| R1 | Current grid load (kWh) per zone | Seconds | Control-room dashboard / API |
| R2 | Current renewable (solar) contribution % per zone | Seconds | Control-room dashboard / API |
| R3 | Alert when renewable contribution in a zone drops below threshold | Seconds | Ops alerting |
| R4 | Alert when a meter or zone goes silent (data staleness) | Seconds–minutes | Data-platform ops |
| R5 | Per-household bill for the completed simulated day, once tariff data lands | Per simulated day | Billing dept / customers |
| R6 | Per-household solar contribution and self-consumption ratio for the day | Per simulated day | Billing dept |
| R7 | Ability to restate a previously issued bill if tariff data arrives late or wrong | On demand | Billing dept |

**R7 is the requirement that drives the architecture decision.** Billing is money — it must be exactly recomputable, not approximately recomputable. Note this explicitly in the report; it is the hinge of the 20-mark criterion.

**Derived non-functional requirements:**
- Exactly-once (or effectively-once) semantics for billing aggregates — no double-billed kWh.
- Late and out-of-order meter readings tolerated up to a bounded watermark.
- Full replay of any simulated day without touching the production serving tables.
- The pipeline must be diagnosable when it breaks, not just when it works.

---

## 2. Architecture Decision: Kappa (chosen) vs Lambda (rejected)

### 2.1 The decision

**Chosen: Kappa architecture.** All data — streaming meter telemetry *and* the daily tariff/weather reference feeds — enters the system as an immutable, replayable Kafka log. A single Spark Structured Streaming codebase performs all transformations. Historical correction and the daily billing report are produced by **replaying the same code over the same log**, not by a parallel batch implementation.

### 2.2 Why Kappa fits this use case

**a) The "batch" source is reference data, not a fact feed.**
The brief's daily source is `household_id, tariff_rate, billing_tier, subsidy_flag` — a small, slow-changing dimension (hundreds to thousands of rows), plus a per-zone weather forecast. This is not a high-volume end-of-day fact extract. Small, keyed, slowly-changing data is the textbook case for a **log-compacted Kafka topic**: the topic retains the latest value per `household_id` forever, so the stream job can hold it as broadcast state and join it to telemetry as a **stream-static join**. There is no volume-driven need for a separate batch processing engine. (Contrast with Use Case 1, where the daily garage-expense file is a genuine fact feed — Lambda is more attractive there. Say this in the report; it demonstrates that the decision was made *for this use case*, not copied.)

**b) Replay is cheap here, which removes Kappa's main weakness.**
Kappa's classic objection is that reprocessing all history is expensive. In this system the total retained data is one simulated week ≈ 35 real minutes of telemetry — megabytes. Kafka retention is configured to cover the entire simulated history. Reprocessing an entire "month" of billing is a sub-minute replay, so the objection does not bite at this scale. Be honest in the report: at true utility scale (millions of meters at 1 Hz), this calculus changes, and a tiered approach — compacted/archived Parquet as a replay source feeding the *same* streaming code — is what production would use. That is the "what I'd do differently at production scale" section.

**c) R7 (restatement) is *better* served by Kappa than by Lambda.**
Under Lambda, a corrected tariff means running the batch layer with a fix and hoping the batch and speed implementations agree. Under Kappa, a corrected tariff is simply a **new record published to the compacted topic**, followed by a replay of the affected simulated day into a **new output table version**, which is then atomically swapped in. One code path, one definition of "a bill", no reconciliation between two engines.

**d) Single codebase eliminates the dual-logic bug class.**
Lambda's well-documented cost is maintaining the same business logic twice (e.g. the tiered-tariff calculation) in two runtimes, and the drift between them. The tiered billing rule here (`billing_tier` + `subsidy_flag` + block rates) is exactly the kind of fiddly logic that silently diverges between a Spark batch job and a streaming job. Writing it once is a correctness argument, not a convenience argument.

### 2.3 Why Lambda was rejected (state this honestly — the rubric rewards it)

Lambda's genuine advantages, and why each does not decide it here:

| Lambda advantage | Why it does not win here |
|---|---|
| Batch layer gives an authoritative, recomputed "source of truth" for billing | Kappa's replay-into-a-new-table gives the same guarantee, from the same code, at this data volume |
| Batch layer tolerates a buggy streaming layer (the speed layer is disposable) | Real mitigation, but paid for with permanent dual-implementation cost; at 2-week project scope the dual codebase is the larger risk |
| Mature batch tooling for complex joins over complete data | The only join needed is a small stream-static dimension join, well within Structured Streaming's capability |
| Cost: batch over cold storage is cheaper than long Kafka retention at scale | True at scale; irrelevant at a retained volume of megabytes |

**Honest concession to include in the report:** if the daily source were a high-volume *fact* feed (e.g. per-meter half-hourly settlement extracts from a national metering agency rather than a tariff table), or if regulatory audit demanded a physically separate, independently-implemented billing computation, Lambda would be the correct answer. The decision is scenario-dependent, and here the scenario's daily feed is dimensional.

### 2.4 Consistency & latency budget (put this table in the report)

| Path | Latency target | Consistency guarantee | Mechanism |
|---|---|---|---|
| Zone load / renewable mix | < 10 s end-to-end | At-least-once, idempotent upsert on `(zone, window_start)` | Structured Streaming 5 s micro-batch + Postgres upsert |
| Threshold alerts | < 15 s | At-least-once, deduplicated by alert key + window | Alert evaluator on aggregate stream |
| Daily household bill | Within 1 sim-day (5 min) of day close | Effectively-once; deterministic on replay | Airflow-sealed sim-day + watermark + idempotent write |
| Restated bill | On demand | Deterministic; new version, atomic swap | Replay from Kafka offset into versioned table |

---

## 3. Technology Stack & Justification

Each choice must be tied to a *use-case constraint*, not popularity. Use this table in the report.

| Layer | Choice | Justification tied to this use case | Alternative considered & why rejected |
|---|---|---|---|
| Messaging / ingestion | **Apache Kafka (KRaft mode, no ZooKeeper)** | Kappa requires a durable, replayable, offset-addressable log — this *is* the architecture, not a queue. Partitioning by `grid_zone` gives per-zone ordering, which the windowed zone aggregates depend on. Log compaction on the tariff topic gives free latest-value-per-household semantics. | RabbitMQ / Redis Streams: no durable long-retention replay by offset, no compaction — would make Kappa impossible. Kafka in KRaft mode chosen over ZooKeeper mode to cut one container and match current Kafka releases. |
| Stream processing | **Apache Spark Structured Streaming (PySpark)** | Needs event-time windowing, watermarking for late meter readings, stateful stream-static joins, and checkpointed exactly-once sinks — all first-class in Structured Streaming. Same DataFrame API serves replay, so one codebase covers both live and reprocessing (the Kappa requirement). | Apache Storm (also permitted by the brief): excellent sub-second latency, but has no native event-time windowing, no watermarking, and no checkpointed state → the billing aggregation would have to be hand-rolled. Our latency requirement is seconds, not milliseconds, so Storm's advantage is unused while its costs are real. |
| Orchestration | **Apache Airflow** | The simulated day boundary is a scheduling concern, not a streaming one: something must *seal* a sim-day, trigger report materialisation, run data-quality gates, and expose a manual replay trigger with parameters. Airflow's DAG dependencies, retries, and backfill map directly onto that. | Cron: no dependency graph, no retry/backfill semantics, no run history for the viva. Note explicitly in the report: Airflow here orchestrates *jobs around* the stream; it is **not** a Lambda batch layer, and the report must be clear on this distinction or the architecture claim looks inconsistent. |
| Serving store | **PostgreSQL 16** | Serving needs are low-latency point lookups (`/households/{id}/bill`), small range scans over 1-minute zone windows, and transactional atomic swap for restated bills. That is a relational OLTP access pattern. `ON CONFLICT DO UPDATE` gives the idempotent upsert our at-least-once sink needs. | Cassandra: better at write-heavy wide-partition telemetry, but gives no multi-row transactional swap for bill restatement and no ad-hoc SQL for the report. Rejected because our serving queries are small and relational, not high-cardinality time-series at scale. |
| Curated archive | **Parquet on MinIO (S3-compatible)** | Columnar archive of validated readings, partitioned by `sim_date`/`grid_zone`, supports the production-scale replay story and matches the module outline's emphasis on columnar formats. | HDFS: heavier to run in Compose for identical benefit at this scale. |
| Serving API | **FastAPI + Uvicorn** | Async, minimal, auto-generates OpenAPI docs (useful in the demo), and `prometheus-client` integrates in a few lines. | Flask: synchronous, more boilerplate for the same result. |
| Metrics & dashboard | **Prometheus + Grafana + Alertmanager** | Rubric demands metrics and alert *rules*; Prometheus' rule engine expresses "no data in N minutes" and "error rate above threshold" declaratively, and Grafana gives the live dashboard deliverable in the same stack. | Ad-hoc logging + manual checks: fails the observability criterion. |
| Logging | **`structlog` → JSON to stdout** | Machine-parseable structured logs with a correlation id per record allow tracing one meter reading across ingestion → processing → storage, which is exactly what the 10-mark observability criterion asks for. | Plain `logging` with f-strings: not queryable, no consistent fields. |
| Packaging | **Docker Compose** | Explicitly "strongly recommended" by the brief and directly assessed under reproducibility. | Local installs: unreproducible for the marker. |

---

## 4. System Architecture

### 4.1 Logical flow

```
┌─────────────────────┐
│ meter_simulator.py  │ ~200 meters, 5 zones, event every 2s
│ (streaming source)  │ injects: late events, dupes, nulls, negative kWh, meter dropout
└──────────┬──────────┘
           │ produce (key = grid_zone)
           ▼
   ┌────────────────────────────┐
   │ Kafka: meter.readings.v1   │  6 partitions, retention = full sim history
   └──────────┬─────────────────┘
              │
┌─────────────┴──────────────────────────────────────────────┐
│          SPARK STRUCTURED STREAMING (single codebase)      │
│                                                            │
│  Job A  validate → dedupe(watermark) → enrich → route      │
│         ├─ valid   → meter.readings.clean.v1  + Parquet    │
│         └─ invalid → meter.readings.dlq.v1                 │
│                                                            │
│  Job B  1-min tumbling window per zone                     │
│         → zone_load_1m (Postgres)                          │
│         → renewable_pct, active_meter_count                │
│                                                            │
│  Job C  stream-static join w/ tariff broadcast state       │
│         → running per-household kWh + cost for sim-day     │
│         → household_billing_running (Postgres)             │
│                                                            │
│  Job D  alert evaluator → alerts (Postgres) + metrics      │
└─────────────┬──────────────────────────────────────────────┘
              ▲                          │
              │ compacted topic          ▼
   ┌──────────┴──────────────┐   ┌──────────────────┐
   │ Kafka: tariff.ref.v1    │   │   PostgreSQL     │◄── FastAPI ──► client
   │ Kafka: weather.fcst.v1  │   │  (serving store) │◄── Grafana
   └──────────▲──────────────┘   └────────▲─────────┘
              │ loader                    │
   ┌──────────┴──────────────┐            │
   │ tariff_simulator.py     │   ┌────────┴─────────┐
   │ drops CSV+JSON per      │   │ AIRFLOW          │
   │ simulated day → /drop   │   │ • seal_sim_day   │
   └─────────────────────────┘   │ • daily_report   │
                                 │ • dq_checks      │
                                 │ • replay (manual)│
                                 └──────────────────┘
```

Produce this as a proper diagram (draw.io / Excalidraw / Mermaid) for the report — the rubric assesses diagram clarity. Draw **three** views: (1) layered architecture, (2) data flow with topics and tables named, (3) observability/metric flow.

### 4.2 Container inventory (docker-compose)

| Service | Purpose |
|---|---|
| `kafka` | Kafka 3.7+ in KRaft mode, single broker |
| `kafka-init` | One-shot: creates topics with correct partitions/compaction/retention |
| `postgres` | Serving store + Airflow metadata DB (separate databases) |
| `minio` + `minio-init` | S3-compatible object store for curated Parquet |
| `spark-master`, `spark-worker` | Spark standalone cluster |
| `spark-submit-jobs` | Submits streaming jobs A–D, restarts on failure |
| `meter-simulator` | Streaming source |
| `tariff-simulator` | Daily-batch source (file drop) |
| `batch-loader` | Watches drop dir, publishes to compacted topics |
| `airflow-webserver`, `airflow-scheduler` | Orchestration |
| `api` | FastAPI serving layer + `/metrics` |
| `prometheus`, `alertmanager`, `grafana` | Observability stack |

---

## 5. Repository Structure

```
smartgrid-kappa/
├── README.md                      # architecture summary, setup, reproduce steps, sim clock
├── docker-compose.yml
├── .env.example
├── Makefile                       # make up / down / logs / demo / replay / test
├── docs/
│   ├── architecture/              # diagrams (source + exported PNG)
│   ├── ADR-001-kappa-vs-lambda.md # architecture decision record
│   ├── ADR-002-tech-stack.md
│   └── report/                    # LaTeX or Markdown source of the 8–15 page report
├── common/
│   ├── config.py                  # env-driven settings (pydantic-settings)
│   ├── logging_setup.py           # structlog JSON, correlation ids
│   ├── metrics.py                 # shared Prometheus collectors
│   ├── schemas.py                 # pydantic + Spark StructType, single source of truth
│   └── sim_clock.py               # simulated-time helpers (1 sim-day = 5 min)
├── simulators/
│   ├── meter_simulator.py
│   ├── tariff_simulator.py
│   ├── fault_injection.py         # dupes, late events, nulls, dropout, spikes
│   └── reference/
│       ├── households.csv         # household_id, meter_id, grid_zone, has_solar
│       └── zones.csv              # grid_zone, name, capacity_kw
├── ingestion/
│   └── batch_loader.py            # drop-dir watcher → compacted Kafka topics
├── processing/
│   ├── job_a_clean_enrich.py
│   ├── job_b_zone_aggregates.py
│   ├── job_c_household_billing.py
│   ├── job_d_alert_evaluator.py
│   ├── transforms/                # pure functions — unit-testable
│   │   ├── validation.py
│   │   ├── enrichment.py
│   │   ├── billing.py             # tiered tariff + subsidy logic (ONE implementation)
│   │   └── windows.py
│   └── replay.py                  # parameterised replay entrypoint
├── serving/
│   ├── api/
│   │   ├── main.py
│   │   ├── routers/               # zones, households, alerts, health
│   │   └── repositories.py
│   └── sql/
│       ├── 001_schema.sql
│       ├── 002_indexes.sql
│       └── 003_views.sql
├── orchestration/
│   └── dags/
│       ├── seal_sim_day.py
│       ├── daily_billing_report.py
│       ├── data_quality_checks.py
│       └── replay_sim_day.py
├── observability/
│   ├── prometheus/prometheus.yml
│   ├── prometheus/rules/alerts.yml
│   ├── alertmanager/alertmanager.yml
│   └── grafana/dashboards/        # provisioned JSON dashboards
└── tests/
    ├── unit/                      # transforms, billing maths, validation
    ├── integration/               # Kafka round-trip, Postgres upsert idempotency
    └── fixtures/
```

---

## 6. Data Contracts

### 6.1 Streaming — `meter.readings.v1`

Key: `grid_zone` (string, UTF-8). Value: JSON.

```json
{
  "event_id": "uuid4",
  "meter_id": "MTR-0042",
  "household_id": "HH-0042",
  "grid_zone": "ZONE-A",
  "power_consumption_kwh": 0.0412,
  "solar_generation_kwh": 0.0170,
  "event_timestamp": "2026-01-14T09:31:04Z",
  "sim_date": "2026-01-14",
  "producer_emitted_at": "2026-09-26T08:12:44.331Z",
  "schema_version": 1
}
```

- 6 partitions, replication 1, retention covering full sim history.
- kWh values are *per-interval consumption*, not cumulative — state this; it avoids a whole class of billing bugs.
- Solar generation follows a diurnal curve scaled by the zone's weather forecast cloud cover.

### 6.2 Daily reference — `tariff.reference.v1` (log-compacted)

Key: `household_id`. Value:

```json
{
  "household_id": "HH-0042",
  "sim_date": "2026-01-14",
  "tariff_rate": 32.50,
  "billing_tier": "TIER_2",
  "subsidy_flag": true,
  "effective_from": "2026-01-14T00:00:00Z",
  "schema_version": 1
}
```

### 6.3 Daily reference — `weather.forecast.v1` (log-compacted)

Key: `grid_zone|sim_date`. Fields: `grid_zone`, `sim_date`, `cloud_cover_pct`, `expected_solar_index` (0–1), `forecast_issued_at`.

### 6.4 Dead-letter — `meter.readings.dlq.v1`

Original payload + `rejection_reason`, `rejected_at`, `job_name`, `correlation_id`.

### 6.5 Serving schema (PostgreSQL)

```sql
zone_load_1m(grid_zone, window_start, window_end, total_consumption_kwh,
             total_solar_kwh, renewable_pct, active_meter_count,
             late_event_count, updated_at)
             PRIMARY KEY (grid_zone, window_start)

household_billing_running(household_id, sim_date, consumption_kwh, solar_kwh,
             net_grid_kwh, self_consumption_ratio, running_cost, updated_at)
             PRIMARY KEY (household_id, sim_date)

household_billing_daily(household_id, sim_date, version, consumption_kwh,
             solar_kwh, net_grid_kwh, tariff_rate, billing_tier, subsidy_flag,
             gross_cost, subsidy_amount, final_bill, is_current, generated_at)
             PRIMARY KEY (household_id, sim_date, version)

alerts(alert_id, alert_type, severity, entity_type, entity_id, sim_date,
       window_start, message, metric_value, threshold_value, raised_at,
       resolved_at)   -- UNIQUE (alert_type, entity_id, window_start)

pipeline_run_audit(run_id, job_name, stage, sim_date, records_in, records_out,
       records_rejected, started_at, finished_at, status, notes)

sim_day_state(sim_date, opened_at, sealed_at, tariff_received_at,
       weather_received_at, report_generated_at, status)
```

`is_current` + `version` on `household_billing_daily` is what makes bill restatement (R7) atomic and auditable — call this out in the viva, it's the payoff of the Kappa argument.

---

## 7. Processing Logic (the 15-mark criterion)

Transformations must be **meaningful**, not pass-through. Implement all of these.

### Job A — Clean & Enrich
1. Parse JSON against explicit `StructType` (never `inferSchema` on a stream — explain why in the report: schema inference on an unbounded source is non-deterministic).
2. **Validation:** reject nulls in required fields; reject negative kWh; reject `solar_generation_kwh` above the physical panel cap; reject `event_timestamp` more than N minutes in the future. Rejects → DLQ with reason.
3. **Deduplication:** `dropDuplicates(["event_id"])` with `withWatermark("event_timestamp", "2 minutes")` — bounded state, handles the simulator's injected duplicates.
4. **Enrichment:** join zone metadata (capacity, name) and household attributes (`has_solar`, tier) from a broadcast static dimension; derive `net_grid_kwh = consumption - solar`, `is_exporting = net_grid_kwh < 0`, `time_of_day_bucket`, `sim_date`.
5. **Sinks:** clean topic + Parquet on MinIO partitioned by `sim_date`, `grid_zone`.

### Job B — Zone aggregates (the real-time answer to R1/R2)
- `withWatermark("event_timestamp", "1 minute")`, `window(event_timestamp, "1 minute")` tumbling, grouped by `grid_zone`.
- Aggregate: `sum(consumption)`, `sum(solar)`, `approx_count_distinct(meter_id)`, `count` of late events.
- Derive `renewable_pct = solar / NULLIF(consumption, 0) * 100`, clamped.
- Output mode `update`, sink via `foreachBatch` → Postgres `ON CONFLICT (grid_zone, window_start) DO UPDATE` (idempotent — this is how at-least-once delivery becomes effectively-once storage; say this in the report).

### Job C — Household billing (stream-static join, the R5/R6 answer)
- Read the compacted `tariff.reference.v1` into a broadcast dimension, refreshed per micro-batch inside `foreachBatch` (simplest correct approach; document the trade-off vs. a true stream-stream join with state).
- Aggregate per `(household_id, sim_date)`: consumption, solar, net grid draw, self-consumption ratio.
- Apply `transforms/billing.py` — **the single implementation of billing maths**:
  - block/tiered rate by `billing_tier`
  - `subsidy_flag` discount
  - export credit where `net_grid_kwh < 0`
  - rounding rule stated once and applied consistently
- If tariff for that `sim_date` has not yet arrived, write the running kWh with `tariff_rate = NULL` and flag it — do **not** guess. The report should note this as an explicit availability-vs-correctness choice.
- Unit-test this module hard. It is the most defensible thing you can show in a viva.

### Job D — Alert evaluator
Consumes the zone aggregate stream and emits alerts to the `alerts` table + Prometheus:
- `LOW_RENEWABLE`: `renewable_pct < 15%` for 3 consecutive 1-minute windows in a zone during simulated daylight hours.
- `ZONE_OVERLOAD`: `total_consumption_kwh` above zone capacity threshold.
- `METER_SILENT`: meter seen previously but no reading in the last N windows.
- Deduplicate via unique `(alert_type, entity_id, window_start)`.

### Airflow — `daily_billing_report`
Runs on the sim-day boundary (every 5 real minutes):
1. Sensor: tariff + weather for the sealed `sim_date` have landed.
2. Data-quality gate: reject rate < 2%, expected meter coverage ≥ 95%, no unexplained kWh gap. Fail the DAG loudly if breached.
3. Materialise `household_billing_daily` version N+1 from clean data, flip `is_current` transactionally.
4. Render the consolidated report: CSV + a PDF/HTML summary (per-zone totals, top-10 consumers, solar contribution league table, alert digest).
5. Write `pipeline_run_audit`, update `sim_day_state`.

### Airflow — `replay_sim_day` (manual, parameterised)
Takes `sim_date` + optional `from_offset`; runs `processing/replay.py` with a fresh consumer group writing to a shadow table; on success, promotes the new `version`. **This DAG is your live proof of the Kappa claim — demo it.**

---

## 8. Observability Design (10 marks — treat as a first-class deliverable)

**Logging.** `structlog` JSON to stdout everywhere. Mandatory fields on every line: `timestamp`, `level`, `service`, `job_name`, `stage` (ingest/process/store/serve), `sim_date`, `correlation_id`, `event`, plus stage-specific counts. Log at stage boundaries with record counts in/out/rejected — not inside hot loops.

**Metrics (Prometheus).** Minimum set:

| Metric | Type | Labels |
|---|---|---|
| `smartgrid_events_produced_total` | counter | `source`, `grid_zone` |
| `smartgrid_events_consumed_total` | counter | `job`, `topic` |
| `smartgrid_events_rejected_total` | counter | `job`, `reason` |
| `smartgrid_processing_latency_seconds` | histogram | `job` |
| `smartgrid_last_event_timestamp_seconds` | gauge | `grid_zone` |
| `smartgrid_batch_micro_duration_seconds` | histogram | `job` |
| `smartgrid_kafka_consumer_lag` | gauge | `topic`, `group` |
| `smartgrid_zone_renewable_pct` | gauge | `grid_zone` |
| `smartgrid_active_alerts` | gauge | `alert_type`, `severity` |
| `smartgrid_daily_report_success_total` | counter | — |
| `smartgrid_sim_day_current` | gauge | — |

Spark jobs expose metrics via a small HTTP server in the driver plus `StreamingQueryListener` → push to Pushgateway (or expose from the `foreachBatch` sink process). Pick one and document it.

**Alert rules (`observability/prometheus/rules/alerts.yml`).** Required by the brief — implement at least these four:
1. `NoDataReceived`: `time() - smartgrid_last_event_timestamp_seconds > 120` for 1m → critical.
2. `HighRejectRate`: `rate(rejected_total[5m]) / rate(consumed_total[5m]) > 0.05` for 2m → warning.
3. `ConsumerLagGrowing`: `smartgrid_kafka_consumer_lag > 10000` for 3m → warning.
4. `DailyReportMissed`: no `daily_report_success_total` increase in 10m → critical.

**Tracing.** A `correlation_id` generated by the producer, carried on the Kafka record header, logged at every stage, and stored on DLQ records. Demo requirement: pick one rejected record from the DLQ and show its full path through the logs. That single demonstration answers the "detect and diagnose pipeline failures" wording in the rubric directly.

**Grafana.** Two provisioned dashboards: *Business* (zone load, renewable mix, active meters, bills issued, alerts) and *Platform* (throughput, lag, reject rate, micro-batch duration, freshness).

---

## 9. Serving API (FastAPI)

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | Liveness + downstream dependency status |
| GET | `/metrics` | Prometheus scrape |
| GET | `/api/v1/zones/load?window=15m` | Real-time load + renewable mix per zone (R1, R2) |
| GET | `/api/v1/zones/{zone}/timeseries` | 1-minute series for charts |
| GET | `/api/v1/grid/summary` | System-wide load, renewable %, active meters |
| GET | `/api/v1/households/{id}/bill?sim_date=` | Current version of the daily bill (R5, R6) |
| GET | `/api/v1/households/{id}/bill/versions` | Restatement history (R7 evidence) |
| GET | `/api/v1/alerts?active=true` | Current alerts (R3, R4) |
| GET | `/api/v1/reports/daily/{sim_date}` | Consolidated daily report payload |
| GET | `/api/v1/pipeline/status` | Sim-day state, last report, freshness |

---

## 10. Build Phases (2 weeks)

**Phase 0 — Scaffold (Day 1).** Repo, Compose skeleton with Kafka + Postgres + MinIO, `common/` config, logging and schemas, Makefile, empty README with the sim-clock statement. *Checkpoint: `make up` brings up infra; topics created with correct compaction settings.*

**Phase 1 — Simulators (Days 1–2).** Meter simulator with diurnal solar curve and configurable rates; tariff/weather simulator writing to the drop dir on the sim-day boundary; fault injection toggled by env vars; batch loader. *Checkpoint: `kafka-console-consumer` shows well-formed events on all three topics; tariff topic compacts correctly.*

**Phase 2 — Job A (Days 3–4).** Validation, dedupe, enrichment, DLQ, Parquet sink, checkpointing. *Checkpoint: injected bad records land in DLQ with correct reasons; restarting the job does not duplicate Parquet output.*

**Phase 3 — Jobs B & C (Days 5–7).** Zone aggregates and household billing with the stream-static join. Unit tests on `transforms/billing.py` first, then the job. *Checkpoint: bills recompute to identical values on replay; upserts are idempotent under forced restart.*

**Phase 4 — Serving (Day 8).** Postgres schema, FastAPI, Grafana business dashboard. *Checkpoint: every endpoint returns live data; dashboard updates within 10 s.*

**Phase 5 — Airflow (Days 9–10).** All four DAGs, sim-day sealing, report rendering, replay. *Checkpoint: a full sim-day produces a report file automatically; replay produces version 2 and flips `is_current`.*

**Phase 6 — Observability (Day 11).** Metrics everywhere, Prometheus rules, Alertmanager, platform dashboard, correlation-id tracing. *Checkpoint: kill the simulator → `NoDataReceived` fires within 2 minutes and is visible in Alertmanager.*

**Phase 7 — Hardening (Day 12).** Tests, README, `make demo` one-command reproduction, code cleanup, config externalised.

**Phase 8 — Report & demo (Days 13–14).** Diagrams, 8–15 page report, screenshots, 5–10 minute demo video.

---

## 11. Demo Script (rehearse this — 40% of the end-semester mark is demo + viva)

1. `make up` — show the stack starting; point out the sim-clock line in the logs.
2. Grafana business dashboard filling with live zone load and renewable mix.
3. `curl /api/v1/zones/load` and `/api/v1/grid/summary`.
4. Trigger low-solar weather → `LOW_RENEWABLE` alert appears in the alerts table, the API, and Grafana.
5. Wait for a sim-day boundary → Airflow `daily_billing_report` runs green → show the generated report file and `/api/v1/households/{id}/bill`.
6. Kill the meter simulator → `NoDataReceived` fires in Alertmanager within 2 minutes. Restart it; show recovery and consumer lag draining.
7. Show a DLQ record; grep its `correlation_id` across services to trace it end to end.
8. **The finale:** publish a corrected tariff, run `replay_sim_day`, show version 2 of the bill superseding version 1. This is the Kappa architecture argument made concrete.

---

## 12. Report Outline (15 marks, 8–15 pages)

1. Introduction & use case — the business problem in your own words.
2. Requirements — the R1–R7 table from §1, with latency and consistency classes.
3. **Architecture decision** (longest section, 20 marks rides on it) — Kappa chosen; §2.2 arguments; §2.3 Lambda rejection table; the honest concession about when Lambda would win; the latency/consistency budget table.
4. Architecture design — three diagrams, topic and table names, data contracts.
5. Technology stack — the §3 table, every row tied to a use-case constraint plus its rejected alternative.
6. Implementation — simulators, fault injection, the four jobs, billing logic, Airflow DAGs; note the simulated clock.
7. Observability design — what is measured, how, why; the alert rules; the correlation-id trace walkthrough with a screenshot.
8. Results — dashboard screenshots, a sample daily billing report, an alert firing, the before/after of a restated bill.
9. Limitations & trade-offs — single broker (no fault tolerance), micro-batch not true per-event streaming, broadcast dimension does not scale past a few hundred thousand households, simulated data lacks real meter pathologies, replay cost grows linearly with retained history.
10. Production-scale changes — multi-broker Kafka with replication ≥ 3, schema registry with Avro and compatibility enforcement, tiered storage so replay reads from object store, exactly-once sink via transactional writes or Delta/Iceberg, Kubernetes, secrets management, PII handling on household data (ties to LO-4: ethics, security, governance — the module explicitly assesses this, so include a short paragraph on consumption data as personal data and access control).
11. Conclusion.
12. Assumptions & simplifications — collect them all here; the brief asks for them explicitly.

---

## 13. Rubric Traceability

| Criterion | Marks | Where this plan earns it |
|---|---|---|
| Architecture Decision & Justification | 20 | §2, ADR-001, report §3, replay demo step 8 |
| Technology Stack Selection | 10 | §3 table, ADR-002, report §5 |
| Data Ingestion Implementation | 15 | §5 simulators, fault injection, batch loader, §6 contracts |
| Processing Layer Implementation | 15 | §7 Jobs A–D, watermarking, dedupe, joins, billing unit tests |
| Storage & Serving Layer | 10 | §6.5 schema, §9 API, Grafana, idempotent upserts, versioned bills |
| Observability | 10 | §8 logging, metrics, 4 alert rules, correlation-id tracing |
| Report | 15 | §12 outline, 3 diagrams, honest limitations section |
| Code Quality & Documentation | 5 | §5 structure, README, Makefile, Compose, tests, env config |

---

## 14. Assumptions to State Explicitly

- 1 simulated day = 5 real minutes; simulated dates start 2026-01-01.
- ~200 households across 5 grid zones; ~60% have solar.
- Meter readings are per-interval kWh, not cumulative counters.
- Tariff file for day *D* is delivered at the start of day *D+1* (models a real end-of-day feed); bills for *D* are therefore issued during *D+1*.
- Currency is LKR; rates are illustrative and not real CEB tariffs.
- Single Kafka broker, replication factor 1 — acceptable for demonstration, not production.
- No authentication on the API or Kafka in this environment; production requirements noted in the report.
