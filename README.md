# Smart Grid Energy Monitoring & Billing — a Kappa Architecture Platform

**EC8202 Big Data Analytics** · Dept. of Electrical & Information Engineering, University of Ruhuna
Individual mini-project · Use Case 3

---

## ⏱️ Simulated clock: 1 simulated day = 5 real minutes

**Everything in this system runs on compressed time.** One simulated day elapses
every 300 real seconds — a compression ratio of **288×**. A dashboard panel
showing "three days of billing" represents fifteen real minutes.

This is stated here, printed by every service at startup, and repeated in the
report, because a reader who assumes real time will misread every chart. Time
compression is what lets a two-week project demonstrate multi-day billing,
late-arriving tariff data, and the replay of a *past* day — the behaviours the
architecture is actually being judged on.

Simulated time is pinned by **two separate anchors**, and the distinction
matters:

- `SIM_START_DATE` (default `2026-01-01`) — the simulated date the demo begins on.
- `SIM_ANCHOR_REAL` — the real instant that date began. `make up` stamps this
  automatically on every run, so each demo starts on day 0 rather than resuming
  a previous run's timeline.

Collapsing these into one fixed epoch is a trap: because simulated time runs 288×
faster, elapsed real time since a past date gets multiplied by 288, and the clock
ends up reading a date centuries in the future. `common/sim_clock.py` documents
this and `tests/unit/test_sim_clock.py` has a regression test for it.

The ratio is configurable (`SIM_DAY_REAL_SECONDS` in `.env`); slow it down if a
demo step needs to be talked through.

---

## What this is

An end-to-end streaming data platform that ingests smart-meter telemetry
continuously, ingests daily tariff and weather reference data once per simulated
day, processes both through **one** Spark Structured Streaming codebase, and
serves the results through a REST API and live dashboards — with billing that
can be **exactly recomputed and restated** from the log.

It is built on a **Kappa architecture**: all data enters as an immutable,
replayable Kafka log, and historical correction is done by *replaying the same
code over the same log*, not by maintaining a parallel batch implementation.

**Why Kappa and not Lambda, in two sentences.** The daily "batch" source here is
a small, keyed, slowly-changing *dimension* (tariffs per household), not a
high-volume fact feed — which makes it a textbook log-compacted Kafka topic
joined to the stream, with no volume-driven need for a second processing engine.
And because billing is money, the requirement that decides it is R7: a bill must
be *exactly* recomputable when a tariff arrives late or wrong, which one codebase
replayed over an immutable log guarantees and two independently-implemented
engines merely hope for.

The full decision, the honest case *for* Lambda, and the scenario in which
Lambda would be the right answer are in
[`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md) §2.

---

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Docker Engine | 24+ | with the Compose v2 plugin (`docker compose`, not `docker-compose`) |
| GNU Make | 4+ | on Windows, use Git Bash or WSL |
| Python | 3.11+ | only needed to run the unit tests on the host |

Allocate Docker at least **6 GB of memory** — Kafka, Spark and Airflow together
need it in later phases.

---

## Quick start

```bash
git clone <this-repo> && cd smartgrid-kappa-pipeline
make up
```

`make up` copies `.env.example` to `.env` if it is missing, builds and starts
the stack, and waits for the one-shot init containers to create the Kafka topics
and the MinIO bucket.

```bash
make help     # list every target
make ps       # container status
make topics   # topic inventory and configuration
make logs     # tail everything (make logs S=kafka for one service)
make s3       # object store endpoints and buckets
make consume  # show events on all three topics
make faults   # show the deliberately injected faults
make drop     # list the daily-feed drop directory
make test     # run the unit tests
make down     # stop, keeping all data
make clean    # stop and DESTROY all data (prompts first)
```

---

## Current status: Phase 1 (Simulators) complete

The build follows the eight phases in
[`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md) §10.

| Phase | Scope | Status |
|---|---|---|
| 0 | Scaffold: Compose infra, `common/`, Makefile | ✅ complete |
| 1 | Simulators + batch loader | ✅ complete |
| 2 | Job A — clean, validate, dedupe, enrich, DLQ | pending |
| 3 | Jobs B & C — zone aggregates, household billing | pending |
| 4 | Serving: Postgres schema, FastAPI, Grafana | pending |
| 5 | Airflow: sealing, daily report, replay | pending |
| 6 | Observability: metrics, alert rules, tracing | pending |
| 7 | Hardening: tests, `make demo` | pending |
| 8 | Report & demo | pending |


### Verifying the Phase 1 checkpoint

> *"`kafka-console-consumer` shows well-formed events on all three topics; tariff
> topic compacts correctly."*

**1. All three topics carry well-formed events.**

```bash
make consume
```

`meter.readings.v1` fills immediately, keyed by `grid_zone`. The two reference
topics stay **empty for the first ~5 real minutes** — that is correct, not a
failure: §14 specifies that day *D*'s tariff is delivered at the start of day
*D+1*, so the first simulated day genuinely has no tariff. This is what exercises
Job C's explicit choice to write the running kWh with `tariff_rate = NULL` rather
than invent a rate.

**2. The tariff topic compacts by key.** This is the mechanism the whole Kappa
argument rests on, so it gets its own target:

```bash
make compaction    # takes ~2 min: it must force a segment roll
```

It publishes a *corrected* tariff for one household under the same key, then shows
only the corrected value surviving. A restatement is an **append, never an
update** — which is exactly how R7 is satisfied without a second processing
engine.

The target deliberately forces a segment roll, because Kafka's log cleaner never
compacts the **active** segment. Publishing a correction and simply waiting shows
both versions indefinitely and looks like compaction is broken when it is working
as designed.

**3. Deliberate faults are being injected** — these are the fixtures Phase 2's DLQ
and Phase 3's deduplication are built to catch:

```bash
make faults
```

| Fault | Rate | What it exercises |
|---|---|---|
| `null_required_field` | 0.2% | Job A DLQ → `null_required_field` |
| `negative_kwh` | 0.2% | Job A DLQ → `negative_kwh` |
| `solar_above_capacity` | 0.2% | Job A DLQ → `solar_above_capacity` |
| `timestamp_in_future` | 0.2% | Job A DLQ → `timestamp_in_future` |
| `duplicate` | 1.0% | `dropDuplicates(["event_id"])` |
| `late_event` | 1.0% | the 2-minute watermark (back-dated 4 sim min) |
| `meter_dropout` | 0.1% | `METER_SILENT` alert, `NoDataReceived` rule |

The four rejection faults sum to **0.8%**, deliberately below the 2% data-quality
gate the Airflow DAG enforces (§7) — larger values would make the pipeline fail its
own gate by design. Every injected fault logs its `correlation_id`, so a DLQ record
found in Phase 2 can be traced back to the exact moment it was created. That is the
§11 step 7 trace demo.

**4. The daily file feed lands and is loaded.**

```bash
make drop
```

Expect a `tariff_<sim_date>.csv` / `weather_<sim_date>.json` pair per completed
simulated day, plus a `.processed` marker per loaded file. Files are written to a
temp name and atomically renamed, so the polling loader can never read a
half-written file.

**5. Per-zone ordering holds.** Kafka orders only *within* a partition, so the
1-minute tumbling zone aggregates depend on each zone's records staying on one
partition:

```bash
source .env && docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic $TOPIC_METER_READINGS \
  --partition 0 --from-beginning --max-messages 20 \
  --property print.key=true --property print.value=false
```

Every key on a given partition is the same zone. Note that with 5 zone keys across
6 partitions, some partitions are empty and two zones may share one — both are
correct: the guarantee needed is per-zone ordering, not one-zone-per-partition.

**6. Unit tests pass.**

```bash
make test
```

### Verifying the Phase 0 checkpoint

> *"`make up` brings up infra; topics created with correct compaction settings."*

**1. Services are healthy and the init tasks completed.**

```bash
make ps
```

`kafka`, `postgres` and `minio` should show `healthy`; `kafka-init` and
`minio-init` should show `Exited (0)` — they are one-shot tasks, not services.

**2. Topics exist with the settings the architecture depends on.**

```bash
make topics
```

Two things in that output are load-bearing, not incidental:

- `cleanup.policy=compact` on **`tariff.reference.v1`** and
  **`weather.forecast.v1`**. Compaction retains the latest value per key
  forever, which *is* the broadcast dimension that the stream-static join in Job
  C reads. Without it the tariff data ages out and bills silently lose their
  tariff.
- `PartitionCount: 6` on **`meter.readings.v1`**. Kafka guarantees ordering only
  within a partition, so keying by `grid_zone` across 6 partitions is what gives
  the per-zone ordering the 1-minute tumbling aggregates rely on.

**3. Both databases exist.**

```bash
make psql    # then: \l
```

Expect `smartgrid` (serving store) and `airflow` (orchestration metadata) —
isolated by database on one instance.

**4. The curated bucket exists.**

```bash
make s3   # prints the endpoints and lists the buckets
```

Expect `smartgrid-curated` in the `ListAllMyBucketsResult`.

**5. Unit tests pass.**

```bash
make test
```

**6. The simulated clock reports correctly.**

```bash
make clock
```

Expect `sim_start=2026-01-01` and a current `sim_date` on or shortly after it —
**not** a far-future date. Both anchors are printed so you can see which real
moment the simulated timeline was pinned to.

**7. Reproducibility — the property actually being assessed.**

```bash
make clean && make up
```

The stack must reach the same state from empty volumes.

---


## The simulated world

Committed in `simulators/reference/`, **not generated at startup**:

- **200 households** across **5 grid zones**, **60% with solar** (§14).
- Zone sizes are deliberately uneven, so zone aggregates differ visibly rather
  than all tracking the same line.
- Tariff tier correlates with a household's base load, so the billing report's
  consumer ranking is coherent.

The population is fixed rather than random because the headline demo (§11 step 8)
replays a simulated day and shows the bill recomputing to the same value. With a
randomly generated population, a replay would run against different households and
"the bill came out the same" would be meaningless.

Consumption and solar generation are **deterministic functions of (meter,
simulated instant)** — seeded per reading rather than drawn from a global RNG — for
the same reason.

## Repository layout

```
common/          Shared library: config, logging, schemas, sim clock, metrics.
                 Imported by every service, so there is exactly one definition
                 of each contract.
simulators/      Data sources: meter telemetry, daily tariff/weather feeds.
ingestion/       Drop-dir watcher publishing reference files to Kafka.
processing/      The single Kappa codebase: Spark jobs A-D plus replay.
  transforms/    Pure, unit-tested functions. billing.py is the ONE
                 implementation of the tariff maths.
serving/         Postgres schema and the FastAPI read API.
orchestration/   Airflow DAGs — jobs *around* the stream, not a batch layer.
observability/   Prometheus config and rules, Alertmanager, Grafana dashboards.
docker/          Image definitions and one-shot init scripts.
tests/           unit/ (no infrastructure needed) and integration/.
docs/            Implementation plan, ADRs, diagrams, report.
```

---

## Configuration

Every host, port, credential, topic name, partition count and threshold is read
from the environment through `common/config.py`. Nothing is hardcoded anywhere
in the codebase.

This is not tidiness. It is what makes the stack reproducible on a machine that
is not the author's, and it is what lets a **replay run be the same code** as
the live run — pointed at a different consumer group and a different output
table. If a replay needed an edited source file, the single-codebase claim at
the heart of this architecture would not be true.

See [`.env.example`](.env.example); every variable is documented inline.

---

## Known limitations

Stated up front, and expanded in the report:

- **Single Kafka broker, replication factor 1.** No fault tolerance — losing the
  broker loses the log. Production needs ≥ 3 brokers with replication ≥ 3.
- **Micro-batch, not true per-event streaming.** Spark Structured Streaming
  trades sub-second latency for exactly-once semantics and checkpointed state.
  The requirement here is seconds, so this is the right trade.
- **The broadcast tariff dimension does not scale** past a few hundred thousand
  households; beyond that it needs a true stateful join.
- **Replay cost grows linearly** with retained history. Fine at megabytes;
  at utility scale replay would read tiered Parquet from object storage.
- **No authentication** on the API or Kafka in this environment.
- **Household consumption data is personal data.** Access control and PII
  handling are out of scope here and addressed in the report's governance
  section.

---

## Reference

[`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md) is the authoritative
specification: architecture decision, technology justification, data contracts,
processing logic, observability design and build order.

Where implementation has had to depart from that specification, the departure is
recorded as an ADR rather than made silently:

- [`docs/ADR-003-object-store.md`](docs/ADR-003-object-store.md) — the curated
  archive runs on SeaweedFS rather than MinIO, because MinIO's images now
  require registry authentication, which would break reproducibility from a
  clean clone. The requirement (an S3-compatible object store) is unchanged.
