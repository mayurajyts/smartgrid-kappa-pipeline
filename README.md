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

Allocate Docker at least **4 GB of memory**; **6 GB** is comfortable. The full
stack measures around 2.5 GB at rest, and the tight part is three Spark drivers
plus Airflow running at the same time.

> On a machine with 8 GB of RAM or less, cap the WSL2 backend explicitly in
> `%USERPROFILE%\.wslconfig` (`[wsl2]` / `memory=4GB`). Without a cap WSL grows on
> demand and competes with the host until the Docker engine wedges — every API call
> then returns `500 Internal Server Error` and neither the CLI nor Docker Desktop
> can stop a container. Closing other memory-heavy applications while the stack runs
> helps materially.

---

## Quick start

```bash
make demo
```

One command. It builds every image, starts all sixteen long-running services (plus three one-shot init containers), applies the
serving schema, waits for the API to report healthy, and prints the demo URLs in
the order §11 walks through them.

First run pulls images and can take several minutes. Later runs take about a
minute.

Then prove it works:

```bash
make smoke
```

Fifteen checks across every layer — Kafka offsets, the Spark aggregates, the
serving tables, the Airflow artifacts, the API, Prometheus targets and the alert
rules. It exits non-zero if any layer is broken, so it is also the thing to run
after a change.

### What you get

| URL | What it shows |
|---|---|
| `localhost:3000/d/smartgrid-business` | Zone load, renewable mix, meter coverage, bills |
| `localhost:3000/d/smartgrid-platform` | Throughput, reject rate, latency, consumer lag |
| `localhost:8080/docs` | Interactive API page, all ten §9 endpoints |
| `localhost:8081` | Airflow — the four DAGs (`admin`/`admin`) |
| `localhost:9090/alerts` | Prometheus alert rules and their state |
| `localhost:9093` | Alertmanager |
| `localhost:8090` | Spark master |

`make demo-urls` reprints that list at any time.

---

## Status: all build phases complete

| Phase | Deliverable | Verified by |
|---|---|---|
| 0 | Kafka (KRaft), Postgres, object store, `common/`, Makefile | `make topics` |
| 1 | Meter + tariff/weather simulators, fault injection, batch loader | `make consume` `make faults` |
| 2 | Job A — validate, dedupe, enrich, DLQ, Parquet archive | `make dlq` `make parquet` |
| 3 | Jobs B & C — zone aggregates, household billing, idempotent upserts | `make zones` `make bills` |
| 4 | FastAPI serving layer, Grafana business dashboard | `make endpoints` |
| 5 | Four Airflow DAGs, sim-day sealing, daily report, replay | `make dags` `make report` |
| 6 | Prometheus, seven alert rules, Alertmanager, platform dashboard | `make metrics` `make alerts` |
| 7 | `make demo`, `make smoke`, README, config externalised | `make smoke` |

Test suite: **235 host tests** (`make test`, no infrastructure needed) plus
**60 Spark transform tests** (`make test-spark`, in-container).

---

## The headline demonstration: bill restatement (R7)

This is the architecture decision made concrete, and it is §11's finale.

```bash
make versions     # one version per day, the current one flagged
make restate      # publish a restatement: re-price and issue version N+1
make versions     # version 1 superseded, version 2 current, BOTH readable
```

A restated bill is never an `UPDATE`. It is a new row at version N+1 plus one
transaction that flips `is_current`, so "what did we invoice, and what did we
correct it to" is answerable from the table itself. A partial unique index
enforces that exactly one version is ever live, so a mistake fails loudly rather
than leaving two current bills.

Both the streaming job and the restatement DAG price bills by calling
`processing/transforms/billing.py`. Not a copy of it — the same module. That is
the whole "why not Lambda" argument in one file, and it is checkable: the DAG's
re-pricing reproduces the stream's stored cost to the cent.

---

## Verifying each layer

Every phase has a make target that prints its own pass criteria, so a claim in
this README can be checked rather than taken on trust.

### Processing (Phase 3)

```bash
make zones     # zone aggregates; asserts the window span is 288 sim-minutes
make bills     # running bills; asserts running_cost IS NULL iff tariff_missing
```

`make upsert-restart` is the stronger one: it SIGKILLs Job B mid-batch, restarts
it, and compares settled windows against a baseline. Zero differing rows and zero
duplicate keys is the pass — at-least-once delivery becoming effectively-once
storage, which is what the `ON CONFLICT DO UPDATE` sink is for.

### Serving (Phase 4)

```bash
make endpoints   # all ten §9 endpoints, with status and payload size
```

### Orchestration (Phase 5)

```bash
make dags        # DAG list, the sim-day lifecycle table, recent audit rows
make report      # the generated CSV and HTML, and issued bills by version
```

### Observability (Phase 6)

```bash
make metrics     # scrape targets and key metric values
make alerts      # all seven rules and their state
```

`inactive` means the condition is false (healthy). `pending` means it is true and
waiting out its `for:` duration. `LowRenewableContribution` pending overnight in
simulated time is **correct** — that is the diurnal cycle, not a fault.

To see an alert actually fire (§11 step 6):

```bash
docker compose stop meter-simulator
# NoDataReceived fires within ~2 minutes; watch localhost:9093
docker compose up -d meter-simulator
```

### Tracing one record end to end (§11 step 7)

```bash
make dlq                      # rejected records grouped by reason
make trace CID=<correlation_id>
```

A `correlation_id` is minted by the producer and carried through every stage and
onto the DLQ record, so one rejected reading can be followed from ingestion to
rejection across service logs. That single demonstration is what the
observability criterion asks for.

---

## Timing: the one thing to get right

**The simulated clock is 288× wall clock, and almost every confusing observation
traces back to forgetting that.**

Three separate bugs in this project were the same mistake — a duration left on
the wrong axis:

- A watermark specified as "2 minutes" is **0.42 real seconds** at 288×, which is
  shorter than one micro-batch, so the stateful operator drops nearly everything.
- A future-timestamp tolerance of "5 minutes" is **1.04 real seconds**, less than
  Kafka transit plus one micro-batch — so 100% of readings get rejected as
  future-dated and the clean topic silently stops growing.
- A "1 minute" tumbling window would write **~86,400 rows per real minute**.

The convention that resolves it: **every duration is declared in REAL minutes and
multiplied by `compression_ratio()`** inside the job. Job B additionally asserts
the watermark/window/batch-span relationship at startup and refuses to run if it
breaks, so a tuning change cannot silently halve the output.

`tests/unit/test_time_budgets.py` pins these relationships as pure arithmetic —
thirteen tests that run on the host and never skip.

Two commands worth knowing:

```bash
make fast          # 1 sim day = 60s, for development
make demo-config   # 1 sim day = 300s, the documented timing — run before any demo
```

`make demo` runs `demo-config` for you.

---

## Development

```bash
make dev        # code bind-mounted; edit and restart, no rebuild
make rebuild    # rebuild the thin Spark image (~5s) and restart jobs A/B/C
make test       # 235 host tests (~30s)
make test-spark # 60 Spark transform tests, in-container
```

`make up` bakes code into images (reproducible, for the demo); `make dev`
bind-mounts it (fast, not reproducible from images alone). That separation is
deliberate.

The Spark image is split in two: a base with the dependencies and ~500 MB of
JARs, and a thin layer with the application code. A code change rebuilds in about
five seconds rather than six minutes. **A change to the base — a new JAR or pip
dependency — needs `make spark-base-force`**; `make rebuild` will not pick it up.

### If Docker wedges

On a memory-constrained host, Docker can reach a state where every API call
returns `500 Internal Server Error`. Recovery: stop Docker Desktop, run
`wsl --shutdown`, start Docker Desktop.

The stack needs roughly 2.5 GB. `%USERPROFILE%\.wslconfig` caps it; raise
`memory=` toward 5 GB if you have the headroom, since three Spark drivers plus
Airflow is the tight part.

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
- [`docs/ADR-004-parquet-sink.md`](docs/ADR-004-parquet-sink.md) — the Parquet
  archive is written to a volume rather than through `s3a://`, because Hadoop's
  S3A committer finalises writes with a rename that the object store's gateway
  does not support. The partitioning and the replay story are unchanged. This is
  the problem Delta Lake and Iceberg exist to solve, and the report says so.
