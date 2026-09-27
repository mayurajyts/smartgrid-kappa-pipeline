# =============================================================================
# EC8202 Smart Grid Kappa Pipeline
#
# The Makefile is the reproducibility contract (plan §3, §13): the marker should
# never need to know a docker compose incantation. `make up` on a clean clone
# must reach a working stack.
#
# Phase 0 targets: up, down, ps, logs, topics, psql, test, clean.
# `demo` and `replay` arrive with Phases 5-7.
# =============================================================================

SHELL := /bin/bash
# Without explicit SHELLFLAGS, make on Windows/MSYS can invoke the shell in a
# way that mangles long flags in recipes (e.g. --bootstrap-server), and a failing
# command mid-pipeline would otherwise go unnoticed. -e -u -o pipefail also make
# the recipes fail loudly, which is what a reproducibility check needs.
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

# Compose v2 ships as a `docker compose` CLI plugin, but on some hosts (notably
# Git Bash on Windows) make's PATH does not include the plugin directory, so
# `docker compose` resolves to a docker binary that rejects the subcommand while
# the identical command works in an interactive shell. Detecting once here, and
# falling back to the equivalent `docker-compose` binary, keeps every target
# working regardless of how Compose is installed on the marker's machine.
COMPOSE := $(shell docker compose version >/dev/null 2>&1 && echo "docker compose" || echo "docker-compose")

.PHONY: help env anchor up down ps logs topics psql s3 test clock clean \
        consume compaction drop faults dlq trace parquet spark-ui test-spark

help:  ## Show available targets
	@echo "Smart Grid Kappa Pipeline"
	@echo ""
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

# .env is generated from the committed template rather than being committed
# itself, so a clean clone works immediately while local edits stay local.
env:
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "Created .env from .env.example"; \
	fi

# Stamp the real instant at which simulated time starts, so every container
# shares one anchor. Without this each container would fall back to its own
# start time and they would disagree about the current simulated day — and a
# reading attributed to the wrong sim_date is billed on the wrong day's tariff.
# Re-stamped on every `make up`, so each run of the demo begins at
# SIM_START_DATE rather than continuing a previous run's simulated timeline.
anchor: env
	@now=$$(date -u +%Y-%m-%dT%H:%M:%SZ); 	if grep -q '^SIM_ANCHOR_REAL=' .env; then 		sed -i.bak "s|^SIM_ANCHOR_REAL=.*|SIM_ANCHOR_REAL=$$now|" .env && rm -f .env.bak; 	else 		echo "SIM_ANCHOR_REAL=$$now" >> .env; 	fi; 	start=$$(grep '^SIM_START_DATE=' .env | cut -d= -f2); 	echo "Simulated time anchored at $$now -> simulated day 0 = $$start"

up: env anchor  ## Start the stack and create topics/buckets
	$(COMPOSE) up -d --build
	@echo ""
	@echo "Waiting for the one-shot init containers to finish..."
	@# `compose up -d` returns once the init containers have STARTED, not once
	@# they have finished creating topics and buckets. Polling until they are no
	@# longer running makes `make up` mean "the stack is ready", so the
	@# checkpoint cannot be inspected while setup is still in flight.
	@for i in $$(seq 1 60); do 		running=$$($(COMPOSE) ps --status running --services 2>/dev/null 			| grep -E '^(kafka-init|objectstore-init)$$' || true); 		[ -z "$$running" ] && break; 		sleep 2; 	done
	@echo ""
	@$(MAKE) --no-print-directory ps
	@echo ""
	@echo "Verify the Phase 0 checkpoint with:  make topics"

down:  ## Stop the stack, preserving all data
	$(COMPOSE) down

ps:  ## Show container status
	@# -a, so the one-shot init containers remain visible after they exit. Their
	@# `Exited (0)` is part of the checkpoint evidence: it shows topic and bucket
	@# creation ran to completion rather than never having run at all.
	@$(COMPOSE) ps -a

logs:  ## Tail logs (make logs S=kafka for one service)
	$(COMPOSE) logs -f --tail=100 $(S)

topics:  ## PHASE 0 CHECKPOINT: list topics and show compaction settings
	@echo "=================================================================="
	@echo "TOPIC INVENTORY"
	@echo "=================================================================="
	@$(COMPOSE) exec kafka kafka-topics --bootstrap-server localhost:9092 --list
	@echo ""
	@echo "=================================================================="
	@echo "TOPIC CONFIGURATION"
	@echo "  Expect: cleanup.policy=compact on the two reference topics"
	@echo "          (the stream-static join depends on it), and"
	@echo "          PartitionCount=6 on meter.readings.v1"
	@echo "          (per-zone ordering for the windowed aggregates)."
	@echo "=================================================================="
	@$(COMPOSE) exec kafka kafka-topics --bootstrap-server localhost:9092 --describe

psql:  ## Open a psql shell on the serving database
	@source .env && $(COMPOSE) exec postgres \
		psql -U $$POSTGRES_SUPERUSER -d $$POSTGRES_DB

s3:  ## Show the object store endpoints and the curated bucket
	@source .env \
		&& echo "S3 API:       http://localhost:$$S3_API_HOST_PORT" \
		&& echo "File browser: http://localhost:$$S3_CONSOLE_HOST_PORT" \
		&& echo "" \
		&& echo "Buckets:" \
		&& curl -s "http://localhost:$$S3_API_HOST_PORT/" ; echo


# ---------------------------------------------------------------------------
# PHASE 1 CHECKPOINT targets
# ---------------------------------------------------------------------------

consume:  ## PHASE 1 CHECKPOINT: show well-formed events on all three topics
	@source .env && echo "==================================================================" \
		&& echo "1/3  $$TOPIC_METER_READINGS  (telemetry, keyed by grid_zone)" \
		&& echo "==================================================================" \
		&& $(COMPOSE) exec kafka kafka-console-consumer \
			--bootstrap-server localhost:9092 \
			--topic $$TOPIC_METER_READINGS \
			--property print.key=true --property key.separator=' | ' \
			--max-messages 5 --timeout-ms 20000 || true
	@source .env && echo "" \
		&& echo "==================================================================" \
		&& echo "2/3  $$TOPIC_TARIFF_REFERENCE  (compacted, keyed by household_id)" \
		&& echo "     Empty until the first simulated day boundary (~5 real min):" \
		&& echo "     day D tariff is delivered at the start of day D+1 per S14." \
		&& echo "==================================================================" \
		&& $(COMPOSE) exec kafka kafka-console-consumer \
			--bootstrap-server localhost:9092 \
			--topic $$TOPIC_TARIFF_REFERENCE --from-beginning \
			--property print.key=true --property key.separator=' | ' \
			--max-messages 3 --timeout-ms 15000 || true
	@source .env && echo "" \
		&& echo "==================================================================" \
		&& echo "3/3  $$TOPIC_WEATHER_FORECAST  (compacted, keyed grid_zone|sim_date)" \
		&& echo "==================================================================" \
		&& $(COMPOSE) exec kafka kafka-console-consumer \
			--bootstrap-server localhost:9092 \
			--topic $$TOPIC_WEATHER_FORECAST --from-beginning \
			--property print.key=true --property key.separator=' | ' \
			--max-messages 3 --timeout-ms 15000 || true

# The second half of the Phase 1 checkpoint, and the mechanism the entire Kappa
# argument rests on: a corrected tariff is a NEW record under the same key, never
# a mutation. Log compaction then retains only the latest value per household,
# which is what makes the topic usable as a broadcast dimension in Job C and what
# makes bill restatement (R7) possible without a second processing engine.
#
# WHY THIS TARGET FORCES A SEGMENT ROLL:
# Kafka's log cleaner only compacts CLOSED segments - it never touches the active
# one. With segment.ms=60000 a segment closes when a write arrives more than 60s
# after the segment was created. So simply publishing a corrected record and
# waiting shows BOTH versions indefinitely, which looks like compaction is broken
# when it is working exactly as designed. This target therefore publishes the
# correction, waits past segment.ms, writes one more record to trigger the roll,
# and only then reads back - so the collapse is actually observable.
compaction:  ## PHASE 1 CHECKPOINT: prove the tariff topic compacts by key
	@source .env \
		&& echo "==================================================================" \
		&& echo "STEP 1  Publish a CORRECTED tariff for HH-0001 under the SAME key" \
		&& echo "        (this is what an R7 restatement looks like: an append," \
		&& echo "         never an update)" \
		&& echo "==================================================================" \
		&& printf 'HH-0001|{"household_id":"HH-0001","sim_date":"2026-01-01","tariff_rate":99.99,"billing_tier":"TIER_4","subsidy_flag":false,"effective_from":"2026-01-01T00:00:00Z","schema_version":1}\n' \
		| $(COMPOSE) exec --no-TTY kafka kafka-console-producer \
			--bootstrap-server localhost:9092 \
			--topic $$TOPIC_TARIFF_REFERENCE \
			--property parse.key=true --property key.separator='|' \
		&& echo "" \
		&& echo "Both versions are now in the log (the original and the correction):" \
		&& $(COMPOSE) exec kafka kafka-console-consumer \
			--bootstrap-server localhost:9092 \
			--topic $$TOPIC_TARIFF_REFERENCE --from-beginning \
			--property print.key=true --property key.separator=' | ' \
			--timeout-ms 15000 2>/dev/null | grep '^HH-0001' || true
	@source .env \
		&& echo "" \
		&& echo "==================================================================" \
		&& echo "STEP 2  Force the active segment to close so the cleaner can run" \
		&& echo "        (Kafka never compacts the ACTIVE segment; segment.ms=60s," \
		&& echo "         so we wait past it and then write once more)" \
		&& echo "==================================================================" \
		&& sleep 70 \
		&& printf 'HH-9999|{"household_id":"HH-9999","sim_date":"2026-01-01","tariff_rate":1.0,"billing_tier":"TIER_1","subsidy_flag":false,"effective_from":"2026-01-01T00:00:00Z","schema_version":1}\n' \
		| $(COMPOSE) exec --no-TTY kafka kafka-console-producer \
			--bootstrap-server localhost:9092 \
			--topic $$TOPIC_TARIFF_REFERENCE \
			--property parse.key=true --property key.separator='|' \
		&& echo "Waiting for the log cleaner..." \
		&& sleep 45
	@source .env \
		&& echo "" \
		&& echo "==================================================================" \
		&& echo "STEP 3  Read back: ONE surviving record per household_id." \
		&& echo "        The superseded tariff is gone; the correction remains." \
		&& echo "==================================================================" \
		&& $(COMPOSE) exec kafka kafka-console-consumer \
			--bootstrap-server localhost:9092 \
			--topic $$TOPIC_TARIFF_REFERENCE --from-beginning \
			--property print.key=true --property key.separator=' | ' \
			--timeout-ms 20000 2>/dev/null | grep '^HH-0001' || true
	@echo ""
	@echo "Log cleaner activity (proof the broker compacted, not just that we read):"
	@$(COMPOSE) logs kafka 2>/dev/null | grep -i "cleaned log" | tail -3 || true
	@echo ""
	@echo "Topic configuration (cleanup.policy=compact is what makes this work):"
	@source .env && $(COMPOSE) exec kafka kafka-configs \
		--bootstrap-server localhost:9092 --describe \
		--entity-type topics --entity-name $$TOPIC_TARIFF_REFERENCE \
		| grep -o "cleanup.policy=compact" | head -1

drop:  ## List the daily-feed drop directory and processed markers
	@echo "Drop directory (tariff CSV + weather JSON, one pair per simulated day):"
	@$(COMPOSE) exec batch-loader ls -la /data/drop || true
	@echo ""
	@echo "Processed markers (written only after a successful Kafka flush):"
	@$(COMPOSE) exec batch-loader ls -la /data/drop/.processed || true

faults:  ## Show the deliberately injected faults (Phase 2 DLQ fixtures)
	@echo "Injected faults so far - each carries the correlation_id that will"
	@echo "appear on the matching DLQ record once Job A is running (Phase 2):"
	@$(COMPOSE) logs meter-simulator 2>/dev/null \
		| grep fault_injected | tail -20 || echo "  (none yet)"
	@echo ""
	@echo "Counts by fault type:"
	@$(COMPOSE) logs meter-simulator 2>/dev/null \
		| grep -o '"fault": "[a-z_]*"' | sort | uniq -c | sort -rn || true


# ---------------------------------------------------------------------------
# PHASE 2 CHECKPOINT targets
# ---------------------------------------------------------------------------

# The first half of the Phase 2 checkpoint. Shows not just THAT records were
# rejected but WITH WHICH REASON - a DLQ full of records carrying the wrong reasons
# would pass a naive count check while making the trace demo meaningless.
dlq:  ## PHASE 2 CHECKPOINT: show DLQ records grouped by rejection reason
	@source .env \
		&& echo "==================================================================" \
		&& echo "DEAD-LETTER QUEUE: $$TOPIC_METER_READINGS_DLQ" \
		&& echo "==================================================================" \
		&& $(COMPOSE) exec kafka kafka-console-consumer \
			--bootstrap-server localhost:9092 \
			--topic $$TOPIC_METER_READINGS_DLQ --from-beginning \
			--timeout-ms 20000 2>/dev/null > /tmp/sg_dlq.json || true
	@echo ""
	@echo "Rejection reasons (should cover all four validation rules):"
	@grep -o '"rejection_reason":"[a-z_]*"' /tmp/sg_dlq.json 2>/dev/null \
		| sort | uniq -c | sort -rn || echo "  (no DLQ records yet)"
	@echo ""
	@echo "Sample records:"
	@head -3 /tmp/sg_dlq.json 2>/dev/null || true
	@echo ""
	@echo "Total DLQ records: $$(wc -l < /tmp/sg_dlq.json 2>/dev/null || echo 0)"

# The section 11 step 7 demo, and the most direct answer to the observability
# criterion's "detect and diagnose pipeline failures" wording: one correlation id
# followed from the moment the fault was deliberately injected, through Job A
# rejecting it, to the DLQ record itself.
trace:  ## Trace one correlation id end to end (make trace CID=<id>)
	@test -n "$(CID)" || { echo "usage: make trace CID=<correlation_id>"; \
		echo ""; echo "Pick one from an injected fault:"; \
		$(COMPOSE) logs meter-simulator 2>/dev/null | grep fault_injected \
			| tail -3 | grep -o '"correlation_id": "[^"]*"' || true; exit 1; }
	@echo "=================================================================="
	@echo "TRACING $(CID)"
	@echo "=================================================================="
	@echo ""
	@echo "--- 1. PRODUCER (simulators/meter_simulator.py): fault injected ---"
	@$(COMPOSE) logs meter-simulator 2>/dev/null | grep "$(CID)" || echo "  (not found)"
	@echo ""
	@echo "--- 2. PROCESSING (Job A): the batch that carried it ---"
	@$(COMPOSE) logs job-a 2>/dev/null | grep "$(CID)" || \
		echo "  (Job A logs per batch, not per record - see the DLQ record below)"
	@echo ""
	@echo "--- 3. DEAD-LETTER QUEUE: the rejected record and its reason ---"
	@source .env && $(COMPOSE) exec kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic $$TOPIC_METER_READINGS_DLQ --from-beginning \
		--timeout-ms 20000 2>/dev/null | grep "$(CID)" || echo "  (not found)"

parquet:  ## Show the curated Parquet archive partitions and row counts
	@source .env \
		&& echo "Curated archive: $$PARQUET_PATH" \
		&& echo "" \
		&& echo "Partitions (sim_date / grid_zone - the two predicates every replay" \
		&& echo "and every daily report filters on, so pruning skips whole dirs):" \
		&& curl -s "http://localhost:$$S3_API_HOST_PORT/$$S3_CURATED_BUCKET/?list-type=2&prefix=readings/&delimiter=/" \
			| grep -o '<Prefix>[^<]*</Prefix>' | sed 's/<[^>]*>//g' | sort || true
	@echo ""
	@echo "Total objects and bytes under readings/:"
	@source .env && curl -s "http://localhost:$$S3_API_HOST_PORT/$$S3_CURATED_BUCKET/?list-type=2&prefix=readings/" \
		| grep -c "<Key>" | sed 's/^/  objects: /' || true

spark-ui:  ## Print the Spark master UI URL
	@source .env && echo "Spark master UI: http://localhost:$$SPARK_MASTER_UI_PORT"
	@echo "  (shows the running Job A query, its executors and micro-batch history)"

# The transform tests need a working Spark. PySpark's local mode requires a Hadoop
# native environment (winutils.exe on Windows) that `pip install pyspark` does not
# provide, so on some hosts those tests SKIP rather than fail - keeping `make test`
# green on a clean clone. This target runs them where Spark is known to work, which
# is also where the jobs actually run. It is part of the Phase 2 checkpoint, not an
# optional extra: tests that only ever skip would prove nothing.
test-spark:  ## Run the Spark transform tests inside the Spark container
	@$(COMPOSE) run --rm --no-deps \
		-e SIM_START_DATE=2026-01-01 -e SIM_DAY_REAL_SECONDS=300 \
		--entrypoint sh job-a -c \
		"pip install -q pytest >/dev/null 2>&1; cd /app && python3 -m pytest tests/unit/test_validation.py tests/unit/test_enrichment.py -q"

test:  ## Run the unit test suite
	python -m pytest tests/unit -v

clock:  ## Print the simulated-clock banner
	@python -c "from common.sim_clock import startup_banner; print(startup_banner())"

# DESTRUCTIVE: removes the named volumes, so Kafka's log, the Postgres
# databases and the curated object-store bucket are all lost. Prompts first,
# because deleting
# the Kafka log destroys the replay history the whole architecture rests on.
clean:  ## Destroy the stack AND all data (prompts first)
	@echo "This deletes all Kafka log data, Postgres databases and archived objects."
	@read -p "Type 'yes' to confirm: " confirm && [ "$$confirm" = "yes" ]
	$(COMPOSE) down -v
	@echo "Clean. Run 'make up' to rebuild from scratch."
