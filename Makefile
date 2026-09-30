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
        consume compaction drop faults dlq trace parquet spark-ui test-spark \
        dev fast demo-config spark-base rebuild \
        serving-schema spark-base-force zones bills bill-replay upsert-restart \
        api grafana endpoints \
        airflow dags report restate versions \
        metrics alerts observability

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

up: env anchor spark-base  ## Start the stack and create topics/buckets
	$(COMPOSE) up -d --build
	@echo ""
	@echo "Waiting for the one-shot init containers to finish..."
	@# `compose up -d` returns once the init containers have STARTED, not once
	@# they have finished creating topics and buckets. Polling until they are no
	@# longer running makes `make up` mean "the stack is ready", so the
	@# checkpoint cannot be inspected while setup is still in flight.
	@for i in $$(seq 1 60); do 		running=$$($(COMPOSE) ps --status running --services 2>/dev/null 			| grep -E '^(kafka-init|objectstore-init)$$' || true); 		[ -z "$$running" ] && break; 		sleep 2; 	done
	@echo ""
	@# Applied on EVERY `make up`, not only on a fresh volume. The init hook in
	@# docker/postgres/init/ fires only when the data directory is empty, so an
	@# existing volume would otherwise never gain the Phase 3 tables. Safe to
	@# repeat because 001_schema.sql is entirely IF NOT EXISTS.
	@$(MAKE) --no-print-directory serving-schema
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

serving-schema:  ## Apply serving/sql/001_schema.sql to a running Postgres (idempotent)
	@# WHY THIS TARGET EXISTS: the postgres init hook (docker/postgres/init/) runs
	@# ONLY when the data volume is empty. Anyone whose postgres-data volume
	@# predates Phase 3 -- which is everyone who ran Phases 0-2 -- would otherwise
	@# never get these tables, and Job B would die on its first upsert with a bare
	@# JDBC "relation does not exist" that says nothing about why.
	@#
	@# The file is piped in on STDIN rather than read from inside the container, so
	@# this works with no rebuild and no `make clean`. Every statement in it is
	@# IF NOT EXISTS, which is what makes running it on every `make up` correct
	@# rather than merely tolerable.
	@source .env \
		&& $(COMPOSE) exec -T postgres \
		psql -v ON_ERROR_STOP=1 -U $$POSTGRES_SUPERUSER -d $$POSTGRES_DB \
		< serving/sql/001_schema.sql > /dev/null
	@# The tables are created by the SUPERUSER, so the role the Spark jobs connect
	@# as owns none of them. Without this grant every upsert fails on permissions,
	@# which surfaces inside a Spark executor log rather than here.
	@source .env \
		&& $(COMPOSE) exec -T postgres \
		psql -v ON_ERROR_STOP=1 -U $$POSTGRES_SUPERUSER -d $$POSTGRES_DB -c \
		"GRANT ALL ON ALL TABLES IN SCHEMA public TO $$POSTGRES_USER;\
		 ALTER DEFAULT PRIVILEGES IN SCHEMA public\
		 GRANT ALL ON TABLES TO $$POSTGRES_USER;" > /dev/null
	@echo "Serving schema applied to the smartgrid database (idempotent)."

zones:  ## PHASE 3 CHECKPOINT: Job B zone windows and their size
	@echo "=================================================================="
	@echo "ZONE AGGREGATES (zone_load_1m) -- Job B, answering R1 and R2"
	@echo ""
	@echo "PASS criteria:"
	@echo "  * window span is exactly 288 simulated minutes (= 1 REAL minute"
	@echo "    at 288x). A literal reading of the plan gives 1 sim-minute and"
	@echo "    ~86,400 rows/real-minute -- see the Job B module docstring."
	@echo "  * rows grow at ~5 per real minute (one per zone), not thousands"
	@echo "  * renewable_pct in [0,100], or NULL for a zone with no load"
	@echo "  * active_meter_count near 40 per zone (200 households / 5 zones)"
	@echo "=================================================================="
	@source .env && $(COMPOSE) exec -T postgres \
		psql -q -P pager=off -U $$POSTGRES_SUPERUSER -d $$POSTGRES_DB -c "SELECT grid_zone, window_start, round(total_consumption_kwh,3) AS kwh, round(total_solar_kwh,3) AS solar, round(renewable_pct,1) AS renew_pct, active_meter_count AS meters, late_event_count AS late FROM zone_load_1m ORDER BY window_start DESC, grid_zone LIMIT 15;"
	@echo "--- window span in simulated minutes (MUST be 288) ---"
	@source .env && $(COMPOSE) exec -T postgres \
		psql -q -P pager=off -U $$POSTGRES_SUPERUSER -d $$POSTGRES_DB -c "SELECT DISTINCT extract(epoch FROM (window_end-window_start))/60 AS sim_minutes FROM zone_load_1m;"
	@echo "--- totals ---"
	@source .env && $(COMPOSE) exec -T postgres \
		psql -q -P pager=off -U $$POSTGRES_SUPERUSER -d $$POSTGRES_DB -c "SELECT count(*) AS rows, count(DISTINCT window_start) AS windows, count(DISTINCT grid_zone) AS zones, min(window_start) AS first_window, max(window_start) AS last_window FROM zone_load_1m;"

bills:  ## PHASE 3 CHECKPOINT: Job C running household bills
	@echo "=================================================================="
	@echo "RUNNING BILLS (household_billing_running) -- Job C, R5 and R6"
	@echo ""
	@echo "A LIVE ESTIMATE, not an issued bill. household_billing_daily is"
	@echo "versioned and written by Airflow in Phase 5."
	@echo ""
	@echo "PASS criteria:"
	@echo "  * ~200 rows per sim_date (one per household)"
	@echo "  * tariff_missing TRUE for the current day (feed arrives at D+1,"
	@echo "    assumption 14) and FALSE for completed days"
	@echo "  * running_cost NULL exactly where tariff_missing -- never guessed"
	@echo "=================================================================="
	@source .env && $(COMPOSE) exec -T postgres \
		psql -q -P pager=off -U $$POSTGRES_SUPERUSER -d $$POSTGRES_DB -c "SELECT sim_date, count(*) AS households, count(running_cost) AS priced, sum(CASE WHEN tariff_missing THEN 1 ELSE 0 END) AS unpriced, round(sum(consumption_kwh),2) AS total_kwh, round(sum(running_cost),2) AS total_lkr FROM household_billing_running GROUP BY sim_date ORDER BY sim_date DESC;"
	@echo "--- sample priced bills (the block ladder in action) ---"
	@source .env && $(COMPOSE) exec -T postgres \
		psql -q -P pager=off -U $$POSTGRES_SUPERUSER -d $$POSTGRES_DB -c "SELECT household_id, sim_date, round(consumption_kwh,3) AS cons, round(solar_kwh,3) AS solar, round(net_grid_kwh,3) AS net, round(self_consumption_ratio,3) AS self_ratio, running_cost, tariff_missing FROM household_billing_running WHERE running_cost IS NOT NULL ORDER BY running_cost DESC LIMIT 8;"
	@echo "--- INVARIANT: running_cost IS NULL iff tariff_missing (MUST be 0) ---"
	@source .env && $(COMPOSE) exec -T postgres \
		psql -q -P pager=off -U $$POSTGRES_SUPERUSER -d $$POSTGRES_DB -c "SELECT count(*) AS contradictions FROM household_billing_running WHERE (running_cost IS NULL) <> tariff_missing;"

upsert-restart:  ## PHASE 3 CHECKPOINT: upserts idempotent under forced restart
	@echo "=================================================================="
	@echo "IDEMPOTENCE UNDER FORCED RESTART"
	@echo ""
	@echo "SIGKILL, not a graceful stop: a graceful stop lets the query finish"
	@echo "its batch and commit, which tests nothing. The point is a crash"
	@echo "between the staging write and the checkpoint commit."
	@echo "=================================================================="
	@source .env && $(COMPOSE) exec -T postgres \
		psql -q -P pager=off -U $$POSTGRES_SUPERUSER -d $$POSTGRES_DB -c "DROP TABLE IF EXISTS zone_replay_baseline; CREATE TABLE zone_replay_baseline AS SELECT * FROM zone_load_1m;"
	@source .env && $(COMPOSE) exec -T postgres \
		psql -q -P pager=off -U $$POSTGRES_SUPERUSER -d $$POSTGRES_DB -c "SELECT count(*) AS baseline_rows FROM zone_replay_baseline;"
	@echo "Killing job-b (SIGKILL)..."
	@docker kill --signal=KILL smartgrid-job-b >/dev/null 2>&1 || true
	@$(COMPOSE) up -d job-b >/dev/null 2>&1
	@echo "Restarted. Waiting 90s for the uncommitted batch to replay..."
	@sleep 90
	@echo "--- rows CHANGED in a SETTLED window (MUST be 0) ---"
	@echo "    Windows inside the watermark may legitimately change: update"
	@echo "    mode re-emits them as late data arrives, so only windows older"
	@echo "    than one watermark (576 sim-min) are compared."
	@source .env && $(COMPOSE) exec -T postgres \
		psql -q -P pager=off -U $$POSTGRES_SUPERUSER -d $$POSTGRES_DB -c "SELECT count(*) AS differing_rows FROM zone_replay_baseline b JOIN zone_load_1m z USING (grid_zone, window_start) WHERE z.window_end < (SELECT max(window_end) FROM zone_load_1m) - interval '576 minutes' AND (b.total_consumption_kwh IS DISTINCT FROM z.total_consumption_kwh OR b.total_solar_kwh IS DISTINCT FROM z.total_solar_kwh OR b.active_meter_count IS DISTINCT FROM z.active_meter_count);"
	@echo "--- duplicate primary keys (MUST be 0) ---"
	@source .env && $(COMPOSE) exec -T postgres \
		psql -q -P pager=off -U $$POSTGRES_SUPERUSER -d $$POSTGRES_DB -c "SELECT count(*) AS duplicate_keys FROM (SELECT grid_zone, window_start FROM zone_load_1m GROUP BY 1,2 HAVING count(*) > 1) d;"
	@echo "--- staging tables hold one batch, never grow ---"
	@source .env && $(COMPOSE) exec -T postgres \
		psql -q -P pager=off -U $$POSTGRES_SUPERUSER -d $$POSTGRES_DB -c "SELECT relname, n_live_tup FROM pg_stat_user_tables WHERE relname LIKE 'stg_%%' ORDER BY relname;"

api:  ## Show the serving API URLs (Phase 4)
	@source .env \
		&& echo "OpenAPI docs:  http://localhost:$$API_HOST_PORT/docs" \
		&& echo "Health:        http://localhost:$$API_HOST_PORT/health" \
		&& echo "Zone load:     http://localhost:$$API_HOST_PORT/api/v1/zones/load" \
		&& echo "Grid summary:  http://localhost:$$API_HOST_PORT/api/v1/grid/summary" \
		&& echo "Metrics:       http://localhost:$$API_HOST_PORT/metrics"

grafana:  ## Show the Grafana dashboard URL (Phase 4)
	@source .env \
		&& echo "Business dashboard:" \
		&& echo "  http://localhost:$$GRAFANA_HOST_PORT/d/smartgrid-business" \
		&& echo "" \
		&& echo "Anonymous viewing is enabled, so no login is needed to view." \
		&& echo "To edit: $$GRAFANA_ADMIN_USER / $$GRAFANA_ADMIN_PASSWORD"

endpoints:  ## PHASE 4 CHECKPOINT: every section 9 endpoint returns live data
	@echo "=================================================================="
	@echo "SERVING API CHECKPOINT (section 9)"
	@echo ""
	@echo "PASS: every endpoint returns 200 with a non-empty body."
	@echo "  /alerts returns an empty list until Phase 6 -- that is correct,"
	@echo "  not a failure: the table is created by Job D."
	@echo "=================================================================="
	@source .env && base=http://localhost:$$API_HOST_PORT; \
		hh=$$($(COMPOSE) exec -T postgres psql -tAq -U $$POSTGRES_SUPERUSER \
		   -d $$POSTGRES_DB -c "SELECT household_id FROM household_billing_running LIMIT 1" \
		   2>/dev/null | tr -d "[:space:]"); \
		[ -z "$$hh" ] && hh=HH-0001; \
		zone=$$($(COMPOSE) exec -T postgres psql -tAq -U $$POSTGRES_SUPERUSER \
		   -d $$POSTGRES_DB -c "SELECT grid_zone FROM zone_load_1m LIMIT 1" \
		   2>/dev/null | tr -d "[:space:]"); \
		[ -z "$$zone" ] && zone=ZONE-A; \
		sd=$$($(COMPOSE) exec -T postgres psql -tAq -U $$POSTGRES_SUPERUSER \
		   -d $$POSTGRES_DB -c "SELECT max(sim_date) FROM household_billing_running" \
		   2>/dev/null | tr -d "[:space:]"); \
		for path in /health \
		    /api/v1/zones/load \
		    "/api/v1/zones/$$zone/timeseries?limit=5" \
		    /api/v1/grid/summary \
		    "/api/v1/households/$$hh/bill" \
		    "/api/v1/households/$$hh/bill/versions" \
		    /api/v1/households/top \
		    "/api/v1/reports/daily/$$sd" \
		    "/api/v1/alerts?active=true" \
		    /api/v1/pipeline/status; do \
		  code=$$(curl -s -o /tmp/sg_api.json -w "%{http_code}" "$$base$$path"); \
		  size=$$(wc -c < /tmp/sg_api.json | tr -d " "); \
		  printf "  %-4s %-6s %s\n" "$$code" "$${size}B" "$$path"; \
		done
	@echo ""
	@echo "Sample payload (grid summary):"
	@source .env && curl -s http://localhost:$$API_HOST_PORT/api/v1/grid/summary | head -c 600
	@echo ""

airflow:  ## Show the Airflow UI URL and DAG list (Phase 5)
	@source .env \
		&& echo "Airflow UI:  http://localhost:$$AIRFLOW_HOST_PORT" \
		&& echo "Login:       $$AIRFLOW_ADMIN_USER / $$AIRFLOW_ADMIN_PASSWORD" \
		&& echo ""
	@$(COMPOSE) exec -T airflow airflow dags list 2>/dev/null \
		| grep -E "smartgrid|dag_id|seal|billing|quality|replay" || \
		echo "  (airflow not running yet - try: docker compose up -d airflow)"

dags:  ## PHASE 5 CHECKPOINT: DAG state and recent runs
	@echo "=================================================================="
	@echo "AIRFLOW DAGS (section 7)"
	@echo ""
	@echo "  seal_sim_day         every 2 real min - declares a sim-day complete"
	@echo "  data_quality_checks  every 5 real min - reject rate / coverage / kWh"
	@echo "  daily_billing_report every 5 real min - issues version 1 of a bill"
	@echo "  replay_sim_day       MANUAL - restates a bill as version N+1 (R7)"
	@echo "=================================================================="
	@$(COMPOSE) exec -T airflow airflow dags list 2>/dev/null | head -12 || true
	@echo ""
	@echo "--- sim-day lifecycle ---"
	@source .env && $(COMPOSE) exec -T postgres \
		psql -q -P pager=off -U $$POSTGRES_SUPERUSER -d $$POSTGRES_DB -c "SELECT sim_date, status, sealed_at IS NOT NULL AS sealed, tariff_received_at IS NOT NULL AS has_tariff, report_generated_at IS NOT NULL AS reported FROM sim_day_state ORDER BY sim_date DESC LIMIT 8;"
	@echo "--- recent audit rows ---"
	@source .env && $(COMPOSE) exec -T postgres \
		psql -q -P pager=off -U $$POSTGRES_SUPERUSER -d $$POSTGRES_DB -c "SELECT job_name, sim_date, records_in, records_out, status, left(notes, 60) AS notes FROM pipeline_run_audit ORDER BY started_at DESC LIMIT 8;"

report:  ## PHASE 5 CHECKPOINT: show generated billing report files
	@echo "=================================================================="
	@echo "BILLING REPORT ARTIFACTS (section 7 step 4)"
	@echo ""
	@echo "PASS: at least one CSV and one HTML file per billed simulated day."
	@echo "=================================================================="
	@$(COMPOSE) exec -T airflow ls -la /data/reports 2>/dev/null \
		|| echo "  (no reports yet - daily_billing_report has not succeeded)"
	@echo ""
	@echo "--- issued bills by version ---"
	@source .env && $(COMPOSE) exec -T postgres \
		psql -q -P pager=off -U $$POSTGRES_SUPERUSER -d $$POSTGRES_DB -c "SELECT sim_date, version, count(*) AS households, round(sum(final_bill),2) AS total_lkr, bool_and(is_current) AS is_current FROM household_billing_daily GROUP BY sim_date, version ORDER BY sim_date DESC, version DESC LIMIT 10;"

restate:  ## PHASE 5 CHECKPOINT: trigger replay_sim_day (R7 restatement)
	@echo "Triggering replay_sim_day for the most recently billed day..."
	@$(COMPOSE) exec -T airflow airflow dags trigger replay_sim_day \
		-c '{"reason":"make restate - demonstrating R7"}' 2>&1 | tail -3
	@echo ""
	@echo "Watch it in the UI, then run: make versions"

versions:  ## PHASE 5 CHECKPOINT: prove R7 - version 2 supersedes version 1
	@echo "=================================================================="
	@echo "BILL RESTATEMENT (R7) - the payoff of the Kappa decision"
	@echo ""
	@echo "PASS criteria:"
	@echo "  * more than one version exists for a restated day"
	@echo "  * EXACTLY ONE version is is_current (enforced by a partial"
	@echo "    unique index, so a breach fails loudly rather than silently)"
	@echo "  * the superseded version is still readable - that is the audit"
	@echo "    trail the whole architecture argument rests on"
	@echo "=================================================================="
	@source .env && $(COMPOSE) exec -T postgres \
		psql -q -P pager=off -U $$POSTGRES_SUPERUSER -d $$POSTGRES_DB -c "SELECT sim_date, version, count(*) AS households, round(sum(final_bill),2) AS total_lkr, bool_and(is_current) AS current FROM household_billing_daily GROUP BY sim_date, version ORDER BY sim_date DESC, version DESC;"
	@echo "--- INVARIANT: exactly one current version per day (MUST be 0) ---"
	@source .env && $(COMPOSE) exec -T postgres \
		psql -q -P pager=off -U $$POSTGRES_SUPERUSER -d $$POSTGRES_DB -c "SELECT count(*) AS days_with_wrong_current_count FROM (SELECT sim_date, count(DISTINCT version) AS v FROM household_billing_daily WHERE is_current GROUP BY sim_date HAVING count(DISTINCT version) <> 1) bad;"
	@echo "--- one household across versions (the before/after) ---"
	@source .env && $(COMPOSE) exec -T postgres \
		psql -q -P pager=off -U $$POSTGRES_SUPERUSER -d $$POSTGRES_DB -c "SELECT household_id, sim_date, version, tariff_rate, billing_tier, gross_cost, final_bill, is_current FROM household_billing_daily WHERE household_id = (SELECT household_id FROM household_billing_daily ORDER BY version DESC, final_bill DESC LIMIT 1) ORDER BY sim_date DESC, version;"

observability:  ## Show all observability URLs (Phase 6)
	@source .env \
		&& echo "Grafana business:  http://localhost:$$GRAFANA_HOST_PORT/d/smartgrid-business" \
		&& echo "Grafana platform:  http://localhost:$$GRAFANA_HOST_PORT/d/smartgrid-platform" \
		&& echo "Prometheus:        http://localhost:$$PROMETHEUS_HOST_PORT" \
		&& echo "  alert rules:     http://localhost:$$PROMETHEUS_HOST_PORT/alerts" \
		&& echo "  scrape targets:  http://localhost:$$PROMETHEUS_HOST_PORT/targets" \
		&& echo "Alertmanager:      http://localhost:$$ALERTMANAGER_HOST_PORT" \
		&& echo "Airflow:           http://localhost:$$AIRFLOW_HOST_PORT" \
		&& echo "API docs:          http://localhost:$$API_HOST_PORT/docs"

metrics:  ## PHASE 6 CHECKPOINT: scrape targets and key metric values
	@echo "=================================================================="
	@echo "PROMETHEUS TARGETS (section 8)"
	@echo ""
	@echo "PASS: every target healthy. `up == 0` is the one signal that stays"
	@echo "true when a process is too broken to export anything else."
	@echo "=================================================================="
	@source .env && curl -s \
		"http://localhost:$$PROMETHEUS_HOST_PORT/api/v1/targets?state=active" \
		| python -c "import json,sys; d=json.load(sys.stdin)['data']['activeTargets']; \
		print('  %d up / %d total' % (len([t for t in d if t['health']=='up']), len(d))); \
		[print('  %-22s %s' % (t['labels'].get('instance'), t['health'])) for t in d]"
	@echo ""
	@echo "--- key metric values ---"
	@source .env && for q in \
		'sum(rate(smartgrid_events_produced_total[2m]))' \
		'sum(rate(smartgrid_events_consumed_total[2m]))' \
		'sum(rate(smartgrid_serving_rows_upserted_total[5m]))' \
		'max(smartgrid_billing_unpriced_households)' \
		'smartgrid_daily_report_success_total'; do \
		  v=$$(curl -s --get --data-urlencode "query=$$q" \
		     "http://localhost:$$PROMETHEUS_HOST_PORT/api/v1/query" \
		     | python -c "import json,sys; r=json.load(sys.stdin)['data']['result']; \
		       print(r[0]['value'][1] if r else 'no data')"); \
		  printf "  %-52s %s\n" "$$q" "$$v"; \
		done

alerts:  ## PHASE 6 CHECKPOINT: alert rules loaded and their state
	@echo "=================================================================="
	@echo "ALERT RULES (section 8 requires at least four; seven are defined)"
	@echo ""
	@echo "  inactive = condition false (healthy)"
	@echo "  pending  = condition TRUE, waiting out its `for:` duration"
	@echo "  firing   = alert sent to Alertmanager"
	@echo ""
	@echo "PASS: all seven present. LowRenewableContribution pending overnight"
	@echo "in simulated time is CORRECT - the diurnal cycle, not a fault."
	@echo "=================================================================="
	@source .env && curl -s http://localhost:$$PROMETHEUS_HOST_PORT/api/v1/rules \
		| python -c "import json,sys; g=json.load(sys.stdin)['data']['groups']; \
		[print('  %-9s %-28s %s' % (r['state'], r['name'], \
		  r['labels'].get('severity',''))) for x in g for r in x['rules']]"
	@echo ""
	@echo "--- currently firing in Alertmanager ---"
	@source .env && curl -s http://localhost:$$ALERTMANAGER_HOST_PORT/api/v2/alerts \
		| python -c "import json,sys; a=json.load(sys.stdin); \
		print('  (none)') if not a else \
		[print('  %s %s' % (x['labels'].get('alertname'), \
		  x['labels'].get('grid_zone',''))) for x in a]" 2>/dev/null \
		|| echo "  (alertmanager not reachable)"

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


# ---------------------------------------------------------------------------
# DEVELOPMENT SPEEDUPS
#
# These exist because Phase 2 showed the bottleneck was never the tests (the host
# suite runs in 26 seconds) - it was image rebuilds and waiting on simulated-day
# boundaries. Each target below removes one of those stalls.
# ---------------------------------------------------------------------------

# Build the expensive Spark layers (pip install + ~500 MB of connector JARs) ONCE.
# `make up` and `make rebuild` depend on this; it only re-runs when a dependency or
# connector version actually changes.
spark-base:  ## Build the Spark base image (deps + JARs; slow, run once)
	@if ! docker image inspect smartgrid-spark-base:latest >/dev/null 2>&1; then \
		echo "Building smartgrid-spark-base (one-off, several minutes)..."; \
		docker build -f docker/spark/Dockerfile.base -t smartgrid-spark-base:latest .; \
	else \
		echo "smartgrid-spark-base already present (delete it to force a rebuild)"; \
	fi

# Rebuild ONLY the thin application layer. ~5 seconds, versus ~6 minutes before the
# base image was split out.
spark-base-force:  ## Force-rebuild the Spark base image (after a JAR or dep change)
	@# `make spark-base` deliberately SKIPS the build when the image already exists,
	@# which is what keeps the inner loop at ~5 seconds. That gate also means a new
	@# JAR or pip dependency in docker/spark/Dockerfile.base is silently ignored --
	@# the stale image is served instead, and the symptom is a runtime error that
	@# looks like application code (e.g. "No suitable driver found").
	@#
	@# This target removes the image so the build actually runs. ~6 real minutes,
	@# and needed only when Dockerfile.base itself changes.
	docker image rm -f smartgrid-spark-base:latest >/dev/null 2>&1 || true
	@$(MAKE) --no-print-directory spark-base
	@docker build -q -f docker/spark/Dockerfile -t smartgrid-spark:latest . >/dev/null
	@echo "Base and app images rebuilt."

rebuild: spark-base  ## Rebuild the Spark app image after a code change (~5s)
	@docker build -q -f docker/spark/Dockerfile -t smartgrid-spark:latest . >/dev/null
	@# All three streaming jobs share one image, so a code change to any of them
	@# needs all three restarted -- restarting only job-a would leave B and C
	@# running the previous build, which reads as a change that had no effect.
	@$(COMPOSE) up -d job-a job-b job-c
	@echo "Rebuilt and restarted job-a, job-b, job-c."

# Start the stack with the source tree bind-mounted, so a code edit needs only a
# container restart and no rebuild at all. See docker-compose.dev.yml for the
# trade-off: a bind-mounted stack is NOT reproducible from images alone, which is
# why `make up` (the demo path) does not do this.
dev: env anchor spark-base  ## Start with code bind-mounted (fast inner loop)
	$(COMPOSE) -f docker-compose.yml -f docker-compose.dev.yml up -d --build
	@echo ""
	@echo "Dev mode: code is bind-mounted."
	@echo "  After editing a file:  docker compose restart job-a   (~20s, no rebuild)"
	@echo "  For the demo, use:     make demo-config && make up"

# Compress the simulated day so day-boundary behaviour (tariff drops, bill
# generation, sim-day sealing) is observable in 1 real minute instead of 5.
#
# NOT for the demo: the report, the README and the logs all state
# "1 simulated day = 5 real minutes", and that claim must hold when it is assessed.
# `make demo-config` restores it, and this target prints the warning every time so
# the change cannot be made and then forgotten.
fast: env  ## Shorten the sim day to 60s for development (5x faster feedback)
	@sed -i.bak 's/^SIM_DAY_REAL_SECONDS=.*/SIM_DAY_REAL_SECONDS=60/' .env && rm -f .env.bak
	@echo "SIM_DAY_REAL_SECONDS=60  (1 sim day = 1 real minute)"
	@echo ""
	@echo "!! DEVELOPMENT ONLY. The report and README state 5 real minutes."
	@echo "!! Run 'make demo-config' before recording the demo or taking screenshots."
	@echo ""
	@echo "Restart the stack for this to take effect:  make dev   (or make up)"

demo-config: env  ## Restore the documented demo timings (1 sim day = 5 min)
	@sed -i.bak 's/^SIM_DAY_REAL_SECONDS=.*/SIM_DAY_REAL_SECONDS=300/' .env && rm -f .env.bak
	@echo "SIM_DAY_REAL_SECONDS=300  (1 sim day = 5 real minutes, as documented)"
	@echo "Restart the stack for this to take effect:  make up"

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
