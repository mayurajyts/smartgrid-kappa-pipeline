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

.PHONY: help env anchor up down ps logs topics psql s3 test clock clean

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
