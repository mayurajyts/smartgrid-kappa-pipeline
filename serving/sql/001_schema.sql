-- ============================================================================
-- Serving store schema (plan §6.5)
--
-- WHY THIS EXISTS IN THE ARCHITECTURE
-- -----------------------------------
-- This is where the Kappa pipeline stops being a stream and becomes an answer.
-- Everything upstream is an immutable, replayable log; these tables are the
-- mutable, queryable projection of it that the API (§9) and Grafana read.
--
-- The distinction matters in the viva: losing this database loses nothing
-- permanent, because it can be rebuilt by replaying the log through the same
-- jobs. That is the property Lambda pays for with a second codebase and Kappa
-- gets structurally.
--
-- WHY POSTGRES AND NOT A TIME-SERIES OR WIDE-COLUMN STORE (§3)
-- ------------------------------------------------------------
-- The serving access pattern is point lookups (/households/{id}/bill), small
-- range scans over recent windows, and -- decisively -- the transactional
-- multi-row swap that bill restatement (R7) needs. Cassandra is better at the
-- write path and gives neither of the last two.
--
-- IDEMPOTENCY IS A SCHEMA CONCERN, NOT ONLY A CODE ONE
-- ----------------------------------------------------
-- Spark's foreachBatch sink is at-least-once: a crash before the checkpoint
-- commits replays the whole batch. Every table written by a streaming job
-- therefore has a PRIMARY KEY on its business identity, so the writer can use
-- INSERT ... ON CONFLICT DO UPDATE and a replayed batch overwrites rather than
-- duplicates. That is how at-least-once delivery becomes effectively-once
-- storage -- the argument §7 asks to be stated explicitly.
--
-- NUMERIC, NEVER DOUBLE PRECISION
-- -------------------------------
-- Every column that holds money or is summed is NUMERIC. Binary floating point
-- cannot represent 22.50 exactly, and a daily bill accumulates ~288 additions
-- per household. With float8 a replay would produce a bill that is *nearly* the
-- same -- which is worse than one clearly different, because it would quietly
-- falsify the claim that replay is deterministic. This is the same argument that
-- forces Decimal in processing/transforms/billing.py; the two must agree, or the
-- guarantee only holds up to the weaker of them.
--
-- RE-RUNNABLE BY CONSTRUCTION
-- ---------------------------
-- Every statement is IF NOT EXISTS. This file is applied both by the Postgres
-- init hook (only on an empty data volume) and by `make serving-schema` (any
-- time, against a running instance). Running it twice must be a no-op, because
-- the second path runs on every `make up`.
--
-- SCOPE: §6.5's `alerts` and `sim_day_state` are deliberately ABSENT. They are
-- written by Job D (Phase 6) and Airflow (Phase 5) respectively, and shipping a
-- schema before the code that writes it exists would mean shipping a contract
-- nobody has yet validated against a real writer.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- zone_load_1m -- Job B. The real-time answer to R1 (grid load per zone) and
-- R2 (renewable contribution per zone).
--
-- THE "1m" IN THE NAME MEANS ONE REAL MINUTE, NOT ONE SIMULATED MINUTE.
-- This is the single most important comment in this file.
--
-- §7 specifies a "1 minute" tumbling window. But event_timestamp is on the
-- SIMULATED axis, which runs 288x faster than wall clock (1 sim day = 5 real
-- minutes, §0). One simulated minute is 0.21 real seconds, so a literal reading
-- would close a window roughly five times per second and write about 86,400 rows
-- into this table every real minute -- unplottable in Grafana, and a write storm
-- on the demo host.
--
-- §7 was written against the observer's clock: a control-room dashboard refreshes
-- about once a minute. So the window is ONE REAL MINUTE, converted onto the
-- simulated axis by compression_ratio() to 288 simulated minutes -- 4.8 simulated
-- hours, 5 buckets per simulated day, 5 rows per real minute across the 5 zones.
-- The table name from §6.5 is kept because it is a contract; its meaning is
-- recorded here and in the Job B module docstring.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS zone_load_1m (
    grid_zone              TEXT           NOT NULL,
    window_start           TIMESTAMPTZ    NOT NULL,
    window_end             TIMESTAMPTZ    NOT NULL,

    total_consumption_kwh  NUMERIC(18, 6) NOT NULL,
    total_solar_kwh        NUMERIC(18, 6) NOT NULL,

    -- NULL when total_consumption_kwh is 0: solar/0 has no defensible value, and
    -- writing 0 would misreport a silent zone as having no renewable generation
    -- when in truth it reported nothing at all.
    --
    -- CONSEQUENCE FOR PHASE 6, recorded here so it is not rediscovered: the
    -- LOW_RENEWABLE alert rule (§7, Job D) must treat NULL as "not low".
    -- Otherwise a zone that has gone silent fires a renewable alert instead of
    -- the staleness alert (R4) that actually describes what is wrong.
    renewable_pct          NUMERIC(6, 3),

    -- approx_count_distinct, not an exact count. It is a health signal, not a
    -- billed quantity, and an exact distinct count would force a full shuffle per
    -- window for a number nobody reconciles. Expected ~40 per zone (200/5).
    active_meter_count     INTEGER        NOT NULL,

    -- Readings whose producer_emitted_at lagged their event_timestamp by more
    -- than one producer tick -- i.e. the simulator's injected late-event fault,
    -- which Job A passes through as valid data.
    --
    -- NOT the count of events dropped BY the watermark. Those never reach the
    -- aggregation, so they are uncountable inside a groupBy; observing them needs
    -- numRowsDroppedByWatermark from a StreamingQueryListener, a different
    -- mechanism (noted for Phase 6). §7 does not distinguish the two readings;
    -- this column implements the one that is implementable here.
    late_event_count       INTEGER        NOT NULL DEFAULT 0,

    updated_at             TIMESTAMPTZ    NOT NULL DEFAULT now(),

    -- The idempotency key. Job B runs in `update` output mode, so an open window
    -- is re-emitted every micro-batch as more of its data arrives; each
    -- re-emission must overwrite the previous row, not append beside it.
    PRIMARY KEY (grid_zone, window_start)
);

-- Serves "the last N windows across all zones", which is what the dashboard and
-- /api/v1/zones/load ask for. DESC because every such query wants the newest end.
CREATE INDEX IF NOT EXISTS zone_load_1m_window_start_idx
    ON zone_load_1m (window_start DESC);


-- ----------------------------------------------------------------------------
-- household_billing_running -- Job C. A LIVE ESTIMATE of the current simulated
-- day's bill (R5, R6), updated every micro-batch.
--
-- THIS IS NOT THE BILL. The authoritative, issued bill is
-- household_billing_daily, materialised by Airflow (Phase 5) once the day is
-- sealed and its tariff has landed. Keeping the running estimate in a separate
-- table from the issued bill is what allows the estimate to be incomplete -- it
-- is computed before the tariff for the day has necessarily arrived -- without
-- ever risking an incorrect invoice. Job C must never write the daily table, or
-- R7's version/is_current semantics become ambiguous about who authored a
-- version.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS household_billing_running (
    household_id            TEXT           NOT NULL,
    sim_date                DATE           NOT NULL,

    consumption_kwh         NUMERIC(18, 6) NOT NULL,
    solar_kwh               NUMERIC(18, 6) NOT NULL,

    -- Signed. Negative means the household was a net exporter over the day.
    net_grid_kwh            NUMERIC(18, 6) NOT NULL,

    -- self_consumed / generated. NULL when the household generated nothing -- a
    -- house with no panels has no self-consumption ratio, as distinct from one
    -- with panels and a ratio of zero.
    self_consumption_ratio  NUMERIC(6, 4),

    -- NULL when the tariff for this sim_date has not arrived. See tariff_missing.
    running_cost            NUMERIC(14, 2),

    -- DEVIATION FROM §6.5, deliberate and flagged in the report.
    --
    -- §6.5 lists no such column, which leaves "running_cost IS NULL" carrying two
    -- different meanings: "the tariff feed has not arrived yet" (§7's explicit
    -- availability-vs-correctness choice -- write the kWh, never guess a rate)
    -- and "the tariff arrived and the cost is genuinely zero" (a household that
    -- drew nothing from the grid). The API (§9) has to tell a customer WHICH, and
    -- no query over the other columns can recover the distinction once lost.
    --
    -- Defaults TRUE because a row can only be inserted before pricing succeeds.
    tariff_missing          BOOLEAN        NOT NULL DEFAULT TRUE,

    updated_at              TIMESTAMPTZ    NOT NULL DEFAULT now(),

    -- Same role as zone_load_1m's key: Job C aggregates in `update` mode, so each
    -- micro-batch re-emits the running total for every household active in it.
    PRIMARY KEY (household_id, sim_date)
);

CREATE INDEX IF NOT EXISTS household_billing_running_sim_date_idx
    ON household_billing_running (sim_date);


-- ----------------------------------------------------------------------------
-- household_billing_daily -- the ISSUED bill. Written by Airflow's
-- daily_billing_report DAG in Phase 5, not by any streaming job.
--
-- Created now, ahead of its writer, because it is the table the whole
-- architecture argument points at, and Job C's docstring has to be able to refer
-- to something real when it explains what it deliberately does not write.
--
-- (version, is_current) IS THE R7 MECHANISM.
-- A restated bill is not an UPDATE. It is a new row at version N+1, followed by
-- one transaction that flips is_current. Version N remains, so "what did we
-- invoice on the 14th, and what did we correct it to" is answerable from the
-- table itself rather than from a log. That auditability is the concrete payoff
-- of the Kappa decision (§2.2c): a corrected tariff is a new record on a
-- compacted topic plus a replay into a new version, with one definition of what a
-- bill is throughout.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS household_billing_daily (
    household_id     TEXT           NOT NULL,
    sim_date         DATE           NOT NULL,

    -- Monotonic per (household_id, sim_date). 1 is the original issue.
    version          INTEGER        NOT NULL,

    consumption_kwh  NUMERIC(18, 6) NOT NULL,
    solar_kwh        NUMERIC(18, 6) NOT NULL,
    net_grid_kwh     NUMERIC(18, 6) NOT NULL,

    -- The rate as PUBLISHED BY THE TARIFF FEED (§6.2), retained for audit: it is
    -- the input the bill was computed from, so a restatement can be explained by
    -- diffing it against the superseded version.
    --
    -- IMPORTANT, AND A GENUINE WART IN THE SPEC: this is NOT the rate the bill was
    -- charged at. Under the block-rate structure (§7, "block/tiered rate by
    -- billing_tier") consumption is split across blocks cheapest-first and
    -- billing_tier caps the ladder, so a TIER_3 household drawing 75 kWh pays
    -- 22.50 on the first 30, 32.50 on the next 30 and 45.00 on the last 15 -- an
    -- effective 31.00 LKR/kWh against a published tariff_rate of 45.00. The two
    -- coincide only below one block's width. tariff_rate is therefore the
    -- household's MARGINAL (top-block) rate; effective_rate is what it actually
    -- paid, and both are stored so a customer reading the bill can reconcile them.
    tariff_rate      NUMERIC(10, 4),
    billing_tier     TEXT,
    subsidy_flag     BOOLEAN        NOT NULL DEFAULT FALSE,

    gross_cost       NUMERIC(14, 2),
    subsidy_amount   NUMERIC(14, 2),

    -- Credit for energy exported to the grid, at a configurable fraction of the
    -- import rate (a feed-in tariff). Stored positive; subtracted in final_bill.
    export_credit    NUMERIC(14, 2),

    -- gross_cost - subsidy_amount - export_credit. NOT constrained >= 0: a net
    -- exporter's bill is legitimately negative and that is a credit the utility
    -- owes. A non-negative constraint here would silently confiscate it.
    final_bill       NUMERIC(14, 2),

    -- gross_cost / billable kWh. The number that actually explains the invoice;
    -- see the tariff_rate comment above.
    effective_rate   NUMERIC(10, 4),

    -- Exactly one version per (household, sim_date) may be current. Enforced by
    -- the partial unique index below rather than by convention, because "which
    -- bill is in force" is precisely the question an auditor asks.
    is_current       BOOLEAN        NOT NULL DEFAULT TRUE,

    generated_at     TIMESTAMPTZ    NOT NULL DEFAULT now(),

    PRIMARY KEY (household_id, sim_date, version)
);

-- The R7 invariant, in the schema rather than in the DAG. A restatement that
-- forgot to clear the previous is_current fails loudly here instead of leaving
-- two live bills for one day -- the failure mode that would actually reach a
-- customer.
CREATE UNIQUE INDEX IF NOT EXISTS household_billing_daily_current_idx
    ON household_billing_daily (household_id, sim_date)
    WHERE is_current;

CREATE INDEX IF NOT EXISTS household_billing_daily_sim_date_idx
    ON household_billing_daily (sim_date)
    WHERE is_current;


-- ----------------------------------------------------------------------------
-- pipeline_run_audit -- one row per job run or per sealed sim-day (§6.5).
--
-- Written by Airflow (Phase 5). Exists because "the pipeline must be diagnosable
-- when it breaks, not just when it works" (§1) needs somewhere durable to record
-- in/out/rejected counts. Logs answer that too, but they roll over and are not
-- joinable against the data they describe; this table is queryable next to the
-- bills whose provenance it explains.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_run_audit (
    run_id            TEXT           NOT NULL,
    job_name          TEXT           NOT NULL,

    -- ingest | process | store | serve | orchestrate -- the same vocabulary as
    -- the structured logs (§8), so a log line and an audit row can be correlated
    -- without a translation table.
    stage             TEXT           NOT NULL,

    sim_date          DATE,

    records_in        BIGINT         NOT NULL DEFAULT 0,
    records_out       BIGINT         NOT NULL DEFAULT 0,
    records_rejected  BIGINT         NOT NULL DEFAULT 0,

    started_at        TIMESTAMPTZ    NOT NULL,
    finished_at       TIMESTAMPTZ,
    status            TEXT           NOT NULL,
    notes             TEXT,

    PRIMARY KEY (run_id, job_name, stage)
);

CREATE INDEX IF NOT EXISTS pipeline_run_audit_sim_date_idx
    ON pipeline_run_audit (sim_date, started_at DESC);


-- ============================================================================
-- Staging tables for the streaming upserts.
--
-- WHY THESE EXIST: the Spark image carries the Postgres JDBC driver, and plain
-- df.write.jdbc() can only append or overwrite -- it has no ON CONFLICT. So each
-- micro-batch overwrites a staging table and then runs ONE
-- INSERT ... SELECT ... ON CONFLICT DO UPDATE against the target inside a single
-- transaction (see processing/sinks/postgres_sink.py). The target is therefore
-- never partially updated from one batch.
--
-- Created here, with LIKE, so they exist with the right column TYPES before the
-- first write. This is not cosmetic: the writer passes truncate=true precisely so
-- that Spark does not DROP and recreate them, which would replace NUMERIC(14,2)
-- with float8 and silently end the exactness guarantee this whole file rests on.
--
-- DELIBERATELY NO PRIMARY KEY (LIKE without INCLUDING ALL). A PK on staging would
-- turn a duplicate key inside a single batch into a constraint violation that
-- crashes the query, instead of letting the MERGE's ON CONFLICT resolve it.
-- Spark's aggregate output is unique per key within a batch -- but encoding that
-- assumption as a constraint converts a benign assumption into an outage.
--
-- Names are deterministic (stg_<job>_<target>), not random. A crashed batch must
-- overwrite the SAME staging table when Spark replays it; a random suffix would
-- leak one table per restart and break that.
-- ============================================================================
CREATE TABLE IF NOT EXISTS stg_job_b_zone_aggregates_zone_load_1m
    (LIKE zone_load_1m);

CREATE TABLE IF NOT EXISTS stg_job_c_household_billing_household_billing_running
    (LIKE household_billing_running);
