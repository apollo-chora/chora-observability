-- =============================================================================
-- chora-observability : 0001_initial.sql
--
-- Domain        : Observability (supporting/platform)
-- Database      : chora_observability
-- Author        : agent-a5e52e89b73ede1d2 (db-migrations-11-services)
-- Date          : 2026-05-08
-- Architecture  : Architecture Review locked 2026-05-07 (Tier 3 D12) — ai-cost-tracking
--
-- Aggregates owned by this database:
--   - token_usage_ledger (APPEND-ONLY canonical billing-grade LLM cost ledger)
--   - agent_decision_log (APPEND-ONLY routing/escalate/refuse/respond decisions)
--   - budgets (per-tenant SGD cap + threshold crossings)
--
-- Cost stored as int64 micros (1e-6 USD/SGD) — never float64. Trace context
-- (W3C traceparent / tracestate) recorded alongside every entry for
-- distributed-tracing correlation.
-- =============================================================================

BEGIN;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- -----------------------------------------------------------------------------
-- ENUMs
-- -----------------------------------------------------------------------------
CREATE TYPE decision_type AS ENUM ('route', 'escalate', 'refuse', 'respond');
CREATE TYPE risk_tier     AS ENUM ('low', 'medium', 'high', 'critical');

-- -----------------------------------------------------------------------------
-- token_usage_ledger — APPEND-ONLY canonical LLM cost record
--
-- Each entry = one LLM call (or batch). Stored as int64 micros to avoid
-- float drift. Outbox publishes chora.ai_kernel.token-usage.recorded.v1 on
-- each insert.
-- -----------------------------------------------------------------------------
CREATE TABLE token_usage_ledger (
    ledger_id            UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id            UUID         NOT NULL,
    gcid                 UUID         NOT NULL,                     -- billing subject
    agid                 UUID         NULL,                         -- empty = user-direct
    model_id             VARCHAR(64)  NOT NULL,
    prompt_tokens        INTEGER      NOT NULL CHECK (prompt_tokens >= 0),
    completion_tokens    INTEGER      NOT NULL CHECK (completion_tokens >= 0),
    cost_usd_micros      BIGINT       NOT NULL CHECK (cost_usd_micros >= 0),
    cost_sgd_micros      BIGINT       NOT NULL DEFAULT 0 CHECK (cost_sgd_micros >= 0),
    fx_rate              REAL         NOT NULL DEFAULT 1.35,         -- USD→SGD snapshot
    trace_id             CHAR(32)     NOT NULL,                     -- W3C 16-byte hex
    span_id              CHAR(16)     NOT NULL,                     -- W3C 8-byte hex
    recorded_at          TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_token_ledger_tenant     ON token_usage_ledger (tenant_id, recorded_at DESC);
CREATE INDEX idx_token_ledger_gcid       ON token_usage_ledger (gcid);
CREATE INDEX idx_token_ledger_model      ON token_usage_ledger (model_id);
CREATE INDEX idx_token_ledger_trace      ON token_usage_ledger (trace_id);
CREATE INDEX idx_token_ledger_recorded   ON token_usage_ledger (recorded_at DESC);

CREATE OR REPLACE FUNCTION enforce_token_ledger_append_only()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'token_usage_ledger is append-only (billing invariant): % rejected', TG_OP;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_token_ledger_no_update
    BEFORE UPDATE ON token_usage_ledger
    FOR EACH ROW EXECUTE FUNCTION enforce_token_ledger_append_only();

CREATE TRIGGER trg_token_ledger_no_delete
    BEFORE DELETE ON token_usage_ledger
    FOR EACH ROW EXECUTE FUNCTION enforce_token_ledger_append_only();

ALTER TABLE token_usage_ledger ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON token_usage_ledger
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- -----------------------------------------------------------------------------
-- agent_decision_log — APPEND-ONLY routing/escalate/refuse/respond decisions
--
-- input_hash + output_hash are SHA-256 over canonical-form payloads — this lets
-- us correlate decisions without persisting full prompt content (PII sensitive).
-- -----------------------------------------------------------------------------
CREATE TABLE agent_decision_log (
    log_id               UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id            UUID            NOT NULL,
    agid                 UUID            NOT NULL,
    decision_type        decision_type   NOT NULL,
    reasoning_summary    TEXT            NOT NULL DEFAULT '' CHECK (length(reasoning_summary) <= 2048),
    risk_tier            risk_tier       NOT NULL,
    correlation_id       VARCHAR(128)    NOT NULL,
    input_hash           CHAR(64)        NOT NULL,
    output_hash          CHAR(64)        NOT NULL,
    latency_ms           INTEGER         NOT NULL DEFAULT 0 CHECK (latency_ms >= 0),
    traceparent          VARCHAR(64)     NULL,
    recorded_at          TIMESTAMPTZ     NOT NULL DEFAULT now()
);

CREATE INDEX idx_adl_tenant     ON agent_decision_log (tenant_id, recorded_at DESC);
CREATE INDEX idx_adl_agid       ON agent_decision_log (agid);
CREATE INDEX idx_adl_correlation ON agent_decision_log (correlation_id);
CREATE INDEX idx_adl_decision_type ON agent_decision_log (decision_type);
CREATE INDEX idx_adl_risk_tier  ON agent_decision_log (risk_tier);

CREATE OR REPLACE FUNCTION enforce_adl_append_only()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'agent_decision_log is append-only: % rejected', TG_OP;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_adl_no_update
    BEFORE UPDATE ON agent_decision_log
    FOR EACH ROW EXECUTE FUNCTION enforce_adl_append_only();

CREATE TRIGGER trg_adl_no_delete
    BEFORE DELETE ON agent_decision_log
    FOR EACH ROW EXECUTE FUNCTION enforce_adl_append_only();

ALTER TABLE agent_decision_log ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON agent_decision_log
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- -----------------------------------------------------------------------------
-- budgets — per-tenant SGD cap + threshold crossings (3-level: warn / soft / hard)
-- -----------------------------------------------------------------------------
CREATE TABLE budgets (
    budget_id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id              UUID         NOT NULL,
    period                 VARCHAR(16)  NOT NULL,                  -- e.g. '2026-05', '2026-Q2'
    period_start           TIMESTAMPTZ  NOT NULL,
    period_end             TIMESTAMPTZ  NOT NULL,
    amount_sgd_cap_micros  BIGINT       NOT NULL CHECK (amount_sgd_cap_micros > 0),
    threshold_crossings    JSONB        NOT NULL DEFAULT '{}'::jsonb,  -- {warn: at, soft: at, hard: at}
    spent_sgd_micros       BIGINT       NOT NULL DEFAULT 0 CHECK (spent_sgd_micros >= 0),
    created_at             TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, period),
    CHECK (period_end > period_start)
);

CREATE INDEX idx_budgets_tenant ON budgets (tenant_id);
CREATE INDEX idx_budgets_period ON budgets (period);

CREATE OR REPLACE FUNCTION observability_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_budgets_updated_at
    BEFORE UPDATE ON budgets
    FOR EACH ROW EXECUTE FUNCTION observability_set_updated_at();

ALTER TABLE budgets ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON budgets
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- -----------------------------------------------------------------------------
-- traces_correlation — minimal trace_id ↔ subject correlation index
--
-- We do NOT mirror full trace data here (the trace store is canonical).
-- This index lets us correlate a trace_id back to its tenant/gcid/agid for
-- replay and incident-response queries without round-tripping to the trace store.
-- -----------------------------------------------------------------------------
CREATE TABLE traces_correlation (
    trace_id            CHAR(32)     PRIMARY KEY,
    tenant_id           UUID         NOT NULL,
    gcid                UUID         NULL,
    agid                UUID         NULL,
    root_service        VARCHAR(64)  NOT NULL,
    started_at          TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_traces_correlation_tenant ON traces_correlation (tenant_id, started_at DESC);
CREATE INDEX idx_traces_correlation_gcid   ON traces_correlation (gcid) WHERE gcid IS NOT NULL;
CREATE INDEX idx_traces_correlation_agid   ON traces_correlation (agid) WHERE agid IS NOT NULL;

ALTER TABLE traces_correlation ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON traces_correlation
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

COMMIT;
