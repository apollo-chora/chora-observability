-- =============================================================================
-- chora-observability : 0002_schema_lockdown.sql
--
-- Domain        : Observability (supporting/platform)
-- Database      : chora_observability
-- Author        : agent A-Platform-Obs (Phyllis Stage S1.2)
-- Date          : 2026-05-09
-- Architecture  : Architecture Review locked 2026-05-07
--                  Tier 3 D10 (cost tracking, atomic ledger, 3-level budgets)
--                  Tier 3 D12 (OTLP-everywhere; trace correlation)
--                  Tier 5 D18 (4 lifecycle stages: ci/pre_merge/runtime/...)
--                  ADR-141    (canonical IMDA dimension labels)
-- Skills        : ai-cost-tracking + ai-observability-cloud-trace +
--                  imda-governance-4-dimensions
--
-- Purpose: lock-down the TokenUsageLedger + AgentDecisionLog schemas to the
-- production-ready endgoal columns. Adds:
--
--   token_usage_ledger:
--     + agent_id, model_version, cached_tokens, currency_code,
--       pricing_config_version, request_id, traceparent (full W3C string)
--
--   agent_decision_log:
--     + agent_id, run_id, model_id, model_version, prompt_id, input_hash,
--       output_hash, guardrail_verdict, autonomy_level (enum),
--       chora_imda_dimension (enum, ADR-141), imda_lifecycle_stage (enum,
--       Tier 5 D18), accountability_owner, decided_at
--
-- Plus 3-level budget tables (per ai-cost-tracking skill):
--   budget_per_tenant  — hard cap with daily/monthly windows
--   budget_per_user    — fairness slice riding tenant cap
--   budget_per_agent   — kill-switch threshold
--
-- Plus pricing_config_version table (ledger row references this).
--
-- IDEMPOTENT: every ALTER uses IF NOT EXISTS / IF EXISTS where supported.
-- Safe to re-run.
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- ENUMs (additive — new types only; deprecated v1 IMDA labels accepted on
-- input but normalised at the application layer per ADR-141)
-- -----------------------------------------------------------------------------

DO $$ BEGIN
    CREATE TYPE imda_dimension AS ENUM (
        'accountability',
        'transparency',
        'safety_and_robustness',
        'fairness_and_human_oversight'
    );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE imda_lifecycle_stage AS ENUM (
        'ci_pre_merge',
        'pre_deploy',
        'runtime',
        'post_deploy'
    );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- HOOTL = Human-out-of-the-loop, HOTL = Human-on-the-loop,
-- HITL_LEVEL_0..2 = Human-in-the-loop graded.
-- Level 3 (full HITL veto) is PROHIBITED per the imda-governance-4-dimensions
-- skill — agents that require Level 3 have not passed pre-deploy gating.
DO $$ BEGIN
    CREATE TYPE autonomy_level AS ENUM (
        'hootl',
        'hotl',
        'hitl_level_0',
        'hitl_level_1',
        'hitl_level_2'
    );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- -----------------------------------------------------------------------------
-- pricing_config_version — every ledger row references a versioned YAML
-- (data, not code) — see services/chora-observability/config/pricing.yaml
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pricing_config_version (
    pricing_version    VARCHAR(64)  PRIMARY KEY,           -- e.g. "2026.05.09-1"
    effective_from     TIMESTAMPTZ  NOT NULL,
    effective_until    TIMESTAMPTZ  NULL,
    config_yaml_sha256 CHAR(64)     NOT NULL,              -- integrity check
    description        TEXT         NOT NULL DEFAULT '',
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pricing_version_effective
    ON pricing_config_version (effective_from, effective_until);

-- pricing_config_version is platform-global (all tenants share the same
-- pricing) — RLS deliberately disabled.

-- Seed initial version so 0001 ledger rows can reference SOMETHING.
INSERT INTO pricing_config_version (pricing_version, effective_from,
                                     config_yaml_sha256, description)
VALUES ('2026.05.09-1', now(),
        '0000000000000000000000000000000000000000000000000000000000000000',
        'Phyllis MVP seed — replaced by checksum at first config deploy')
ON CONFLICT (pricing_version) DO NOTHING;

-- -----------------------------------------------------------------------------
-- token_usage_ledger — additive columns per ai-cost-tracking schema lockdown
-- -----------------------------------------------------------------------------

ALTER TABLE token_usage_ledger
    ADD COLUMN IF NOT EXISTS agent_id              VARCHAR(64)  NULL,
    ADD COLUMN IF NOT EXISTS model_version         VARCHAR(64)  NULL,
    ADD COLUMN IF NOT EXISTS cached_tokens         INTEGER      NOT NULL DEFAULT 0
        CHECK (cached_tokens >= 0),
    ADD COLUMN IF NOT EXISTS currency_code         CHAR(3)      NOT NULL DEFAULT 'USD',
    ADD COLUMN IF NOT EXISTS pricing_config_version VARCHAR(64) NULL,
    ADD COLUMN IF NOT EXISTS request_id            VARCHAR(128) NULL,
    ADD COLUMN IF NOT EXISTS traceparent           VARCHAR(64)  NULL,
    ADD COLUMN IF NOT EXISTS feature               VARCHAR(64)  NULL,
    ADD COLUMN IF NOT EXISTS domain                VARCHAR(32)  NULL,
    ADD COLUMN IF NOT EXISTS cost_center           VARCHAR(16)  NULL DEFAULT 'prod'
        CHECK (cost_center IN ('prod', 'eval', 'ops')),
    ADD COLUMN IF NOT EXISTS is_eval_run           BOOLEAN      NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS source                VARCHAR(16)  NOT NULL DEFAULT 'internal'
        CHECK (source IN ('internal', 'a2a-external')),
    ADD COLUMN IF NOT EXISTS contract_id           VARCHAR(64)  NULL,
    ADD COLUMN IF NOT EXISTS adapter_version       VARCHAR(64)  NULL,
    ADD COLUMN IF NOT EXISTS cache_hit             BOOLEAN      NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS latency_ms            INTEGER      NOT NULL DEFAULT 0
        CHECK (latency_ms >= 0);

-- pricing_config_version FK (lazy validation — pre-existing rows may have NULL)
DO $$ BEGIN
    ALTER TABLE token_usage_ledger
        ADD CONSTRAINT fk_token_usage_pricing_version
        FOREIGN KEY (pricing_config_version)
        REFERENCES pricing_config_version (pricing_version)
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED;
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_token_ledger_agent
    ON token_usage_ledger (agent_id) WHERE agent_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_token_ledger_traceparent
    ON token_usage_ledger (traceparent) WHERE traceparent IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_token_ledger_pricing_version
    ON token_usage_ledger (pricing_config_version);
CREATE INDEX IF NOT EXISTS idx_token_ledger_eval_run
    ON token_usage_ledger (is_eval_run, recorded_at DESC) WHERE is_eval_run = TRUE;

-- The append-only trigger from 0001 already covers UPDATE + DELETE.

-- -----------------------------------------------------------------------------
-- agent_decision_log — additive columns per imda-governance-4-dimensions
-- -----------------------------------------------------------------------------

ALTER TABLE agent_decision_log
    ADD COLUMN IF NOT EXISTS agent_id               VARCHAR(64)           NULL,
    ADD COLUMN IF NOT EXISTS run_id                 UUID                  NULL,
    ADD COLUMN IF NOT EXISTS model_id               VARCHAR(64)           NULL,
    ADD COLUMN IF NOT EXISTS model_version          VARCHAR(64)           NULL,
    ADD COLUMN IF NOT EXISTS prompt_id              UUID                  NULL,
    ADD COLUMN IF NOT EXISTS guardrail_verdict      VARCHAR(16)           NULL
        CHECK (guardrail_verdict IS NULL OR
               guardrail_verdict IN ('pass', 'block', 'redact')),
    ADD COLUMN IF NOT EXISTS autonomy_level         autonomy_level        NULL,
    ADD COLUMN IF NOT EXISTS chora_imda_dimension   imda_dimension        NULL,
    ADD COLUMN IF NOT EXISTS imda_lifecycle_stage   imda_lifecycle_stage  NULL,
    ADD COLUMN IF NOT EXISTS accountability_owner   VARCHAR(128)          NULL,
    ADD COLUMN IF NOT EXISTS decided_at             TIMESTAMPTZ           NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_adl_agent_id
    ON agent_decision_log (agent_id) WHERE agent_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_adl_run_id
    ON agent_decision_log (run_id) WHERE run_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_adl_imda_dimension
    ON agent_decision_log (chora_imda_dimension) WHERE chora_imda_dimension IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_adl_lifecycle_stage
    ON agent_decision_log (imda_lifecycle_stage) WHERE imda_lifecycle_stage IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_adl_autonomy_level
    ON agent_decision_log (autonomy_level) WHERE autonomy_level IS NOT NULL;

-- -----------------------------------------------------------------------------
-- 3-level budget tables — per ai-cost-tracking skill 3-level enforcement
--
-- Per-tenant: HARD CAP enforced at Router (sub-10ms path).
-- Per-user:   FAIRNESS slice within tenant cap.
-- Per-agent:  KILL-SWITCH at 100x baseline (circuit breaker).
--
-- Time windows: daily + monthly. Reset job runs at window boundary.
-- Hot enforcement is at the Router, NOT the analytics path.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS budget_per_tenant (
    budget_id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           UUID         NOT NULL,
    window_kind         VARCHAR(8)   NOT NULL CHECK (window_kind IN ('daily', 'monthly')),
    window_start        TIMESTAMPTZ  NOT NULL,
    window_end          TIMESTAMPTZ  NOT NULL CHECK (window_end > window_start),
    cap_micros          BIGINT       NOT NULL CHECK (cap_micros > 0),
    spent_micros        BIGINT       NOT NULL DEFAULT 0 CHECK (spent_micros >= 0),
    currency_code       CHAR(3)      NOT NULL DEFAULT 'USD',
    enforcement         VARCHAR(8)   NOT NULL DEFAULT 'hard'
        CHECK (enforcement IN ('hard', 'soft')),
    threshold_crossings JSONB        NOT NULL DEFAULT '[]'::jsonb,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, window_kind, window_start)
);

CREATE INDEX IF NOT EXISTS idx_budget_tenant_window
    ON budget_per_tenant (tenant_id, window_kind, window_end DESC);

ALTER TABLE budget_per_tenant ENABLE ROW LEVEL SECURITY;
DO $$ BEGIN
    CREATE POLICY tenant_isolation ON budget_per_tenant
        FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS budget_per_user (
    budget_id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           UUID         NOT NULL,                   -- RLS scope
    gcid                UUID         NOT NULL,                   -- per-user
    window_kind         VARCHAR(8)   NOT NULL CHECK (window_kind IN ('daily', 'monthly')),
    window_start        TIMESTAMPTZ  NOT NULL,
    window_end          TIMESTAMPTZ  NOT NULL CHECK (window_end > window_start),
    cap_micros          BIGINT       NOT NULL CHECK (cap_micros > 0),
    spent_micros        BIGINT       NOT NULL DEFAULT 0 CHECK (spent_micros >= 0),
    currency_code       CHAR(3)      NOT NULL DEFAULT 'USD',
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, gcid, window_kind, window_start)
);

CREATE INDEX IF NOT EXISTS idx_budget_user
    ON budget_per_user (tenant_id, gcid, window_end DESC);

ALTER TABLE budget_per_user ENABLE ROW LEVEL SECURITY;
DO $$ BEGIN
    CREATE POLICY tenant_isolation ON budget_per_user
        FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS budget_per_agent (
    budget_id            UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id            UUID         NULL,                         -- NULL = platform-wide agent baseline
    agent_id             VARCHAR(64)  NOT NULL,
    window_kind          VARCHAR(8)   NOT NULL CHECK (window_kind IN ('daily', 'monthly')),
    window_start         TIMESTAMPTZ  NOT NULL,
    window_end           TIMESTAMPTZ  NOT NULL CHECK (window_end > window_start),
    baseline_micros      BIGINT       NOT NULL CHECK (baseline_micros >= 0),
    multiplier_kill      INTEGER      NOT NULL DEFAULT 100 CHECK (multiplier_kill > 0),
    spent_micros         BIGINT       NOT NULL DEFAULT 0 CHECK (spent_micros >= 0),
    kill_switch_active   BOOLEAN      NOT NULL DEFAULT FALSE,
    last_kill_switched_at TIMESTAMPTZ NULL,
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, agent_id, window_kind, window_start)
);

CREATE INDEX IF NOT EXISTS idx_budget_agent
    ON budget_per_agent (agent_id, window_end DESC);
CREATE INDEX IF NOT EXISTS idx_budget_agent_kill
    ON budget_per_agent (kill_switch_active) WHERE kill_switch_active = TRUE;

ALTER TABLE budget_per_agent ENABLE ROW LEVEL SECURITY;
-- Per-agent budgets may be tenant-scoped (RLS) or platform-wide (NULL tenant)
-- — when tenant_id IS NULL, only platform-Auditor reads it (via SECURITY DEFINER
-- view in a follow-up migration). For now, RLS allows NULL OR matching tenant.
DO $$ BEGIN
    CREATE POLICY tenant_isolation_or_platform ON budget_per_agent
        FOR ALL USING (
            tenant_id IS NULL OR
            tenant_id = current_setting('chora.tenant_id', true)::uuid
        );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- -----------------------------------------------------------------------------
-- updated_at triggers (reuse 0001's observability_set_updated_at function)
-- -----------------------------------------------------------------------------

DROP TRIGGER IF EXISTS trg_budget_per_tenant_updated_at ON budget_per_tenant;
CREATE TRIGGER trg_budget_per_tenant_updated_at
    BEFORE UPDATE ON budget_per_tenant
    FOR EACH ROW EXECUTE FUNCTION observability_set_updated_at();

DROP TRIGGER IF EXISTS trg_budget_per_user_updated_at ON budget_per_user;
CREATE TRIGGER trg_budget_per_user_updated_at
    BEFORE UPDATE ON budget_per_user
    FOR EACH ROW EXECUTE FUNCTION observability_set_updated_at();

DROP TRIGGER IF EXISTS trg_budget_per_agent_updated_at ON budget_per_agent;
CREATE TRIGGER trg_budget_per_agent_updated_at
    BEFORE UPDATE ON budget_per_agent
    FOR EACH ROW EXECUTE FUNCTION observability_set_updated_at();

-- -----------------------------------------------------------------------------
-- Append-only invariant verification — re-affirm the 0001 triggers protect
-- the schema-locked tables. The triggers don't need replay (already there
-- from 0001), but documented here for completeness:
--
--   trg_token_ledger_no_update + trg_token_ledger_no_delete  (0001:67-73)
--   trg_adl_no_update          + trg_adl_no_delete           (0001:113-119)
-- -----------------------------------------------------------------------------

COMMIT;
