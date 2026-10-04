-- =============================================================================
-- chora-observability : 0008_per_tenant_llm_budget.up.sql
--
-- Domain        : Observability (supporting/platform)
-- Database      : chora_observability
-- Date          : 2026-05-24
-- Architecture  : ADR-163 — Model Gateway on GKE asia-southeast1 (PROPOSED
--                 2026-05-24). Phase 0 Sub-phase 0.2 of the M15 plan.
-- Companion     : chora-contracts/proto/services/model_gateway_service.proto
--
-- Purpose:
--   Per-tenant LLM budget cap + enforcement policy consumed by the
--   chora-model-gateway. The gateway's domain/service.go Invoke flow does
--   (per .claude/skills/multi-tenant-rls/SKILL.md "Per-tenant LLM budget"
--   subsection added 2026-05-24):
--
--     BEGIN;
--       SET LOCAL chora.tenant_id = $1;
--       SELECT ... FROM per_tenant_llm_budget
--         WHERE tenant_id = $1
--           AND budget_period_start <= now()
--           AND budget_period_end   >  now()
--         FOR UPDATE;                            -- pessimistic per-row lock
--       -- gateway decides (block / downgrade / alert) based on .policy
--       UPDATE per_tenant_llm_budget
--         SET spent_usd_micros = spent_usd_micros + $delta
--         WHERE budget_id = $row_id;
--     COMMIT;
--
--   The gateway holds the lock only briefly (microseconds; row-level), so
--   contention is bounded even when a single tenant fires concurrent LLM
--   calls. Sub-tenant fan-out (e.g. one tenant with 10k learners) does NOT
--   need separate rows — the (tenant_id, window) row is per-tenant; the
--   gateway aggregates cost across all of the tenant's traffic against it.
--
-- Why not extend the existing `budgets` table from 0001?
--   `budgets` (0001) is a generic SGD-denominated month/quarter cap. The
--   LLM gateway needs:
--     - USD-denominated (LLM pricing is canonical USD; FX is downstream)
--     - per-call policy enum (block / downgrade / alert) the gateway reads
--     - a downgrade-to model id (the fallback model when policy=downgrade)
--   Keeping a separate table avoids leaking gateway-specific schema into
--   the general budgets surface (which BFF dashboards already read).
--
-- Resilience (per feedback_resilience_priority memory):
--   - Pod-death survival: gateway's BEGIN/COMMIT around the UPDATE means a
--     mid-call pod death rolls back; on retry, the new gateway pod
--     re-checks the row + atomically debits.
--   - Multi-tenant chaos isolation: per-tenant RLS policy means tenant A's
--     overspend cannot starve tenant B's budget enforcement read latency.
--
-- Idempotency:
--   - This is an additive forward migration; the table does NOT yet exist
--     on `chora-cloudsql-platform`. Re-applying is safe (`CREATE TABLE
--     IF NOT EXISTS`); the ENUM type creation is guarded by a `DO` block.
--   - Replacement migrate pod re-runs the entire .up.sql in one txn.
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- ENUM: llm_budget_policy — gateway enforcement decision
--
--   block      — gateway returns FAILED_PRECONDITION + finish_reason
--                FINISH_REASON_BUDGET_BLOCK once spent >= budget. The caller
--                receives an error and MUST surface a user-facing message.
--   downgrade  — gateway transparently re-routes to
--                downgrade_to_logical_model_id (e.g. Flash-Lite) once
--                spent >= budget. Caller receives a successful completion
--                from the cheaper model; InvokeResponse.fallback_chain
--                records the downgrade for audit.
--   alert      — gateway DOES NOT block; emits a high-priority OTLP
--                violation span + Pub/Sub event for the O+ dashboard but
--                lets the call through. For tenants on "alert mode"
--                onboarding or contractually exempt accounts.
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'llm_budget_policy') THEN
        CREATE TYPE llm_budget_policy AS ENUM ('block', 'downgrade', 'alert');
    END IF;
END$$;

-- -----------------------------------------------------------------------------
-- per_tenant_llm_budget — per-tenant LLM spending cap + policy
--
-- One row per (tenant, window). Multiple concurrent windows are allowed
-- (e.g. a monthly cap AND a daily cap can both be active); the gateway
-- evaluates ALL matching rows and applies the strictest decision.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS per_tenant_llm_budget (
    budget_id                       UUID                 PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id                       UUID                 NOT NULL,

    -- Window (half-open: [budget_period_start, budget_period_end)).
    budget_period_start             TIMESTAMPTZ          NOT NULL,
    budget_period_end               TIMESTAMPTZ          NOT NULL,

    -- USD cap + spent. Integer micros (1e-6 USD) per the same convention
    -- as token_usage_ledger.cost_usd_micros.
    budget_usd_micros               BIGINT               NOT NULL CHECK (budget_usd_micros > 0),
    spent_usd_micros                BIGINT               NOT NULL DEFAULT 0 CHECK (spent_usd_micros >= 0),

    -- Gateway enforcement policy.
    policy                          llm_budget_policy    NOT NULL DEFAULT 'block',

    -- For policy='downgrade': the fallback model the gateway re-routes to
    -- once spent >= budget. NULL for 'block' or 'alert'. Free-text logical
    -- model id matching InvokeRequest.logical_model_id (e.g. "gemini-2.5-flash-lite").
    downgrade_to_logical_model_id   TEXT                 NULL,

    created_at                      TIMESTAMPTZ          NOT NULL DEFAULT now(),
    updated_at                      TIMESTAMPTZ          NOT NULL DEFAULT now(),

    -- A tenant can have at most one budget row per exact window. Multiple
    -- *different* windows (daily + monthly) are still allowed.
    UNIQUE (tenant_id, budget_period_start, budget_period_end),

    CHECK (budget_period_end > budget_period_start),

    -- Enforce the downgrade->fallback-model invariant: a policy of
    -- 'downgrade' MUST carry a non-null fallback model id; other policies
    -- MAY or MAY NOT (NULL is fine for block + alert).
    CHECK (
        (policy = 'downgrade' AND downgrade_to_logical_model_id IS NOT NULL)
        OR (policy <> 'downgrade')
    )
);

-- Hot path on the gateway: lookup by tenant + active-window check.
CREATE INDEX IF NOT EXISTS idx_per_tenant_llm_budget_tenant_window
    ON per_tenant_llm_budget (tenant_id, budget_period_start DESC, budget_period_end DESC);

-- Audit-side reads for the O+ dashboard (over-spend / near-threshold panels).
CREATE INDEX IF NOT EXISTS idx_per_tenant_llm_budget_spent_ratio
    ON per_tenant_llm_budget (tenant_id, ((spent_usd_micros::float / NULLIF(budget_usd_micros, 0)::float)));

-- Reuse the observability_set_updated_at trigger function from 0001_initial.sql.
CREATE TRIGGER trg_per_tenant_llm_budget_updated_at
    BEFORE UPDATE ON per_tenant_llm_budget
    FOR EACH ROW EXECUTE FUNCTION observability_set_updated_at();

-- -----------------------------------------------------------------------------
-- RLS — same shape as token_usage_ledger / budgets / traces_correlation.
-- Gateway sets `chora.tenant_id` via `SET LOCAL chora.tenant_id = $1;` at
-- the start of every transaction; the policy ensures only matching rows
-- are visible. The migrate role bypasses RLS (table owner); app_rw +
-- app_ro do NOT bypass.
-- -----------------------------------------------------------------------------
ALTER TABLE per_tenant_llm_budget ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON per_tenant_llm_budget
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- -----------------------------------------------------------------------------
-- Grants — app_rw + app_ro per 9999_grant_app_roles.sql convention.
-- 9999 runs last in lex order so it picks this table up automatically;
-- these explicit grants are belt-and-suspenders for environments that
-- apply migrations out-of-order.
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_rw') THEN
        EXECUTE 'GRANT SELECT, INSERT, UPDATE ON per_tenant_llm_budget TO app_rw';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_ro') THEN
        EXECUTE 'GRANT SELECT ON per_tenant_llm_budget TO app_ro';
    END IF;
END$$;

COMMIT;
