-- =============================================================================
-- chora-observability : 0014_external_egress_policy.up.sql
--
-- Jira            : CHO-2113 (P5 Far Sight — gateway grounded-search surface)
-- ADR             : ADR-231 D6 (external-egress governance, gateway-enforced) ·
--                   ADR-220 D4 (external_egress default OFF for franchise) ·
--                   ADR-163 (gateway is the un-bypassable chokepoint)
-- Domain          : Observability (supporting)  /  Database : chora_observability
-- Date            : 2026-07-11
-- Companion       : services/chora-model-gateway (domain.ExternalEgressGate;
--                   internal/adapter/pg egress-gate + daily-counter reads/writes)
--
-- Purpose:
--   The three tables the chora-model-gateway GroundedSearch chain reads to
--   FAIL-CLOSED gate the ONE controlled web-egress surface (ADR-231 D6),
--   sourced from chora_observability "beside the budget" (per_tenant_llm_budget,
--   migration 0008):
--
--     1. platform_egress_killswitch  — a single-row O+ kill-switch that
--        disables ALL grounded egress platform-wide (ADR-231 D6). Platform-
--        level (NO RLS); flipped by O+ operators.
--     2. external_egress_policy       — per-tenant entitlement + daily ceiling.
--        DEFAULT is deny: a tenant with NO row is treated as egress OFF
--        (ADR-220 D4 franchise default). RLS per-tenant like the budget table.
--     3. grounded_search_daily_usage  — per-(tenant, day) grounded-call counter
--        the gate checks against the tenant's daily ceiling and RecordEgress
--        increments post-success. RLS per-tenant.
--
--   The gateway does, per grounded call (SET LOCAL chora.tenant_id = $1):
--     SELECT engaged FROM platform_egress_killswitch;               -- kill-switch
--     SELECT egress_enabled, daily_call_ceiling                     -- entitlement
--       FROM external_egress_policy WHERE tenant_id = $1;
--     SELECT call_count FROM grounded_search_daily_usage            -- ceiling
--       WHERE tenant_id = $1 AND usage_date = current_date;
--     -- (post-success) INSERT ... ON CONFLICT DO UPDATE call_count + 1
--
-- Deploy note:
--   ADDITIVE + idempotent (CREATE TABLE IF NOT EXISTS; guarded policy/seed).
--   HELD until owner applies (ADR-231 Phase 4 activation) — until applied the
--   gate reads fail (missing table) ⇒ the gateway fails CLOSED (all grounded
--   egress denied), which is the safe default while the Seekers are DARK.
--   HARD RULE: cross-database queries forbidden; all objects local to
--   chora_observability. GRANTs to the per-DB roles + delegated to 9999.
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1. platform_egress_killswitch — single-row platform-wide O+ kill-switch.
--    The `singleton` CHECK guarantees at most one row. Platform-level: NO RLS
--    (not tenant-scoped). Seeded engaged=FALSE (grounded egress permitted,
--    subject to the per-tenant entitlement).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS platform_egress_killswitch (
    singleton   BOOLEAN     PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    engaged     BOOLEAN     NOT NULL DEFAULT FALSE,
    reason      TEXT        NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO platform_egress_killswitch (singleton, engaged)
VALUES (TRUE, FALSE)
ON CONFLICT (singleton) DO NOTHING;

CREATE TRIGGER trg_platform_egress_killswitch_updated_at
    BEFORE UPDATE ON platform_egress_killswitch
    FOR EACH ROW EXECUTE FUNCTION observability_set_updated_at();

-- -----------------------------------------------------------------------------
-- 2. external_egress_policy — per-tenant external_egress entitlement + daily
--    grounded-call ceiling. NO row ⇒ egress OFF (ADR-220 D4 franchise default).
--    daily_call_ceiling is the conservative, per-tenant-tunable cap (decision 6).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS external_egress_policy (
    tenant_id           UUID        PRIMARY KEY,
    egress_enabled      BOOLEAN     NOT NULL DEFAULT FALSE,
    -- Conservative default daily grounded-call ceiling per tenant (ADR-231
    -- decision 6). Tunable per tenant; the priciest action in the catalogue.
    daily_call_ceiling  INTEGER     NOT NULL DEFAULT 50 CHECK (daily_call_ceiling >= 0),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TRIGGER trg_external_egress_policy_updated_at
    BEFORE UPDATE ON external_egress_policy
    FOR EACH ROW EXECUTE FUNCTION observability_set_updated_at();

-- -----------------------------------------------------------------------------
-- 3. grounded_search_daily_usage — per-(tenant, day) grounded-call counter.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS grounded_search_daily_usage (
    tenant_id   UUID        NOT NULL,
    usage_date  DATE        NOT NULL,
    call_count  INTEGER     NOT NULL DEFAULT 0 CHECK (call_count >= 0),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, usage_date)
);

CREATE TRIGGER trg_grounded_search_daily_usage_updated_at
    BEFORE UPDATE ON grounded_search_daily_usage
    FOR EACH ROW EXECUTE FUNCTION observability_set_updated_at();

-- -----------------------------------------------------------------------------
-- RLS — tenant-scoped tables mirror per_tenant_llm_budget (0008). The gateway
-- SETs `chora.tenant_id` via SET LOCAL at the start of each transaction. The
-- kill-switch table is platform-level (no RLS). The migrate role owns the
-- tables (bypasses RLS); the app_rw / app_ro roles do NOT bypass.
-- -----------------------------------------------------------------------------
ALTER TABLE external_egress_policy ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON external_egress_policy
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

ALTER TABLE grounded_search_daily_usage ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON grounded_search_daily_usage
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- -----------------------------------------------------------------------------
-- Grants — the per-DB roles (chora_observability_app_rw / _app_ro). 9999 runs
-- last in lex order and re-grants over ALL tables; these explicit grants are
-- belt-and-suspenders for out-of-order application.
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_observability_app_rw') THEN
        EXECUTE 'GRANT SELECT, INSERT, UPDATE ON platform_egress_killswitch   TO chora_observability_app_rw';
        EXECUTE 'GRANT SELECT, INSERT, UPDATE ON external_egress_policy        TO chora_observability_app_rw';
        EXECUTE 'GRANT SELECT, INSERT, UPDATE ON grounded_search_daily_usage   TO chora_observability_app_rw';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_observability_app_ro') THEN
        EXECUTE 'GRANT SELECT ON platform_egress_killswitch   TO chora_observability_app_ro';
        EXECUTE 'GRANT SELECT ON external_egress_policy        TO chora_observability_app_ro';
        EXECUTE 'GRANT SELECT ON grounded_search_daily_usage   TO chora_observability_app_ro';
    END IF;
END$$;

COMMIT;

-- VERIFICATION (run after apply):
--   SELECT engaged FROM platform_egress_killswitch;                 -- => f
--   -- enable chora-master (adults) egress, ADR-220 D4:
--   -- INSERT INTO external_egress_policy (tenant_id, egress_enabled)
--   --   VALUES ('<chora-master-tenant-uuid>', TRUE);
-- =============================================================================
