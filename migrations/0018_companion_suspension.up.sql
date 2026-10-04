-- =============================================================================
-- chora-observability : 0018_companion_suspension.up.sql
--
-- ADR             : ADR-252 D2 (two tables, mirroring ADR-231), D3 (roles),
--                   D4 (audit the operator write), D6 (fail closed, absent = off);
--                   ADR-254 D7 (the gateway reads these uncached, deny-before-
--                   debit, AHEAD of ManaMetering) and D11 (consumption projects
--                   the audit event; it never reads these tables).
-- Domain          : Observability (supporting)  /  Database : chora_observability
-- Date            : 2026-08-22
-- Companion       : services/chora-model-gateway internal/adapter/pg/pg_suspension.go
--                   (the enforcement read) · services/chora-observability
--                   internal/adapter/pg/companion_suspension_repository.go (the
--                   O+ write path) · chora-gateway /api/v1/admin/companion/suspension
--
-- Purpose:
--   An operator contains the Learning Companion WITHOUT a deploy: platform-wide,
--   per tenant, or per skill (skill_key = the mana action code that names the
--   skill; NULL = every companion turn in that scope). Two tables, exactly the
--   ADR-231 shape, so the RLS question never arises (ADR-252 Q4):
--
--     1. platform_companion_suspension : platform scope. NO tenant column, NO
--        RLS (sibling of platform_egress_killswitch). Engaged / released by
--        PLATFORM_OPERATOR only (ADR-252 D3).
--     2. companion_suspension_policy    : tenant scope. Tenant-scoped, RLS
--        ENFORCING on `chora.tenant_id` (the one canonical GUC in this DB since
--        0015), read with tenant context set, exactly as external_egress_policy.
--        Engaged / released by that tenant's admin or owner, or the operator.
--
--   Evaluation order at the gateway copies Repo.Authorize: read the platform
--   table FIRST without tenant context, then the tenant table under
--   SET LOCAL chora.tenant_id. A turn is denied if EITHER yields an engaged row
--   matching the call (skill_key IS NULL OR skill_key = action_code). Absent
--   rows mean NOT suspended (D6: the safe default of THIS control is "runs").
--
--   Release is a state change on the row (engaged = FALSE + released_by/at),
--   never a delete, so the containment history survives (soft-delete doctrine).
--   `version` is the per-row monotonic counter the consumption projection
--   guards on (ADR-254 D11): engage = 1, release = 2.
--
-- Deploy note:
--   ADDITIVE + idempotent (IF NOT EXISTS; guarded grants). Until applied the
--   gateway's read ERRORS (missing relation) and the decorator DENIES every
--   companion turn (ADR-252 D6: unreadable denies; empty allows): a loud
--   condition, never a silent "nobody is suspended". Apply BEFORE the gateway
--   image that reads it. GRANTs to the per-DB roles + delegated to 9999.
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1. platform_companion_suspension: platform scope, NO RLS.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS platform_companion_suspension (
    id           UUID        PRIMARY KEY,                 -- UUIDv7, minted by the writer
    skill_key    TEXT        NULL,                        -- NULL = ALL companion turns, platform-wide
    engaged      BOOLEAN     NOT NULL,
    reason       TEXT        NOT NULL CHECK (length(btrim(reason)) > 0),
    engaged_by   UUID        NOT NULL,                    -- GCID of the operator
    engaged_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    released_by  UUID        NULL,
    released_at  TIMESTAMPTZ NULL,
    version      BIGINT      NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- A released row names who released it and when; an engaged row has neither.
    CONSTRAINT platform_companion_suspension_release_consistent CHECK (
        (engaged = TRUE  AND released_by IS NULL     AND released_at IS NULL) OR
        (engaged = FALSE AND released_by IS NOT NULL AND released_at IS NOT NULL)
    )
);

COMMENT ON TABLE platform_companion_suspension IS
    'ADR-252 D2: platform-scope Learning Companion containment (skill_key NULL = every '
    'companion turn). No tenant column, no RLS: a platform control, sibling of '
    'platform_egress_killswitch. Read uncached by chora-model-gateway on every '
    'companion turn (ADR-254 D7). Release is a state change, never a delete.';

-- The gateway's hot read: engaged rows for (NULL | this skill).
CREATE INDEX IF NOT EXISTS idx_platform_companion_suspension_engaged
    ON platform_companion_suspension (skill_key)
    WHERE engaged = TRUE;

CREATE TRIGGER trg_platform_companion_suspension_updated_at
    BEFORE UPDATE ON platform_companion_suspension
    FOR EACH ROW EXECUTE FUNCTION observability_set_updated_at();

-- -----------------------------------------------------------------------------
-- 2. companion_suspension_policy: tenant scope, RLS ENFORCING on chora.tenant_id.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS companion_suspension_policy (
    id           UUID        PRIMARY KEY,                 -- UUIDv7, minted by the writer
    tenant_id    UUID        NOT NULL,
    skill_key    TEXT        NULL,                        -- NULL = ALL companion turns in this tenant
    engaged      BOOLEAN     NOT NULL,
    reason       TEXT        NOT NULL CHECK (length(btrim(reason)) > 0),
    engaged_by   UUID        NOT NULL,                    -- GCID of the admin / owner / operator
    engaged_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    released_by  UUID        NULL,
    released_at  TIMESTAMPTZ NULL,
    version      BIGINT      NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT companion_suspension_policy_release_consistent CHECK (
        (engaged = TRUE  AND released_by IS NULL     AND released_at IS NULL) OR
        (engaged = FALSE AND released_by IS NOT NULL AND released_at IS NOT NULL)
    )
);

COMMENT ON TABLE companion_suspension_policy IS
    'ADR-252 D2: tenant-scope Learning Companion containment (skill_key NULL = every '
    'companion turn in the tenant). RLS ENFORCING on chora.tenant_id, read by '
    'chora-model-gateway under SET LOCAL chora.tenant_id on every companion turn '
    '(ADR-254 D7). Release is a state change, never a delete.';

CREATE INDEX IF NOT EXISTS idx_companion_suspension_policy_engaged
    ON companion_suspension_policy (tenant_id, skill_key)
    WHERE engaged = TRUE;

CREATE TRIGGER trg_companion_suspension_policy_updated_at
    BEFORE UPDATE ON companion_suspension_policy
    FOR EACH ROW EXECUTE FUNCTION observability_set_updated_at();

ALTER TABLE companion_suspension_policy ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON companion_suspension_policy;
CREATE POLICY tenant_isolation ON companion_suspension_policy
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- -----------------------------------------------------------------------------
-- Grants: the per-DB roles (chora_observability_app_rw / _app_ro). 9999 runs
-- last in lex order and re-grants over ALL tables; these explicit grants are
-- belt-and-suspenders for out-of-order application. The gateway connects as
-- app_rw (non-bypassing), so the tenant table is read under RLS.
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_observability_app_rw') THEN
        EXECUTE 'GRANT SELECT, INSERT, UPDATE ON platform_companion_suspension TO chora_observability_app_rw';
        EXECUTE 'GRANT SELECT, INSERT, UPDATE ON companion_suspension_policy   TO chora_observability_app_rw';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_observability_app_ro') THEN
        EXECUTE 'GRANT SELECT ON platform_companion_suspension TO chora_observability_app_ro';
        EXECUTE 'GRANT SELECT ON companion_suspension_policy   TO chora_observability_app_ro';
    END IF;
END$$;

COMMIT;

-- VERIFICATION (run after apply):
--   SELECT count(*) FROM platform_companion_suspension WHERE engaged;  -- => 0 (nobody contained)
--   SELECT relrowsecurity FROM pg_class WHERE relname = 'companion_suspension_policy'; -- => t
-- =============================================================================
