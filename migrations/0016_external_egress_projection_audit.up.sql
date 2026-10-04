-- =============================================================================
-- chora-observability : 0016_external_egress_projection_audit.up.sql
--
-- Domain   : Observability (supporting/platform)
-- Database : chora_observability
-- Story    : CHO-2148 (Far Sight egress governance write-path)
-- ADR      : ADR-220 D4 · ADR-231 D6 · PLAN.md §4.2.4
--
-- Makes `external_egress_policy` a PROJECTION rather than a hand-seeded table.
--
-- chora-tenancy owns the entitlement; it publishes
-- chora.tenancy.external_egress_policy.updated.v1 and chora-observability
-- projects that event into external_egress_policy — the read-copy the
-- model-gateway consults fail-closed on every grounded call. Cross-DB writes
-- are FORBIDDEN, so the projection is the ONLY bridge. Until now the table had
-- NO write path at all (0014 shipped it and the P5 activation seeded one row by
-- direct SQL).
--
-- Two things a projection needs that a hand-seeded table did not:
--
--   1. source_version — the monotonic guard. Pub/Sub is at-least-once and NOT
--      order-preserving, so a slow retry of an older change can arrive AFTER a
--      newer one. Without this, a redelivered "egress ON" could silently
--      resurrect an entitlement the tenant has since turned OFF. The projection
--      applies an event only when it is STRICTLY newer.
--
--   2. external_egress_policy_audit — the append-only trail. PLAN.md §4.2.4
--      fences tenant self-service with "an audit record on every change"; the
--      event already carries the actor and the before/after values, so the
--      projection records them here rather than reaching back across the DB
--      boundary (which it cannot do anyway).
--
-- ADDITIVE ONLY. The model-gateway's read path
-- (`SELECT egress_enabled, daily_call_ceiling FROM external_egress_policy
--   WHERE tenant_id = $1::uuid`, pg_grounded.go) names its columns explicitly
-- and is unaffected. Existing rows default to source_version = 0, so the FIRST
-- real event for any tenant (version >= 1) always wins over a hand-seeded row.
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1. external_egress_policy — the monotonic guard
-- -----------------------------------------------------------------------------
ALTER TABLE external_egress_policy
    ADD COLUMN IF NOT EXISTS source_version BIGINT NOT NULL DEFAULT 0;

COMMENT ON COLUMN external_egress_policy.source_version IS
    'Monotonic version from chora_tenancy.tenant_external_egress_policies. The '
    'projection applies an event ONLY when its version is strictly greater, so an '
    'out-of-order Pub/Sub redelivery cannot resurrect a revoked entitlement. '
    '0 = hand-seeded before CHO-2148; any real event (>= 1) supersedes it.';

-- The actor behind the currently-projected state (mirrors the source row).
ALTER TABLE external_egress_policy
    ADD COLUMN IF NOT EXISTS updated_by_gcid UUID NULL;

-- -----------------------------------------------------------------------------
-- 2. external_egress_policy_audit — append-only trail (safety net b)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS external_egress_policy_audit (
    audit_id                    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id                   UUID        NOT NULL,

    -- The source event. UNIQUE so a Pub/Sub redelivery that somehow slips past
    -- the idempotency inbox still cannot double-write the trail.
    event_id                    UUID        NOT NULL UNIQUE,
    source_version              BIGINT      NOT NULL,

    -- After-state.
    egress_enabled              BOOLEAN     NOT NULL,
    daily_call_ceiling          INTEGER     NOT NULL,

    -- Before-state — carried on the event so the trail is complete without a
    -- cross-DB read back into chora_tenancy (which is forbidden).
    previous_egress_enabled     BOOLEAN     NOT NULL,
    previous_daily_call_ceiling INTEGER     NOT NULL,

    -- Who decided. An egress change is never anonymous (IMDA D1).
    updated_by_gcid             UUID        NOT NULL,

    occurred_at                 TIMESTAMPTZ NOT NULL,
    projected_at                TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE external_egress_policy_audit IS
    'Append-only trail of every external web-egress entitlement change (CHO-2148). '
    'Answers "who enabled web egress, for which tenant, when, and from what" for '
    'O+ and IMDA D1. Written by the projection subscriber, never by hand.';

CREATE INDEX IF NOT EXISTS idx_external_egress_audit_tenant
    ON external_egress_policy_audit (tenant_id, occurred_at DESC);

ALTER TABLE external_egress_policy_audit ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON external_egress_policy_audit;
CREATE POLICY tenant_isolation ON external_egress_policy_audit
    FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- -----------------------------------------------------------------------------
-- 3. platform_egress_killswitch — name the operator who flipped it
-- -----------------------------------------------------------------------------
-- The kill-switch disables grounded egress for EVERY tenant at once. It already
-- carried a free-text `reason`; it must also carry WHO. No RLS: it is a
-- platform-global singleton, not tenant data.
ALTER TABLE platform_egress_killswitch
    ADD COLUMN IF NOT EXISTS updated_by_gcid UUID NULL;

-- -----------------------------------------------------------------------------
-- 4. Grants (guarded — only if the roles exist)
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_observability_app_rw') THEN
        GRANT SELECT, INSERT, UPDATE ON external_egress_policy_audit TO chora_observability_app_rw;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_observability_app_ro') THEN
        GRANT SELECT ON external_egress_policy_audit TO chora_observability_app_ro;
    END IF;
END $$;

COMMIT;
