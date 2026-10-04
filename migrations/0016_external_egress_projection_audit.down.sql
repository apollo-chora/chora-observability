-- =============================================================================
-- chora-observability : 0016_external_egress_projection_audit.down.sql
--
-- Reverses 0016. Dropping source_version disables the monotonic guard, so the
-- projection would once again be vulnerable to an out-of-order redelivery —
-- roll back only alongside the subscriber that depends on it.
--
-- external_egress_policy itself (0014) is left intact: it is the model-gateway's
-- fail-closed read-copy and dropping it would deny all grounded egress.
-- =============================================================================

BEGIN;

DROP POLICY IF EXISTS tenant_isolation ON external_egress_policy_audit;
DROP TABLE IF EXISTS external_egress_policy_audit;

ALTER TABLE platform_egress_killswitch DROP COLUMN IF EXISTS updated_by_gcid;

ALTER TABLE external_egress_policy DROP COLUMN IF EXISTS updated_by_gcid;
ALTER TABLE external_egress_policy DROP COLUMN IF EXISTS source_version;

COMMIT;
