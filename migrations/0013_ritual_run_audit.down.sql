-- =============================================================================
-- chora-observability : 0013_ritual_run_audit.down.sql
--
-- Reverses 0013_ritual_run_audit.up.sql — drops the ritual_run_audit table
-- (its RLS policy + indexes drop with it). Idempotent: DROP ... IF EXISTS.
-- =============================================================================

BEGIN;

DROP POLICY IF EXISTS tenant_isolation_ritual_run_audit ON ritual_run_audit;
DROP TABLE IF EXISTS ritual_run_audit;

COMMIT;
