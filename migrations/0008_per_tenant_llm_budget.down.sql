-- =============================================================================
-- chora-observability : 0008_per_tenant_llm_budget.down.sql
--
-- Reverse of 0008_per_tenant_llm_budget.up.sql (ADR-163 Phase 0 Sub-phase 0.2).
--
-- Safe to apply against any environment where 0008 was applied:
--   - Trigger + RLS policy drop first
--   - Table drop next (CASCADE not used; the table has no inbound FKs)
--   - ENUM type drop last (only if no other table references it; we keep
--     `IF EXISTS` guards + a DO block to gate the type drop on its actual
--     usage count, matching the additive ENUM convention in 0001)
--
-- If a rollback is needed mid-flight, the gateway service MUST be stopped
-- BEFORE running this; otherwise in-flight Invoke transactions will fail
-- with "relation does not exist" mid-commit.
-- =============================================================================

BEGIN;

DROP TRIGGER IF EXISTS trg_per_tenant_llm_budget_updated_at ON per_tenant_llm_budget;
DROP POLICY  IF EXISTS tenant_isolation                     ON per_tenant_llm_budget;

DROP INDEX IF EXISTS idx_per_tenant_llm_budget_tenant_window;
DROP INDEX IF EXISTS idx_per_tenant_llm_budget_spent_ratio;

DROP TABLE IF EXISTS per_tenant_llm_budget;

-- Only drop the ENUM type if no OTHER table is using it. The DO block
-- handles the "ENUM was created by 0008 but is referenced by a later
-- migration that adds another column of this type" case gracefully.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_depend d
          JOIN pg_type   t ON t.oid = d.refobjid
         WHERE t.typname = 'llm_budget_policy'
           AND d.deptype = 'n'                         -- normal dependency
    ) THEN
        DROP TYPE IF EXISTS llm_budget_policy;
    END IF;
END$$;

COMMIT;
