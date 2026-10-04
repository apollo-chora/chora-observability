-- 0015_rls_guc_rekey — re-key legacy-GUC RLS policies to chora.tenant_id (CHO-2140).
--
-- These 5 tenant_isolation policies were keyed on the DEAD GUC
-- `app.current_tenant_id` while all live code sets `chora.tenant_id`, so under
-- RLS they read EMPTY for the app role (ritual_run_audit confirmed dark live by
-- the value-stream session). This is the mig-0035 (chora_sharing) dark-surface
-- class. Analytics (0004) policies were already modern and are left untouched.
--
-- Applied live via psql on 2026-07-11 (idempotent DROP IF EXISTS + CREATE);
-- this file makes the change reproducible. Isolation is ENGAGED, never widened.

BEGIN;

-- 0007_familiar_growth_audit (4)
DROP POLICY IF EXISTS tenant_isolation_familiar_growth_audit_ledger ON familiar_growth_audit_ledger;
CREATE POLICY tenant_isolation_familiar_growth_audit_ledger ON familiar_growth_audit_ledger
    USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

DROP POLICY IF EXISTS tenant_isolation_familiar_growth_daily_metrics ON familiar_growth_daily_metrics;
CREATE POLICY tenant_isolation_familiar_growth_daily_metrics ON familiar_growth_daily_metrics
    USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

DROP POLICY IF EXISTS tenant_isolation_breed_roll_audit ON breed_roll_audit;
CREATE POLICY tenant_isolation_breed_roll_audit ON breed_roll_audit
    USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

DROP POLICY IF EXISTS tenant_isolation_egg_funnel_metrics ON egg_funnel_metrics;
CREATE POLICY tenant_isolation_egg_funnel_metrics ON egg_funnel_metrics
    USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

-- 0013_ritual_run_audit (1)
DROP POLICY IF EXISTS tenant_isolation_ritual_run_audit ON ritual_run_audit;
CREATE POLICY tenant_isolation_ritual_run_audit ON ritual_run_audit
    USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);

COMMIT;
