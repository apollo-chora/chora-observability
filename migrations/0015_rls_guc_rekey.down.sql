-- 0015_rls_guc_rekey (down) — restore the prior (legacy-GUC) policy definitions.
-- NB: reverting re-darks these tables for the app role (which sets chora.tenant_id);
-- provided only for migration reversibility.

BEGIN;

DROP POLICY IF EXISTS tenant_isolation_familiar_growth_audit_ledger ON familiar_growth_audit_ledger;
CREATE POLICY tenant_isolation_familiar_growth_audit_ledger ON familiar_growth_audit_ledger
    USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

DROP POLICY IF EXISTS tenant_isolation_familiar_growth_daily_metrics ON familiar_growth_daily_metrics;
CREATE POLICY tenant_isolation_familiar_growth_daily_metrics ON familiar_growth_daily_metrics
    USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

DROP POLICY IF EXISTS tenant_isolation_breed_roll_audit ON breed_roll_audit;
CREATE POLICY tenant_isolation_breed_roll_audit ON breed_roll_audit
    USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

DROP POLICY IF EXISTS tenant_isolation_egg_funnel_metrics ON egg_funnel_metrics;
CREATE POLICY tenant_isolation_egg_funnel_metrics ON egg_funnel_metrics
    USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

DROP POLICY IF EXISTS tenant_isolation_ritual_run_audit ON ritual_run_audit;
CREATE POLICY tenant_isolation_ritual_run_audit ON ritual_run_audit
    USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

COMMIT;
