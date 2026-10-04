-- =============================================================================
-- chora-observability : 0013_ritual_run_audit.up.sql
--
-- Grimoire Ritual run audit (ADR-215 auditor projection / ADR-219 CHO-2016).
--
-- Projects chora.consumption.familiar.ritual_run_completed.v1 (a learner-
-- composed Ritual run reaching a terminal state) into ONE audit table in
-- chora_observability so O+ auditors see every ritual run — terminal status +
-- mana charge + sink + the per-step ADR-197 decision stamps (the O+ full
-- record). One row per inbound event; UNIQUE on source_event_id for
-- idempotency under at-least-once redelivery.
--
-- IMDA D1 (accountability): every run (completed / failed / blocked /
--                           skipped_budget) is persisted with its mana charge
--                           + trigger source + revision.
-- IMDA D2 (transparency):   the per-step decision stamps (prompt version/hash,
--                           tools invoked, citations) are retained for
--                           retrospective audit.
--
-- stamps is a JSON ARRAY (one object per step) — it mirrors chora-consumption
-- familiar_ritual_runs.decision_stamp (migration 0071: JSONB DEFAULT '[]'),
-- which is the source-of-truth wire shape ([]StepStamp), NOT an object.
--
-- Domain   : Observability (supporting/platform)
-- Database : chora_observability
-- Date     : 2026-07-08
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- ritual_run_audit — flat event ledger, one row per inbound
-- ritual_run_completed event. UNIQUE on source_event_id for idempotency.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ritual_run_audit (
    audit_id        UUID         PRIMARY KEY,
    tenant_id       UUID         NOT NULL,
    source_topic    TEXT         NOT NULL,
    source_event_id UUID         NOT NULL UNIQUE,
    run_id          UUID         NOT NULL,
    ritual_id       UUID         NULL,
    familiar_id     UUID         NULL,
    owner_gcid      UUID         NULL,
    revision_no     INT          NULL,
    trigger_source  TEXT         NULL,
    status          TEXT         NOT NULL,
    mana_charged    INT          NULL,
    sink_ref        TEXT         NULL,
    error_text      TEXT         NULL,
    stamps          JSONB        NOT NULL DEFAULT '[]'::jsonb,
    occurred_at     TIMESTAMPTZ  NULL,
    received_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rra_tenant_received
    ON ritual_run_audit (tenant_id, received_at DESC);

CREATE INDEX IF NOT EXISTS idx_rra_tenant_familiar
    ON ritual_run_audit (tenant_id, familiar_id, received_at DESC)
    WHERE familiar_id IS NOT NULL;

-- -----------------------------------------------------------------------------
-- RLS — tenant-scoped per multi-tenant-rls. The session GUC
-- app.current_tenant_id is set by the service via SET LOCAL on every request
-- handler / subscriber message (WithAppTenantTx). Mirrors migration 0007's
-- exact USING clause. New-table grants are inherited from 9999 (ALTER DEFAULT
-- PRIVILEGES + GRANT ... ON ALL TABLES) — no explicit GRANT here, matching
-- 0007 / 0012.
-- -----------------------------------------------------------------------------
ALTER TABLE ritual_run_audit ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_ritual_run_audit
    ON ritual_run_audit
    USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

COMMIT;
