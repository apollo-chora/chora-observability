-- =============================================================================
-- chora-observability : 0006_outbox_d6_canonical.up.sql
--
-- Brings the chora_observability outbox table up to the D6.2 canonical
-- shape used by the chora-guardrail reference (B.6.2.a) per M12.3 Wave 2
-- (`docs/architecture/m12-3-outbox-templates-plan-2026-05-12.md`).
--
-- The existing migration 0003_outbox.sql created the operational outbox
-- (`outbox_events`, `outbox_poll_checkpoints`, `outbox_dead_letters`) using
-- the older chora-go-common/outbox shape. That shape lacks four invariants
-- the D6.3 multi-tenant chaos contract relies on:
--
--   1. tenant_id as a top-level column (Pillar 3 — indexed isolation)
--   2. gcid (subject) + agid (agent identity) as top-level columns
--   3. idempotency_key UNIQUE column for producer-side dedupe
--   4. RLS policy keyed by app.current_tenant GUC
--
-- This migration ADDs the missing columns + indexes + RLS without rewriting
-- the existing table. Pre-existing rows backfill to defaults (tenant_id
-- inferred from envelope JSONB).
--
-- Domain   : Observability (supporting/platform)
-- Database : chora_observability
-- Date     : 2026-05-12
--
-- Mirrors:
--   services/chora-guardrail/migrations/0004_outbox.sql (canonical reference)
-- =============================================================================

BEGIN;

-- ----------------------------------------------------------------------------
-- 1. Add the D6.3 top-level isolation columns.
--    tenant_id NOT NULL (default 'platform' for any pre-existing rows).
--    gcid NULLABLE (system events may have no subject).
--    agid NULLABLE (only AI Kernel agent emissions carry it).
--    idempotency_key NOT NULL UNIQUE (producer-side dedupe).
-- ----------------------------------------------------------------------------

ALTER TABLE outbox_events
    ADD COLUMN IF NOT EXISTS tenant_id       TEXT NOT NULL DEFAULT 'platform',
    ADD COLUMN IF NOT EXISTS gcid            TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS agid            TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS idempotency_key TEXT NOT NULL DEFAULT '';

-- Backfill idempotency_key from envelope JSONB where possible (best-effort).
-- For rows missing the key, fall back to the event id so the UNIQUE index
-- can be created without collisions.
UPDATE outbox_events
   SET idempotency_key = COALESCE(envelope->>'idempotency_key', id)
 WHERE idempotency_key = '' OR idempotency_key IS NULL;

UPDATE outbox_events
   SET tenant_id = COALESCE(NULLIF(envelope->>'tenant_id', ''), 'platform')
 WHERE tenant_id = 'platform';

UPDATE outbox_events
   SET gcid = COALESCE(envelope->>'gcid', '')
 WHERE gcid = '';

-- ----------------------------------------------------------------------------
-- 2. Indexes — match the canonical guardrail shape.
-- ----------------------------------------------------------------------------

-- D6.3 multi-tenant isolation lookup (tenant + status + occurred_at).
CREATE INDEX IF NOT EXISTS outbox_events_tenant_idx
    ON outbox_events (tenant_id, status, occurred_at);

-- Idempotency dedupe — unique on idempotency_key.
CREATE UNIQUE INDEX IF NOT EXISTS outbox_events_idempotency_idx
    ON outbox_events (idempotency_key);

-- ----------------------------------------------------------------------------
-- 3. Multi-tenant RLS — same policy shape as the closure / guardrail outbox.
--    Production wires the GUC via per-connection SET LOCAL app.current_tenant.
--    Policy is permissive when the GUC is unset to keep the dispatcher
--    runnable in dev + multi-tenant aggregation jobs.
-- ----------------------------------------------------------------------------

ALTER TABLE outbox_events ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS outbox_events_tenant_isolation ON outbox_events;

CREATE POLICY outbox_events_tenant_isolation ON outbox_events
    USING (
        current_setting('app.current_tenant', TRUE) IS NULL
        OR current_setting('app.current_tenant', TRUE) = ''
        OR tenant_id = current_setting('app.current_tenant', TRUE)
    );

COMMIT;
