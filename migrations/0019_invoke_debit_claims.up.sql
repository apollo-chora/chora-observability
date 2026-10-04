-- =============================================================================
-- chora-observability : 0019_invoke_debit_claims.up.sql
--
-- ADR             : ADR-254 D7 (R22 gateway ledger idempotency) · ADR-253 D3a
--                   condition 1 (receive-side idempotency is still built) ·
--                   ADR-177 (the gateway is the SOLE meter)
-- Domain          : Observability (supporting)  /  Database : chora_observability
-- Date            : 2026-08-22
-- Companion       : services/chora-model-gateway internal/adapter/pg/pg_debit_claim.go
--                   + internal/adapter/middleware/mana_metering.go
--
-- Purpose:
--   Pub/Sub is at-least-once: a dispatched agent turn can be redelivered and the
--   agent re-invokes the gateway with the SAME dispatch idempotency_key. The
--   learner must be billed ONCE. The metering middleware takes a keyed claim on
--   (gcid, dispatch_idempotency_key, action_code) BEFORE the mana debit:
--
--     INSERT ... ON CONFLICT DO NOTHING   -- first claim wins and debits;
--     a second claim on the same key inserts nothing, debits nothing, and the
--     call proceeds (debit_deduped=true in the log and recorded here on the
--     claim row: dedupe_count / last_deduped_invocation_id / last_deduped_at).
--
--   This table IS the gateway's ledger of dispatch-keyed debits; the G1 proof
--   reads it ("redelivered request bills once"). It lives here because every
--   gateway-owned table lives in chora_observability (0008, 0014, 0018).
--
--   No RLS: the row is keyed by the learner's GCID and a workflow key, written
--   by the gateway (chora_observability_app_rw) on its own transaction with no
--   tenant context (the mana debit itself is tenant-checked by chora-identity).
--   tenant_id is recorded for audit, not for isolation.
--
-- Deploy note:
--   ADDITIVE + idempotent. Until applied the claim ERRORS and the middleware
--   serves-but-does-not-charge the dispatched turn (logged loudly), never a
--   double debit. Apply BEFORE the gateway image that claims.
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS invoke_debit_claims (
    gcid                        UUID        NOT NULL,
    dispatch_idempotency_key    TEXT        NOT NULL CHECK (length(dispatch_idempotency_key) > 0),
    action_code                 TEXT        NOT NULL CHECK (length(action_code) > 0),
    tenant_id                   UUID        NULL,
    invocation_id               TEXT        NOT NULL,                   -- the invocation that won the claim
    claimed_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    dedupe_count                INTEGER     NOT NULL DEFAULT 0 CHECK (dedupe_count >= 0),
    last_deduped_invocation_id  TEXT        NULL,
    last_deduped_at             TIMESTAMPTZ NULL,
    PRIMARY KEY (gcid, dispatch_idempotency_key, action_code)
);

COMMENT ON TABLE invoke_debit_claims IS
    'ADR-254 D7 (R22): keyed debit claims taken by chora-model-gateway before the '
    'mana debit of a dispatched agent turn. First claim on (gcid, dispatch key, '
    'action_code) debits; a redelivery finds the row, debits nothing, and is '
    'counted in dedupe_count. The gateway ledger row the redelivery proof reads.';

CREATE INDEX IF NOT EXISTS idx_invoke_debit_claims_claimed_at
    ON invoke_debit_claims (claimed_at);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_observability_app_rw') THEN
        EXECUTE 'GRANT SELECT, INSERT, UPDATE ON invoke_debit_claims TO chora_observability_app_rw';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chora_observability_app_ro') THEN
        EXECUTE 'GRANT SELECT ON invoke_debit_claims TO chora_observability_app_ro';
    END IF;
END$$;

COMMIT;
