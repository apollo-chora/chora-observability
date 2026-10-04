-- =============================================================================
-- chora-observability : 0003_outbox.sql
--
-- Adds the transactional outbox primitive to chora_observability per the
-- Phyllis Wave-B service-wiring directive (`feedback_resilience_priority`).
--
-- Domain   : Observability (supporting/platform)
-- Database : chora_observability
-- Date     : 2026-05-11
--
-- Mirrors the canonical fixture in
--   libs/chora-common/outbox/sql_fixtures/outbox_events.up.sql
-- so the existing chora-common/outbox PostgresRecorder + Relay drop in
-- without modification.
--
-- The chora-observability service emits chora.ai_kernel.token-usage.recorded.v1
-- on every TokenUsageLedger insert (see ai-cost-tracking skill). Without an
-- outbox the publish-after-write pattern leaks events under partial failure;
-- this migration makes the publish atomic with the ledger insert.
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS outbox_events (
    id              TEXT        PRIMARY KEY,
    aggregate_type  TEXT        NOT NULL,
    aggregate_id    TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    topic           TEXT        NOT NULL,
    payload         BYTEA       NOT NULL,
    envelope        JSONB       NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','published','failed','deadlettered')),
    retry_count     INT         NOT NULL DEFAULT 0,
    last_error      TEXT        NOT NULL DEFAULT '',
    last_attempt_at TIMESTAMPTZ,
    published_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS outbox_events_pending_idx
    ON outbox_events (occurred_at ASC)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS outbox_events_aggregate_idx
    ON outbox_events (aggregate_type, aggregate_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS outbox_events_topic_idx
    ON outbox_events (topic, status);

CREATE TABLE IF NOT EXISTS outbox_poll_checkpoints (
    worker_id                  TEXT        NOT NULL,
    topic                      TEXT        NOT NULL,
    last_processed_outbox_id   TEXT        NOT NULL,
    last_processed_occurred_at TIMESTAMPTZ NOT NULL,
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (worker_id, topic)
);

CREATE INDEX IF NOT EXISTS outbox_poll_checkpoints_topic_idx
    ON outbox_poll_checkpoints (topic, updated_at DESC);

CREATE TABLE IF NOT EXISTS outbox_dead_letters (
    outbox_event_id   TEXT        PRIMARY KEY REFERENCES outbox_events(id),
    failure_reason    TEXT        NOT NULL,
    attempt_count     INT         NOT NULL,
    worker_id         TEXT        NOT NULL,
    deadlettered_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at       TIMESTAMPTZ,
    resolution_note   TEXT        NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS outbox_dead_letters_unresolved_idx
    ON outbox_dead_letters (deadlettered_at DESC)
    WHERE resolved_at IS NULL;

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key           TEXT        PRIMARY KEY,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    ttl_at        TIMESTAMPTZ NOT NULL,
    result_hash   TEXT        NULL
);

CREATE INDEX IF NOT EXISTS idempotency_keys_ttl_idx
    ON idempotency_keys (ttl_at);

COMMIT;
