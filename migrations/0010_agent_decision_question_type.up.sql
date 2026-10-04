-- =============================================================================
-- chora-observability : 0010_agent_decision_question_type.up.sql
--
-- Domain        : Observability (supporting/platform)
-- Database      : chora_observability
-- Date          : 2026-06-08
-- Jira          : CHO-1670 (O+ /o/agents nested qgen + oe_grading crews)
-- Companion     : chora-contracts/proto/events/observability/agent_decision.proto
--                 (field-21 attributes map; key "question_type" per ADR-167 D5)
--
-- Purpose:
--   The O+ /o/agents view renders the qgen crew as THREE tiles — qgen-mcq /
--   qgen-OE / qgen-critique. The MCQ vs OE split for the qgen_question agent is
--   keyed on the per-question-type tag the qgen crew emits in the proto
--   field-21 attributes map under key "question_type" ("mcq" | "oe"). The
--   agent_decision_log read model had nowhere to persist that tag, so the
--   split could not be reconstructed on the read side.
--
--   This migration adds the additive, nullable projection column the ADR-167
--   binding writes (agent_decision_binding.go extracts attributes["question_type"]).
--
-- Domain / not a hard enum:
--   question_type is a free-form VARCHAR(16), NOT a CHECK-constrained set. The
--   canonical values today are "mcq" | "oe", but a future question kind (e.g.
--   a new authoring format) must NOT cause the consumer to NACK a real
--   decision → DLQ over an unrecognised tag. The O+ handler simply renders no
--   tile for an unknown value (forward-compatible). NULL = non-question
--   decision (routing-only / critic-without-tag / non-qgen agents).
--
-- Append-only safety:
--   agent_decision_log carries BEFORE-UPDATE / BEFORE-DELETE triggers
--   (enforce_adl_append_only, migration 0001). ALTER TABLE ADD COLUMN is DDL,
--   not a row UPDATE — the row-level triggers do not fire on schema change, so
--   an additive nullable column is safe and does not violate the append-only
--   invariant. No existing row is rewritten (NULL backfill is metadata-only on
--   Postgres for a nullable-without-default column).
--
-- Idempotent: ADD COLUMN IF NOT EXISTS + CREATE INDEX IF NOT EXISTS.
-- =============================================================================

ALTER TABLE agent_decision_log
    ADD COLUMN IF NOT EXISTS question_type VARCHAR(16) NULL;

-- The qgen tile split aggregates per (agent_id, question_type). A partial
-- composite index — only question-tagged rows carry the cost — supports any
-- SQL-side grouping that follows the in-memory aggregation the handler does
-- today (mirrors migration 0009's idx_adl_crew_id grouping-column precedent).
CREATE INDEX IF NOT EXISTS idx_adl_agent_question_type
    ON agent_decision_log (agent_id, question_type)
    WHERE question_type IS NOT NULL;
