-- =============================================================================
-- chora-observability : 0009_agent_decision_plus9_projection.up.sql
--
-- Domain        : Observability (supporting/platform)
-- Database      : chora_observability
-- Date          : 2026-06-02
-- Jira          : CHO-1560 (+9-field AgentDecisionLogged projection)
-- Companion     : chora-contracts/proto/events/observability/agent_decision.proto
--                 (model_id f6 / confidence f10 / crew_name f12 / crew_id f13 /
--                  guardrail_outcome f17 / prompt_tokens f18 /
--                  completion_tokens f19 / cached_tokens f20)
--
-- Purpose:
--   The O+ Decision Traces tab renders Model / Confidence / Cost / autonomy
--   columns from the chora-observability GET /api/agent-decisions read model,
--   reshaped by the chora-gateway mapAgentDecision transformer. The proto
--   contract + Python producer already EMIT model_id / confidence / crew_* /
--   token counts / guardrail_outcome, but the agent_decision_log read model
--   never persisted them — so those columns rendered blank.
--
--   This migration adds the additive, nullable projection columns that the
--   ADR-167 binding writes (model_id + autonomy_level already exist from
--   migration 0002 — they only needed WRITING; guardrail_outcome maps onto
--   the existing guardrail_verdict column, same pass|block|redact domain).
--
-- Append-only safety:
--   agent_decision_log carries BEFORE-UPDATE / BEFORE-DELETE triggers
--   (enforce_adl_append_only, migration 0001). ALTER TABLE ADD COLUMN is DDL,
--   not a row UPDATE — the row-level triggers do not fire on schema change, so
--   additive nullable columns are safe and do not violate the append-only
--   invariant. No existing row is rewritten (NULL backfill is metadata-only on
--   Postgres for nullable-without-default columns).
--
-- Idempotent: ADD COLUMN IF NOT EXISTS throughout.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- agent_decision_log — +9-field projection (CHO-1560)
--
-- Already present (migration 0002): model_id, autonomy_level (enum),
-- guardrail_verdict, agent_id, run_id, decided_at, ... — NOT re-added here.
--
-- New, all NULLable (a routing-only / non-LLM decision legitimately has no
-- token counts, no confidence, no cost):
--   confidence          REAL    — agent self-reported confidence in [0..1]
--   cost_usd_micros     BIGINT  — per-decision cost in 1e-6 USD, derived from
--                                 token counts x pricing.yaml at projection
--                                 time (proto carries no cost field)
--   crew_name           VARCHAR — crew the agent acted in (proto f12)
--   crew_id             VARCHAR — crew instance / orchestration id (proto f13)
--   prompt_tokens       BIGINT  — gen_ai.usage.prompt_tokens (proto f18)
--   completion_tokens   BIGINT  — gen_ai.usage.completion_tokens (proto f19)
--   cached_tokens       BIGINT  — gen_ai.usage.cached_tokens (proto f20)
-- -----------------------------------------------------------------------------
ALTER TABLE agent_decision_log
    ADD COLUMN IF NOT EXISTS confidence        REAL
        CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
    ADD COLUMN IF NOT EXISTS cost_usd_micros   BIGINT
        CHECK (cost_usd_micros IS NULL OR cost_usd_micros >= 0),
    ADD COLUMN IF NOT EXISTS crew_name         VARCHAR(128) NULL,
    ADD COLUMN IF NOT EXISTS crew_id           VARCHAR(64)  NULL,
    ADD COLUMN IF NOT EXISTS prompt_tokens     BIGINT
        CHECK (prompt_tokens IS NULL OR prompt_tokens >= 0),
    ADD COLUMN IF NOT EXISTS completion_tokens BIGINT
        CHECK (completion_tokens IS NULL OR completion_tokens >= 0),
    ADD COLUMN IF NOT EXISTS cached_tokens     BIGINT
        CHECK (cached_tokens IS NULL OR cached_tokens >= 0);

-- crew_id powers the Decision-Traces drill-down grouping (multi-agent
-- decisions back to a single orchestration). Partial index — only populated
-- rows carry the cost of the index.
CREATE INDEX IF NOT EXISTS idx_adl_crew_id
    ON agent_decision_log (crew_id) WHERE crew_id IS NOT NULL;
