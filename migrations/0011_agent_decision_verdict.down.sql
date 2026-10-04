-- =============================================================================
-- chora-observability : 0011_agent_decision_verdict.down.sql
--
-- Reverses 0011_agent_decision_verdict.up.sql — drops the additive nullable
-- verdict projection column. Idempotent: DROP COLUMN IF EXISTS.
-- =============================================================================

ALTER TABLE agent_decision_log
    DROP COLUMN IF EXISTS verdict;
