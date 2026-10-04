-- =============================================================================
-- chora-observability : 0012_agent_decision_prompt_conditions.down.sql
--
-- Reverses 0012_agent_decision_prompt_conditions.up.sql — drops the additive
-- nullable prompt_conditions JSONB projection column. Idempotent:
-- DROP COLUMN IF EXISTS.
-- =============================================================================

ALTER TABLE agent_decision_log
    DROP COLUMN IF EXISTS prompt_conditions;
