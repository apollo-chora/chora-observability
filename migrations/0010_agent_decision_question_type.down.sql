-- =============================================================================
-- chora-observability : 0010_agent_decision_question_type.down.sql
--
-- Reverses 0010_agent_decision_question_type.up.sql. Drops only the column +
-- index this migration introduced.
-- =============================================================================

DROP INDEX IF EXISTS idx_adl_agent_question_type;

ALTER TABLE agent_decision_log
    DROP COLUMN IF EXISTS question_type;
