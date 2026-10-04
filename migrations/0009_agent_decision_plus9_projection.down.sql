-- =============================================================================
-- chora-observability : 0009_agent_decision_plus9_projection.down.sql
--
-- Reverses 0009_agent_decision_plus9_projection.up.sql. Drops only the columns
-- this migration introduced — model_id / autonomy_level / guardrail_verdict are
-- owned by migration 0002 and are NOT touched here.
-- =============================================================================

DROP INDEX IF EXISTS idx_adl_crew_id;

ALTER TABLE agent_decision_log
    DROP COLUMN IF EXISTS confidence,
    DROP COLUMN IF EXISTS cost_usd_micros,
    DROP COLUMN IF EXISTS crew_name,
    DROP COLUMN IF EXISTS crew_id,
    DROP COLUMN IF EXISTS prompt_tokens,
    DROP COLUMN IF EXISTS completion_tokens,
    DROP COLUMN IF EXISTS cached_tokens;
