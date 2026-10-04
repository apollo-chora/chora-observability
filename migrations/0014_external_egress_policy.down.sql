-- =============================================================================
-- chora-observability : 0014_external_egress_policy.down.sql
-- Reverses 0014_external_egress_policy.up.sql (CHO-2113 / ADR-231 D6).
-- =============================================================================

BEGIN;

DROP TRIGGER IF EXISTS trg_grounded_search_daily_usage_updated_at ON grounded_search_daily_usage;
DROP TRIGGER IF EXISTS trg_external_egress_policy_updated_at        ON external_egress_policy;
DROP TRIGGER IF EXISTS trg_platform_egress_killswitch_updated_at    ON platform_egress_killswitch;

DROP TABLE IF EXISTS grounded_search_daily_usage;
DROP TABLE IF EXISTS external_egress_policy;
DROP TABLE IF EXISTS platform_egress_killswitch;

COMMIT;
