-- chora-observability : 0018_companion_suspension.down.sql
-- Reverts 0018. Dropping the tables makes the gateway's suspension read ERROR,
-- and the decorator then DENIES every companion turn (ADR-252 D6): roll the
-- gateway image back first, then apply this.
BEGIN;
DROP TABLE IF EXISTS companion_suspension_policy;
DROP TABLE IF EXISTS platform_companion_suspension;
COMMIT;
