-- chora-observability : 0019_invoke_debit_claims.down.sql
-- Reverts 0019. Roll the gateway image back first: without the table the
-- middleware serves dispatched turns uncharged (logged), never double-bills.
BEGIN;
DROP TABLE IF EXISTS invoke_debit_claims;
COMMIT;
