-- =============================================================================
-- chora-observability : 0021_token_usage_image_count.down.sql
--
-- Reverts the image_count column from token_usage_ledger.
-- Drops the index first, then the column.
--
-- ⚠ IDEMPOTENT: uses IF EXISTS so safe to re-run.
-- =============================================================================;

BEGIN;

-- Drop the index
DROP INDEX IF EXISTS idx_token_ledger_image_count;

-- Drop the column
ALTER TABLE token_usage_ledger
    DROP COLUMN IF EXISTS image_count;

COMMIT;
