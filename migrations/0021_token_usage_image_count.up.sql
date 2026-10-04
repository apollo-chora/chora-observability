-- =============================================================================
-- chora-observability : 0021_token_usage_image_count.up.sql
--
-- Database : chora_observability
-- Context  : Adds image_count column to token_usage_ledger to track the number
--            of generated images per LLM call. Needed for flat-rate image models
--            (e.g., muse-image at $0.01/image) where billing is per-image rather
--            than per-token.
--
--            The column defaults to 0 for existing rows (text-only models) and
--            is populated by the model gateway for image-generating models.
--
-- ⚠ IDEMPOTENT: uses IF NOT EXISTS so safe to re-run.
-- =============================================================================;

BEGIN;

-- Add image_count column with default 0 and CHECK constraint
ALTER TABLE token_usage_ledger
    ADD COLUMN IF NOT EXISTS image_count INTEGER NOT NULL DEFAULT 0
        CHECK (image_count >= 0);

-- Index for querying image generation stats by tenant
CREATE INDEX IF NOT EXISTS idx_token_ledger_image_count
    ON token_usage_ledger (tenant_id, image_count)
    WHERE image_count > 0;

COMMIT;
