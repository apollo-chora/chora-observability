-- =============================================================================
-- chora-observability : 0007_familiar_growth_audit.up.sql
--
-- ADR-149 Familiar Growth audit + IMDA D1/D2 governance dashboards.
--
-- Subscribes / projects into 4 audit tables in chora_observability for the
-- 6 Familiar Growth event topics published by chora-consumption + chora-tenancy:
--
--   - chora.consumption.familiar.exp_awarded.v1
--   - chora.consumption.familiar.stage_up.v1
--   - chora.consumption.familiar.breed_revealed.v1
--   - chora.consumption.familiar.source_revelation.v1
--   - chora.consumption.familiar.egg_purchased.v1
--   - chora.tenancy.familiar_egg.payment_succeeded.v1
--
-- IMDA D1 (accountability):  every EXP increment + stage transition is
--                            persisted in familiar_growth_audit_ledger and
--                            aggregated into familiar_growth_daily_metrics.
-- IMDA D2 (transparency):    every breed lootbox roll persists outcome +
--                            distribution snapshot in breed_roll_audit so the
--                            empirical distribution can be audited against
--                            the published SKU breed_distribution table.
--
-- Domain   : Observability (supporting/platform)
-- Database : chora_observability
-- Date     : 2026-05-13
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- familiar_growth_audit_ledger — flat event ledger, one row per inbound
-- event. UNIQUE on source_event_id for idempotency under at-least-once
-- redelivery.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS familiar_growth_audit_ledger (
    audit_id        UUID         PRIMARY KEY,
    tenant_id       UUID         NOT NULL,
    source_topic    TEXT         NOT NULL,
    source_event_id UUID         NOT NULL UNIQUE,
    familiar_id     UUID         NULL,
    owner_gcid      UUID         NULL,
    event_type      TEXT         NOT NULL,
    payload         JSONB        NOT NULL DEFAULT '{}'::jsonb,
    received_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fgal_tenant_received
    ON familiar_growth_audit_ledger (tenant_id, received_at DESC);

CREATE INDEX IF NOT EXISTS idx_fgal_tenant_familiar
    ON familiar_growth_audit_ledger (tenant_id, familiar_id, received_at DESC)
    WHERE familiar_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_fgal_tenant_source
    ON familiar_growth_audit_ledger (tenant_id, source_topic, received_at DESC);

-- -----------------------------------------------------------------------------
-- familiar_growth_daily_metrics — per-tenant per-day per-source rollup.
-- Updated by the subscriber on every exp_awarded / stage_up event via UPSERT.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS familiar_growth_daily_metrics (
    tenant_id        UUID         NOT NULL,
    day_bucket       DATE         NOT NULL,
    source           TEXT         NOT NULL,
    total_exp_awarded BIGINT      NOT NULL DEFAULT 0 CHECK (total_exp_awarded >= 0),
    event_count       BIGINT      NOT NULL DEFAULT 0 CHECK (event_count       >= 0),
    stage_ups_count   INT         NOT NULL DEFAULT 0 CHECK (stage_ups_count   >= 0),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, day_bucket, source)
);

CREATE INDEX IF NOT EXISTS idx_fgdm_tenant_day
    ON familiar_growth_daily_metrics (tenant_id, day_bucket DESC);

-- -----------------------------------------------------------------------------
-- breed_roll_audit — every FamiliarBreedRevealed event (lootbox roll
-- outcome). distribution_snapshot is the full breed_distribution table at
-- roll time for retrospective IMDA D2 transparency audits.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS breed_roll_audit (
    audit_id              UUID          PRIMARY KEY,
    tenant_id             UUID          NOT NULL,
    familiar_id           UUID          NOT NULL,
    owner_gcid            UUID          NULL,
    egg_sku               TEXT          NOT NULL,
    species               TEXT          NOT NULL,
    shiny                 BOOLEAN       NOT NULL DEFAULT FALSE,
    rarity                TEXT          NOT NULL,
    rolled_probability    NUMERIC(5,2)  NOT NULL,
    distribution_snapshot JSONB         NOT NULL DEFAULT '{}'::jsonb,
    revealed_at           TIMESTAMPTZ   NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_bra_tenant_sku_time
    ON breed_roll_audit (tenant_id, egg_sku, revealed_at DESC);

CREATE INDEX IF NOT EXISTS idx_bra_tenant_species
    ON breed_roll_audit (tenant_id, species, revealed_at DESC);

-- -----------------------------------------------------------------------------
-- egg_funnel_metrics — per-tenant per-day egg-funnel rollup (purchases vs
-- hatches vs expirations). Hatched count is incremented on the
-- breed_revealed event (proxy for hatch), purchased on egg_purchased.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS egg_funnel_metrics (
    tenant_id                UUID         NOT NULL,
    day_bucket               DATE         NOT NULL,
    eggs_purchased           INT          NOT NULL DEFAULT 0 CHECK (eggs_purchased           >= 0),
    eggs_hatched             INT          NOT NULL DEFAULT 0 CHECK (eggs_hatched             >= 0),
    eggs_expired_unhatched   INT          NOT NULL DEFAULT 0 CHECK (eggs_expired_unhatched   >= 0),
    updated_at               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, day_bucket)
);

CREATE INDEX IF NOT EXISTS idx_efm_tenant_day
    ON egg_funnel_metrics (tenant_id, day_bucket DESC);

-- -----------------------------------------------------------------------------
-- RLS — every audit table is tenant-scoped per multi-tenant-rls. The session
-- GUC app.current_tenant_id is set by the service via SET LOCAL on every
-- request handler / subscriber message.
-- -----------------------------------------------------------------------------
ALTER TABLE familiar_growth_audit_ledger  ENABLE ROW LEVEL SECURITY;
ALTER TABLE familiar_growth_daily_metrics ENABLE ROW LEVEL SECURITY;
ALTER TABLE breed_roll_audit              ENABLE ROW LEVEL SECURITY;
ALTER TABLE egg_funnel_metrics            ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_familiar_growth_audit_ledger
    ON familiar_growth_audit_ledger
    USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

CREATE POLICY tenant_isolation_familiar_growth_daily_metrics
    ON familiar_growth_daily_metrics
    USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

CREATE POLICY tenant_isolation_breed_roll_audit
    ON breed_roll_audit
    USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

CREATE POLICY tenant_isolation_egg_funnel_metrics
    ON egg_funnel_metrics
    USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

COMMIT;
