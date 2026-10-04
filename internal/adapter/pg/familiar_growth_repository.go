// familiar_growth_repository.go — pgx-backed implementation of
// familiargrowth.Repository (ADR-149 Familiar Growth audit, PROD-H).
//
// Backs the 4 audit tables in chora_observability (migration 0007):
//
//	familiar_growth_audit_ledger   (one row per inbound event; UNIQUE source_event_id)
//	familiar_growth_daily_metrics  (per-tenant per-day per-source rollup; UPSERT)
//	breed_roll_audit               (one row per breed_revealed; IMDA D2)
//	egg_funnel_metrics             (per-tenant per-day egg funnel; UPSERT)
//
// Mirrors pg.LedgerRepository / pg.DecisionRepository: a Querier abstracts the
// SQL surface; tenant-scoped statements run inside WithTenantTx so RLS passes.
//
// CHO-2140 completion (2026-07-17): these 4 tables were born (migration 0007)
// keyed on the legacy `app.current_tenant_id` GUC, and this repository used the
// matching WithAppTenantTx seam. 0015_rls_guc_rekey re-keyed all 4 policies to
// the canonical `chora.tenant_id` and the seam was NOT updated, so every write
// set a GUC no policy read: the policy evaluated against NULL and Postgres
// refused the INSERT with 42501. All 4 tables held ZERO rows — the lane's
// ingress was independently broken (CHO-2257), so the RLS fault only surfaced
// once real events finally arrived. Verified against live pg_policy, not the
// migration comments: 17 of 17 tenant policies read `chora.tenant_id`, none read
// the legacy GUC. rls_guc_coherence_test.go now enforces the match.
//
// Ingest is ATOMIC: the ledger insert + the optional breed-roll insert +
// daily-metrics UPSERT + egg-funnel UPSERT all run in ONE transaction. A
// UNIQUE collision on source_event_id surfaces as fg.ErrDuplicateSourceEvent
// (idempotent replay — the subscriber treats it as a no-op).
//
// Cross-DB queries forbidden — chora-observability reads only
// chora_observability (per ddd-enforcement).
package pg

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	fg "github.com/apollo-chora/chora-observability/internal/domain/familiargrowth"
)

// FamiliarGrowthRepository is the pgx-backed familiargrowth.Repository.
type FamiliarGrowthRepository struct {
	q Querier
}

// NewFamiliarGrowthRepository wraps an Querier (PgxPoolQuerier in
// production).
func NewFamiliarGrowthRepository(q Querier) *FamiliarGrowthRepository {
	return &FamiliarGrowthRepository{q: q}
}

// Ingest atomically writes the audit ledger row + optional side effects.
func (r *FamiliarGrowthRepository) Ingest(ctx context.Context, req fg.IngestRequest) error {
	if strings.TrimSpace(req.Ledger.AuditID) == "" ||
		strings.TrimSpace(req.Ledger.TenantID) == "" ||
		strings.TrimSpace(req.Ledger.SourceEventID) == "" {
		return fg.ErrInvalidArgument
	}

	row := req.Ledger
	if row.ReceivedAt.IsZero() {
		row.ReceivedAt = time.Now().UTC()
	}
	payloadJSON, err := json.Marshal(orEmptyMap(row.Payload))
	if err != nil {
		return fmt.Errorf("pg.FamiliarGrowthRepository.Ingest: marshal payload: %w", err)
	}

	tenantID := row.TenantID
	return r.q.WithTenantTx(ctx, tenantID, func(ctx context.Context, tx TenantScopedQuerier) error {
		const insertLedger = `
            INSERT INTO familiar_growth_audit_ledger (
                audit_id, tenant_id, source_topic, source_event_id,
                familiar_id, owner_gcid, event_type, payload, received_at
            ) VALUES (
                $1, $2, $3, $4, NULLIF($5,'')::uuid, NULLIF($6,'')::uuid, $7, $8::jsonb, $9
            )`
		if err := tx.Exec(ctx, insertLedger,
			row.AuditID, tenantID, row.SourceTopic, row.SourceEventID,
			row.FamiliarID, row.OwnerGCID, row.EventType, string(payloadJSON), row.ReceivedAt,
		); err != nil {
			if isUniqueViolationErr(err) {
				return fg.ErrDuplicateSourceEvent
			}
			return fmt.Errorf("pg.FamiliarGrowthRepository.Ingest: insert ledger: %w", err)
		}

		if req.BreedRoll != nil {
			b := *req.BreedRoll
			if strings.TrimSpace(b.AuditID) == "" {
				b.AuditID = row.AuditID
			}
			if b.TenantID == "" {
				b.TenantID = tenantID
			}
			snapJSON, mErr := json.Marshal(orEmptyMap(b.DistributionSnapshot))
			if mErr != nil {
				return fmt.Errorf("pg.FamiliarGrowthRepository.Ingest: marshal snapshot: %w", mErr)
			}
			revealedAt := b.RevealedAt
			if revealedAt.IsZero() {
				revealedAt = row.ReceivedAt
			}
			const insertBreed = `
                INSERT INTO breed_roll_audit (
                    audit_id, tenant_id, familiar_id, owner_gcid, egg_sku,
                    species, shiny, rarity, rolled_probability,
                    distribution_snapshot, revealed_at
                ) VALUES (
                    $1, $2, $3, NULLIF($4,'')::uuid, $5,
                    $6, $7, $8, $9, $10::jsonb, $11
                )`
			if err := tx.Exec(ctx, insertBreed,
				b.AuditID, b.TenantID, b.FamiliarID, b.OwnerGCID, b.EggSKU,
				b.Species, b.Shiny, b.Rarity, b.RolledProbability,
				string(snapJSON), revealedAt,
			); err != nil {
				return fmt.Errorf("pg.FamiliarGrowthRepository.Ingest: insert breed_roll: %w", err)
			}
		}

		if d := req.MetricsDelta; d != nil {
			const upsertMetrics = `
                INSERT INTO familiar_growth_daily_metrics (
                    tenant_id, day_bucket, source,
                    total_exp_awarded, event_count, stage_ups_count, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (tenant_id, day_bucket, source) DO UPDATE SET
                    total_exp_awarded = familiar_growth_daily_metrics.total_exp_awarded + EXCLUDED.total_exp_awarded,
                    event_count       = familiar_growth_daily_metrics.event_count       + EXCLUDED.event_count,
                    stage_ups_count   = familiar_growth_daily_metrics.stage_ups_count   + EXCLUDED.stage_ups_count,
                    updated_at        = EXCLUDED.updated_at`
			if err := tx.Exec(ctx, upsertMetrics,
				tenantID, d.DayBucket.UTC(), d.Source,
				d.ExpAwardedDelta, d.EventCountDelta, d.StageUpsDelta, row.ReceivedAt,
			); err != nil {
				return fmt.Errorf("pg.FamiliarGrowthRepository.Ingest: upsert daily_metrics: %w", err)
			}
		}

		if f := req.FunnelDelta; f != nil {
			const upsertFunnel = `
                INSERT INTO egg_funnel_metrics (
                    tenant_id, day_bucket,
                    eggs_purchased, eggs_hatched, eggs_expired_unhatched, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (tenant_id, day_bucket) DO UPDATE SET
                    eggs_purchased         = egg_funnel_metrics.eggs_purchased         + EXCLUDED.eggs_purchased,
                    eggs_hatched           = egg_funnel_metrics.eggs_hatched           + EXCLUDED.eggs_hatched,
                    eggs_expired_unhatched = egg_funnel_metrics.eggs_expired_unhatched + EXCLUDED.eggs_expired_unhatched,
                    updated_at             = EXCLUDED.updated_at`
			if err := tx.Exec(ctx, upsertFunnel,
				tenantID, f.DayBucket.UTC(),
				f.PurchasedDelta, f.HatchedDelta, f.ExpiredDelta, row.ReceivedAt,
			); err != nil {
				return fmt.Errorf("pg.FamiliarGrowthRepository.Ingest: upsert egg_funnel: %w", err)
			}
		}
		return nil
	})
}

// ListLedger returns audit_ledger rows for the tenant, sorted received_at DESC.
func (r *FamiliarGrowthRepository) ListLedger(ctx context.Context, tenantID string, f fg.AuditFilter) ([]fg.AuditLedgerRow, error) {
	limit, offset := boundLimitOffset(f.Limit, f.Offset)
	const q = `
        SELECT audit_id, tenant_id, source_topic, source_event_id,
               COALESCE(familiar_id::text, ''), COALESCE(owner_gcid::text, ''),
               event_type, payload::text, received_at
        FROM familiar_growth_audit_ledger
        WHERE tenant_id = $1
          AND ($2::timestamptz IS NULL OR received_at >= $2)
          AND ($3::timestamptz IS NULL OR received_at <  $3)
          AND ($4::text = '' OR source_topic = $4)
          AND ($5::text = '' OR familiar_id = NULLIF($5,'')::uuid)
        ORDER BY received_at DESC
        LIMIT $6 OFFSET $7`
	out := make([]fg.AuditLedgerRow, 0)
	err := r.q.WithTenantTx(ctx, tenantID, func(ctx context.Context, tx TenantScopedQuerier) error {
		rows, err := tx.Query(ctx, q,
			tenantID, nullableTime(f.From), nullableTime(f.To), f.Source, f.FamiliarID, limit, offset,
		)
		if err != nil {
			return fmt.Errorf("pg.FamiliarGrowthRepository.ListLedger: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var (
				row         fg.AuditLedgerRow
				payloadJSON string
			)
			if err := rows.Scan(
				&row.AuditID, &row.TenantID, &row.SourceTopic, &row.SourceEventID,
				&row.FamiliarID, &row.OwnerGCID, &row.EventType, &payloadJSON, &row.ReceivedAt,
			); err != nil {
				return fmt.Errorf("pg.FamiliarGrowthRepository.ListLedger scan: %w", err)
			}
			row.Payload = decodeJSONMap(payloadJSON)
			out = append(out, row)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ListMetrics returns daily-rollup rows for tenant + window.
func (r *FamiliarGrowthRepository) ListMetrics(ctx context.Context, tenantID string, f fg.AuditFilter) ([]fg.DailyMetricsRow, error) {
	const q = `
        SELECT tenant_id, day_bucket, source,
               total_exp_awarded, event_count, stage_ups_count, updated_at
        FROM familiar_growth_daily_metrics
        WHERE tenant_id = $1
          AND ($2::timestamptz IS NULL OR day_bucket >= $2)
          AND ($3::timestamptz IS NULL OR day_bucket <  $3)
          AND ($4::text = '' OR source = $4)
        ORDER BY day_bucket DESC, source ASC`
	out := make([]fg.DailyMetricsRow, 0)
	err := r.q.WithTenantTx(ctx, tenantID, func(ctx context.Context, tx TenantScopedQuerier) error {
		rows, err := tx.Query(ctx, q, tenantID, nullableTime(f.From), nullableTime(f.To), f.Source)
		if err != nil {
			return fmt.Errorf("pg.FamiliarGrowthRepository.ListMetrics: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var row fg.DailyMetricsRow
			if err := rows.Scan(
				&row.TenantID, &row.DayBucket, &row.Source,
				&row.TotalExpAwarded, &row.EventCount, &row.StageUpsCount, &row.UpdatedAt,
			); err != nil {
				return fmt.Errorf("pg.FamiliarGrowthRepository.ListMetrics scan: %w", err)
			}
			out = append(out, row)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ListBreedRolls returns breed_roll_audit rows for tenant + filter.
func (r *FamiliarGrowthRepository) ListBreedRolls(ctx context.Context, tenantID string, f fg.AuditFilter) ([]fg.BreedRollAuditRow, error) {
	const q = `
        SELECT audit_id, tenant_id, familiar_id, COALESCE(owner_gcid::text, ''),
               egg_sku, species, shiny, rarity, rolled_probability,
               distribution_snapshot::text, revealed_at
        FROM breed_roll_audit
        WHERE tenant_id = $1
          AND ($2::timestamptz IS NULL OR revealed_at >= $2)
          AND ($3::timestamptz IS NULL OR revealed_at <  $3)
          AND ($4::text = '' OR egg_sku = $4)
        ORDER BY revealed_at DESC`
	out := make([]fg.BreedRollAuditRow, 0)
	err := r.q.WithTenantTx(ctx, tenantID, func(ctx context.Context, tx TenantScopedQuerier) error {
		rows, err := tx.Query(ctx, q, tenantID, nullableTime(f.From), nullableTime(f.To), f.EggSKU)
		if err != nil {
			return fmt.Errorf("pg.FamiliarGrowthRepository.ListBreedRolls: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var (
				row      fg.BreedRollAuditRow
				snapJSON string
			)
			if err := rows.Scan(
				&row.AuditID, &row.TenantID, &row.FamiliarID, &row.OwnerGCID,
				&row.EggSKU, &row.Species, &row.Shiny, &row.Rarity, &row.RolledProbability,
				&snapJSON, &row.RevealedAt,
			); err != nil {
				return fmt.Errorf("pg.FamiliarGrowthRepository.ListBreedRolls scan: %w", err)
			}
			row.DistributionSnapshot = decodeJSONMap(snapJSON)
			out = append(out, row)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ListEggFunnel returns egg_funnel_metrics rows for tenant + window.
func (r *FamiliarGrowthRepository) ListEggFunnel(ctx context.Context, tenantID string, f fg.AuditFilter) ([]fg.EggFunnelRow, error) {
	const q = `
        SELECT tenant_id, day_bucket,
               eggs_purchased, eggs_hatched, eggs_expired_unhatched, updated_at
        FROM egg_funnel_metrics
        WHERE tenant_id = $1
          AND ($2::timestamptz IS NULL OR day_bucket >= $2)
          AND ($3::timestamptz IS NULL OR day_bucket <  $3)
        ORDER BY day_bucket DESC`
	out := make([]fg.EggFunnelRow, 0)
	err := r.q.WithTenantTx(ctx, tenantID, func(ctx context.Context, tx TenantScopedQuerier) error {
		rows, err := tx.Query(ctx, q, tenantID, nullableTime(f.From), nullableTime(f.To))
		if err != nil {
			return fmt.Errorf("pg.FamiliarGrowthRepository.ListEggFunnel: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var row fg.EggFunnelRow
			if err := rows.Scan(
				&row.TenantID, &row.DayBucket,
				&row.EggsPurchased, &row.EggsHatched, &row.EggsExpiredUnhatched, &row.UpdatedAt,
			); err != nil {
				return fmt.Errorf("pg.FamiliarGrowthRepository.ListEggFunnel scan: %w", err)
			}
			out = append(out, row)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

func orEmptyMap(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}

func decodeJSONMap(s string) map[string]any {
	if strings.TrimSpace(s) == "" {
		return map[string]any{}
	}
	out := map[string]any{}
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return map[string]any{}
	}
	return out
}

func boundLimitOffset(limit, offset int) (int, int) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}

// isUniqueViolationErr loosely detects a Postgres unique-violation (SQLSTATE
// 23505) without binding to a specific driver error type — mirrors the
// outbox adapter's isUniqueViolation.
func isUniqueViolationErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "23505") ||
		strings.Contains(msg, "duplicate key value violates unique constraint")
}

// Compile-time check.
var _ fg.Repository = (*FamiliarGrowthRepository)(nil)
