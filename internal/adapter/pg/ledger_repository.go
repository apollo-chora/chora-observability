// ledger_repository.go — pgx-backed implementation of ledger.Repository.
//
// SQL contract:
//
//   - Append: INSERT INTO token_usage_ledger (...). The migration installs
//     a trigger that rejects UPDATE/DELETE — callers cannot mutate after
//     insert (append-only invariant).
//   - List: filter by tenant + optional time window with stable ORDER BY
//     recorded_at + LIMIT/OFFSET pagination.
//   - SumCost: int64 aggregate over the filtered rows; uses domain
//     SumCost helper for overflow detection (we re-aggregate in Go to
//     keep the int64 overflow guard centralised).
//
// Cost stored as int64 micros (1e-6 USD) — never float64. fx_rate +
// cost_sgd_micros are populated by the upstream caller (Model Gateway);
// this repo persists what it's given.
package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/ledger"
)

// LedgerRepository is the pgx-backed implementation of ledger.Repository.
type LedgerRepository struct {
	q Querier
}

// NewLedgerRepository wraps a Querier.
func NewLedgerRepository(q Querier) *LedgerRepository {
	return &LedgerRepository{q: q}
}

// Append persists the entry. token_usage_ledger column order per
// migrations/0001_initial.sql:
//
//	ledger_id, tenant_id, gcid, agid, model_id,
//	prompt_tokens, completion_tokens, cost_usd_micros,
//	cost_sgd_micros, fx_rate, trace_id, span_id, recorded_at
func (r *LedgerRepository) Append(ctx context.Context, e *ledger.Entry) error {
	if e == nil {
		return errors.New("pg.LedgerRepository.Append: nil entry")
	}
	// Entry.Agid carries the crew agent_role (e.g. "qgen_question") — a
	// STRING, not a UUID. It belongs in the agent_id VARCHAR column; the
	// agid UUID column stays NULL (role-only crew agents have no AGID).
	// Writing the role into agid::uuid is the latent defect that kept this
	// table empty ("invalid input syntax for type uuid: \"qgen_question\"").
	q := `
        INSERT INTO token_usage_ledger (
            ledger_id, tenant_id, gcid, agent_id, model_id,
            prompt_tokens, completion_tokens, cost_usd_micros,
            cost_sgd_micros, fx_rate, trace_id, span_id, recorded_at
        )
        VALUES ($1, $2, $3, NULLIF($4, ''), $5,
                $6, $7, $8,
                $9, $10, $11, $12, $13)
    `
	// tenant_id is normalised ("platform"/"" → NilTenantUUID) so it both
	// satisfies the UUID column AND matches the SET LOCAL chora.tenant_id
	// GUC the RLS tenant_isolation policy checks. Without the tenant tx the
	// INSERT is RLS-rejected ("new row violates row-level security policy
	// for table token_usage_ledger").
	tenantID := NormalizeTenantForRLS(e.TenantID)
	// cost_sgd_micros + fx_rate are platform-set defaults until Model
	// Gateway pipes them in; we pass 0 + 1.35 here to satisfy NOT NULL
	// constraints. Reconciliation jobs (cmd/reconcile) backfill canonical
	// values.
	return r.q.WithTenantTx(ctx, tenantID, func(ctx context.Context, tx TenantScopedQuerier) error {
		return tx.Exec(ctx, q,
			e.LedgerID,
			tenantID,
			e.Gcid,
			e.Agid,
			e.ModelID,
			e.PromptTokens,
			e.CompletionTokens,
			e.CostUsdMicros,
			int64(0),
			float32(1.35),
			e.TraceID,
			e.SpanID,
			e.RecordedAt,
		)
	})
}

// List returns entries for the tenant matching the filter, sorted
// recorded_at ASC.
func (r *LedgerRepository) List(ctx context.Context, tenantID string, f ledger.ListFilter) ([]*ledger.Entry, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	offset := f.Offset
	if offset < 0 {
		offset = 0
	}
	q := `
        SELECT ledger_id, tenant_id, gcid, COALESCE(agent_id, ''), model_id,
               prompt_tokens, completion_tokens, cost_usd_micros,
               trace_id, span_id, recorded_at
        FROM token_usage_ledger
        WHERE tenant_id = $1
          AND ($2::timestamptz IS NULL OR recorded_at >= $2)
          AND ($3::timestamptz IS NULL OR recorded_at <  $3)
        ORDER BY recorded_at ASC
        LIMIT $4 OFFSET $5
    `
	// Reads are RLS-scoped too — without the tenant GUC the policy filters
	// every row to zero regardless of the WHERE clause.
	tenantID = NormalizeTenantForRLS(tenantID)
	out := make([]*ledger.Entry, 0)
	err := r.q.WithTenantTx(ctx, tenantID, func(ctx context.Context, tx TenantScopedQuerier) error {
		rows, err := tx.Query(ctx, q,
			tenantID,
			nullableTime(f.From),
			nullableTime(f.To),
			limit, offset,
		)
		if err != nil {
			return fmt.Errorf("pg.LedgerRepository.List: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var e ledger.Entry
			if err := rows.Scan(
				&e.LedgerID,
				&e.TenantID,
				&e.Gcid,
				&e.Agid,
				&e.ModelID,
				&e.PromptTokens,
				&e.CompletionTokens,
				&e.CostUsdMicros,
				&e.TraceID,
				&e.SpanID,
				&e.RecordedAt,
			); err != nil {
				return fmt.Errorf("pg.LedgerRepository.List scan: %w", err)
			}
			out = append(out, &e)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// SumCost aggregates cost_usd_micros over the filtered rows.
//
// We delegate to ledger.SumCost in Go (rather than SQL SUM()) so the
// int64-overflow guard in the domain is the single source of truth.
func (r *LedgerRepository) SumCost(ctx context.Context, tenantID string, f ledger.ListFilter) (int64, int, error) {
	full := f
	full.Limit = 0
	full.Offset = 0
	q := `
        SELECT cost_usd_micros
        FROM token_usage_ledger
        WHERE tenant_id = $1
          AND ($2::timestamptz IS NULL OR recorded_at >= $2)
          AND ($3::timestamptz IS NULL OR recorded_at <  $3)
    `
	tenantID = NormalizeTenantForRLS(tenantID)
	costs := make([]int64, 0)
	count := 0
	err := r.q.WithTenantTx(ctx, tenantID, func(ctx context.Context, tx TenantScopedQuerier) error {
		rows, err := tx.Query(ctx, q,
			tenantID,
			nullableTime(full.From),
			nullableTime(full.To),
		)
		if err != nil {
			return fmt.Errorf("pg.LedgerRepository.SumCost: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var c int64
			if err := rows.Scan(&c); err != nil {
				return fmt.Errorf("pg.LedgerRepository.SumCost scan: %w", err)
			}
			costs = append(costs, c)
			count++
		}
		return rows.Err()
	})
	if err != nil {
		return 0, count, err
	}
	total, err := ledger.SumCost(costs)
	if err != nil {
		return 0, count, err
	}
	return total, count, nil
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// Compile-time check.
var _ ledger.Repository = (*LedgerRepository)(nil)
