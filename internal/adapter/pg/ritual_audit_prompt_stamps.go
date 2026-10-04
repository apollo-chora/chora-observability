// ritual_audit_prompt_stamps.go - pgx implementation of the CHO-2364
// (ADR-197 read slice) ritualaudit.PromptStampRepository port on
// pg.RitualAuditRepository.
//
// Two statements run inside ONE WithTenantTx (both must see the same RLS
// scope):
//  1. run totals: COUNT(*) + MAX(run time) over ritual_run_audit.
//  2. per-version counts: LATERAL jsonb_array_elements over the stamps
//     array, counting DISTINCT runs per non-empty prompt version.
//
// Stamp key duality: the canonical wire key is snake_case 'prompt_version'
// (CHO-2136); rows ingested from the pre-CHO-2136 legacy JSON wire stored Go
// field names ('PromptVersion'). The reader coalesces snake first, Camel
// second, so legacy runs stay visible in the evidence instead of silently
// vanishing.
//
// Run time = COALESCE(occurred_at, received_at): occurred_at is the honest
// event time but is nullable; received_at (NOT NULL ingest time) is the
// fallback, never a fabricated value.
package pg

import (
	"context"
	"fmt"
	"time"

	ra "github.com/apollo-chora/chora-observability/internal/domain/ritualaudit"
)

// AggregatePromptStamps implements ritualaudit.PromptStampRepository.
func (r *RitualAuditRepository) AggregatePromptStamps(ctx context.Context, tenantID string) (ra.PromptStampSummary, error) {
	const totalsQ = `
        SELECT COUNT(*)::bigint, MAX(COALESCE(occurred_at, received_at))
        FROM ritual_run_audit
        WHERE tenant_id = $1
    `
	const versionsQ = `
        SELECT COALESCE(NULLIF(s.stamp->>'prompt_version', ''), NULLIF(s.stamp->>'PromptVersion', '')) AS pv,
               COUNT(DISTINCT r.run_id)::bigint,
               MAX(COALESCE(r.occurred_at, r.received_at))
        FROM ritual_run_audit r
        CROSS JOIN LATERAL jsonb_array_elements(r.stamps) AS s(stamp)
        WHERE r.tenant_id = $1
          AND COALESCE(NULLIF(s.stamp->>'prompt_version', ''), NULLIF(s.stamp->>'PromptVersion', '')) IS NOT NULL
        GROUP BY 1
        ORDER BY 3 DESC, 1
    `
	sum := ra.PromptStampSummary{Versions: make([]ra.StampVersionCount, 0)}
	err := r.q.WithTenantTx(ctx, tenantID, func(ctx context.Context, tx TenantScopedQuerier) error {
		var lastRun *time.Time
		if err := tx.QueryRow(ctx, totalsQ, tenantID).Scan(&sum.RunsTotal, &lastRun); err != nil {
			return fmt.Errorf("pg.RitualAuditRepository.AggregatePromptStamps totals: %w", err)
		}
		if lastRun != nil {
			sum.LastRunAt = *lastRun
		}
		rows, err := tx.Query(ctx, versionsQ, tenantID)
		if err != nil {
			return fmt.Errorf("pg.RitualAuditRepository.AggregatePromptStamps versions: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var v ra.StampVersionCount
			if err := rows.Scan(&v.PromptVersion, &v.Runs, &v.LastSeen); err != nil {
				return fmt.Errorf("pg.RitualAuditRepository.AggregatePromptStamps scan: %w", err)
			}
			sum.Versions = append(sum.Versions, v)
		}
		return rows.Err()
	})
	if err != nil {
		return ra.PromptStampSummary{}, err
	}
	return sum, nil
}

// Compile-time check.
var _ ra.PromptStampRepository = (*RitualAuditRepository)(nil)
