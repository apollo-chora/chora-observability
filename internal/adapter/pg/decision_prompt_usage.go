// decision_prompt_usage.go - pgx implementation of the CHO-2364 (ADR-197
// read slice) decision.PromptUsageRepository port on pg.DecisionRepository.
//
// One grouped SELECT over agent_decision_log partitions the tenant's rows by
// (agent_id, set_mode presence, campaign surface, prompt_version,
// prompt_source). The HTTP layer folds these groups into the wire shape, so
// the SQL stays a plain partition (every row lands in exactly one group and
// per-agent totals are exact sums).
//
// JSONB semantics are NULL-safe by construction:
//   - COALESCE(prompt_conditions ? 'set_mode', false): a row with NULL
//     conditions has no set_mode key (counts as the single-question AI-assist
//     use case, not as SQL NULL that would silently drop the row).
//   - COALESCE(prompt_conditions->>'request_surface', '') = 'campaign'
//     matches the daily-dose surface; an absent surface is false, which also
//     matches the batch bucket's IS DISTINCT FROM 'campaign' reading.
//
// Tenant isolation mirrors DecisionRepository.List: NormalizeTenantForRLS +
// WithTenantTx (SET LOCAL chora.tenant_id) so the migration-0001 RLS policy
// passes.
package pg

import (
	"context"
	"fmt"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/decision"
)

// AggregatePromptUsage implements decision.PromptUsageRepository.
func (r *DecisionRepository) AggregatePromptUsage(ctx context.Context, tenantID string, agentIDs []string) ([]decision.PromptUsageGroup, error) {
	const q = `
        SELECT COALESCE(agent_id, ''),
               COALESCE(prompt_conditions ? 'set_mode', false),
               COALESCE(prompt_conditions->>'request_surface', '') = 'campaign',
               COALESCE(prompt_conditions->>'prompt_version', ''),
               COALESCE(prompt_conditions->>'prompt_source', ''),
               COUNT(*)::bigint,
               MAX(recorded_at)
        FROM agent_decision_log
        WHERE tenant_id = $1
          AND agent_id = ANY($2)
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY 1, 7
    `
	tenantID = NormalizeTenantForRLS(tenantID)
	out := make([]decision.PromptUsageGroup, 0)
	err := r.q.WithTenantTx(ctx, tenantID, func(ctx context.Context, tx TenantScopedQuerier) error {
		rows, err := tx.Query(ctx, q, tenantID, agentIDs)
		if err != nil {
			return fmt.Errorf("pg.DecisionRepository.AggregatePromptUsage: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var g decision.PromptUsageGroup
			if err := rows.Scan(
				&g.AgentID,
				&g.HasSetMode,
				&g.IsCampaign,
				&g.PromptVersion,
				&g.PromptSource,
				&g.Decisions,
				&g.LastSeen,
			); err != nil {
				return fmt.Errorf("pg.DecisionRepository.AggregatePromptUsage scan: %w", err)
			}
			out = append(out, g)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Compile-time check.
var _ decision.PromptUsageRepository = (*DecisionRepository)(nil)
