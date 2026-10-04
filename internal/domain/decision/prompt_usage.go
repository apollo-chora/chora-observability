// prompt_usage.go - CHO-2364 (ADR-197 read slice) prompt-evidence
// aggregation port over agent_decision_log.prompt_conditions.
//
// The O+ agent-prompts endpoint reports, per agent, which prompt versions
// actually drove production decisions. The repository returns compact
// aggregate groups keyed on the prompt_conditions discriminators; the HTTP
// layer composes the wire shape (totals, latest, versions[], use-case
// buckets) from these groups so pg and inmem share ONE composition path.
package decision

import (
	"context"
	"time"
)

// PromptUsageGroup is one aggregated bucket over agent_decision_log rows:
// GROUP BY (agent_id, set_mode presence, campaign surface, prompt_version,
// prompt_source) with a row count + the most recent recorded_at.
//
// The two booleans are the ADR-197 use-case discriminators:
//   - HasSetMode: the prompt_conditions JSONB contains the 'set_mode' key
//     (rows with NULL/absent conditions count as false, never NULL).
//   - IsCampaign: prompt_conditions->>'request_surface' = 'campaign'
//     (NULL-safe; absent surface counts as false).
//
// PromptVersion/PromptSource are '' when the row carried no such condition;
// version-less groups still contribute to per-agent totals but are excluded
// from versions[] and latest-version resolution (never fabricate).
type PromptUsageGroup struct {
	AgentID       string
	HasSetMode    bool
	IsCampaign    bool
	PromptVersion string
	PromptSource  string
	Decisions     int64
	LastSeen      time.Time
}

// PromptUsageRepository is the read port for the prompt-evidence
// aggregation. Implemented by the pg and inmem decision repositories; the
// agent-prompts endpoint 503s loudly when its decision repository does not
// provide it (no silent fallback).
type PromptUsageRepository interface {
	// AggregatePromptUsage returns the aggregate groups for the tenant,
	// restricted to the given agent ids. A tenant with no matching rows
	// returns an empty slice (honest zero), never an error.
	AggregatePromptUsage(ctx context.Context, tenantID string, agentIDs []string) ([]PromptUsageGroup, error)
}
