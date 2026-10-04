// prompt_evidence.go - in-memory twins of the CHO-2364 (ADR-197 read slice)
// prompt-evidence aggregation ports. Hermetic fixtures for the agent-prompts
// handler tests + the no-DSN dev path; they mirror the pg SQL semantics
// exactly (set_mode presence, campaign surface, non-empty version grouping,
// per-run stamp dedupe, legacy CamelCase stamp-key fallback).
package inmem

import (
	"context"
	"sort"

	"github.com/apollo-chora/chora-observability/internal/domain/decision"
	ra "github.com/apollo-chora/chora-observability/internal/domain/ritualaudit"
)

// promptUsageKey mirrors the pg GROUP BY columns.
type promptUsageKey struct {
	agentID       string
	hasSetMode    bool
	isCampaign    bool
	promptVersion string
	promptSource  string
}

// AggregatePromptUsage implements decision.PromptUsageRepository.
func (r *DecisionRepository) AggregatePromptUsage(_ context.Context, tenantID string, agentIDs []string) ([]decision.PromptUsageGroup, error) {
	allowed := make(map[string]bool, len(agentIDs))
	for _, id := range agentIDs {
		allowed[id] = true
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	groups := make(map[promptUsageKey]*decision.PromptUsageGroup)
	for _, l := range r.logs {
		if l.TenantID != tenantID || !allowed[l.Agid] {
			continue
		}
		pc := l.PromptConditions
		_, hasSetMode := pc["set_mode"]
		k := promptUsageKey{
			agentID:       l.Agid,
			hasSetMode:    hasSetMode,
			isCampaign:    pc["request_surface"] == "campaign",
			promptVersion: pc["prompt_version"],
			promptSource:  pc["prompt_source"],
		}
		g, ok := groups[k]
		if !ok {
			g = &decision.PromptUsageGroup{
				AgentID:       k.agentID,
				HasSetMode:    k.hasSetMode,
				IsCampaign:    k.isCampaign,
				PromptVersion: k.promptVersion,
				PromptSource:  k.promptSource,
			}
			groups[k] = g
		}
		g.Decisions++
		if l.CreatedAt.After(g.LastSeen) {
			g.LastSeen = l.CreatedAt
		}
	}
	out := make([]decision.PromptUsageGroup, 0, len(groups))
	for _, g := range groups {
		out = append(out, *g)
	}
	// Deterministic order mirroring the pg ORDER BY (agent, last_seen).
	sort.Slice(out, func(i, j int) bool {
		if out[i].AgentID != out[j].AgentID {
			return out[i].AgentID < out[j].AgentID
		}
		if !out[i].LastSeen.Equal(out[j].LastSeen) {
			return out[i].LastSeen.Before(out[j].LastSeen)
		}
		if out[i].PromptVersion != out[j].PromptVersion {
			return out[i].PromptVersion < out[j].PromptVersion
		}
		return out[i].PromptSource < out[j].PromptSource
	})
	return out, nil
}

// stampPromptVersion reads the per-step prompt version from one stamp map:
// the canonical snake_case 'prompt_version' first (CHO-2136 wire), then the
// legacy Go-field-name 'PromptVersion' (pre-CHO-2136 JSON rows). Returns ”
// when neither carries a non-empty string.
func stampPromptVersion(stamp map[string]any) string {
	if v, ok := stamp["prompt_version"].(string); ok && v != "" {
		return v
	}
	if v, ok := stamp["PromptVersion"].(string); ok && v != "" {
		return v
	}
	return ""
}

// AggregatePromptStamps implements ritualaudit.PromptStampRepository.
func (r *RitualAuditRepository) AggregatePromptStamps(_ context.Context, tenantID string) (ra.PromptStampSummary, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	sum := ra.PromptStampSummary{Versions: make([]ra.StampVersionCount, 0)}
	acc := make(map[string]*ra.StampVersionCount)
	for _, row := range r.rows {
		if row.TenantID != tenantID {
			continue
		}
		runAt := row.OccurredAt
		if runAt.IsZero() {
			runAt = row.ReceivedAt
		}
		sum.RunsTotal++
		if runAt.After(sum.LastRunAt) {
			sum.LastRunAt = runAt
		}
		// Distinct versions within THIS run: a version repeated across steps
		// of one run counts as one run (mirrors COUNT(DISTINCT run_id)).
		seen := make(map[string]bool)
		for _, stamp := range row.Stamps {
			v := stampPromptVersion(stamp)
			if v == "" || seen[v] {
				continue
			}
			seen[v] = true
			c, ok := acc[v]
			if !ok {
				c = &ra.StampVersionCount{PromptVersion: v}
				acc[v] = c
			}
			c.Runs++
			if runAt.After(c.LastSeen) {
				c.LastSeen = runAt
			}
		}
	}
	for _, c := range acc {
		sum.Versions = append(sum.Versions, *c)
	}
	// Deterministic order mirroring the pg ORDER BY (last_seen DESC, version).
	sort.Slice(sum.Versions, func(i, j int) bool {
		if !sum.Versions[i].LastSeen.Equal(sum.Versions[j].LastSeen) {
			return sum.Versions[i].LastSeen.After(sum.Versions[j].LastSeen)
		}
		return sum.Versions[i].PromptVersion < sum.Versions[j].PromptVersion
	})
	return sum, nil
}

// Compile-time checks.
var (
	_ decision.PromptUsageRepository = (*DecisionRepository)(nil)
	_ ra.PromptStampRepository       = (*RitualAuditRepository)(nil)
)
