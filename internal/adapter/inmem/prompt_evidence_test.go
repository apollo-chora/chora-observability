// prompt_evidence_test.go - unit tests for the in-memory twins of the
// CHO-2364 prompt-evidence aggregation ports (ADR-197 read slice):
// DecisionRepository.AggregatePromptUsage + RitualAuditRepository.
// AggregatePromptStamps. The twins back the agent-prompts handler tests and
// must mirror the pg SQL semantics exactly (set_mode presence, campaign
// surface, non-empty version grouping, per-run stamp dedupe, legacy
// CamelCase stamp-key fallback).
package inmem_test

import (
	"context"
	"testing"
	"time"

	"github.com/apollo-chora/chora-observability/internal/adapter/inmem"
	"github.com/apollo-chora/chora-observability/internal/domain/decision"
	ra "github.com/apollo-chora/chora-observability/internal/domain/ritualaudit"
)

const (
	peTenant      = "pe-tenant-1"
	peOtherTenant = "pe-tenant-2"
)

func peSeedDecision(t *testing.T, repo *inmem.DecisionRepository, logID, tenant, agid string, at time.Time, pc map[string]string) {
	t.Helper()
	err := repo.Append(context.Background(), &decision.Log{
		LogID:            logID,
		TenantID:         tenant,
		Agid:             agid,
		DecisionType:     decision.TypeRespond,
		RiskTier:         decision.TierLow,
		CreatedAt:        at,
		PromptConditions: pc,
	})
	if err != nil {
		t.Fatalf("Append %s: %v", logID, err)
	}
}

func TestInmemAggregatePromptUsage_GroupsAndFilters(t *testing.T) {
	t.Parallel()
	repo := inmem.NewDecisionRepository()
	base := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)
	// Two ai-assist rows on v3/hub (no set_mode), one batch row (set_mode,
	// studio surface), one campaign row (set_mode, campaign surface) and a
	// condition-less row (nil map -> no set_mode, no version).
	peSeedDecision(t, repo, "l1", peTenant, "qgen_question", base, map[string]string{
		"prompt_version": "v3", "prompt_source": "hub",
	})
	peSeedDecision(t, repo, "l2", peTenant, "qgen_question", base.Add(time.Hour), map[string]string{
		"prompt_version": "v3", "prompt_source": "hub",
	})
	peSeedDecision(t, repo, "l3", peTenant, "qgen_question", base.Add(2*time.Hour), map[string]string{
		"prompt_version": "v3", "prompt_source": "hub", "set_mode": "batch", "request_surface": "studio",
	})
	peSeedDecision(t, repo, "l4", peTenant, "qgen_question", base.Add(3*time.Hour), map[string]string{
		"prompt_version": "v4", "prompt_source": "grimoire", "set_mode": "set", "request_surface": "campaign",
	})
	peSeedDecision(t, repo, "l5", peTenant, "qgen_question", base.Add(4*time.Hour), nil)
	// Excluded: other agent + other tenant.
	peSeedDecision(t, repo, "l6", peTenant, "moderator", base, map[string]string{"prompt_version": "v9"})
	peSeedDecision(t, repo, "l7", peOtherTenant, "qgen_question", base, map[string]string{"prompt_version": "v8"})

	groups, err := repo.AggregatePromptUsage(context.Background(), peTenant, []string{"qgen_question", "qgen_critic"})
	if err != nil {
		t.Fatalf("AggregatePromptUsage: %v", err)
	}
	type key struct {
		hasSet, isCampaign     bool
		version, source, agent string
	}
	got := map[key]decision.PromptUsageGroup{}
	var total int64
	for _, g := range groups {
		got[key{g.HasSetMode, g.IsCampaign, g.PromptVersion, g.PromptSource, g.AgentID}] = g
		total += g.Decisions
	}
	if total != 5 {
		t.Errorf("total decisions across groups = %d; want 5 (agent+tenant filtered)", total)
	}
	assist := got[key{false, false, "v3", "hub", "qgen_question"}]
	if assist.Decisions != 2 || !assist.LastSeen.Equal(base.Add(time.Hour)) {
		t.Errorf("assist group = %+v; want 2 decisions, last_seen=base+1h", assist)
	}
	batch := got[key{true, false, "v3", "hub", "qgen_question"}]
	if batch.Decisions != 1 || !batch.LastSeen.Equal(base.Add(2*time.Hour)) {
		t.Errorf("batch group = %+v; want 1 decision", batch)
	}
	campaign := got[key{true, true, "v4", "grimoire", "qgen_question"}]
	if campaign.Decisions != 1 || !campaign.LastSeen.Equal(base.Add(3*time.Hour)) {
		t.Errorf("campaign group = %+v; want 1 decision", campaign)
	}
	bare := got[key{false, false, "", "", "qgen_question"}]
	if bare.Decisions != 1 || !bare.LastSeen.Equal(base.Add(4*time.Hour)) {
		t.Errorf("condition-less group = %+v; want 1 decision with empty version", bare)
	}
	for k := range got {
		if k.agent != "qgen_question" {
			t.Errorf("unexpected agent in groups: %q", k.agent)
		}
	}
}

func TestInmemAggregatePromptUsage_EmptyIsHonestZero(t *testing.T) {
	t.Parallel()
	repo := inmem.NewDecisionRepository()
	groups, err := repo.AggregatePromptUsage(context.Background(), peTenant, []string{"qgen_question"})
	if err != nil {
		t.Fatalf("AggregatePromptUsage: %v", err)
	}
	if groups == nil || len(groups) != 0 {
		t.Errorf("groups = %#v; want empty non-nil slice", groups)
	}
}

func peIngestRun(t *testing.T, repo *inmem.RitualAuditRepository, n, tenant string, at time.Time, stamps []map[string]any) {
	t.Helper()
	err := repo.Ingest(context.Background(), ra.RitualRunAuditRow{
		AuditID:       "audit-" + n,
		TenantID:      tenant,
		SourceTopic:   ra.TopicFamiliarRitualRunCompleted,
		SourceEventID: "evt-" + n,
		RunID:         "run-" + n,
		Status:        "completed",
		Stamps:        stamps,
		OccurredAt:    at,
		ReceivedAt:    at.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("Ingest %s: %v", n, err)
	}
}

func TestInmemAggregatePromptStamps_DedupesPerRunAndReadsLegacyKey(t *testing.T) {
	t.Parallel()
	repo := inmem.NewRitualAuditRepository()
	base := time.Date(2026, 7, 22, 8, 0, 0, 0, time.UTC)
	// runA: v3 twice in one run -> ONE run for v3.
	peIngestRun(t, repo, "a", peTenant, base, []map[string]any{
		{"prompt_version": "v3"}, {"prompt_version": "v3"},
	})
	// runB: legacy CamelCase key (pre-CHO-2136 JSON rows) + v4 + empty.
	peIngestRun(t, repo, "b", peTenant, base.Add(time.Hour), []map[string]any{
		{"PromptVersion": "v3"}, {"prompt_version": "v4"}, {"prompt_version": ""},
	})
	// runC: stampless run still counts toward runs_total + last_run_at.
	peIngestRun(t, repo, "c", peTenant, base.Add(2*time.Hour), nil)
	// Excluded: other tenant.
	peIngestRun(t, repo, "x", peOtherTenant, base.Add(3*time.Hour), []map[string]any{
		{"prompt_version": "v9"},
	})

	sum, err := repo.AggregatePromptStamps(context.Background(), peTenant)
	if err != nil {
		t.Fatalf("AggregatePromptStamps: %v", err)
	}
	if sum.RunsTotal != 3 {
		t.Errorf("RunsTotal = %d; want 3", sum.RunsTotal)
	}
	if !sum.LastRunAt.Equal(base.Add(2 * time.Hour)) {
		t.Errorf("LastRunAt = %s; want %s", sum.LastRunAt, base.Add(2*time.Hour))
	}
	got := map[string]ra.StampVersionCount{}
	for _, v := range sum.Versions {
		got[v.PromptVersion] = v
	}
	if len(got) != 2 {
		t.Fatalf("versions = %#v; want v3 + v4 only", sum.Versions)
	}
	if got["v3"].Runs != 2 || !got["v3"].LastSeen.Equal(base.Add(time.Hour)) {
		t.Errorf("v3 = %+v; want runs=2 (runA + legacy runB)", got["v3"])
	}
	if got["v4"].Runs != 1 || !got["v4"].LastSeen.Equal(base.Add(time.Hour)) {
		t.Errorf("v4 = %+v; want runs=1", got["v4"])
	}
}

func TestInmemAggregatePromptStamps_EmptyIsHonestZero(t *testing.T) {
	t.Parallel()
	repo := inmem.NewRitualAuditRepository()
	sum, err := repo.AggregatePromptStamps(context.Background(), peTenant)
	if err != nil {
		t.Fatalf("AggregatePromptStamps: %v", err)
	}
	if sum.RunsTotal != 0 || !sum.LastRunAt.IsZero() {
		t.Errorf("totals = %d/%s; want 0/zero-time", sum.RunsTotal, sum.LastRunAt)
	}
	if sum.Versions == nil || len(sum.Versions) != 0 {
		t.Errorf("Versions = %#v; want empty non-nil slice", sum.Versions)
	}
}
