// decision_prompt_usage_test.go - pgx adapter tests for the CHO-2364
// AggregatePromptUsage read (ADR-197 prompt-evidence aggregation over
// agent_decision_log.prompt_conditions). Uses the package's stub-Querier
// SQL-smoke idiom: capture the SELECT text + tenant-tx seam, serve scripted
// rows to verify the scan column order without a live DB.
package pg_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-observability/internal/adapter/pg"
)

var promptUsageAgentIDs = []string{"qgen_question", "qgen_critic", "oe_evaluator", "oe_moderator"}

// TestDecisionRepository_AggregatePromptUsage_SQLShape proves the aggregation
// runs inside WithTenantTx (RLS GUC), targets agent_decision_log, groups on
// the prompt_conditions JSONB discriminators (set_mode presence + campaign
// request_surface + prompt_version + prompt_source) and restricts to the
// caller's agent-id allowlist.
func TestDecisionRepository_AggregatePromptUsage_SQLShape(t *testing.T) {
	t.Parallel()
	lastSeen := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)
	q := &decisionStubQuerier{
		// Column order MUST match the repo's SELECT list:
		// agent_id, has_set_mode, is_campaign, prompt_version, prompt_source,
		// decisions, last_seen.
		row: []any{
			"qgen_question", true, false, "v3", "hub", int64(4), lastSeen,
		},
	}
	repo := pg.NewDecisionRepository(q)
	tenant := uuid.NewString()
	groups, err := repo.AggregatePromptUsage(context.Background(), tenant, promptUsageAgentIDs)
	if err != nil {
		t.Fatalf("AggregatePromptUsage: %v", err)
	}
	if !q.txCalled {
		t.Fatal("AggregatePromptUsage must run inside WithTenantTx (RLS GUC)")
	}
	if q.tenantID != tenant {
		t.Errorf("tenant GUC: got %q want %q", q.tenantID, tenant)
	}
	for _, want := range []string{
		"FROM agent_decision_log",
		"prompt_conditions ? 'set_mode'",
		"request_surface",
		"prompt_version",
		"prompt_source",
		"GROUP BY",
		"agent_id = ANY",
	} {
		if !containsSub(q.querySQL, want) {
			t.Errorf("SQL missing %q:\n%s", want, q.querySQL)
		}
	}
	if len(groups) != 1 {
		t.Fatalf("groups = %d; want 1", len(groups))
	}
	g := groups[0]
	if g.AgentID != "qgen_question" || !g.HasSetMode || g.IsCampaign ||
		g.PromptVersion != "v3" || g.PromptSource != "hub" ||
		g.Decisions != 4 || !g.LastSeen.Equal(lastSeen) {
		t.Errorf("group = %+v; want qgen_question/set_mode/no-campaign/v3/hub/4/%s", g, lastSeen)
	}
	// The agent allowlist must bind as a query arg, never interpolate.
	found := false
	for _, a := range q.queryArgs {
		if ids, ok := a.([]string); ok && len(ids) == 4 && ids[0] == "qgen_question" {
			found = true
		}
	}
	if !found {
		t.Errorf("agent-id allowlist not bound as []string arg: %#v", q.queryArgs)
	}
}

// TestDecisionRepository_AggregatePromptUsage_EmptyResult proves a zero-row
// aggregation returns an empty (non-nil) slice with no error: an idle tenant
// is an honest zero, never a fabricated group.
func TestDecisionRepository_AggregatePromptUsage_EmptyResult(t *testing.T) {
	t.Parallel()
	q := &decisionStubQuerier{}
	repo := pg.NewDecisionRepository(q)
	groups, err := repo.AggregatePromptUsage(context.Background(), uuid.NewString(), promptUsageAgentIDs)
	if err != nil {
		t.Fatalf("AggregatePromptUsage: %v", err)
	}
	if groups == nil || len(groups) != 0 {
		t.Errorf("groups = %#v; want empty non-nil slice", groups)
	}
}

// TestDecisionRepository_AggregatePromptUsage_NormalizesTenant proves the
// platform sentinel is normalised to the nil-UUID before the SET LOCAL GUC
// (mirrors List/Count).
func TestDecisionRepository_AggregatePromptUsage_NormalizesTenant(t *testing.T) {
	t.Parallel()
	q := &decisionStubQuerier{}
	repo := pg.NewDecisionRepository(q)
	if _, err := repo.AggregatePromptUsage(context.Background(), "platform", promptUsageAgentIDs); err != nil {
		t.Fatalf("AggregatePromptUsage: %v", err)
	}
	if q.tenantID != pg.NilTenantUUID {
		t.Errorf("tenant GUC = %q; want NilTenantUUID for the platform sentinel", q.tenantID)
	}
}
