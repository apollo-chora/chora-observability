// Package cost_test exercises the by-act breakdown.
//
// Mirrors the Sequel comic's per-act cost callouts. Acts can correspond to
// value-streams (Atom Orchestration, Learning Delivery, Social Learning,
// etc.) and the breakdown sums cost by act_name. Used by the
// /cost/by-act?tenant_id=X&period=Y endpoint.
package cost_test

import (
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/cost"
)

func TestActBreakdown_Empty(t *testing.T) {
	t.Parallel()
	out := cost.GroupByAct(nil)
	if len(out) != 0 {
		t.Errorf("empty -> %d groups; want 0", len(out))
	}
}

func TestActBreakdown_GroupsByActName(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	acts := []cost.Act{
		{Name: "Atom Orchestration", CostUsdMicros: 120000, OccurredAt: now},
		{Name: "Learning Delivery", CostUsdMicros: 50000, OccurredAt: now},
		{Name: "Atom Orchestration", CostUsdMicros: 30000, OccurredAt: now},
		{Name: "Social Learning", CostUsdMicros: 70000, OccurredAt: now},
	}
	groups := cost.GroupByAct(acts)
	if len(groups) != 3 {
		t.Fatalf("groups = %d; want 3", len(groups))
	}
	// Sorted by name asc — deterministic.
	if groups[0].Name != "Atom Orchestration" {
		t.Errorf("groups[0] = %s; want Atom Orchestration", groups[0].Name)
	}
	if groups[0].TotalUsdMicros != 150000 {
		t.Errorf("Atom Orchestration sum = %d; want 150000", groups[0].TotalUsdMicros)
	}
	if groups[0].EntryCount != 2 {
		t.Errorf("Atom Orchestration count = %d; want 2", groups[0].EntryCount)
	}
}

func TestActBreakdown_SortedByName(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	acts := []cost.Act{
		{Name: "Z", CostUsdMicros: 1, OccurredAt: now},
		{Name: "M", CostUsdMicros: 1, OccurredAt: now},
		{Name: "A", CostUsdMicros: 1, OccurredAt: now},
	}
	groups := cost.GroupByAct(acts)
	if groups[0].Name != "A" || groups[1].Name != "M" || groups[2].Name != "Z" {
		t.Errorf("not sorted: %v", groups)
	}
}

func TestActBreakdown_SequelTotals(t *testing.T) {
	t.Parallel()
	// Sequel pivot: each "act" has its own value-stream label. When we
	// roll up by-act, the all-acts total must still match the comic's
	// $0.76 total. This locks the tie between cumulative + by-act views.
	now := time.Now().UTC()
	costs := []int64{120000, 40000, 50000, 30000, 40000, 60000, 20000, 30000,
		70000, 20000, 30000, 10000, 40000, 120000, 80000}
	streams := []string{
		"Atom Orchestration", "Training Administration", "Learning Delivery",
		"Cross-Tenant Identity", "Community Contribution", "Exam Preparation",
		"Social Learning", "Social Learning", "Familiar Companion",
		"Marketplace", "Cross-Tenant Identity", "Self-Healing",
		"Communications", "Exam Preparation", "Campus Operations",
	}
	acts := make([]cost.Act, len(costs))
	for i := range costs {
		acts[i] = cost.Act{
			Name:          streams[i],
			CostUsdMicros: costs[i],
			OccurredAt:    now,
		}
	}
	groups := cost.GroupByAct(acts)
	var sum int64
	for _, g := range groups {
		sum += g.TotalUsdMicros
	}
	if sum != 760000 {
		t.Errorf("total across acts = %d; want 760000 ($0.76)", sum)
	}
}

func TestActBreakdown_RejectsNegative(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic on negative cost")
		}
	}()
	cost.GroupByAct([]cost.Act{
		{Name: "x", CostUsdMicros: -1, OccurredAt: time.Now()},
	})
}
