// Package ledger_test exercises the per-tenant Budget aggregate.
//
// Budget invariants (per Tier 3 + ai-cost-tracking skill):
//   - Cap is in micro-USD (int64) to avoid drift
//   - Threshold-crossing emits exactly ONE event per threshold per period
//   - Thresholds: 50%, 80%, 100%, 110% (over-cap)
//   - Period rolls forward (key = tenant_id + period_id e.g. "2026-05")
package ledger_test

import (
	"testing"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/ledger"
)

func TestNewBudget_Valid(t *testing.T) {
	t.Parallel()
	b, err := ledger.NewBudget(ledger.BudgetParams{
		TenantID:     "t1",
		Period:       "2026-05",
		CapUsdMicros: 1_000_000, // $1.00
	})
	if err != nil {
		t.Fatalf("NewBudget: %v", err)
	}
	if b.BudgetID == "" {
		t.Errorf("BudgetID empty")
	}
	if b.SpentUsdMicros != 0 {
		t.Errorf("initial spent = %d; want 0", b.SpentUsdMicros)
	}
}

func TestNewBudget_RejectsZeroCap(t *testing.T) {
	t.Parallel()
	_, err := ledger.NewBudget(ledger.BudgetParams{
		TenantID:     "t1",
		Period:       "2026-05",
		CapUsdMicros: 0,
	})
	if err == nil {
		t.Errorf("expected error for zero cap")
	}
}

func TestNewBudget_RejectsNegativeCap(t *testing.T) {
	t.Parallel()
	_, err := ledger.NewBudget(ledger.BudgetParams{
		TenantID:     "t1",
		Period:       "2026-05",
		CapUsdMicros: -1,
	})
	if err == nil {
		t.Errorf("expected error for negative cap")
	}
}

func TestNewBudget_RejectsMissingTenant(t *testing.T) {
	t.Parallel()
	_, err := ledger.NewBudget(ledger.BudgetParams{
		Period:       "2026-05",
		CapUsdMicros: 1_000_000,
	})
	if err == nil {
		t.Errorf("expected error for missing tenant")
	}
}

func TestNewBudget_RejectsMissingPeriod(t *testing.T) {
	t.Parallel()
	_, err := ledger.NewBudget(ledger.BudgetParams{
		TenantID:     "t1",
		CapUsdMicros: 1_000_000,
	})
	if err == nil {
		t.Errorf("expected error for missing period")
	}
}

func TestBudget_RecordSpend_AccumulatesAndCrossesThreshold50(t *testing.T) {
	t.Parallel()
	b, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID:     "t1",
		Period:       "2026-05",
		CapUsdMicros: 1_000_000,
	})
	// Spend 600,000 micros (60%) — must cross 50% threshold once.
	crossed := b.RecordSpend(600_000)
	if len(crossed) != 1 || crossed[0] != ledger.Threshold50 {
		t.Errorf("expected single 50%% crossing; got %v", crossed)
	}
	if b.SpentUsdMicros != 600_000 {
		t.Errorf("spent = %d; want 600000", b.SpentUsdMicros)
	}
}

func TestBudget_RecordSpend_CrossesMultipleThresholdsAtomically(t *testing.T) {
	t.Parallel()
	b, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID:     "t1",
		Period:       "2026-05",
		CapUsdMicros: 1_000_000,
	})
	// Single big spend: 1,150,000 micros = 115% — crosses 50, 80, 100, 110.
	crossed := b.RecordSpend(1_150_000)
	if len(crossed) != 4 {
		t.Fatalf("expected 4 thresholds crossed; got %v", crossed)
	}
	want := []ledger.BudgetThreshold{
		ledger.Threshold50, ledger.Threshold80,
		ledger.Threshold100, ledger.Threshold110,
	}
	for i, w := range want {
		if crossed[i] != w {
			t.Errorf("crossed[%d] = %s; want %s", i, crossed[i], w)
		}
	}
}

func TestBudget_RecordSpend_EmitsEachThresholdOnlyOnce(t *testing.T) {
	t.Parallel()
	b, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID:     "t1",
		Period:       "2026-05",
		CapUsdMicros: 1_000_000,
	})
	// Three small spends crossing 50%, then more pushing past 80%.
	crossed1 := b.RecordSpend(200_000) // 20% — none
	if len(crossed1) != 0 {
		t.Errorf("20%% should not cross; got %v", crossed1)
	}
	crossed2 := b.RecordSpend(400_000) // 60% — crosses 50%
	if len(crossed2) != 1 || crossed2[0] != ledger.Threshold50 {
		t.Errorf("60%% should cross 50%%; got %v", crossed2)
	}
	// Re-spend small amount keeping below 80%, should NOT re-emit 50%.
	crossed3 := b.RecordSpend(100_000) // 70% — none new
	if len(crossed3) != 0 {
		t.Errorf("70%% (already past 50%%) must not re-emit; got %v", crossed3)
	}
	// Now push past 80%.
	crossed4 := b.RecordSpend(150_000) // 85% — crosses 80%
	if len(crossed4) != 1 || crossed4[0] != ledger.Threshold80 {
		t.Errorf("85%% should cross 80%%; got %v", crossed4)
	}
}

func TestBudget_RecordSpend_RejectsNegative(t *testing.T) {
	t.Parallel()
	b, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID:     "t1",
		Period:       "2026-05",
		CapUsdMicros: 1_000_000,
	})
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic for negative spend")
		}
	}()
	b.RecordSpend(-1)
}

func TestBudget_PercentSpent(t *testing.T) {
	t.Parallel()
	b, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID:     "t1",
		Period:       "2026-05",
		CapUsdMicros: 1_000_000,
	})
	if got := b.PercentSpent(); got != 0.0 {
		t.Errorf("initial pct = %v; want 0", got)
	}
	b.RecordSpend(500_000)
	if got := b.PercentSpent(); got != 50.0 {
		t.Errorf("pct = %v; want 50", got)
	}
}

func TestBudgetThresholds_Sorted(t *testing.T) {
	t.Parallel()
	want := []ledger.BudgetThreshold{
		ledger.Threshold50, ledger.Threshold80,
		ledger.Threshold100, ledger.Threshold110,
	}
	got := ledger.BudgetThresholdsAsc()
	if len(got) != len(want) {
		t.Fatalf("len = %d; want %d", len(got), len(want))
	}
	for i, t1 := range want {
		if got[i] != t1 {
			t.Errorf("order[%d] = %s; want %s", i, got[i], t1)
		}
	}
}
