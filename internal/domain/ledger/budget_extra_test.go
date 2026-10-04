// Package ledger_test exercises the under-covered Budget helpers
// (Remaining, IsOverCap, PercentSpent, thresholdRatio fallback).
package ledger_test

import (
	"testing"

	"github.com/apollo-chora/chora-observability/internal/domain/ledger"
)

func TestBudget_Remaining_PositiveAndZero(t *testing.T) {
	t.Parallel()
	b, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: "t1", Period: "2026-05", CapUsdMicros: 1_000_000,
	})
	if r := b.Remaining(); r != 1_000_000 {
		t.Errorf("initial remaining = %d; want 1_000_000", r)
	}
	b.RecordSpend(400_000)
	if r := b.Remaining(); r != 600_000 {
		t.Errorf("post-spend remaining = %d; want 600_000", r)
	}
	b.RecordSpend(700_000) // over cap
	if r := b.Remaining(); r != 0 {
		t.Errorf("over-cap remaining = %d; want 0", r)
	}
}

func TestBudget_IsOverCap(t *testing.T) {
	t.Parallel()
	b, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: "t1", Period: "2026-05", CapUsdMicros: 1_000_000,
	})
	if b.IsOverCap() {
		t.Errorf("zero spend should not be over-cap")
	}
	b.RecordSpend(1_000_000) // exactly at cap
	if b.IsOverCap() {
		t.Errorf("at-cap should not be IsOverCap (strict >)")
	}
	b.RecordSpend(1)
	if !b.IsOverCap() {
		t.Errorf("over-cap should be true")
	}
}

func TestBudget_PercentSpent_ZeroCapDefensive(t *testing.T) {
	t.Parallel()
	// Hand-set a Budget pointer with zero cap (NewBudget rejects this; defensive).
	b := &ledger.Budget{CapUsdMicros: 0, SpentUsdMicros: 100}
	if got := b.PercentSpent(); got != 0.0 {
		t.Errorf("zero cap pct = %v; want 0.0", got)
	}
}

func TestBudget_RecordSpend_ZeroAmountIsNoop(t *testing.T) {
	t.Parallel()
	b, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: "t1", Period: "2026-05", CapUsdMicros: 1_000_000,
	})
	crossed := b.RecordSpend(0)
	if len(crossed) != 0 {
		t.Errorf("zero amount should not cross thresholds; got %v", crossed)
	}
}
