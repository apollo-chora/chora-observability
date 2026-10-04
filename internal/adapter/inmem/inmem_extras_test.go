// Package inmem_test covers the remaining in-memory adapter gaps: the
// LedgerHook-facing AccrueSpend port on BudgetLookup, defensive-copy
// semantics of DecisionRepository.GetByID, and the nil-budget defensive
// copy branch that SetTenantBudget exercises.
package inmem_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/apollo-chora/chora-observability/internal/adapter/inmem"
	"github.com/apollo-chora/chora-observability/internal/domain/decision"
	"github.com/apollo-chora/chora-observability/internal/domain/ledger"
)

const (
	extTenant  = "01970000-0000-7000-8000-000000000001"
	extGcid    = "01970000-0000-7000-9000-000000000001"
	extAgid    = "01970000-0000-7000-a000-000000000001"
	extCorrID  = "01970000-0000-7000-b000-000000000001"
	extTraceID = "00000000000000000000000000000001"
	extSpanID  = "0000000000000001"
)

func TestBudgetLookup_AccrueSpend(t *testing.T) {
	repo := inmem.NewBudgetLookup()
	b, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: extTenant, Period: "2026-05", CapUsdMicros: 1_000_000,
	})
	if err := repo.SetTenantBudget(context.Background(), b); err != nil {
		t.Fatalf("SetTenantBudget: %v", err)
	}

	// AccrueSpend through the canonical BudgetAccruer port.
	var accruer ledger.BudgetAccruer = repo
	if err := accruer.AccrueSpend(context.Background(), ledger.AccrueSpendInput{
		TenantID: extTenant, Gcid: extGcid, AgentID: extAgid,
		Period: "2026-05", AmountMicros: 250_000,
	}); err != nil {
		t.Fatalf("AccrueSpend: %v", err)
	}

	got, err := repo.GetTenantBudget(context.Background(), extTenant, "2026-05")
	if err != nil {
		t.Fatalf("GetTenantBudget: %v", err)
	}
	if got.SpentUsdMicros != 250_000 {
		t.Errorf("spent = %d; want 250000", got.SpentUsdMicros)
	}
}

func TestDecisionRepository_GetByID(t *testing.T) {
	repo := inmem.NewDecisionRepository()

	log, err := decision.New(decision.NewParams{
		TenantID: extTenant, Agid: extAgid,
		DecisionType: decision.TypeRoute, RiskTier: decision.TierLow,
		CorrelationID: extCorrID,
		Reasoning:     mustReasoningLog(t),
	})
	if err != nil {
		t.Fatalf("decision.New: %v", err)
	}
	if err := repo.Append(context.Background(), log); err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, err := repo.GetByID(context.Background(), extTenant, log.LogID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.LogID != log.LogID || got.Reasoning == nil {
		t.Fatalf("GetByID returned %+v", got)
	}

	// defensive deep-copy: mutating the returned log must not leak into the
	// stored append-only slice.
	got.Reasoning.LatencyMs = 999999
	got.TenantID = "mutated"
	again, err := repo.GetByID(context.Background(), extTenant, log.LogID)
	if err != nil {
		t.Fatalf("GetByID #2: %v", err)
	}
	if again.TenantID != extTenant || again.Reasoning.LatencyMs == 999999 {
		t.Fatalf("storage leaked mutation: %+v", again)
	}

	// wrong tenant + missing id -> ErrNotFound
	if _, err := repo.GetByID(context.Background(), "other-tenant", log.LogID); !errors.Is(err, decision.ErrNotFound) {
		t.Fatalf("wrong tenant err = %v; want ErrNotFound", err)
	}
	if _, err := repo.GetByID(context.Background(), extTenant, "missing"); !errors.Is(err, decision.ErrNotFound) {
		t.Fatalf("missing id err = %v; want ErrNotFound", err)
	}
}

func mustReasoningLog(t *testing.T) *decision.ReasoningSummary {
	t.Helper()
	rs, err := decision.NewReasoningSummary(decision.ReasoningParams{
		Input: "in", Output: "out", LatencyMs: 10,
	})
	if err != nil {
		t.Fatalf("NewReasoningSummary: %v", err)
	}
	return rs
}

// TestLedgerRepository_FilterAndPaging completes the filter branches of the
// in-memory ledger List/SumCost (window bounds, offset past-end, limit cap).
func TestLedgerRepository_FilterAndPaging(t *testing.T) {
	repo := inmem.NewLedgerRepository()

	base := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 4; i++ {
		e, err := ledger.New(ledger.NewParams{
			TenantID: extTenant, Gcid: extGcid, ModelID: "m",
			CostUsdMicros: int64(100 * (i + 1)), TraceID: extTraceID, SpanID: extSpanID,
			RecordedAt: base.Add(time.Duration(i) * time.Hour),
		})
		if err != nil {
			t.Fatalf("ledger.New: %v", err)
		}
		if err := repo.Append(context.Background(), e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	// window keeps base+1h and base+2h
	windowed, err := repo.List(context.Background(), extTenant, ledger.ListFilter{
		From: base.Add(30 * time.Minute), To: base.Add(2*time.Hour + 30*time.Minute),
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(windowed) != 2 {
		t.Fatalf("window rows = %d; want 2", len(windowed))
	}
	// ascending order
	if windowed[0].RecordedAt.After(windowed[1].RecordedAt) {
		t.Fatal("expected RecordedAt ascending")
	}

	// offset past the end -> empty
	past, _ := repo.List(context.Background(), extTenant, ledger.ListFilter{Offset: 50})
	if len(past) != 0 {
		t.Errorf("offset-past rows = %d; want 0", len(past))
	}
	// offset within range + limit trim
	page, _ := repo.List(context.Background(), extTenant, ledger.ListFilter{Limit: 2, Offset: 1})
	if len(page) != 2 {
		t.Errorf("paged rows = %d; want 2", len(page))
	}
	// oversized limit clamps to 1000
	huge, _ := repo.List(context.Background(), extTenant, ledger.ListFilter{Limit: 5000})
	if len(huge) != 4 {
		t.Errorf("clamped rows = %d; want 4", len(huge))
	}

	// SumCost with window = 200+300
	total, count, err := repo.SumCost(context.Background(), extTenant, ledger.ListFilter{
		From: base.Add(30 * time.Minute), To: base.Add(2*time.Hour + 30*time.Minute),
	})
	if err != nil {
		t.Fatalf("SumCost: %v", err)
	}
	if total != 500 || count != 2 {
		t.Errorf("sum = (%d, %d); want (500, 2)", total, count)
	}
}

// TestLedgerRepository_SumCostOverflow drives the SumCost overflow guard.
func TestLedgerRepository_SumCostOverflow(t *testing.T) {
	repo := inmem.NewLedgerRepository()
	for _, c := range []int64{math.MaxInt64, 1} {
		e, err := ledger.New(ledger.NewParams{
			TenantID: extTenant, Gcid: extGcid, ModelID: "m",
			CostUsdMicros: c, TraceID: extTraceID, SpanID: extSpanID,
		})
		if err != nil {
			t.Fatalf("ledger.New: %v", err)
		}
		if err := repo.Append(context.Background(), e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if _, _, err := repo.SumCost(context.Background(), extTenant, ledger.ListFilter{}); err == nil {
		t.Fatal("expected overflow error from SumCost")
	}
}

// TestDecisionRepository_ListFilters completes the decision List filter
// branches (Agid, window, descending order, offset, limit clamp).
func TestDecisionRepository_ListFilters(t *testing.T) {
	repo := inmem.NewDecisionRepository()

	for i := 0; i < 3; i++ {
		d, err := decision.New(decision.NewParams{
			TenantID: extTenant, Agid: extAgid,
			DecisionType: decision.TypeRoute, RiskTier: decision.TierLow,
			CorrelationID: extCorrID, PromptTokens: int64(100 * (i + 1)),
		})
		if err != nil {
			t.Fatalf("decision.New: %v", err)
		}
		if err := repo.Append(context.Background(), d); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	// Agid filter (extra rows with another agid first)
	other, _ := decision.New(decision.NewParams{
		TenantID: extTenant, Agid: "other-agent",
		DecisionType: decision.TypeRoute, RiskTier: decision.TierLow,
		CorrelationID: extCorrID,
	})
	_ = repo.Append(context.Background(), other)

	byAgid, err := repo.List(context.Background(), extTenant, decision.ListFilter{Agid: extAgid})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(byAgid) != 3 {
		t.Fatalf("agid rows = %d; want 3", len(byAgid))
	}

	// descending orders newest-first (appended last = 'other' is filtered out
	// by Agid, so newest of the original three)
	desc, _ := repo.List(context.Background(), extTenant, decision.ListFilter{Agid: extAgid, Descending: true, Limit: 1})
	if len(desc) != 1 {
		t.Fatalf("desc rows = %d; want 1", len(desc))
	}
	if desc[0].PromptTokens != 300 {
		t.Errorf("desc first prompt_tokens = %d; want 300 (newest)", desc[0].PromptTokens)
	}

	// offset within range
	page, _ := repo.List(context.Background(), extTenant, decision.ListFilter{Agid: extAgid, Offset: 1, Limit: 1})
	if len(page) != 1 || page[0].PromptTokens != 200 {
		t.Errorf("paged = %+v; want the 200-token row", page)
	}

	// oversized limit clamps
	huge, _ := repo.List(context.Background(), extTenant, decision.ListFilter{Agid: extAgid, Limit: 5000})
	if len(huge) != 3 {
		t.Errorf("clamped rows = %d; want 3", len(huge))
	}

	// window bounds: a From in the future excludes everything, a To in the
	// past excludes everything — exercises both filter continues.
	future, _ := repo.List(context.Background(), extTenant, decision.ListFilter{
		From: time.Now().Add(24 * time.Hour),
	})
	if len(future) != 0 {
		t.Errorf("future-window rows = %d; want 0", len(future))
	}
	past, _ := repo.List(context.Background(), extTenant, decision.ListFilter{
		To: time.Now().Add(-24 * time.Hour),
	})
	if len(past) != 0 {
		t.Errorf("past-window rows = %d; want 0", len(past))
	}
}
