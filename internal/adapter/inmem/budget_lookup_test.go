// Package inmem_test exercises the BudgetLookup adapter (3-level cascade
// per ai-cost-tracking skill).
package inmem_test

import (
	"context"
	"errors"
	"testing"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/inmem"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/ledger"
)

func TestBudgetLookup_PerTenant(t *testing.T) {
	t.Parallel()
	repo := inmem.NewBudgetLookup()
	b, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: "t1", Period: "2026-05", CapUsdMicros: 1_000_000,
	})
	if err := repo.SetTenantBudget(context.Background(), b); err != nil {
		t.Fatalf("SetTenantBudget: %v", err)
	}
	got, err := repo.GetTenantBudget(context.Background(), "t1", "2026-05")
	if err != nil {
		t.Fatalf("GetTenantBudget: %v", err)
	}
	if got.CapUsdMicros != 1_000_000 {
		t.Errorf("cap = %d", got.CapUsdMicros)
	}
}

func TestBudgetLookup_PerUser(t *testing.T) {
	t.Parallel()
	repo := inmem.NewBudgetLookup()
	b, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: "t1", Period: "2026-05", CapUsdMicros: 50_000,
	})
	if err := repo.SetUserBudget(context.Background(), "g1", b); err != nil {
		t.Fatalf("SetUserBudget: %v", err)
	}
	got, err := repo.GetUserBudget(context.Background(), "t1", "g1", "2026-05")
	if err != nil {
		t.Fatalf("GetUserBudget: %v", err)
	}
	if got.CapUsdMicros != 50_000 {
		t.Errorf("cap = %d", got.CapUsdMicros)
	}
}

func TestBudgetLookup_PerAgent(t *testing.T) {
	t.Parallel()
	repo := inmem.NewBudgetLookup()
	b, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: "t1", Period: "2026-05", CapUsdMicros: 1_000,
	})
	if err := repo.SetAgentBudget(context.Background(), "agent-x", b); err != nil {
		t.Fatalf("SetAgentBudget: %v", err)
	}
	got, err := repo.GetAgentBudget(context.Background(), "t1", "agent-x", "2026-05")
	if err != nil {
		t.Fatalf("GetAgentBudget: %v", err)
	}
	if got.CapUsdMicros != 1_000 {
		t.Errorf("cap = %d", got.CapUsdMicros)
	}
}

func TestBudgetLookup_NotFoundIsErrBudgetNotFound(t *testing.T) {
	t.Parallel()
	repo := inmem.NewBudgetLookup()
	if _, err := repo.GetTenantBudget(context.Background(), "t-none", "2026-05"); !errors.Is(err, ledger.ErrBudgetNotFound) {
		t.Errorf("tenant not-found = %v", err)
	}
	if _, err := repo.GetUserBudget(context.Background(), "t1", "g-none", "2026-05"); !errors.Is(err, ledger.ErrBudgetNotFound) {
		t.Errorf("user not-found = %v", err)
	}
	if _, err := repo.GetAgentBudget(context.Background(), "t1", "agent-none", "2026-05"); !errors.Is(err, ledger.ErrBudgetNotFound) {
		t.Errorf("agent not-found = %v", err)
	}
}

func TestBudgetLookup_TenantIsolation(t *testing.T) {
	t.Parallel()
	repo := inmem.NewBudgetLookup()
	b, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: "t1", Period: "2026-05", CapUsdMicros: 1_000_000,
	})
	_ = repo.SetTenantBudget(context.Background(), b)
	if _, err := repo.GetTenantBudget(context.Background(), "t2", "2026-05"); !errors.Is(err, ledger.ErrBudgetNotFound) {
		t.Errorf("cross-tenant leak; err = %v", err)
	}
}

func TestBudgetLookup_DeepCopyOnGet(t *testing.T) {
	t.Parallel()
	repo := inmem.NewBudgetLookup()
	b, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: "t1", Period: "2026-05", CapUsdMicros: 1_000_000,
	})
	_ = repo.SetTenantBudget(context.Background(), b)
	got, _ := repo.GetTenantBudget(context.Background(), "t1", "2026-05")
	got.RecordSpend(500_000)
	got2, _ := repo.GetTenantBudget(context.Background(), "t1", "2026-05")
	if got2.SpentUsdMicros != 0 {
		t.Errorf("Get returned shared pointer (mutation leaked); spent = %d", got2.SpentUsdMicros)
	}
}

func TestBudgetLookup_RecordSpend_AllLevels(t *testing.T) {
	t.Parallel()
	// RecordSpend should atomically apply spend to ALL configured levels.
	repo := inmem.NewBudgetLookup()
	tb, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: "t1", Period: "2026-05", CapUsdMicros: 1_000_000,
	})
	ub, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: "t1", Period: "2026-05", CapUsdMicros: 50_000,
	})
	ab, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: "t1", Period: "2026-05", CapUsdMicros: 1_000,
	})
	_ = repo.SetTenantBudget(context.Background(), tb)
	_ = repo.SetUserBudget(context.Background(), "g1", ub)
	_ = repo.SetAgentBudget(context.Background(), "agent-x", ab)

	if err := repo.RecordSpend(context.Background(), inmem.RecordSpendInput{
		TenantID: "t1", Gcid: "g1", AgentID: "agent-x", Period: "2026-05",
		AmountMicros: 100,
	}); err != nil {
		t.Fatalf("RecordSpend: %v", err)
	}

	tb2, _ := repo.GetTenantBudget(context.Background(), "t1", "2026-05")
	if tb2.SpentUsdMicros != 100 {
		t.Errorf("tenant spend = %d; want 100", tb2.SpentUsdMicros)
	}
	ub2, _ := repo.GetUserBudget(context.Background(), "t1", "g1", "2026-05")
	if ub2.SpentUsdMicros != 100 {
		t.Errorf("user spend = %d; want 100", ub2.SpentUsdMicros)
	}
	ab2, _ := repo.GetAgentBudget(context.Background(), "t1", "agent-x", "2026-05")
	if ab2.SpentUsdMicros != 100 {
		t.Errorf("agent spend = %d; want 100", ab2.SpentUsdMicros)
	}
}

func TestBudgetLookup_RecordSpend_SkipsMissingLevels(t *testing.T) {
	t.Parallel()
	// Only tenant level configured — RecordSpend must not error on
	// missing user/agent levels.
	repo := inmem.NewBudgetLookup()
	tb, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: "t1", Period: "2026-05", CapUsdMicros: 1_000_000,
	})
	_ = repo.SetTenantBudget(context.Background(), tb)
	if err := repo.RecordSpend(context.Background(), inmem.RecordSpendInput{
		TenantID: "t1", Gcid: "g1", AgentID: "agent-x", Period: "2026-05",
		AmountMicros: 100,
	}); err != nil {
		t.Errorf("RecordSpend should ignore missing levels; got %v", err)
	}
}
