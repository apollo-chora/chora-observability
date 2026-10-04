// Package inmem exercises the BudgetRepository.
package inmem_test

import (
	"context"
	"errors"
	"testing"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/inmem"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/ledger"
)

func TestBudgetRepository_SetThenGet(t *testing.T) {
	t.Parallel()
	r := inmem.NewBudgetRepository()
	b, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: "t1", Period: "2026-05", CapUsdMicros: 1_000_000,
	})
	if err := r.Set(context.Background(), b); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := r.Get(context.Background(), "t1", "2026-05")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.CapUsdMicros != 1_000_000 {
		t.Errorf("cap = %d; want 1000000", got.CapUsdMicros)
	}
}

func TestBudgetRepository_Get_NotFound(t *testing.T) {
	t.Parallel()
	r := inmem.NewBudgetRepository()
	_, err := r.Get(context.Background(), "t1", "missing")
	if !errors.Is(err, ledger.ErrBudgetNotFound) {
		t.Errorf("err = %v; want ErrBudgetNotFound", err)
	}
}

func TestBudgetRepository_TenantIsolation(t *testing.T) {
	t.Parallel()
	r := inmem.NewBudgetRepository()
	b1, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: "t1", Period: "2026-05", CapUsdMicros: 1000,
	})
	_ = r.Set(context.Background(), b1)
	// t2 shouldn't see t1's budget.
	_, err := r.Get(context.Background(), "t2", "2026-05")
	if !errors.Is(err, ledger.ErrBudgetNotFound) {
		t.Errorf("cross-tenant leak; err = %v", err)
	}
}

func TestBudgetRepository_Update(t *testing.T) {
	t.Parallel()
	r := inmem.NewBudgetRepository()
	b, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: "t1", Period: "2026-05", CapUsdMicros: 1_000_000,
	})
	_ = r.Set(context.Background(), b)
	b.RecordSpend(500_000)
	if err := r.Set(context.Background(), b); err != nil {
		t.Fatalf("Set update: %v", err)
	}
	got, _ := r.Get(context.Background(), "t1", "2026-05")
	if got.SpentUsdMicros != 500_000 {
		t.Errorf("spent = %d; want 500000", got.SpentUsdMicros)
	}
}
