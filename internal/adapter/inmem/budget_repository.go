package inmem

import (
	"context"
	"sync"

	"github.com/apollo-chora/chora-observability/internal/domain/ledger"
)

// BudgetRepository is a goroutine-safe map-backed repository, keyed by
// (tenantID + period). Implements ledger.BudgetRepository.
type BudgetRepository struct {
	mu    sync.RWMutex
	store map[string]*ledger.Budget
}

// NewBudgetRepository constructs an initialised repository.
func NewBudgetRepository() *BudgetRepository {
	return &BudgetRepository{store: make(map[string]*ledger.Budget)}
}

func budgetKey(tenantID, period string) string {
	return tenantID + "::" + period
}

// Set persists or replaces the budget for (tenant_id, period). Defensively
// deep-copies the threshold-crossings slice.
func (r *BudgetRepository) Set(_ context.Context, b *ledger.Budget) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	clone := *b
	clone.ThresholdsCrossed = append([]ledger.BudgetThreshold(nil), b.ThresholdsCrossed...)
	r.store[budgetKey(b.TenantID, b.Period)] = &clone
	return nil
}

// Get returns the budget for (tenant_id, period) or ledger.ErrBudgetNotFound.
// Returned pointer is a defensive deep-copy.
func (r *BudgetRepository) Get(_ context.Context, tenantID, period string) (*ledger.Budget, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	b, ok := r.store[budgetKey(tenantID, period)]
	if !ok {
		return nil, ledger.ErrBudgetNotFound
	}
	clone := *b
	clone.ThresholdsCrossed = append([]ledger.BudgetThreshold(nil), b.ThresholdsCrossed...)
	return &clone, nil
}
