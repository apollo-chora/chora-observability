// Package inmem provides in-memory implementations of the Observability
// domain repositories. Used for tests and the M10 skeleton; Cloud SQL is
// deferred to Tier 2.
package inmem

import (
	"context"
	"sort"
	"sync"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/ledger"
)

// LedgerRepository is a goroutine-safe append-only repository.
type LedgerRepository struct {
	mu      sync.RWMutex
	entries []*ledger.Entry // append-only slice
}

// NewLedgerRepository constructs an initialised repository.
func NewLedgerRepository() *LedgerRepository {
	return &LedgerRepository{entries: make([]*ledger.Entry, 0)}
}

// Append persists the entry. Defensively deep-copies to prevent post-Append
// external mutation from leaking into storage.
func (r *LedgerRepository) Append(_ context.Context, e *ledger.Entry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	clone := *e
	r.entries = append(r.entries, &clone)
	return nil
}

// List returns entries for the tenant matching the filter, sorted RecordedAt
// ascending. Defensively deep-copies the result.
func (r *LedgerRepository) List(_ context.Context, tenantID string, f ledger.ListFilter) ([]*ledger.Entry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]*ledger.Entry, 0)
	for _, e := range r.entries {
		if e.TenantID != tenantID {
			continue
		}
		if !f.From.IsZero() && e.RecordedAt.Before(f.From) {
			continue
		}
		if !f.To.IsZero() && !e.RecordedAt.Before(f.To) {
			continue
		}
		clone := *e
		out = append(out, &clone)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].RecordedAt.Before(out[j].RecordedAt)
	})

	if f.Offset > 0 && f.Offset < len(out) {
		out = out[f.Offset:]
	} else if f.Offset >= len(out) {
		out = nil
	}
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// SumCost aggregates cost_usd_micros over entries matching the filter,
// using the domain's overflow-safe SumCost helper.
func (r *LedgerRepository) SumCost(ctx context.Context, tenantID string, f ledger.ListFilter) (int64, int, error) {
	// We DON'T use the f.Limit/Offset here — cost aggregation is over the
	// full filtered set. Force a no-paging filter copy.
	full := f
	full.Limit = 0
	full.Offset = 0

	r.mu.RLock()
	defer r.mu.RUnlock()
	costs := make([]int64, 0)
	count := 0
	for _, e := range r.entries {
		if e.TenantID != tenantID {
			continue
		}
		if !full.From.IsZero() && e.RecordedAt.Before(full.From) {
			continue
		}
		if !full.To.IsZero() && !e.RecordedAt.Before(full.To) {
			continue
		}
		costs = append(costs, e.CostUsdMicros)
		count++
	}
	total, err := ledger.SumCost(costs)
	if err != nil {
		return 0, count, err
	}
	_ = ctx
	return total, count, nil
}
