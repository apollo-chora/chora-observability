// Repository is the TokenUsageLedger persistence port.
//
// Hexagonal: the domain owns the interface; adapters (in-memory, Cloud SQL
// via pgx, etc.) implement it. The port is APPEND-ONLY by design — there
// is no Update or Delete method.
package ledger

import (
	"context"
	"errors"
)

// ErrNotFound is the canonical sentinel.
var ErrNotFound = errors.New("ledger entry not found")

// Repository is the persistence port. Append-only.
type Repository interface {
	// Append persists the entry. Implementations MUST defensively copy
	// to prevent post-Append external mutation from leaking into storage.
	Append(ctx context.Context, e *Entry) error

	// List returns entries for the tenant matching the filter, sorted
	// RecordedAt ascending.
	List(ctx context.Context, tenantID string, filter ListFilter) ([]*Entry, error)

	// SumCost aggregates cost_usd_micros over entries matching the filter.
	// Returns (total_micros, count, error). Implementations MUST detect
	// int64 overflow.
	SumCost(ctx context.Context, tenantID string, filter ListFilter) (int64, int, error)
}
