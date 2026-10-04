// Repository is the AgentDecisionLog persistence port. Append-only.
package decision

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is the canonical sentinel.
var ErrNotFound = errors.New("decision log not found")

// Repository is the persistence port. Append-only.
type Repository interface {
	// Append persists the log. Implementations MUST defensively copy.
	Append(ctx context.Context, l *Log) error

	// List returns logs for the tenant matching the filter, sorted
	// CreatedAt ascending.
	List(ctx context.Context, tenantID string, filter ListFilter) ([]*Log, error)

	// GetByID returns the log with the given ID for the tenant or
	// ErrNotFound. Defensively deep-copies.
	GetByID(ctx context.Context, tenantID, logID string) (*Log, error)

	// Count returns the number of decision logs for the tenant in the
	// half-open window [since, until). Used by the O+ dashboard
	// `recent_decisions_24h` rollup. A zero-row result is a legitimate
	// answer (no recent activity) — query errors are surfaced loudly.
	Count(ctx context.Context, tenantID string, since, until time.Time) (int64, error)
}
