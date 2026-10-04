// Repository is the TraceCorrelation persistence port.
package correlation

import (
	"context"
	"errors"
)

// ErrNotFound is the canonical sentinel.
var ErrNotFound = errors.New("correlation not found")

// Repository is the persistence port.
type Repository interface {
	// Register persists the correlation. Implementations MUST defensively
	// copy.
	Register(ctx context.Context, c *Correlation) error

	// Get returns the correlation by (tenantID, correlationID).
	Get(ctx context.Context, tenantID, correlationID string) (*Correlation, error)
}
