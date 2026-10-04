package inmem

import (
	"context"
	"sync"

	"github.com/apollo-chora/chora-observability/internal/domain/correlation"
)

// CorrelationRepository is a goroutine-safe map-backed repository, keyed
// by (tenantID + correlationID).
type CorrelationRepository struct {
	mu    sync.RWMutex
	store map[string]*correlation.Correlation
}

// NewCorrelationRepository constructs an initialised repository.
func NewCorrelationRepository() *CorrelationRepository {
	return &CorrelationRepository{store: make(map[string]*correlation.Correlation)}
}

func key(tenantID, corrID string) string {
	return tenantID + "::" + corrID
}

// Register persists the correlation with defensive deep-copy.
func (r *CorrelationRepository) Register(_ context.Context, c *correlation.Correlation) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	clone := *c
	clone.ChildSpans = append([]string(nil), c.ChildSpans...)
	r.store[key(c.TenantID, c.CorrelationID)] = &clone
	return nil
}

// Get returns the correlation for (tenantID, correlationID) or ErrNotFound.
func (r *CorrelationRepository) Get(_ context.Context, tenantID, correlationID string) (*correlation.Correlation, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.store[key(tenantID, correlationID)]
	if !ok {
		return nil, correlation.ErrNotFound
	}
	clone := *c
	clone.ChildSpans = append([]string(nil), c.ChildSpans...)
	return &clone, nil
}
