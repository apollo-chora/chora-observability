// Package inmem holds the composite in-memory store for the analytics
// slice of chora-observability (consolidated at M12.2.E.4 from chora-analytics).
//
// The analytics slice holds NO durable state on its own — every derived
// view is rebuildable by replaying the Pub/Sub event log into the
// in-memory aggregator. M12+ swaps the in-memory aggregator for a
// Postgres materialised view (chora_observability schema); this package
// goes away then.
package inmem

import (
	"sync"

	"github.com/apollo-chora/chora-observability/internal/analytics/cohort"
	"github.com/apollo-chora/chora-observability/internal/analytics/eventaggregator"
)

// Store is the composed in-memory store the HTTP adapter consumes.
type Store struct {
	Aggregator *eventaggregator.Aggregator
	Cohorts    *cohort.Registry

	mu        sync.RWMutex
	dashCache map[string]any // optional: caller-managed dashboard cache
}

// NewStore constructs a fresh Store.
func NewStore() *Store {
	return &Store{
		Aggregator: eventaggregator.NewAggregator(),
		Cohorts:    cohort.NewRegistry(),
		dashCache:  make(map[string]any),
	}
}
