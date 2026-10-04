// Package inmem_test exercises the composite analytics in-memory store.
// NewStore composes the event aggregator + cohort registry; the store itself
// is a plain struct with no methods, so the test covers construction and
// the wiring of its public parts.
package inmem_test

import (
	"testing"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/analytics/inmem"
)

func TestNewStore_ComposesAggregatorAndCohorts(t *testing.T) {
	s := inmem.NewStore()
	if s == nil {
		t.Fatal("NewStore returned nil")
	}
	if s.Aggregator == nil {
		t.Error("expected Aggregator to be initialised")
	}
	if s.Cohorts == nil {
		t.Error("expected Cohorts registry to be initialised")
	}
}