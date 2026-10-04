// Package eventsubscriber_test exercises the WithInboxTTL guard rails that
// the existing external suite reaches only through a valid subscriber.
package eventsubscriber_test

import (
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/analytics/eventaggregator"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/analytics/eventsubscriber"
	"github.com/5007-Capstone/chora/libs/chora-go-common/idempotent"
)

func TestWithInboxTTL_GuardRails(t *testing.T) {
	t.Parallel()
	s := eventsubscriber.New(eventaggregator.NewAggregator(), idempotent.NewMemoryStore())
	if s == nil {
		t.Fatal("New returned nil")
	}

	// positive TTL applied
	s2 := s.WithInboxTTL(30 * time.Minute)
	if s2 == nil {
		t.Fatal("WithInboxTTL(positive) must return the subscriber")
	}

	// non-positive TTL leaves it untouched (returns receiver unchanged)
	if s3 := s.WithInboxTTL(0); s3 != s {
		t.Error("WithInboxTTL(0) should return the receiver")
	}
	if s4 := s.WithInboxTTL(-time.Minute); s4 != s {
		t.Error("WithInboxTTL(negative) should return the receiver")
	}

	// nil receiver is a no-op
	var nilSub *eventsubscriber.Subscriber
	if got := nilSub.WithInboxTTL(time.Hour); got != nil {
		t.Error("nil receiver WithInboxTTL should stay nil")
	}
}