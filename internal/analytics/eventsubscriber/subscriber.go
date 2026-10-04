// Package eventsubscriber is the Pub/Sub-shaped event subscriber for the
// analytics slice of chora-observability (consolidated at M12.2.E.4 from
// chora-analytics).
//
// The skeleton is intentionally Pub/Sub-shaped (Envelope-driven, retries
// folded into the aggregator's idempotency check) so that swapping the
// adapter from in-memory to a real Pub/Sub PullSubscriber at M12+ is a
// drop-in replacement — the domain code does not move.
//
// READ-ONLY guarantee: this subscriber NEVER republishes. Per the brief
// constraint "read-only on event ingestion (don't republish)" — Analytics
// is a pure leaf consumer in the chora event graph.
//
// W2c (M12.3 Wave 2, 2026-05-12): inbox dedup wired via
// `libs/chora-go-common/idempotent.Store`. The aggregator's internal
// `seen map[string]struct{}` was insufficient under chaos (pod-death
// loses state, multi-replica fragments dedup, no TTL bound). The inbox
// is the canonical Pillar 2 (consumer-side dual of the outbox) per
// `.claude/skills/agentic-resilience-d6/SKILL.md`.
package eventsubscriber

import (
	"context"
	"sync"
	"time"

	"github.com/5007-Capstone/chora/libs/chora-go-common/idempotent"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/analytics/eventaggregator"
)

// InboxTTL is the dedupe-key retention window for the analytics subscriber.
// 24h covers Pub/Sub's max redelivery window comfortably while keeping
// the idempotency_keys table small. Production may tune via WithInboxTTL.
const InboxTTL = 24 * time.Hour

// Subscriber consumes envelopes and folds them into the Aggregator.
//
// Inbox dedupe (W2c): the per-event dedupe key lives in a
// chora-go-common/idempotent.Store. Production wires PostgresStore
// against the chora_observability database's idempotency_keys table;
// dev / tests use MemoryStore. The store survives pod-death and is
// shared across replicas, which the previous in-process aggregator
// seen-map did NOT.
type Subscriber struct {
	mu    sync.Mutex
	agg   *eventaggregator.Aggregator
	inbox idempotent.Store
	ttl   time.Duration
}

// New wires a new Subscriber against the Aggregator + inbox.
//
// inbox is REQUIRED — bootstrap supplies idempotent.PostgresStore in
// production and idempotent.MemoryStore in dev / tests. Passing nil
// triggers a defensive MemoryStore fallback; this preserves test
// fixtures that pre-date the inbox refactor.
func New(agg *eventaggregator.Aggregator, inbox idempotent.Store) *Subscriber {
	if inbox == nil {
		inbox = idempotent.NewMemoryStore()
	}
	return &Subscriber{
		agg:   agg,
		inbox: inbox,
		ttl:   InboxTTL,
	}
}

// WithInboxTTL overrides the default dedupe-key retention window. Mostly
// useful in tests that want a short TTL to exercise expiry / reprocess.
func (s *Subscriber) WithInboxTTL(ttl time.Duration) *Subscriber {
	if s == nil || ttl <= 0 {
		return s
	}
	s.ttl = ttl
	return s
}

// Handle ingests one envelope. Designed for direct call (in-memory) and
// for future wiring to Pub/Sub Pull (PullSubscriber.Receive(...) wraps a
// callback per message → Handle).
//
// Idempotency is enforced by inbox.Process(...) keyed by
// (tenant_id :: idempotency_key). A duplicate Pub/Sub delivery (pod
// restart, multi-replica race, retry-after-ack-window) hits the same key
// + skips the handler body, returning nil. The dedup token lives for
// InboxTTL in the domain's idempotency_keys table.
//
// Errors propagate so a real Pub/Sub adapter can NACK + retry on
// transient failures; the aggregator's own idempotency check + the
// inbox both ensure retries never inflate counts.
func (s *Subscriber) Handle(ctx context.Context, env eventaggregator.Envelope) error {
	// Pre-validate so the inbox doesn't dedupe garbage. agg.Ingest also
	// validates but we want bad envelopes to surface as errors BEFORE the
	// inbox claim so the test "stop-on-first-error" semantics are
	// preserved.
	if env.TenantID == "" || env.IdempotencyKey == "" {
		// Direct call — let the aggregator raise the canonical validation
		// error so callers see the same shape they saw pre-inbox.
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.agg.Ingest(env)
	}

	key := inboxKey(env)
	return s.inbox.Process(ctx, key, s.ttl, func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.agg.Ingest(env)
	})
}

// HandleBatch ingests a slice of envelopes; returns the first error.
func (s *Subscriber) HandleBatch(ctx context.Context, batch []eventaggregator.Envelope) error {
	for _, e := range batch {
		if err := s.Handle(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

// inboxKey builds the deterministic dedupe key for an analytics envelope.
//
// Composition: "analytics:{tenant_id}:{idempotency_key}"
//
//   - "analytics:" prefix scopes the key namespace (other subscribers in
//     the same DB use different prefixes — e.g. "closure:" for
//     closure_subscriber).
//   - tenant_id is folded in so cross-tenant collisions are impossible
//     (D6.3 multi-tenant isolation contract).
//   - idempotency_key is the producer-supplied dedupe token. The
//     envelope is constructed via NewEnvelope which rejects empty keys.
func inboxKey(env eventaggregator.Envelope) string {
	return "analytics:" + env.TenantID + ":" + env.IdempotencyKey
}
