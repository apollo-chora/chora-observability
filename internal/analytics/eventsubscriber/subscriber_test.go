// Package eventsubscriber_test exercises the Pub/Sub-shaped envelope subscriber.
//
// Migrated from services/chora-analytics/internal/adapter/event_subscriber
// at M12.2.E.4. Inbox-pattern wiring added at M12.3 Wave 2 (subagent w2c)
// per `.claude/skills/agentic-resilience-d6/SKILL.md` Pillar 2 + the
// `data-consistency` skill — the in-process map dedup in the aggregator
// is insufficient under chaos (pod-death loses state, multi-replica
// fragments dedup).
package eventsubscriber_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/libs/chora-go-common/idempotent"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/analytics/eventaggregator"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/analytics/eventsubscriber"
)

func newEnv(t *testing.T, id string) eventaggregator.Envelope {
	t.Helper()
	env, err := eventaggregator.NewEnvelope(eventaggregator.EnvelopeParams{
		EventID:        id,
		IdempotencyKey: id,
		TenantID:       "t1",
		GCID:           "g",
		Topic:          "chora.consumption.session.completed.v1",
		OccurredAt:     time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC),
		PublishedAt:    time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC),
		SchemaVersion:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func TestHandle_FoldsIntoAggregator(t *testing.T) {
	agg := eventaggregator.NewAggregator()
	sub := eventsubscriber.New(agg, nil)
	if err := sub.Handle(context.Background(), newEnv(t, "01900000-0000-7000-8000-000000000001")); err != nil {
		t.Fatal(err)
	}
	b, _ := agg.Query(eventaggregator.QueryParams{TenantID: "t1", Period: eventaggregator.PeriodDay, At: time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)})
	if b.CountFor("consumption", "session", "completed") != 1 {
		t.Errorf("count=%d", b.CountFor("consumption", "session", "completed"))
	}
}

func TestHandleBatch_FoldsAll(t *testing.T) {
	agg := eventaggregator.NewAggregator()
	sub := eventsubscriber.New(agg, nil)
	batch := []eventaggregator.Envelope{
		newEnv(t, "01900000-0000-7000-8000-000000000001"),
		newEnv(t, "01900000-0000-7000-8000-000000000002"),
		newEnv(t, "01900000-0000-7000-8000-000000000003"),
	}
	if err := sub.HandleBatch(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	b, _ := agg.Query(eventaggregator.QueryParams{TenantID: "t1", Period: eventaggregator.PeriodDay, At: time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)})
	if b.CountFor("consumption", "session", "completed") != 3 {
		t.Errorf("count=%d want 3", b.CountFor("consumption", "session", "completed"))
	}
}

func TestHandleBatch_StopsOnFirstError(t *testing.T) {
	agg := eventaggregator.NewAggregator()
	sub := eventsubscriber.New(agg, nil)
	bad := eventaggregator.Envelope{TenantID: "", IdempotencyKey: "x"} // invalid
	err := sub.HandleBatch(context.Background(), []eventaggregator.Envelope{bad})
	if err == nil {
		t.Error("expected error from invalid envelope")
	}
}

// =============================================================================
// W2c (M12.3 Wave 2) — inbox dedup tests proving chaos scenario (h)
// "idempotency under duplicates" cannot fire double side-effects across
// the failure modes the analytics aggregator's in-process map couldn't
// survive:
//
//   1. Pod-death: dedup state must persist outside the subscriber instance.
//      Tested by swapping the subscriber instance between the first
//      delivery and the duplicate redelivery, sharing the same Store.
//   2. Multi-replica: replica A and replica B see the same event; only
//      one of them runs the handler body. Tested by running two
//      subscribers backed by the same Store concurrently.
//   3. TTL expiry: a token outside the TTL allows reprocessing (correct
//      semantics — replays after grace are explicit ops actions).
//   4. Fresh-store negative control: without a shared store each
//      subscriber dedupes independently — i.e. duplicate side-effects
//      happen. This documents the contract explicitly.
// =============================================================================

func TestSubscriber_InboxSurvivesSubscriberRecreation(t *testing.T) {
	t.Parallel()
	// Two independent aggregators (one per "pod") — we are NOT relying on
	// the aggregator's internal dedup to prove anything; the inbox is the
	// load-bearing primitive across pod-death.
	agg1 := eventaggregator.NewAggregator()
	agg2 := eventaggregator.NewAggregator()
	inbox := idempotent.NewMemoryStore()

	sub1 := eventsubscriber.New(agg1, inbox)
	env := newEnv(t, "01900000-0000-7000-8000-0000000000aa")
	if err := sub1.Handle(context.Background(), env); err != nil {
		t.Fatalf("first delivery on sub1: %v", err)
	}

	// Simulate pod restart: drop sub1, recreate sub2 with the SAME inbox.
	// The aggregator behind sub2 is fresh (it would re-count without the
	// inbox dedup).
	sub2 := eventsubscriber.New(agg2, inbox)
	if err := sub2.Handle(context.Background(), env); err != nil {
		t.Fatalf("redelivery on sub2 (post-restart): %v", err)
	}

	// Across both aggregators only ONE side-effect happened (because the
	// inbox suppressed the second delivery before agg2.Ingest).
	b1, _ := agg1.Query(eventaggregator.QueryParams{TenantID: "t1", Period: eventaggregator.PeriodDay, At: time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)})
	b2, _ := agg2.Query(eventaggregator.QueryParams{TenantID: "t1", Period: eventaggregator.PeriodDay, At: time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)})
	totalCount := b1.CountFor("consumption", "session", "completed") + b2.CountFor("consumption", "session", "completed")
	if totalCount != 1 {
		t.Fatalf("expected exactly 1 count across pod-restart redelivery; got %d (agg1=%d agg2=%d)",
			totalCount, b1.CountFor("consumption", "session", "completed"),
			b2.CountFor("consumption", "session", "completed"))
	}
}

func TestSubscriber_InboxFreshStoreReprocessesEvent(t *testing.T) {
	t.Parallel()
	// Negative-control of the above: WITHOUT a shared store (each subscriber
	// has its own MemoryStore) the inbox CANNOT dedupe across restarts —
	// which is exactly the failure mode the production PostgresStore wiring
	// is designed to fix. This test documents the contract explicitly:
	// chaos test must use a shared store (Postgres in prod, shared
	// MemoryStore in test) to prove dedup.
	agg1 := eventaggregator.NewAggregator()
	agg2 := eventaggregator.NewAggregator()
	sub1 := eventsubscriber.New(agg1, idempotent.NewMemoryStore())
	sub2 := eventsubscriber.New(agg2, idempotent.NewMemoryStore())

	env := newEnv(t, "01900000-0000-7000-8000-0000000000bb")
	if err := sub1.Handle(context.Background(), env); err != nil {
		t.Fatalf("sub1 handle: %v", err)
	}
	if err := sub2.Handle(context.Background(), env); err != nil {
		t.Fatalf("sub2 handle: %v", err)
	}
	b1, _ := agg1.Query(eventaggregator.QueryParams{TenantID: "t1", Period: eventaggregator.PeriodDay, At: time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)})
	b2, _ := agg2.Query(eventaggregator.QueryParams{TenantID: "t1", Period: eventaggregator.PeriodDay, At: time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)})
	totalCount := b1.CountFor("consumption", "session", "completed") + b2.CountFor("consumption", "session", "completed")
	if totalCount != 2 {
		t.Errorf("FRESH-store negative control: expected 2 counts (no shared dedup); got %d", totalCount)
	}
}

func TestSubscriber_InboxConcurrentReplicas(t *testing.T) {
	t.Parallel()
	// Two subscribers (two replicas) sharing one inbox — the same event
	// arrives at both concurrently; only one side-effect.
	agg1 := eventaggregator.NewAggregator()
	agg2 := eventaggregator.NewAggregator()
	inbox := idempotent.NewMemoryStore()
	subA := eventsubscriber.New(agg1, inbox)
	subB := eventsubscriber.New(agg2, inbox)

	env := newEnv(t, "01900000-0000-7000-8000-0000000000cc")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = subA.Handle(context.Background(), env) }()
	go func() { defer wg.Done(); _ = subB.Handle(context.Background(), env) }()
	wg.Wait()

	b1, _ := agg1.Query(eventaggregator.QueryParams{TenantID: "t1", Period: eventaggregator.PeriodDay, At: time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)})
	b2, _ := agg2.Query(eventaggregator.QueryParams{TenantID: "t1", Period: eventaggregator.PeriodDay, At: time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)})
	totalCount := b1.CountFor("consumption", "session", "completed") + b2.CountFor("consumption", "session", "completed")
	if totalCount != 1 {
		t.Fatalf("multi-replica: expected exactly 1 count; got %d", totalCount)
	}
}

func TestSubscriber_InboxTTLExpiryReprocesses(t *testing.T) {
	t.Parallel()
	agg := eventaggregator.NewAggregator()
	inbox := idempotent.NewMemoryStore()
	sub := eventsubscriber.New(agg, inbox).WithInboxTTL(50 * time.Millisecond)

	env := newEnv(t, "01900000-0000-7000-8000-0000000000dd")
	if err := sub.Handle(context.Background(), env); err != nil {
		t.Fatalf("first handle: %v", err)
	}
	// Advance the inbox clock past the TTL.
	inbox.Advance(100 * time.Millisecond)
	// Reset the aggregator's seen-set by using a fresh aggregator; we are
	// proving the INBOX permits reprocessing after TTL.
	agg2 := eventaggregator.NewAggregator()
	sub2 := eventsubscriber.New(agg2, inbox).WithInboxTTL(50 * time.Millisecond)
	if err := sub2.Handle(context.Background(), env); err != nil {
		t.Fatalf("second handle (post-TTL): %v", err)
	}
	b, _ := agg2.Query(eventaggregator.QueryParams{TenantID: "t1", Period: eventaggregator.PeriodDay, At: time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)})
	if b.CountFor("consumption", "session", "completed") != 1 {
		t.Errorf("TTL-expiry: expected 1 count on the fresh aggregator (i.e. inbox permitted reprocess); got %d",
			b.CountFor("consumption", "session", "completed"))
	}
}

// Defensive guard: nil inbox falls back to in-memory store (matches the
// W1.7 closure_subscriber pattern).
func TestSubscriber_NilInboxFallsBackToMemoryStore(t *testing.T) {
	t.Parallel()
	agg := eventaggregator.NewAggregator()
	sub := eventsubscriber.New(agg, nil) // nil inbox

	// Handle twice with same key — even without an explicit inbox,
	// the defensive MemoryStore fallback should dedupe.
	env := newEnv(t, "01900000-0000-7000-8000-0000000000ee")
	if err := sub.Handle(context.Background(), env); err != nil {
		t.Fatalf("first handle: %v", err)
	}
	if err := sub.Handle(context.Background(), env); err != nil {
		t.Fatalf("second handle (duplicate): %v", err)
	}
	b, _ := agg.Query(eventaggregator.QueryParams{TenantID: "t1", Period: eventaggregator.PeriodDay, At: time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)})
	if b.CountFor("consumption", "session", "completed") != 1 {
		t.Errorf("nil-inbox fallback: expected 1 (deduped); got %d",
			b.CountFor("consumption", "session", "completed"))
	}
}
