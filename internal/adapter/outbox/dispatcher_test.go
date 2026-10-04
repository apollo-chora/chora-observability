// Package outbox_test — Dispatcher drain-loop tests for chora-observability.
//
// The Dispatcher drains outbox_events rows to Cloud Pub/Sub. It composes
// Store.FetchPending + Bus.Publish + Store.MarkPublished / MarkFailed /
// Deadletter. On max-attempts exhaustion the row lands in
// outbox_dead_letters AND a Pub/Sub-side DLQ subscription (configured in
// Terraform — see m10-pubsub-dlq).
//
// Per `feedback_d6_resilience_first_class` B.6.2.a — dispatcher is the
// retry + DLQ ladder for the producer-side outbox. Subscriber-side
// ack-after-processing is a separate concern (B.6.2.b).
package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	cgcenvelope "github.com/5007-Capstone/chora/libs/chora-go-common/envelope"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/outbox"
)

type recordingBus struct {
	mu      sync.Mutex
	calls   []recordedPub
	fails   int
	failErr error
}

type recordedPub struct {
	Topic    string
	Envelope cgcenvelope.Envelope
	Payload  []byte
}

func (b *recordingBus) Publish(_ context.Context, topic string, env cgcenvelope.Envelope, payload []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fails > 0 {
		b.fails--
		return b.failErr
	}
	cp := append([]byte(nil), payload...)
	b.calls = append(b.calls, recordedPub{Topic: topic, Envelope: env, Payload: cp})
	return nil
}

func (b *recordingBus) callCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.calls)
}

func insertRow(t *testing.T, store *outbox.InMemoryStore, id, tenant string) {
	t.Helper()
	now := time.Now().UTC()
	envelope := map[string]string{
		"event_id":        id,
		"idempotency_key": "idem-" + id,
		"tenant_id":       tenant,
		"occurred_at":     now.Format(time.RFC3339Nano),
		"published_at":    now.Format(time.RFC3339Nano),
		"traceparent":     "00-deadbeefdeadbeefdeadbeefdeadbeef-1111111122222222-01",
		"source_project":  "chora-489812",
		"source_service":  "chora-observability",
		"schema_version":  "1",
	}
	r := outbox.Row{
		ID:             id,
		TenantID:       tenant,
		GCID:           "00000000-0000-0000-0000-000000000001",
		AgentID:        "",
		EventType:      "observability.token_usage.recorded",
		Topic:          "chora.observability.token_usage.recorded.v1",
		Payload:        []byte(`{"id":"` + id + `"}`),
		Envelope:       envelope,
		IdempotencyKey: "idem-" + id,
		OccurredAt:     now,
	}
	if err := store.Insert(context.Background(), r); err != nil {
		t.Fatalf("Insert: %v", err)
	}
}

func TestDispatcher_DrainOnce_PublishesAllAndMarksPublished(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	insertRow(t, store, "r1", "t1")
	insertRow(t, store, "r2", "t1")
	insertRow(t, store, "r3", "t2")

	bus := &recordingBus{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store:       store,
		Bus:         bus,
		WorkerID:    "w1",
		MaxAttempts: 5,
	})
	n, err := d.DrainOnce(context.Background(), 10)
	if err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
	if n != 3 {
		t.Errorf("publish count = %d; want 3", n)
	}
	if bus.callCount() != 3 {
		t.Errorf("bus.Publish calls = %d; want 3", bus.callCount())
	}
	rows, _ := store.FetchPending(context.Background(), 10)
	if len(rows) != 0 {
		t.Errorf("pending after drain = %d; want 0", len(rows))
	}
	if len(store.Published()) != 3 {
		t.Errorf("Published count = %d; want 3", len(store.Published()))
	}
}

func TestDispatcher_DrainOnce_TransientFailure_IncrementsRetryAndRetainsPending(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	insertRow(t, store, "rA", "t1")
	bus := &recordingBus{fails: 1, failErr: errors.New("pubsub blip")}

	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store:       store,
		Bus:         bus,
		WorkerID:    "w1",
		MaxAttempts: 3,
	})
	n, _ := d.DrainOnce(context.Background(), 10)
	if n != 0 {
		t.Errorf("first drain publish count = %d; want 0 (transient fail)", n)
	}
	rows, _ := store.FetchPending(context.Background(), 10)
	if len(rows) != 1 {
		t.Fatalf("pending after transient fail = %d; want 1", len(rows))
	}
	if rows[0].RetryCount != 1 {
		t.Errorf("retry_count = %d; want 1", rows[0].RetryCount)
	}
	n2, _ := d.DrainOnce(context.Background(), 10)
	if n2 != 1 {
		t.Errorf("second drain publish count = %d; want 1", n2)
	}
}

func TestDispatcher_DrainOnce_MaxAttemptsExhausted_DeadletterAndRemoveFromPending(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	insertRow(t, store, "rDL", "t1")

	persistentFail := &recordingBus{fails: 10, failErr: errors.New("permanent")}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store:       store,
		Bus:         persistentFail,
		WorkerID:    "w-dlq",
		MaxAttempts: 3,
	})
	for i := 0; i < 3; i++ {
		if _, err := d.DrainOnce(context.Background(), 10); err != nil {
			t.Fatalf("DrainOnce pass %d: %v", i, err)
		}
	}
	rows, _ := store.FetchPending(context.Background(), 10)
	if len(rows) != 0 {
		t.Errorf("pending after deadletter = %d; want 0", len(rows))
	}
	dl := store.DeadLetters()
	if len(dl) != 1 {
		t.Fatalf("DeadLetters count = %d; want 1", len(dl))
	}
	if dl[0].RowID != "rDL" {
		t.Errorf("DeadLetter row = %q; want rDL", dl[0].RowID)
	}
	if dl[0].AttemptCount < 3 {
		t.Errorf("DeadLetter attempt_count = %d; want >= 3", dl[0].AttemptCount)
	}
	if !strings.Contains(dl[0].FailureReason, "permanent") {
		t.Errorf("DeadLetter failure_reason = %q; want contains 'permanent'", dl[0].FailureReason)
	}
	if dl[0].WorkerID != "w-dlq" {
		t.Errorf("DeadLetter worker_id = %q; want w-dlq", dl[0].WorkerID)
	}
}

func TestDispatcher_DrainOnce_RespectsContextCancellation(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	for i := 0; i < 5; i++ {
		insertRow(t, store, string(rune('a'+i)), "t")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	bus := &recordingBus{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: bus, WorkerID: "w1", MaxAttempts: 3,
	})
	_, err := d.DrainOnce(ctx, 10)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Errorf("DrainOnce err = %v; want context.Canceled", err)
	}
}

func TestDispatcher_DrainOnce_EnvelopeReconstructedForBus(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	insertRow(t, store, "rE", "00000000-0000-0000-0000-0000000000aa")
	bus := &recordingBus{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: bus, WorkerID: "w1", MaxAttempts: 3,
	})
	if _, err := d.DrainOnce(context.Background(), 10); err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
	if len(bus.calls) != 1 {
		t.Fatalf("bus.calls = %d; want 1", len(bus.calls))
	}
	env := bus.calls[0].Envelope
	if env.EventID != "rE" {
		t.Errorf("envelope.EventID = %q; want rE", env.EventID)
	}
	if env.SourceProject != "chora-489812" {
		t.Errorf("envelope.SourceProject = %q; want chora-489812", env.SourceProject)
	}
	if env.SourceService != "chora-observability" {
		t.Errorf("envelope.SourceService = %q; want chora-observability", env.SourceService)
	}
	if env.SchemaVersion != 1 {
		t.Errorf("envelope.SchemaVersion = %d; want 1", env.SchemaVersion)
	}
	if env.TenantID != "00000000-0000-0000-0000-0000000000aa" {
		t.Errorf("envelope.TenantID = %q; want ...aa", env.TenantID)
	}
}

func TestDispatcher_Validates_RequiresWorkerID(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("NewDispatcher with empty WorkerID should panic")
		}
	}()
	outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: outbox.NewInMemoryStore(), Bus: &recordingBus{},
	})
}

func TestDispatcher_Validates_DefaultMaxAttempts(t *testing.T) {
	t.Parallel()
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: outbox.NewInMemoryStore(), Bus: &recordingBus{},
		WorkerID: "w1",
	})
	if d.MaxAttempts() != 5 {
		t.Errorf("default MaxAttempts = %d; want 5", d.MaxAttempts())
	}
}

func TestDispatcher_Run_StopsOnContextCancel(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	insertRow(t, store, "r1", "t1")
	bus := &recordingBus{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: bus, WorkerID: "w1", MaxAttempts: 3,
		PollInterval: 5 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := d.Run(ctx, 10); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Errorf("Run err = %v; want context cancel/deadline", err)
	}
	if bus.callCount() < 1 {
		t.Errorf("bus calls = %d; want >= 1 (drained at least once before cancel)", bus.callCount())
	}
}

func TestDispatcher_DrainOnce_BackoffBetweenRetries(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	insertRow(t, store, "rB", "t1")
	bus := &recordingBus{fails: 2, failErr: errors.New("blip")}

	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store:        store,
		Bus:          bus,
		WorkerID:     "w1",
		MaxAttempts:  3,
		BackoffBase:  10 * time.Millisecond,
		BackoffCap:   50 * time.Millisecond,
		PollInterval: 5 * time.Millisecond,
	})
	for i := 0; i < 3; i++ {
		if _, err := d.DrainOnce(context.Background(), 10); err != nil {
			t.Fatalf("DrainOnce pass %d: %v", i, err)
		}
	}
	if bus.callCount() != 1 {
		t.Errorf("publish calls = %d; want 1 (third attempt succeeded)", bus.callCount())
	}
	rows, _ := store.FetchPending(context.Background(), 10)
	if len(rows) != 0 {
		t.Errorf("pending = %d; want 0 (published)", len(rows))
	}
}

// validatingBus reproduces the production CloudPublisher contract: it REJECTS
// any envelope that fails the shared mandatory-field validation — the same
// check that yields `pubsub: envelope: traceparent is required`. Used to prove
// the dispatcher never hands Bus.Publish an envelope with an empty traceparent.
type validatingBus struct {
	mu    sync.Mutex
	calls []recordedPub
}

func (b *validatingBus) Publish(_ context.Context, topic string, env cgcenvelope.Envelope, payload []byte) error {
	if err := cgcenvelope.Validate(env); err != nil {
		return fmt.Errorf("pubsub: %w", err) // mirrors CloudPublisher's error wrapping
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls = append(b.calls, recordedPub{Topic: topic, Envelope: env, Payload: append([]byte(nil), payload...)})
	return nil
}

// TestDispatcher_DrainOnce_EmptyStoredTraceparent_MintsValidTraceparent is the
// event-fabric-repair regression (2026-07-01). ~168 token_usage.recorded rows
// dead-lettered with `pubsub: envelope: traceparent is required`: the shared
// chora_observability.outbox_events table also carries rows written by
// chora-model-gateway (see store.go FetchPending) whose persisted envelope has
// no snake_case `traceparent`. reconstructEnvelope hand-rolled the envelope and
// copied the (empty) traceparent verbatim — with column fallbacks for every
// OTHER mandatory field but none for traceparent — so Bus.Publish rejected it.
// The emit path MUST guarantee a non-empty W3C traceparent on every published
// envelope regardless of what the producer persisted.
func TestDispatcher_DrainOnce_EmptyStoredTraceparent_MintsValidTraceparent(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()

	// A row whose persisted envelope carries every mandatory field EXCEPT a
	// snake_case traceparent — reproduces the cross-writer rows that poisoned
	// the shared outbox table.
	now := time.Now().UTC()
	r := outbox.Row{
		ID:        "no-tp",
		TenantID:  "00000000-0000-0000-0000-0000000000bb",
		EventType: "observability.token_usage.recorded",
		Topic:     "chora.observability.token_usage.recorded.v1",
		Payload:   []byte(`{"id":"no-tp"}`),
		Envelope: map[string]string{
			"event_id":        "no-tp",
			"idempotency_key": "idem-no-tp",
			"tenant_id":       "00000000-0000-0000-0000-0000000000bb",
			"occurred_at":     now.Format(time.RFC3339Nano),
			"published_at":    now.Format(time.RFC3339Nano),
			// traceparent DELIBERATELY ABSENT.
			"source_project": "chora-489812",
			"source_service": "chora-observability",
			"schema_version": "1",
		},
		IdempotencyKey: "idem-no-tp",
		OccurredAt:     now,
	}
	if err := store.Insert(context.Background(), r); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	bus := &validatingBus{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: bus, WorkerID: "w-tp", MaxAttempts: 1,
	})
	n, err := d.DrainOnce(context.Background(), 10)
	if err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
	if n != 1 {
		t.Fatalf("published = %d; want 1 (an empty stored traceparent must not deadletter)", n)
	}
	if dl := store.DeadLetters(); len(dl) != 0 {
		t.Fatalf("deadletters = %d; want 0 (traceparent must be minted, not rejected)", len(dl))
	}
	if len(bus.calls) != 1 {
		t.Fatalf("bus.Publish calls = %d; want 1", len(bus.calls))
	}
	got := bus.calls[0].Envelope
	if got.Traceparent == "" {
		t.Fatal("published envelope.Traceparent is empty; want a minted W3C traceparent")
	}
	if err := cgcenvelope.Validate(got); err != nil {
		t.Errorf("published envelope failed mandatory-field validation: %v", err)
	}
}

// TestTopicObservabilitySinkFailure_ConformsToTopicNamingRule guards the
// event-fabric-repair rename (2026-07-01). The sink-failure alert previously
// published to `chora.governance.observability_sink_failure.v1`, which violates
// the mandatory `chora.{domain}.{aggregate}.{event_type}.v{N}` topic rule — the
// tail `observability_sink_failure` is a single run-on segment, not an
// aggregate.event pair — so every such publish dead-lettered (cascade of ~854).
// The conforming topic is exactly 5 dot-separated parts owned by the emitting
// (observability) domain.
func TestTopicObservabilitySinkFailure_ConformsToTopicNamingRule(t *testing.T) {
	t.Parallel()
	topic := outbox.TopicObservabilitySinkFailure
	parts := strings.Split(topic, ".")
	if len(parts) != 5 {
		t.Fatalf("topic %q has %d dot-parts; want 5 (chora.{domain}.{aggregate}.{event_type}.v{N})", topic, len(parts))
	}
	if parts[0] != "chora" {
		t.Errorf("topic %q: first segment = %q; want \"chora\"", topic, parts[0])
	}
	for i := 1; i < 4; i++ {
		if parts[i] == "" {
			t.Errorf("topic %q: segment %d is empty", topic, i)
		}
	}
	if m, _ := regexp.MatchString(`^v[0-9]+$`, parts[4]); !m {
		t.Errorf("topic %q: version suffix = %q; want v{N}", topic, parts[4])
	}
	// The failure is observability-owned (the emitting service), not governance.
	if parts[1] != "observability" {
		t.Errorf("topic %q: domain segment = %q; want \"observability\"", topic, parts[1])
	}
}
