// Package outbox_test — extra coverage for low-coverage helpers + error
// paths to clear the 85% domain-package gate per `feedback_strict_tdd`.
package outbox_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-observability/internal/adapter/outbox"
)

// -----------------------------------------------------------------------------
// Row.AggregateType + Row.AggregateID — derive defaults from EventType + ID.
// -----------------------------------------------------------------------------

func TestRow_AggregateType_FromCanonicalEventType(t *testing.T) {
	t.Parallel()
	r := outbox.Row{EventType: "observability.token_usage.recorded"}
	if got := r.AggregateType(); got != "token_usage" {
		t.Errorf("AggregateType = %q; want token_usage", got)
	}
}

func TestRow_AggregateType_FallsBackToFirstSegment(t *testing.T) {
	t.Parallel()
	r := outbox.Row{EventType: "decision_only"}
	if got := r.AggregateType(); got != "decision_only" {
		t.Errorf("AggregateType = %q; want decision_only", got)
	}
}

func TestRow_AggregateType_EmptyEventType_FallsBackToEvent(t *testing.T) {
	t.Parallel()
	r := outbox.Row{}
	if got := r.AggregateType(); got != "event" {
		t.Errorf("AggregateType empty event_type = %q; want event", got)
	}
}

func TestRow_AggregateID_PrefersIDOverIdempotencyKey(t *testing.T) {
	t.Parallel()
	r := outbox.Row{ID: "id-1", IdempotencyKey: "idem-1"}
	if got := r.AggregateID(); got != "id-1" {
		t.Errorf("AggregateID = %q; want id-1", got)
	}
}

func TestRow_AggregateID_FallsBackToIdempotencyKey(t *testing.T) {
	t.Parallel()
	r := outbox.Row{ID: "", IdempotencyKey: "idem-1"}
	if got := r.AggregateID(); got != "idem-1" {
		t.Errorf("AggregateID empty id = %q; want idem-1", got)
	}
}

// -----------------------------------------------------------------------------
// PostgresStore error paths — wrap the driver error.
// -----------------------------------------------------------------------------

func TestPostgresStore_Insert_PassesThroughNonUniqueError(t *testing.T) {
	t.Parallel()
	db := &stubDB{execErr: errors.New("connection refused")}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	r := newRow("x", "t", time.Now().UTC())
	err := store.Insert(context.Background(), r)
	if err == nil {
		t.Fatal("Insert with driver error = nil; want error")
	}
	if errors.Is(err, outbox.ErrDuplicateIdempotencyKey) {
		t.Errorf("non-unique error misclassified as duplicate: %v", err)
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("err = %v; want wrap of driver error", err)
	}
}

func TestPostgresStore_MarkPublished_WrapsDriverError(t *testing.T) {
	t.Parallel()
	db := &stubDB{execErr: errors.New("boom")}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	err := store.MarkPublished(context.Background(), "x")
	if err == nil {
		t.Errorf("MarkPublished with driver error = nil; want error")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("err = %v; want contains 'boom'", err)
	}
}

func TestPostgresStore_MarkFailed_WrapsDriverError(t *testing.T) {
	t.Parallel()
	db := &stubDB{execErr: errors.New("boom")}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	err := store.MarkFailed(context.Background(), "x", "msg")
	if err == nil {
		t.Errorf("MarkFailed with driver error = nil; want error")
	}
}

func TestPostgresStore_Deadletter_WrapsInsertError(t *testing.T) {
	t.Parallel()
	db := &stubDB{execErr: errors.New("boom-insert")}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	err := store.Deadletter(context.Background(), "x", "fatal", 5)
	if err == nil {
		t.Errorf("Deadletter with driver error = nil; want error")
	}
}

func TestPostgresStore_FetchPending_WrapsQueryError(t *testing.T) {
	t.Parallel()
	db := &stubDB{queryErr: errors.New("query-boom")}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	_, err := store.FetchPending(context.Background(), 10)
	if err == nil {
		t.Errorf("FetchPending with query error = nil; want error")
	}
}

// -----------------------------------------------------------------------------
// Dispatcher.publishOne — envelope reconstruct corruption path.
// -----------------------------------------------------------------------------

func TestDispatcher_DrainOnce_EnvelopeReconstructEmpty_DefaultsApply(t *testing.T) {
	t.Parallel()
	// An almost-empty envelope still publishes (defaults stamped per
	// reconstructEnvelope). This exercises the default-fill branches that
	// the canonical happy-path doesn't hit.
	store := outbox.NewInMemoryStore()
	now := time.Now().UTC()
	r := outbox.Row{
		ID:             "rEmpty",
		TenantID:       "t1",
		Topic:          "chora.observability.test.bare.v1",
		Payload:        []byte(`{}`),
		Envelope:       map[string]string{}, // empty — every field falls back to default
		IdempotencyKey: "idem-empty",
		OccurredAt:     now,
	}
	if err := store.Insert(context.Background(), r); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	bus := &recordingBus{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: bus, WorkerID: "w1", MaxAttempts: 3,
	})
	n, err := d.DrainOnce(context.Background(), 10)
	if err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
	if n != 1 {
		t.Errorf("publish count = %d; want 1", n)
	}
	if bus.calls[0].Envelope.SourceService != "chora-observability" {
		t.Errorf("default SourceService = %q; want chora-observability",
			bus.calls[0].Envelope.SourceService)
	}
	if bus.calls[0].Envelope.SourceProject != "chora-489812" {
		t.Errorf("default SourceProject = %q; want chora-489812",
			bus.calls[0].Envelope.SourceProject)
	}
	if bus.calls[0].Envelope.SchemaVersion != 1 {
		t.Errorf("default SchemaVersion = %d; want 1", bus.calls[0].Envelope.SchemaVersion)
	}
	if bus.calls[0].Envelope.EventID != "rEmpty" {
		t.Errorf("default EventID = %q; want rEmpty (row id fallback)", bus.calls[0].Envelope.EventID)
	}
}

// -----------------------------------------------------------------------------
// parseTime — fallbacks
// -----------------------------------------------------------------------------

func TestDispatcher_DrainOnce_ParseTime_RFC3339(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	r := outbox.Row{
		ID:             "rTime",
		TenantID:       "t",
		Topic:          "chora.observability.test.time.v1",
		Payload:        []byte(`{}`),
		Envelope:       map[string]string{"occurred_at": "2026-05-12T10:00:00Z"}, // RFC3339, not nano
		IdempotencyKey: "idem-time",
		OccurredAt:     time.Now().UTC(),
	}
	_ = store.Insert(context.Background(), r)
	bus := &recordingBus{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: bus, WorkerID: "w1", MaxAttempts: 3,
	})
	if _, err := d.DrainOnce(context.Background(), 10); err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
	// Sanity: published OK.
	if bus.callCount() != 1 {
		t.Errorf("publish calls = %d; want 1", bus.callCount())
	}
}

// -----------------------------------------------------------------------------
// truncate covers the >n path.
// -----------------------------------------------------------------------------

func TestPostgresStore_MarkFailed_TruncatesLongError(t *testing.T) {
	t.Parallel()
	db := &stubDB{}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	long := strings.Repeat("X", 2000)
	if err := store.MarkFailed(context.Background(), "x", long); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	// 1000-char ceiling on truncate.
	if len(db.execArgs[0][1].(string)) > 1000 {
		t.Errorf("truncated len = %d; want <= 1000", len(db.execArgs[0][1].(string)))
	}
}
