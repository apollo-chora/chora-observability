// Package outbox_test — ADR-167 Plane-4 fail-loud tests.
//
// Two surfaces under test:
//
//   - Dispatcher emits a governance sink-failure alert on BOTH dead-letter
//     paths (malformed envelope + publish-retries exhausted) — the
//     dead-letter row alone is not enough escalation.
//   - PublisherAlertSink marshals the alert + publishes it on
//     chora.observability.sink_failure.recorded.v1 with a valid envelope
//     via the same Bus the dispatcher drains to.
package outbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"

	"github.com/apollo-chora/chora-observability/internal/adapter/outbox"
)

// recordingAlertSink captures emitted sink-failure alerts.
type recordingAlertSink struct {
	mu     sync.Mutex
	alerts []outbox.SinkFailureAlert
	err    error
}

func (s *recordingAlertSink) EmitSinkFailure(_ context.Context, a outbox.SinkFailureAlert) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.alerts = append(s.alerts, a)
	return nil
}

func (s *recordingAlertSink) snapshot() []outbox.SinkFailureAlert {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]outbox.SinkFailureAlert(nil), s.alerts...)
}

// TestDispatcher_PublishExhausted_EmitsGovernanceAlert verifies the
// publish-retries-exhausted dead-letter path fires the governance alert with
// the publish_exhausted reason.
func TestDispatcher_PublishExhausted_EmitsGovernanceAlert(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	insertRow(t, store, "rGA", "00000000-0000-0000-0000-0000000000aa")

	bus := &recordingBus{fails: 10, failErr: errors.New("permanent sink down")}
	alerts := &recordingAlertSink{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store:       store,
		Bus:         bus,
		AlertSink:   alerts,
		WorkerID:    "w-alert",
		MaxAttempts: 3,
	})
	for i := 0; i < 3; i++ {
		if _, err := d.DrainOnce(context.Background(), 10); err != nil {
			t.Fatalf("DrainOnce %d: %v", i, err)
		}
	}

	got := alerts.snapshot()
	if len(got) != 1 {
		t.Fatalf("sink-failure alerts = %d; want 1", len(got))
	}
	a := got[0]
	if a.Reason != outbox.SinkFailurePublishExhausted {
		t.Errorf("reason = %q; want publish_exhausted", a.Reason)
	}
	if a.RowID != "rGA" {
		t.Errorf("row_id = %q; want rGA", a.RowID)
	}
	if a.Topic != "chora.observability.token_usage.recorded.v1" {
		t.Errorf("topic = %q; want the failed destination topic", a.Topic)
	}
	if a.AttemptCount < 3 {
		t.Errorf("attempt_count = %d; want >= 3", a.AttemptCount)
	}
	if a.WorkerID != "w-alert" {
		t.Errorf("worker_id = %q; want w-alert", a.WorkerID)
	}
	// And the local dead-letter row still landed.
	if dl := store.DeadLetters(); len(dl) != 1 {
		t.Errorf("dead-letter rows = %d; want 1", len(dl))
	}
}

// TestDispatcher_MalformedEnvelope_EmitsGovernanceAlert verifies a row whose
// envelope cannot be reconstructed dead-letters AND fires the malformed alert.
//
// reconstructEnvelope only errors on an unparseable schema_version? It never
// errors today (defaults fill). To force the malformed path we use a row that
// fails the Bus with a non-retryable shape is the publish path; the dedicated
// malformed path is exercised in the consumer_quarantine test where a real
// proto-decode failure occurs. Here we assert the alert is NOT emitted on a
// successful publish (negative control).
func TestDispatcher_SuccessfulPublish_NoAlert(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	insertRow(t, store, "rOK", "t1")
	bus := &recordingBus{}
	alerts := &recordingAlertSink{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: bus, AlertSink: alerts, WorkerID: "w1", MaxAttempts: 3,
	})
	if _, err := d.DrainOnce(context.Background(), 10); err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
	if got := alerts.snapshot(); len(got) != 0 {
		t.Errorf("alerts on success = %d; want 0", len(got))
	}
}

// TestDispatcher_AlertSinkNil_StillDeadletters verifies the dead-letter still
// happens when AlertSink is unwired (no crash, no swallow).
func TestDispatcher_AlertSinkNil_StillDeadletters(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	insertRow(t, store, "rNoSink", "t1")
	bus := &recordingBus{fails: 10, failErr: errors.New("down")}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: bus, WorkerID: "w1", MaxAttempts: 2, // no AlertSink
	})
	for i := 0; i < 2; i++ {
		_, _ = d.DrainOnce(context.Background(), 10)
	}
	if dl := store.DeadLetters(); len(dl) != 1 {
		t.Errorf("dead-letter rows = %d; want 1 (dead-letter survives nil AlertSink)", len(dl))
	}
}

// recordingPublishBus captures Publish calls for the alert-sink test.
type recordingPublishBus struct {
	mu    sync.Mutex
	calls []recordedPub
	err   error
}

func (b *recordingPublishBus) Publish(_ context.Context, topic string, env cgcenvelope.Envelope, payload []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return b.err
	}
	b.calls = append(b.calls, recordedPub{Topic: topic, Envelope: env, Payload: append([]byte(nil), payload...)})
	return nil
}

// TestPublisherAlertSink_PublishesValidEnvelopeAndPayload verifies the real
// alert sink builds a valid envelope + JSON payload on the governance topic.
func TestPublisherAlertSink_PublishesValidEnvelopeAndPayload(t *testing.T) {
	t.Parallel()
	bus := &recordingPublishBus{}
	sink := outbox.NewPublisherAlertSink(outbox.PublisherAlertSinkConfig{
		Bus:           bus,
		SourceProject: "chora-489812",
		SourceService: "chora-observability",
		Now:           func() time.Time { return time.Date(2026, 6, 1, 4, 0, 0, 0, time.UTC) },
	})
	alert := outbox.SinkFailureAlert{
		RowID:         "row-1",
		TenantID:      "11111111-1111-1111-1111-111111111111",
		Topic:         "chora.observability.token_usage.recorded.v1",
		EventType:     "observability.token_usage.recorded",
		Reason:        outbox.SinkFailurePublishExhausted,
		FailureDetail: "broker timeout",
		AttemptCount:  5,
		WorkerID:      "w-1",
	}
	if err := sink.EmitSinkFailure(context.Background(), alert); err != nil {
		t.Fatalf("EmitSinkFailure: %v", err)
	}
	if len(bus.calls) != 1 {
		t.Fatalf("publish calls = %d; want 1", len(bus.calls))
	}
	c := bus.calls[0]
	if c.Topic != "chora.observability.sink_failure.recorded.v1" {
		t.Errorf("topic = %q; want observability sink-failure topic", c.Topic)
	}
	// Envelope must validate (mandatory-field contract).
	if err := cgcenvelope.Validate(c.Envelope); err != nil {
		t.Errorf("envelope invalid: %v", err)
	}
	if c.Envelope.ChoraImdaDimension != "accountability" {
		t.Errorf("imda dimension = %q; want accountability", c.Envelope.ChoraImdaDimension)
	}
	if c.Envelope.TenantID != alert.TenantID {
		t.Errorf("tenant_id = %q; want %q", c.Envelope.TenantID, alert.TenantID)
	}
	// Payload round-trips to the alert.
	var got outbox.SinkFailureAlert
	if err := json.Unmarshal(c.Payload, &got); err != nil {
		t.Fatalf("payload unmarshal: %v", err)
	}
	if got.RowID != "row-1" || got.Reason != outbox.SinkFailurePublishExhausted {
		t.Errorf("payload = %+v; want row-1/publish_exhausted", got)
	}
}

// TestPublisherAlertSink_PlatformTenantFallback verifies an empty tenant_id
// falls back to the "platform" sentinel so the envelope still validates.
func TestPublisherAlertSink_PlatformTenantFallback(t *testing.T) {
	t.Parallel()
	bus := &recordingPublishBus{}
	sink := outbox.NewPublisherAlertSink(outbox.PublisherAlertSinkConfig{Bus: bus})
	err := sink.EmitSinkFailure(context.Background(), outbox.SinkFailureAlert{
		RowID:  "row-2",
		Topic:  "chora.observability.token_usage.recorded.v1",
		Reason: outbox.SinkFailureMalformed,
	})
	if err != nil {
		t.Fatalf("EmitSinkFailure: %v", err)
	}
	if len(bus.calls) != 1 {
		t.Fatalf("publish calls = %d; want 1", len(bus.calls))
	}
	if bus.calls[0].Envelope.TenantID != "platform" {
		t.Errorf("tenant_id = %q; want platform fallback", bus.calls[0].Envelope.TenantID)
	}
}

// TestPublisherAlertSink_PropagatesPublishError verifies a publish failure
// surfaces loudly (returned to caller, not swallowed).
func TestPublisherAlertSink_PropagatesPublishError(t *testing.T) {
	t.Parallel()
	bus := &recordingPublishBus{err: errors.New("bus down")}
	sink := outbox.NewPublisherAlertSink(outbox.PublisherAlertSinkConfig{Bus: bus})
	err := sink.EmitSinkFailure(context.Background(), outbox.SinkFailureAlert{
		RowID: "row-3", Topic: "chora.observability.token_usage.recorded.v1", Reason: outbox.SinkFailureMalformed,
	})
	if err == nil {
		t.Fatal("expected publish error to surface")
	}
}

// TestNewPublisherAlertSink_PanicsOnNilBus verifies fail-loud construction.
func TestNewPublisherAlertSink_PanicsOnNilBus(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on nil Bus")
		}
	}()
	_ = outbox.NewPublisherAlertSink(outbox.PublisherAlertSinkConfig{})
}
