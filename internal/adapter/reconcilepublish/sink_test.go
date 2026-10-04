// Package reconcilepublish_test verifies the real Pub/Sub anomaly + degraded
// publisher adapter for the reconciliation harness.
package reconcilepublish_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-observability/internal/adapter/reconcilepublish"
	"github.com/apollo-chora/chora-observability/internal/domain/reconcile"
)

type recordedPublish struct {
	Topic    string
	Envelope cgcenvelope.Envelope
	Payload  []byte
}

type stubPublisher struct {
	mu    sync.Mutex
	calls []recordedPublish
	err   error
}

func (p *stubPublisher) Publish(_ context.Context, topic string, env cgcenvelope.Envelope, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.calls = append(p.calls, recordedPublish{Topic: topic, Envelope: env, Payload: append([]byte(nil), payload...)})
	return nil
}

func fixedNow() time.Time { return time.Date(2026, 6, 1, 4, 0, 0, 0, time.UTC) }

func TestPubSubEventSink_Emit_PublishesAnomalyOnCanonicalTopic(t *testing.T) {
	t.Parallel()
	pub := &stubPublisher{}
	sink := reconcilepublish.NewPubSubEventSink(reconcilepublish.Config{
		Publisher: pub, SourceProject: "chora-489812", SourceService: "chora-observability", Now: fixedNow,
	})
	ws := time.Date(2026, 5, 31, 0, 0, 0, 0, time.UTC)
	ev := reconcile.Verdict{
		IsAnomaly:           true,
		LedgerSumMicros:     1_000_000,
		VertexBillingMicros: 1_000_500,
		DriftMicros:         500,
		DriftFraction:       0.0005,
		WindowStart:         ws,
		WindowEnd:           ws.AddDate(0, 0, 1),
	}.AsEvent()

	if err := sink.Emit(context.Background(), ev); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if len(pub.calls) != 1 {
		t.Fatalf("publish calls = %d; want 1", len(pub.calls))
	}
	c := pub.calls[0]
	if c.Topic != reconcile.CanonicalReconcileAnomalyTopic {
		t.Errorf("topic = %q; want %q", c.Topic, reconcile.CanonicalReconcileAnomalyTopic)
	}
	if c.Topic != "chora.governance.payment_reconciliation.anomaly.v1" {
		t.Errorf("topic literal = %q", c.Topic)
	}
	if err := cgcenvelope.Validate(c.Envelope); err != nil {
		t.Errorf("envelope invalid: %v", err)
	}
	if c.Envelope.TenantID != "platform" {
		t.Errorf("tenant_id = %q; want platform", c.Envelope.TenantID)
	}
	if c.Envelope.ChoraImdaDimension != "accountability" {
		t.Errorf("imda dimension = %q; want accountability", c.Envelope.ChoraImdaDimension)
	}
	if c.Envelope.SourceService != "chora-observability" {
		t.Errorf("source_service = %q", c.Envelope.SourceService)
	}
	// Payload round-trips.
	var got reconcile.AnomalyEvent
	if err := json.Unmarshal(c.Payload, &got); err != nil {
		t.Fatalf("payload unmarshal: %v", err)
	}
	if got.DriftMicros != 500 || !got.WindowStart.Equal(ws) {
		t.Errorf("payload = %+v; want drift 500 + window start %v", got, ws)
	}
}

func TestPubSubEventSink_EmitDegraded_PublishesOnDegradedTopic(t *testing.T) {
	t.Parallel()
	pub := &stubPublisher{}
	sink := reconcilepublish.NewPubSubEventSink(reconcilepublish.Config{Publisher: pub, Now: fixedNow})
	ws := time.Date(2026, 5, 31, 0, 0, 0, 0, time.UTC)
	ev := reconcile.DegradedEvent{
		Topic:              reconcile.CanonicalReconcileDegradedTopic,
		EventType:          "payment_reconciliation.degraded.v1",
		Reason:             "circuit open",
		UpstreamComponent:  "vertex_billing",
		WindowStart:        ws,
		WindowEnd:          ws.AddDate(0, 0, 1),
		ChoraImdaDimension: "accountability",
		ImdaLifecycleStage: "post_deploy",
		OccurredAt:         fixedNow(),
	}
	if err := sink.EmitDegraded(context.Background(), ev); err != nil {
		t.Fatalf("EmitDegraded: %v", err)
	}
	if len(pub.calls) != 1 {
		t.Fatalf("publish calls = %d; want 1", len(pub.calls))
	}
	if pub.calls[0].Topic != reconcile.CanonicalReconcileDegradedTopic {
		t.Errorf("topic = %q; want degraded topic", pub.calls[0].Topic)
	}
	if err := cgcenvelope.Validate(pub.calls[0].Envelope); err != nil {
		t.Errorf("envelope invalid: %v", err)
	}
}

func TestPubSubEventSink_Emit_PropagatesPublishError(t *testing.T) {
	t.Parallel()
	pub := &stubPublisher{err: errors.New("broker down")}
	sink := reconcilepublish.NewPubSubEventSink(reconcilepublish.Config{Publisher: pub})
	err := sink.Emit(context.Background(), reconcile.Verdict{IsAnomaly: true}.AsEvent())
	if err == nil {
		t.Fatal("expected publish error to surface (fail loud)")
	}
}

func TestNewPubSubEventSink_PanicsOnNilPublisher(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on nil Publisher")
		}
	}()
	_ = reconcilepublish.NewPubSubEventSink(reconcilepublish.Config{})
}

// Compile-time assertion echoed in test form: the sink satisfies both ports.
func TestPubSubEventSink_SatisfiesBothPorts(t *testing.T) {
	t.Parallel()
	pub := &stubPublisher{}
	sink := reconcilepublish.NewPubSubEventSink(reconcilepublish.Config{Publisher: pub})
	var _ reconcile.EventSink = sink
	var _ reconcile.DegradedSink = sink
}
