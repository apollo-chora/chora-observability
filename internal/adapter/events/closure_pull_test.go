// Tests for the closure pull-loop adapter (CHO-1719 gap 4) — binds the
// federated closure-saga ClosureSubscriber to the eventbus.Handler
// contract (the JetStream consume loop acks on nil, naks on error, per D6.2).
package events_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/eventbus"

	"github.com/apollo-chora/chora-observability/internal/adapter/events"
)

// newPullFixture builds a fully-wired ClosureSubscriber on the in-memory
// repo/publisher doubles + the shared PIIClosureMap fixture.
func newPullFixture(t *testing.T) (*events.ClosureSubscriber, *events.InMemoryClosurePublisher, *events.InMemoryClosureRepo) {
	t.Helper()
	pub := events.NewInMemoryClosurePublisher()
	repo := events.NewInMemoryClosureRepo()
	repo.SeedSubject(testTenantID, testGCID)
	return events.NewClosureSubscriber(repo, pub, newPIIMap("chora_observability"), nil), pub, repo
}

func TestClosurePullHandler_ValidPayloadAcksOnNewTopic(t *testing.T) {
	t.Parallel()
	sub, pub, repo := newPullFixture(t)
	handler := events.ClosurePullHandler(sub)

	body, err := json.Marshal(events.PseudonymiseRequestedPayload{
		SagaID: testSagaID, Gcid: testGCID, TenantID: testTenantID,
		Traceparent: testTrace, Tracestate: testTracestate,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	msg := eventbus.Message{
		Subject:  events.TopicPseudonymiseRequested,
		Envelope: envelope.Envelope{},
		Payload:  body,
	}
	if err := handler(context.Background(), msg); err != nil {
		t.Fatalf("handler: %v", err)
	}
	emitted := pub.ClosureRecordedByTopic(events.TopicPseudonymiseCompleted)
	if len(emitted) != 1 {
		t.Fatalf("expected 1 completed ack; got %d", len(emitted))
	}
	if emitted[0].Topic != "chora.observability.account.pseudonymised.v1" {
		t.Fatalf("ack topic = %q; want chora.observability.account.pseudonymised.v1", emitted[0].Topic)
	}
	pseudonymised, err := repo.IsPseudonymised(context.Background(), testTenantID, testGCID)
	if err != nil {
		t.Fatalf("IsPseudonymised: %v", err)
	}
	if !pseudonymised {
		t.Fatalf("subject not pseudonymised in repo")
	}
}

func TestClosurePullHandler_MalformedJSONErrors(t *testing.T) {
	t.Parallel()
	sub, pub, _ := newPullFixture(t)
	handler := events.ClosurePullHandler(sub)

	msg := eventbus.Message{
		Subject: events.TopicPseudonymiseRequested,
		Payload: []byte("{not-json"),
	}
	if err := handler(context.Background(), msg); err == nil {
		t.Fatalf("expected decode error for malformed payload")
	}
	if got := len(pub.ClosureRecorded()); got != 0 {
		t.Fatalf("expected no acks for malformed payload; got %d", got)
	}
}

func TestClosurePullHandler_TraceparentFallsBackToEnvelope(t *testing.T) {
	t.Parallel()
	sub, pub, _ := newPullFixture(t)
	handler := events.ClosurePullHandler(sub)

	// Payload omits traceparent/tracestate — the handler must fall back to
	// the Pub/Sub envelope.
	body, err := json.Marshal(events.PseudonymiseRequestedPayload{
		SagaID: testSagaID, Gcid: testGCID, TenantID: testTenantID,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	msg := eventbus.Message{
		Subject: events.TopicPseudonymiseRequested,
		Envelope: envelope.Envelope{
			Traceparent: testTrace,
			Tracestate:  testTracestate,
		},
		Payload: body,
	}
	if err := handler(context.Background(), msg); err != nil {
		t.Fatalf("handler: %v", err)
	}
	emitted := pub.ClosureRecordedByTopic(events.TopicPseudonymiseCompleted)
	if len(emitted) != 1 {
		t.Fatalf("expected 1 completed ack; got %d", len(emitted))
	}
	if emitted[0].Traceparent != testTrace {
		t.Fatalf("traceparent fallback = %q; want %q", emitted[0].Traceparent, testTrace)
	}
}

func TestClosurePullHandler_NilGuards(t *testing.T) {
	t.Parallel()
	if err := events.ClosurePullHandler(nil)(context.Background(), eventbus.Message{}); err == nil {
		t.Fatalf("nil subscriber should error")
	}
}
