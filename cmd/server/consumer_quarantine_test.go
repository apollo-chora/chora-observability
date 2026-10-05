// consumer_quarantine_test.go — ADR-167 Plane-4 consumer-side fail-loud tests.
//
// Verifies that a malformed inbound event routed through withQuarantine:
//   - writes a LOCAL dead-letter row (Insert + Deadletter on the outbox store),
//   - emits a governance sink-failure alert (malformed reason),
//   - still returns the original error so the broker Nacks (defence in depth).
//
// And that a well-formed event passes through untouched (no quarantine, no
// alert).
package main

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/apollo-chora/chora-common/eventbus"
	obsoutbox "github.com/apollo-chora/chora-observability/internal/adapter/outbox"
)

// recordingQuarantineAlert captures alerts emitted by the quarantine wrapper.
type recordingQuarantineAlert struct {
	mu     sync.Mutex
	alerts []obsoutbox.SinkFailureAlert
}

func (s *recordingQuarantineAlert) EmitSinkFailure(_ context.Context, a obsoutbox.SinkFailureAlert) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.alerts = append(s.alerts, a)
	return nil
}

func (s *recordingQuarantineAlert) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.alerts)
}

func (s *recordingQuarantineAlert) first() obsoutbox.SinkFailureAlert {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.alerts[0]
}

func TestWithQuarantine_MalformedEvent_DeadlettersAndAlertsAndReturnsError(t *testing.T) {
	store := obsoutbox.NewInMemoryStore()
	alert := &recordingQuarantineAlert{}

	rejectErr := errors.New("proto decode payload: cannot parse")
	inner := func(_ context.Context, _ eventbus.Message) error { return rejectErr }

	h := withQuarantine(inner, quarantineDeps{
		ConsumerName: "token_usage",
		Topic:        "chora.observability.token_usage.recorded.v1",
		Store:        store,
		Alert:        alert,
	})

	msg := eventbus.Message{
		Subject: "chora.observability.token_usage.recorded.v1",
		Payload: []byte{0xff, 0x00, 0xfe}, // garbage bytes
	}
	msg.Envelope.EventID = "evt-bad-1"
	msg.Envelope.TenantID = "11111111-1111-1111-1111-111111111111"

	err := h(context.Background(), msg)
	if err == nil {
		t.Fatal("quarantine wrapper must return the original error (broker Nack)")
	}
	if !errors.Is(err, rejectErr) {
		t.Errorf("err = %v; want wrap/identity of rejectErr", err)
	}

	// Local dead-letter row landed.
	dls := store.DeadLetters()
	if len(dls) != 1 {
		t.Fatalf("dead-letter rows = %d; want 1", len(dls))
	}
	if dls[0].RowID != "evt-bad-1" {
		t.Errorf("dead-letter row_id = %q; want evt-bad-1", dls[0].RowID)
	}

	// Governance alert emitted with malformed reason.
	if alert.count() != 1 {
		t.Fatalf("alerts = %d; want 1", alert.count())
	}
	a := alert.first()
	if a.Reason != obsoutbox.SinkFailureMalformed {
		t.Errorf("reason = %q; want malformed", a.Reason)
	}
	if a.RowID != "evt-bad-1" {
		t.Errorf("alert row_id = %q; want evt-bad-1", a.RowID)
	}
	if a.TenantID != "11111111-1111-1111-1111-111111111111" {
		t.Errorf("alert tenant_id = %q; want propagated tenant", a.TenantID)
	}
}

func TestWithQuarantine_WellFormedEvent_PassesThroughNoAlert(t *testing.T) {
	store := obsoutbox.NewInMemoryStore()
	alert := &recordingQuarantineAlert{}

	inner := func(_ context.Context, _ eventbus.Message) error { return nil }
	h := withQuarantine(inner, quarantineDeps{
		ConsumerName: "agent_decision",
		Topic:        "chora.observability.agent_decision.logged.v1",
		Store:        store,
		Alert:        alert,
	})

	msg := eventbus.Message{Subject: "chora.observability.agent_decision.logged.v1"}
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("well-formed event must pass through: %v", err)
	}
	if len(store.DeadLetters()) != 0 {
		t.Errorf("dead-letters = %d; want 0 on success", len(store.DeadLetters()))
	}
	if alert.count() != 0 {
		t.Errorf("alerts = %d; want 0 on success", alert.count())
	}
}

func TestWithQuarantine_NilStore_StillAlertsAndReturnsError(t *testing.T) {
	alert := &recordingQuarantineAlert{}
	rejectErr := errors.New("validation: tenant_id required")
	inner := func(_ context.Context, _ eventbus.Message) error { return rejectErr }

	h := withQuarantine(inner, quarantineDeps{
		ConsumerName: "token_usage",
		Topic:        "chora.observability.token_usage.recorded.v1",
		Store:        nil, // dev path — no DB pool
		Alert:        alert,
	})

	err := h(context.Background(), eventbus.Message{})
	if err == nil {
		t.Fatal("expected original error returned")
	}
	if alert.count() != 1 {
		t.Errorf("alerts = %d; want 1 even without a store", alert.count())
	}
}

func TestWithQuarantine_MalformedEvent_SynthesisesRowIDWhenAttrsBlank(t *testing.T) {
	store := obsoutbox.NewInMemoryStore()
	alert := &recordingQuarantineAlert{}
	inner := func(_ context.Context, _ eventbus.Message) error { return errors.New("boom") }
	h := withQuarantine(inner, quarantineDeps{
		ConsumerName: "token_usage",
		Topic:        "chora.observability.token_usage.recorded.v1",
		Store:        store,
		Alert:        alert,
	})
	// Message with blank routing attrs → row id must be synthesised + tenant
	// falls back to "platform".
	if err := h(context.Background(), eventbus.Message{}); err == nil {
		t.Fatal("expected error")
	}
	dls := store.DeadLetters()
	if len(dls) != 1 {
		t.Fatalf("dead-letters = %d; want 1", len(dls))
	}
	if dls[0].RowID == "" {
		t.Error("synthesised row_id must be non-empty")
	}
	if alert.first().TenantID != "platform" {
		t.Errorf("tenant fallback = %q; want platform", alert.first().TenantID)
	}
}
