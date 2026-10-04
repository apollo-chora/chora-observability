// RED-phase specs for the external-egress policy projection subscriber (CHO-2148).
//
// This subscriber is the ONLY bridge between chora_tenancy (which owns the
// entitlement) and chora_observability.external_egress_policy (which the
// model-gateway consults fail-closed on every grounded call). Cross-DB writes
// are forbidden, so if this projection is wrong, the gateway is wrong.
//
// Two failure modes matter more than the happy path:
//
//   - A STALE redelivery must be DISCARDED but ACKed. Pub/Sub is at-least-once
//     and not order-preserving. If a late "egress ON" overwrote a newer "egress
//     OFF", we would silently resurrect an entitlement the tenant just revoked.
//     And if we NACKed it instead of ACKing, a merely-late-but-valid event would
//     poison-loop into the DLQ.
//
//   - A MALFORMED event must FAIL LOUD (NACK → retry → DLQ), never be silently
//     dropped — a dropped event leaves the gateway's view diverged from the
//     tenant's actual decision, with nothing to alert on.
package events_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/libs/chora-go-common/idempotent"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/events"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/externalegress"
)

const (
	eeTenant = "11111111-1111-7111-8111-111111111111"
	eeActor  = "00000000-0000-7000-8000-000000001999"
)

// --- fake projection repo --------------------------------------------------

type fakeEgressProjection struct {
	calls    []externalegress.PolicyChanged
	applied  bool
	err      error
	appliedN int
}

func (f *fakeEgressProjection) Project(_ context.Context, ev externalegress.PolicyChanged) (bool, error) {
	f.calls = append(f.calls, ev)
	if f.err != nil {
		return false, f.err
	}
	if f.applied {
		f.appliedN++
	}
	return f.applied, nil
}

func validPolicyChanged() externalegress.PolicyChanged {
	return externalegress.PolicyChanged{
		EventID:                  "01980000-0000-7000-8000-aaaaaaaaaaaa",
		TenantID:                 eeTenant,
		EgressEnabled:            true,
		DailyCallCeiling:         50,
		Version:                  3,
		UpdatedByGCID:            eeActor,
		PreviousEgressEnabled:    false,
		PreviousDailyCallCeiling: 50,
		OccurredAt:               time.Now().UTC(),
	}
}

func newEgressConsumer(repo externalegress.Repository) *events.ExternalEgressPolicyConsumer {
	return events.NewExternalEgressPolicyConsumer(events.ExternalEgressPolicyConsumerConfig{
		Repo:  repo,
		Inbox: idempotent.NewMemoryStore(),
	})
}

// --- happy path ------------------------------------------------------------

func TestExternalEgressConsumer_Handle_ProjectsTheEvent(t *testing.T) {
	t.Parallel()
	repo := &fakeEgressProjection{applied: true}
	c := newEgressConsumer(repo)

	if err := c.Handle(context.Background(), validPolicyChanged()); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(repo.calls) != 1 {
		t.Fatalf("Project called %d times, want 1", len(repo.calls))
	}
	got := repo.calls[0]
	if !got.EgressEnabled || got.Version != 3 || got.UpdatedByGCID != eeActor {
		t.Errorf("projected event lost fidelity: %+v", got)
	}
}

// --- idempotency (Pub/Sub is at-least-once) --------------------------------

func TestExternalEgressConsumer_Handle_DuplicateDelivery_ProjectsOnce(t *testing.T) {
	t.Parallel()
	repo := &fakeEgressProjection{applied: true}
	c := newEgressConsumer(repo)
	ev := validPolicyChanged()

	if err := c.Handle(context.Background(), ev); err != nil {
		t.Fatalf("first Handle: %v", err)
	}
	if err := c.Handle(context.Background(), ev); err != nil {
		t.Fatalf("duplicate Handle must ACK (nil error), got: %v", err)
	}

	if len(repo.calls) != 1 {
		t.Fatalf("Project called %d times for the SAME event_id, want exactly 1 "+
			"(the inbox must dedupe an at-least-once redelivery)", len(repo.calls))
	}
}

// --- staleness (Pub/Sub is not order-preserving) ---------------------------

// The repo reports the event was NOT applied (its version was not strictly
// newer). That is a normal, expected outcome — the consumer must ACK it.
func TestExternalEgressConsumer_Handle_StaleEvent_AcksWithoutError(t *testing.T) {
	t.Parallel()
	repo := &fakeEgressProjection{applied: false} // discarded as stale
	c := newEgressConsumer(repo)

	err := c.Handle(context.Background(), validPolicyChanged())
	if err != nil {
		t.Fatalf("a STALE event must ACK (nil error), got %v — NACKing it would "+
			"poison-loop a merely-late-but-valid redelivery into the DLQ", err)
	}
	if len(repo.calls) != 1 {
		t.Fatalf("Project should still have been consulted once, got %d", len(repo.calls))
	}
}

// --- fail loud -------------------------------------------------------------

func TestExternalEgressConsumer_Handle_RepoError_FailsLoud(t *testing.T) {
	t.Parallel()
	boom := errors.New("db down")
	repo := &fakeEgressProjection{err: boom}
	c := newEgressConsumer(repo)

	err := c.Handle(context.Background(), validPolicyChanged())
	if err == nil {
		t.Fatal("a projection failure must return an error (→ NACK → retry → DLQ), " +
			"never be swallowed — a dropped egress event silently diverges the gateway")
	}
	if !errors.Is(err, boom) {
		t.Errorf("error must wrap the cause, got %v", err)
	}
}

// A repo failure must NOT claim the inbox key, so the retry re-runs the write.
func TestExternalEgressConsumer_Handle_RepoError_DoesNotClaimInboxKey(t *testing.T) {
	t.Parallel()
	repo := &fakeEgressProjection{err: errors.New("transient")}
	c := newEgressConsumer(repo)
	ev := validPolicyChanged()

	if err := c.Handle(context.Background(), ev); err == nil {
		t.Fatal("expected an error on the first attempt")
	}

	// The broker retries. The write must be attempted again.
	repo.err = nil
	repo.applied = true
	if err := c.Handle(context.Background(), ev); err != nil {
		t.Fatalf("retry after a transient failure: %v", err)
	}
	if len(repo.calls) != 2 {
		t.Fatalf("Project called %d times, want 2 — a FAILED attempt must not claim "+
			"the inbox key, or the retry would be silently skipped and the change lost",
			len(repo.calls))
	}
}

func TestExternalEgressConsumer_Handle_MalformedEvent_Rejected(t *testing.T) {
	t.Parallel()

	cases := map[string]func(*externalegress.PolicyChanged){
		"no event_id":       func(e *externalegress.PolicyChanged) { e.EventID = "" },
		"no tenant_id":      func(e *externalegress.PolicyChanged) { e.TenantID = "" },
		"no actor":          func(e *externalegress.PolicyChanged) { e.UpdatedByGCID = "" },
		"version 0":         func(e *externalegress.PolicyChanged) { e.Version = 0 },
		"negative ceiling":  func(e *externalegress.PolicyChanged) { e.DailyCallCeiling = -1 },
	}

	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			repo := &fakeEgressProjection{applied: true}
			c := newEgressConsumer(repo)

			ev := validPolicyChanged()
			mutate(&ev)

			if err := c.Handle(context.Background(), ev); err == nil {
				t.Fatalf("a malformed event (%s) must FAIL LOUD, not be silently applied", name)
			}
			if len(repo.calls) != 0 {
				t.Errorf("a malformed event must never reach the projection; got %d calls", len(repo.calls))
			}
		})
	}
}
