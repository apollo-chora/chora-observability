// Specs for the observability-side external-egress domain (CHO-2148).
//
// Validate() is the gate between an untrusted Pub/Sub payload and the read-copy
// the model-gateway enforces on. Everything it rejects is something that must
// NOT be silently projected — a malformed egress event is a producer bug, and
// applying it (or dropping it quietly) would diverge the gateway from the
// tenant's actual decision with nothing to alert on.
package externalegress_test

import (
	"errors"
	"testing"
	"time"

	"github.com/apollo-chora/chora-observability/internal/domain/externalegress"
)

func valid() externalegress.PolicyChanged {
	return externalegress.PolicyChanged{
		EventID:                  "01980000-0000-7000-8000-aaaaaaaaaaaa",
		TenantID:                 "11111111-1111-7111-8111-111111111111",
		EgressEnabled:            true,
		DailyCallCeiling:         50,
		Version:                  3,
		UpdatedByGCID:            "00000000-0000-7000-8000-000000001999",
		PreviousEgressEnabled:    false,
		PreviousDailyCallCeiling: 50,
		OccurredAt:               time.Now().UTC(),
	}
}

func TestPolicyChanged_Validate_AcceptsAWellFormedEvent(t *testing.T) {
	t.Parallel()
	if err := valid().Validate(); err != nil {
		t.Fatalf("a well-formed event must validate: %v", err)
	}
}

func TestPolicyChanged_Validate_RejectsEmptyEventID(t *testing.T) {
	t.Parallel()
	ev := valid()
	ev.EventID = ""
	if err := ev.Validate(); !errors.Is(err, externalegress.ErrEmptyEventID) {
		t.Fatalf("err = %v, want ErrEmptyEventID — without an event_id the inbox "+
			"cannot dedupe an at-least-once redelivery", err)
	}
}

func TestPolicyChanged_Validate_RejectsEmptyTenantID(t *testing.T) {
	t.Parallel()
	ev := valid()
	ev.TenantID = ""
	if err := ev.Validate(); !errors.Is(err, externalegress.ErrEmptyTenantID) {
		t.Fatalf("err = %v, want ErrEmptyTenantID", err)
	}
}

func TestPolicyChanged_Validate_RejectsEmptyActor(t *testing.T) {
	t.Parallel()
	ev := valid()
	ev.UpdatedByGCID = ""
	if err := ev.Validate(); !errors.Is(err, externalegress.ErrEmptyActor) {
		t.Fatalf("err = %v, want ErrEmptyActor — the trail must always be able to "+
			"answer 'who turned this on' (IMDA D1)", err)
	}
}

// Version 0 is reserved for "hand-seeded / never persisted". A real change is
// always >= 1. Projecting a version-0 event would tie with the seed row and be
// silently discarded by the monotonic guard — the change would vanish.
func TestPolicyChanged_Validate_RejectsVersionZero(t *testing.T) {
	t.Parallel()
	ev := valid()
	ev.Version = 0
	if err := ev.Validate(); !errors.Is(err, externalegress.ErrNonPositiveVersion) {
		t.Fatalf("err = %v, want ErrNonPositiveVersion", err)
	}
}

func TestPolicyChanged_Validate_RejectsNegativeVersion(t *testing.T) {
	t.Parallel()
	ev := valid()
	ev.Version = -1
	if err := ev.Validate(); !errors.Is(err, externalegress.ErrNonPositiveVersion) {
		t.Fatalf("err = %v, want ErrNonPositiveVersion", err)
	}
}

func TestPolicyChanged_Validate_RejectsNegativeCeiling(t *testing.T) {
	t.Parallel()
	ev := valid()
	ev.DailyCallCeiling = -1
	if err := ev.Validate(); !errors.Is(err, externalegress.ErrNegativeCeiling) {
		t.Fatalf("err = %v, want ErrNegativeCeiling", err)
	}
}

// A zero ceiling is legitimate: "entitled, but budgeted to nothing today". The
// gateway's per-day counter denies at 0. It must NOT be treated as malformed.
func TestPolicyChanged_Validate_AcceptsZeroCeiling(t *testing.T) {
	t.Parallel()
	ev := valid()
	ev.DailyCallCeiling = 0
	if err := ev.Validate(); err != nil {
		t.Fatalf("a zero ceiling is a valid budgeted-to-nothing state, not malformed: %v", err)
	}
}

// An opt-OUT (egress_enabled=false) is the safety-critical direction. It must
// always validate — refusing it would strand a tenant in the ON state.
func TestPolicyChanged_Validate_AcceptsOptOut(t *testing.T) {
	t.Parallel()
	ev := valid()
	ev.EgressEnabled = false
	ev.PreviousEgressEnabled = true
	if err := ev.Validate(); err != nil {
		t.Fatalf("an opt-OUT must always validate — refusing it would strand the "+
			"tenant egress-ON: %v", err)
	}
}
