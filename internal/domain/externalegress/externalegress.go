// Package externalegress holds the chora-observability side of the Far Sight
// external web-egress governance model (CHO-2148; ADR-220 D4, ADR-231 D6,
// PLAN.md §4.2.4).
//
// Observability owns NEITHER the entitlement nor the decision. chora-tenancy is
// the source of truth: a Tenant Admin sets the policy in H+, and chora-tenancy
// publishes chora.tenancy.external_egress_policy.updated.v1. This service holds
// only the runtime READ-PROJECTION (`external_egress_policy`) that the
// model-gateway consults fail-closed on every grounded call, plus the audit
// trail and the platform kill-switch.
//
// Cross-DB writes are forbidden, so that event is the ONLY bridge between the
// two databases. Everything here exists to project it faithfully — which mostly
// means refusing to project it when it is stale.
package externalegress

import (
	"context"
	"errors"
	"time"
)

// InboxTTL is the dedupe-key retention window for the projection subscriber.
// Matches the Pub/Sub message-retention window (7d) so a redelivery inside the
// broker's retention can never be reapplied.
const InboxTTL = 7 * 24 * time.Hour

var (
	// ErrEmptyEventID — the envelope carried no event_id, so the inbox cannot
	// dedupe it. Fail loud: an unidentifiable event is a broken producer, not
	// something to silently apply.
	ErrEmptyEventID = errors.New("externalegress: event_id is required")

	// ErrEmptyTenantID — no tenant to project onto.
	ErrEmptyTenantID = errors.New("externalegress: tenant_id is required")

	// ErrEmptyActor — an egress change with no actor. Refused: the trail must
	// always be able to answer "who turned this on" (IMDA D1).
	ErrEmptyActor = errors.New("externalegress: updated_by_gcid is required")

	// ErrNonPositiveVersion — a real policy change always carries version >= 1.
	// Version 0 is reserved for "hand-seeded / never persisted", so projecting
	// it would let a malformed event tie with (and be ignored by, or worse
	// overwrite) the seed row.
	ErrNonPositiveVersion = errors.New("externalegress: version must be >= 1")

	// ErrNegativeCeiling — a negative daily ceiling is meaningless.
	ErrNegativeCeiling = errors.New("externalegress: daily_call_ceiling must be >= 0")
)

// PolicyChanged is the domain view of
// chora.tenancy.external_egress_policy.updated.v1.
type PolicyChanged struct {
	EventID  string
	TenantID string

	// After-state.
	EgressEnabled    bool
	DailyCallCeiling int32

	// Version is MONOTONIC in the source aggregate. The projection applies this
	// event only when it is strictly newer than what it already holds — Pub/Sub
	// is at-least-once and not order-preserving, and without this a redelivered
	// "egress ON" could silently resurrect an entitlement the tenant has since
	// turned OFF.
	Version int64

	UpdatedByGCID string

	// Before-state — carried on the event so the audit trail is complete without
	// a cross-DB read back into chora_tenancy.
	PreviousEgressEnabled    bool
	PreviousDailyCallCeiling int32

	OccurredAt time.Time
}

// Validate rejects an event the projection must not apply. A validation failure
// is a FAIL-LOUD error: the subscriber NACKs, the broker retries, and it
// eventually dead-letters — which is correct, because a malformed egress event
// is a producer bug and silently dropping it would leave the gateway's
// entitlement view diverged from the tenant's actual decision.
func (e PolicyChanged) Validate() error {
	if e.EventID == "" {
		return ErrEmptyEventID
	}
	if e.TenantID == "" {
		return ErrEmptyTenantID
	}
	if e.UpdatedByGCID == "" {
		return ErrEmptyActor
	}
	if e.Version < 1 {
		return ErrNonPositiveVersion
	}
	if e.DailyCallCeiling < 0 {
		return ErrNegativeCeiling
	}
	return nil
}

// Repository projects policy changes into the model-gateway's read-copy.
type Repository interface {
	// Project applies the change to external_egress_policy and appends the audit
	// row, in one transaction.
	//
	// It reports whether the event was APPLIED (true) or discarded as STALE
	// (false). A stale event is NOT an error: it must be ACKed, never NACKed
	// into a poison loop that would dead-letter a perfectly valid — merely
	// late — redelivery.
	Project(ctx context.Context, ev PolicyChanged) (applied bool, err error)
}

// KillSwitch is the platform-wide egress override (ADR-231 D6). One row, no
// tenant scope: engaging it denies grounded egress for EVERY tenant, regardless
// of any tenant entitlement.
type KillSwitch struct {
	Engaged       bool
	Reason        string
	UpdatedByGCID string
	UpdatedAt     time.Time
}

// KillSwitchRepository reads and writes the platform egress kill-switch.
type KillSwitchRepository interface {
	Get(ctx context.Context) (KillSwitch, error)

	// Set flips the switch. actorGCID is required — disabling web egress for the
	// entire platform is never anonymous.
	Set(ctx context.Context, engaged bool, reason, actorGCID string) (KillSwitch, error)
}
