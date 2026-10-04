package companionsuspension_test

import (
	"errors"
	"testing"
	"time"

	cs "github.com/apollo-chora/chora-observability/internal/domain/companionsuspension"
)

const (
	tenantA = "11111111-1111-7111-8111-111111111111"
	actor   = "00000000-0000-7000-8000-000000001999"
)

// --- EngageRequest.Validate ---------------------------------------------------

func TestEngageRequest_Validate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		req  cs.EngageRequest
		want error
	}{
		{"platform all skills", cs.EngageRequest{Scope: cs.ScopePlatform, Reason: "incident", ActorGCID: actor}, nil},
		{"platform one skill", cs.EngageRequest{Scope: cs.ScopePlatform, SkillKey: "companion_skill_cite_atom", Reason: "leak", ActorGCID: actor}, nil},
		{"tenant all skills", cs.EngageRequest{Scope: cs.ScopeTenant, TenantID: tenantA, Reason: "drift", ActorGCID: actor}, nil},
		{"bad scope", cs.EngageRequest{Scope: "global", Reason: "x", ActorGCID: actor}, cs.ErrInvalidScope},
		{"tenant scope without tenant", cs.EngageRequest{Scope: cs.ScopeTenant, Reason: "x", ActorGCID: actor}, cs.ErrEmptyTenantID},
		{"platform scope with tenant", cs.EngageRequest{Scope: cs.ScopePlatform, TenantID: tenantA, Reason: "x", ActorGCID: actor}, cs.ErrTenantOnPlatformScope},
		{"no reason", cs.EngageRequest{Scope: cs.ScopePlatform, Reason: "   ", ActorGCID: actor}, cs.ErrEmptyReason},
		{"no actor", cs.EngageRequest{Scope: cs.ScopePlatform, Reason: "x"}, cs.ErrEmptyActor},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.req.Validate()
			if !errors.Is(got, tc.want) {
				t.Fatalf("Validate() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestReleaseRequest_Validate_RequiresReasonAndActor(t *testing.T) {
	t.Parallel()
	ok := cs.ReleaseRequest{Scope: cs.ScopeTenant, TenantID: tenantA, Reason: "resolved", ActorGCID: actor}
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid release rejected: %v", err)
	}
	if err := (cs.ReleaseRequest{Scope: cs.ScopePlatform, ActorGCID: actor}).Validate(); !errors.Is(err, cs.ErrEmptyReason) {
		t.Fatalf("release without a reason must fail (audit trail), got %v", err)
	}
	if err := (cs.ReleaseRequest{Scope: cs.ScopePlatform, Reason: "r"}).Validate(); !errors.Is(err, cs.ErrEmptyActor) {
		t.Fatalf("release without an actor must fail, got %v", err)
	}
}

// --- role matrix (ADR-252 D3) --------------------------------------------------

func TestCanWrite_RoleMatrix(t *testing.T) {
	t.Parallel()
	cases := []struct {
		roles []string
		scope cs.Scope
		want  bool
	}{
		{[]string{"platform_operator"}, cs.ScopePlatform, true},
		{[]string{"platform_operator"}, cs.ScopeTenant, true},
		{[]string{"admin"}, cs.ScopeTenant, true},
		{[]string{"owner"}, cs.ScopeTenant, true},
		{[]string{"admin"}, cs.ScopePlatform, false},
		{[]string{"owner"}, cs.ScopePlatform, false},
		{[]string{"auditor"}, cs.ScopeTenant, false}, // auditors observe, never act
		{[]string{"auditor"}, cs.ScopePlatform, false},
		{[]string{"learner"}, cs.ScopeTenant, false},
		{nil, cs.ScopeTenant, false},
		{[]string{"platform_operator_readonly"}, cs.ScopePlatform, false}, // exact match only
		{[]string{"Platform-Operator"}, cs.ScopePlatform, true},           // case + dash normalised
	}
	for _, tc := range cases {
		if got := cs.CanWrite(tc.roles, tc.scope); got != tc.want {
			t.Errorf("CanWrite(%v, %s) = %v, want %v", tc.roles, tc.scope, got, tc.want)
		}
	}
}

func TestCanRead_RoleMatrix(t *testing.T) {
	t.Parallel()
	for _, r := range []string{"platform_operator", "auditor", "admin", "owner"} {
		if !cs.CanRead([]string{r}) {
			t.Errorf("CanRead(%s) = false, want true", r)
		}
	}
	for _, r := range []string{"learner", "instructor", "", "auditor_readonly"} {
		if cs.CanRead([]string{r}) {
			t.Errorf("CanRead(%q) = true, want false", r)
		}
	}
	if cs.CanRead(nil) {
		t.Error("CanRead(nil) must be false (fail closed)")
	}
}

// --- audit event --------------------------------------------------------------

func TestChangedEventFor_CarriesEveryAuditField(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 22, 15, 0, 0, 0, time.UTC)
	s := cs.Suspension{
		ID: "01990000-0000-7000-8000-000000000001", Scope: cs.ScopeTenant, TenantID: tenantA,
		SkillKey: "companion_chat_turn_basic", Engaged: false, Reason: "drift", EngagedBy: actor,
		EngagedAt: at.Add(-time.Hour), ReleasedBy: actor, ReleasedAt: at, Version: 2,
	}
	ev := cs.ChangedEventFor(s, "resolved", actor, "01990000-0000-7000-8000-0000000000ee", at)
	if ev.EventID == "" || ev.SuspensionID != s.ID || ev.Scope != cs.ScopeTenant || ev.TenantID != tenantA ||
		ev.SkillKey != s.SkillKey || ev.Engaged || ev.Version != 2 || ev.Reason != "resolved" ||
		ev.ActorGCID != actor || !ev.ChangedAt.Equal(at) {
		t.Fatalf("event is missing an audit field: %+v", ev)
	}
	if ev.IdempotencyKey() != s.ID+":2" {
		t.Fatalf("idempotency key must be suspension_id:version, got %q", ev.IdempotencyKey())
	}
}

func TestScope_Valid(t *testing.T) {
	t.Parallel()
	if !cs.ScopePlatform.Valid() || !cs.ScopeTenant.Valid() || cs.Scope("x").Valid() {
		t.Fatal("Scope.Valid wrong")
	}
}
