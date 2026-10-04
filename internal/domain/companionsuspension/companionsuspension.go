// Package companionsuspension holds the chora-observability side of the
// Learning Companion containment control (ADR-252, wire fields in ADR-254 D7,
// advisory projection in ADR-254 D11).
//
// An O+ operator can contain the Learning Companion WITHOUT a deploy: platform-
// wide, per tenant, or per skill (skill_key = the mana action code that names
// the skill; empty = every companion turn in that scope). Observability OWNS the
// two tables (platform_companion_suspension, companion_suspension_policy,
// migration 0018) and the operator write path; chora-model-gateway READS them
// uncached on every companion turn and is the control (deny-before-debit);
// chora-consumption never reads them and projects the audit event instead.
//
// Every engage and every release is audited durably through
// chora.governance.audit.companion_suspension_changed.v1, written into the
// outbox in the SAME transaction as the row (ADR-252 D4). Denied turns are NOT
// individually audited (counted + spanned at the gateway).
package companionsuspension

import (
	"context"
	"errors"
	"strings"
	"time"
)

// Scope is where a suspension applies.
type Scope string

const (
	// ScopePlatform contains every tenant (PLATFORM_OPERATOR only, ADR-252 D3).
	ScopePlatform Scope = "platform"
	// ScopeTenant contains one tenant (its admin / owner, or the operator).
	ScopeTenant Scope = "tenant"
)

// Valid reports whether s is a known scope.
func (s Scope) Valid() bool { return s == ScopePlatform || s == ScopeTenant }

// Roles that appear in the ADR-252 D3 matrix (x-mesh-user-roles entries,
// normalised lower-case with '-' folded to '_').
const (
	RolePlatformOperator = "platform_operator"
	RoleAuditor          = "auditor"
	RoleAdmin            = "admin"
	RoleOwner            = "owner"
)

var (
	ErrInvalidScope          = errors.New("companionsuspension: scope must be platform or tenant")
	ErrEmptyTenantID         = errors.New("companionsuspension: tenant_id is required for tenant scope")
	ErrTenantOnPlatformScope = errors.New("companionsuspension: platform scope carries no tenant_id")
	ErrEmptyReason           = errors.New("companionsuspension: reason is required (an unexplained containment is not auditable)")
	ErrEmptyActor            = errors.New("companionsuspension: actor gcid is required (a containment change is never anonymous)")
)

// Suspension is one containment row (platform or tenant table). Release is a
// state change on the row, never a delete: the history survives.
type Suspension struct {
	ID       string
	Scope    Scope
	TenantID string // empty for platform scope
	SkillKey string // empty = every companion turn in the scope
	Engaged  bool
	Reason   string
	// EngagedBy / EngagedAt name who engaged it and when.
	EngagedBy string
	EngagedAt time.Time
	// ReleasedBy / ReleasedAt are set when Engaged is false.
	ReleasedBy string
	ReleasedAt time.Time
	// Version is the per-row monotonic counter the consumption projection guards
	// on (ADR-254 D11): engage = 1, release = 2.
	Version int64
}

// EngageRequest asks to contain (scope, tenant, skill).
type EngageRequest struct {
	Scope     Scope
	TenantID  string
	SkillKey  string
	Reason    string
	ActorGCID string
}

// Validate rejects a request that must not be written.
func (r EngageRequest) Validate() error {
	return validateKey(r.Scope, r.TenantID, r.Reason, r.ActorGCID)
}

// ReleaseRequest asks to release every engaged row matching (scope, tenant,
// skill). The release reason rides the audit event.
type ReleaseRequest struct {
	Scope     Scope
	TenantID  string
	SkillKey  string
	Reason    string
	ActorGCID string
}

// Validate rejects a request that must not be written. A release names its
// reason too: the trail must answer "why was it lifted" as well as "why was it
// engaged" (IMDA D1).
func (r ReleaseRequest) Validate() error {
	return validateKey(r.Scope, r.TenantID, r.Reason, r.ActorGCID)
}

func validateKey(scope Scope, tenantID, reason, actor string) error {
	if !scope.Valid() {
		return ErrInvalidScope
	}
	if scope == ScopeTenant && strings.TrimSpace(tenantID) == "" {
		return ErrEmptyTenantID
	}
	if scope == ScopePlatform && strings.TrimSpace(tenantID) != "" {
		return ErrTenantOnPlatformScope
	}
	if strings.TrimSpace(reason) == "" {
		return ErrEmptyReason
	}
	if strings.TrimSpace(actor) == "" {
		return ErrEmptyActor
	}
	return nil
}

// NormalizeRole folds a mesh role entry to the matrix spelling: lower-case,
// trimmed, '-' -> '_'. Exact-entry matching afterwards (a superset name such as
// "platform_operator_readonly" never passes).
func NormalizeRole(role string) string {
	return strings.ReplaceAll(strings.ToLower(strings.TrimSpace(role)), "-", "_")
}

func hasRole(roles []string, want ...string) bool {
	for _, r := range roles {
		n := NormalizeRole(r)
		for _, w := range want {
			if n == w {
				return true
			}
		}
	}
	return false
}

// CanWrite is the ADR-252 D3 write matrix: platform scope is PLATFORM_OPERATOR
// only; tenant scope is the tenant's admin or owner, or the operator. Auditors
// observe and never act. Fail closed on anything else.
func CanWrite(roles []string, scope Scope) bool {
	switch scope {
	case ScopePlatform:
		return hasRole(roles, RolePlatformOperator)
	case ScopeTenant:
		return hasRole(roles, RolePlatformOperator, RoleAdmin, RoleOwner)
	default:
		return false
	}
}

// CanRead is the ADR-252 D3 read matrix: operator and auditor read everything;
// a tenant's admin / owner read their tenant's rows (+ the platform rows).
func CanRead(roles []string) bool {
	return hasRole(roles, RolePlatformOperator, RoleAuditor, RoleAdmin, RoleOwner)
}

// IsPlatformOperator reports whether the caller may target ANY tenant.
func IsPlatformOperator(roles []string) bool { return hasRole(roles, RolePlatformOperator) }

// ChangedEvent is the domain view of
// chora.governance.audit.companion_suspension_changed.v1 (ADR-252 D4): one per
// engage and one per release, carrying scope, tenant, skill, engaged/released,
// reason, actor and timestamp.
type ChangedEvent struct {
	EventID      string
	SuspensionID string
	Scope        Scope
	TenantID     string
	SkillKey     string
	Engaged      bool
	Version      int64
	Reason       string
	ActorGCID    string
	ChangedAt    time.Time
}

// IdempotencyKey is suspension_id:version, so a re-emitted change of the same
// row state collapses to one outbox row and one audit row downstream.
func (e ChangedEvent) IdempotencyKey() string {
	return e.SuspensionID + ":" + itoa(e.Version)
}

// ChangedEventFor builds the audit event for the state s now carries.
func ChangedEventFor(s Suspension, reason, actorGCID, eventID string, at time.Time) ChangedEvent {
	return ChangedEvent{
		EventID:      eventID,
		SuspensionID: s.ID,
		Scope:        s.Scope,
		TenantID:     s.TenantID,
		SkillKey:     s.SkillKey,
		Engaged:      s.Engaged,
		Version:      s.Version,
		Reason:       reason,
		ActorGCID:    actorGCID,
		ChangedAt:    at,
	}
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// Repository is the write + read port over the two ADR-252 tables. Every write
// also enqueues the ChangedEvent on the observability outbox in the SAME
// transaction; the dispatcher publishes it by topic.
type Repository interface {
	// ListPlatform returns platform-scope rows, engaged first, newest first.
	ListPlatform(ctx context.Context) ([]Suspension, error)
	// ListTenant returns the tenant's rows under RLS, engaged first, newest first.
	ListTenant(ctx context.Context, tenantID string) ([]Suspension, error)
	// Engage contains (scope, tenant, skill). Idempotent: an identical ENGAGED
	// row is returned as-is (no new row, no new event).
	Engage(ctx context.Context, req EngageRequest) (Suspension, error)
	// Release lifts every engaged row matching (scope, tenant, skill) and returns
	// them (empty when nothing was engaged). One event per released row.
	Release(ctx context.Context, req ReleaseRequest) ([]Suspension, error)
}
