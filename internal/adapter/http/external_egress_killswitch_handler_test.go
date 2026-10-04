// Specs for the O+ platform egress kill-switch endpoint (CHO-2148).
//
// This switch denies external web egress for EVERY tenant at once. The gate is
// the load-bearing part: it must fail CLOSED on an absent or unrecognised role
// (the shape that left twelve chora-tenancy admin gates silently unenforced in
// production until CHO-2072), and it must never accept an anonymous flip.
package httpadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/libs/chora-go-common/auth/servicemesh"
	ee "github.com/5007-Capstone/chora/services/chora-observability/internal/domain/externalegress"
)

const ksActor = "00000000-0000-7000-8000-000000001999"

type fakeKillSwitchRepo struct {
	ks       ee.KillSwitch
	setCalls int
	lastArgs struct {
		engaged bool
		reason  string
		actor   string
	}
	getErr error
	setErr error
}

func (f *fakeKillSwitchRepo) Get(context.Context) (ee.KillSwitch, error) {
	if f.getErr != nil {
		return ee.KillSwitch{}, f.getErr
	}
	return f.ks, nil
}

func (f *fakeKillSwitchRepo) Set(_ context.Context, engaged bool, reason, actor string) (ee.KillSwitch, error) {
	f.setCalls++
	f.lastArgs.engaged = engaged
	f.lastArgs.reason = reason
	f.lastArgs.actor = actor
	if f.setErr != nil {
		return ee.KillSwitch{}, f.setErr
	}
	f.ks = ee.KillSwitch{Engaged: engaged, Reason: reason, UpdatedByGCID: actor, UpdatedAt: time.Now().UTC()}
	return f.ks, nil
}

func ksReq(method, body, roles string) *http.Request {
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, "/api/v1/admin/egress/kill-switch", nil)
	} else {
		r = httptest.NewRequest(method, "/api/v1/admin/egress/kill-switch", bytes.NewBufferString(body))
	}
	r.Header.Set(servicemesh.HeaderGCID, ksActor)
	if roles != "" {
		r.Header.Set(servicemesh.HeaderUserRoles, roles)
	}
	return r
}

// --- authz (fail CLOSED) ---------------------------------------------------

func TestKillSwitch_NoRoleHeader_403(t *testing.T) {
	t.Parallel()
	repo := &fakeKillSwitchRepo{}
	w := httptest.NewRecorder()

	NewEgressKillSwitchHandler(repo).ServeHTTP(w, ksReq(http.MethodGet, "", ""))

	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 — an absent mesh-roles header MUST deny", w.Code)
	}
}

func TestKillSwitch_TenantAdmin_403_OperatorOnly(t *testing.T) {
	t.Parallel()
	repo := &fakeKillSwitchRepo{}
	w := httptest.NewRecorder()

	NewEgressKillSwitchHandler(repo).ServeHTTP(w,
		ksReq(http.MethodPatch, `{"engaged":true}`, "tenant_admin,owner"))

	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 — a tenant admin must NOT be able to disable "+
			"egress for the whole platform", w.Code)
	}
	if repo.setCalls != 0 {
		t.Error("a refused caller must not reach the repository")
	}
}

// A superset role name must not sneak through a prefix/substring match.
func TestKillSwitch_SupersetRoleName_403(t *testing.T) {
	t.Parallel()
	repo := &fakeKillSwitchRepo{}
	w := httptest.NewRecorder()

	NewEgressKillSwitchHandler(repo).ServeHTTP(w,
		ksReq(http.MethodGet, "", "platform_operator_readonly"))

	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 — role matching must be EXACT per entry", w.Code)
	}
}

func TestKillSwitch_Operator_CanRead(t *testing.T) {
	t.Parallel()
	repo := &fakeKillSwitchRepo{ks: ee.KillSwitch{Engaged: false}}
	w := httptest.NewRecorder()

	NewEgressKillSwitchHandler(repo).ServeHTTP(w, ksReq(http.MethodGet, "", "platform_operator"))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var dto map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &dto); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if dto["engaged"] != false {
		t.Errorf("engaged = %v, want false", dto["engaged"])
	}
}

// Roles arrive comma-joined; the operator entry may sit anywhere in the list.
func TestKillSwitch_Operator_AmongOtherRoles_Allowed(t *testing.T) {
	t.Parallel()
	repo := &fakeKillSwitchRepo{}
	w := httptest.NewRecorder()

	NewEgressKillSwitchHandler(repo).ServeHTTP(w,
		ksReq(http.MethodPatch, `{"engaged":true,"reason":"incident-42"}`, "learner,platform_operator"))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	if repo.setCalls != 1 {
		t.Fatalf("Set called %d times, want 1", repo.setCalls)
	}
	if !repo.lastArgs.engaged {
		t.Error("engaged not propagated")
	}
	if repo.lastArgs.reason != "incident-42" {
		t.Errorf("reason = %q, want incident-42", repo.lastArgs.reason)
	}
	if repo.lastArgs.actor != ksActor {
		t.Errorf("actor = %q, want %q — a kill-switch flip is never anonymous",
			repo.lastArgs.actor, ksActor)
	}
}

// --- validation ------------------------------------------------------------

func TestKillSwitch_PATCH_MissingEngaged_422(t *testing.T) {
	t.Parallel()
	repo := &fakeKillSwitchRepo{}
	w := httptest.NewRecorder()

	NewEgressKillSwitchHandler(repo).ServeHTTP(w,
		ksReq(http.MethodPatch, `{"reason":"oops"}`, "platform_operator"))

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 — `engaged` must be explicit; defaulting it "+
			"could silently DISENGAGE the switch", w.Code)
	}
	if repo.setCalls != 0 {
		t.Error("nothing must be written on a validation failure")
	}
}

func TestKillSwitch_PATCH_NoActor_401(t *testing.T) {
	t.Parallel()
	repo := &fakeKillSwitchRepo{}
	r := httptest.NewRequest(http.MethodPatch, "/api/v1/admin/egress/kill-switch",
		bytes.NewBufferString(`{"engaged":true}`))
	r.Header.Set(servicemesh.HeaderUserRoles, "platform_operator")
	// no GCID header
	w := httptest.NewRecorder()

	NewEgressKillSwitchHandler(repo).ServeHTTP(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 — disabling egress platform-wide is never anonymous", w.Code)
	}
	if repo.setCalls != 0 {
		t.Error("an anonymous flip must not be written")
	}
}

func TestKillSwitch_RepoError_FailsLoud(t *testing.T) {
	t.Parallel()
	repo := &fakeKillSwitchRepo{setErr: errors.New("db down")}
	w := httptest.NewRecorder()

	NewEgressKillSwitchHandler(repo).ServeHTTP(w,
		ksReq(http.MethodPatch, `{"engaged":true}`, "platform_operator"))

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 — a failed write must NOT report success", w.Code)
	}
}

func TestKillSwitch_MethodNotAllowed_405(t *testing.T) {
	t.Parallel()
	repo := &fakeKillSwitchRepo{}
	w := httptest.NewRecorder()

	NewEgressKillSwitchHandler(repo).ServeHTTP(w, ksReq(http.MethodDelete, "", "platform_operator"))

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", w.Code)
	}
}

// Unwired repo must 503, never a silent "not engaged" — that answer is
// indistinguishable from a real one and would hide the fact that the platform
// has no working override.
func TestKillSwitch_Unwired_503(t *testing.T) {
	t.Parallel()
	h := &Handler{}
	w := httptest.NewRecorder()

	h.egressKillSwitchRoute(w, ksReq(http.MethodGet, "", "platform_operator"))

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 when unwired", w.Code)
	}
}
