// Specs for the O+ Learning Companion containment endpoint (ADR-252 D3/D4,
// ADR-254 D7). The role matrix is the load-bearing part: platform scope is
// PLATFORM_OPERATOR only; tenant scope is that tenant's admin / owner (or the
// operator); auditors read and never write; an absent role DENIES.
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
	cs "github.com/5007-Capstone/chora/services/chora-observability/internal/domain/companionsuspension"
)

const (
	csTestActor  = "00000000-0000-7000-8000-000000001999"
	csTestTenant = "11111111-1111-7111-8111-111111111111"
	csOtherT     = "22222222-2222-7222-8222-222222222222"
)

type fakeSuspensionRepo struct {
	platform     []cs.Suspension
	tenant       map[string][]cs.Suspension
	engageCalls  []cs.EngageRequest
	releaseCalls []cs.ReleaseRequest
	err          error
}

func (f *fakeSuspensionRepo) ListPlatform(context.Context) ([]cs.Suspension, error) {
	return f.platform, f.err
}
func (f *fakeSuspensionRepo) ListTenant(_ context.Context, tenantID string) ([]cs.Suspension, error) {
	return f.tenant[tenantID], f.err
}
func (f *fakeSuspensionRepo) Engage(_ context.Context, req cs.EngageRequest) (cs.Suspension, error) {
	f.engageCalls = append(f.engageCalls, req)
	if f.err != nil {
		return cs.Suspension{}, f.err
	}
	return cs.Suspension{ID: "01990000-0000-7000-8000-0000000000e1", Scope: req.Scope, TenantID: req.TenantID, SkillKey: req.SkillKey,
		Engaged: true, Reason: req.Reason, EngagedBy: req.ActorGCID, EngagedAt: time.Date(2026, 8, 22, 15, 0, 0, 0, time.UTC), Version: 1}, nil
}
func (f *fakeSuspensionRepo) Release(_ context.Context, req cs.ReleaseRequest) ([]cs.Suspension, error) {
	f.releaseCalls = append(f.releaseCalls, req)
	if f.err != nil {
		return nil, f.err
	}
	return []cs.Suspension{{ID: "01990000-0000-7000-8000-0000000000e1", Scope: req.Scope, TenantID: req.TenantID, SkillKey: req.SkillKey,
		Engaged: false, Reason: "earlier", EngagedBy: req.ActorGCID, ReleasedBy: req.ActorGCID, ReleasedAt: time.Now().UTC(), Version: 2}}, nil
}

func csReq(method, body, roles, tenant string) *http.Request {
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, "/api/v1/admin/companion/suspension", nil)
	} else {
		r = httptest.NewRequest(method, "/api/v1/admin/companion/suspension", bytes.NewBufferString(body))
	}
	r.Header.Set(servicemesh.HeaderGCID, csTestActor)
	if roles != "" {
		r.Header.Set(servicemesh.HeaderUserRoles, roles)
	}
	if tenant != "" {
		r = r.WithContext(context.WithValue(r.Context(), ctxKeyTenantID, tenant))
	}
	return r
}

func decodeBody(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &m); err != nil {
		t.Fatalf("body is not JSON: %v: %s", err, w.Body.String())
	}
	return m
}

// --- authz (fail CLOSED) ---------------------------------------------------------

func TestCompanionSuspension_NoRoleHeader_403(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	NewCompanionSuspensionHandler(&fakeSuspensionRepo{}).ServeHTTP(w, csReq(http.MethodGet, "", "", csTestTenant))
	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403: an absent mesh-roles header MUST deny", w.Code)
	}
}

func TestCompanionSuspension_Auditor_CanReadNotWrite(t *testing.T) {
	t.Parallel()
	repo := &fakeSuspensionRepo{platform: []cs.Suspension{{ID: "p1", Scope: cs.ScopePlatform, Engaged: true, Reason: "incident"}}}
	w := httptest.NewRecorder()
	NewCompanionSuspensionHandler(repo).ServeHTTP(w, csReq(http.MethodGet, "", "auditor", csTestTenant))
	if w.Code != http.StatusOK {
		t.Fatalf("auditor GET status = %d, want 200: %s", w.Code, w.Body.String())
	}
	body := decodeBody(t, w)
	if _, ok := body["platform"]; !ok {
		t.Fatalf("GET body must carry platform rows: %v", body)
	}

	w = httptest.NewRecorder()
	NewCompanionSuspensionHandler(repo).ServeHTTP(w, csReq(http.MethodPatch, `{"scope":"tenant","engaged":true,"reason":"x"}`, "auditor", csTestTenant))
	if w.Code != http.StatusForbidden || len(repo.engageCalls) != 0 {
		t.Fatalf("auditor PATCH status = %d (engage calls %d), want 403: auditors observe, never act", w.Code, len(repo.engageCalls))
	}
}

func TestCompanionSuspension_TenantAdmin_PlatformScope_403(t *testing.T) {
	t.Parallel()
	repo := &fakeSuspensionRepo{}
	w := httptest.NewRecorder()
	NewCompanionSuspensionHandler(repo).ServeHTTP(w, csReq(http.MethodPatch, `{"scope":"platform","engaged":true,"reason":"x"}`, "admin,owner", csTestTenant))
	if w.Code != http.StatusForbidden || len(repo.engageCalls) != 0 {
		t.Fatalf("status = %d, want 403: platform scope is PLATFORM_OPERATOR only", w.Code)
	}
}

func TestCompanionSuspension_TenantAdmin_TenantScope_UsesCallerTenant(t *testing.T) {
	t.Parallel()
	repo := &fakeSuspensionRepo{}
	w := httptest.NewRecorder()
	NewCompanionSuspensionHandler(repo).ServeHTTP(w, csReq(http.MethodPatch, `{"scope":"tenant","skill_key":"companion_chat_turn_basic","engaged":true,"reason":"drift"}`, "admin", csTestTenant))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if len(repo.engageCalls) != 1 || repo.engageCalls[0].TenantID != csTestTenant || repo.engageCalls[0].SkillKey != "companion_chat_turn_basic" || repo.engageCalls[0].ActorGCID != csTestActor {
		t.Fatalf("engage request wrong: %+v", repo.engageCalls)
	}
	body := decodeBody(t, w)
	s, _ := body["suspension"].(map[string]any)
	if s == nil || s["scope"] != "tenant" || s["tenant_id"] != csTestTenant || s["engaged"] != true || s["skill_key"] != "companion_chat_turn_basic" {
		t.Fatalf("response suspension DTO wrong: %v", body)
	}
}

func TestCompanionSuspension_TenantAdmin_OtherTenant_403(t *testing.T) {
	t.Parallel()
	repo := &fakeSuspensionRepo{}
	w := httptest.NewRecorder()
	NewCompanionSuspensionHandler(repo).ServeHTTP(w, csReq(http.MethodPatch, `{"scope":"tenant","tenant_id":"`+csOtherT+`","engaged":true,"reason":"x"}`, "owner", csTestTenant))
	if w.Code != http.StatusForbidden || len(repo.engageCalls) != 0 {
		t.Fatalf("status = %d, want 403: a tenant admin may only contain their OWN tenant", w.Code)
	}
}

func TestCompanionSuspension_Operator_CanTargetAnyTenantAndPlatform(t *testing.T) {
	t.Parallel()
	repo := &fakeSuspensionRepo{}
	w := httptest.NewRecorder()
	NewCompanionSuspensionHandler(repo).ServeHTTP(w, csReq(http.MethodPatch, `{"scope":"tenant","tenant_id":"`+csOtherT+`","engaged":true,"reason":"x"}`, "platform_operator", csTestTenant))
	if w.Code != http.StatusOK || len(repo.engageCalls) != 1 || repo.engageCalls[0].TenantID != csOtherT {
		t.Fatalf("operator cross-tenant engage: status=%d calls=%+v", w.Code, repo.engageCalls)
	}
	w = httptest.NewRecorder()
	NewCompanionSuspensionHandler(repo).ServeHTTP(w, csReq(http.MethodPatch, `{"scope":"platform","engaged":true,"reason":"incident 42"}`, "platform_operator", csTestTenant))
	if w.Code != http.StatusOK || len(repo.engageCalls) != 2 || repo.engageCalls[1].Scope != cs.ScopePlatform {
		t.Fatalf("operator platform engage: status=%d calls=%+v", w.Code, repo.engageCalls)
	}
}

func TestCompanionSuspension_Release_ReturnsReleasedRows(t *testing.T) {
	t.Parallel()
	repo := &fakeSuspensionRepo{}
	w := httptest.NewRecorder()
	NewCompanionSuspensionHandler(repo).ServeHTTP(w, csReq(http.MethodPatch, `{"scope":"platform","engaged":false,"reason":"resolved"}`, "platform_operator", csTestTenant))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body.String())
	}
	if len(repo.releaseCalls) != 1 || repo.releaseCalls[0].Reason != "resolved" {
		t.Fatalf("release call wrong: %+v", repo.releaseCalls)
	}
	body := decodeBody(t, w)
	rel, _ := body["released"].([]any)
	if len(rel) != 1 {
		t.Fatalf("released list missing: %v", body)
	}
}

// --- validation / plumbing -------------------------------------------------------

func TestCompanionSuspension_Validation_422(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"missing engaged": `{"scope":"platform","reason":"x"}`,
		"missing reason":  `{"scope":"platform","engaged":true}`,
		"bad scope":       `{"scope":"galaxy","engaged":true,"reason":"x"}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			repo := &fakeSuspensionRepo{}
			w := httptest.NewRecorder()
			NewCompanionSuspensionHandler(repo).ServeHTTP(w, csReq(http.MethodPatch, body, "platform_operator", csTestTenant))
			if w.Code != http.StatusUnprocessableEntity || len(repo.engageCalls)+len(repo.releaseCalls) != 0 {
				t.Fatalf("status = %d, want 422 with no repo call: %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestCompanionSuspension_InvalidJSON_400(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	NewCompanionSuspensionHandler(&fakeSuspensionRepo{}).ServeHTTP(w, csReq(http.MethodPatch, `{not json`, "platform_operator", csTestTenant))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestCompanionSuspension_NoActor_401(t *testing.T) {
	t.Parallel()
	r := csReq(http.MethodPatch, `{"scope":"platform","engaged":true,"reason":"x"}`, "platform_operator", csTestTenant)
	r.Header.Del(servicemesh.HeaderGCID)
	w := httptest.NewRecorder()
	NewCompanionSuspensionHandler(&fakeSuspensionRepo{}).ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401: a containment change is never anonymous", w.Code)
	}
}

func TestCompanionSuspension_RepoError_FailsLoud(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	NewCompanionSuspensionHandler(&fakeSuspensionRepo{err: errors.New("pg down")}).ServeHTTP(w, csReq(http.MethodGet, "", "platform_operator", csTestTenant))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500: never a fabricated empty state", w.Code)
	}
}

func TestCompanionSuspension_MethodNotAllowed(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	NewCompanionSuspensionHandler(&fakeSuspensionRepo{}).ServeHTTP(w, csReq(http.MethodDelete, "", "platform_operator", csTestTenant))
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", w.Code)
	}
}

// Unwired repo must 503, never a silent "nobody is suspended": that answer is
// indistinguishable from a real one and would hide that the platform has no
// working containment control.
func TestCompanionSuspension_Unwired_503(t *testing.T) {
	t.Parallel()
	h := &Handler{} // no WithCompanionSuspensionRepo option
	w := httptest.NewRecorder()
	h.companionSuspensionRoute(w, csReq(http.MethodGet, "", "platform_operator", csTestTenant))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503: unwired must never answer a plausible default", w.Code)
	}
}
