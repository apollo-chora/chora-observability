// Package httpadapter — Phase-6 BFF-facing events handler tests.
//
// Tests the new GET /events?tenant_id=X endpoint added so the chora-gateway
// HTTPUpstream.GetAuditEvents method has a concrete downstream target.
// Returns the AgentDecisionLog entries projected as audit events for the
// caller's tenant.
package httpadapter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/decision"
)

// phase6FakeDecisionRepo is a minimal decision.Repository stub.
type phase6FakeDecisionRepo struct {
	logs []*decision.Log
}

func (f *phase6FakeDecisionRepo) Append(_ context.Context, l *decision.Log) error {
	f.logs = append(f.logs, l)
	return nil
}

func (f *phase6FakeDecisionRepo) List(_ context.Context, tenantID string, _ decision.ListFilter) ([]*decision.Log, error) {
	var out []*decision.Log
	for _, l := range f.logs {
		if l.TenantID == tenantID {
			out = append(out, l)
		}
	}
	return out, nil
}

func (f *phase6FakeDecisionRepo) GetByID(_ context.Context, tenantID, logID string) (*decision.Log, error) {
	for _, l := range f.logs {
		if l.TenantID == tenantID && l.LogID == logID {
			return l, nil
		}
	}
	return nil, decision.ErrNotFound
}

func (f *phase6FakeDecisionRepo) Count(_ context.Context, tenantID string, since, until time.Time) (int64, error) {
	var n int64
	for _, l := range f.logs {
		if l.TenantID != tenantID {
			continue
		}
		if !since.IsZero() && l.CreatedAt.Before(since) {
			continue
		}
		if !until.IsZero() && !l.CreatedAt.Before(until) {
			continue
		}
		n++
	}
	return n, nil
}

func TestPhase6_Events_ByTenantID_Returns200(t *testing.T) {
	repo := &phase6FakeDecisionRepo{
		logs: []*decision.Log{
			{LogID: "log-1", TenantID: "tenant-a", Agid: "agent-1", DecisionType: "policy_check", Reason: "allowed"},
			{LogID: "log-2", TenantID: "tenant-a", Agid: "agent-2", DecisionType: "policy_check", Reason: "denied"},
			{LogID: "log-3", TenantID: "tenant-b", Agid: "agent-3", DecisionType: "policy_check", Reason: "allowed"},
		},
	}
	h := newPhase6EventsHandler(repo)

	req := httptest.NewRequest(http.MethodGet, "/events?tenant_id=tenant-a", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200; body=%s", w.Code, w.Body.String())
	}
	var arr []map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &arr); err != nil {
		t.Fatalf("body unmarshal: %v", err)
	}
	if len(arr) != 2 {
		t.Errorf("events len = %d; want 2 (only tenant-a)", len(arr))
	}
	if arr[0]["tenant_id"] != "tenant-a" {
		t.Errorf("event 0 tenant_id = %v; want tenant-a", arr[0]["tenant_id"])
	}
}

func TestPhase6_Events_MissingTenantID_Returns400(t *testing.T) {
	repo := &phase6FakeDecisionRepo{}
	h := newPhase6EventsHandler(repo)

	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("missing tenant_id: status = %d; want 400", w.Code)
	}
}

func TestPhase6_Events_OnlyGET(t *testing.T) {
	repo := &phase6FakeDecisionRepo{}
	h := newPhase6EventsHandler(repo)

	req := httptest.NewRequest(http.MethodPost, "/events?tenant_id=tenant-a", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST: status = %d; want 405", w.Code)
	}
}

func TestPhase6_Events_EmptyTenantReturnsEmptyArray(t *testing.T) {
	repo := &phase6FakeDecisionRepo{}
	h := newPhase6EventsHandler(repo)

	req := httptest.NewRequest(http.MethodGet, "/events?tenant_id=tenant-none", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", w.Code)
	}
	body := w.Body.String()
	if body != "[]\n" && body != "[]" && body != "null\n" && body != "null" {
		// Accept either empty array or null literal — handler will decide.
		// We just want non-error response.
		var arr []any
		if err := json.Unmarshal(w.Body.Bytes(), &arr); err != nil {
			t.Errorf("body not a valid JSON array/null: %q (err=%v)", body, err)
		}
		if len(arr) != 0 {
			t.Errorf("empty-tenant body should be empty array; got len=%d", len(arr))
		}
	}
}
