// Package httpadapter — analytics HTTP route tests.
//
// Migrated + adapted from services/chora-analytics/internal/adapter/http
// at M12.2.E.4. Routes now mount under /api/analytics/* per the
// observability service URL space; tenantContext middleware already
// enforces X-Tenant-Id header for /api/* paths so per-endpoint checks
// are not repeated.
package httpadapter_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	httpadapter "github.com/apollo-chora/chora-observability/internal/adapter/http"
	analyticsinmem "github.com/apollo-chora/chora-observability/internal/analytics/inmem"
)

// newAnalyticsTestServer wires the observability router with only the analytics
// store option enabled, so unrelated endpoints stay 503-friendly.
func newAnalyticsTestServer() (http.Handler, *analyticsinmem.Store) {
	store := analyticsinmem.NewStore()
	// Use the dedicated test constructor that requires only analytics deps.
	h := httpadapter.NewAnalyticsRouter(store)
	return h, store
}

func doA(t *testing.T, h http.Handler, method, path, tenant, body string) *httptest.ResponseRecorder {
	t.Helper()
	var rd *bytes.Reader
	if body != "" {
		rd = bytes.NewReader([]byte(body))
	} else {
		rd = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, rd)
	if tenant != "" {
		req.Header.Set("X-Tenant-Id", tenant)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestAnalytics_Dashboards_RequireTenant(t *testing.T) {
	h, _ := newAnalyticsTestServer()
	for _, p := range []string{
		"/api/analytics/dashboards/learner?gcid=g",
		"/api/analytics/dashboards/instructor?gcid=g",
		"/api/analytics/dashboards/admin?tenant_id=t1",
	} {
		w := doA(t, h, http.MethodGet, p, "", "")
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s without tenant: code=%d want 400", p, w.Code)
		}
	}
}

func TestAnalytics_LearnerDashboard_OK(t *testing.T) {
	h, _ := newAnalyticsTestServer()
	w := doA(t, h, http.MethodGet,
		"/api/analytics/dashboards/learner?gcid=01900000-0000-7000-8000-000000000001", "t1", "")
	if w.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}
}

func TestAnalytics_LearnerDashboard_RequiresGCID(t *testing.T) {
	h, _ := newAnalyticsTestServer()
	w := doA(t, h, http.MethodGet, "/api/analytics/dashboards/learner", "t1", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("code=%d", w.Code)
	}
}

func TestAnalytics_InstructorDashboard_OK(t *testing.T) {
	h, _ := newAnalyticsTestServer()
	w := doA(t, h, http.MethodGet,
		"/api/analytics/dashboards/instructor?gcid=01900000-0000-7000-8000-000000000001", "t1", "")
	if w.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}
}

func TestAnalytics_InstructorDashboard_RequiresGCID(t *testing.T) {
	h, _ := newAnalyticsTestServer()
	w := doA(t, h, http.MethodGet, "/api/analytics/dashboards/instructor", "t1", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("code=%d", w.Code)
	}
}

func TestAnalytics_AdminDashboard_OK_TenantMatch(t *testing.T) {
	h, _ := newAnalyticsTestServer()
	w := doA(t, h, http.MethodGet, "/api/analytics/dashboards/admin?tenant_id=t1", "t1", "")
	if w.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}
}

func TestAnalytics_AdminDashboard_RejectsCrossTenant(t *testing.T) {
	h, _ := newAnalyticsTestServer()
	w := doA(t, h, http.MethodGet, "/api/analytics/dashboards/admin?tenant_id=t-other", "t1", "")
	if w.Code != http.StatusForbidden {
		t.Fatalf("code=%d want 403", w.Code)
	}
}

func TestAnalytics_AdminDashboard_RequiresQueryTenant(t *testing.T) {
	h, _ := newAnalyticsTestServer()
	w := doA(t, h, http.MethodGet, "/api/analytics/dashboards/admin", "t1", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("code=%d", w.Code)
	}
}

func TestAnalytics_Cohorts_CreateThenRetention(t *testing.T) {
	h, _ := newAnalyticsTestServer()
	body := `{"name":"Q2 Onboarders","criteria":{"min_atoms":"5"}}`
	w := doA(t, h, http.MethodPost, "/api/analytics/cohorts", "t1", body)
	if w.Code != http.StatusCreated {
		t.Fatalf("create code=%d body=%s", w.Code, w.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	id, _ := got["id"].(string)
	if id == "" {
		t.Fatal("id empty")
	}

	// Retention curve on new cohort with 0 members → 200 + total_size 0.
	w2 := doA(t, h, http.MethodGet, "/api/analytics/cohorts/"+id+"/retention", "t1", "")
	if w2.Code != http.StatusOK {
		t.Fatalf("retention code=%d body=%s", w2.Code, w2.Body.String())
	}
}

func TestAnalytics_Cohorts_CreateRejectsBlankName(t *testing.T) {
	h, _ := newAnalyticsTestServer()
	w := doA(t, h, http.MethodPost, "/api/analytics/cohorts", "t1", `{"name":"","criteria":{}}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("code=%d", w.Code)
	}
}

func TestAnalytics_Cohorts_CreateRejectsBadJSON(t *testing.T) {
	h, _ := newAnalyticsTestServer()
	w := doA(t, h, http.MethodPost, "/api/analytics/cohorts", "t1", `not-json`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("code=%d", w.Code)
	}
}

func TestAnalytics_Cohorts_RetentionUnknownCohort(t *testing.T) {
	h, _ := newAnalyticsTestServer()
	w := doA(t, h, http.MethodGet,
		"/api/analytics/cohorts/00000000-0000-0000-0000-000000000000/retention", "t1", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("code=%d", w.Code)
	}
}

func TestAnalytics_Cohorts_RetentionUnknownSubpath(t *testing.T) {
	h, _ := newAnalyticsTestServer()
	w := doA(t, h, http.MethodGet, "/api/analytics/cohorts/abc/something-else", "t1", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("code=%d", w.Code)
	}
}

func TestAnalytics_Cohorts_Wrong_Method(t *testing.T) {
	h, _ := newAnalyticsTestServer()
	w := doA(t, h, http.MethodDelete, "/api/analytics/cohorts", "t1", "")
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("code=%d", w.Code)
	}
}

func TestAnalytics_Dashboard_Wrong_Method(t *testing.T) {
	h, _ := newAnalyticsTestServer()
	w := doA(t, h, http.MethodPost, "/api/analytics/dashboards/learner?gcid=g", "t1", "")
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("code=%d", w.Code)
	}
	w2 := doA(t, h, http.MethodPost, "/api/analytics/dashboards/instructor?gcid=g", "t1", "")
	if w2.Code != http.StatusMethodNotAllowed {
		t.Fatalf("code=%d", w2.Code)
	}
	w3 := doA(t, h, http.MethodPost, "/api/analytics/dashboards/admin?tenant_id=t1", "t1", "")
	if w3.Code != http.StatusMethodNotAllowed {
		t.Fatalf("code=%d", w3.Code)
	}
}
