// wiring_extra_test.go — covers the optional-port wiring surfaces that the
// split route tests bypass: the Handler-level /api/analytics/* wrappers
// (WithAnalyticsStore wired + the uninitialised 503 path) and the
// WithEgressKillSwitchRepo Option on the main router.
package httpadapter_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	analyticsinmem "github.com/5007-Capstone/chora/services/chora-observability/internal/analytics/inmem"
	httpadapter "github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/http"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/inmem"
	ee "github.com/5007-Capstone/chora/services/chora-observability/internal/domain/externalegress"
)

// wiredAnalyticsRouter builds the main router with the analytics store
// wired through the Option.
func wiredAnalyticsRouter(t *testing.T) http.Handler {
	t.Helper()
	return httpadapter.NewRouter(
		inmem.NewLedgerRepository(),
		inmem.NewDecisionRepository(),
		inmem.NewCorrelationRepository(),
		httpadapter.WithAnalyticsStore(analyticsinmem.NewStore()),
	)
}

func TestAnalyticsRoutes_503WhenStoreUnwired(t *testing.T) {
	t.Parallel()
	router := httpadapter.NewRouter(
		inmem.NewLedgerRepository(),
		inmem.NewDecisionRepository(),
		inmem.NewCorrelationRepository(),
	)
	cases := []string{
		"/api/analytics/dashboards/learner?gcid=" + gcidA,
		"/api/analytics/dashboards/instructor?gcid=" + gcidA,
		"/api/analytics/dashboards/admin?tenant_id=" + tenantA,
		"/api/analytics/cohorts",
	}
	for _, path := range cases {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, authedReq(http.MethodGet, path, nil))
		if w.Code != http.StatusServiceUnavailable {
			t.Errorf("%s: status = %d; want 503", path, w.Code)
		}
	}
	// POST to cohorts also 503s unwired
	w := httptest.NewRecorder()
	router.ServeHTTP(w, authedReq(http.MethodPost, "/api/analytics/cohorts", map[string]any{"name": "x"}))
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("cohorts POST: status = %d; want 503", w.Code)
	}
}

func TestAnalyticsRoutes_WiredThroughMainRouter(t *testing.T) {
	t.Parallel()
	router := wiredAnalyticsRouter(t)

	// learner: gcid required
	w := httptest.NewRecorder()
	router.ServeHTTP(w, authedReq(http.MethodGet, "/api/analytics/dashboards/learner", nil))
	if w.Code != http.StatusBadRequest {
		t.Errorf("learner no-gcid status = %d; want 400", w.Code)
	}
	w = httptest.NewRecorder()
	router.ServeHTTP(w, authedReq(http.MethodGet, "/api/analytics/dashboards/learner?gcid="+gcidA, nil))
	if w.Code != http.StatusOK {
		t.Errorf("learner status = %d; want 200 (body=%s)", w.Code, w.Body.String())
	}

	// instructor
	w = httptest.NewRecorder()
	router.ServeHTTP(w, authedReq(http.MethodGet, "/api/analytics/dashboards/instructor?gcid="+gcidA, nil))
	if w.Code != http.StatusOK {
		t.Errorf("instructor status = %d; want 200", w.Code)
	}

	// admin: cross-tenant query param refused
	w = httptest.NewRecorder()
	router.ServeHTTP(w, authedReq(http.MethodGet, "/api/analytics/dashboards/admin?tenant_id=other-tenant", nil))
	if w.Code != http.StatusForbidden {
		t.Errorf("admin cross-tenant status = %d; want 403", w.Code)
	}
	w = httptest.NewRecorder()
	router.ServeHTTP(w, authedReq(http.MethodGet, "/api/analytics/dashboards/admin?tenant_id="+tenantA, nil))
	if w.Code != http.StatusOK {
		t.Errorf("admin status = %d; want 200", w.Code)
	}
}

func TestAnalyticsCohorts_WiredMainRouter(t *testing.T) {
	t.Parallel()
	router := wiredAnalyticsRouter(t)

	// bad JSON -> 400
	w := httptest.NewRecorder()
	bad := httptest.NewRequest(http.MethodPost, "/api/analytics/cohorts", bytes.NewBufferString("{not json"))
	bad.Header.Set("X-Tenant-Id", tenantA)
	router.ServeHTTP(w, bad)
	if w.Code != http.StatusBadRequest {
		t.Errorf("bad json status = %d; want 400", w.Code)
	}

	// create cohort -> 201
	w = httptest.NewRecorder()
	router.ServeHTTP(w, authedReq(http.MethodPost, "/api/analytics/cohorts",
		map[string]any{"name": "Cohort A", "criteria": map[string]string{"gcid": gcidA}}))
	if w.Code != http.StatusCreated {
		t.Fatalf("create status = %d; want 201 (body=%s)", w.Code, w.Body.String())
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil || created.ID == "" {
		t.Fatalf("create decode = %+v (err=%v)", created, err)
	}

	// retention: no members -> 200 zero-size curve
	w = httptest.NewRecorder()
	router.ServeHTTP(w, authedReq(http.MethodGet, "/api/analytics/cohorts/"+created.ID+"/retention", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("retention status = %d; want 200 (body=%s)", w.Code, w.Body.String())
	}
	var curve struct {
		TotalSize int `json:"total_size"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &curve); err != nil || curve.TotalSize != 0 {
		t.Errorf("retention curve = %+v; want total_size 0", curve)
	}

	// unknown sub-path -> 404
	w = httptest.NewRecorder()
	router.ServeHTTP(w, authedReq(http.MethodGet, "/api/analytics/cohorts/"+created.ID+"/bogus", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("unknown sub-path status = %d; want 404", w.Code)
	}

	// members list error / unknown cohort -> 404
	w = httptest.NewRecorder()
	router.ServeHTTP(w, authedReq(http.MethodGet, "/api/analytics/cohorts/nope/retention", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("unknown cohort status = %d; want 404", w.Code)
	}
}

// ---------------------------------------------------------------------------
// WithEgressKillSwitchRepo on the main router
// ---------------------------------------------------------------------------

type wiringKillSwitchRepo struct {
	ks ee.KillSwitch
}

func (f *wiringKillSwitchRepo) Get(context.Context) (ee.KillSwitch, error) {
	return f.ks, nil
}

func (f *wiringKillSwitchRepo) Set(_ context.Context, engaged bool, reason, actor string) (ee.KillSwitch, error) {
	f.ks = ee.KillSwitch{Engaged: engaged, Reason: reason, UpdatedByGCID: actor, UpdatedAt: time.Now().UTC()}
	return f.ks, nil
}

func killSwitchRequest(method, body string) *http.Request {
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, "/api/v1/admin/egress/kill-switch", nil)
	} else {
		r = httptest.NewRequest(method, "/api/v1/admin/egress/kill-switch", bytes.NewBufferString(body))
	}
	r.Header.Set("X-Tenant-Id", tenantA)
	r.Header.Set("x-mesh-user-roles", "platform_operator")
	r.Header.Set("chora-gcid", "00000000-0000-7000-8000-000000001999")
	return r
}

func TestKillSwitch_WiredThroughMainRouterOption(t *testing.T) {
	t.Parallel()
	repo := &wiringKillSwitchRepo{}
	router := httpadapter.NewRouter(
		inmem.NewLedgerRepository(),
		inmem.NewDecisionRepository(),
		inmem.NewCorrelationRepository(),
		httpadapter.WithEgressKillSwitchRepo(repo),
	)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, killSwitchRequest(http.MethodGet, ""))
	if w.Code != http.StatusOK {
		t.Fatalf("GET status = %d; want 200 (body=%s)", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"engaged":false`) {
		t.Errorf("GET body = %s", w.Body.String())
	}

	// PATCH flips the switch (actor header present)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, killSwitchRequest(http.MethodPatch, `{"engaged":true,"reason":"incident-77"}`))
	if w.Code != http.StatusOK {
		t.Fatalf("PATCH status = %d; want 200 (body=%s)", w.Code, w.Body.String())
	}
	if !repo.ks.Engaged {
		t.Error("expected kill switch engaged after PATCH")
	}
}