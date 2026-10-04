// final_http_test.go — the last reachable error-mapping branches of the REST
// surface: method guards on sub-resources, repo-error propagation on
// previously uncovered endpoints, and the pure report builder's type-switch
// branches. Everything here runs against in-memory + stub repos (no live DB).
package httpadapter

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/apollo-chora/chora-observability/internal/adapter/inmem"
	"github.com/apollo-chora/chora-observability/internal/domain/decision"
	"github.com/apollo-chora/chora-observability/internal/domain/familiargrowth"
	"github.com/apollo-chora/chora-observability/internal/domain/ledger"
)

func TestTokenUsageSub_MethodAndPathGuards(t *testing.T) {
	t.Parallel()
	h := &Handler{}
	base := func(method, path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(method, path, nil)
		r.Header.Set("X-Tenant-Id", "t-1")
		h.tokenUsageSub(w, r)
		return w
	}
	if w := base(http.MethodPatch, "/api/token-usage/budget"); w.Code != http.StatusMethodNotAllowed {
		t.Errorf("budget PATCH = %d; want 405", w.Code)
	}
	if w := base(http.MethodDelete, "/api/token-usage/cost"); w.Code != http.StatusMethodNotAllowed {
		t.Errorf("cost DELETE = %d; want 405", w.Code)
	}
	if w := base(http.MethodDelete, "/api/token-usage/aggregate"); w.Code != http.StatusMethodNotAllowed {
		t.Errorf("aggregate DELETE = %d; want 405", w.Code)
	}
	if w := base(http.MethodGet, "/api/token-usage/bogus"); w.Code != http.StatusNotFound {
		t.Errorf("bogus sub-resource = %d; want 404", w.Code)
	}
	if w := base(http.MethodGet, "/api/token-usage/budget-check"); w.Code == 0 {
		t.Error("budget-check dispatch must run without panicking")
	}
}

func TestCostByAct_ErrorPaths(t *testing.T) {
	t.Parallel()
	h := &Handler{ledgers: &failLedgerHTTP{}}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/cost/by-act", nil)
	r.Header.Set("X-Tenant-Id", "t-1")
	h.costByAct(w, r)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("repo error = %d; want 500", w.Code)
	}
	w = httptest.NewRecorder()
	h.costByAct(w, httptest.NewRequest(http.MethodPut, "/api/cost/by-act", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("PUT = %d; want 405", w.Code)
	}
}

func TestPhase6Events_NonGetIs405(t *testing.T) {
	t.Parallel()
	h := newPhase6EventsHandler(inmem.NewDecisionRepository())
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/events", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST = %d; want 405", w.Code)
	}
}

func TestSpanstoreAgentDecisions_RepoErrorIs500(t *testing.T) {
	t.Parallel()
	h := &Handler{decisions: &failDecisionHTTP{}}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/v1/observability/agent-decisions?run_id=x", nil)
	r.Header.Set("X-Tenant-Id", "t-1")
	r.Header.Set("Authorization", "Bearer token")
	r.Header.Set("X-Chora-Role", "observer")
	h.spanstoreAgentDecisions(w, r)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("repo error = %d; want 500", w.Code)
	}
}

func TestCallerGCID_LegacyHeaderFallback(t *testing.T) {
	t.Parallel()
	// http.Header.Get canonicalises keys ("X-Gcid"), so the map literal must
	// use the canonical spelling.
	if got := callerGCID(map[string][]string{"X-Gcid": {"legacy-gcid"}}); got != "legacy-gcid" {
		t.Errorf("legacy header = %q; want legacy-gcid", got)
	}
	if got := callerGCID(map[string][]string{"Gcid": {"bare-gcid"}}); got != "bare-gcid" {
		t.Errorf("bare gcid = %q; want bare-gcid", got)
	}
	if got := callerGCID(map[string][]string{}); got != "" {
		t.Errorf("absent = %q; want empty", got)
	}
}

func TestBreedDistributionReport_TypeSwitchBranches(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 5, 13, 0, 0, 0, 0, time.UTC)
	rolls := []familiargrowth.BreedRollAuditRow{
		{
			TenantID: "t-1", EggSKU: "egg-a", Species: "common",
			RevealedAt: t0,
			DistributionSnapshot: map[string]any{
				"common":  float64(0.5), // already 0-1
				"rare":    float32(25),  // percent (float32)
				"epic":    int64(25),    // percent (int64)
				"legend":  int(0),       // percent (int)
				"unknown": "nope",       // ignored type
			},
		},
	}
	report := buildBreedDistributionReport("t-1", familiargrowth.AuditFilter{EggSKU: "egg-a", From: t0, To: t0}, rolls)
	if report.SampleSize != 1 {
		t.Fatalf("sample = %d; want 1", report.SampleSize)
	}
	// normalised: common 0.5 stays, 25% -> 0.25
	if got := report.Claimed["common"]; got != 0.5 {
		t.Errorf("common = %v; want 0.5", got)
	}
	if got := report.Claimed["rare"]; got != 0.25 {
		t.Errorf("rare = %v; want 0.25", got)
	}
	if got := report.Claimed["epic"]; got != 0.25 {
		t.Errorf("epic = %v; want 0.25", got)
	}
	if report.Claimed["legend"] != 0 {
		t.Errorf("legend = %v; want 0", report.Claimed["legend"])
	}
	if _, present := report.Claimed["unknown"]; present {
		t.Error("non-numeric snapshot type must not be claimed")
	}
	if report.ChiSquare == nil {
		t.Error("expected chi-square when claimed distribution present")
	}

	// empty roll list + empty snapshot -> zero-report branches
	empty := buildBreedDistributionReport("t-1", familiargrowth.AuditFilter{EggSKU: "egg-a"}, nil)
	if empty.SampleSize != 0 || len(empty.Claimed) != 0 {
		t.Errorf("empty report = %+v", empty)
	}
	noSnapshot := buildBreedDistributionReport("t-1", familiargrowth.AuditFilter{EggSKU: "egg-a"},
		[]familiargrowth.BreedRollAuditRow{{TenantID: "t-1", Species: "common", DistributionSnapshot: nil, RevealedAt: t0}})
	if noSnapshot.ChiSquare != nil {
		t.Error("no snapshot -> no chi-square")
	}
}

// --- fail-Loud stub repos ------------------------------------------------

type failLedgerHTTP struct{}

func (*failLedgerHTTP) Append(context.Context, *ledger.Entry) error { return errors.New("boom") }
func (*failLedgerHTTP) List(context.Context, string, ledger.ListFilter) ([]*ledger.Entry, error) {
	return nil, errors.New("boom")
}
func (*failLedgerHTTP) SumCost(context.Context, string, ledger.ListFilter) (int64, int, error) {
	return 0, 0, errors.New("boom")
}

type failDecisionHTTP struct{}

func (*failDecisionHTTP) Append(context.Context, *decision.Log) error { return nil }
func (*failDecisionHTTP) List(context.Context, string, decision.ListFilter) ([]*decision.Log, error) {
	return nil, errors.New("boom")
}
func (*failDecisionHTTP) GetByID(context.Context, string, string) (*decision.Log, error) {
	return nil, errors.New("boom")
}
func (*failDecisionHTTP) Count(context.Context, string, time.Time, time.Time) (int64, error) {
	return 0, nil
}
