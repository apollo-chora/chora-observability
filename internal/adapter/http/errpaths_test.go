// errpaths_test.go — exercises the error-mapping branches across the REST
// handlers that happy-path tests cannot reach: repo failures (via stub repos
// whose methods always error), parse failures, method guards, and the
// optional-port 503 paths. Also drives the familiar-growth + ritual-audit
// handlers directly (bypassing tenantContext) to reach the resolveTenant
// header/query fallbacks and the limit-bound branches.
package httpadapter_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	httpadapter "github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/http"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/inmem"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/agents"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/correlation"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/decision"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/familiargrowth"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/ledger"
)

// failLedger implements ledger.Repository with fail-Loud errors.
type failLedger struct{ err error }

func (f *failLedger) Append(_ context.Context, _ *ledger.Entry) error { return f.err }
func (f *failLedger) List(_ context.Context, _ string, _ ledger.ListFilter) ([]*ledger.Entry, error) {
	return nil, f.err
}
func (f *failLedger) SumCost(_ context.Context, _ string, _ ledger.ListFilter) (int64, int, error) {
	return 0, 0, f.err
}

// failDecision implements decision.Repository with fail-Loud errors.
type failDecision struct{ err error }

func (f *failDecision) Append(_ context.Context, _ *decision.Log) error { return f.err }
func (f *failDecision) List(_ context.Context, _ string, _ decision.ListFilter) ([]*decision.Log, error) {
	return nil, f.err
}
func (f *failDecision) GetByID(_ context.Context, _, _ string) (*decision.Log, error) { return nil, f.err }
func (f *failDecision) Count(_ context.Context, _ string, _, _ time.Time) (int64, error) {
	return 0, f.err
}

// failCorrelation implements correlation.Repository with fail-Loud errors.
type failCorrelation struct{ err error }

func (f *failCorrelation) Register(_ context.Context, _ *correlation.Correlation) error { return f.err }
func (f *failCorrelation) Get(_ context.Context, _, _ string) (*correlation.Correlation, error) {
	return nil, f.err
}

// failBudget implements ledger.BudgetRepository.
type failBudget struct{ err error }

func (f *failBudget) Set(_ context.Context, _ *ledger.Budget) error { return f.err }
func (f *failBudget) Get(_ context.Context, _, _ string) (*ledger.Budget, error) {
	return nil, f.err
}

const boomer = "connection reset"

func TestTokenUsage_ErrorPaths(t *testing.T) {
	t.Parallel()

	// filter parse error -> 400
	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/token-usage?from=notarfc", nil))
	if w.Code != http.StatusBadRequest {
		t.Errorf("list filter status = %d; want 400", w.Code)
	}
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/token-usage/cost?from=notarfc", nil))
	if w.Code != http.StatusBadRequest {
		t.Errorf("cost filter status = %d; want 400", w.Code)
	}

	// repo error -> 500 on list + cost
	fail := httpadapter.NewRouter(&failLedger{err: errors.New(boomer)}, inmem.NewDecisionRepository(), inmem.NewCorrelationRepository())
	w = httptest.NewRecorder()
	fail.ServeHTTP(w, authedReq(http.MethodGet, "/api/token-usage", nil))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("list repo status = %d; want 500", w.Code)
	}
	w = httptest.NewRecorder()
	fail.ServeHTTP(w, authedReq(http.MethodGet, "/api/token-usage/cost", nil))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("cost repo status = %d; want 500", w.Code)
	}

	// ledger SumCost overflow -> 500
	overflowLedger := inmem.NewLedgerRepository()
	for _, c := range []int64{int64(^uint64(0) >> 1), 1} {
		e, _ := ledger.New(ledger.NewParams{
			TenantID: tenantA, Gcid: gcidA, ModelID: "m",
			CostUsdMicros: c, TraceID: traceA, SpanID: spanA,
		})
		_ = overflowLedger.Append(context.Background(), e)
	}
	overflow := httpadapter.NewRouter(overflowLedger, inmem.NewDecisionRepository(), inmem.NewCorrelationRepository())
	w = httptest.NewRecorder()
	overflow.ServeHTTP(w, authedReq(http.MethodGet, "/api/token-usage/cost", nil))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("sum overflow status = %d; want 500", w.Code)
	}

	// append repo error -> 500
	w = httptest.NewRecorder()
	fail.ServeHTTP(w, authedReq(http.MethodPost, "/api/token-usage", map[string]any{
		"gcid": gcidA, "model_id": "m", "prompt_tokens": 1, "cost_usd_micros": 5,
		"trace_id": traceA, "span_id": spanA,
	}))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("append repo status = %d; want 500", w.Code)
	}

	// malformed body -> 400
	w = httptest.NewRecorder()
	bad := httptest.NewRequest(http.MethodPost, "/api/token-usage", bytes.NewBufferString("{bad"))
	bad.Header.Set("X-Tenant-Id", tenantA)
	srv.ServeHTTP(w, bad)
	if w.Code != http.StatusBadRequest {
		t.Errorf("append bad body status = %d; want 400", w.Code)
	}

	// invalid ledger payload (bad trace id) -> 400
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/token-usage", map[string]any{
		"gcid": gcidA, "model_id": "m", "trace_id": "bogus", "span_id": spanA,
	}))
	if w.Code != http.StatusBadRequest {
		t.Errorf("invalid ledger status = %d; want 400", w.Code)
	}
}

func TestAgentDecision_ErrorPaths(t *testing.T) {
	t.Parallel()
	srv := newServer(t)

	// unknown sub-resource + slashes -> 404
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/agent-decisions/", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("empty-id status = %d; want 404", w.Code)
	}
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/agent-decisions/a/b", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("slashed-id status = %d; want 404", w.Code)
	}

	// non-GET on item -> 405
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/agent-decisions/some-id", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("item POST status = %d; want 405", w.Code)
	}

	// unknown id -> 404
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/agent-decisions/01970000-0000-7000-8000-ffffffffffff", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("missing id status = %d; want 404", w.Code)
	}

	// repo error -> 500
	fail := httpadapter.NewRouter(inmem.NewLedgerRepository(), &failDecision{err: errors.New(boomer)}, inmem.NewCorrelationRepository())
	w = httptest.NewRecorder()
	fail.ServeHTTP(w, authedReq(http.MethodGet, "/api/agent-decisions/01970000-0000-7000-8000-ffffffffffff", nil))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("get repo status = %d; want 500", w.Code)
	}

	// list filter parse -> 400, append repo error -> 500, invalid decision -> 400
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/agent-decisions?from=notarfc", nil))
	if w.Code != http.StatusBadRequest {
		t.Errorf("list filter status = %d; want 400", w.Code)
	}
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/agent-decisions?limit=abc", nil))
	if w.Code != http.StatusBadRequest {
		t.Errorf("list limit status = %d; want 400", w.Code)
	}
	w = httptest.NewRecorder()
	fail.ServeHTTP(w, authedReq(http.MethodPost, "/api/agent-decisions", map[string]any{
		"agid": agidA, "decision_type": "route", "risk_tier": "low", "correlation_id": corrA,
	}))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("append repo status = %d; want 500", w.Code)
	}
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/agent-decisions", map[string]any{
		"agid": agidA, "decision_type": "bogus", "risk_tier": "low", "correlation_id": corrA,
	}))
	if w.Code != http.StatusBadRequest {
		t.Errorf("invalid decision status = %d; want 400", w.Code)
	}
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/agent-decisions", map[string]any{
		"agid": agidA, "decision_type": "route", "risk_tier": "low", "correlation_id": corrA,
		"latency_ms": -3,
	}))
	if w.Code != http.StatusBadRequest {
		t.Errorf("invalid latency status = %d; want 400", w.Code)
	}
}

func TestCorrelation_ErrorPaths(t *testing.T) {
	t.Parallel()
	srv := newServer(t)

	// unknown / missing id -> 404; non-GET -> 405
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/correlations/", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("empty-id status = %d; want 404", w.Code)
	}
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/correlations/missing", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("missing status = %d; want 404", w.Code)
	}
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/correlations/x", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("item POST status = %d; want 405", w.Code)
	}

	// register: bad body -> 400, invalid trace -> 400, invalid child span -> 400
	w = httptest.NewRecorder()
	bad := httptest.NewRequest(http.MethodPost, "/api/correlations", bytes.NewBufferString("{bad"))
	bad.Header.Set("X-Tenant-Id", tenantA)
	srv.ServeHTTP(w, bad)
	if w.Code != http.StatusBadRequest {
		t.Errorf("bad body status = %d; want 400", w.Code)
	}
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/correlations", map[string]any{
		"correlation_id": corrA, "trace_id": "bogus", "parent_span_id": spanA,
	}))
	if w.Code != http.StatusBadRequest {
		t.Errorf("bad trace status = %d; want 400", w.Code)
	}
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/correlations", map[string]any{
		"correlation_id": corrA, "trace_id": traceA, "parent_span_id": spanA,
		"child_spans": []string{"nope"},
	}))
	if w.Code != http.StatusBadRequest {
		t.Errorf("bad child status = %d; want 400", w.Code)
	}

	// repo errors -> 500
	fail := httpadapter.NewRouter(inmem.NewLedgerRepository(), inmem.NewDecisionRepository(), &failCorrelation{err: errors.New(boomer)})
	w = httptest.NewRecorder()
	fail.ServeHTTP(w, authedReq(http.MethodPost, "/api/correlations", map[string]any{
		"correlation_id": corrA, "trace_id": traceA, "parent_span_id": spanA,
	}))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("register repo status = %d; want 500", w.Code)
	}
	w = httptest.NewRecorder()
	fail.ServeHTTP(w, authedReq(http.MethodGet, "/api/correlations/x", nil))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("get repo status = %d; want 500", w.Code)
	}
}

func TestCostAndAggregate_ErrorPaths(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	fail := httpadapter.NewRouter(&failLedger{err: errors.New(boomer)}, inmem.NewDecisionRepository(), inmem.NewCorrelationRepository())

	cases := []struct {
		base string
		bad  string
	}{
		{"/api/cost/cumulative", "/api/cost/cumulative?from=notarfc"},
		{"/api/token-usage/aggregate?group_by=model", "/api/token-usage/aggregate?group_by=model&from=notarfc"},
	}
	for _, c := range cases {
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, authedReq(http.MethodGet, c.bad, nil))
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s filter status = %d; want 400", c.base, w.Code)
		}
		w = httptest.NewRecorder()
		fail.ServeHTTP(w, authedReq(http.MethodGet, c.base, nil))
		if w.Code != http.StatusInternalServerError {
			t.Errorf("%s repo status = %d; want 500", c.base, w.Code)
		}
	}
}

func TestBudget_ErrorPaths(t *testing.T) {
	t.Parallel()

	// no WithBudgetRepo -> 503 on both routes
	unwired := httpadapter.NewRouter(inmem.NewLedgerRepository(), inmem.NewDecisionRepository(), inmem.NewCorrelationRepository())
	for _, req := range []*http.Request{
		authedReq(http.MethodPost, "/api/token-usage/budget", map[string]any{"period": "2026-05", "cap_usd_micros": 100}),
		authedReq(http.MethodGet, "/api/token-usage/budget?period=2026-05", nil),
	} {
		w := httptest.NewRecorder()
		unwired.ServeHTTP(w, req)
		if w.Code != http.StatusServiceUnavailable {
			t.Errorf("unwired budget status = %d; want 503", w.Code)
		}
	}

	srv := newServer(t) // WithBudgetRepo wired
	// missing period -> 400
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/token-usage/budget", nil))
	if w.Code != http.StatusBadRequest {
		t.Errorf("missing period status = %d; want 400", w.Code)
	}
	// unset budget -> 404
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/token-usage/budget?period=2099-01", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("not-found status = %d; want 404", w.Code)
	}
	// invalid budget (zero cap) -> 400
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/token-usage/budget", map[string]any{"period": "x", "cap_usd_micros": 0}))
	if w.Code != http.StatusBadRequest {
		t.Errorf("invalid budget status = %d; want 400", w.Code)
	}
	// malformed body -> 400
	w = httptest.NewRecorder()
	bad := httptest.NewRequest(http.MethodPost, "/api/token-usage/budget", bytes.NewBufferString("{bad"))
	bad.Header.Set("X-Tenant-Id", tenantA)
	srv.ServeHTTP(w, bad)
	if w.Code != http.StatusBadRequest {
		t.Errorf("bad body status = %d; want 400", w.Code)
	}

	// repo errors -> 500
	fail := httpadapter.NewRouter(inmem.NewLedgerRepository(), inmem.NewDecisionRepository(), inmem.NewCorrelationRepository(),
		httpadapter.WithBudgetRepo(&failBudget{err: errors.New(boomer)}))
	w = httptest.NewRecorder()
	fail.ServeHTTP(w, authedReq(http.MethodGet, "/api/token-usage/budget?period=2026-05", nil))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("get repo status = %d; want 500", w.Code)
	}
	w = httptest.NewRecorder()
	fail.ServeHTTP(w, authedReq(http.MethodPost, "/api/token-usage/budget", map[string]any{"period": "2026-05", "cap_usd_micros": 10}))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("set repo status = %d; want 500", w.Code)
	}
}

func TestTracesExport_ErrorPaths(t *testing.T) {
	t.Parallel()
	srv := newServer(t)

	// non-POST -> 405
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/traces/export", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET status = %d; want 405", w.Code)
	}
	// exporter unwired -> 503
	w = httptest.NewRecorder()
	routerWithoutTrace(t).ServeHTTP(w, authedReq(http.MethodPost, "/api/traces/export", map[string]any{
		"since": "2026-05-01T00:00:00Z", "until": "2026-05-02T00:00:00Z",
	}))
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("unwired status = %d; want 503", w.Code)
	}
	// invalid timestamps -> 400; inverted range -> 400
	for _, body := range []map[string]any{
		{"since": "bad", "until": "2026-05-02T00:00:00Z"},
		{"since": "2026-05-02T00:00:00Z", "until": "2026-05-01T00:00:00Z"},
	} {
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/traces/export", body))
		if w.Code != http.StatusBadRequest {
			t.Errorf("body %v status = %d; want 400", body, w.Code)
		}
	}
	// exporter error -> 500
	efail := httpadapter.NewRouter(inmem.NewLedgerRepository(), inmem.NewDecisionRepository(), inmem.NewCorrelationRepository(),
		httpadapter.WithTraceExporter(&failingTraceExporter{}))
	w = httptest.NewRecorder()
	efail.ServeHTTP(w, authedReq(http.MethodPost, "/api/traces/export", map[string]any{
		"since": "2026-05-01T00:00:00Z", "until": "2026-05-02T00:00:00Z",
	}))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("exporter error status = %d; want 500", w.Code)
	}
	// success -> 202
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/traces/export", map[string]any{
		"since": "2026-05-01T00:00:00Z", "until": "2026-05-02T00:00:00Z",
	}))
	if w.Code != http.StatusAccepted {
		t.Errorf("success status = %d; want 202", w.Code)
	}
}

func TestAgents_ErrorPaths(t *testing.T) {
	t.Parallel()
	// non-GET -> 405 on the wired router
	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/v1/observability/agents", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST status = %d; want 405", w.Code)
	}
	// registry unwired -> 503 via WithAgentsRegistry absent (newServer does not add it)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/v1/observability/agents", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("unwired registry status = %d; want 503", w.Code)
	}
	// decision repo error -> 500
	fail := httpadapter.NewRouter(inmem.NewLedgerRepository(), &failDecision{err: errors.New(boomer)}, inmem.NewCorrelationRepository(),
		httpadapter.WithAgentsRegistry(&agents.Registry{Crews: []agents.Entry{{Name: "ai_kernel_orchestrator", Domain: "obs", Language: "python"}}}))
	w = httptest.NewRecorder()
	fail.ServeHTTP(w, authedReq(http.MethodGet, "/api/v1/observability/agents", nil))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("repo status = %d; want 500", w.Code)
	}
}

// ---------------------------------------------------------------------------
// Direct handler calls: resolveTenant fallbacks + pagination bounds (these
// run outside tenantContext, so the header + query fallbacks are reachable).
// ---------------------------------------------------------------------------

func TestFamiliarGrowth_ResolveTenantFallbacks(t *testing.T) {
	t.Parallel()
	repo := inmem.NewFamiliarGrowthRepository()
	h := httpadapter.NewFamiliarGrowthHandler(repo)

	// X-Tenant-Id header (no middleware context) -> resolved from header
	r := httptest.NewRequest(http.MethodGet, "/v1/audit/familiar-growth/events", nil)
	r.Header.Set("X-Tenant-Id", "header-tenant")
	w := httptest.NewRecorder()
	h.ListEvents(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("header-tenant status = %d; want 200", w.Code)
	}

	// ?tenant_id= query fallback
	r = httptest.NewRequest(http.MethodGet, "/v1/audit/familiar-growth/events?tenant_id=query-tenant", nil)
	w = httptest.NewRecorder()
	h.ListEvents(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("query-tenant status = %d; want 200", w.Code)
	}

	// neither -> 400
	r = httptest.NewRequest(http.MethodGet, "/v1/audit/familiar-growth/events", nil)
	w = httptest.NewRecorder()
	h.ListEvents(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("no-tenant status = %d; want 400", w.Code)
	}

	// bad filter -> 400
	r = httptest.NewRequest(http.MethodGet, "/v1/audit/familiar-growth/events?tenant_id=t&from=notarfc", nil)
	w = httptest.NewRecorder()
	h.ListEvents(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bad filter status = %d; want 400", w.Code)
	}
}

func TestFamiliarGrowth_DirectHandlerBranches(t *testing.T) {
	t.Parallel()
	tenantReq := func(path string) *http.Request {
		return httptest.NewRequest(http.MethodGet, path, nil)
	}

	// metric + funnel + breed-distribution happy paths with proper filters
	repo := inmem.NewFamiliarGrowthRepository()
	h := httpadapter.NewFamiliarGrowthHandler(repo)

	// BreedDistribution requires egg_sku -> 400 without it
	w := httptest.NewRecorder()
	h.BreedDistribution(w, tenantReq("/v1/audit/familiar-growth/breed-distribution?tenant_id=t"))
	if w.Code != http.StatusBadRequest {
		t.Errorf("no-sku status = %d; want 400", w.Code)
	}

	w = httptest.NewRecorder()
	h.ListMetrics(w, tenantReq("/v1/audit/familiar-growth/metrics?tenant_id=t&source="+familiargrowth.TopicExpAwarded+"&limit=abc"))
	if w.Code != http.StatusBadRequest {
		t.Errorf("bad limit status = %d; want 400", w.Code)
	}

	// EggFunnel happy path
	w = httptest.NewRecorder()
	h.EggFunnel(w, tenantReq("/v1/audit/familiar-growth/egg-funnel?tenant_id=t"))
	if w.Code != http.StatusOK {
		t.Errorf("egg-funnel status = %d; want 200", w.Code)
	}

	// BreedDistribution happy path (empty rolls -> zero-empirical report)
	w = httptest.NewRecorder()
	h.BreedDistribution(w, tenantReq("/v1/audit/familiar-growth/breed-distribution?tenant_id=t&egg_sku=egg-a"))
	if w.Code != http.StatusOK {
		t.Errorf("breed-distribution status = %d; want 200", w.Code)
	}
}

func TestRitualAudit_HandlerBranches(t *testing.T) {
	t.Parallel()
	repo := inmem.NewRitualAuditRepository()
	h := httpadapter.NewRitualAuditHandler(repo)

	// non-GET -> 405
	w := httptest.NewRecorder()
	h.ListRuns(w, httptest.NewRequest(http.MethodPost, "/v1/audit/ritual-runs", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST status = %d; want 405", w.Code)
	}
	// no tenant -> 400
	w = httptest.NewRecorder()
	h.ListRuns(w, httptest.NewRequest(http.MethodGet, "/v1/audit/ritual-runs", nil))
	if w.Code != http.StatusBadRequest {
		t.Errorf("no-tenant status = %d; want 400", w.Code)
	}
	// bad limit -> 400
	w = httptest.NewRecorder()
	h.ListRuns(w, httptest.NewRequest(http.MethodGet, "/v1/audit/ritual-runs?tenant_id=t&limit=abc", nil))
	if w.Code != http.StatusBadRequest {
		t.Errorf("bad limit status = %d; want 400", w.Code)
	}
	// success
	w = httptest.NewRecorder()
	h.ListRuns(w, httptest.NewRequest(http.MethodGet, "/v1/audit/ritual-runs?tenant_id=t&limit=5000", nil))
	if w.Code != http.StatusOK {
		t.Errorf("success status = %d; want 200", w.Code)
	}
}