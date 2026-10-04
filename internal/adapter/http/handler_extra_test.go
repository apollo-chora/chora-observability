// Additional edge-case tests for the HTTP handler — bumping coverage above
// the 60% adapter gate.
package httpadapter_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	httpadapter "github.com/apollo-chora/chora-observability/internal/adapter/http"
	"github.com/apollo-chora/chora-observability/internal/adapter/inmem"
)

// -----------------------------------------------------------------------------
// Index page + readyz uninitialised path
// -----------------------------------------------------------------------------

func TestIndex_Returns200(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if w.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", w.Code)
	}
	body := w.Body.String()
	if !bytes.Contains([]byte(body), []byte("chora-observability")) {
		t.Errorf("expected service banner; got %s", body)
	}
}

func TestIndex_404OnUnknownAuthedSubpath(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	// /nope is non-public, so middleware applies + we provide tenant header.
	// indexHandler then 404s anything that isn't the bare "/" path.
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/nope", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d; want 404", w.Code)
	}
}

func TestReadyz_503WhenReposNil(t *testing.T) {
	t.Parallel()
	// Build router with all-nil repos.
	router := httpadapter.NewRouter(nil, nil, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d; want 503", w.Code)
	}
}

// -----------------------------------------------------------------------------
// Method-not-allowed paths
// -----------------------------------------------------------------------------

func TestTokenUsage_PutMethodNotAllowed(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPut, "/api/token-usage", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d; want 405", w.Code)
	}
}

func TestTokenUsageCost_PostMethodNotAllowed(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/token-usage/cost", map[string]any{}))
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d; want 405", w.Code)
	}
}

func TestTokenUsage_UnknownSubpath404(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/token-usage/random", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d; want 404", w.Code)
	}
}

func TestAgentDecisions_PutMethodNotAllowed(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPut, "/api/agent-decisions", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d; want 405", w.Code)
	}
}

func TestCorrelationsCollection_PutMethodNotAllowed(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPut, "/api/correlations", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d; want 405", w.Code)
	}
}

func TestCorrelationsItem_PutMethodNotAllowed(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	// Seed first
	body := map[string]any{
		"correlation_id": corrA,
		"trace_id":       traceA,
		"parent_span_id": spanA,
	}
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/correlations", body))
	if w.Code != http.StatusCreated {
		t.Fatalf("seed failed: %d body=%s", w.Code, w.Body.String())
	}
	w2 := httptest.NewRecorder()
	srv.ServeHTTP(w2, authedReq(http.MethodPut, "/api/correlations/"+corrA, nil))
	if w2.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d; want 405", w2.Code)
	}
}

// -----------------------------------------------------------------------------
// Bad query parameters → 400
// -----------------------------------------------------------------------------

func TestListTokenUsage_BadFromParameter(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet,
		"/api/token-usage?from=not-a-date", nil))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", w.Code)
	}
}

func TestListTokenUsage_BadToParameter(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet,
		"/api/token-usage?to=not-a-date", nil))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", w.Code)
	}
}

func TestListTokenUsage_BadLimit(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet,
		"/api/token-usage?limit=abc", nil))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", w.Code)
	}
}

func TestListTokenUsage_BadOffset(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet,
		"/api/token-usage?offset=abc", nil))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", w.Code)
	}
}

func TestListAgentDecisions_BadFrom(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet,
		"/api/agent-decisions?from=not-a-date", nil))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", w.Code)
	}
}

func TestListAgentDecisions_BadTo(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet,
		"/api/agent-decisions?to=not-a-date", nil))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", w.Code)
	}
}

func TestListAgentDecisions_BadLimit(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet,
		"/api/agent-decisions?limit=abc", nil))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", w.Code)
	}
}

func TestListAgentDecisions_BadOffset(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet,
		"/api/agent-decisions?offset=abc", nil))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", w.Code)
	}
}

func TestListAgentDecisions_AcceptsPagination(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet,
		"/api/agent-decisions?limit=10&offset=0&from=2020-01-01T00:00:00Z&to=2099-01-01T00:00:00Z", nil))
	if w.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", w.Code)
	}
}

// -----------------------------------------------------------------------------
// Bad request bodies → 400
// -----------------------------------------------------------------------------

func TestPostTokenUsage_RejectsBadJSON(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/api/token-usage",
		bytes.NewBufferString(`{"not_a_field":"x"`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-Tenant-Id", tenantA)
	srv.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", w.Code)
	}
}

func TestPostAgentDecision_RejectsBadJSON(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/api/agent-decisions",
		bytes.NewBufferString(`{not_json`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-Tenant-Id", tenantA)
	srv.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", w.Code)
	}
}

func TestPostCorrelation_RejectsBadJSON(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/api/correlations",
		bytes.NewBufferString(`{`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-Tenant-Id", tenantA)
	srv.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", w.Code)
	}
}

func TestPostCorrelation_RejectsBadTrace(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	body := map[string]any{
		"correlation_id": corrA,
		"trace_id":       "tooshort",
		"parent_span_id": spanA,
	}
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/correlations", body))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", w.Code)
	}
}

func TestPostCorrelation_RejectsBadChildSpan(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	body := map[string]any{
		"correlation_id": corrA,
		"trace_id":       traceA,
		"parent_span_id": spanA,
		"child_spans":    []string{"badhex"},
	}
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/correlations", body))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", w.Code)
	}
}

func TestPostCorrelation_AcceptsValidChildSpan(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	body := map[string]any{
		"correlation_id": corrA,
		"trace_id":       traceA,
		"parent_span_id": spanA,
		"child_spans":    []string{"0000000000000002"},
	}
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/correlations", body))
	if w.Code != http.StatusCreated {
		t.Errorf("status = %d body=%s; want 201", w.Code, w.Body.String())
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	cs, _ := got["child_spans"].([]any)
	if len(cs) != 1 {
		t.Errorf("child_spans len = %d; want 1", len(cs))
	}
}

func TestCorrelationsItem_RejectsNestedSubpath(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet,
		"/api/correlations/abc/extra", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d; want 404", w.Code)
	}
}

func TestPostAgentDecision_FallsBackToHeaderTraceparent(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	body := map[string]any{
		"agid":           agidA,
		"decision_type":  "respond",
		"reason":         "ok",
		"risk_tier":      "medium",
		"correlation_id": corrA,
		// no traceparent in body — should fall back to header
	}
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/agent-decisions", body))
	if w.Code != http.StatusCreated {
		t.Errorf("status = %d body=%s; want 201", w.Code, w.Body.String())
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["traceparent"] != tpA {
		t.Errorf("traceparent fallback = %v; want %s", got["traceparent"], tpA)
	}
}

// Ensure the seeded inmem repo factories work standalone too.
func TestRepoFactories_NotNil(t *testing.T) {
	t.Parallel()
	if inmem.NewLedgerRepository() == nil {
		t.Errorf("ledger repo factory returned nil")
	}
	if inmem.NewDecisionRepository() == nil {
		t.Errorf("decision repo factory returned nil")
	}
	if inmem.NewCorrelationRepository() == nil {
		t.Errorf("correlation repo factory returned nil")
	}
}
