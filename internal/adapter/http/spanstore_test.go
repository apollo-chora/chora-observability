// spanstore_test.go — the Spanstore query API for the O+ Decision-Log
// Explorer (Stage 6): GET /api/v1/observability/spans + the RLS-scoped
// GET /api/v1/observability/agent-decisions. The role gate accepts
// Authorization: Bearer + X-Chora-Role in {observer, auditor} (in-memory MVP).
package httpadapter_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	httpadapter "github.com/apollo-chora/chora-observability/internal/adapter/http"
	"github.com/apollo-chora/chora-observability/internal/adapter/inmem"
	"github.com/apollo-chora/chora-observability/internal/domain/decision"
)

// failingTraceExporter returns an error from Export so the 500 mapping is
// exercised (the shared newMockTraceExporter never errors).
type failingTraceExporter struct{}

func (f *failingTraceExporter) Export(_ context.Context, _ httpadapter.TraceExportRequest) (httpadapter.TraceExportResponse, error) {
	return httpadapter.TraceExportResponse{}, errors.New("trace store down")
}

// spanstoreReq builds a GET with the observer role headers.
func spanstoreReq(method, path string) *http.Request {
	r := httptest.NewRequest(method, path, nil)
	r.Header.Set("X-Tenant-Id", tenantA)
	r.Header.Set("Authorization", "Bearer test-token")
	r.Header.Set("X-Chora-Role", "observer")
	return r
}

// routerWithoutTrace builds a router WITHOUT the trace-exporter option.
func routerWithoutTrace(t *testing.T) http.Handler {
	t.Helper()
	return httpadapter.NewRouter(
		inmem.NewLedgerRepository(),
		inmem.NewDecisionRepository(),
		inmem.NewCorrelationRepository(),
	)
}

func TestSpanstoreQuery_EndpointGuardRails(t *testing.T) {
	t.Parallel()
	srv := newServer(t)

	// non-GET -> 405
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, spanstoreReq(http.MethodPost, "/api/v1/observability/spans"))
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST status = %d; want 405", w.Code)
	}

	// missing role headers -> 403
	w = httptest.NewRecorder()
	req := spanstoreReq(http.MethodGet, "/api/v1/observability/spans?trace_id="+traceA)
	req.Header.Del("Authorization")
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Errorf("no-auth status = %d; want 403", w.Code)
	}

	// exporter unconfigured -> 503
	w = httptest.NewRecorder()
	routerWithoutTrace(t).ServeHTTP(w, spanstoreReq(http.MethodGet, "/api/v1/observability/spans?trace_id="+traceA))
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("unwired status = %d; want 503", w.Code)
	}

	// missing trace_id -> 400
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, spanstoreReq(http.MethodGet, "/api/v1/observability/spans"))
	if w.Code != http.StatusBadRequest {
		t.Errorf("missing trace_id status = %d; want 400", w.Code)
	}
}

func TestSpanstoreQuery_Success(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, spanstoreReq(http.MethodGet, "/api/v1/observability/spans?trace_id="+traceA))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 (body=%s)", w.Code, w.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["trace_id"] != traceA {
		t.Errorf("trace_id = %v; want %s", body["trace_id"], traceA)
	}
	if body["export"] == nil {
		t.Error("expected export payload")
	}
}

func TestSpanstoreQuery_ExporterError(t *testing.T) {
	t.Parallel()
	router := httpadapter.NewRouter(
		inmem.NewLedgerRepository(),
		inmem.NewDecisionRepository(),
		inmem.NewCorrelationRepository(),
		httpadapter.WithTraceExporter(&failingTraceExporter{}),
	)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, spanstoreReq(http.MethodGet, "/api/v1/observability/spans?trace_id="+traceA))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d; want 500", w.Code)
	}
	if !strings.Contains(w.Body.String(), "OBS_SPANSTORE_ERROR") {
		t.Errorf("body = %s; want OBS_SPANSTORE_ERROR", w.Body.String())
	}
}

func TestSpanstoreAgentDecisions_GuardRails(t *testing.T) {
	t.Parallel()
	srv := newServer(t)

	// non-GET -> 405
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, spanstoreReq(http.MethodPost, "/api/v1/observability/agent-decisions"))
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST status = %d; want 405", w.Code)
	}

	// missing role -> 403
	w = httptest.NewRecorder()
	req := spanstoreReq(http.MethodGet, "/api/v1/observability/agent-decisions?run_id=x")
	req.Header.Del("X-Chora-Role")
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Errorf("no-role status = %d; want 403", w.Code)
	}

	// missing run_id -> 400
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, spanstoreReq(http.MethodGet, "/api/v1/observability/agent-decisions"))
	if w.Code != http.StatusBadRequest {
		t.Errorf("missing run_id status = %d; want 400", w.Code)
	}
}

func TestSpanstoreAgentDecisions_Success(t *testing.T) {
	t.Parallel()
	repos := inmem.NewDecisionRepository()
	d, err := decision.New(decision.NewParams{
		TenantID: tenantA, Agid: agidA,
		DecisionType: decision.TypeRoute, RiskTier: decision.TierLow,
		CorrelationID: "run-42",
	})
	if err != nil {
		t.Fatalf("decision.New: %v", err)
	}
	if err := repos.Append(context.Background(), d); err != nil {
		t.Fatalf("append: %v", err)
	}
	router := httpadapter.NewRouter(
		inmem.NewLedgerRepository(),
		repos,
		inmem.NewCorrelationRepository(),
		httpadapter.WithTraceExporter(newMockTraceExporter()),
	)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, spanstoreReq(http.MethodGet, "/api/v1/observability/agent-decisions?run_id=run-42"))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 (body=%s)", w.Code, w.Body.String())
	}
	var body struct {
		RunID string `json:"run_id"`
		Total int    `json:"total"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.RunID != "run-42" || body.Total != 1 {
		t.Errorf("body = %+v; want run_id=run-42 total=1", body)
	}
}
