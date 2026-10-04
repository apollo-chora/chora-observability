// Package httpadapter_test exercises the chora-observability REST endpoints
// against in-memory repositories.
package httpadapter_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	httpadapter "github.com/apollo-chora/chora-observability/internal/adapter/http"
	"github.com/apollo-chora/chora-observability/internal/adapter/inmem"
	"github.com/apollo-chora/chora-observability/internal/domain/decision"
)

const (
	tenantA = "01970000-0000-7000-8000-000000000001"
	gcidA   = "01970000-0000-7000-9000-000000000001"
	agidA   = "01970000-0000-7000-a000-000000000001"
	corrA   = "01970000-0000-7000-b000-000000000001"
	traceA  = "00000000000000000000000000000001"
	spanA   = "0000000000000001"
	tpA     = "00-00000000000000000000000000000001-0000000000000001-01"
)

// newServer wires the HTTP router with the full optional set (budget repo +
// mock trace exporter) so tests across handler / cost / budget / spanstore
// suites all see the same endpoints. Production main.go applies the same
// option set conditionally on env-driven config.
func newServer(t *testing.T) http.Handler {
	t.Helper()
	return httpadapter.NewRouter(
		inmem.NewLedgerRepository(),
		inmem.NewDecisionRepository(),
		inmem.NewCorrelationRepository(),
		httpadapter.WithBudgetRepo(inmem.NewBudgetRepository()),
		httpadapter.WithTraceExporter(newMockTraceExporter()),
	)
}

// mockTraceExporter satisfies httpadapter.TraceExporter for tests. Returns
// synthetic export metadata.
type mockTraceExporter struct{}

func newMockTraceExporter() httpadapter.TraceExporter { return &mockTraceExporter{} }

func (m *mockTraceExporter) Export(_ context.Context, req httpadapter.TraceExportRequest) (httpadapter.TraceExportResponse, error) {
	return httpadapter.TraceExportResponse{
		ExportID: "01970000-0000-7000-aaaa-bbbbbbbbbbbb",
		Status:   "queued",
		Endpoint: "trace.example.com:4317",
		Mock:     true,
		Since:    req.Since,
		Until:    req.Until,
		QueuedAt: time.Now().UTC(),
	}, nil
}

func authedReq(method, path string, body any) *http.Request {
	var buf *bytes.Buffer
	if body != nil {
		b, _ := json.Marshal(body)
		buf = bytes.NewBuffer(b)
	} else {
		buf = bytes.NewBuffer(nil)
	}
	r := httptest.NewRequest(method, path, buf)
	r.Header.Set("X-Tenant-Id", tenantA)
	r.Header.Set("traceparent", tpA)
	r.Header.Set("Content-Type", "application/json")
	return r
}

// -----------------------------------------------------------------------------
// Health
// -----------------------------------------------------------------------------

func TestHealthz_OK(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz/", nil))
	if w.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", w.Code)
	}
}

func TestReadyz_OK(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if w.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", w.Code)
	}
}

// -----------------------------------------------------------------------------
// Token usage — append (POST /api/token-usage)
// -----------------------------------------------------------------------------

func TestPostTokenUsage_Returns201(t *testing.T) {
	t.Parallel()

	srv := newServer(t)
	w := httptest.NewRecorder()
	body := map[string]any{
		"gcid":              gcidA,
		"model_id":          "gemini-3-pro",
		"prompt_tokens":     100,
		"completion_tokens": 50,
		"cost_usd_micros":   250000,
		"trace_id":          traceA,
		"span_id":           spanA,
	}
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/token-usage", body))
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d body=%s; want 201", w.Code, w.Body.String())
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["ledger_id"] == nil || got["ledger_id"] == "" {
		t.Errorf("ledger_id missing: %v", got)
	}
}

func TestPostTokenUsage_AcceptsNullAgid(t *testing.T) {
	t.Parallel()

	srv := newServer(t)
	w := httptest.NewRecorder()
	body := map[string]any{
		"gcid":            gcidA,
		"model_id":        "gemini-3-pro",
		"prompt_tokens":   10,
		"cost_usd_micros": 100,
		"trace_id":        traceA,
		"span_id":         spanA,
	}
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/token-usage", body))
	if w.Code != http.StatusCreated {
		t.Errorf("status = %d body=%s; want 201", w.Code, w.Body.String())
	}
}

func TestPostTokenUsage_RejectsBadTraceID(t *testing.T) {
	t.Parallel()

	srv := newServer(t)
	w := httptest.NewRecorder()
	body := map[string]any{
		"gcid":            gcidA,
		"model_id":        "gemini-3-pro",
		"prompt_tokens":   10,
		"cost_usd_micros": 100,
		"trace_id":        "abc",
		"span_id":         spanA,
	}
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/token-usage", body))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", w.Code)
	}
}

func TestPostTokenUsage_RejectsMissingTenant(t *testing.T) {
	t.Parallel()

	srv := newServer(t)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/api/token-usage",
		bytes.NewBufferString(`{}`))
	r.Header.Set("Content-Type", "application/json")
	srv.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", w.Code)
	}
}

// -----------------------------------------------------------------------------
// Token usage — list (GET /api/token-usage)
// -----------------------------------------------------------------------------

func TestGetTokenUsage_FiltersByTenant(t *testing.T) {
	t.Parallel()

	srv := newServer(t)
	// Append one
	w := httptest.NewRecorder()
	body := map[string]any{
		"gcid":            gcidA,
		"model_id":        "gemini-3-pro",
		"prompt_tokens":   10,
		"cost_usd_micros": 100,
		"trace_id":        traceA,
		"span_id":         spanA,
	}
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/token-usage", body))
	if w.Code != http.StatusCreated {
		t.Fatalf("seed failed: %d %s", w.Code, w.Body.String())
	}

	// List
	w2 := httptest.NewRecorder()
	srv.ServeHTTP(w2, authedReq(http.MethodGet, "/api/token-usage", nil))
	if w2.Code != http.StatusOK {
		t.Fatalf("list status = %d", w2.Code)
	}
	var resp struct {
		Items []map[string]any `json:"items"`
		Total int              `json:"total"`
	}
	_ = json.Unmarshal(w2.Body.Bytes(), &resp)
	if resp.Total < 1 {
		t.Errorf("expected ≥1 entry; got %d", resp.Total)
	}
	for _, item := range resp.Items {
		if item["tenant_id"] != tenantA {
			t.Errorf("cross-tenant entry leaked: %v", item)
		}
	}
}

// -----------------------------------------------------------------------------
// Cost aggregation
// -----------------------------------------------------------------------------

func TestGetCost_AggregatesCorrectly(t *testing.T) {
	t.Parallel()

	srv := newServer(t)
	// Append three entries: 100 + 250 + 50 = 400 micros
	for _, c := range []int64{100, 250, 50} {
		w := httptest.NewRecorder()
		body := map[string]any{
			"gcid":            gcidA,
			"model_id":        "gemini-3-pro",
			"prompt_tokens":   10,
			"cost_usd_micros": c,
			"trace_id":        traceA,
			"span_id":         spanA,
		}
		srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/token-usage", body))
		if w.Code != http.StatusCreated {
			t.Fatalf("seed (%d) failed: %d %s", c, w.Code, w.Body.String())
		}
	}

	// Cost
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/token-usage/cost", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("cost status = %d body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		TotalMicros int64  `json:"total_cost_usd_micros"`
		TotalUSD    string `json:"total_cost_usd"`
		Count       int    `json:"entry_count"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.TotalMicros != 400 {
		t.Errorf("total_cost_usd_micros = %d; want 400", resp.TotalMicros)
	}
	if resp.Count != 3 {
		t.Errorf("entry_count = %d; want 3", resp.Count)
	}
}

// -----------------------------------------------------------------------------
// Agent decisions
// -----------------------------------------------------------------------------

func TestPostAgentDecision_Returns201(t *testing.T) {
	t.Parallel()

	srv := newServer(t)
	w := httptest.NewRecorder()
	body := map[string]any{
		"agid":           agidA,
		"decision_type":  "route",
		"reason":         "matched cheap-route",
		"risk_tier":      "low",
		"correlation_id": corrA,
		"traceparent":    tpA,
	}
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/agent-decisions", body))
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d body=%s; want 201", w.Code, w.Body.String())
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["log_id"] == nil || got["log_id"] == "" {
		t.Errorf("log_id missing")
	}
}

func TestPostAgentDecision_RejectsInvalidTier(t *testing.T) {
	t.Parallel()

	srv := newServer(t)
	w := httptest.NewRecorder()
	body := map[string]any{
		"agid":           agidA,
		"decision_type":  "route",
		"reason":         "x",
		"risk_tier":      "nuclear",
		"correlation_id": corrA,
		"traceparent":    tpA,
	}
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/agent-decisions", body))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", w.Code)
	}
}

func TestGetAgentDecisions_FiltersByAgid(t *testing.T) {
	t.Parallel()

	srv := newServer(t)
	// Seed: two entries for agidA, none for "other"
	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		body := map[string]any{
			"agid":           agidA,
			"decision_type":  "route",
			"reason":         "x",
			"risk_tier":      "low",
			"correlation_id": corrA,
			"traceparent":    tpA,
		}
		srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/agent-decisions", body))
		if w.Code != http.StatusCreated {
			t.Fatalf("seed failed: %d", w.Code)
		}
	}
	// List by agid
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet,
		"/api/agent-decisions?agid="+agidA, nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	var resp struct {
		Items []map[string]any `json:"items"`
		Total int              `json:"total"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Total != 2 {
		t.Errorf("total = %d; want 2", resp.Total)
	}
}

// TestGetAgentDecisions_SinceWindowNewestFirst proves the O+ Decision Traces
// list (1) honors the `since` query param as an alias for `from` (so the
// gateway's `?since=<now-24h>` window is applied instead of silently dropped),
// and (2) returns rows newest-first. Pre-fix this returned the OLDEST 50 rows
// of all time because `since` was ignored (From stayed zero) + the repo sorted
// recorded_at ASC.
func TestGetAgentDecisions_SinceWindowNewestFirst(t *testing.T) {
	t.Parallel()

	repo := inmem.NewDecisionRepository()
	now := time.Now().UTC()
	seed := func(reason string, createdAt time.Time) {
		d, err := decision.New(decision.NewParams{
			TenantID: tenantA, Agid: agidA,
			DecisionType: decision.TypeRoute, Reason: reason,
			RiskTier: decision.TierLow, CorrelationID: corrA,
			Traceparent: tpA,
		})
		if err != nil {
			t.Fatalf("decision.New: %v", err)
		}
		d.CreatedAt = createdAt
		if err := repo.Append(context.Background(), d); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	// One STALE row (8 days ago) the `since` window must exclude + two recent.
	seed("stale", now.Add(-8*24*time.Hour))
	seed("older-recent", now.Add(-2*time.Hour))
	seed("newest", now.Add(-1*time.Hour))

	srv := httpadapter.NewRouter(
		inmem.NewLedgerRepository(),
		repo,
		inmem.NewCorrelationRepository(),
	)

	since := now.Add(-24 * time.Hour).Format(time.RFC3339)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet,
		"/api/agent-decisions?since="+url.QueryEscape(since), nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		Items []map[string]any `json:"items"`
		Total int              `json:"total"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// `since` (now-24h) dropped the 8-day-old stale row → 2 items.
	if resp.Total != 2 {
		t.Fatalf("total = %d; want 2 (since must drop the 8d-old row)", resp.Total)
	}
	for _, it := range resp.Items {
		if it["reason"] == "stale" {
			t.Fatalf("stale row leaked past the since window")
		}
	}
	// Newest-first.
	if got := resp.Items[0]["reason"]; got != "newest" {
		t.Errorf("items[0].reason = %v; want \"newest\" (newest-first)", got)
	}
	if got := resp.Items[1]["reason"]; got != "older-recent" {
		t.Errorf("items[1].reason = %v; want \"older-recent\"", got)
	}
}

// TestGetAgentDecisions_SerializesPlus9Fields proves GET /api/agent-decisions
// emits the CHO-1560 projection keys EXACTLY as chora-gateway's
// upstream.AgentDecisionRow reads them (model_id / confidence / cost_usd /
// crew_name) so the O+ Decision Traces Model / Confidence / Cost columns render
// non-blank. Seeds the repo directly (the POST create path predates +9).
func TestGetAgentDecisions_SerializesPlus9Fields(t *testing.T) {
	t.Parallel()

	repo := inmem.NewDecisionRepository()
	conf := float32(0.83)
	cost := int64(258)
	d, err := decision.New(decision.NewParams{
		TenantID:         tenantA,
		Agid:             "qgen_critic",
		DecisionType:     decision.TypeRespond,
		Reason:           "PASS",
		RiskTier:         decision.TierLow,
		CorrelationID:    corrA,
		Traceparent:      tpA,
		ModelID:          "vertex_ai/gemini-2.5-flash",
		Confidence:       &conf,
		CostUsdMicros:    &cost,
		CrewName:         "mcq_ai_assist",
		CrewID:           "crew-1",
		PromptTokens:     1000,
		CompletionTokens: 200,
		CachedTokens:     100,
		GuardrailOutcome: "pass",
	})
	if err != nil {
		t.Fatalf("decision.New: %v", err)
	}
	d.PopulateProjectionOut()
	if err := repo.Append(context.Background(), d); err != nil {
		t.Fatalf("Append: %v", err)
	}

	srv := httpadapter.NewRouter(
		inmem.NewLedgerRepository(),
		repo,
		inmem.NewCorrelationRepository(),
	)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/agent-decisions?agid=qgen_critic", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("items = %d; want 1", len(resp.Items))
	}
	it := resp.Items[0]
	for _, key := range []string{"model_id", "confidence", "cost_usd", "crew_name", "guardrail_outcome"} {
		if _, ok := it[key]; !ok {
			t.Errorf("serialized decision missing gateway key %q", key)
		}
	}
	if it["model_id"] != "vertex_ai/gemini-2.5-flash" {
		t.Errorf("model_id = %v", it["model_id"])
	}
	if cu, ok := it["cost_usd"].(float64); !ok || cu < 0.000257 || cu > 0.000259 {
		t.Errorf("cost_usd = %v; want ~0.000258 (258 micros)", it["cost_usd"])
	}
	if it["crew_name"] != "mcq_ai_assist" {
		t.Errorf("crew_name = %v", it["crew_name"])
	}
}

// -----------------------------------------------------------------------------
// Correlations
// -----------------------------------------------------------------------------

func TestPostCorrelation_Returns201(t *testing.T) {
	t.Parallel()

	srv := newServer(t)
	w := httptest.NewRecorder()
	body := map[string]any{
		"correlation_id": corrA,
		"trace_id":       traceA,
		"parent_span_id": spanA,
	}
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/correlations", body))
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d body=%s; want 201", w.Code, w.Body.String())
	}
}

func TestGetCorrelation_Returns200(t *testing.T) {
	t.Parallel()

	srv := newServer(t)
	// Register
	w := httptest.NewRecorder()
	body := map[string]any{
		"correlation_id": corrA,
		"trace_id":       traceA,
		"parent_span_id": spanA,
	}
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/correlations", body))
	if w.Code != http.StatusCreated {
		t.Fatalf("seed failed: %d body=%s", w.Code, w.Body.String())
	}

	// Get
	w2 := httptest.NewRecorder()
	srv.ServeHTTP(w2, authedReq(http.MethodGet, "/api/correlations/"+corrA, nil))
	if w2.Code != http.StatusOK {
		t.Fatalf("get status = %d body=%s", w2.Code, w2.Body.String())
	}
	var got map[string]any
	_ = json.Unmarshal(w2.Body.Bytes(), &got)
	if got["trace_id"] != traceA {
		t.Errorf("trace_id = %v; want %s", got["trace_id"], traceA)
	}
}

func TestGetCorrelation_404OnUnknown(t *testing.T) {
	t.Parallel()

	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet,
		"/api/correlations/01970000-0000-7000-cccc-dddddddddddd", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d; want 404", w.Code)
	}
}

// -----------------------------------------------------------------------------
// Traceparent extraction (echo back)
// -----------------------------------------------------------------------------

func TestTraceparent_EchoedBack(t *testing.T) {
	t.Parallel()

	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz/", nil))
	got := w.Header().Get("traceparent")
	// Health endpoint doesn't require traceparent — should still be safe.
	_ = got // permit empty

	// Authed call WITH traceparent should echo it.
	w2 := httptest.NewRecorder()
	srv.ServeHTTP(w2, authedReq(http.MethodGet, "/api/token-usage", nil))
	if !strings.Contains(w2.Header().Get("traceparent"), traceA) {
		t.Errorf("traceparent not echoed; got %q", w2.Header().Get("traceparent"))
	}
}
