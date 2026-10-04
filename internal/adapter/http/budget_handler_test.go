// Package httpadapter_test exercises /api/token-usage/budget +
// /api/token-usage/aggregate.
package httpadapter_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPostBudget_SetsCap(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	body := map[string]any{
		"period":         "2026-05",
		"cap_usd_micros": 1000000, // $1.00
	}
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/token-usage/budget", body))
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d body=%s; want 201", w.Code, w.Body.String())
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["budget_id"] == nil || got["budget_id"] == "" {
		t.Errorf("budget_id missing")
	}
}

func TestPostBudget_RejectsZeroCap(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	body := map[string]any{
		"period":         "2026-05",
		"cap_usd_micros": 0,
	}
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/token-usage/budget", body))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", w.Code)
	}
}

func TestGetBudget_ReturnsCurrentSpendVsCap(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	// Set the cap first.
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/token-usage/budget", map[string]any{
		"period":         "2026-05",
		"cap_usd_micros": 1000000,
	}))
	if w.Code != http.StatusCreated {
		t.Fatalf("budget seed failed: %d %s", w.Code, w.Body.String())
	}
	// Spend 600,000 micros → 60% (crosses 50%).
	for _, c := range []int64{300000, 300000} {
		ww := httptest.NewRecorder()
		body := map[string]any{
			"gcid":            gcidA,
			"model_id":        "gemini-3-pro",
			"prompt_tokens":   10,
			"cost_usd_micros": c,
			"trace_id":        traceA,
			"span_id":         spanA,
		}
		srv.ServeHTTP(ww, authedReq(http.MethodPost, "/api/token-usage", body))
		if ww.Code != http.StatusCreated {
			t.Fatalf("usage seed failed: %d %s", ww.Code, ww.Body.String())
		}
	}
	// Get budget status.
	w2 := httptest.NewRecorder()
	srv.ServeHTTP(w2, authedReq(http.MethodGet,
		"/api/token-usage/budget?period=2026-05", nil))
	if w2.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", w2.Code, w2.Body.String())
	}
	var resp struct {
		BudgetID          string   `json:"budget_id"`
		Period            string   `json:"period"`
		CapUsdMicros      int64    `json:"cap_usd_micros"`
		SpentUsdMicros    int64    `json:"spent_usd_micros"`
		PercentSpent      float64  `json:"percent_spent"`
		ThresholdsCrossed []string `json:"thresholds_crossed"`
	}
	_ = json.Unmarshal(w2.Body.Bytes(), &resp)
	if resp.CapUsdMicros != 1000000 {
		t.Errorf("cap = %d; want 1000000", resp.CapUsdMicros)
	}
	if resp.SpentUsdMicros != 600000 {
		t.Errorf("spent = %d; want 600000", resp.SpentUsdMicros)
	}
	if resp.PercentSpent != 60.0 {
		t.Errorf("pct = %v; want 60.0", resp.PercentSpent)
	}
	if len(resp.ThresholdsCrossed) != 1 || resp.ThresholdsCrossed[0] != "50%" {
		t.Errorf("thresholds = %v; want [50%%]", resp.ThresholdsCrossed)
	}
}

func TestGetBudget_404OnUnknownPeriod(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet,
		"/api/token-usage/budget?period=1999-01", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d; want 404", w.Code)
	}
}

func TestGetTokenUsageAggregate_GroupsByModel(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	// Seed: 2 entries gemini-3-pro, 1 entry gemma-tenant-lora.
	for _, m := range []string{"gemini-3-pro", "gemini-3-pro", "gemma-tenant-lora"} {
		w := httptest.NewRecorder()
		body := map[string]any{
			"gcid":            gcidA,
			"model_id":        m,
			"prompt_tokens":   10,
			"cost_usd_micros": 100,
			"trace_id":        traceA,
			"span_id":         spanA,
		}
		srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/token-usage", body))
		if w.Code != http.StatusCreated {
			t.Fatalf("seed failed: %d", w.Code)
		}
	}
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet,
		"/api/token-usage/aggregate?group_by=model", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		Groups []struct {
			Group              string `json:"group"`
			TotalCostUsdMicros int64  `json:"total_cost_usd_micros"`
			EntryCount         int    `json:"entry_count"`
		} `json:"groups"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if len(resp.Groups) != 2 {
		t.Fatalf("groups = %d; want 2", len(resp.Groups))
	}
}

func TestGetAgentDecisionByID_ReturnsFullDetail(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	body := map[string]any{
		"agid":              agidA,
		"decision_type":     "route",
		"reason":            "matched cheap-route",
		"risk_tier":         "low",
		"correlation_id":    corrA,
		"traceparent":       tpA,
		"input_text":        "user wants to learn LeChatelier",
		"output_text":       "model produced explanation",
		"latency_ms":        250,
		"reasoning_summary": "router -> generator",
	}
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/agent-decisions", body))
	if w.Code != http.StatusCreated {
		t.Fatalf("seed failed: %d %s", w.Code, w.Body.String())
	}
	var created map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &created)
	logID, _ := created["log_id"].(string)
	if logID == "" {
		t.Fatalf("log_id missing")
	}

	// Get by ID.
	w2 := httptest.NewRecorder()
	srv.ServeHTTP(w2, authedReq(http.MethodGet, "/api/agent-decisions/"+logID, nil))
	if w2.Code != http.StatusOK {
		t.Fatalf("get status = %d body=%s", w2.Code, w2.Body.String())
	}
	var got map[string]any
	_ = json.Unmarshal(w2.Body.Bytes(), &got)
	if got["log_id"] != logID {
		t.Errorf("log_id mismatch: %v vs %s", got["log_id"], logID)
	}
	rs, ok := got["reasoning"].(map[string]any)
	if !ok {
		t.Fatalf("reasoning missing: %v", got)
	}
	if rs["input_hash"] == nil || rs["output_hash"] == nil {
		t.Errorf("hashes missing: %v", rs)
	}
	if rs["latency_ms"] == nil || rs["latency_ms"].(float64) != 250 {
		t.Errorf("latency = %v; want 250", rs["latency_ms"])
	}
}

func TestGetAgentDecisionByID_404OnUnknown(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet,
		"/api/agent-decisions/01970000-0000-7000-cccc-dddddddddddd", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d; want 404", w.Code)
	}
}
