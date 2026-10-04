// Package httpadapter_test exercises /api/cost/* and /api/traces/export.
package httpadapter_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Sequel canonical seed — 15 ticks, sum = 760000 micros = $0.76.
var sequelSeed = []int64{
	120000, 40000, 50000, 30000, 40000, 60000, 20000, 30000,
	70000, 20000, 30000, 10000, 40000, 120000, 80000,
}

func seedSequel(t *testing.T, srv http.Handler) {
	t.Helper()
	for i, c := range sequelSeed {
		w := httptest.NewRecorder()
		body := map[string]any{
			"gcid":            gcidA,
			"model_id":        "gemini-3-pro",
			"prompt_tokens":   10,
			"cost_usd_micros": c,
			"trace_id":        traceA,
			"span_id":         spanA,
		}
		_ = i
		srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/token-usage", body))
		if w.Code != http.StatusCreated {
			t.Fatalf("seed[%d] failed: %d %s", i, w.Code, w.Body.String())
		}
	}
}

func TestGetCumulativeCost_SequelTicker(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	seedSequel(t, srv)

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet,
		"/api/cost/cumulative?tenant_id="+tenantA, nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		TotalUsdMicros int64  `json:"total_cost_usd_micros"`
		TotalUsd       string `json:"total_cost_usd"`
		Ticks          []struct {
			CumulativeUsdMicros int64 `json:"cumulative_cost_usd_micros"`
		} `json:"ticks"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.TotalUsdMicros != 760000 {
		t.Errorf("total micros = %d; want 760000 ($0.76)", resp.TotalUsdMicros)
	}
	if resp.TotalUsd != "$0.76" {
		t.Errorf("total usd = %q; want $0.76", resp.TotalUsd)
	}
	if len(resp.Ticks) != 15 {
		t.Errorf("tick count = %d; want 15", len(resp.Ticks))
	}
	if resp.Ticks[0].CumulativeUsdMicros != 120000 {
		t.Errorf("first tick = %d; want 120000", resp.Ticks[0].CumulativeUsdMicros)
	}
}

func TestGetCostByAct_GroupsByModel(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	// Two models, three entries each
	for _, m := range []string{"gemini-3-pro", "gemma-tenant-lora"} {
		for _, c := range []int64{100000, 50000, 30000} {
			w := httptest.NewRecorder()
			body := map[string]any{
				"gcid":            gcidA,
				"model_id":        m,
				"prompt_tokens":   10,
				"cost_usd_micros": c,
				"trace_id":        traceA,
				"span_id":         spanA,
			}
			srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/token-usage", body))
			if w.Code != http.StatusCreated {
				t.Fatalf("seed failed: %d", w.Code)
			}
		}
	}
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet,
		"/api/cost/by-act?tenant_id="+tenantA, nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		Groups []struct {
			Name           string `json:"name"`
			TotalUsdMicros int64  `json:"total_cost_usd_micros"`
			TotalUsd       string `json:"total_cost_usd"`
			EntryCount     int    `json:"entry_count"`
		} `json:"groups"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Groups) != 2 {
		t.Fatalf("groups = %d; want 2", len(resp.Groups))
	}
	// Sorted asc — gemini-3-pro before gemma-tenant-lora? alphabetical g-e-m-i < g-e-m-m
	if resp.Groups[0].Name != "gemini-3-pro" {
		t.Errorf("groups[0] = %s; want gemini-3-pro", resp.Groups[0].Name)
	}
	if resp.Groups[0].TotalUsdMicros != 180000 {
		t.Errorf("gemini sum = %d; want 180000", resp.Groups[0].TotalUsdMicros)
	}
}

// TestGetCumulativeCost_FromTo_FiltersWindow proves /api/cost/cumulative honors
// the `from`/`to` RFC3339 window query params (backs the O+ day/week/month
// time-window filter). Entries are recorded at ~now, so a future `from`
// excludes everything and a past `from` includes everything.
func TestGetCumulativeCost_FromTo_FiltersWindow(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	seedSequel(t, srv) // 15 ticks recorded ~now, sum = 760000 micros

	future := time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339)
	past := time.Now().Add(-24 * time.Hour).UTC().Format(time.RFC3339)

	var resp struct {
		TotalUsdMicros int64 `json:"total_cost_usd_micros"`
	}

	// from in the FUTURE excludes every (now-recorded) entry → $0.
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet,
		"/api/cost/cumulative?tenant_id="+tenantA+"&from="+future, nil))
	if w.Code != http.StatusOK {
		t.Fatalf("future-from status = %d body=%s", w.Code, w.Body.String())
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.TotalUsdMicros != 0 {
		t.Errorf("future-from total = %d; want 0 (window excludes all entries)", resp.TotalUsdMicros)
	}

	// from in the PAST keeps every entry → full $0.76.
	w2 := httptest.NewRecorder()
	srv.ServeHTTP(w2, authedReq(http.MethodGet,
		"/api/cost/cumulative?tenant_id="+tenantA+"&from="+past, nil))
	if err := json.Unmarshal(w2.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.TotalUsdMicros != 760000 {
		t.Errorf("past-from total = %d; want 760000 (window includes all entries)", resp.TotalUsdMicros)
	}
}

// TestGetCumulativeCost_RejectsBadFrom — a malformed `from` is a 400, not a
// silently-ignored param (fail-loud).
func TestGetCumulativeCost_RejectsBadFrom(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet,
		"/api/cost/cumulative?tenant_id="+tenantA+"&from=not-a-date", nil))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400 on malformed from", w.Code)
	}
}

// TestAggregateTokenUsage_FromTo_FiltersWindow proves
// /api/token-usage/aggregate honors the `from`/`to` window (backs the O+
// by_model / by_agent breakdowns under the time-window filter).
func TestAggregateTokenUsage_FromTo_FiltersWindow(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	seedSequel(t, srv) // entries recorded ~now under model gemini-3-pro

	future := time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339)
	past := time.Now().Add(-24 * time.Hour).UTC().Format(time.RFC3339)

	type aggResp struct {
		Groups []struct {
			Group string `json:"group"`
		} `json:"groups"`
	}

	// future-from → no entries in window → empty groups.
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet,
		"/api/token-usage/aggregate?group_by=model&from="+future, nil))
	if w.Code != http.StatusOK {
		t.Fatalf("future-from status = %d body=%s", w.Code, w.Body.String())
	}
	var fr aggResp
	if err := json.Unmarshal(w.Body.Bytes(), &fr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(fr.Groups) != 0 {
		t.Errorf("future-from groups = %d; want 0 (window excludes all)", len(fr.Groups))
	}

	// past-from → entries present.
	w2 := httptest.NewRecorder()
	srv.ServeHTTP(w2, authedReq(http.MethodGet,
		"/api/token-usage/aggregate?group_by=model&from="+past, nil))
	var pr aggResp
	if err := json.Unmarshal(w2.Body.Bytes(), &pr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(pr.Groups) == 0 {
		t.Errorf("past-from groups = 0; want >=1 (window includes all)")
	}
}

// TestAggregateTokenUsage_RejectsBadFrom — malformed `from` → 400.
func TestAggregateTokenUsage_RejectsBadFrom(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet,
		"/api/token-usage/aggregate?group_by=model&from=not-a-date", nil))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400 on malformed from", w.Code)
	}
}

func TestPostTraceExport_ReturnsExportMetadata(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	body := map[string]any{
		"since": "2026-05-08T00:00:00Z",
		"until": "2026-05-08T23:59:59Z",
	}
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/traces/export", body))
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d body=%s; want 202", w.Code, w.Body.String())
	}
	var resp struct {
		ExportID string `json:"export_id"`
		Status   string `json:"status"`
		Endpoint string `json:"endpoint"`
		Mock     bool   `json:"mock"`
		Since    string `json:"since"`
		Until    string `json:"until"`
		QueuedAt string `json:"queued_at"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.ExportID == "" {
		t.Errorf("export_id missing")
	}
	if !resp.Mock {
		t.Errorf("MVP must report mock=true")
	}
	if resp.Status != "queued" {
		t.Errorf("status = %s; want queued", resp.Status)
	}
}

func TestPostTraceExport_RejectsBadDates(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	body := map[string]any{
		"since": "not-a-date",
		"until": "2026-05-08T23:59:59Z",
	}
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/traces/export", body))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", w.Code)
	}
}

func TestPostTraceExport_RejectsInvertedRange(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	w := httptest.NewRecorder()
	body := map[string]any{
		"since": "2026-05-09T00:00:00Z",
		"until": "2026-05-08T00:00:00Z",
	}
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/traces/export", body))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", w.Code)
	}
}
