// Package httpadapter_test exercises the 3-level budget pre-check endpoint
// /api/token-usage/budget-check that the Gateway calls BEFORE every LLM
// invocation.
//
// Per ai-cost-tracking skill ("3-level budget cascade") + Tier 3 D10 hard-cap
// enforcement at the Router/Gateway boundary.
package httpadapter_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	httpadapter "github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/http"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/inmem"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/ledger"
)

// newServerWithLookup wires a router that includes a 3-level BudgetLookup
// pre-seeded for the test scenarios.
func newServerWithLookup(t *testing.T, lookup *inmem.BudgetLookup) http.Handler {
	t.Helper()
	return httpadapter.NewRouter(
		inmem.NewLedgerRepository(),
		inmem.NewDecisionRepository(),
		inmem.NewCorrelationRepository(),
		httpadapter.WithBudgetRepo(inmem.NewBudgetRepository()),
		httpadapter.WithBudgetLookup(lookup),
	)
}

func TestBudgetCheck_AllowedWhenNoLimits(t *testing.T) {
	t.Parallel()
	srv := newServerWithLookup(t, inmem.NewBudgetLookup())
	w := httptest.NewRecorder()
	body := map[string]any{
		"tenant_id":             tenantA,
		"gcid":                  gcidA,
		"agent_id":              "agent-x",
		"period":                "2026-05",
		"projected_cost_micros": 100,
	}
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/token-usage/budget-check", body))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", w.Code, w.Body.String())
	}
	var dec ledger.AllowDecision
	_ = json.Unmarshal(w.Body.Bytes(), &dec)
	if !dec.Allowed {
		t.Errorf("expected ALLOW; got %+v", dec)
	}
}

func TestBudgetCheck_DeniedWhenTenantOverCap(t *testing.T) {
	t.Parallel()
	lookup := inmem.NewBudgetLookup()
	tb, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: tenantA, Period: "2026-05", CapUsdMicros: 1_000_000,
	})
	tb.RecordSpend(950_000) // 95% spent.
	_ = lookup.SetTenantBudget(context.Background(), tb)

	srv := newServerWithLookup(t, lookup)
	w := httptest.NewRecorder()
	body := map[string]any{
		"tenant_id":             tenantA,
		"gcid":                  gcidA,
		"agent_id":              "agent-x",
		"period":                "2026-05",
		"projected_cost_micros": 100_000, // → 105% over cap
	}
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/token-usage/budget-check", body))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", w.Code, w.Body.String())
	}
	var dec ledger.AllowDecision
	_ = json.Unmarshal(w.Body.Bytes(), &dec)
	if dec.Allowed {
		t.Errorf("expected DENY; got %+v", dec)
	}
	if dec.Reason != ledger.ReasonTenantCapExceeded {
		t.Errorf("reason = %s; want %s", dec.Reason, ledger.ReasonTenantCapExceeded)
	}
	if dec.RetryAfterSeconds <= 0 {
		t.Errorf("retry_after must be > 0 on hard-block")
	}
}

func TestBudgetCheck_503WhenLookupNotConfigured(t *testing.T) {
	t.Parallel()
	srv := httpadapter.NewRouter(
		inmem.NewLedgerRepository(),
		inmem.NewDecisionRepository(),
		inmem.NewCorrelationRepository(),
		// No budget lookup option.
	)
	w := httptest.NewRecorder()
	body := map[string]any{
		"tenant_id":             tenantA,
		"gcid":                  gcidA,
		"agent_id":              "agent-x",
		"period":                "2026-05",
		"projected_cost_micros": 100,
	}
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/token-usage/budget-check", body))
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d; want 503", w.Code)
	}
}

func TestBudgetCheck_400OnNegativeProjection(t *testing.T) {
	t.Parallel()
	srv := newServerWithLookup(t, inmem.NewBudgetLookup())
	w := httptest.NewRecorder()
	body := map[string]any{
		"tenant_id":             tenantA,
		"gcid":                  gcidA,
		"agent_id":              "agent-x",
		"period":                "2026-05",
		"projected_cost_micros": -1,
	}
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/token-usage/budget-check", body))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", w.Code)
	}
}
