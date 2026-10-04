// agent_decisions_count_handler_test.go — integration tests for the
// GET /api/v1/observability/agent-decisions/count endpoint that backs the
// O+ Dashboard recent_decisions_24h rollup.
package httpadapter_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	httpadapter "github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/http"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/inmem"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/decision"
)

const countTenant = "01970000-0000-7000-8000-000000000111"
const countAgid = "01970000-0000-7000-a000-000000000222"
const countCorr = "01970000-0000-7000-b000-000000000333"
const countTraceparent = "00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01"

// newCountRouter wires the router with a decision repo plus an optional
// seed callback for the count tests.
func newCountRouter(t *testing.T, seedFn func(*inmem.DecisionRepository)) http.Handler {
	t.Helper()
	ledgers := inmem.NewLedgerRepository()
	decisions := inmem.NewDecisionRepository()
	correlations := inmem.NewCorrelationRepository()
	if seedFn != nil {
		seedFn(decisions)
	}
	return httpadapter.NewRouter(ledgers, decisions, correlations)
}

// seedNDecisionsForTenant appends n RouteAccepted decisions for the given
// tenant at the current moment.
func seedNDecisionsForTenant(t *testing.T, repo *inmem.DecisionRepository, tenantID string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		d, err := decision.New(decision.NewParams{
			TenantID:      tenantID,
			Agid:          countAgid,
			DecisionType:  decision.TypeRoute,
			Reason:        "test seed",
			RiskTier:      decision.TierLow,
			CorrelationID: countCorr,
			Traceparent:   countTraceparent,
		})
		if err != nil {
			t.Fatalf("decision.New: %v", err)
		}
		if err := repo.Append(context.Background(), d); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
}

func TestAgentDecisionsCount_RequiresGET(t *testing.T) {
	t.Parallel()
	router := newCountRouter(t, nil)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/observability/agent-decisions/count", nil)
	req.Header.Set("X-Tenant-Id", countTenant)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d; want 405", rr.Code)
	}
}

func TestAgentDecisionsCount_RequiresTenant(t *testing.T) {
	t.Parallel()
	router := newCountRouter(t, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/observability/agent-decisions/count", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400 (X-Tenant-Id required)", rr.Code)
	}
}

func TestAgentDecisionsCount_EmptyRepoReturnsZero(t *testing.T) {
	t.Parallel()
	router := newCountRouter(t, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/observability/agent-decisions/count", nil)
	req.Header.Set("X-Tenant-Id", countTenant)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200; body=%s", rr.Code, rr.Body.String())
	}
	var body struct {
		Count int64  `json:"count"`
		Since string `json:"since"`
		Until string `json:"until"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Count != 0 {
		t.Errorf("count = %d; want 0 (honest zero, not stub)", body.Count)
	}
	// Default window is now-24h..now.
	if body.Since == "" || body.Until == "" {
		t.Errorf("since/until empty: since=%q until=%q", body.Since, body.Until)
	}
	if _, err := time.Parse(time.RFC3339, body.Since); err != nil {
		t.Errorf("since not RFC3339: %v", err)
	}
	if _, err := time.Parse(time.RFC3339, body.Until); err != nil {
		t.Errorf("until not RFC3339: %v", err)
	}
}

func TestAgentDecisionsCount_DefaultWindow24h(t *testing.T) {
	t.Parallel()
	router := newCountRouter(t, func(repo *inmem.DecisionRepository) {
		seedNDecisionsForTenant(t, repo, countTenant, 5)
	})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/observability/agent-decisions/count", nil)
	req.Header.Set("X-Tenant-Id", countTenant)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200", rr.Code)
	}
	var body struct {
		Count int64 `json:"count"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Count != 5 {
		t.Errorf("count = %d; want 5", body.Count)
	}
}

func TestAgentDecisionsCount_TenantIsolation(t *testing.T) {
	t.Parallel()
	router := newCountRouter(t, func(repo *inmem.DecisionRepository) {
		seedNDecisionsForTenant(t, repo, countTenant, 4)
		// Foreign tenant — must NOT leak.
		seedNDecisionsForTenant(t, repo, "01970000-0000-7000-8000-0000000099aa", 9)
	})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/observability/agent-decisions/count", nil)
	req.Header.Set("X-Tenant-Id", countTenant)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200", rr.Code)
	}
	var body struct {
		Count int64 `json:"count"`
	}
	_ = json.NewDecoder(rr.Body).Decode(&body)
	if body.Count != 4 {
		t.Errorf("count = %d; want 4 (tenant isolation breach)", body.Count)
	}
}

func TestAgentDecisionsCount_ExplicitSinceUntil(t *testing.T) {
	t.Parallel()
	router := newCountRouter(t, func(repo *inmem.DecisionRepository) {
		seedNDecisionsForTenant(t, repo, countTenant, 2)
	})
	since := time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339)
	until := time.Now().UTC().Add(1 * time.Hour).Format(time.RFC3339)
	url := "/api/v1/observability/agent-decisions/count?since=" + since + "&until=" + until
	req := httptest.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("X-Tenant-Id", countTenant)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200; body=%s", rr.Code, rr.Body.String())
	}
	var body struct {
		Count int64  `json:"count"`
		Since string `json:"since"`
		Until string `json:"until"`
	}
	_ = json.NewDecoder(rr.Body).Decode(&body)
	if body.Count != 2 {
		t.Errorf("count = %d; want 2", body.Count)
	}
	if body.Since == "" || body.Until == "" {
		t.Errorf("since/until missing: since=%q until=%q", body.Since, body.Until)
	}
}

func TestAgentDecisionsCount_InvalidSinceReturns400(t *testing.T) {
	t.Parallel()
	router := newCountRouter(t, nil)
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/observability/agent-decisions/count?since=not-rfc3339", nil)
	req.Header.Set("X-Tenant-Id", countTenant)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", rr.Code)
	}
}

func TestAgentDecisionsCount_InvalidUntilReturns400(t *testing.T) {
	t.Parallel()
	router := newCountRouter(t, nil)
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/observability/agent-decisions/count?until=not-rfc3339", nil)
	req.Header.Set("X-Tenant-Id", countTenant)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", rr.Code)
	}
}

func TestAgentDecisionsCount_SinceAfterUntilReturns400(t *testing.T) {
	t.Parallel()
	router := newCountRouter(t, nil)
	since := time.Now().UTC().Add(1 * time.Hour).Format(time.RFC3339)
	until := time.Now().UTC().Add(-1 * time.Hour).Format(time.RFC3339)
	url := "/api/v1/observability/agent-decisions/count?since=" + since + "&until=" + until
	req := httptest.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("X-Tenant-Id", countTenant)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", rr.Code)
	}
}
