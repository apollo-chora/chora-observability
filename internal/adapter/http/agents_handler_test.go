// agents_handler_test.go — integration tests for /api/v1/observability/agents.
package httpadapter_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	httpadapter "github.com/apollo-chora/chora-observability/internal/adapter/http"
	"github.com/apollo-chora/chora-observability/internal/adapter/inmem"
	"github.com/apollo-chora/chora-observability/internal/domain/agents"
	"github.com/apollo-chora/chora-observability/internal/domain/correlation"
	"github.com/apollo-chora/chora-observability/internal/domain/decision"
	"github.com/apollo-chora/chora-observability/internal/domain/ledger"
)

// testRegistry builds a small registry matching the canonical registry.json
// layout used by chora-infra/agents-cli.
func testRegistry() *agents.Registry {
	return &agents.Registry{
		Version: "v1",
		Crews: []agents.Entry{
			{
				Name:      "ai_kernel_orchestrator",
				Pattern:   "P8",
				Language:  "python",
				Framework: "langgraph",
				Domain:    "ai-kernel",
				Region:    "us-central1",
			},
			{
				Name:       "qgen_question",
				Pattern:    "P2",
				Language:   "go",
				Framework:  "adk",
				Domain:     "ai-kernel",
				Region:     "us-central1",
				LiveEngine: "projects/381315455325/locations/us-central1/reasoningEngines/8635637442075951104",
			},
			{
				Name:       "qgen_critic",
				Pattern:    "P1",
				Language:   "go",
				Domain:     "ai-kernel",
				Region:     "us-central1",
				LiveEngine: "projects/381315455325/locations/us-central1/reasoningEngines/2658824174880948224",
			},
			{
				Name:       "content_moderation",
				Pattern:    "P6",
				Language:   "go",
				Domain:     "ai-kernel",
				Region:     "us-central1",
				LiveEngine: "projects/381315455325/locations/us-central1/reasoningEngines/6834103033127763968",
				MultiAgent: true,
				SubAgents:  []string{"moderator", "critic"},
			},
		},
	}
}

func newAgentsRouter(t *testing.T, registry *agents.Registry, seedFn func(*inmem.DecisionRepository)) http.Handler {
	t.Helper()
	ledgers := inmem.NewLedgerRepository()
	decisions := inmem.NewDecisionRepository()
	correlations := inmem.NewCorrelationRepository()
	if seedFn != nil {
		seedFn(decisions)
	}
	// projectFromEnv() already defaults to chora-local when CHORA_PROJECT is
	// unset, so no t.Setenv here (which would conflict with t.Parallel).
	opts := []httpadapter.Option{}
	if registry != nil {
		opts = append(opts, httpadapter.WithAgentsRegistry(registry))
	}
	var _ ledger.Repository = ledgers
	var _ decision.Repository = decisions
	var _ correlation.Repository = correlations
	return httpadapter.NewRouter(ledgers, decisions, correlations, opts...)
}

// -----------------------------------------------------------------------------
// Method + tenant gating
// -----------------------------------------------------------------------------

func TestAgents_RequiresGET(t *testing.T) {
	t.Parallel()
	router := newAgentsRouter(t, testRegistry(), nil)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/observability/agents", nil)
	req.Header.Set("X-Tenant-Id", "tenant-1")
	req.Header.Set("gcid", "user-1")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d; want 405", rr.Code)
	}
}

func TestAgents_RequiresTenant(t *testing.T) {
	t.Parallel()
	router := newAgentsRouter(t, testRegistry(), nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/observability/agents", nil)
	// Missing X-Tenant-Id.
	req.Header.Set("gcid", "user-1")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400 (X-Tenant-Id required)", rr.Code)
	}
}

func TestAgents_RegistryNotWiredReturns503(t *testing.T) {
	t.Parallel()
	router := newAgentsRouter(t, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/observability/agents", nil)
	req.Header.Set("X-Tenant-Id", "tenant-1")
	req.Header.Set("gcid", "user-1")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d; want 503 (registry unwired)", rr.Code)
	}
}

// -----------------------------------------------------------------------------
// Empty cluster — invocations_24h=0 + null stats; NEVER fabricates
// -----------------------------------------------------------------------------

func TestAgents_EmptyClusterReturnsZeroStats(t *testing.T) {
	t.Parallel()
	router := newAgentsRouter(t, testRegistry(), nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/observability/agents", nil)
	req.Header.Set("X-Tenant-Id", "tenant-1")
	req.Header.Set("gcid", "user-1")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200; body=%s", rr.Code, rr.Body.String())
	}
	var body struct {
		Crews []struct {
			CrewName string `json:"crew_name"`
			CrewID   string `json:"crew_id"`
			Region   string `json:"region"`
			Agents   []struct {
				AgentID  string `json:"agent_id"`
				EngineID string `json:"engine_id"`
				Stats    struct {
					Invocations24h int      `json:"invocations_24h"`
					P95LatencyMs   *int     `json:"p95_latency_ms"`
					RefusalRate    *float64 `json:"refusal_rate"`
				} `json:"stats"`
			} `json:"agents"`
		} `json:"crews"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Crews) != 4 {
		t.Fatalf("len(crews) = %d; want 4", len(body.Crews))
	}
	for _, c := range body.Crews {
		for _, a := range c.Agents {
			if a.Stats.Invocations24h != 0 {
				t.Errorf("crew=%s agent=%s invocations_24h = %d; want 0",
					c.CrewName, a.AgentID, a.Stats.Invocations24h)
			}
			if a.Stats.P95LatencyMs != nil {
				t.Errorf("crew=%s agent=%s p95_latency_ms = %v; want null",
					c.CrewName, a.AgentID, *a.Stats.P95LatencyMs)
			}
			if a.Stats.RefusalRate != nil {
				t.Errorf("crew=%s agent=%s refusal_rate = %v; want null",
					c.CrewName, a.AgentID, *a.Stats.RefusalRate)
			}
		}
	}
}

// TestAgents_LangGraphOrchestratorSuppressesRefusalRate — ai_kernel_orchestrator
// is a pure LangGraph orchestrator (no LLM call), so refusal_rate is N/A and
// must stay null even when it has invocations (per the no-LLM #5 fix).
func TestAgents_LangGraphOrchestratorSuppressesRefusalRate(t *testing.T) {
	t.Parallel()
	seedFn := func(repo *inmem.DecisionRepository) {
		d, err := decision.New(decision.NewParams{
			TenantID:      "tenant-1",
			Agid:          "ai_kernel_orchestrator",
			DecisionType:  decision.TypeRoute,
			Reason:        "routed",
			RiskTier:      decision.TierLow,
			CorrelationID: "corr-orch",
			Traceparent:   "00-" + strings.Repeat("f", 32) + "-" + strings.Repeat("b", 16) + "-01",
		})
		if err != nil {
			t.Fatalf("decision.New: %v", err)
		}
		if err := repo.Append(context.Background(), d); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	router := newAgentsRouter(t, testRegistry(), seedFn)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/observability/agents", nil)
	req.Header.Set("X-Tenant-Id", "tenant-1")
	req.Header.Set("gcid", "user-1")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	var body struct {
		Crews []struct {
			CrewName string `json:"crew_name"`
			Agents   []struct {
				AgentID string `json:"agent_id"`
				Stats   struct {
					Invocations24h int      `json:"invocations_24h"`
					RefusalRate    *float64 `json:"refusal_rate"`
				} `json:"stats"`
			} `json:"agents"`
		} `json:"crews"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	found := false
	for _, c := range body.Crews {
		for _, a := range c.Agents {
			if a.AgentID != "ai_kernel_orchestrator" {
				continue
			}
			found = true
			if a.Stats.Invocations24h != 1 {
				t.Errorf("orchestrator invocations = %d; want 1", a.Stats.Invocations24h)
			}
			if a.Stats.RefusalRate != nil {
				t.Errorf("orchestrator refusal_rate = %v; want null (no LLM call)", *a.Stats.RefusalRate)
			}
		}
	}
	if !found {
		t.Fatal("ai_kernel_orchestrator not present in response")
	}
}

// TestAgents_WindowDefaultsToLastQuarter — the aggregation window is ~90 days
// (last quarter), NOT 24h. Asserted via the response since/now spread.
func TestAgents_WindowDefaultsToLastQuarter(t *testing.T) {
	t.Parallel()
	router := newAgentsRouter(t, testRegistry(), nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/observability/agents", nil)
	req.Header.Set("X-Tenant-Id", "tenant-1")
	req.Header.Set("gcid", "user-1")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	var body struct {
		Since string `json:"since"`
		Now   string `json:"now"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	since, err := time.Parse(time.RFC3339, body.Since)
	if err != nil {
		t.Fatalf("parse since: %v", err)
	}
	now, err := time.Parse(time.RFC3339, body.Now)
	if err != nil {
		t.Fatalf("parse now: %v", err)
	}
	spreadDays := now.Sub(since).Hours() / 24
	if spreadDays < 80 || spreadDays > 100 {
		t.Errorf("window spread = %.1f days; want ~90 (last quarter), not 24h", spreadDays)
	}
}

// -----------------------------------------------------------------------------
// Multi-agent crew expands sub-agents
// -----------------------------------------------------------------------------

func TestAgents_MultiAgentCrewExpandsSubAgents(t *testing.T) {
	t.Parallel()
	router := newAgentsRouter(t, testRegistry(), nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/observability/agents", nil)
	req.Header.Set("X-Tenant-Id", "tenant-1")
	req.Header.Set("gcid", "user-1")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	var body struct {
		Crews []struct {
			CrewName string `json:"crew_name"`
			Agents   []struct {
				AgentID string `json:"agent_id"`
			} `json:"agents"`
		} `json:"crews"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, c := range body.Crews {
		if c.CrewName != "content_moderation" {
			continue
		}
		if len(c.Agents) != 2 {
			t.Errorf("len(agents) for content_moderation = %d; want 2 (moderator + critic)", len(c.Agents))
		}
		ids := make(map[string]bool)
		for _, a := range c.Agents {
			ids[a.AgentID] = true
		}
		if !ids["moderator"] || !ids["critic"] {
			t.Errorf("multi-agent sub-agents missing: %v", ids)
		}
	}
}

// -----------------------------------------------------------------------------
// Stats populate when decisions exist
// -----------------------------------------------------------------------------

func TestAgents_StatsPopulateFromDecisions(t *testing.T) {
	t.Parallel()
	seedFn := func(repo *inmem.DecisionRepository) {
		// Seed 4 invocations for qgen_question — 3 respond + 1 refuse.
		for i := 0; i < 3; i++ {
			rs, err := decision.NewReasoningSummary(decision.ReasoningParams{LatencyMs: 100 + i})
			if err != nil {
				t.Fatalf("NewReasoningSummary: %v", err)
			}
			d, err := decision.New(decision.NewParams{
				TenantID:      "tenant-1",
				Agid:          "qgen_question",
				DecisionType:  decision.TypeRespond,
				Reason:        "ok",
				RiskTier:      decision.TierLow,
				CorrelationID: "corr-" + itoa(i),
				Traceparent:   "00-" + strings.Repeat("a", 32) + "-" + strings.Repeat("b", 16) + "-01",
				Reasoning:     rs,
			})
			if err != nil {
				t.Fatalf("decision.New: %v", err)
			}
			if err := repo.Append(context.Background(), d); err != nil {
				t.Fatalf("Append: %v", err)
			}
		}
		// 1 refusal.
		refuse, err := decision.New(decision.NewParams{
			TenantID:      "tenant-1",
			Agid:          "qgen_question",
			DecisionType:  decision.TypeRefuse,
			Reason:        "guardrail",
			RiskTier:      decision.TierMedium,
			CorrelationID: "corr-refuse",
			Traceparent:   "00-" + strings.Repeat("c", 32) + "-" + strings.Repeat("d", 16) + "-01",
		})
		if err != nil {
			t.Fatalf("decision.New refuse: %v", err)
		}
		if err := repo.Append(context.Background(), refuse); err != nil {
			t.Fatalf("Append refuse: %v", err)
		}
	}
	router := newAgentsRouter(t, testRegistry(), seedFn)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/observability/agents", nil)
	req.Header.Set("X-Tenant-Id", "tenant-1")
	req.Header.Set("gcid", "user-1")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200", rr.Code)
	}
	var body struct {
		Crews []struct {
			CrewName string `json:"crew_name"`
			Agents   []struct {
				AgentID string `json:"agent_id"`
				Stats   struct {
					Invocations24h int      `json:"invocations_24h"`
					P95LatencyMs   *int     `json:"p95_latency_ms"`
					RefusalRate    *float64 `json:"refusal_rate"`
				} `json:"stats"`
			} `json:"agents"`
		} `json:"crews"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	var got int
	for _, c := range body.Crews {
		for _, a := range c.Agents {
			if a.AgentID != "qgen_question" {
				continue
			}
			got = a.Stats.Invocations24h
			if got != 4 {
				t.Errorf("invocations_24h = %d; want 4", got)
			}
			if a.Stats.P95LatencyMs == nil {
				t.Errorf("p95_latency_ms = nil; want non-nil")
			}
			if a.Stats.RefusalRate == nil {
				t.Errorf("refusal_rate = nil; want non-nil")
			} else if *a.Stats.RefusalRate < 0.24 || *a.Stats.RefusalRate > 0.26 {
				t.Errorf("refusal_rate = %f; want ~0.25", *a.Stats.RefusalRate)
			}
		}
	}
	if got != 4 {
		t.Error("qgen_question not found in response or wrong invocation count")
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	s := ""
	for i > 0 {
		s = string(rune('0'+i%10)) + s
		i /= 10
	}
	return s
}

// -----------------------------------------------------------------------------
// Nested crews (CHO-1670) — qgen 3-tile synthesis + oe_grading
// -----------------------------------------------------------------------------

// nestedCrewRegistry mirrors the post-restructure registry.json: the qgen +
// oe_grading multiAgent crews. The qgen crew's qgen_question agent is split by
// question_type into qgen-mcq + qgen-OE tiles; qgen_critic renders as a single
// qgen-critique tile.
func nestedCrewRegistry() *agents.Registry {
	return &agents.Registry{
		Version: "v2",
		Crews: []agents.Entry{
			{
				Name:       "qgen",
				Pattern:    "P2",
				Language:   "go",
				Framework:  "adk",
				Domain:     "ai-kernel",
				MultiAgent: true,
				SubAgents:  []string{"qgen_question", "qgen_critic"},
			},
			{
				Name:       "oe_grading",
				Pattern:    "P1",
				Language:   "go",
				Framework:  "adk",
				Domain:     "delivery",
				MultiAgent: true,
				SubAgents:  []string{"oe_evaluator", "oe_moderator"},
			},
		},
	}
}

// seedQ appends one respond-decision for the given agent + question_type.
func seedQ(t *testing.T, repo *inmem.DecisionRepository, agid, qtype string) {
	t.Helper()
	d, err := decision.New(decision.NewParams{
		TenantID:      "tenant-1",
		Agid:          agid,
		DecisionType:  decision.TypeRespond,
		Reason:        "x",
		RiskTier:      decision.TierLow,
		CorrelationID: "corr-" + agid + "-" + qtype,
		Traceparent:   "00-" + strings.Repeat("a", 32) + "-" + strings.Repeat("b", 16) + "-01",
		QuestionType:  qtype,
	})
	if err != nil {
		t.Fatalf("decision.New: %v", err)
	}
	if err := repo.Append(context.Background(), d); err != nil {
		t.Fatalf("Append: %v", err)
	}
}

type tileView struct {
	AgentID string `json:"agent_id"`
	Role    string `json:"role"`
	Stats   struct {
		Invocations24h int      `json:"invocations_24h"`
		P95LatencyMs   *int     `json:"p95_latency_ms"`
		RefusalRate    *float64 `json:"refusal_rate"`
	} `json:"stats"`
}

func doAgentsGET(t *testing.T, router http.Handler) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/observability/agents", nil)
	req.Header.Set("X-Tenant-Id", "tenant-1")
	req.Header.Set("gcid", "user-1")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200; body=%s", rr.Code, rr.Body.String())
	}
	return rr
}

// crewTiles decodes the response + returns the agents (tiles) of the named crew.
func crewTiles(t *testing.T, rr *httptest.ResponseRecorder, crew string) []tileView {
	t.Helper()
	var body struct {
		Crews []struct {
			CrewName string     `json:"crew_name"`
			Agents   []tileView `json:"agents"`
		} `json:"crews"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, c := range body.Crews {
		if c.CrewName == crew {
			return c.Agents
		}
	}
	t.Fatalf("crew %q not found in response: %s", crew, rr.Body.String())
	return nil
}

func tileInvocations(tiles []tileView) map[string]int {
	out := map[string]int{}
	for _, tl := range tiles {
		out[tl.AgentID] = tl.Stats.Invocations24h
	}
	return out
}

// crewActivityMap decodes the response + returns crew_name → has_recent_activity
// — the crew-level boolean the O+ /o/agents banner reads to decide whether to
// render the "no recent activity in the last quarter" empty-state.
func crewActivityMap(t *testing.T, rr *httptest.ResponseRecorder) map[string]bool {
	t.Helper()
	var body struct {
		Crews []struct {
			CrewName          string `json:"crew_name"`
			HasRecentActivity bool   `json:"has_recent_activity"`
		} `json:"crews"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	out := map[string]bool{}
	for _, c := range body.Crews {
		out[c.CrewName] = c.HasRecentActivity
	}
	return out
}

// TestAgents_QgenCrewSynthesizesThreeTiles proves the qgen crew renders THREE
// tiles — qgen-mcq (qgen_question×mcq), qgen-OE (qgen_question×oe) and
// qgen-critique (all qgen_critic, summed across question_type) — with the
// per-(agent, question_type) counts.
func TestAgents_QgenCrewSynthesizesThreeTiles(t *testing.T) {
	t.Parallel()
	seedFn := func(repo *inmem.DecisionRepository) {
		seedQ(t, repo, "qgen_question", "mcq")
		seedQ(t, repo, "qgen_question", "mcq")
		seedQ(t, repo, "qgen_question", "oe")
		// critic critiques both kinds — qgen-critique sums across question_type.
		seedQ(t, repo, "qgen_critic", "mcq")
		seedQ(t, repo, "qgen_critic", "oe")
	}
	router := newAgentsRouter(t, nestedCrewRegistry(), seedFn)
	tiles := crewTiles(t, doAgentsGET(t, router), "qgen")
	got := tileInvocations(tiles)
	if len(tiles) != 3 {
		t.Fatalf("qgen tiles = %d; want 3 (qgen-mcq/qgen-OE/qgen-critique): %v", len(tiles), got)
	}
	if got["qgen-mcq"] != 2 {
		t.Errorf("qgen-mcq invocations = %d; want 2", got["qgen-mcq"])
	}
	if got["qgen-OE"] != 1 {
		t.Errorf("qgen-OE invocations = %d; want 1", got["qgen-OE"])
	}
	if got["qgen-critique"] != 2 {
		t.Errorf("qgen-critique invocations = %d; want 2 (mcq+oe critic)", got["qgen-critique"])
	}
}

// TestAgents_OeGradingCrewRendersBothTiles proves the oe_grading crew renders
// the oe_evaluator + oe_moderator tiles (generic sub-agent expansion).
func TestAgents_OeGradingCrewRendersBothTiles(t *testing.T) {
	t.Parallel()
	seedFn := func(repo *inmem.DecisionRepository) {
		seedQ(t, repo, "oe_evaluator", "oe")
		seedQ(t, repo, "oe_moderator", "oe")
	}
	router := newAgentsRouter(t, nestedCrewRegistry(), seedFn)
	tiles := crewTiles(t, doAgentsGET(t, router), "oe_grading")
	got := tileInvocations(tiles)
	if len(tiles) != 2 {
		t.Fatalf("oe_grading tiles = %d; want 2 (oe_evaluator/oe_moderator): %v", len(tiles), got)
	}
	if got["oe_evaluator"] != 1 {
		t.Errorf("oe_evaluator invocations = %d; want 1", got["oe_evaluator"])
	}
	if got["oe_moderator"] != 1 {
		t.Errorf("oe_moderator invocations = %d; want 1", got["oe_moderator"])
	}
}

// TestAgents_QgenTilesZeroWhenNoTraffic proves all 3 qgen tiles still render
// (with zero stats, NEVER fabricated) when there's no decision traffic.
func TestAgents_QgenTilesZeroWhenNoTraffic(t *testing.T) {
	t.Parallel()
	router := newAgentsRouter(t, nestedCrewRegistry(), nil)
	tiles := crewTiles(t, doAgentsGET(t, router), "qgen")
	if len(tiles) != 3 {
		t.Fatalf("qgen tiles = %d; want 3 even with no traffic", len(tiles))
	}
	ids := map[string]bool{}
	for _, tl := range tiles {
		ids[tl.AgentID] = true
		if tl.Stats.Invocations24h != 0 {
			t.Errorf("tile %s invocations = %d; want 0", tl.AgentID, tl.Stats.Invocations24h)
		}
		if tl.Stats.P95LatencyMs != nil || tl.Stats.RefusalRate != nil {
			t.Errorf("tile %s should have null p95/refusal with no traffic", tl.AgentID)
		}
	}
	for _, want := range []string{"qgen-mcq", "qgen-OE", "qgen-critique"} {
		if !ids[want] {
			t.Errorf("missing qgen tile %q", want)
		}
	}
}

// -----------------------------------------------------------------------------
// Crew-level has_recent_activity (the /o/agents banner flag)
// -----------------------------------------------------------------------------

// TestAgents_CrewHasRecentActivityReflectsInvocations proves the crew-level
// has_recent_activity is the OR over its agent tiles' invocation counts: a crew
// with ≥1 agent invocation in the window is true; an idle crew is false. The FE
// reads crew.has_recent_activity to gate the "no recent activity in the last
// quarter" banner — without it the banner showed even when sub-agent tiles had
// non-zero counts.
func TestAgents_CrewHasRecentActivityReflectsInvocations(t *testing.T) {
	t.Parallel()
	seedFn := func(repo *inmem.DecisionRepository) {
		// One invocation for the qgen_critic single-agent crew only.
		d, err := decision.New(decision.NewParams{
			TenantID:      "tenant-1",
			Agid:          "qgen_critic",
			DecisionType:  decision.TypeRespond,
			Reason:        "ok",
			RiskTier:      decision.TierLow,
			CorrelationID: "corr-activity",
			Traceparent:   "00-" + strings.Repeat("a", 32) + "-" + strings.Repeat("b", 16) + "-01",
		})
		if err != nil {
			t.Fatalf("decision.New: %v", err)
		}
		if err := repo.Append(context.Background(), d); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	router := newAgentsRouter(t, testRegistry(), seedFn)
	got := crewActivityMap(t, doAgentsGET(t, router))
	if !got["qgen_critic"] {
		t.Errorf("qgen_critic has_recent_activity = false; want true (1 invocation)")
	}
	if got["ai_kernel_orchestrator"] {
		t.Errorf("ai_kernel_orchestrator has_recent_activity = true; want false (no traffic)")
	}
	if got["qgen_question"] {
		t.Errorf("qgen_question has_recent_activity = true; want false (no traffic)")
	}
}

// TestAgents_CrewHasRecentActivityMultiAgentSumsSubAgents proves a MultiAgent
// crew is active when ANY sub-agent tile has invocations (the OR over tiles),
// exercising the generic buildCrewViews sub-agent path.
func TestAgents_CrewHasRecentActivityMultiAgentSumsSubAgents(t *testing.T) {
	t.Parallel()
	seedFn := func(repo *inmem.DecisionRepository) {
		d, err := decision.New(decision.NewParams{
			TenantID:      "tenant-1",
			Agid:          "moderator", // sub-agent of content_moderation
			DecisionType:  decision.TypeRespond,
			Reason:        "ok",
			RiskTier:      decision.TierLow,
			CorrelationID: "corr-mod",
			Traceparent:   "00-" + strings.Repeat("a", 32) + "-" + strings.Repeat("b", 16) + "-01",
		})
		if err != nil {
			t.Fatalf("decision.New: %v", err)
		}
		if err := repo.Append(context.Background(), d); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	router := newAgentsRouter(t, testRegistry(), seedFn)
	got := crewActivityMap(t, doAgentsGET(t, router))
	if !got["content_moderation"] {
		t.Errorf("content_moderation has_recent_activity = false; want true (moderator sub-agent invoked)")
	}
}

// TestAgents_CrewHasRecentActivityFalseWhenEmpty proves every crew reports
// has_recent_activity=false when there is no decision traffic in the window
// (NEVER fabricated).
func TestAgents_CrewHasRecentActivityFalseWhenEmpty(t *testing.T) {
	t.Parallel()
	router := newAgentsRouter(t, testRegistry(), nil)
	got := crewActivityMap(t, doAgentsGET(t, router))
	if len(got) != 4 {
		t.Fatalf("crews = %d; want 4", len(got))
	}
	for crew, active := range got {
		if active {
			t.Errorf("crew %s has_recent_activity = true; want false (no traffic)", crew)
		}
	}
}

// TestAgents_QgenCrewHasRecentActivity proves the special qgen crew path
// (buildQgenCrew, distinct from buildCrewViews) also emits has_recent_activity:
// true when its synthetic tiles carry invocations. The ONE activity rule must
// apply in both code paths.
func TestAgents_QgenCrewHasRecentActivity(t *testing.T) {
	t.Parallel()
	seedFn := func(repo *inmem.DecisionRepository) {
		seedQ(t, repo, "qgen_question", "mcq")
	}
	router := newAgentsRouter(t, nestedCrewRegistry(), seedFn)
	got := crewActivityMap(t, doAgentsGET(t, router))
	if !got["qgen"] {
		t.Errorf("qgen has_recent_activity = false; want true (qgen-mcq tile has 1 invocation)")
	}
	if got["oe_grading"] {
		t.Errorf("oe_grading has_recent_activity = true; want false (no traffic)")
	}
}

// TestAgents_QgenCrewHasRecentActivityFalseWhenIdle proves the qgen crew is
// false when none of its 3 synthetic tiles carry traffic.
func TestAgents_QgenCrewHasRecentActivityFalseWhenIdle(t *testing.T) {
	t.Parallel()
	router := newAgentsRouter(t, nestedCrewRegistry(), nil)
	got := crewActivityMap(t, doAgentsGET(t, router))
	if got["qgen"] {
		t.Errorf("qgen has_recent_activity = true; want false (no traffic)")
	}
}
