// agents_handler.go — GET /api/v1/observability/agents per Phase B of the
// O+ hydration plan atomic-napping-spring.md.
//
// Response shape (crews + agents hierarchy, NOT the deprecated flat 7-agent
// invariant — per anchoring decision #7):
//
//	{
//	  "crews": [
//	    { "crew_name": "qgen_pipeline", "crew_id": "...",
//	      "agents": [
//	        { "agent_id": "qgen_question", "role": "MCQ generator",
//	          "engine_id": "8635637442075951104",
//	          "stats": { "invocations_24h": 0, "p95_latency_ms": null,
//	                     "refusal_rate": null } },
//	        ...
//	      ] },
//	    ...
//	  ],
//	  "since": "...",
//	  "now":   "..."
//	}
//
// Source: aggregate over the local agent_decision_log (via
// decision.Repository.List) grouped by agid, joined to the static
// registry.json metadata for the crew + engine deep-links.
//
// Per [[feedback-no-stubs-real-wiring]]: NEVER fabricate stats. When the
// repo returns zero rows for the window (default ~90d / last quarter), the
// stats payload reports invocations_24h=0 + null for the percentile / rate
// fields. The FE renders this as "no recent activity" — NOT a mock banner.
//
// The response carries no per-agent trace deep-link: the local stack reads
// traces from Grafana Tempo via the Spanstore query API
// (/api/v1/observability/spans), not from a console link.
package httpadapter

import (
	"context"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/apollo-chora/chora-observability/internal/domain/agents"
	"github.com/apollo-chora/chora-observability/internal/domain/decision"
)

// defaultAgentsWindowDays is the aggregation window for the /o/agents view —
// "last quarter" (~90 days), NOT the legacy 24h. Overridable via
// CHORA_AGENTS_WINDOW_DAYS so ops can tune it without a redeploy.
const defaultAgentsWindowDays = 90

// agentsHandler holds the request-time deps.
type agentsHandler struct {
	decisions decision.Repository
	registry  *agents.Registry
	window    time.Duration // aggregation window (default ~90d / last quarter)
}

// newAgentsHandler constructs the handler.
func newAgentsHandler(d decision.Repository, r *agents.Registry) *agentsHandler {
	return &agentsHandler{decisions: d, registry: r, window: agentsWindow()}
}

// agentsWindow returns the aggregation window: CHORA_AGENTS_WINDOW_DAYS days
// (default 90 — "last quarter"). Falls back to the default on unset / invalid.
func agentsWindow() time.Duration {
	days := defaultAgentsWindowDays
	if v := os.Getenv("CHORA_AGENTS_WINDOW_DAYS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			days = n
		}
	}
	return time.Duration(days) * 24 * time.Hour
}

// agentsResponse is the top-level payload.
type agentsResponse struct {
	Crews []crewView `json:"crews"`
	Since string     `json:"since"`
	Now   string     `json:"now"`
}

type crewView struct {
	CrewName string `json:"crew_name"`
	CrewID   string `json:"crew_id"`
	Domain   string `json:"domain,omitempty"`
	Pattern  string `json:"pattern,omitempty"`
	Region   string `json:"region,omitempty"`
	// HasRecentActivity is the crew-level rollup the O+ /o/agents banner reads
	// to decide whether to render the "no recent activity in the last quarter"
	// empty-state. True iff the SUM of this crew's agent-tile invocation counts
	// over the window is > 0. Computed once here at the source (see
	// crewHasActivity) so the BFF pass-through preserves it.
	HasRecentActivity bool        `json:"has_recent_activity"`
	Agents            []agentView `json:"agents"`
}

type agentView struct {
	AgentID  string     `json:"agent_id"`
	Role     string     `json:"role,omitempty"`
	EngineID string     `json:"engine_id,omitempty"`
	Stats    agentStats `json:"stats"`
}

type agentStats struct {
	Invocations24h int      `json:"invocations_24h"`
	P95LatencyMs   *int     `json:"p95_latency_ms"` // null when no data
	RefusalRate    *float64 `json:"refusal_rate"`   // null when no data
}

// serveOPlusAgents handles GET /api/v1/observability/agents.
func (h *agentsHandler) serveOPlusAgents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED",
			"only GET is supported on /api/v1/observability/agents")
		return
	}
	if h.decisions == nil {
		writeError(w, http.StatusServiceUnavailable, "OBS_DECISIONS_UNAVAILABLE",
			"decision repository not wired")
		return
	}
	if h.registry == nil {
		writeError(w, http.StatusServiceUnavailable, "OBS_REGISTRY_UNAVAILABLE",
			"agents registry not wired (config/registry.json absent)")
		return
	}
	tenantID := tenantFromContext(r.Context())
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "OBS_TENANT_REQUIRED",
			"X-Tenant-Id header is required")
		return
	}

	now := time.Now().UTC()
	since := now.Add(-h.window)

	// Aggregate AgentDecisionLog rows over the window (default ~90d), keyed by
	// agent_id AND by (agent_id, question_type) so the qgen crew can split
	// qgen_question into qgen-mcq + qgen-OE tiles.
	bundle, err := h.collectStats(r.Context(), tenantID, since)
	if err != nil {
		log.Printf("agents stats: %v", err)
		writeError(w, http.StatusInternalServerError, "OBS_REPO_ERROR",
			"agent decision aggregation failed")
		return
	}

	crews := h.buildCrewViews(bundle)
	writeJSON(w, http.StatusOK, agentsResponse{
		Crews: crews,
		Since: since.Format(time.RFC3339),
		Now:   now.Format(time.RFC3339),
	})
}

// perAgentStats holds the running aggregate during collectStats.
type perAgentStats struct {
	invocations int
	refusals    int
	latencies   []int
}

// add folds one decision row into the running aggregate.
func (s *perAgentStats) add(l *decision.Log) {
	s.invocations++
	if l.DecisionType == decision.TypeRefuse {
		s.refusals++
	}
	if l.Reasoning != nil && l.Reasoning.LatencyMs > 0 {
		s.latencies = append(s.latencies, l.Reasoning.LatencyMs)
	}
}

// agentStatKey keys the per-(agent, question_type) aggregate. questionType is
// "" for decisions that carry no question_type tag.
type agentStatKey struct {
	agentID      string
	questionType string
}

// statsBundle holds two parallel aggregations built in one pass:
//   - byAgent: keyed by agent_id only (sums across question_type). Powers the
//     standard sub-agent + single-agent tiles AND qgen-critique.
//   - byAgentQType: keyed by (agent_id, question_type). Powers the qgen crew's
//     qgen-mcq + qgen-OE split.
//
// byAgent is updated on EVERY row in recorded_at ASC order, so its
// latestTraceparent is automatically the global most-recent for that agent —
// no cross-bucket merge math.
type statsBundle struct {
	byAgent      map[string]*perAgentStats
	byAgentQType map[agentStatKey]*perAgentStats
}

// collectStats walks the decision repo for the tenant + window and aggregates
// per-agent and per-(agent, question_type) counts.
//
// Crew membership is derived from registry.json metadata; the Agid (persisted
// as agent_id) is the linking key. The qgen crew additionally splits
// qgen_question by the question_type attribute the binding extracts from the
// proto field-21 attributes map.
func (h *agentsHandler) collectStats(ctx context.Context, tenantID string, since time.Time) (*statsBundle, error) {
	bundle := &statsBundle{
		byAgent:      make(map[string]*perAgentStats),
		byAgentQType: make(map[agentStatKey]*perAgentStats),
	}
	// List the broad window — repo caps at 1000 per call; sufficient for the
	// dashboard view at expected volumes.
	rows, err := h.decisions.List(ctx, tenantID, decision.ListFilter{
		From:  since,
		Limit: 1000,
	})
	if err != nil {
		return nil, err
	}
	for _, l := range rows {
		sa, ok := bundle.byAgent[l.Agid]
		if !ok {
			sa = &perAgentStats{}
			bundle.byAgent[l.Agid] = sa
		}
		sa.add(l)

		k := agentStatKey{agentID: l.Agid, questionType: l.QuestionType}
		sq, ok := bundle.byAgentQType[k]
		if !ok {
			sq = &perAgentStats{}
			bundle.byAgentQType[k] = sq
		}
		sq.add(l)
	}
	return bundle, nil
}

// qgenCrewName is the registry crew whose qgen_question sub-agent is split by
// question_type into qgen-mcq + qgen-OE tiles (qgen_critic stays whole as
// qgen-critique). The agent that splits + the synthetic tile ids below are the
// contract the O+ FE renders.
const (
	qgenCrewName        = "qgen"
	qgenQuestionAgentID = "qgen_question"
	qgenCriticAgentID   = "qgen_critic"
	qgenTileMCQ         = "qgen-mcq"
	qgenTileOE          = "qgen-OE"
	qgenTileCritique    = "qgen-critique"
	questionTypeMCQ     = "mcq"
	questionTypeOE      = "oe"
)

// buildCrewViews composes the response payload from registry.json + the stats
// bundle. Crews with no traffic still appear (zero stats, never fabricated).
//
// The qgen crew is rendered specially: qgen_question is split into qgen-mcq +
// qgen-OE tiles by question_type, and qgen_critic renders as qgen-critique.
// All other crews expand generically (MultiAgent → one tile per sub-agent;
// single-agent → one tile keyed off the crew name).
func (h *agentsHandler) buildCrewViews(bundle *statsBundle) []crewView {
	crews := make([]crewView, 0, len(h.registry.Crews))
	for _, e := range h.registry.Crews {
		if e.Name == qgenCrewName {
			crews = append(crews, h.buildQgenCrew(e, bundle))
			continue
		}
		cv := crewView{
			CrewName: e.Name,
			CrewID:   e.Name, // canonical: crew_id == crew_name
			Domain:   e.Domain,
			Pattern:  e.Pattern,
			Region:   e.Location(),
		}
		// MultiAgent crews surface each sub-agent as its own agentView.
		// Single-agent crews surface a single agentView keyed off the crew name.
		if e.MultiAgent && len(e.SubAgents) > 0 {
			for _, sub := range e.SubAgents {
				cv.Agents = append(cv.Agents, h.buildAgentView(sub, e, bundle.byAgent[sub]))
			}
		} else {
			cv.Agents = append(cv.Agents, h.buildAgentView(e.Name, e, bundle.byAgent[e.Name]))
		}
		cv.HasRecentActivity = crewHasActivity(cv.Agents)
		crews = append(crews, cv)
	}
	// Sort by crew_name for deterministic FE rendering.
	sort.Slice(crews, func(i, j int) bool { return crews[i].CrewName < crews[j].CrewName })
	return crews
}

// buildQgenCrew synthesizes the qgen crew's THREE tiles:
//   - qgen-mcq      — qgen_question decisions tagged question_type="mcq"
//   - qgen-OE       — qgen_question decisions tagged question_type="oe"
//   - qgen-critique — all qgen_critic decisions (summed across question_type)
func (h *agentsHandler) buildQgenCrew(e agents.Entry, bundle *statsBundle) crewView {
	cv := crewView{
		CrewName: e.Name,
		CrewID:   e.Name,
		Domain:   e.Domain,
		Pattern:  e.Pattern,
		Region:   e.Location(),
	}
	mcq := bundle.byAgentQType[agentStatKey{agentID: qgenQuestionAgentID, questionType: questionTypeMCQ}]
	oe := bundle.byAgentQType[agentStatKey{agentID: qgenQuestionAgentID, questionType: questionTypeOE}]
	cv.Agents = []agentView{
		h.buildTileView(qgenTileMCQ, "MCQ generator", e, mcq),
		h.buildTileView(qgenTileOE, "Open-ended generator", e, oe),
		h.buildTileView(qgenTileCritique, "Quality critic", e, bundle.byAgent[qgenCriticAgentID]),
	}
	cv.HasRecentActivity = crewHasActivity(cv.Agents)
	return cv
}

// crewHasActivity reports whether a crew has recent activity in the window:
// true iff the SUM of its agent tiles' invocation counts is > 0. This is the
// single rule behind the crew-level has_recent_activity flag — used by BOTH
// buildCrewViews (generic) and buildQgenCrew (qgen special path) so there is
// exactly ONE definition of "active". (Invocations24h is the count over the
// configured window — ~90d by default — despite the legacy field name.)
func crewHasActivity(agents []agentView) bool {
	total := 0
	for _, a := range agents {
		total += a.Stats.Invocations24h
	}
	return total > 0
}

// buildAgentView renders a tile whose display id == the real agent_id (the
// common case). Delegates to buildTileView.
func (h *agentsHandler) buildAgentView(agentID string, e agents.Entry, s *perAgentStats) agentView {
	return h.buildTileView(agentID, agentRoleFromEntry(agentID, e), e, s)
}

// buildTileView renders one tile. tileID is the display/agent_id the FE shows
// (may be synthetic, e.g. "qgen-mcq").
func (h *agentsHandler) buildTileView(tileID, role string, e agents.Entry, s *perAgentStats) agentView {
	av := agentView{
		AgentID:  tileID,
		Role:     role,
		EngineID: e.EngineID(),
	}
	if s == nil {
		// No traffic in the window — zero invocations + null stats. NEVER
		// fabricate; the FE renders "no recent activity".
		av.Stats = agentStats{Invocations24h: 0}
		return av
	}
	av.Stats.Invocations24h = s.invocations
	if len(s.latencies) > 0 {
		p95 := percentile(s.latencies, 95)
		av.Stats.P95LatencyMs = &p95
	}
	// Refusal rate is an LLM-guardrail metric. Pure orchestrators (LangGraph,
	// which Chora reserves exclusively for orchestrators — they issue no LLM
	// call, e.g. ai_kernel_orchestrator) never "refuse", so the rate is N/A:
	// leave it null rather than reporting a misleading 0%.
	if s.invocations > 0 && !isOrchestrator(e) {
		rate := float64(s.refusals) / float64(s.invocations)
		av.Stats.RefusalRate = &rate
	}
	return av
}

// isOrchestrator reports whether the entry is a pure LangGraph orchestrator
// (no LLM call). Per the Chora convention "Python/LangGraph = orchestrators
// only" (CLAUDE.md), framework=langgraph uniquely identifies these.
func isOrchestrator(e agents.Entry) bool {
	return strings.EqualFold(e.Framework, "langgraph")
}

// agentRoleFromEntry returns a short human-readable role for the FE column.
// Falls back to the registry domain when no agent-specific role mapping is known.
func agentRoleFromEntry(agentID string, e agents.Entry) string {
	switch agentID {
	case "qgen_question":
		return "MCQ generator"
	case "qgen_critic":
		return "Quality critic"
	case "familiar_companion":
		return "Familiar RPG companion"
	case "content_recommender":
		return "Daily-dose recommender"
	case "moderator":
		return "Content moderator"
	case "critic":
		return "Moderation critic"
	case "fog_orchestrator":
		return "Per-user KG fog orchestrator"
	case "ai_kernel_orchestrator":
		return "AI Kernel LangGraph orchestrator"
	case "oe_grader":
		return "OE answer grader"
	case "oe_evaluator":
		return "OE answer evaluator"
	case "oe_moderator":
		return "OE grading moderator"
	case "grading_orchestrator":
		return "Grading saga orchestrator"
	default:
		return e.Domain
	}
}

// percentile returns the p-th percentile (0-100) of latencies via the
// nearest-rank method. Returns 0 for empty input.
func percentile(latencies []int, p int) int {
	if len(latencies) == 0 {
		return 0
	}
	sorted := make([]int, len(latencies))
	copy(sorted, latencies)
	sort.Ints(sorted)
	rank := (p * len(sorted)) / 100
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

// projectFromEnv returns the canonical source-project stamp for the index
// response. Falls back to chora-local when CHORA_PROJECT is unset.
func projectFromEnv() string {
	if p := os.Getenv("CHORA_PROJECT"); p != "" {
		return p
	}
	return "chora-local"
}
