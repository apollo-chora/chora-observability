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
//	          "cloud_trace_template_url": "...",
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
// Per anchoring decision #2 (selective deep-link): each agent row carries a
// `cloud_trace_template_url` so the FE can render a "View in Cloud Trace ↗"
// button without O+ rendering raw spans. The link prefers the agent's actual
// latest trace (?tid=) and falls back to a chora.agent_id label filter.
// (The Vertex AI Agent Engine deep-link was removed — decommissioned per
// ADR-169.)
package httpadapter

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/agents"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/decision"
)

// defaultAgentsWindowDays is the aggregation window for the /o/agents view —
// "last quarter" (~90 days), NOT the legacy 24h. Overridable via
// CHORA_AGENTS_WINDOW_DAYS so ops can tune it without a redeploy.
const defaultAgentsWindowDays = 90

// agentsHandler holds the request-time deps.
type agentsHandler struct {
	decisions decision.Repository
	registry  *agents.Registry
	project   string        // GCP project for the Cloud Trace deep-links
	window    time.Duration // aggregation window (default ~90d / last quarter)
}

// newAgentsHandler constructs the handler.
func newAgentsHandler(d decision.Repository, r *agents.Registry, project string) *agentsHandler {
	return &agentsHandler{decisions: d, registry: r, project: project, window: agentsWindow()}
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
	AgentID               string     `json:"agent_id"`
	Role                  string     `json:"role,omitempty"`
	EngineID              string     `json:"engine_id,omitempty"`
	CloudTraceTemplateURL string     `json:"cloud_trace_template_url,omitempty"`
	Stats                 agentStats `json:"stats"`
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
	// latestTraceparent is the W3C traceparent of the agent's most-recent
	// decision row — the source for the per-agent Cloud Trace deep-link
	// (lands on the agent's actual latest trace / parent span).
	latestTraceparent string
}

// add folds one decision row into the running aggregate. List returns rows
// recorded_at ASC, so calling add in iteration order leaves latestTraceparent
// pointing at the agent's most-recent trace.
func (s *perAgentStats) add(l *decision.Log) {
	s.invocations++
	if l.DecisionType == decision.TypeRefuse {
		s.refusals++
	}
	if l.Reasoning != nil && l.Reasoning.LatencyMs > 0 {
		s.latencies = append(s.latencies, l.Reasoning.LatencyMs)
	}
	if l.Traceparent != "" {
		s.latestTraceparent = l.Traceparent
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
//
// Each tile deep-links Cloud Trace to ITS OWN most-recent actual trace — the
// mcq / OE tiles use their per-question-type traceparent (so qgen-mcq lands on
// an mcq generation, qgen-OE on an OE one); see cloudTraceLink.
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
// (may be synthetic, e.g. "qgen-mcq"). The Cloud Trace deep-link is derived
// purely from the tile's own stats (its most-recent traceparent) — see
// cloudTraceLink — so the synthetic id never leaks into the link.
func (h *agentsHandler) buildTileView(tileID, role string, e agents.Entry, s *perAgentStats) agentView {
	av := agentView{
		AgentID:               tileID,
		Role:                  role,
		EngineID:              e.EngineID(),
		CloudTraceTemplateURL: h.cloudTraceLink(s),
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

// cloudTraceLink returns the per-tile Cloud Trace deep-link. When the tile has
// a recent decision row it deep-links to that ACTUAL trace via the new Trace
// Explorer ;traceId= matrix param and — when the row's W3C traceparent carries
// a span id — pins ;spanId= so the link lands on THAT agent's own span. The
// producer (chora-ai-kernel-orchestrator) stamps a per-agent marker span
// (agent.<agid>, attribute chora.agent_id) into each decision's traceparent, so
// span ids differ per agent within the shared crew trace. When the agent has no
// recent decision row it falls back to the Trace Explorer scoped to the project
// (the new Explorer ignores the legacy ?filter=; there is no usable per-agent
// fallback filter).
func (h *agentsHandler) cloudTraceLink(s *perAgentStats) string {
	if s != nil {
		if tid := traceIDFromTraceparent(s.latestTraceparent); tid != "" {
			return cloudTraceTraceURL(h.project, tid, spanIDFromTraceparent(s.latestTraceparent))
		}
	}
	return cloudTraceExplorerURL(h.project)
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

// cloudTraceTraceURL deep-links to a SPECIFIC trace in the new Cloud Trace
// Explorer via the ;traceId= matrix param (verified 2026-06-09: it opens the
// trace's waterfall on cold load; the console auto-adds the default ;query= +
// ;duration=PT1H). When spanID is non-empty it additionally pins ;spanId= so
// the link selects that agent's own span within the trace (verified: clicking
// a span adds ;spanId=). The legacy /traces/list?tid= is DEAD — the console
// redirects it to /traces/explorer and silently drops the param.
func cloudTraceTraceURL(project, traceID, spanID string) string {
	if project == "" || traceID == "" {
		return ""
	}
	if spanID != "" {
		return fmt.Sprintf(
			"https://console.cloud.google.com/traces/explorer;traceId=%s;spanId=%s?project=%s",
			traceID, spanID, project,
		)
	}
	return fmt.Sprintf(
		"https://console.cloud.google.com/traces/explorer;traceId=%s?project=%s",
		traceID, project,
	)
}

// cloudTraceExplorerURL is the no-recent-trace fallback: it opens the Cloud
// Trace Explorer scoped to the project. There is no usable per-agent span
// attribute to filter on (see cloudTraceLink), and the new Explorer ignores
// the legacy ?filter= param, so this intentionally does not attempt a per-agent
// filter — tiles with a recent trace deep-link via cloudTraceTraceURL instead.
func cloudTraceExplorerURL(project string) string {
	if project == "" {
		return ""
	}
	return fmt.Sprintf("https://console.cloud.google.com/traces/explorer?project=%s", project)
}

// traceIDFromTraceparent extracts the 32-hex trace-id from a W3C traceparent
// ("version-traceid-spanid-flags", e.g. 00-<32hex>-<16hex>-01). Returns ""
// when the value is absent or malformed.
func traceIDFromTraceparent(tp string) string {
	parts := strings.Split(tp, "-")
	if len(parts) < 3 {
		return ""
	}
	tid := parts[1]
	if len(tid) != 32 {
		return ""
	}
	for _, c := range tid {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return ""
		}
	}
	return tid
}

// spanIDFromTraceparent extracts the 16-hex span-id (parts[2]) from a W3C
// traceparent ("version-traceid-spanid-flags"). Returns "" when absent or
// malformed. Used to pin ;spanId= so the Cloud Trace deep-link selects the
// agent's own per-agent span within the shared crew trace.
func spanIDFromTraceparent(tp string) string {
	parts := strings.Split(tp, "-")
	if len(parts) < 3 {
		return ""
	}
	sid := parts[2]
	if len(sid) != 16 {
		return ""
	}
	for _, c := range sid {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return ""
		}
	}
	return sid
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

// projectFromEnv returns the canonical project for the deep-link URLs.
// Falls back to chora-489812 per ADR-144 when CHORA_PROJECT is unset.
func projectFromEnv() string {
	if p := os.Getenv("CHORA_PROJECT"); p != "" {
		return p
	}
	if p := os.Getenv("GOOGLE_CLOUD_PROJECT"); p != "" {
		return p
	}
	return "chora-489812"
}
