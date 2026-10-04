// agent_prompts_handler.go - GET /api/v1/observability/agent-prompts
// (CHO-2364, ADR-197 read slice).
//
// The O+ prompt-evidence view answers "which prompt versions actually drove
// production decisions, per agent". Exactly five agent rows are ALWAYS
// present, in this order:
//
//	qgen_question  (crew qgen,       decision evidence, 3 use-case buckets)
//	qgen_critic    (crew qgen,       decision evidence, 3 use-case buckets)
//	oe_evaluator   (crew oe_grading, decision evidence, no use cases)
//	oe_moderator   (crew oe_grading, decision evidence, no use cases)
//	familiar       (crew familiar,   ritual stamp evidence)
//
// Decision evidence aggregates agent_decision_log.prompt_conditions via the
// decision.PromptUsageRepository port; familiar evidence aggregates the
// ritual_run_audit stamps array via ritualaudit.PromptStampRepository. Rows
// with no evidence report zeros + empty arrays; latest_*/last_* keys are
// OMITTED when unknown, never fabricated. Timestamps are RFC3339 UTC.
//
// Use-case buckets (qgen_question + qgen_critic only) partition the same
// decision rows by the ADR-197 discriminators:
//
//	ai_assist_single: NOT (prompt_conditions ? 'set_mode')
//	batch:            (prompt_conditions ? 'set_mode') AND
//	                  request_surface IS DISTINCT FROM 'campaign'
//	daily_dose:       request_surface = 'campaign'
//
// All three buckets are always present, even at zero decisions.
package httpadapter

import (
	"log"
	"net/http"
	"sort"
	"time"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/decision"
	ra "github.com/5007-Capstone/chora/services/chora-observability/internal/domain/ritualaudit"
)

// Five-agent contract constants. qgenQuestionAgentID / qgenCriticAgentID /
// qgenCrewName come from agents_handler.go (same package).
const (
	oeGradingCrewName    = "oe_grading"
	oeEvaluatorAgentID   = "oe_evaluator"
	oeModeratorAgentID   = "oe_moderator"
	familiarPromptRowID  = "familiar"
	evidenceKindDecision = "decisions"
	evidenceKindStamps   = "ritual_stamps"

	useCaseAIAssistSingle = "ai_assist_single"
	useCaseBatch          = "batch"
	useCaseDailyDose      = "daily_dose"
)

// promptEvidenceAgentSpec is one decision-evidence row of the contract.
type promptEvidenceAgentSpec struct {
	agentID      string
	crewName     string
	withUseCases bool
}

// promptEvidenceDecisionAgents is the fixed decision-evidence agent order.
var promptEvidenceDecisionAgents = []promptEvidenceAgentSpec{
	{agentID: qgenQuestionAgentID, crewName: qgenCrewName, withUseCases: true},
	{agentID: qgenCriticAgentID, crewName: qgenCrewName, withUseCases: true},
	{agentID: oeEvaluatorAgentID, crewName: oeGradingCrewName, withUseCases: false},
	{agentID: oeModeratorAgentID, crewName: oeGradingCrewName, withUseCases: false},
}

// agentPromptsHandler holds the two aggregation ports.
type agentPromptsHandler struct {
	usage  decision.PromptUsageRepository
	stamps ra.PromptStampRepository
}

// newAgentPromptsHandler constructs the handler. Either port may be nil (the
// route then 503s loudly; see serveAgentPrompts).
func newAgentPromptsHandler(usage decision.PromptUsageRepository, stamps ra.PromptStampRepository) *agentPromptsHandler {
	return &agentPromptsHandler{usage: usage, stamps: stamps}
}

// -----------------------------------------------------------------------------
// Wire shapes (snake_case; the BFF passes the body through untouched)
// -----------------------------------------------------------------------------

type agentPromptsResponse struct {
	Agents []agentPromptRowView `json:"agents"`
}

type agentPromptRowView struct {
	AgentID      string `json:"agent_id"`
	CrewName     string `json:"crew_name"`
	EvidenceKind string `json:"evidence_kind"`
	// DecisionsTotal is set (0 included) on decision-evidence rows only;
	// RunsTotal is set on the familiar row only. Pointers keep the absent
	// counter off the other kind's row.
	DecisionsTotal      *int64              `json:"decisions_total,omitempty"`
	LastDecisionAt      string              `json:"last_decision_at,omitempty"`
	RunsTotal           *int64              `json:"runs_total,omitempty"`
	LastRunAt           string              `json:"last_run_at,omitempty"`
	LatestPromptVersion string              `json:"latest_prompt_version,omitempty"`
	LatestPromptSource  string              `json:"latest_prompt_source,omitempty"`
	Versions            []promptVersionView `json:"versions"`
	UseCases            []promptUseCaseView `json:"use_cases,omitempty"`
}

type promptVersionView struct {
	PromptVersion string `json:"prompt_version"`
	PromptSource  string `json:"prompt_source,omitempty"`
	// Decisions is set on decision-evidence entries; Runs on ritual-stamp
	// entries. Entries only exist with a count >= 1, so omitempty never
	// hides a real zero.
	Decisions *int64 `json:"decisions,omitempty"`
	Runs      *int64 `json:"runs,omitempty"`
	LastSeen  string `json:"last_seen"`
}

type promptUseCaseView struct {
	Key                 string `json:"key"`
	Decisions           int64  `json:"decisions"`
	LastSeen            string `json:"last_seen,omitempty"`
	LatestPromptVersion string `json:"latest_prompt_version,omitempty"`
	LatestPromptSource  string `json:"latest_prompt_source,omitempty"`
}

// -----------------------------------------------------------------------------
// Handler
// -----------------------------------------------------------------------------

// serveAgentPrompts handles GET /api/v1/observability/agent-prompts.
func (h *agentPromptsHandler) serveAgentPrompts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED",
			"only GET is supported on /api/v1/observability/agent-prompts")
		return
	}
	if h.usage == nil {
		writeError(w, http.StatusServiceUnavailable, "OBS_PROMPT_EVIDENCE_UNWIRED",
			"prompt usage aggregation not wired on the decision repository")
		return
	}
	if h.stamps == nil {
		writeError(w, http.StatusServiceUnavailable, "OBS_PROMPT_EVIDENCE_UNWIRED",
			"prompt stamp aggregation not wired on the ritual audit repository")
		return
	}
	tenantID := tenantFromContext(r.Context())
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "OBS_TENANT_REQUIRED",
			"X-Tenant-Id header is required")
		return
	}

	agentIDs := make([]string, 0, len(promptEvidenceDecisionAgents))
	for _, spec := range promptEvidenceDecisionAgents {
		agentIDs = append(agentIDs, spec.agentID)
	}
	groups, err := h.usage.AggregatePromptUsage(r.Context(), tenantID, agentIDs)
	if err != nil {
		log.Printf("agent prompts usage aggregation: %v", err)
		writeError(w, http.StatusInternalServerError, "OBS_REPO_ERROR",
			"prompt usage aggregation failed")
		return
	}
	stampSum, err := h.stamps.AggregatePromptStamps(r.Context(), tenantID)
	if err != nil {
		log.Printf("agent prompts stamp aggregation: %v", err)
		writeError(w, http.StatusInternalServerError, "OBS_REPO_ERROR",
			"prompt stamp aggregation failed")
		return
	}

	byAgent := make(map[string][]decision.PromptUsageGroup)
	for _, g := range groups {
		byAgent[g.AgentID] = append(byAgent[g.AgentID], g)
	}

	rows := make([]agentPromptRowView, 0, len(promptEvidenceDecisionAgents)+1)
	for _, spec := range promptEvidenceDecisionAgents {
		rows = append(rows, buildDecisionPromptRow(spec, byAgent[spec.agentID]))
	}
	rows = append(rows, buildFamiliarPromptRow(stampSum))
	writeJSON(w, http.StatusOK, agentPromptsResponse{Agents: rows})
}

// -----------------------------------------------------------------------------
// Row composition
// -----------------------------------------------------------------------------

// buildDecisionPromptRow folds one agent's aggregate groups into its wire row.
func buildDecisionPromptRow(spec promptEvidenceAgentSpec, groups []decision.PromptUsageGroup) agentPromptRowView {
	row := agentPromptRowView{
		AgentID:      spec.agentID,
		CrewName:     spec.crewName,
		EvidenceKind: evidenceKindDecision,
		Versions:     make([]promptVersionView, 0),
	}
	var total int64
	var lastDecision time.Time
	for _, g := range groups {
		total += g.Decisions
		if g.LastSeen.After(lastDecision) {
			lastDecision = g.LastSeen
		}
	}
	row.DecisionsTotal = &total
	if total > 0 && !lastDecision.IsZero() {
		row.LastDecisionAt = rfc3339UTC(lastDecision)
	}
	row.Versions = buildDecisionVersionViews(groups)
	if v, s, ok := latestVersionSource(groups, nil); ok {
		row.LatestPromptVersion = v
		row.LatestPromptSource = s
	}
	if spec.withUseCases {
		row.UseCases = buildUseCaseViews(groups)
	}
	return row
}

// buildDecisionVersionViews folds the non-empty-version groups into the
// versions[] array: GROUP BY (prompt_version, prompt_source) with summed
// counts + max last_seen, ordered most-recent-first (deterministic).
func buildDecisionVersionViews(groups []decision.PromptUsageGroup) []promptVersionView {
	type vk struct{ version, source string }
	type agg struct {
		decisions int64
		lastSeen  time.Time
	}
	acc := make(map[vk]*agg)
	for _, g := range groups {
		if g.PromptVersion == "" {
			continue
		}
		k := vk{g.PromptVersion, g.PromptSource}
		a, ok := acc[k]
		if !ok {
			a = &agg{}
			acc[k] = a
		}
		a.decisions += g.Decisions
		if g.LastSeen.After(a.lastSeen) {
			a.lastSeen = g.LastSeen
		}
	}
	out := make([]promptVersionView, 0, len(acc))
	for k, a := range acc {
		d := a.decisions
		out = append(out, promptVersionView{
			PromptVersion: k.version,
			PromptSource:  k.source,
			Decisions:     &d,
			LastSeen:      rfc3339UTC(a.lastSeen),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].LastSeen != out[j].LastSeen {
			return out[i].LastSeen > out[j].LastSeen
		}
		if out[i].PromptVersion != out[j].PromptVersion {
			return out[i].PromptVersion < out[j].PromptVersion
		}
		return out[i].PromptSource < out[j].PromptSource
	})
	return out
}

// buildUseCaseViews partitions the agent's groups into the three fixed
// ADR-197 use-case buckets. Buckets are always present, even at zero.
func buildUseCaseViews(groups []decision.PromptUsageGroup) []promptUseCaseView {
	buckets := []struct {
		key   string
		match func(decision.PromptUsageGroup) bool
	}{
		{useCaseAIAssistSingle, func(g decision.PromptUsageGroup) bool { return !g.HasSetMode }},
		{useCaseBatch, func(g decision.PromptUsageGroup) bool { return g.HasSetMode && !g.IsCampaign }},
		{useCaseDailyDose, func(g decision.PromptUsageGroup) bool { return g.IsCampaign }},
	}
	out := make([]promptUseCaseView, 0, len(buckets))
	for _, b := range buckets {
		uc := promptUseCaseView{Key: b.key}
		var lastSeen time.Time
		for _, g := range groups {
			if !b.match(g) {
				continue
			}
			uc.Decisions += g.Decisions
			if g.LastSeen.After(lastSeen) {
				lastSeen = g.LastSeen
			}
		}
		if uc.Decisions > 0 && !lastSeen.IsZero() {
			uc.LastSeen = rfc3339UTC(lastSeen)
		}
		if v, s, ok := latestVersionSource(groups, b.match); ok {
			uc.LatestPromptVersion = v
			uc.LatestPromptSource = s
		}
		out = append(out, uc)
	}
	return out
}

// latestVersionSource resolves the latest non-empty prompt version + source
// among the groups matching filter (nil filter = all groups): the group with
// the greatest LastSeen whose PromptVersion is non-empty. ok=false when no
// such group exists (the caller then omits both keys, never fabricates).
// Ties break deterministically on (version, source) ascending.
func latestVersionSource(groups []decision.PromptUsageGroup, filter func(decision.PromptUsageGroup) bool) (version, source string, ok bool) {
	var best decision.PromptUsageGroup
	for _, g := range groups {
		if g.PromptVersion == "" {
			continue
		}
		if filter != nil && !filter(g) {
			continue
		}
		if !ok ||
			g.LastSeen.After(best.LastSeen) ||
			(g.LastSeen.Equal(best.LastSeen) &&
				(g.PromptVersion < best.PromptVersion ||
					(g.PromptVersion == best.PromptVersion && g.PromptSource < best.PromptSource))) {
			best = g
			ok = true
		}
	}
	if !ok {
		return "", "", false
	}
	return best.PromptVersion, best.PromptSource, true
}

// buildFamiliarPromptRow projects the ritual stamp summary into the familiar
// wire row.
func buildFamiliarPromptRow(sum ra.PromptStampSummary) agentPromptRowView {
	total := sum.RunsTotal
	row := agentPromptRowView{
		AgentID:      familiarPromptRowID,
		CrewName:     familiarPromptRowID,
		EvidenceKind: evidenceKindStamps,
		RunsTotal:    &total,
		Versions:     make([]promptVersionView, 0, len(sum.Versions)),
	}
	if sum.RunsTotal > 0 && !sum.LastRunAt.IsZero() {
		row.LastRunAt = rfc3339UTC(sum.LastRunAt)
	}
	for _, v := range sum.Versions {
		runs := v.Runs
		row.Versions = append(row.Versions, promptVersionView{
			PromptVersion: v.PromptVersion,
			Runs:          &runs,
			LastSeen:      rfc3339UTC(v.LastSeen),
		})
	}
	// Deterministic most-recent-first ordering (mirrors the decision rows).
	sort.Slice(row.Versions, func(i, j int) bool {
		if row.Versions[i].LastSeen != row.Versions[j].LastSeen {
			return row.Versions[i].LastSeen > row.Versions[j].LastSeen
		}
		return row.Versions[i].PromptVersion < row.Versions[j].PromptVersion
	})
	return row
}

// rfc3339UTC renders a timestamp as RFC3339 in UTC (the wire contract).
func rfc3339UTC(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}
