// agent_decisions_count_handler.go — GET /api/v1/observability/agent-decisions/count
//
// Backs the O+ Dashboard `recent_decisions_24h` rollup. The chora-gateway BFF
// calls this endpoint with the last-24h window so the dashboard envelope can
// populate the FE-visible "Recent Decisions (24h)" counter without leaking the
// raw agent_decision_log rows.
//
// Query contract:
//
//	GET /api/v1/observability/agent-decisions/count?since=<RFC3339>&until=<RFC3339>
//
// Tenant scope: X-Tenant-Id header (extracted by the tenantContext middleware).
//
// Defaults:
//
//   - since unset → 24h ago (UTC)
//   - until unset → now (UTC)
//
// Backing query (pg.DecisionRepository.Count): real SELECT COUNT(*) over
// chora_observability.agent_decision_log with the half-open [since, until)
// window. In-memory dev path mirrors the same semantics. Per
// [[feedback-no-stubs-real-wiring]] — a zero-count result is honest; query
// errors surface loudly as 500.
package httpadapter

import (
	"log"
	"net/http"
	"time"
)

// agentDecisionsCountResponse is the GET response envelope.
type agentDecisionsCountResponse struct {
	Count int64  `json:"count"`
	Since string `json:"since"`
	Until string `json:"until"`
}

// agentDecisionsCount handles GET /api/v1/observability/agent-decisions/count.
//
// Window semantics:
//
//   - When ?since is present it must be RFC3339; missing → now-24h
//   - When ?until is present it must be RFC3339; missing → now
//   - When since >= until the handler returns 400 (window must be positive)
func (h *Handler) agentDecisionsCount(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED",
			"only GET is supported on /api/v1/observability/agent-decisions/count")
		return
	}
	if h.decisions == nil {
		writeError(w, http.StatusServiceUnavailable, "OBS_DECISIONS_UNAVAILABLE",
			"decision repository not wired")
		return
	}
	tenantID := tenantFromContext(r.Context())
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "OBS_TENANT_REQUIRED",
			"X-Tenant-Id header is required")
		return
	}
	now := time.Now().UTC()
	q := r.URL.Query()
	until := now
	if v := q.Get("until"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "OBS_INVALID_QUERY",
				"until must be RFC3339")
			return
		}
		until = t.UTC()
	}
	since := until.Add(-24 * time.Hour)
	if v := q.Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "OBS_INVALID_QUERY",
				"since must be RFC3339")
			return
		}
		since = t.UTC()
	}
	if !since.Before(until) {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_QUERY",
			"since must be strictly before until")
		return
	}
	n, err := h.decisions.Count(r.Context(), tenantID, since, until)
	if err != nil {
		log.Printf("agent_decisions_count: %v", err)
		writeError(w, http.StatusInternalServerError, "OBS_REPO_ERROR",
			"agent decision count failed")
		return
	}
	writeJSON(w, http.StatusOK, agentDecisionsCountResponse{
		Count: n,
		Since: since.Format(time.RFC3339),
		Until: until.Format(time.RFC3339),
	})
}
