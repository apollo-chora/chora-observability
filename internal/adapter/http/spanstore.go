// spanstore.go — Spanstore query API for the O+ Decision-Log Explorer.
//
// Per gap-action-list 2026-05-09 §3.4 + audit-platform-fillgaps §3.4 (Spanstore
// query API for O+ — currently absent). Two endpoints:
//
//	GET /api/v1/observability/spans?trace_id={X}        — Cloud Trace read
//	GET /api/v1/observability/agent-decisions?run_id={X} — RLS-scoped local DB read
//
// Auth: Bearer token; role must be Observer or Auditor (per the task brief).
// In the in-memory MVP, the role check accepts X-Chora-Role header values
// "observer" or "auditor". Production will replace with JWT claims from
// Identity Platform.
package httpadapter

import (
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/decision"
)

// spanstoreQuery GET /api/v1/observability/spans?trace_id={X}
//
// Reads from Cloud Trace via the trace-exporter port. trace_id is a 32-hex
// W3C trace ID.
func (h *Handler) spanstoreQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED",
			"only GET is supported")
		return
	}
	if !checkObserverOrAuditorRole(r) {
		writeError(w, http.StatusForbidden, "OBS_FORBIDDEN",
			"role must be Observer or Auditor")
		return
	}
	if h.traceExport == nil {
		writeError(w, http.StatusServiceUnavailable, "OBS_TRACES_DISABLED",
			"trace exporter not configured")
		return
	}
	traceID := strings.TrimSpace(r.URL.Query().Get("trace_id"))
	if traceID == "" {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_QUERY",
			"trace_id is required")
		return
	}
	tenantID := tenantFromContext(r.Context())
	// Default range = last 24h; callers can override.
	now := time.Now().UTC()
	since := now.Add(-24 * time.Hour)
	until := now
	res, err := h.traceExport.Export(r.Context(), TraceExportRequest{
		TenantID: tenantID,
		TraceID:  traceID,
		Since:    since,
		Until:    until,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "OBS_SPANSTORE_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"trace_id": traceID,
		"export":   res,
	})
}

// spanstoreAgentDecisions GET /api/v1/observability/agent-decisions?run_id={X}
//
// RLS-scoped read of AgentDecisionLog for the caller's tenant_id, filtered
// by correlation_id (= run_id in the schema).
func (h *Handler) spanstoreAgentDecisions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED",
			"only GET is supported")
		return
	}
	if !checkObserverOrAuditorRole(r) {
		writeError(w, http.StatusForbidden, "OBS_FORBIDDEN",
			"role must be Observer or Auditor")
		return
	}
	runID := strings.TrimSpace(r.URL.Query().Get("run_id"))
	if runID == "" {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_QUERY",
			"run_id is required")
		return
	}
	tenantID := tenantFromContext(r.Context())
	// List all decisions for the tenant, filter by correlation_id in-handler.
	all, err := h.decisions.List(r.Context(), tenantID, decision.ListFilter{Limit: 1000})
	if err != nil {
		log.Printf("spanstore decisions list: %v", err)
		writeError(w, http.StatusInternalServerError, "OBS_REPO_ERROR", "failed to list decisions")
		return
	}
	matched := make([]*decision.Log, 0)
	for _, d := range all {
		if d.CorrelationID == runID {
			matched = append(matched, d)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"run_id": runID,
		"items":  matched,
		"total":  len(matched),
	})
}

// checkObserverOrAuditorRole inspects the Authorization Bearer header + the
// X-Chora-Role header. In the in-memory MVP, role is a header pass-through;
// production replaces with JWT claims.
//
// Accepts: role ∈ {"observer", "auditor"} (case-insensitive).
func checkObserverOrAuditorRole(r *http.Request) bool {
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(strings.ToLower(auth), "bearer ") {
		return false
	}
	role := strings.ToLower(strings.TrimSpace(r.Header.Get("X-Chora-Role")))
	switch role {
	case "observer", "auditor":
		return true
	}
	return false
}
