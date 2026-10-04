// Package httpadapter — Phase-6 BFF-facing events handler.
//
// Added so the chora-gateway HTTPUpstream.GetAuditEvents method has a
// concrete downstream target. Projects AgentDecisionLog entries as audit
// events for the named tenant.
//
// Phase 6 contract:
//
//	GET /events?tenant_id=X → array of decision log entries (audit events)
//
// Response body: JSON array of AgentDecisionLog objects.
//
// This file is non-overlapping with the existing handler.go to satisfy
// the Phase-6 guardrail (NEW files OK; route-registration added at the
// bottom of handler.go with a clear "Phase 6:" comment marker).
package httpadapter

import (
	"log"
	"net/http"
	"strings"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/decision"
)

// phase6EventsHandler is the GET /events?tenant_id=X handler.
type phase6EventsHandler struct {
	decisions decision.Repository
}

// newPhase6EventsHandler constructs the handler.
func newPhase6EventsHandler(repo decision.Repository) http.Handler {
	return &phase6EventsHandler{decisions: repo}
}

// ServeHTTP implements http.Handler.
func (h *phase6EventsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED",
			"only GET is supported on /events")
		return
	}
	tenantID := strings.TrimSpace(r.URL.Query().Get("tenant_id"))
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "OBS_TENANT_REQUIRED",
			"tenant_id query parameter is required")
		return
	}
	logs, err := h.decisions.List(r.Context(), tenantID, decision.ListFilter{})
	if err != nil {
		log.Printf("phase6 events list error: %v", err)
		writeError(w, http.StatusInternalServerError, "OBS_REPO_ERROR", "list failed")
		return
	}
	// Normalise nil slice -> empty array so the BFF gets a stable shape.
	if logs == nil {
		logs = []*decision.Log{}
	}
	writeJSON(w, http.StatusOK, logs)
}
