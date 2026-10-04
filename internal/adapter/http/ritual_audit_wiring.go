// ritual_audit_wiring.go — Option + dispatcher for the ADR-215/ADR-219 ritual
// run audit read route. Lives in a separate file so it can be added without
// touching the long-standing handler.go router definition (mirrors
// familiar_growth_wiring.go).
package httpadapter

import (
	"net/http"

	ra "github.com/5007-Capstone/chora/services/chora-observability/internal/domain/ritualaudit"
)

// WithRitualAuditRepo enables GET /v1/audit/ritual-runs backed by the supplied
// repository. When unset the route returns 503 (see ritualRuns).
func WithRitualAuditRepo(repo ra.Repository) Option {
	return func(h *Handler) { h.ritualAudit = repo }
}

// MountRitualAuditRoutes binds the ritual-run audit read endpoint onto mux.
// Used by NewRouter; the route returns 503 when the repo is not wired.
func (h *Handler) MountRitualAuditRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/v1/audit/ritual-runs", h.ritualRuns)
}

// ritualRuns dispatches GET /v1/audit/ritual-runs.
func (h *Handler) ritualRuns(w http.ResponseWriter, r *http.Request) {
	if h.ritualAudit == nil {
		writeError(w, http.StatusServiceUnavailable, "OBS_RITUAL_AUDIT_UNWIRED",
			"ritual run audit repo not wired")
		return
	}
	NewRitualAuditHandler(h.ritualAudit).ListRuns(w, r)
}
