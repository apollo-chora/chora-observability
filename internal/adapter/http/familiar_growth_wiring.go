// familiar_growth_wiring.go — Option + dispatchers for the ADR-149 Familiar
// Growth audit routes. Lives in a separate file so it can be added without
// touching the long-standing handler.go router definition.
//
// The 4 routes are bound by handler.go (mux.HandleFunc against
// h.familiarGrowth*) — those handler methods live here to keep handler.go
// stable.
package httpadapter

import (
	"net/http"

	fg "github.com/5007-Capstone/chora/services/chora-observability/internal/domain/familiargrowth"
)

// WithFamiliarGrowthRepo enables the ADR-149 audit endpoints
// (/v1/audit/familiar-growth/*) backed by the supplied repository (PROD-H).
//
// When unset, the 4 dispatcher methods return 503 — see
// familiarGrowthEvents / familiarGrowthMetrics / familiarGrowthBreedDistribution /
// familiarGrowthEggFunnel.
func WithFamiliarGrowthRepo(repo fg.Repository) Option {
	return func(h *Handler) { h.familiarGrowth = repo }
}

// MountFamiliarGrowthRoutes binds the 4 PROD-H audit endpoints onto the
// supplied mux. Used by NewRouter via the option pattern; tests call this
// directly to exercise the routes without the tenantContext middleware.
//
// When the handler's familiarGrowth repo is nil the routes return 503.
func (h *Handler) MountFamiliarGrowthRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/v1/audit/familiar-growth/events", h.familiarGrowthEvents)
	mux.HandleFunc("/v1/audit/familiar-growth/metrics", h.familiarGrowthMetrics)
	mux.HandleFunc("/v1/audit/familiar-growth/breed-distribution", h.familiarGrowthBreedDistribution)
	mux.HandleFunc("/v1/audit/familiar-growth/egg-funnel", h.familiarGrowthEggFunnel)
}

// familiarGrowthEvents dispatches GET /v1/audit/familiar-growth/events.
func (h *Handler) familiarGrowthEvents(w http.ResponseWriter, r *http.Request) {
	if h.familiarGrowth == nil {
		writeError(w, http.StatusServiceUnavailable, "OBS_FG_UNWIRED",
			"familiar growth audit repo not wired")
		return
	}
	NewFamiliarGrowthHandler(h.familiarGrowth).ListEvents(w, r)
}

// familiarGrowthMetrics dispatches GET /v1/audit/familiar-growth/metrics.
func (h *Handler) familiarGrowthMetrics(w http.ResponseWriter, r *http.Request) {
	if h.familiarGrowth == nil {
		writeError(w, http.StatusServiceUnavailable, "OBS_FG_UNWIRED",
			"familiar growth audit repo not wired")
		return
	}
	NewFamiliarGrowthHandler(h.familiarGrowth).ListMetrics(w, r)
}

// familiarGrowthBreedDistribution dispatches GET
// /v1/audit/familiar-growth/breed-distribution.
func (h *Handler) familiarGrowthBreedDistribution(w http.ResponseWriter, r *http.Request) {
	if h.familiarGrowth == nil {
		writeError(w, http.StatusServiceUnavailable, "OBS_FG_UNWIRED",
			"familiar growth audit repo not wired")
		return
	}
	NewFamiliarGrowthHandler(h.familiarGrowth).BreedDistribution(w, r)
}

// familiarGrowthEggFunnel dispatches GET /v1/audit/familiar-growth/egg-funnel.
func (h *Handler) familiarGrowthEggFunnel(w http.ResponseWriter, r *http.Request) {
	if h.familiarGrowth == nil {
		writeError(w, http.StatusServiceUnavailable, "OBS_FG_UNWIRED",
			"familiar growth audit repo not wired")
		return
	}
	NewFamiliarGrowthHandler(h.familiarGrowth).EggFunnel(w, r)
}
