// companion_suspension_wiring.go: Option + dispatcher for the O+ Learning
// Companion containment control (ADR-252 via ADR-254 D7).
//
//	GET   /api/v1/admin/companion/suspension
//	PATCH /api/v1/admin/companion/suspension
//
// Lives in its own file (mirrors external_egress_killswitch_wiring.go) so the
// long-standing handler.go router definition stays untouched. Unwired => 503,
// never a silent "nobody is suspended": a stubbed answer is indistinguishable
// from a real one and would hide that the platform has no working control.
package httpadapter

import (
	"errors"
	"net/http"

	cs "github.com/5007-Capstone/chora/services/chora-observability/internal/domain/companionsuspension"
)

// WithCompanionSuspensionRepo enables the containment routes.
func WithCompanionSuspensionRepo(repo cs.Repository) Option {
	return func(h *Handler) { h.companionSuspension = repo }
}

// MountCompanionSuspensionRoutes binds the containment endpoint onto mux.
func (h *Handler) MountCompanionSuspensionRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/admin/companion/suspension", h.companionSuspensionRoute)
}

func (h *Handler) companionSuspensionRoute(w http.ResponseWriter, r *http.Request) {
	if h.companionSuspension == nil {
		writeError(w, http.StatusServiceUnavailable, "OBS_COMPANION_SUSPENSION_UNWIRED",
			"companion containment repo not wired")
		return
	}
	NewCompanionSuspensionHandler(h.companionSuspension).ServeHTTP(w, r)
}

// errorsIs keeps the handler file free of a direct errors import beside the
// domain sentinels (small helper; trivially inlined).
func errorsIs(err, target error) bool { return errors.Is(err, target) }
