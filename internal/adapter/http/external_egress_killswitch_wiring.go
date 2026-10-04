// external_egress_killswitch_wiring.go — Option + dispatcher for the O+ platform
// egress kill-switch (CHO-2148; ADR-231 D6).
//
//	GET   /api/v1/admin/egress/kill-switch
//	PATCH /api/v1/admin/egress/kill-switch
//
// The kill-switch is the platform's override on Far Sight web egress: engaging
// it makes the model-gateway deny EVERY grounded call, for every tenant,
// regardless of any tenant entitlement — with no deploy. It is operator-owned,
// PLATFORM_OPERATOR-only, and fails closed.
//
// Why this path and not /bff/oplus/…: the gateway's `/bff/oplus/` prefix has no
// Cloud Armor method-enforcement carve-out (the WAF 403s PATCH at the edge and
// the policy is at its rule cap), and its AuditorGate admits auditor/admin/owner
// but NOT platform_operator — a pure operator would be refused before reaching
// this handler. `/api/v1/admin/` is already covered by both. The O+ surface still
// owns the UI.
//
// Lives in its own file (mirrors ritual_audit_wiring.go / familiar_growth_wiring.go)
// so the long-standing handler.go router definition stays untouched.
package httpadapter

import (
	"net/http"

	ee "github.com/5007-Capstone/chora/services/chora-observability/internal/domain/externalegress"
)

// WithEgressKillSwitchRepo enables the platform egress kill-switch routes.
// When unset the route returns 503 (never a silent "not engaged" — a stubbed
// answer here is indistinguishable from a real one and would hide the fact that
// the platform has no working override).
func WithEgressKillSwitchRepo(repo ee.KillSwitchRepository) Option {
	return func(h *Handler) { h.egressKillSwitch = repo }
}

// MountEgressKillSwitchRoutes binds the kill-switch endpoint onto mux.
func (h *Handler) MountEgressKillSwitchRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/admin/egress/kill-switch", h.egressKillSwitchRoute)
}

func (h *Handler) egressKillSwitchRoute(w http.ResponseWriter, r *http.Request) {
	if h.egressKillSwitch == nil {
		writeError(w, http.StatusServiceUnavailable, "OBS_EGRESS_KILLSWITCH_UNWIRED",
			"platform egress kill-switch repo not wired")
		return
	}
	NewEgressKillSwitchHandler(h.egressKillSwitch).ServeHTTP(w, r)
}
