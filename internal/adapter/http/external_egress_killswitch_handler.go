// external_egress_killswitch_handler.go — the O+ platform egress kill-switch
// (CHO-2148; ADR-231 D6).
//
// Engaging this denies EVERY grounded web-egress call, for every tenant, with no
// deploy — the model-gateway reads `platform_egress_killswitch` on each grounded
// call and refuses when engaged, regardless of the tenant's own entitlement.
//
// PLATFORM_OPERATOR only, fail-closed, and never anonymous.
package httpadapter

import (
	"encoding/json"
	"net/http"
	"time"

	ee "github.com/5007-Capstone/chora/services/chora-observability/internal/domain/externalegress"
)

// EgressKillSwitchHandler serves GET/PATCH on the platform egress kill-switch.
type EgressKillSwitchHandler struct {
	repo ee.KillSwitchRepository
}

// NewEgressKillSwitchHandler constructs the handler.
func NewEgressKillSwitchHandler(repo ee.KillSwitchRepository) *EgressKillSwitchHandler {
	return &EgressKillSwitchHandler{repo: repo}
}

type killSwitchDTO struct {
	Engaged       bool   `json:"engaged"`
	Reason        string `json:"reason,omitempty"`
	UpdatedByGCID string `json:"updated_by_gcid,omitempty"`
	UpdatedAt     string `json:"updated_at,omitempty"`
}

type killSwitchPatchReq struct {
	Engaged *bool  `json:"engaged"`
	Reason  string `json:"reason"`
}

func toKillSwitchDTO(ks ee.KillSwitch) killSwitchDTO {
	dto := killSwitchDTO{
		Engaged:       ks.Engaged,
		Reason:        ks.Reason,
		UpdatedByGCID: ks.UpdatedByGCID,
	}
	if !ks.UpdatedAt.IsZero() {
		dto.UpdatedAt = ks.UpdatedAt.UTC().Format(time.RFC3339)
	}
	return dto
}

func (h *EgressKillSwitchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Fail CLOSED. An absent or unrecognised role denies — never the shape that
	// left twelve chora-tenancy admin gates unenforced in production (CHO-2072).
	if !callerIsPlatformOperator(r.Header) {
		writeError(w, http.StatusForbidden, "OBS_FORBIDDEN",
			"the platform egress kill-switch is PLATFORM_OPERATOR-only")
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.get(w, r)
	case http.MethodPatch:
		h.patch(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED", "GET or PATCH only")
	}
}

func (h *EgressKillSwitchHandler) get(w http.ResponseWriter, r *http.Request) {
	ks, err := h.repo.Get(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "OBS_EGRESS_KILLSWITCH_READ_FAILED", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, toKillSwitchDTO(ks))
}

func (h *EgressKillSwitchHandler) patch(w http.ResponseWriter, r *http.Request) {
	actor := callerGCID(r.Header)
	if actor == "" {
		// Disabling web egress platform-wide is never anonymous.
		writeError(w, http.StatusUnauthorized, "OBS_UNAUTHENTICATED",
			"gcid missing — a kill-switch change must name its actor")
		return
	}

	var req killSwitchPatchReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_BODY", err.Error())
		return
	}
	if req.Engaged == nil {
		writeError(w, http.StatusUnprocessableEntity, "OBS_VALIDATION_FAILED",
			"engaged is required")
		return
	}

	ks, err := h.repo.Set(r.Context(), *req.Engaged, req.Reason, actor)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "OBS_EGRESS_KILLSWITCH_WRITE_FAILED", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, toKillSwitchDTO(ks))
}
