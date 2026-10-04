// companion_suspension_handler.go: the O+ Learning Companion containment
// endpoint (ADR-252 D3/D4 via ADR-254 D7).
//
//	GET   /api/v1/admin/companion/suspension
//	PATCH /api/v1/admin/companion/suspension
//
// An operator contains the companion platform-wide, per tenant, or per skill,
// without a deploy; chora-model-gateway reads the result on every companion
// turn and refuses contained turns before any debit. Role matrix (D3):
// platform scope = PLATFORM_OPERATOR only; tenant scope = that tenant's admin /
// owner, or the operator; auditors read and never write; an absent or
// unrecognised role DENIES (fail closed, CHO-2072 lesson). A tenant admin may
// target only their own tenant (the mesh tenant header); the operator may name
// any tenant_id in the body.
//
// Same path family as the egress kill-switch (/api/v1/admin/..., not
// /bff/oplus/...): Cloud Armor rule 994 carves PATCH out only there, and the
// gateway's AuditorGate excludes platform_operator on /bff/oplus/.
package httpadapter

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	cs "github.com/apollo-chora/chora-observability/internal/domain/companionsuspension"
)

// CompanionSuspensionHandler serves GET/PATCH on the containment control.
type CompanionSuspensionHandler struct {
	repo cs.Repository
}

// NewCompanionSuspensionHandler constructs the handler.
func NewCompanionSuspensionHandler(repo cs.Repository) *CompanionSuspensionHandler {
	return &CompanionSuspensionHandler{repo: repo}
}

// suspensionDTO is the wire shape of one containment row (also consumed by the
// chora-gateway proxy and the O+ panel).
type suspensionDTO struct {
	ID         string `json:"id"`
	Scope      string `json:"scope"`
	TenantID   string `json:"tenant_id,omitempty"`
	SkillKey   string `json:"skill_key,omitempty"`
	Engaged    bool   `json:"engaged"`
	Reason     string `json:"reason"`
	EngagedBy  string `json:"engaged_by"`
	EngagedAt  string `json:"engaged_at"`
	ReleasedBy string `json:"released_by,omitempty"`
	ReleasedAt string `json:"released_at,omitempty"`
	Version    int64  `json:"version"`
}

type suspensionListDTO struct {
	Platform []suspensionDTO `json:"platform"`
	Tenant   []suspensionDTO `json:"tenant"`
	TenantID string          `json:"tenant_id,omitempty"`
}

type suspensionPatchReq struct {
	Scope    string `json:"scope"`
	TenantID string `json:"tenant_id"`
	SkillKey string `json:"skill_key"`
	Engaged  *bool  `json:"engaged"`
	Reason   string `json:"reason"`
}

func toSuspensionDTO(s cs.Suspension) suspensionDTO {
	dto := suspensionDTO{
		ID: s.ID, Scope: string(s.Scope), TenantID: s.TenantID, SkillKey: s.SkillKey,
		Engaged: s.Engaged, Reason: s.Reason, EngagedBy: s.EngagedBy, Version: s.Version,
	}
	if !s.EngagedAt.IsZero() {
		dto.EngagedAt = s.EngagedAt.UTC().Format(time.RFC3339)
	}
	if !s.Engaged {
		dto.ReleasedBy = s.ReleasedBy
		if !s.ReleasedAt.IsZero() {
			dto.ReleasedAt = s.ReleasedAt.UTC().Format(time.RFC3339)
		}
	}
	return dto
}

func toSuspensionDTOs(in []cs.Suspension) []suspensionDTO {
	out := make([]suspensionDTO, 0, len(in))
	for _, s := range in {
		out = append(out, toSuspensionDTO(s))
	}
	return out
}

func (h *CompanionSuspensionHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	roles := callerRoles(r.Header)
	// Fail CLOSED: an absent or unrecognised role denies before the method switch.
	if !cs.CanRead(roles) {
		writeError(w, http.StatusForbidden, "OBS_FORBIDDEN",
			"companion containment is readable by platform_operator / auditor / admin / owner only")
		return
	}
	switch r.Method {
	case http.MethodGet:
		h.get(w, r)
	case http.MethodPatch:
		h.patch(w, r, roles)
	default:
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED", "GET or PATCH only")
	}
}

func (h *CompanionSuspensionHandler) get(w http.ResponseWriter, r *http.Request) {
	platform, err := h.repo.ListPlatform(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "OBS_COMPANION_SUSPENSION_READ_FAILED", err.Error())
		return
	}
	out := suspensionListDTO{Platform: toSuspensionDTOs(platform), Tenant: []suspensionDTO{}}
	if tenant := tenantFromContext(r.Context()); tenant != "" {
		rows, err := h.repo.ListTenant(r.Context(), tenant)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "OBS_COMPANION_SUSPENSION_READ_FAILED", err.Error())
			return
		}
		out.Tenant = toSuspensionDTOs(rows)
		out.TenantID = tenant
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *CompanionSuspensionHandler) patch(w http.ResponseWriter, r *http.Request, roles []string) {
	actor := callerGCID(r.Header)
	if actor == "" {
		writeError(w, http.StatusUnauthorized, "OBS_UNAUTHENTICATED",
			"gcid missing: a containment change must name its actor")
		return
	}
	var req suspensionPatchReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_BODY", err.Error())
		return
	}
	scope := cs.Scope(strings.TrimSpace(req.Scope))
	if !scope.Valid() {
		writeError(w, http.StatusUnprocessableEntity, "OBS_VALIDATION_FAILED", cs.ErrInvalidScope.Error())
		return
	}
	if req.Engaged == nil {
		writeError(w, http.StatusUnprocessableEntity, "OBS_VALIDATION_FAILED", "engaged is required")
		return
	}
	if strings.TrimSpace(req.Reason) == "" {
		writeError(w, http.StatusUnprocessableEntity, "OBS_VALIDATION_FAILED", cs.ErrEmptyReason.Error())
		return
	}
	if !cs.CanWrite(roles, scope) {
		writeError(w, http.StatusForbidden, "OBS_FORBIDDEN",
			"platform scope is platform_operator only; tenant scope is the tenant's admin / owner or the operator")
		return
	}

	// Tenant resolution: a tenant admin / owner may only contain their OWN tenant
	// (the validated mesh tenant); the operator may name any tenant_id.
	tenantID := ""
	if scope == cs.ScopeTenant {
		caller := tenantFromContext(r.Context())
		target := strings.TrimSpace(req.TenantID)
		switch {
		case target == "":
			tenantID = caller
		case cs.IsPlatformOperator(roles) || target == caller:
			tenantID = target
		default:
			writeError(w, http.StatusForbidden, "OBS_FORBIDDEN",
				"a tenant admin may only contain their own tenant")
			return
		}
		if tenantID == "" {
			writeError(w, http.StatusUnprocessableEntity, "OBS_VALIDATION_FAILED", cs.ErrEmptyTenantID.Error())
			return
		}
	}

	if *req.Engaged {
		s, err := h.repo.Engage(r.Context(), cs.EngageRequest{
			Scope: scope, TenantID: tenantID, SkillKey: strings.TrimSpace(req.SkillKey),
			Reason: strings.TrimSpace(req.Reason), ActorGCID: actor,
		})
		if err != nil {
			h.writeWriteError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"suspension": toSuspensionDTO(s)})
		return
	}
	released, err := h.repo.Release(r.Context(), cs.ReleaseRequest{
		Scope: scope, TenantID: tenantID, SkillKey: strings.TrimSpace(req.SkillKey),
		Reason: strings.TrimSpace(req.Reason), ActorGCID: actor,
	})
	if err != nil {
		h.writeWriteError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"released": toSuspensionDTOs(released)})
}

func (h *CompanionSuspensionHandler) writeWriteError(w http.ResponseWriter, err error) {
	switch {
	case isValidationError(err):
		writeError(w, http.StatusUnprocessableEntity, "OBS_VALIDATION_FAILED", err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "OBS_COMPANION_SUSPENSION_WRITE_FAILED", err.Error())
	}
}

func isValidationError(err error) bool {
	for _, v := range []error{cs.ErrInvalidScope, cs.ErrEmptyTenantID, cs.ErrTenantOnPlatformScope, cs.ErrEmptyReason, cs.ErrEmptyActor} {
		if errorsIs(err, v) {
			return true
		}
	}
	return false
}
