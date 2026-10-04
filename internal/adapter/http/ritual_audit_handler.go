// ritual_audit_handler.go — O+ auditor endpoint for the Grimoire Ritual run
// audit projection (ADR-215 / ADR-219 CHO-2016).
//
// Endpoint:
//
//	GET /v1/audit/ritual-runs?limit=N — the tenant's most-recent ritual runs
//
// tenant_id is read from the auth-context (X-Tenant-Id via the tenantContext
// middleware) or ?tenant_id= (see resolveTenant, familiar_growth_handler.go).
package httpadapter

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	ra "github.com/apollo-chora/chora-observability/internal/domain/ritualaudit"
)

// RitualAuditHandler wraps the ritualaudit.Repository for HTTP.
type RitualAuditHandler struct {
	repo ra.Repository
}

// NewRitualAuditHandler constructs the handler.
func NewRitualAuditHandler(repo ra.Repository) *RitualAuditHandler {
	return &RitualAuditHandler{repo: repo}
}

// ListRuns returns the tenant's most-recent ritual-run audit rows.
func (h *RitualAuditHandler) ListRuns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED", "only GET is supported")
		return
	}
	tenantID, ok := resolveTenant(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "OBS_TENANT_REQUIRED", "tenant_id is required (header or query)")
		return
	}
	limit, err := parseRitualRunsLimit(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_QUERY", err.Error())
		return
	}
	rows, err := h.repo.List(r.Context(), tenantID, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "OBS_REPO_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items": rows,
		"total": len(rows),
	})
}

// parseRitualRunsLimit parses ?limit=N (default 100, capped at 1000).
func parseRitualRunsLimit(r *http.Request) (int, error) {
	v := strings.TrimSpace(r.URL.Query().Get("limit"))
	if v == "" {
		return 100, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("limit must be int")
	}
	if n <= 0 {
		n = 100
	}
	if n > 1000 {
		n = 1000
	}
	return n, nil
}
