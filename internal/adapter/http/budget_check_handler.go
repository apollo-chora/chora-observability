// budget_check_handler.go — POST /api/token-usage/budget-check
//
// 3-level budget cascade pre-check called by the Model Gateway BEFORE every
// LLM invocation. Returns ledger.AllowDecision; the Gateway maps a denied
// decision to HTTP 429 GATEWAY_BUDGET_EXHAUSTED + Retry-After header.
//
// Per ai-cost-tracking skill ("3-level budget cascade") + Tier 3 D10
// hard-cap enforcement at the Router/Gateway boundary.
package httpadapter

import (
	"net/http"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/ledger"
)

// WithBudgetLookup wires the 3-level budget cascade for /api/token-usage/budget-check.
func WithBudgetLookup(l ledger.BudgetLookup) Option {
	return func(h *Handler) { h.budgetLookup = l }
}

// budgetCheckRequest mirrors ledger.CheckRequest from the wire side.
type budgetCheckRequest struct {
	TenantID            string `json:"tenant_id"`
	Gcid                string `json:"gcid"`
	AgentID             string `json:"agent_id"`
	Period              string `json:"period"`
	ProjectedCostMicros int64  `json:"projected_cost_micros"`
}

// budgetCheck POST /api/token-usage/budget-check
func (h *Handler) budgetCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED",
			"only POST is supported on /api/token-usage/budget-check")
		return
	}
	if h.budgetLookup == nil {
		writeError(w, http.StatusServiceUnavailable, "OBS_BUDGET_LOOKUP_DISABLED",
			"budget lookup not configured")
		return
	}
	var req budgetCheckRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_BODY", err.Error())
		return
	}
	if req.ProjectedCostMicros < 0 {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_PROJECTION",
			"projected_cost_micros must be >= 0")
		return
	}

	// Tenant from header context (X-Tenant-ID middleware) takes precedence over
	// payload-supplied tenant_id (defense in depth — never trust caller-supplied
	// tenant id when a header was already validated).
	tenantID := tenantFromContext(r.Context())
	if tenantID == "" {
		tenantID = req.TenantID
	}

	enforcer := ledger.NewBudgetEnforcer(h.budgetLookup)
	dec := enforcer.Check(r.Context(), ledger.CheckRequest{
		TenantID:            tenantID,
		Gcid:                req.Gcid,
		AgentID:             req.AgentID,
		Period:              req.Period,
		ProjectedCostMicros: req.ProjectedCostMicros,
	})
	writeJSON(w, http.StatusOK, dec)
}
