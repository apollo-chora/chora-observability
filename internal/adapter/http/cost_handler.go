// cost_handler.go — Sequel-comic cumulative + by-act handlers.
//
// /api/cost/cumulative + /api/cost/by-act read TokenUsageLedger entries for
// the tenant and roll them up to the comic's "Budget Counter" + value-stream
// breakdown views. Cost stored as int64 micros throughout — no float drift.
package httpadapter

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/apollo-chora/chora-observability/internal/domain/cost"
	"github.com/apollo-chora/chora-observability/internal/domain/ledger"
)

// costCumulative GET /api/cost/cumulative
func (h *Handler) costCumulative(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED",
			"only GET is supported on /api/cost/cumulative")
		return
	}
	tenantID := tenantFromContext(r.Context())
	if v := r.URL.Query().Get("tenant_id"); v != "" {
		// allow query-param override for cross-tenant admin reads
		tenantID = v
	}
	f, err := parseListFilter(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_QUERY", err.Error())
		return
	}
	if f.Limit == 0 {
		f.Limit = 1000 // cost rollups read a fuller window than the 100 default
	}
	entries, err := h.ledgers.List(r.Context(), tenantID, f)
	if err != nil {
		log.Printf("ledger list (cumulative): %v", err)
		writeError(w, http.StatusInternalServerError, "OBS_REPO_ERROR", "failed to list ledger")
		return
	}
	acts := make([]cost.Act, 0, len(entries))
	for _, e := range entries {
		acts = append(acts, cost.Act{
			Name:          e.ModelID,
			CostUsdMicros: e.CostUsdMicros,
			OccurredAt:    e.RecordedAt,
		})
	}
	res := cost.Cumulative(acts)
	writeJSON(w, http.StatusOK, res)
}

// costByAct GET /api/cost/by-act
func (h *Handler) costByAct(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED",
			"only GET is supported on /api/cost/by-act")
		return
	}
	tenantID := tenantFromContext(r.Context())
	if v := r.URL.Query().Get("tenant_id"); v != "" {
		tenantID = v
	}
	entries, err := h.ledgers.List(r.Context(), tenantID, ledger.ListFilter{Limit: 1000})
	if err != nil {
		log.Printf("ledger list (by-act): %v", err)
		writeError(w, http.StatusInternalServerError, "OBS_REPO_ERROR", "failed to list ledger")
		return
	}
	acts := make([]cost.Act, 0, len(entries))
	for _, e := range entries {
		acts = append(acts, cost.Act{
			Name:          e.ModelID,
			CostUsdMicros: e.CostUsdMicros,
			OccurredAt:    e.RecordedAt,
		})
	}
	groups := cost.GroupByAct(acts)
	writeJSON(w, http.StatusOK, map[string]any{"groups": groups})
}

// aggregateTokenUsage GET /api/token-usage/aggregate?group_by=model|agent|gcid
func (h *Handler) aggregateTokenUsage(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantFromContext(r.Context())
	groupBy := r.URL.Query().Get("group_by")
	f, err := parseListFilter(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_QUERY", err.Error())
		return
	}
	if f.Limit == 0 {
		f.Limit = 1000 // cost rollups read a fuller window than the 100 default
	}
	entries, err := h.ledgers.List(r.Context(), tenantID, f)
	if err != nil {
		log.Printf("ledger list (aggregate): %v", err)
		writeError(w, http.StatusInternalServerError, "OBS_REPO_ERROR", "failed to list ledger")
		return
	}
	groups := ledger.AggregateBy(entries, ledger.GroupBy(groupBy))
	writeJSON(w, http.StatusOK, map[string]any{"groups": groups})
}

// postBudget POST /api/token-usage/budget
type postBudgetRequest struct {
	Period       string `json:"period"`
	CapUsdMicros int64  `json:"cap_usd_micros"`
}

func (h *Handler) postBudget(w http.ResponseWriter, r *http.Request) {
	if h.budgets == nil {
		writeError(w, http.StatusServiceUnavailable, "OBS_BUDGET_DISABLED",
			"budget repository not configured")
		return
	}
	var req postBudgetRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_BODY", err.Error())
		return
	}
	tenantID := tenantFromContext(r.Context())
	b, err := ledger.NewBudget(ledger.BudgetParams{
		TenantID:     tenantID,
		Period:       req.Period,
		CapUsdMicros: req.CapUsdMicros,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_BUDGET", err.Error())
		return
	}
	if err := h.budgets.Set(r.Context(), b); err != nil {
		log.Printf("budget set: %v", err)
		writeError(w, http.StatusInternalServerError, "OBS_REPO_ERROR", "failed to persist budget")
		return
	}
	writeJSON(w, http.StatusCreated, b)
}

// getBudget GET /api/token-usage/budget?period=YYYY-MM
type budgetResponse struct {
	BudgetID          string   `json:"budget_id"`
	Period            string   `json:"period"`
	CapUsdMicros      int64    `json:"cap_usd_micros"`
	SpentUsdMicros    int64    `json:"spent_usd_micros"`
	PercentSpent      float64  `json:"percent_spent"`
	ThresholdsCrossed []string `json:"thresholds_crossed"`
}

func (h *Handler) getBudget(w http.ResponseWriter, r *http.Request) {
	if h.budgets == nil {
		writeError(w, http.StatusServiceUnavailable, "OBS_BUDGET_DISABLED",
			"budget repository not configured")
		return
	}
	period := strings.TrimSpace(r.URL.Query().Get("period"))
	if period == "" {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_QUERY", "period is required")
		return
	}
	tenantID := tenantFromContext(r.Context())
	b, err := h.budgets.Get(r.Context(), tenantID, period)
	if err != nil {
		if errors.Is(err, ledger.ErrBudgetNotFound) {
			writeError(w, http.StatusNotFound, "OBS_BUDGET_NOT_FOUND", "budget not found")
			return
		}
		log.Printf("budget get: %v", err)
		writeError(w, http.StatusInternalServerError, "OBS_REPO_ERROR", "internal error")
		return
	}
	// Replay ledger spend into budget for tenants that have spent since
	// the cap was set. Re-runs RecordSpend so threshold-crossings reflect
	// current state. (In production we'd subscribe to the outbox topic,
	// but for the in-memory MVP this aggregate-over-list is correct.)
	entries, err := h.ledgers.List(r.Context(), tenantID, ledger.ListFilter{Limit: 1000})
	if err == nil {
		// Reset spend to zero, replay all entries.
		b.SpentUsdMicros = 0
		b.ThresholdsCrossed = nil
		for _, e := range entries {
			b.RecordSpend(e.CostUsdMicros)
		}
	}

	tcs := make([]string, 0, len(b.ThresholdsCrossed))
	for _, t := range b.ThresholdsCrossed {
		tcs = append(tcs, string(t))
	}
	writeJSON(w, http.StatusOK, budgetResponse{
		BudgetID:          b.BudgetID,
		Period:            b.Period,
		CapUsdMicros:      b.CapUsdMicros,
		SpentUsdMicros:    b.SpentUsdMicros,
		PercentSpent:      b.PercentSpent(),
		ThresholdsCrossed: tcs,
	})
}

// tracesExport POST /api/traces/export — Spanstore range export to the trace
// store (Tempo).
type tracesExportRequest struct {
	Since string `json:"since"`
	Until string `json:"until"`
}

func (h *Handler) tracesExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED",
			"only POST is supported on /api/traces/export")
		return
	}
	if h.traceExport == nil {
		writeError(w, http.StatusServiceUnavailable, "OBS_TRACES_DISABLED",
			"trace exporter not configured")
		return
	}
	var req tracesExportRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_BODY", err.Error())
		return
	}
	since, err := time.Parse(time.RFC3339, req.Since)
	if err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_QUERY",
			"since must be RFC3339")
		return
	}
	until, err := time.Parse(time.RFC3339, req.Until)
	if err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_QUERY",
			"until must be RFC3339")
		return
	}
	if since.After(until) {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_RANGE",
			"since must be before until")
		return
	}
	tenantID := tenantFromContext(r.Context())
	res, err := h.traceExport.Export(r.Context(), TraceExportRequest{
		TenantID: tenantID,
		Since:    since,
		Until:    until,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "OBS_TRACE_EXPORT_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, res)
}

// _ unused json import guard.
var _ = json.NewDecoder
