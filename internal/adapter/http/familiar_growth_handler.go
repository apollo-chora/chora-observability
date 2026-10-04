// familiar_growth_handler.go — O+ governance dashboard endpoints for the
// ADR-149 Familiar Growth audit ledger.
//
// Endpoints:
//
//	GET /v1/audit/familiar-growth/events             — paginated audit ledger
//	GET /v1/audit/familiar-growth/metrics            — daily metrics rollup
//	GET /v1/audit/familiar-growth/breed-distribution — IMDA D2 (empirical vs claimed + chi-square)
//	GET /v1/audit/familiar-growth/egg-funnel         — purchase -> hatch funnel
//
// Query parameters: tenant_id is read from the X-Tenant-Id header (per the
// existing tenantContext middleware) when not supplied in ?tenant_id=.
// from/to are RFC3339; source/familiar_id/egg_sku optional filters.
package httpadapter

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	fg "github.com/apollo-chora/chora-observability/internal/domain/familiargrowth"
)

// FamiliarGrowthHandler wraps the familiargrowth.Repository for HTTP.
type FamiliarGrowthHandler struct {
	repo fg.Repository
}

// NewFamiliarGrowthHandler constructs the handler.
func NewFamiliarGrowthHandler(repo fg.Repository) *FamiliarGrowthHandler {
	return &FamiliarGrowthHandler{repo: repo}
}

// RegisterFamiliarGrowthRoutes binds the 4 audit endpoints onto mux.
func RegisterFamiliarGrowthRoutes(mux *http.ServeMux, repo fg.Repository) {
	h := NewFamiliarGrowthHandler(repo)
	mux.HandleFunc("/v1/audit/familiar-growth/events", h.ListEvents)
	mux.HandleFunc("/v1/audit/familiar-growth/metrics", h.ListMetrics)
	mux.HandleFunc("/v1/audit/familiar-growth/breed-distribution", h.BreedDistribution)
	mux.HandleFunc("/v1/audit/familiar-growth/egg-funnel", h.EggFunnel)
}

// ListEvents returns the paginated audit ledger for the tenant.
func (h *FamiliarGrowthHandler) ListEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED", "only GET is supported")
		return
	}
	tenantID, ok := resolveTenant(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "OBS_TENANT_REQUIRED", "tenant_id is required (header or query)")
		return
	}
	f, err := parseFamiliarGrowthFilter(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_QUERY", err.Error())
		return
	}
	rows, err := h.repo.ListLedger(r.Context(), tenantID, f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "OBS_REPO_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items": rows,
		"total": len(rows),
	})
}

// ListMetrics returns the daily-rollup audit metrics for the tenant.
func (h *FamiliarGrowthHandler) ListMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED", "only GET is supported")
		return
	}
	tenantID, ok := resolveTenant(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "OBS_TENANT_REQUIRED", "tenant_id is required (header or query)")
		return
	}
	f, err := parseFamiliarGrowthFilter(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_QUERY", err.Error())
		return
	}
	rows, err := h.repo.ListMetrics(r.Context(), tenantID, f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "OBS_REPO_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items": rows,
		"total": len(rows),
	})
}

// BreedDistribution returns the IMDA D2 transparency report — empirical
// vs claimed distribution for an egg SKU over the audit window plus a
// chi-square goodness-of-fit summary.
func (h *FamiliarGrowthHandler) BreedDistribution(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED", "only GET is supported")
		return
	}
	tenantID, ok := resolveTenant(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "OBS_TENANT_REQUIRED", "tenant_id is required (header or query)")
		return
	}
	f, err := parseFamiliarGrowthFilter(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_QUERY", err.Error())
		return
	}
	if strings.TrimSpace(f.EggSKU) == "" {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_QUERY", "egg_sku query param is required")
		return
	}
	rolls, err := h.repo.ListBreedRolls(r.Context(), tenantID, f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "OBS_REPO_ERROR", err.Error())
		return
	}
	report := buildBreedDistributionReport(tenantID, f, rolls)
	writeJSON(w, http.StatusOK, report)
}

// EggFunnel returns the per-day egg-funnel rollup for the tenant.
func (h *FamiliarGrowthHandler) EggFunnel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED", "only GET is supported")
		return
	}
	tenantID, ok := resolveTenant(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "OBS_TENANT_REQUIRED", "tenant_id is required (header or query)")
		return
	}
	f, err := parseFamiliarGrowthFilter(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_QUERY", err.Error())
		return
	}
	rows, err := h.repo.ListEggFunnel(r.Context(), tenantID, f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "OBS_REPO_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items": rows,
		"total": len(rows),
	})
}

// buildBreedDistributionReport assembles the response shape from the rolls.
func buildBreedDistributionReport(tenantID string, f fg.AuditFilter, rolls []fg.BreedRollAuditRow) fg.BreedDistributionReport {
	report := fg.BreedDistributionReport{
		TenantID:               tenantID,
		EggSKU:                 f.EggSKU,
		From:                   f.From,
		To:                     f.To,
		SampleSize:             len(rolls),
		Empirical:              make(map[string]int),
		EmpiricalProbabilities: make(map[string]float64),
		Claimed:                make(map[string]float64),
	}
	if len(rolls) == 0 {
		return report
	}
	for _, r := range rolls {
		report.Empirical[r.Species]++
	}
	for k, v := range report.Empirical {
		report.EmpiricalProbabilities[k] = float64(v) / float64(report.SampleSize)
	}
	// Claimed = distribution_snapshot from the most recent roll in the
	// window. rolls are sorted DESC by RevealedAt (per repo contract).
	latest := rolls[0].DistributionSnapshot
	for k, v := range latest {
		switch t := v.(type) {
		case float64:
			report.Claimed[k] = normaliseProbability(t)
		case float32:
			report.Claimed[k] = normaliseProbability(float64(t))
		case int:
			report.Claimed[k] = normaliseProbability(float64(t))
		case int64:
			report.Claimed[k] = normaliseProbability(float64(t))
		}
	}
	if len(report.Claimed) > 0 {
		cs := fg.ChiSquareGoodnessOfFit(report.Empirical, report.Claimed)
		report.ChiSquare = &cs
	}
	return report
}

// normaliseProbability accepts either a 0-100 percentage or a 0-1 ratio
// and returns a 0-1 ratio. Heuristic: values > 1 are treated as percents.
func normaliseProbability(v float64) float64 {
	if v > 1.0 {
		return v / 100.0
	}
	return v
}

// parseFamiliarGrowthFilter parses the common query parameters.
func parseFamiliarGrowthFilter(r *http.Request) (fg.AuditFilter, error) {
	q := r.URL.Query()
	var f fg.AuditFilter
	if v := q.Get("from"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return f, fmt.Errorf("from must be RFC3339")
		}
		f.From = t
	}
	if v := q.Get("to"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return f, fmt.Errorf("to must be RFC3339")
		}
		f.To = t
	}
	if v := q.Get("source"); v != "" {
		f.Source = strings.TrimSpace(v)
	}
	if v := q.Get("familiar_id"); v != "" {
		f.FamiliarID = strings.TrimSpace(v)
	}
	if v := q.Get("egg_sku"); v != "" {
		f.EggSKU = strings.TrimSpace(v)
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return f, fmt.Errorf("limit must be int")
		}
		f.Limit = n
	}
	if v := q.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return f, fmt.Errorf("offset must be int")
		}
		f.Offset = n
	}
	return f, nil
}

// resolveTenant returns the tenant_id from the X-Tenant-Id header (via
// context, when middleware ran) or from ?tenant_id=. Returns (id, true)
// when found.
func resolveTenant(r *http.Request) (string, bool) {
	if v := strings.TrimSpace(tenantFromContext(r.Context())); v != "" {
		return v, true
	}
	if v := strings.TrimSpace(r.Header.Get("X-Tenant-Id")); v != "" {
		return v, true
	}
	if v := strings.TrimSpace(r.URL.Query().Get("tenant_id")); v != "" {
		return v, true
	}
	return "", false
}
