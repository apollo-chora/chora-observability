// eval_evidence_handler.go — the O+ agent-eval evidence read API.
//
//	GET /api/v1/observability/eval-runs                      — crew-run index
//	GET /api/v1/observability/eval-runs/{candidateLabel}     — per-row drill-down
//
// Backs the O+ Agent-Eval drill-down (IMDA D2 transparency evidence) via the
// chora-gateway BFF /bff/oplus/eval-runs[...]. Source: the BigQuery
// agent_eval_evidence view (eval.Repository).
//
// Scope: PLATFORM-level eval telemetry. The view has no tenant_id column —
// the X-Tenant-Id header is required (the tenantContext middleware enforces it
// on /api/*) but is NOT applied as a query filter. Auditor/admin authorization
// is enforced upstream by the BFF AuditorGate; chora-observability trusts the
// forwarded context.
//
// Per [[feedback-no-stubs-real-wiring]]: a real BigQuery read. An empty result
// is honest; query errors surface as 500.
package httpadapter

import (
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/eval"
)

const (
	evalRunsPath       = "/api/v1/observability/eval-runs"
	evalRunsPrefix     = "/api/v1/observability/eval-runs/"
	evalRunsDefaultLim = 50
	evalRunsMaxLim     = 200
	evalRowsDefaultLim = 200
	evalRowsMaxLim     = 1000
)

// --- response DTOs (json shapes per observability-admin.yaml) ---------------

type evalMetricSummaryDTO struct {
	Metric   string  `json:"metric"`
	Count    int     `json:"count"`
	AvgScore float64 `json:"avg_score"`
}

type evalAdversarialSummaryDTO struct {
	Total   int `json:"total"`
	Blocked int `json:"blocked"`
	Leaked  int `json:"leaked"`
}

type evalRunMemberDTO struct {
	Experiment       string                     `json:"experiment"`
	AutoraterMetrics []evalMetricSummaryDTO     `json:"autorater_metrics"`
	Adversarial      *evalAdversarialSummaryDTO `json:"adversarial"`
}

type evalRunDTO struct {
	CandidateLabel string             `json:"candidate_label"`
	LastRecordedAt string             `json:"last_recorded_at"`
	Members        []evalRunMemberDTO `json:"members"`
}

type evalRunListResponse struct {
	Runs []evalRunDTO `json:"runs"`
}

type evalEvidenceRowDTO struct {
	Experiment         string  `json:"experiment"`
	Kind               string  `json:"kind"`
	CaseID             string  `json:"case_id"`
	RowIndex           int     `json:"row_index"`
	Metric             string  `json:"metric"`
	Score              float64 `json:"score"`
	AdversarialVerdict string  `json:"adversarial_verdict,omitempty"`
	Explanation        string  `json:"explanation"`
	Prompt             string  `json:"prompt"`
	Response           string  `json:"response"`
	Reference          string  `json:"reference,omitempty"`
	RecordedAt         string  `json:"recorded_at"`
}

type evalRunEvidenceResponse struct {
	CandidateLabel string               `json:"candidate_label"`
	Rows           []evalEvidenceRowDTO `json:"rows"`
}

// --- handlers ---------------------------------------------------------------

// evalRunsList handles GET /api/v1/observability/eval-runs.
func (h *Handler) evalRunsList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED",
			"only GET is supported on /api/v1/observability/eval-runs")
		return
	}
	if h.evalEvidence == nil {
		writeError(w, http.StatusServiceUnavailable, "OBS_EVAL_UNAVAILABLE",
			"agent-eval evidence repository not wired")
		return
	}
	// Platform-scoped data, but the middleware requires X-Tenant-Id on /api/*.
	if tenantFromContext(r.Context()) == "" {
		writeError(w, http.StatusBadRequest, "OBS_TENANT_REQUIRED",
			"X-Tenant-Id header is required")
		return
	}
	q := r.URL.Query()
	kind := strings.TrimSpace(q.Get("kind"))
	if !validEvalKind(kind) {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_QUERY",
			"kind must be autorater or adversarial")
		return
	}
	f := eval.RunFilter{
		Experiment: strings.TrimSpace(q.Get("experiment")),
		Kind:       kind,
		Limit:      evalRunsDefaultLim,
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "OBS_INVALID_QUERY",
				"limit must be a positive int")
			return
		}
		if n > evalRunsMaxLim {
			n = evalRunsMaxLim
		}
		f.Limit = n
	}
	runs, err := h.evalEvidence.ListRuns(r.Context(), f)
	if err != nil {
		log.Printf("eval list-runs: %v", err)
		writeError(w, http.StatusInternalServerError, "OBS_REPO_ERROR",
			"agent-eval run listing failed")
		return
	}
	writeJSON(w, http.StatusOK, evalRunListResponse{Runs: toEvalRunDTOs(runs)})
}

// evalRunEvidence handles GET /api/v1/observability/eval-runs/{candidateLabel}.
func (h *Handler) evalRunEvidence(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED",
			"only GET is supported on /api/v1/observability/eval-runs/{candidateLabel}")
		return
	}
	if h.evalEvidence == nil {
		writeError(w, http.StatusServiceUnavailable, "OBS_EVAL_UNAVAILABLE",
			"agent-eval evidence repository not wired")
		return
	}
	if tenantFromContext(r.Context()) == "" {
		writeError(w, http.StatusBadRequest, "OBS_TENANT_REQUIRED",
			"X-Tenant-Id header is required")
		return
	}
	label := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, evalRunsPrefix), "/")
	if decoded, err := url.PathUnescape(label); err == nil {
		label = decoded
	}
	if label == "" || strings.Contains(label, "/") {
		writeError(w, http.StatusNotFound, "OBS_NOT_FOUND", "candidate label required")
		return
	}
	q := r.URL.Query()
	kind := strings.TrimSpace(q.Get("kind"))
	if !validEvalKind(kind) {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_QUERY",
			"kind must be autorater or adversarial")
		return
	}
	f := eval.EvidenceFilter{
		CandidateLabel: label,
		Experiment:     strings.TrimSpace(q.Get("experiment")),
		Kind:           kind,
		Limit:          evalRowsDefaultLim,
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "OBS_INVALID_QUERY",
				"limit must be a positive int")
			return
		}
		if n > evalRowsMaxLim {
			n = evalRowsMaxLim
		}
		f.Limit = n
	}
	rows, err := h.evalEvidence.ListEvidence(r.Context(), f)
	if err != nil {
		log.Printf("eval evidence: %v", err)
		writeError(w, http.StatusInternalServerError, "OBS_REPO_ERROR",
			"agent-eval evidence lookup failed")
		return
	}
	writeJSON(w, http.StatusOK, evalRunEvidenceResponse{
		CandidateLabel: label,
		Rows:           toEvidenceRowDTOs(rows),
	})
}

// --- mapping helpers --------------------------------------------------------

func validEvalKind(k string) bool {
	return k == "" || k == "autorater" || k == "adversarial"
}

func toEvalRunDTOs(runs []eval.Run) []evalRunDTO {
	out := make([]evalRunDTO, 0, len(runs))
	for _, r := range runs {
		members := make([]evalRunMemberDTO, 0, len(r.Members))
		for _, m := range r.Members {
			metrics := make([]evalMetricSummaryDTO, 0, len(m.AutoraterMetrics))
			for _, ms := range m.AutoraterMetrics {
				metrics = append(metrics, evalMetricSummaryDTO{
					Metric:   ms.Metric,
					Count:    ms.Count,
					AvgScore: ms.AvgScore,
				})
			}
			md := evalRunMemberDTO{Experiment: m.Experiment, AutoraterMetrics: metrics}
			if m.Adversarial != nil {
				md.Adversarial = &evalAdversarialSummaryDTO{
					Total:   m.Adversarial.Total,
					Blocked: m.Adversarial.Blocked,
					Leaked:  m.Adversarial.Leaked,
				}
			}
			members = append(members, md)
		}
		out = append(out, evalRunDTO{
			CandidateLabel: r.CandidateLabel,
			LastRecordedAt: r.LastRecordedAt.UTC().Format(time.RFC3339),
			Members:        members,
		})
	}
	return out
}

func toEvidenceRowDTOs(rows []eval.EvidenceRow) []evalEvidenceRowDTO {
	out := make([]evalEvidenceRowDTO, 0, len(rows))
	for _, r := range rows {
		out = append(out, evalEvidenceRowDTO{
			Experiment:         r.Experiment,
			Kind:               r.Kind,
			CaseID:             r.CaseID,
			RowIndex:           r.RowIndex,
			Metric:             r.Metric,
			Score:              r.Score,
			AdversarialVerdict: r.AdversarialVerdict,
			Explanation:        r.Explanation,
			Prompt:             r.Prompt,
			Response:           r.Response,
			Reference:          r.Reference,
			RecordedAt:         r.RecordedAt.UTC().Format(time.RFC3339),
		})
	}
	return out
}
