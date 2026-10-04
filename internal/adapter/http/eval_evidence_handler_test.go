package httpadapter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/apollo-chora/chora-observability/internal/adapter/inmem"
	"github.com/apollo-chora/chora-observability/internal/domain/eval"
)

type fakeEvalRepo struct {
	runs    []eval.Run
	rows    []eval.EvidenceRow
	runFilt eval.RunFilter
	evFilt  eval.EvidenceFilter
	err     error
}

func (f *fakeEvalRepo) ListRuns(_ context.Context, flt eval.RunFilter) ([]eval.Run, error) {
	f.runFilt = flt
	if f.err != nil {
		return nil, f.err
	}
	return f.runs, nil
}

func (f *fakeEvalRepo) ListEvidence(_ context.Context, flt eval.EvidenceFilter) ([]eval.EvidenceRow, error) {
	f.evFilt = flt
	if f.err != nil {
		return nil, f.err
	}
	return f.rows, nil
}

func newEvalRouter(t *testing.T, repo eval.Repository) http.Handler {
	t.Helper()
	ledgers := inmem.NewLedgerRepository()
	decisions := inmem.NewDecisionRepository()
	correlations := inmem.NewCorrelationRepository()
	var opts []Option
	if repo != nil {
		opts = append(opts, WithEvalEvidenceRepo(repo))
	}
	return NewRouter(ledgers, decisions, correlations, opts...)
}

func fixtureRuns() []eval.Run {
	t0 := time.Date(2026, 6, 6, 20, 59, 0, 0, time.UTC)
	return []eval.Run{{
		CandidateLabel: "depbump0607",
		LastRecordedAt: t0,
		Members: []eval.RunMember{
			{
				Experiment:       "chora-agent-eval-qgen-critic",
				AutoraterMetrics: []eval.MetricSummary{{Metric: "safety", Count: 4, AvgScore: 1}},
				Adversarial:      nil, // adversarial-less member
			},
			{
				Experiment: "chora-agent-eval-qgen-question",
				AutoraterMetrics: []eval.MetricSummary{
					{Metric: "instruction_following", Count: 4, AvgScore: 4.75},
					{Metric: "safety", Count: 4, AvgScore: 1},
				},
				Adversarial: &eval.AdversarialSummary{Total: 6, Blocked: 6, Leaked: 0},
			},
		},
	}}
}

func fixtureRows() []eval.EvidenceRow {
	t0 := time.Date(2026, 6, 6, 20, 59, 0, 0, time.UTC)
	return []eval.EvidenceRow{
		{Experiment: "chora-agent-eval-qgen-question", Kind: "autorater", CaseID: "c1", RowIndex: 0, Metric: "safety", Score: 1, Explanation: "ok", Prompt: "p", Response: "r", RecordedAt: t0},
		{Experiment: "chora-agent-eval-qgen-question", Kind: "adversarial", CaseID: "a1", RowIndex: 0, Metric: "jailbreak", Score: 1, AdversarialVerdict: "BLOCKED(pass)", Explanation: "refused", Prompt: "attack", Response: "no", RecordedAt: t0},
	}
}

type listRespDTO struct {
	Runs []struct {
		CandidateLabel string `json:"candidate_label"`
		LastRecordedAt string `json:"last_recorded_at"`
		Members        []struct {
			Experiment       string `json:"experiment"`
			AutoraterMetrics []struct {
				Metric   string  `json:"metric"`
				Count    int     `json:"count"`
				AvgScore float64 `json:"avg_score"`
			} `json:"autorater_metrics"`
			Adversarial *struct {
				Total   int `json:"total"`
				Blocked int `json:"blocked"`
				Leaked  int `json:"leaked"`
			} `json:"adversarial"`
		} `json:"members"`
	} `json:"runs"`
}

func TestEvalRuns_RequiresGET(t *testing.T) {
	router := newEvalRouter(t, &fakeEvalRepo{})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/observability/eval-runs", nil)
	req.Header.Set("X-Tenant-Id", "t1")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d; want 405", rr.Code)
	}
}

func TestEvalRuns_RequiresTenant(t *testing.T) {
	router := newEvalRouter(t, &fakeEvalRepo{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/observability/eval-runs", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400 (X-Tenant-Id required)", rr.Code)
	}
}

func TestEvalRuns_NotWiredReturns503(t *testing.T) {
	router := newEvalRouter(t, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/observability/eval-runs", nil)
	req.Header.Set("X-Tenant-Id", "t1")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d; want 503", rr.Code)
	}
}

func TestEvalRuns_HappyPath(t *testing.T) {
	repo := &fakeEvalRepo{runs: fixtureRuns()}
	router := newEvalRouter(t, repo)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/observability/eval-runs", nil)
	req.Header.Set("X-Tenant-Id", "t1")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200; body=%s", rr.Code, rr.Body.String())
	}
	// Default limit applied.
	if repo.runFilt.Limit != 50 {
		t.Errorf("default limit = %d; want 50", repo.runFilt.Limit)
	}
	var got listRespDTO
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Runs) != 1 || got.Runs[0].CandidateLabel != "depbump0607" {
		t.Fatalf("runs = %+v", got.Runs)
	}
	if len(got.Runs[0].Members) != 2 {
		t.Fatalf("members = %d", len(got.Runs[0].Members))
	}
	critic := got.Runs[0].Members[0]
	if critic.Experiment != "chora-agent-eval-qgen-critic" {
		t.Errorf("member[0] = %q", critic.Experiment)
	}
	if critic.Adversarial != nil {
		t.Errorf("critic adversarial = %+v; want null", critic.Adversarial)
	}
	if len(critic.AutoraterMetrics) != 1 {
		t.Errorf("critic metrics = %d; want 1 (non-nil array)", len(critic.AutoraterMetrics))
	}
	question := got.Runs[0].Members[1]
	if question.Adversarial == nil || question.Adversarial.Blocked != 6 {
		t.Errorf("question adversarial = %+v", question.Adversarial)
	}
}

func TestEvalRuns_FiltersAndLimitClamp(t *testing.T) {
	repo := &fakeEvalRepo{runs: fixtureRuns()}
	router := newEvalRouter(t, repo)
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/observability/eval-runs?experiment=chora-agent-eval-qgen-critic&kind=autorater&limit=999", nil)
	req.Header.Set("X-Tenant-Id", "t1")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d; body=%s", rr.Code, rr.Body.String())
	}
	if repo.runFilt.Experiment != "chora-agent-eval-qgen-critic" || repo.runFilt.Kind != "autorater" {
		t.Errorf("filter not passed through: %+v", repo.runFilt)
	}
	if repo.runFilt.Limit != 200 {
		t.Errorf("limit = %d; want clamped to 200", repo.runFilt.Limit)
	}
}

func TestEvalRuns_InvalidKind(t *testing.T) {
	router := newEvalRouter(t, &fakeEvalRepo{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/observability/eval-runs?kind=bogus", nil)
	req.Header.Set("X-Tenant-Id", "t1")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", rr.Code)
	}
}

func TestEvalRuns_InvalidLimit(t *testing.T) {
	router := newEvalRouter(t, &fakeEvalRepo{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/observability/eval-runs?limit=-3", nil)
	req.Header.Set("X-Tenant-Id", "t1")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", rr.Code)
	}
}

func TestEvalRuns_RepoError(t *testing.T) {
	router := newEvalRouter(t, &fakeEvalRepo{err: errors.New("bq down")})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/observability/eval-runs", nil)
	req.Header.Set("X-Tenant-Id", "t1")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("status = %d; want 500", rr.Code)
	}
}

func TestEvalRunEvidence_HappyPath(t *testing.T) {
	repo := &fakeEvalRepo{rows: fixtureRows()}
	router := newEvalRouter(t, repo)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/observability/eval-runs/depbump0607", nil)
	req.Header.Set("X-Tenant-Id", "t1")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d; body=%s", rr.Code, rr.Body.String())
	}
	if repo.evFilt.CandidateLabel != "depbump0607" {
		t.Errorf("candidate_label filter = %q", repo.evFilt.CandidateLabel)
	}
	if repo.evFilt.Limit != 200 {
		t.Errorf("default evidence limit = %d; want 200", repo.evFilt.Limit)
	}
	var got struct {
		CandidateLabel string `json:"candidate_label"`
		Rows           []struct {
			Kind               string  `json:"kind"`
			Metric             string  `json:"metric"`
			Score              float64 `json:"score"`
			AdversarialVerdict string  `json:"adversarial_verdict"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.CandidateLabel != "depbump0607" || len(got.Rows) != 2 {
		t.Fatalf("got = %+v", got)
	}
	// adversarial row carries the verdict; autorater omits it.
	var sawAdv bool
	for _, r := range got.Rows {
		if r.Kind == "adversarial" {
			sawAdv = true
			if r.AdversarialVerdict != "BLOCKED(pass)" {
				t.Errorf("adversarial verdict = %q", r.AdversarialVerdict)
			}
		}
		if r.Kind == "autorater" && r.AdversarialVerdict != "" {
			t.Errorf("autorater row leaked a verdict: %q", r.AdversarialVerdict)
		}
	}
	if !sawAdv {
		t.Error("no adversarial row in evidence")
	}
}

func TestEvalRunEvidence_MissingLabelReturns404(t *testing.T) {
	router := newEvalRouter(t, &fakeEvalRepo{rows: fixtureRows()})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/observability/eval-runs/", nil)
	req.Header.Set("X-Tenant-Id", "t1")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d; want 404", rr.Code)
	}
}

func TestEvalRunEvidence_Filters(t *testing.T) {
	repo := &fakeEvalRepo{rows: fixtureRows()}
	router := newEvalRouter(t, repo)
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/observability/eval-runs/depbump0607?experiment=chora-agent-eval-qgen-question&kind=adversarial&limit=5", nil)
	req.Header.Set("X-Tenant-Id", "t1")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d; body=%s", rr.Code, rr.Body.String())
	}
	if repo.evFilt.Experiment != "chora-agent-eval-qgen-question" || repo.evFilt.Kind != "adversarial" || repo.evFilt.Limit != 5 {
		t.Errorf("evidence filter = %+v", repo.evFilt)
	}
}

func TestEvalRunEvidence_RepoError(t *testing.T) {
	router := newEvalRouter(t, &fakeEvalRepo{err: errors.New("bq down")})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/observability/eval-runs/depbump0607", nil)
	req.Header.Set("X-Tenant-Id", "t1")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("status = %d; want 500", rr.Code)
	}
}

func TestEvalRunEvidence_NotWiredReturns503(t *testing.T) {
	router := newEvalRouter(t, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/observability/eval-runs/depbump0607", nil)
	req.Header.Set("X-Tenant-Id", "t1")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d; want 503", rr.Code)
	}
}
