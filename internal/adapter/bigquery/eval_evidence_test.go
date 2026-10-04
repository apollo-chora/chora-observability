package bigquery

import (
	"strings"
	"testing"
	"time"

	gbq "cloud.google.com/go/bigquery"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/eval"
)

func valid(f float64) gbq.NullFloat64 { return gbq.NullFloat64{Float64: f, Valid: true} }

func TestAssembleRuns_TwoMembersBothKinds(t *testing.T) {
	t0 := time.Date(2026, 6, 6, 20, 59, 0, 0, time.UTC)
	rows := []evalAggRow{
		{CandidateLabel: "depbump0607", Experiment: "chora-agent-eval-qgen-question", Kind: "autorater", Metric: "safety", N: 4, AvgScore: valid(1.0), LastAt: t0},
		{CandidateLabel: "depbump0607", Experiment: "chora-agent-eval-qgen-question", Kind: "autorater", Metric: "instruction_following", N: 4, AvgScore: valid(4.75), LastAt: t0.Add(-2 * time.Second)},
		{CandidateLabel: "depbump0607", Experiment: "chora-agent-eval-qgen-question", Kind: "adversarial", Metric: "jailbreak", N: 6, Blocked: 6, LastAt: t0.Add(-5 * time.Second)},
		{CandidateLabel: "depbump0607", Experiment: "chora-agent-eval-qgen-critic", Kind: "autorater", Metric: "safety", N: 4, AvgScore: valid(1.0), LastAt: t0.Add(-time.Minute)},
	}
	runs := assembleRuns(rows, 50)
	if len(runs) != 1 {
		t.Fatalf("want 1 crew run, got %d", len(runs))
	}
	r := runs[0]
	if r.CandidateLabel != "depbump0607" {
		t.Fatalf("candidate_label = %q", r.CandidateLabel)
	}
	if !r.LastRecordedAt.Equal(t0) {
		t.Errorf("last_recorded_at = %v; want max %v", r.LastRecordedAt, t0)
	}
	if len(r.Members) != 2 {
		t.Fatalf("want 2 members, got %d", len(r.Members))
	}
	// Members sorted by experiment: critic < question.
	if r.Members[0].Experiment != "chora-agent-eval-qgen-critic" {
		t.Errorf("members[0] = %q; want critic first", r.Members[0].Experiment)
	}
	q := r.Members[1]
	if q.Experiment != "chora-agent-eval-qgen-question" {
		t.Fatalf("members[1] = %q", q.Experiment)
	}
	if len(q.AutoraterMetrics) != 2 {
		t.Fatalf("question autorater metrics = %d; want 2", len(q.AutoraterMetrics))
	}
	// Metrics sorted by name: instruction_following < safety.
	if q.AutoraterMetrics[0].Metric != "instruction_following" || q.AutoraterMetrics[1].Metric != "safety" {
		t.Errorf("metric order = %v", q.AutoraterMetrics)
	}
	if q.AutoraterMetrics[1].AvgScore != 1.0 || q.AutoraterMetrics[0].AvgScore != 4.75 {
		t.Errorf("avg scores = %v", q.AutoraterMetrics)
	}
	if q.Adversarial == nil {
		t.Fatal("question adversarial summary nil")
	}
	if q.Adversarial.Total != 6 || q.Adversarial.Blocked != 6 || q.Adversarial.Leaked != 0 {
		t.Errorf("adversarial = %+v; want 6/6/0", *q.Adversarial)
	}
	// Critic had only an autorater row → no adversarial summary.
	if r.Members[0].Adversarial != nil {
		t.Errorf("critic adversarial = %+v; want nil", *r.Members[0].Adversarial)
	}
}

func TestAssembleRuns_OrderingAndLimit(t *testing.T) {
	older := time.Date(2026, 6, 5, 10, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 6, 6, 10, 0, 0, 0, time.UTC)
	rows := []evalAggRow{
		{CandidateLabel: "old", Experiment: "m", Kind: "autorater", Metric: "safety", N: 1, AvgScore: valid(1), LastAt: older},
		{CandidateLabel: "new", Experiment: "m", Kind: "autorater", Metric: "safety", N: 1, AvgScore: valid(1), LastAt: newer},
	}
	all := assembleRuns(rows, 0) // 0 = no cap
	if len(all) != 2 || all[0].CandidateLabel != "new" {
		t.Fatalf("most-recent-first ordering broken: %+v", all)
	}
	capped := assembleRuns(rows, 1)
	if len(capped) != 1 || capped[0].CandidateLabel != "new" {
		t.Fatalf("limit cap broken: %+v", capped)
	}
}

func TestAssembleRuns_AdversarialLeakedAndAdvOnlyMember(t *testing.T) {
	t0 := time.Date(2026, 6, 6, 20, 0, 0, 0, time.UTC)
	rows := []evalAggRow{
		{CandidateLabel: "r", Experiment: "m", Kind: "adversarial", Metric: "jailbreak", N: 3, Blocked: 2, LastAt: t0},
		{CandidateLabel: "r", Experiment: "m", Kind: "adversarial", Metric: "pii_exfiltration", N: 3, Blocked: 3, LastAt: t0},
	}
	runs := assembleRuns(rows, 0)
	if len(runs) != 1 || len(runs[0].Members) != 1 {
		t.Fatalf("shape: %+v", runs)
	}
	m := runs[0].Members[0]
	if len(m.AutoraterMetrics) != 0 {
		t.Errorf("adv-only member should have 0 autorater metrics, got %d", len(m.AutoraterMetrics))
	}
	if m.Adversarial == nil || m.Adversarial.Total != 6 || m.Adversarial.Blocked != 5 || m.Adversarial.Leaked != 1 {
		t.Errorf("adversarial accumulation = %+v; want 6/5/1", m.Adversarial)
	}
}

func TestAssembleRuns_Empty(t *testing.T) {
	if got := assembleRuns(nil, 50); len(got) != 0 {
		t.Fatalf("empty input -> %d runs", len(got))
	}
}

func TestAssembleRuns_InvalidAvgAndTimestampTie(t *testing.T) {
	t0 := time.Date(2026, 6, 6, 20, 0, 0, 0, time.UTC)
	// NullFloat64{} (Valid=false) — the AVG(...) all-null edge case.
	rows := []evalAggRow{
		{CandidateLabel: "b-label", Experiment: "m", Kind: "autorater", Metric: "safety", N: 1, AvgScore: gbq.NullFloat64{}, LastAt: t0},
		// Same LastAt as b-label → tie broken by candidate_label ASC.
		{CandidateLabel: "a-label", Experiment: "m", Kind: "autorater", Metric: "safety", N: 1, AvgScore: valid(1), LastAt: t0},
	}
	runs := assembleRuns(rows, 0)
	if len(runs) != 2 {
		t.Fatalf("runs = %d; want 2", len(runs))
	}
	if runs[0].CandidateLabel != "a-label" || runs[1].CandidateLabel != "b-label" {
		t.Fatalf("tie-break order = %s, %s; want a-label, b-label", runs[0].CandidateLabel, runs[1].CandidateLabel)
	}
	if runs[1].Members[0].AutoraterMetrics[0].AvgScore != 0.0 {
		t.Errorf("invalid avg_score should map to 0.0, got %v", runs[1].Members[0].AutoraterMetrics[0].AvgScore)
	}
}

func TestBuildListRunsQuery(t *testing.T) {
	q, p := buildListRunsQuery("`p.d.v`", eval.RunFilter{})
	if !strings.Contains(q, "GROUP BY candidate_label, experiment, kind, metric") {
		t.Errorf("missing GROUP BY:\n%s", q)
	}
	if !strings.Contains(q, "`p.d.v`") {
		t.Errorf("missing table ref:\n%s", q)
	}
	if !strings.Contains(q, "COUNTIF(STARTS_WITH(adversarial_verdict, 'BLOCKED'))") {
		t.Errorf("missing blocked countif:\n%s", q)
	}
	if len(p) != 0 {
		t.Errorf("no-filter params = %d; want 0", len(p))
	}
	q, p = buildListRunsQuery("`p.d.v`", eval.RunFilter{Experiment: "chora-agent-eval-qgen-critic", Kind: "autorater"})
	if !strings.Contains(q, "@experiment") || !strings.Contains(q, "@kind") {
		t.Errorf("filter clauses missing:\n%s", q)
	}
	if len(p) != 2 {
		t.Errorf("params = %d; want 2", len(p))
	}
}

func TestBuildEvidenceQuery(t *testing.T) {
	q, p := buildEvidenceQuery("`p.d.v`", eval.EvidenceFilter{CandidateLabel: "run1"})
	if !strings.Contains(q, "candidate_label = @candidate_label") {
		t.Errorf("missing candidate_label filter:\n%s", q)
	}
	if !strings.Contains(q, "LIMIT @lim") {
		t.Errorf("missing limit:\n%s", q)
	}
	// candidate_label + lim
	if len(p) != 2 {
		t.Fatalf("base params = %d; want 2", len(p))
	}
	q, p = buildEvidenceQuery("`p.d.v`", eval.EvidenceFilter{CandidateLabel: "run1", Experiment: "m", Kind: "adversarial", Limit: 10})
	if len(p) != 4 {
		t.Fatalf("filtered params = %d; want 4", len(p))
	}
	// limit param value applied
	var lim any
	for _, qp := range p {
		if qp.Name == "lim" {
			lim = qp.Value
		}
	}
	if lim != int64(10) {
		t.Errorf("lim param = %v; want int64(10)", lim)
	}
}

func TestToEvidenceRow_NullHandling(t *testing.T) {
	t0 := time.Date(2026, 6, 6, 20, 0, 0, 0, time.UTC)
	in := evalEvidenceScanRow{
		Experiment: "m",
		Kind:       "autorater",
		CaseID:     gbq.NullString{}, // null → ""
		RowIndex:   gbq.NullInt64{Int64: 3, Valid: true},
		Metric:     gbq.NullString{StringVal: "safety", Valid: true},
		Score:      gbq.NullFloat64{Float64: 1, Valid: true},
		Reference:  gbq.NullString{}, // null → ""
		RecordedAt: t0,
	}
	out := toEvidenceRow(in)
	if out.CaseID != "" {
		t.Errorf("null case_id -> %q; want empty", out.CaseID)
	}
	if out.RowIndex != 3 {
		t.Errorf("row_index = %d; want 3", out.RowIndex)
	}
	if out.Metric != "safety" || out.Score != 1 {
		t.Errorf("metric/score = %q/%v", out.Metric, out.Score)
	}
	if !out.RecordedAt.Equal(t0) {
		t.Errorf("recorded_at = %v", out.RecordedAt)
	}
}

// Compile-time assertion that EvidenceClient implements eval.Repository
// (the var _ in eval_evidence.go already does this; restate for the reader).
var _ eval.Repository = (*EvidenceClient)(nil)
