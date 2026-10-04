// decision_repository_test.go — pgx adapter tests for the CHO-1560 +9-field
// projection on agent_decision_log. Uses a stub Querier that captures the
// Append INSERT args + serves canned rows to List/GetByID so the scan column
// order is verified without a live DB.
package pg_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/pg"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/decision"
)

// decisionStubQuerier captures Append args and serves a scripted single row to
// Query/QueryRow so List + GetByID scan paths can be exercised offline.
type decisionStubQuerier struct {
	execSQL   string
	execArgs  []any
	querySQL  string // captures the SELECT text passed to Query (e.g. List)
	queryArgs []any  // captures the bind args passed to Query
	tenantID  string
	txCalled  bool

	// row scripts the values List/GetByID will Scan, in the exact column order
	// the repository SELECTs. nil → ErrNoRows.
	row []any
}

func (s *decisionStubQuerier) Exec(_ context.Context, sql string, args ...any) error {
	s.execSQL = sql
	s.execArgs = args
	return nil
}

func (s *decisionStubQuerier) QueryRow(_ context.Context, _ string, _ ...any) pg.Row {
	if s.row == nil {
		return &scriptRow{err: pg.ErrNoRows}
	}
	return &scriptRow{vals: s.row}
}

func (s *decisionStubQuerier) Query(_ context.Context, sql string, args ...any) (pg.Rows, error) {
	s.querySQL = sql
	s.queryArgs = args
	if s.row == nil {
		return &scriptRows{}, nil
	}
	return &scriptRows{rows: [][]any{s.row}}, nil
}

func (s *decisionStubQuerier) WithTenantTx(ctx context.Context, tenantID string, fn func(context.Context, pg.TenantScopedQuerier) error) error {
	s.txCalled = true
	s.tenantID = tenantID
	return fn(ctx, s)
}

// scriptRow assigns scripted values into Scan destinations by index.
type scriptRow struct {
	vals []any
	err  error
}

func (r *scriptRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	return assignScan(dest, r.vals)
}

type scriptRows struct {
	rows [][]any
	i    int
}

func (r *scriptRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}
func (r *scriptRows) Scan(dest ...any) error { return assignScan(dest, r.rows[r.i-1]) }
func (r *scriptRows) Close()                 {}
func (r *scriptRows) Err() error             { return nil }

// assignScan copies vals[i] into the pointer dest[i]. Supports the concrete
// destination types the decision repo scans into.
func assignScan(dest, vals []any) error {
	for i := range dest {
		switch d := dest[i].(type) {
		case *string:
			*d = vals[i].(string)
		case *int:
			*d = vals[i].(int)
		case *bool:
			*d = vals[i].(bool)
		case *time.Time:
			*d = vals[i].(time.Time)
		case **float32:
			if vals[i] == nil {
				*d = nil
			} else {
				v := vals[i].(float32)
				*d = &v
			}
		case **int64:
			if vals[i] == nil {
				*d = nil
			} else {
				v := vals[i].(int64)
				*d = &v
			}
		case *int64:
			*d = vals[i].(int64)
		default:
			// Unhandled destination type — the test scripts only the types
			// the repo uses; a panic here surfaces a column/scan mismatch.
			panic("assignScan: unhandled dest type")
		}
	}
	return nil
}

func plus9Log(t *testing.T) *decision.Log {
	t.Helper()
	conf := float32(0.87)
	cost := int64(1234)
	l, err := decision.New(decision.NewParams{
		TenantID:         uuid.NewString(),
		Agid:             "qgen_critic",
		DecisionType:     decision.TypeRespond,
		Reason:           "PASS",
		RiskTier:         decision.TierLow,
		CorrelationID:    "assist-job-1",
		Traceparent:      "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		ModelID:          "vertex_ai/gemini-2.5-flash",
		Confidence:       &conf,
		CostUsdMicros:    &cost,
		CrewName:         "mcq_ai_assist",
		CrewID:           "crew-1",
		PromptTokens:     100,
		CompletionTokens: 50,
		CachedTokens:     10,
		GuardrailOutcome: "pass",
		QuestionType:     "mcq",
		Verdict:          "accepted",
		PromptConditions: map[string]string{"intent": "new_question"},
	})
	if err != nil {
		t.Fatalf("decision.New: %v", err)
	}
	return l
}

// TestDecisionRepository_Append_WritesPlus9Columns proves the Append INSERT
// carries the +9 projection values (model_id / confidence / autonomy_level /
// cost_usd_micros / crew_* / token counts / guardrail_outcome) as bind args,
// inside WithTenantTx (RLS GUC).
func TestDecisionRepository_Append_WritesPlus9Columns(t *testing.T) {
	t.Parallel()
	q := &decisionStubQuerier{}
	repo := pg.NewDecisionRepository(q)
	l := plus9Log(t)

	if err := repo.Append(context.Background(), l); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if !q.txCalled {
		t.Fatal("Append must run inside WithTenantTx (RLS GUC)")
	}
	if !containsSub(q.execSQL, "model_id") || !containsSub(q.execSQL, "cost_usd_micros") ||
		!containsSub(q.execSQL, "confidence") || !containsSub(q.execSQL, "crew_name") ||
		!containsSub(q.execSQL, "guardrail_verdict") || !containsSub(q.execSQL, "prompt_tokens") ||
		!containsSub(q.execSQL, "question_type") || !containsSub(q.execSQL, "verdict") ||
		!containsSub(q.execSQL, "prompt_conditions") {
		t.Errorf("INSERT SQL missing +9/question_type/verdict/prompt_conditions projection columns:\n%s", q.execSQL)
	}
	// Every Append arg should be present; spot-check the projection values are
	// somewhere in the bind args.
	if !argsContain(q.execArgs, "vertex_ai/gemini-2.5-flash") {
		t.Errorf("model_id not in bind args: %v", q.execArgs)
	}
	if !argsContain(q.execArgs, "mcq_ai_assist") {
		t.Errorf("crew_name not in bind args: %v", q.execArgs)
	}
	if !argsContain(q.execArgs, "pass") {
		t.Errorf("guardrail_outcome not in bind args: %v", q.execArgs)
	}
	if !argsContain(q.execArgs, "mcq") {
		t.Errorf("question_type not in bind args: %v", q.execArgs)
	}
	if !argsContain(q.execArgs, "accepted") {
		t.Errorf("verdict not in bind args: %v", q.execArgs)
	}
	// prompt_conditions is marshalled to a JSON object string before binding.
	if !argsContain(q.execArgs, `{"intent":"new_question"}`) {
		t.Errorf("prompt_conditions JSON not in bind args: %v", q.execArgs)
	}
}

// TestDecisionRepository_List_ScansPlus9 proves List reads the new columns back
// and populates the serialization-facing ConfidenceOut + CostUSD (float USD).
func TestDecisionRepository_List_ScansPlus9(t *testing.T) {
	t.Parallel()
	conf := float32(0.87)
	cost := int64(1234)
	now := time.Date(2026, 6, 2, 10, 0, 0, 0, time.UTC)
	q := &decisionStubQuerier{
		// Column order MUST match the repo's SELECT list (see List query).
		row: []any{
			"log-1", uuid.NewString(), "qgen_critic", // log_id, tenant_id, agent_id
			"respond", "PASS", "low", "assist-job-1", // decision_type, reasoning_summary, risk_tier, correlation_id
			"", "", 0, // input_hash, output_hash, latency_ms
			"00-trace-01", now, // traceparent, recorded_at
			"vertex_ai/gemini-2.5-flash", // model_id
			conf,                         // confidence (float32 → *float32 via assignScan)
			cost,                         // cost_usd_micros (int64 → *int64 via assignScan)
			"mcq_ai_assist", "crew-1",    // crew_name, crew_id
			int64(100), int64(50), int64(10), // prompt/completion/cached tokens
			"pass", // guardrail_verdict
			"",     // autonomy_level (text)
			"mcq",  // question_type
			"accepted", // verdict
			`{"intent":"new_question"}`, // prompt_conditions (jsonb::text)
		},
	}
	repo := pg.NewDecisionRepository(q)
	logs, err := repo.List(context.Background(), uuid.NewString(), decision.ListFilter{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("logs = %d; want 1", len(logs))
	}
	got := logs[0]
	if got.ModelID != "vertex_ai/gemini-2.5-flash" {
		t.Errorf("ModelID = %q", got.ModelID)
	}
	// float32(0.87) widened to float64 carries a tiny representation epsilon —
	// compare with tolerance rather than exact equality.
	if got.ConfidenceOut == nil || *got.ConfidenceOut < 0.8699 || *got.ConfidenceOut > 0.8701 {
		t.Errorf("ConfidenceOut = %v; want ~0.87 (populated from confidence)", got.ConfidenceOut)
	}
	if got.CostUSD == nil || *got.CostUSD != 0.001234 {
		t.Errorf("CostUSD = %v; want 0.001234 (micros/1e6)", got.CostUSD)
	}
	if got.CrewName != "mcq_ai_assist" || got.CrewID != "crew-1" {
		t.Errorf("crew = %q/%q", got.CrewName, got.CrewID)
	}
	if got.GuardrailOutcome != "pass" {
		t.Errorf("GuardrailOutcome = %q", got.GuardrailOutcome)
	}
	if got.PromptTokens != 100 || got.CompletionTokens != 50 || got.CachedTokens != 10 {
		t.Errorf("tokens = %d/%d/%d", got.PromptTokens, got.CompletionTokens, got.CachedTokens)
	}
	if got.QuestionType != "mcq" {
		t.Errorf("QuestionType = %q; want mcq", got.QuestionType)
	}
	if got.Verdict != "accepted" {
		t.Errorf("Verdict = %q; want accepted", got.Verdict)
	}
	if got.PromptConditions["intent"] != "new_question" {
		t.Errorf("PromptConditions = %v; want intent=new_question", got.PromptConditions)
	}
}

// TestDecisionRepository_GetByID_ScansQuestionType proves the GetByID scan
// path reads the question_type column back (same column order as List) into
// the domain Log.
func TestDecisionRepository_GetByID_ScansQuestionType(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 6, 8, 10, 0, 0, 0, time.UTC)
	q := &decisionStubQuerier{
		row: []any{
			"log-1", uuid.NewString(), "qgen_question", // log_id, tenant_id, agent_id
			"respond", "PASS", "low", "assist-job-1", // decision_type, reasoning_summary, risk_tier, correlation_id
			"", "", 0, // input_hash, output_hash, latency_ms
			"00-trace-01", now, // traceparent, recorded_at
			"vertex_ai/gemini-2.5-flash", // model_id
			nil,                          // confidence (NULL)
			nil,                          // cost_usd_micros (NULL)
			"", "",                       // crew_name, crew_id
			int64(0), int64(0), int64(0), // tokens
			"",    // guardrail_verdict
			"",    // autonomy_level
			"mcq", // question_type
			"completed_with_warning", // verdict
			"",                       // prompt_conditions (NULL → empty string → empty map)
		},
	}
	repo := pg.NewDecisionRepository(q)
	got, err := repo.GetByID(context.Background(), uuid.NewString(), "log-1")
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.QuestionType != "mcq" {
		t.Errorf("QuestionType = %q; want mcq", got.QuestionType)
	}
	if got.Verdict != "completed_with_warning" {
		t.Errorf("Verdict = %q; want completed_with_warning", got.Verdict)
	}
	// Empty/NULL prompt_conditions must round-trip to an empty map, never a
	// fabricated value.
	if len(got.PromptConditions) != 0 {
		t.Errorf("PromptConditions = %v; want empty (NULL column)", got.PromptConditions)
	}
}

// TestDecisionRepository_List_OrderDirection proves List emits ORDER BY
// recorded_at ASC by default (the /o/agents collectStats contract) and DESC
// when ListFilter.Descending is set (the O+ Decision Traces newest-first audit
// list). The stub captures the SELECT text — the ORDER direction is the SQL the
// real DB executes.
func TestDecisionRepository_List_OrderDirection(t *testing.T) {
	t.Parallel()

	// Default → ASC.
	qAsc := &decisionStubQuerier{}
	repoAsc := pg.NewDecisionRepository(qAsc)
	if _, err := repoAsc.List(context.Background(), uuid.NewString(), decision.ListFilter{}); err != nil {
		t.Fatalf("List ASC: %v", err)
	}
	if !containsSub(qAsc.querySQL, "ORDER BY recorded_at ASC") {
		t.Errorf("default List SQL not ASC:\n%s", qAsc.querySQL)
	}
	if containsSub(qAsc.querySQL, "ORDER BY recorded_at DESC") {
		t.Errorf("default List SQL must not be DESC:\n%s", qAsc.querySQL)
	}

	// Descending → DESC.
	qDesc := &decisionStubQuerier{}
	repoDesc := pg.NewDecisionRepository(qDesc)
	if _, err := repoDesc.List(context.Background(), uuid.NewString(), decision.ListFilter{Descending: true}); err != nil {
		t.Fatalf("List DESC: %v", err)
	}
	if !containsSub(qDesc.querySQL, "ORDER BY recorded_at DESC") {
		t.Errorf("descending List SQL not DESC:\n%s", qDesc.querySQL)
	}
}

func containsSub(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func argsContain(args []any, want string) bool {
	for _, a := range args {
		if v, ok := a.(string); ok && v == want {
			return true
		}
	}
	return false
}
