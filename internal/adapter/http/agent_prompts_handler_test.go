// agent_prompts_handler_test.go - integration tests for
// GET /api/v1/observability/agent-prompts (CHO-2364, ADR-197 read slice).
//
// The endpoint aggregates prompt-evidence for exactly five agent rows:
// qgen_question / qgen_critic / oe_evaluator / oe_moderator (decision
// evidence from agent_decision_log prompt_conditions) + familiar (ritual
// stamp evidence from ritual_run_audit stamps). All five rows are ALWAYS
// present; unknown latest_*/last_* keys are omitted, never fabricated.
package httpadapter_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	httpadapter "github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/http"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/inmem"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/decision"
	ra "github.com/5007-Capstone/chora/services/chora-observability/internal/domain/ritualaudit"
)

const promptsTestTenant = "01980000-0000-7000-8000-0000000000t1"

// newAgentPromptsRouter builds the router with inmem decision + ritual-audit
// repos (both implement the prompt-evidence aggregation ports).
func newAgentPromptsRouter(
	t *testing.T,
	seedDecisions func(*inmem.DecisionRepository),
	seedRituals func(*inmem.RitualAuditRepository),
) http.Handler {
	t.Helper()
	ledgers := inmem.NewLedgerRepository()
	decisions := inmem.NewDecisionRepository()
	correlations := inmem.NewCorrelationRepository()
	rituals := inmem.NewRitualAuditRepository()
	if seedDecisions != nil {
		seedDecisions(decisions)
	}
	if seedRituals != nil {
		seedRituals(rituals)
	}
	return httpadapter.NewRouter(ledgers, decisions, correlations,
		httpadapter.WithRitualAuditRepo(rituals))
}

func doAgentPromptsGET(t *testing.T, router http.Handler) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/observability/agent-prompts", nil)
	req.Header.Set("X-Tenant-Id", promptsTestTenant)
	req.Header.Set("gcid", "user-1")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	return rr
}

// decodePromptRows decodes the response into raw maps so key ABSENCE can be
// asserted (typed structs cannot distinguish omitted from zero).
func decodePromptRows(t *testing.T, rr *httptest.ResponseRecorder) []map[string]any {
	t.Helper()
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200; body=%s", rr.Code, rr.Body.String())
	}
	var body struct {
		Agents []map[string]any `json:"agents"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return body.Agents
}

// seedDecisionLog appends one decision log with an explicit CreatedAt +
// PromptConditions (decision.New stamps time.Now, so recency-ordered fixtures
// build the Log directly).
func seedDecisionLog(t *testing.T, repo *inmem.DecisionRepository, logID, agid string, at time.Time, pc map[string]string) {
	t.Helper()
	l := &decision.Log{
		LogID:            logID,
		TenantID:         promptsTestTenant,
		Agid:             agid,
		DecisionType:     decision.TypeRespond,
		Reason:           "ok",
		RiskTier:         decision.TierLow,
		CorrelationID:    "corr-" + logID,
		CreatedAt:        at,
		PromptConditions: pc,
	}
	if err := repo.Append(context.Background(), l); err != nil {
		t.Fatalf("Append %s: %v", logID, err)
	}
}

// -----------------------------------------------------------------------------
// Method + tenant gating
// -----------------------------------------------------------------------------

func TestAgentPrompts_RequiresGET(t *testing.T) {
	t.Parallel()
	router := newAgentPromptsRouter(t, nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/observability/agent-prompts", nil)
	req.Header.Set("X-Tenant-Id", promptsTestTenant)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d; want 405", rr.Code)
	}
}

func TestAgentPrompts_RequiresTenant(t *testing.T) {
	t.Parallel()
	router := newAgentPromptsRouter(t, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/observability/agent-prompts", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400 (X-Tenant-Id required)", rr.Code)
	}
}

// -----------------------------------------------------------------------------
// Empty evidence: all five rows present with zeros; nothing fabricated
// -----------------------------------------------------------------------------

func TestAgentPrompts_EmptyEvidenceAllFiveRowsPresent(t *testing.T) {
	t.Parallel()
	router := newAgentPromptsRouter(t, nil, nil)
	rows := decodePromptRows(t, doAgentPromptsGET(t, router))
	if len(rows) != 5 {
		t.Fatalf("agents = %d; want 5", len(rows))
	}
	wantOrder := []struct {
		agentID  string
		crewName string
		kind     string
	}{
		{"qgen_question", "qgen", "decisions"},
		{"qgen_critic", "qgen", "decisions"},
		{"oe_evaluator", "oe_grading", "decisions"},
		{"oe_moderator", "oe_grading", "decisions"},
		{"familiar", "familiar", "ritual_stamps"},
	}
	for i, want := range wantOrder {
		row := rows[i]
		if row["agent_id"] != want.agentID {
			t.Errorf("rows[%d].agent_id = %v; want %s", i, row["agent_id"], want.agentID)
		}
		if row["crew_name"] != want.crewName {
			t.Errorf("rows[%d].crew_name = %v; want %s", i, row["crew_name"], want.crewName)
		}
		if row["evidence_kind"] != want.kind {
			t.Errorf("rows[%d].evidence_kind = %v; want %s", i, row["evidence_kind"], want.kind)
		}
		// versions is ALWAYS a (possibly empty) array, never null/absent.
		versions, ok := row["versions"].([]any)
		if !ok {
			t.Errorf("rows[%d].versions not an array: %#v", i, row["versions"])
		} else if len(versions) != 0 {
			t.Errorf("rows[%d].versions = %d entries; want 0", i, len(versions))
		}
		// Unknown latest/last keys are OMITTED with no evidence.
		for _, k := range []string{"last_decision_at", "last_run_at", "latest_prompt_version", "latest_prompt_source"} {
			if _, has := row[k]; has {
				t.Errorf("rows[%d] must omit %q with no evidence: %#v", i, k, row[k])
			}
		}
	}
	// Decision-kind rows carry decisions_total=0 and never runs_total.
	for i := 0; i < 4; i++ {
		if got, ok := rows[i]["decisions_total"].(float64); !ok || got != 0 {
			t.Errorf("rows[%d].decisions_total = %v; want 0", i, rows[i]["decisions_total"])
		}
		if _, has := rows[i]["runs_total"]; has {
			t.Errorf("rows[%d] (decisions kind) must not carry runs_total", i)
		}
	}
	// The familiar row carries runs_total=0 and never decisions_total.
	if got, ok := rows[4]["runs_total"].(float64); !ok || got != 0 {
		t.Errorf("familiar.runs_total = %v; want 0", rows[4]["runs_total"])
	}
	if _, has := rows[4]["decisions_total"]; has {
		t.Error("familiar row must not carry decisions_total")
	}
	// qgen rows carry EXACTLY the three use-case buckets even at zero; oe +
	// familiar rows carry NO use_cases key.
	for i := 0; i < 2; i++ {
		ucs, ok := rows[i]["use_cases"].([]any)
		if !ok || len(ucs) != 3 {
			t.Fatalf("rows[%d].use_cases = %#v; want 3 buckets", i, rows[i]["use_cases"])
		}
		wantKeys := []string{"ai_assist_single", "batch", "daily_dose"}
		for j, raw := range ucs {
			uc, _ := raw.(map[string]any)
			if uc["key"] != wantKeys[j] {
				t.Errorf("rows[%d].use_cases[%d].key = %v; want %s", i, j, uc["key"], wantKeys[j])
			}
			if got, ok := uc["decisions"].(float64); !ok || got != 0 {
				t.Errorf("rows[%d].use_cases[%d].decisions = %v; want 0", i, j, uc["decisions"])
			}
			for _, k := range []string{"last_seen", "latest_prompt_version", "latest_prompt_source"} {
				if _, has := uc[k]; has {
					t.Errorf("rows[%d].use_cases[%d] must omit %q at zero evidence", i, j, k)
				}
			}
		}
	}
	for i := 2; i < 5; i++ {
		if _, has := rows[i]["use_cases"]; has {
			t.Errorf("rows[%d] must not carry use_cases", i)
		}
	}
}

// -----------------------------------------------------------------------------
// Decision-evidence aggregation (versions + latest + use-case buckets)
// -----------------------------------------------------------------------------

func TestAgentPrompts_AggregatesDecisionEvidence(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)
	t0, t1, t2, t3 := base, base.Add(1*time.Hour), base.Add(2*time.Hour), base.Add(3*time.Hour)
	seed := func(repo *inmem.DecisionRepository) {
		// r4: no conditions at all -> ai_assist_single bucket, no version.
		seedDecisionLog(t, repo, "log-0", "qgen_question", t0, nil)
		// r1: single-question AI assist, v3 via prompt hub.
		seedDecisionLog(t, repo, "log-1", "qgen_question", t1, map[string]string{
			"prompt_version": "v3", "prompt_source": "hub",
		})
		// r3: batch set generation (set_mode present, non-campaign surface).
		seedDecisionLog(t, repo, "log-2", "qgen_question", t2, map[string]string{
			"prompt_version": "v3", "prompt_source": "hub",
			"set_mode": "batch", "request_surface": "studio",
		})
		// r2: daily-dose campaign generation, v4 via grimoire (most recent).
		seedDecisionLog(t, repo, "log-3", "qgen_question", t3, map[string]string{
			"prompt_version": "v4", "prompt_source": "grimoire",
			"set_mode": "set", "request_surface": "campaign",
		})
		// Noise: an agent OUTSIDE the five-agent contract must not appear.
		seedDecisionLog(t, repo, "log-4", "familiar_companion", t3, map[string]string{
			"prompt_version": "v9",
		})
		// Noise: another tenant's row must not leak in.
		other := &decision.Log{
			LogID: "log-5", TenantID: "01980000-0000-7000-8000-0000000000t2",
			Agid: "qgen_question", DecisionType: decision.TypeRespond,
			RiskTier: decision.TierLow, CreatedAt: t3,
			PromptConditions: map[string]string{"prompt_version": "v8"},
		}
		if err := repo.Append(context.Background(), other); err != nil {
			t.Fatalf("Append other-tenant: %v", err)
		}
	}
	router := newAgentPromptsRouter(t, seed, nil)
	rows := decodePromptRows(t, doAgentPromptsGET(t, router))
	if len(rows) != 5 {
		t.Fatalf("agents = %d; want 5", len(rows))
	}
	q := rows[0]
	if q["agent_id"] != "qgen_question" {
		t.Fatalf("rows[0].agent_id = %v", q["agent_id"])
	}
	if got := q["decisions_total"].(float64); got != 4 {
		t.Errorf("decisions_total = %v; want 4", got)
	}
	if q["last_decision_at"] != t3.Format(time.RFC3339) {
		t.Errorf("last_decision_at = %v; want %s", q["last_decision_at"], t3.Format(time.RFC3339))
	}
	if q["latest_prompt_version"] != "v4" || q["latest_prompt_source"] != "grimoire" {
		t.Errorf("latest = %v/%v; want v4/grimoire", q["latest_prompt_version"], q["latest_prompt_source"])
	}
	// versions[]: v4/grimoire (1 decision, t3) + v3/hub (2 decisions, t2);
	// most-recent-first ordering; the version-less r4 row is excluded.
	versions, _ := q["versions"].([]any)
	if len(versions) != 2 {
		t.Fatalf("versions = %d; want 2: %#v", len(versions), q["versions"])
	}
	v0, _ := versions[0].(map[string]any)
	if v0["prompt_version"] != "v4" || v0["prompt_source"] != "grimoire" ||
		v0["decisions"].(float64) != 1 || v0["last_seen"] != t3.Format(time.RFC3339) {
		t.Errorf("versions[0] = %#v; want v4/grimoire/1/%s", v0, t3.Format(time.RFC3339))
	}
	v1, _ := versions[1].(map[string]any)
	if v1["prompt_version"] != "v3" || v1["prompt_source"] != "hub" ||
		v1["decisions"].(float64) != 2 || v1["last_seen"] != t2.Format(time.RFC3339) {
		t.Errorf("versions[1] = %#v; want v3/hub/2/%s", v1, t2.Format(time.RFC3339))
	}
	// Use-case buckets.
	ucs, _ := q["use_cases"].([]any)
	if len(ucs) != 3 {
		t.Fatalf("use_cases = %d; want 3", len(ucs))
	}
	assertBucket := func(idx int, key string, decisions float64, lastSeen, version, source string) {
		t.Helper()
		uc, _ := ucs[idx].(map[string]any)
		if uc["key"] != key {
			t.Errorf("use_cases[%d].key = %v; want %s", idx, uc["key"], key)
		}
		if uc["decisions"].(float64) != decisions {
			t.Errorf("%s.decisions = %v; want %v", key, uc["decisions"], decisions)
		}
		if uc["last_seen"] != lastSeen {
			t.Errorf("%s.last_seen = %v; want %v", key, uc["last_seen"], lastSeen)
		}
		if version == "" {
			if _, has := uc["latest_prompt_version"]; has {
				t.Errorf("%s must omit latest_prompt_version", key)
			}
			return
		}
		if uc["latest_prompt_version"] != version || uc["latest_prompt_source"] != source {
			t.Errorf("%s.latest = %v/%v; want %s/%s",
				key, uc["latest_prompt_version"], uc["latest_prompt_source"], version, source)
		}
	}
	assertBucket(0, "ai_assist_single", 2, t1.Format(time.RFC3339), "v3", "hub")
	assertBucket(1, "batch", 1, t2.Format(time.RFC3339), "v3", "hub")
	assertBucket(2, "daily_dose", 1, t3.Format(time.RFC3339), "v4", "grimoire")

	// qgen_critic saw no traffic: zeros, buckets present at zero.
	c := rows[1]
	if c["decisions_total"].(float64) != 0 {
		t.Errorf("qgen_critic decisions_total = %v; want 0", c["decisions_total"])
	}
	// The out-of-contract agent id must not appear anywhere in the payload.
	for _, row := range rows {
		if row["agent_id"] == "familiar_companion" {
			t.Error("familiar_companion must not appear in the five-agent contract")
		}
	}
}

func TestAgentPrompts_OEAgentsCarryNoUseCases(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 7, 21, 9, 0, 0, 0, time.UTC)
	seed := func(repo *inmem.DecisionRepository) {
		seedDecisionLog(t, repo, "log-oe-1", "oe_evaluator", at, map[string]string{
			"prompt_version": "oe-v2", "prompt_source": "hub",
		})
	}
	router := newAgentPromptsRouter(t, seed, nil)
	rows := decodePromptRows(t, doAgentPromptsGET(t, router))
	oe := rows[2]
	if oe["agent_id"] != "oe_evaluator" {
		t.Fatalf("rows[2].agent_id = %v", oe["agent_id"])
	}
	if oe["decisions_total"].(float64) != 1 {
		t.Errorf("decisions_total = %v; want 1", oe["decisions_total"])
	}
	if oe["latest_prompt_version"] != "oe-v2" || oe["latest_prompt_source"] != "hub" {
		t.Errorf("latest = %v/%v; want oe-v2/hub", oe["latest_prompt_version"], oe["latest_prompt_source"])
	}
	if _, has := oe["use_cases"]; has {
		t.Error("oe_evaluator must not carry use_cases")
	}
}

// -----------------------------------------------------------------------------
// Familiar ritual-stamp aggregation
// -----------------------------------------------------------------------------

func TestAgentPrompts_FamiliarAggregatesRitualStamps(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 7, 22, 8, 0, 0, 0, time.UTC)
	t1, t2, t3 := base, base.Add(1*time.Hour), base.Add(2*time.Hour)
	seed := func(repo *inmem.RitualAuditRepository) {
		ingest := func(n string, at time.Time, stamps []map[string]any) {
			err := repo.Ingest(context.Background(), ra.RitualRunAuditRow{
				AuditID:       "audit-" + n,
				TenantID:      promptsTestTenant,
				SourceTopic:   ra.TopicFamiliarRitualRunCompleted,
				SourceEventID: "evt-" + n,
				RunID:         "run-" + n,
				Status:        "completed",
				Stamps:        stamps,
				OccurredAt:    at,
				ReceivedAt:    at.Add(time.Minute),
			})
			if err != nil {
				t.Fatalf("Ingest %s: %v", n, err)
			}
		}
		// runA: v3 twice within one run -> counts as ONE run for v3.
		ingest("a", t1, []map[string]any{
			{"step_index": float64(0), "skill_key": "explain", "prompt_version": "v3"},
			{"step_index": float64(1), "skill_key": "quiz", "prompt_version": "v3"},
		})
		// runB: legacy CamelCase key (pre-CHO-2136 JSON wire) + v4 + one
		// empty version (excluded).
		ingest("b", t2, []map[string]any{
			{"StepIndex": float64(0), "SkillKey": "explain", "PromptVersion": "v3"},
			{"step_index": float64(1), "skill_key": "cite", "prompt_version": "v4"},
			{"step_index": float64(2), "skill_key": "bare", "prompt_version": ""},
		})
		// runC: no stamps at all; still counts toward runs_total/last_run_at.
		ingest("c", t3, nil)
		// Another tenant's run must not leak in.
		err := repo.Ingest(context.Background(), ra.RitualRunAuditRow{
			AuditID: "audit-x", TenantID: "01980000-0000-7000-8000-0000000000t2",
			SourceTopic: ra.TopicFamiliarRitualRunCompleted, SourceEventID: "evt-x",
			RunID: "run-x", Status: "completed", OccurredAt: t3, ReceivedAt: t3,
			Stamps: []map[string]any{{"prompt_version": "v9"}},
		})
		if err != nil {
			t.Fatalf("Ingest other-tenant: %v", err)
		}
	}
	router := newAgentPromptsRouter(t, nil, seed)
	rows := decodePromptRows(t, doAgentPromptsGET(t, router))
	fam := rows[4]
	if fam["agent_id"] != "familiar" || fam["evidence_kind"] != "ritual_stamps" {
		t.Fatalf("rows[4] = %#v; want familiar/ritual_stamps", fam)
	}
	if fam["runs_total"].(float64) != 3 {
		t.Errorf("runs_total = %v; want 3", fam["runs_total"])
	}
	if fam["last_run_at"] != t3.Format(time.RFC3339) {
		t.Errorf("last_run_at = %v; want %s", fam["last_run_at"], t3.Format(time.RFC3339))
	}
	versions, _ := fam["versions"].([]any)
	if len(versions) != 2 {
		t.Fatalf("versions = %d; want 2 (v3 + v4): %#v", len(versions), fam["versions"])
	}
	got := map[string]map[string]any{}
	for _, raw := range versions {
		v, _ := raw.(map[string]any)
		got[v["prompt_version"].(string)] = v
	}
	// v3 appears in runA (canonical key) + runB (legacy CamelCase key) -> 2 runs.
	if v3 := got["v3"]; v3 == nil || v3["runs"].(float64) != 2 || v3["last_seen"] != t2.Format(time.RFC3339) {
		t.Errorf("v3 = %#v; want runs=2 last_seen=%s", got["v3"], t2.Format(time.RFC3339))
	}
	if v4 := got["v4"]; v4 == nil || v4["runs"].(float64) != 1 || v4["last_seen"] != t2.Format(time.RFC3339) {
		t.Errorf("v4 = %#v; want runs=1 last_seen=%s", got["v4"], t2.Format(time.RFC3339))
	}
	// Ritual version entries never fabricate a decisions count.
	for _, raw := range versions {
		v, _ := raw.(map[string]any)
		if _, has := v["decisions"]; has {
			t.Errorf("ritual version entry must not carry decisions: %#v", v)
		}
	}
	// The empty-version stamp and the other tenant's v9 are excluded.
	if _, has := got[""]; has {
		t.Error("empty prompt_version must be excluded")
	}
	if _, has := got["v9"]; has {
		t.Error("other tenant's stamp version leaked in")
	}
}

// -----------------------------------------------------------------------------
// Unwired aggregation ports fail loud (503), never fabricate
// -----------------------------------------------------------------------------

// bareDecisionRepo wraps the inmem repo but hides the prompt-usage
// aggregation port (interface embedding promotes only decision.Repository).
type bareDecisionRepo struct{ decision.Repository }

func TestAgentPrompts_UnwiredPromptUsageReturns503(t *testing.T) {
	t.Parallel()
	ledgers := inmem.NewLedgerRepository()
	correlations := inmem.NewCorrelationRepository()
	router := httpadapter.NewRouter(ledgers,
		bareDecisionRepo{inmem.NewDecisionRepository()}, correlations,
		httpadapter.WithRitualAuditRepo(inmem.NewRitualAuditRepository()))
	rr := doAgentPromptsGET(t, router)
	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d; want 503 (prompt usage port unwired)", rr.Code)
	}
}

func TestAgentPrompts_UnwiredRitualAuditReturns503(t *testing.T) {
	t.Parallel()
	ledgers := inmem.NewLedgerRepository()
	correlations := inmem.NewCorrelationRepository()
	// No WithRitualAuditRepo: the familiar half of the contract is unservable.
	router := httpadapter.NewRouter(ledgers, inmem.NewDecisionRepository(), correlations)
	rr := doAgentPromptsGET(t, router)
	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d; want 503 (ritual stamp port unwired)", rr.Code)
	}
}
