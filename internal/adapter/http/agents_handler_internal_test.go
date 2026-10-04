// agents_handler_internal_test.go — white-box unit tests for the pure
// /o/agents helpers (Cloud Trace deep-link construction + traceparent parsing
// + window + orchestrator classification). package httpadapter (internal) so
// the unexported helpers are directly exercised to edge coverage.
package httpadapter

import (
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-observability/internal/domain/agents"
)

func TestTraceIDFromTraceparent(t *testing.T) {
	t.Parallel()
	tid32 := strings.Repeat("a", 32)
	cases := []struct {
		name string
		tp   string
		want string
	}{
		{"valid", "00-" + tid32 + "-" + strings.Repeat("b", 16) + "-01", tid32},
		{"valid uppercase hex", "00-" + strings.Repeat("A", 32) + "-x-01", strings.Repeat("A", 32)},
		{"empty", "", ""},
		{"too few parts", "00-" + tid32, ""},
		{"wrong length", "00-abc-def-01", ""},
		{"non-hex", "00-" + strings.Repeat("g", 32) + "-b-01", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := traceIDFromTraceparent(c.tp); got != c.want {
				t.Errorf("traceIDFromTraceparent(%q) = %q; want %q", c.tp, got, c.want)
			}
		})
	}
}

func TestSpanIDFromTraceparent(t *testing.T) {
	t.Parallel()
	sid16 := strings.Repeat("b", 16)
	cases := []struct {
		name string
		tp   string
		want string
	}{
		{"valid", "00-" + strings.Repeat("a", 32) + "-" + sid16 + "-01", sid16},
		{"empty", "", ""},
		{"too few parts", "00-" + strings.Repeat("a", 32), ""},
		{"wrong length", "00-" + strings.Repeat("a", 32) + "-abc-01", ""},
		{"non-hex", "00-" + strings.Repeat("a", 32) + "-" + strings.Repeat("z", 16) + "-01", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := spanIDFromTraceparent(c.tp); got != c.want {
				t.Errorf("spanIDFromTraceparent(%q) = %q; want %q", c.tp, got, c.want)
			}
		})
	}
}

func TestCloudTraceTraceURL(t *testing.T) {
	t.Parallel()
	// With a span id → pins ;traceId= AND ;spanId= (lands on the agent's span).
	got := cloudTraceTraceURL("chora-489812", "abc123", "def456")
	if !strings.Contains(got, "traces/explorer;traceId=abc123;spanId=def456") || !strings.Contains(got, "project=chora-489812") {
		t.Errorf("unexpected url: %q", got)
	}
	// Without a span id → ;traceId= only (no ;spanId=).
	noSpan := cloudTraceTraceURL("chora-489812", "abc123", "")
	if !strings.Contains(noSpan, "traces/explorer;traceId=abc123") || strings.Contains(noSpan, "spanId=") {
		t.Errorf("no-span url should be ;traceId= only: %q", noSpan)
	}
	// The legacy /traces/list?tid= format is DEAD (the new Explorer drops it).
	if strings.Contains(got, "traces/list") || strings.Contains(got, "tid=") {
		t.Errorf("must use the new ;traceId= matrix param, not legacy /traces/list?tid=: %q", got)
	}
	if cloudTraceTraceURL("", "abc", "s") != "" {
		t.Error("empty project should yield empty url")
	}
	if cloudTraceTraceURL("p", "", "s") != "" {
		t.Error("empty traceID should yield empty url")
	}
}

func TestCloudTraceExplorerURL(t *testing.T) {
	t.Parallel()
	got := cloudTraceExplorerURL("chora-489812")
	// The no-recent-trace fallback opens the new Trace Explorer scoped to the project.
	if !strings.Contains(got, "console.cloud.google.com/traces/explorer") || !strings.Contains(got, "project=chora-489812") {
		t.Errorf("unexpected url: %q", got)
	}
	// The dead legacy filter (/traces/list?filter=chora.agent_id — ignored by
	// the new Explorer; agent spans carry no chora.agent_id) must not reappear.
	if strings.Contains(got, "traces/list") || strings.Contains(got, "chora.agent_id") || strings.Contains(got, "service.name") {
		t.Errorf("fallback must not use the dead legacy filter: %q", got)
	}
	if cloudTraceExplorerURL("") != "" {
		t.Error("empty project should yield empty url")
	}
}

func TestAgentsWindow(t *testing.T) {
	t.Run("default 90d", func(t *testing.T) {
		t.Setenv("CHORA_AGENTS_WINDOW_DAYS", "")
		if got := agentsWindow(); got != 90*24*time.Hour {
			t.Errorf("default window = %v; want 90d", got)
		}
	})
	t.Run("env override", func(t *testing.T) {
		t.Setenv("CHORA_AGENTS_WINDOW_DAYS", "30")
		if got := agentsWindow(); got != 30*24*time.Hour {
			t.Errorf("override window = %v; want 30d", got)
		}
	})
	t.Run("invalid env falls back", func(t *testing.T) {
		t.Setenv("CHORA_AGENTS_WINDOW_DAYS", "not-a-number")
		if got := agentsWindow(); got != 90*24*time.Hour {
			t.Errorf("invalid env window = %v; want default 90d", got)
		}
	})
	t.Run("zero env falls back", func(t *testing.T) {
		t.Setenv("CHORA_AGENTS_WINDOW_DAYS", "0")
		if got := agentsWindow(); got != 90*24*time.Hour {
			t.Errorf("zero env window = %v; want default 90d", got)
		}
	})
}

func TestIsOrchestrator(t *testing.T) {
	t.Parallel()
	if !isOrchestrator(agents.Entry{Framework: "langgraph"}) {
		t.Error("langgraph entry should be an orchestrator")
	}
	if !isOrchestrator(agents.Entry{Framework: "LangGraph"}) {
		t.Error("framework match should be case-insensitive")
	}
	if isOrchestrator(agents.Entry{Framework: "adk"}) {
		t.Error("adk entry is an LLM agent, not an orchestrator")
	}
	if isOrchestrator(agents.Entry{}) {
		t.Error("empty framework is not an orchestrator")
	}
}

func TestAgentRoleFromEntry(t *testing.T) {
	t.Parallel()
	known := map[string]string{
		"qgen_question":          "MCQ generator",
		"qgen_critic":            "Quality critic",
		"familiar_companion":     "Familiar RPG companion",
		"content_recommender":    "Daily-dose recommender",
		"moderator":              "Content moderator",
		"critic":                 "Moderation critic",
		"fog_orchestrator":       "Per-user KG fog orchestrator",
		"ai_kernel_orchestrator": "AI Kernel LangGraph orchestrator",
		"oe_grader":              "OE answer grader",
		"oe_evaluator":           "OE answer evaluator",
		"oe_moderator":           "OE grading moderator",
		"grading_orchestrator":   "Grading saga orchestrator",
	}
	for id, want := range known {
		e := agents.Entry{Domain: "unused"}
		if got := agentRoleFromEntry(id, e); got != want {
			t.Errorf("agentRoleFromEntry(%q) = %q; want %q", id, got, want)
		}
	}
	// Unknown id -> falls back to the registry domain.
	if got := agentRoleFromEntry("new_agent", agents.Entry{Domain: "obs"}); got != "obs" {
		t.Errorf("unknown id fallback = %q; want registry domain", got)
	}
}

func TestProjectFromEnv(t *testing.T) {
	t.Run("CHORA_PROJECT wins", func(t *testing.T) {
		t.Setenv("CHORA_PROJECT", "chora-custom")
		t.Setenv("GOOGLE_CLOUD_PROJECT", "gcp-other")
		if got := projectFromEnv(); got != "chora-custom" {
			t.Errorf("projectFromEnv = %q; want chora-custom", got)
		}
	})
	t.Run("GOOGLE_CLOUD_PROJECT fallback", func(t *testing.T) {
		t.Setenv("CHORA_PROJECT", "")
		t.Setenv("GOOGLE_CLOUD_PROJECT", "gcp-project-7")
		if got := projectFromEnv(); got != "gcp-project-7" {
			t.Errorf("projectFromEnv = %q; want gcp-project-7", got)
		}
	})
	t.Run("default", func(t *testing.T) {
		t.Setenv("CHORA_PROJECT", "")
		t.Setenv("GOOGLE_CLOUD_PROJECT", "")
		if got := projectFromEnv(); got != "chora-489812" {
			t.Errorf("projectFromEnv = %q; want chora-489812", got)
		}
	})
}

func TestPercentile(t *testing.T) {
	t.Parallel()
	if got := percentile(nil, 95); got != 0 {
		t.Errorf("percentile(empty) = %d; want 0", got)
	}
	if got := percentile([]int{5}, 95); got != 5 {
		t.Errorf("percentile(single) = %d; want 5", got)
	}
	// p=100 clamps the nearest-rank index to the last element.
	if got := percentile([]int{1, 2, 3}, 100); got != 3 {
		t.Errorf("percentile(p=100) = %d; want 3", got)
	}
	// p=50 picks the second element of a 4-element slice (rank = 50*4/100 = 2).
	if got := percentile([]int{10, 20, 30, 40}, 50); got != 30 {
		t.Errorf("percentile(p=50) = %d; want 30", got)
	}
	// Input is copied, not sorted in place.
	in := []int{40, 10, 30, 20}
	_ = percentile(in, 50)
	if in[0] != 40 {
		t.Errorf("percentile mutated its input: %v", in)
	}
}

func TestCloudTraceURLHelpers(t *testing.T) {
	t.Parallel()
	if got := cloudTraceTraceURL("", "tid", ""); got != "" {
		t.Errorf("empty project URL = %q; want empty", got)
	}
	if got := cloudTraceTraceURL("proj", "", ""); got != "" {
		t.Errorf("empty trace URL = %q; want empty", got)
	}
	if !strings.Contains(cloudTraceTraceURL("proj", "0000000000000000000000000000000a", "0000000000000001"), "spanId=0000000000000001") {
		t.Error("span-pinned URL missing spanId param")
	}
	if !strings.Contains(cloudTraceTraceURL("proj", "0000000000000000000000000000000a", ""), "traceId=0000000000000000000000000000000a") {
		t.Error("trace URL missing traceId param")
	}
	if got := cloudTraceExplorerURL(""); got != "" {
		t.Errorf("empty explorer URL = %q; want empty", got)
	}
	if !strings.Contains(cloudTraceExplorerURL("proj"), "traces/explorer?project=proj") {
		t.Errorf("explorer URL = %q", cloudTraceExplorerURL("proj"))
	}
}
