// agents_handler_internal_test.go — white-box unit tests for the pure
// /o/agents helpers (window + orchestrator classification + role mapping +
// percentile). package httpadapter (internal) so the unexported helpers are
// directly exercised to edge coverage.
package httpadapter

import (
	"testing"
	"time"

	"github.com/apollo-chora/chora-observability/internal/domain/agents"
)

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
		if got := projectFromEnv(); got != "chora-custom" {
			t.Errorf("projectFromEnv = %q; want chora-custom", got)
		}
	})
	t.Run("default", func(t *testing.T) {
		t.Setenv("CHORA_PROJECT", "")
		if got := projectFromEnv(); got != "chora-local" {
			t.Errorf("projectFromEnv = %q; want chora-local", got)
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
