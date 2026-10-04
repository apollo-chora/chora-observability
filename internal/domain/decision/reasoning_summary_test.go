// Package decision_test exercises ReasoningSummary — the bounded summary of
// an agent's reasoning chain.
//
// ReasoningSummary is APPENDED inside an immutable Log. It carries:
//   - input_hash + output_hash (sha256 of the full IO)
//   - latency_ms
//   - reasoning_summary text (≤ MaxReasoningLen)
//
// Hashing must be deterministic and same-input -> same-hash so callers can
// detect duplicate decisions cheaply.
package decision_test

import (
	"strings"
	"testing"

	"github.com/apollo-chora/chora-observability/internal/domain/decision"
)

func TestNewReasoningSummary_Valid(t *testing.T) {
	t.Parallel()
	rs, err := decision.NewReasoningSummary(decision.ReasoningParams{
		Input:     "user prompt: explain LeChatelier",
		Output:    "model output: applies pressure shift",
		LatencyMs: 250,
		Summary:   "router -> generator -> critic",
	})
	if err != nil {
		t.Fatalf("NewReasoningSummary: %v", err)
	}
	if rs.InputHash == "" {
		t.Errorf("InputHash empty")
	}
	if rs.OutputHash == "" {
		t.Errorf("OutputHash empty")
	}
	if len(rs.InputHash) != 64 || len(rs.OutputHash) != 64 {
		t.Errorf("hashes must be sha256 hex (64 chars); got %d / %d",
			len(rs.InputHash), len(rs.OutputHash))
	}
	if rs.LatencyMs != 250 {
		t.Errorf("latency = %d; want 250", rs.LatencyMs)
	}
}

func TestReasoningSummary_HashIsDeterministic(t *testing.T) {
	t.Parallel()
	rs1, _ := decision.NewReasoningSummary(decision.ReasoningParams{
		Input:     "same",
		Output:    "out",
		LatencyMs: 100,
		Summary:   "x",
	})
	rs2, _ := decision.NewReasoningSummary(decision.ReasoningParams{
		Input:     "same",
		Output:    "out",
		LatencyMs: 100,
		Summary:   "x",
	})
	if rs1.InputHash != rs2.InputHash {
		t.Errorf("input hash not deterministic: %s vs %s", rs1.InputHash, rs2.InputHash)
	}
	if rs1.OutputHash != rs2.OutputHash {
		t.Errorf("output hash not deterministic: %s vs %s", rs1.OutputHash, rs2.OutputHash)
	}
}

func TestReasoningSummary_DifferentInputDifferentHash(t *testing.T) {
	t.Parallel()
	rs1, _ := decision.NewReasoningSummary(decision.ReasoningParams{
		Input: "alpha", Output: "x", LatencyMs: 1, Summary: "x",
	})
	rs2, _ := decision.NewReasoningSummary(decision.ReasoningParams{
		Input: "beta", Output: "x", LatencyMs: 1, Summary: "x",
	})
	if rs1.InputHash == rs2.InputHash {
		t.Errorf("expected different hashes for different inputs")
	}
}

func TestReasoningSummary_RejectsNegativeLatency(t *testing.T) {
	t.Parallel()
	_, err := decision.NewReasoningSummary(decision.ReasoningParams{
		Input: "x", Output: "y", LatencyMs: -1, Summary: "x",
	})
	if err == nil {
		t.Errorf("expected error for negative latency")
	}
}

func TestReasoningSummary_RejectsLongSummary(t *testing.T) {
	t.Parallel()
	_, err := decision.NewReasoningSummary(decision.ReasoningParams{
		Input: "x", Output: "y", LatencyMs: 100,
		Summary: strings.Repeat("z", decision.MaxReasoningLen+1),
	})
	if err == nil {
		t.Errorf("expected error for long summary")
	}
}

func TestReasoningSummary_AcceptsEmptyIO(t *testing.T) {
	t.Parallel()
	// Empty IO is allowed (e.g., a refused decision with no IO captured)
	// but the hash is still computed deterministically over the empty string.
	rs, err := decision.NewReasoningSummary(decision.ReasoningParams{
		Input: "", Output: "", LatencyMs: 0, Summary: "",
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if rs.InputHash == "" || len(rs.InputHash) != 64 {
		t.Errorf("empty input still needs a hash; got %q", rs.InputHash)
	}
}
