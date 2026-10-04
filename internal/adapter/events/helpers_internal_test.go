// Package events exercises the unexported projection helpers that the
// external test suite reaches only via full event flows: the payload field
// accessors (strField / boolField / floatField / int32Field / int64Field)
// with their wire-format tolerance branches, the ritual-run status mapping,
// the SubscribedTopic accessor, and the PII closure map bootstrap loader.
package events

import (
	"context"
	"testing"
	"time"

	consumptionv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/consumption/v1"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/config"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/decision"
)

func TestFieldAccessors_TolerateWireShapes(t *testing.T) {
	t.Parallel()
	m := map[string]any{
		"s":   "str",
		"b":   true,
		"f64": float64(1.5),
		"f32": float32(2.5),
		"i64": int64(7),
		"i32": int32(8),
		"i":   int(9),
	}

	if got := strField(m, "s"); got != "str" {
		t.Errorf("strField = %q", got)
	}
	if got := strField(m, "missing"); got != "" {
		t.Errorf("strField missing = %q", got)
	}
	if got := strField(m, "i64"); got != "" {
		t.Errorf("strField wrong type = %q; want empty", got)
	}

	if got := boolField(m, "b"); !got {
		t.Error("boolField(true) = false")
	}
	if got := boolField(m, "f64"); got {
		t.Error("boolField wrong type = true; want false")
	}

	if got := floatField(m, "f64"); got != 1.5 {
		t.Errorf("floatField f64 = %v", got)
	}
	if got := floatField(m, "f32"); got != 2.5 {
		t.Errorf("floatField f32 = %v", got)
	}
	if got := floatField(m, "i"); got != 0 {
		t.Errorf("floatField int = %v; want 0", got)
	}

	if got := int32Field(m, "i64"); got != 7 {
		t.Errorf("int32Field i64 = %d", got)
	}
	if got := int32Field(m, "i32"); got != 8 {
		t.Errorf("int32Field i32 = %d", got)
	}
	if got := int32Field(m, "i"); got != 9 {
		t.Errorf("int32Field int = %d", got)
	}
	if got := int32Field(m, "f64"); got != 1 {
		t.Errorf("int32Field f64 = %d", got)
	}
	if got := int32Field(m, "missing"); got != 0 {
		t.Errorf("int32Field missing = %d; want 0", got)
	}

	if got := int64Field(m, "i64"); got != 7 {
		t.Errorf("int64Field i64 = %d", got)
	}
	if got := int64Field(m, "i32"); got != 8 {
		t.Errorf("int64Field i32 = %d", got)
	}
	if got := int64Field(m, "i"); got != 9 {
		t.Errorf("int64Field int = %d", got)
	}
	if got := int64Field(m, "f64"); got != 1 {
		t.Errorf("int64Field f64 = %d", got)
	}
	if got := int64Field(m, "b"); got != 0 {
		t.Errorf("int64Field bool = %d; want 0", got)
	}
}

func TestRitualRunStatusString(t *testing.T) {
	t.Parallel()
	cases := map[consumptionv1.RitualRunStatus]string{
		consumptionv1.RitualRunStatus_RITUAL_RUN_STATUS_RUNNING:        "running",
		consumptionv1.RitualRunStatus_RITUAL_RUN_STATUS_COMPLETED:      "completed",
		consumptionv1.RitualRunStatus_RITUAL_RUN_STATUS_FAILED:         "failed",
		consumptionv1.RitualRunStatus_RITUAL_RUN_STATUS_SKIPPED_BUDGET: "skipped_budget",
		consumptionv1.RitualRunStatus_RITUAL_RUN_STATUS_BLOCKED:        "blocked",
		consumptionv1.RitualRunStatus_RITUAL_RUN_STATUS_UNSPECIFIED:    "",
	}
	for in, want := range cases {
		if got := ritualRunStatusString(in); got != want {
			t.Errorf("ritualRunStatusString(%v) = %q; want %q", in, got, want)
		}
	}
}

func TestAgentDecisionConsumer_SubscribedTopic(t *testing.T) {
	t.Parallel()
	c := NewAgentDecisionConsumer(AgentDecisionConsumerConfig{Repo: &noopDecisionRepo{}})
	if got := c.SubscribedTopic(); got != TopicAgentDecisionLogged {
		t.Errorf("SubscribedTopic = %q; want %q", got, TopicAgentDecisionLogged)
	}

	// nil repo still panics (fail-loud constructor).
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for nil repo")
		}
	}()
	_ = NewAgentDecisionConsumer(AgentDecisionConsumerConfig{})
}

func TestBootstrapClosureSubscriber_LoadsManifest(t *testing.T) {
	t.Parallel()
	// The canonical per-domain manifest (same file config tests load).
	sub, err := BootstrapClosureSubscriber("../../../config/PII_Closure_Map.yaml",
		&noopClosureRepo{}, &noopClosurePub{}, nil)
	if err != nil {
		t.Fatalf("BootstrapClosureSubscriber: %v", err)
	}
	if sub == nil {
		t.Fatal("expected a wired subscriber")
	}
	if sub.pii == nil {
		t.Fatal("expected loaded PII map")
	}
	if _, err := BootstrapClosureSubscriber("does-not-exist.yaml", &noopClosureRepo{}, &noopClosurePub{}, nil); err == nil {
		t.Fatal("expected error for missing map path")
	}
}

// noopDecisionRepo satisfies decision.Repository for constructor tests.
type noopDecisionRepo struct{}

func (n *noopDecisionRepo) Append(_ context.Context, _ *decision.Log) error { return nil }
func (n *noopDecisionRepo) List(_ context.Context, _ string, _ decision.ListFilter) ([]*decision.Log, error) {
	return nil, nil
}
func (n *noopDecisionRepo) GetByID(_ context.Context, _, _ string) (*decision.Log, error) {
	return nil, decision.ErrNotFound
}
func (n *noopDecisionRepo) Count(_ context.Context, _ string, _, _ time.Time) (int64, error) {
	return 0, nil
}

type noopClosureRepo struct{}

func (n *noopClosureRepo) Pseudonymise(_ context.Context, _, _ string, _ []config.TableSpec) (int, error) {
	return 0, nil
}
func (n *noopClosureRepo) IsPseudonymised(_ context.Context, _, _ string) (bool, error) {
	return false, nil
}

type noopClosurePub struct{}

func (n *noopClosurePub) Publish(_ string, _, _, _ string, _ map[string]interface{}) error { return nil }