// Package protodecode exercises the unexported mergeEnvelope helper: the
// nil-envelope guard + every envelope field projection.
package protodecode

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/common/v1"
)

func TestMergeEnvelope_NilGuard(t *testing.T) {
	t.Parallel()
	out := map[string]any{"existing": "keep"}
	mergeEnvelope(nil, out)
	if out["existing"] != "keep" {
		t.Errorf("nil envelope must not touch the map: %v", out)
	}
}

func TestMergeEnvelope_ProjectsAllFields(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 5, 1, 12, 30, 45, 0, time.UTC)
	env := &commonv1.EventEnvelope{
		EventId:           "evt-1",
		TenantId:          "t-1",
		Gcid:              "g-1",
		Traceparent:       "00-x-y-01",
		OccurredAt:        timestamppb.New(at),
		ChoraImdaDimension: "accountability",
		ImdaLifecycleStage: "runtime",
	}
	out := map[string]any{}
	mergeEnvelope(env, out)
	if out["event_id"] != "evt-1" || out["tenant_id"] != "t-1" || out["gcid"] != "g-1" {
		t.Errorf("identity fields: %v", out)
	}
	if out["traceparent"] != "00-x-y-01" {
		t.Errorf("traceparent: %v", out)
	}
	if out["chora_imda_dimension"] != "accountability" || out["imda_lifecycle_stage"] != "runtime" {
		t.Errorf("imda fields: %v", out)
	}
	if got, ok := out["occurred_at"].(string); !ok || got != "2026-05-01T12:30:45Z" {
		t.Errorf("occurred_at = %v", out["occurred_at"])
	}

	// empty-valued envelope leaves the keys absent
	empty := &commonv1.EventEnvelope{}
	out2 := map[string]any{}
	mergeEnvelope(empty, out2)
	if len(out2) != 0 {
		t.Errorf("empty envelope encoded fields: %v", out2)
	}
}