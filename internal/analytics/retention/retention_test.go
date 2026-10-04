// Package retention_test exercises retention curve computation. Two flavours:
//
//   - Cohort-relative retention: % of cohort members who returned with a
//     learner-active event on day-N after their join_at timestamp.
//   - Ebbinghaus forgetting curve: theoretical recall = exp(-t/S) where
//     t is days since last review and S is item stability. Used as a
//     primitive for spaced-repetition scheduling.
//
// Migrated from services/chora-analytics/internal/domain/retention at M12.2.E.4.
package retention_test

import (
	"math"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/analytics/retention"
)

func TestEbbinghausRecall_AtZero_IsOne(t *testing.T) {
	got := retention.EbbinghausRecall(0, 1.0)
	if math.Abs(got-1.0) > 1e-9 {
		t.Errorf("R(0)=%v want 1.0", got)
	}
}

func TestEbbinghausRecall_DecaysOverTime(t *testing.T) {
	r1 := retention.EbbinghausRecall(1, 1.0)
	r2 := retention.EbbinghausRecall(5, 1.0)
	if !(r1 > r2) {
		t.Errorf("expected r1=%v > r2=%v", r1, r2)
	}
	if r1 <= 0 || r1 >= 1 {
		t.Errorf("r1 out of (0,1): %v", r1)
	}
}

func TestEbbinghausRecall_RejectsNonPositiveStability(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for non-positive stability")
		}
	}()
	retention.EbbinghausRecall(1, 0)
}

func TestComputeCohortCurve_HappyPath(t *testing.T) {
	// 4-member cohort joined day 0; activity events for day 1, day 2, day 7, day 30.
	day0 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)

	members := []retention.Member{
		{GCID: "g1", JoinedAt: day0},
		{GCID: "g2", JoinedAt: day0},
		{GCID: "g3", JoinedAt: day0},
		{GCID: "g4", JoinedAt: day0},
	}
	activity := []retention.Activity{
		{GCID: "g1", At: day0.Add(1 * 24 * time.Hour)},
		{GCID: "g2", At: day0.Add(1 * 24 * time.Hour)},
		{GCID: "g3", At: day0.Add(1 * 24 * time.Hour)},
		{GCID: "g4", At: day0.Add(1 * 24 * time.Hour)},

		{GCID: "g1", At: day0.Add(7 * 24 * time.Hour)},
		{GCID: "g2", At: day0.Add(7 * 24 * time.Hour)},

		{GCID: "g1", At: day0.Add(30 * 24 * time.Hour)},
	}
	curve, err := retention.ComputeCohortCurve(members, activity, []int{1, 7, 30})
	if err != nil {
		t.Fatalf("ComputeCohortCurve: %v", err)
	}
	if curve.TotalSize != 4 {
		t.Errorf("TotalSize=%d want 4", curve.TotalSize)
	}
	if len(curve.Points) != 3 {
		t.Fatalf("Points len=%d want 3", len(curve.Points))
	}

	// Day 1: all 4 retained = 100%
	if curve.Points[0].DayOffset != 1 || curve.Points[0].RetainedCount != 4 || curve.Points[0].RetentionPct != 100.0 {
		t.Errorf("day1=%+v", curve.Points[0])
	}
	// Day 7: 2 retained = 50%
	if curve.Points[1].DayOffset != 7 || curve.Points[1].RetainedCount != 2 || curve.Points[1].RetentionPct != 50.0 {
		t.Errorf("day7=%+v", curve.Points[1])
	}
	// Day 30: 1 retained = 25%
	if curve.Points[2].DayOffset != 30 || curve.Points[2].RetainedCount != 1 || curve.Points[2].RetentionPct != 25.0 {
		t.Errorf("day30=%+v", curve.Points[2])
	}
}

func TestComputeCohortCurve_EmptyCohort_ReturnsError(t *testing.T) {
	_, err := retention.ComputeCohortCurve(nil, nil, []int{1})
	if err == nil {
		t.Error("expected error for empty cohort")
	}
}

func TestComputeCohortCurve_NoOffsets_ReturnsError(t *testing.T) {
	members := []retention.Member{{GCID: "g", JoinedAt: time.Now()}}
	_, err := retention.ComputeCohortCurve(members, nil, nil)
	if err == nil {
		t.Error("expected error for no offsets")
	}
}

func TestComputeCohortCurve_RejectsNonPositiveOffset(t *testing.T) {
	members := []retention.Member{{GCID: "g", JoinedAt: time.Now()}}
	_, err := retention.ComputeCohortCurve(members, nil, []int{0})
	if err == nil {
		t.Error("expected error for offset 0")
	}
	_, err = retention.ComputeCohortCurve(members, nil, []int{-1})
	if err == nil {
		t.Error("expected error for negative offset")
	}
}

func TestComputeCohortCurve_DedupsActivityPerGCIDPerOffset(t *testing.T) {
	day0 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	members := []retention.Member{{GCID: "g1", JoinedAt: day0}}
	// Same gcid, two events same day window → still counts as 1
	activity := []retention.Activity{
		{GCID: "g1", At: day0.Add(1 * 24 * time.Hour)},
		{GCID: "g1", At: day0.Add(1*24*time.Hour + 12*time.Hour)},
	}
	curve, err := retention.ComputeCohortCurve(members, activity, []int{1})
	if err != nil {
		t.Fatal(err)
	}
	if curve.Points[0].RetainedCount != 1 {
		t.Errorf("dup activity inflated count: %d", curve.Points[0].RetainedCount)
	}
}

func TestComputeCohortCurve_IgnoresActivityBeforeJoin(t *testing.T) {
	day0 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	members := []retention.Member{{GCID: "g1", JoinedAt: day0}}
	activity := []retention.Activity{
		{GCID: "g1", At: day0.Add(-24 * time.Hour)}, // before join
	}
	curve, err := retention.ComputeCohortCurve(members, activity, []int{1})
	if err != nil {
		t.Fatal(err)
	}
	if curve.Points[0].RetainedCount != 0 {
		t.Errorf("pre-join activity counted: %d", curve.Points[0].RetainedCount)
	}
}
