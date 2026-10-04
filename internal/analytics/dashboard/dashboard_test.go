// Package dashboard_test exercises pre-computed dashboard view rebuild.
//
// Migrated from services/chora-analytics/internal/domain/dashboard at M12.2.E.4.
package dashboard_test

import (
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/analytics/dashboard"
)

func TestNewLearnerDashboard_Validates(t *testing.T) {
	d, err := dashboard.NewLearner(dashboard.LearnerInput{
		TenantID:          "t1",
		GCID:              "01900000-0000-7000-8000-000000000001",
		AtomsCompleted:    12,
		SessionsCount:     30,
		CurrentStreakDays: 4,
	})
	if err != nil {
		t.Fatalf("NewLearner: %v", err)
	}
	if d.GCID == "" {
		t.Error("GCID empty")
	}
	if d.ComputedAt.IsZero() {
		t.Error("ComputedAt zero")
	}
}

func TestNewLearnerDashboard_RejectsBlankGCID(t *testing.T) {
	if _, err := dashboard.NewLearner(dashboard.LearnerInput{TenantID: "t1", GCID: ""}); err == nil {
		t.Error("expected error")
	}
}

func TestNewLearnerDashboard_RejectsBlankTenant(t *testing.T) {
	if _, err := dashboard.NewLearner(dashboard.LearnerInput{TenantID: "", GCID: "g"}); err == nil {
		t.Error("expected error")
	}
}

func TestNewLearnerDashboard_RejectsNegativeCounts(t *testing.T) {
	if _, err := dashboard.NewLearner(dashboard.LearnerInput{TenantID: "t1", GCID: "g", AtomsCompleted: -1}); err == nil {
		t.Error("expected error for negative AtomsCompleted")
	}
	if _, err := dashboard.NewLearner(dashboard.LearnerInput{TenantID: "t1", GCID: "g", SessionsCount: -1}); err == nil {
		t.Error("expected error for negative SessionsCount")
	}
	if _, err := dashboard.NewLearner(dashboard.LearnerInput{TenantID: "t1", GCID: "g", CurrentStreakDays: -1}); err == nil {
		t.Error("expected error for negative CurrentStreakDays")
	}
}

func TestNewInstructorDashboard_ValidatesAndComputesRate(t *testing.T) {
	d, err := dashboard.NewInstructor(dashboard.InstructorInput{
		TenantID:          "t1",
		GCID:              "01900000-0000-7000-8000-000000000001",
		CoursesOwned:      3,
		StudentsTotal:     100,
		StudentsCompleted: 25,
	})
	if err != nil {
		t.Fatalf("NewInstructor: %v", err)
	}
	want := 25.0
	if d.CompletionRatePct != want {
		t.Errorf("CompletionRatePct=%v want %v", d.CompletionRatePct, want)
	}
}

func TestNewInstructorDashboard_ZeroStudents_DefaultsToZeroRate(t *testing.T) {
	d, err := dashboard.NewInstructor(dashboard.InstructorInput{TenantID: "t1", GCID: "g", StudentsTotal: 0})
	if err != nil {
		t.Fatalf("NewInstructor: %v", err)
	}
	if d.CompletionRatePct != 0 {
		t.Errorf("rate=%v want 0", d.CompletionRatePct)
	}
}

func TestNewAdminDashboard_Validates(t *testing.T) {
	d, err := dashboard.NewAdmin(dashboard.AdminInput{
		TenantID:          "t1",
		LearnersActive:    400,
		AtomsPublished:    1200,
		SessionsCompleted: 9000,
	})
	if err != nil {
		t.Fatalf("NewAdmin: %v", err)
	}
	if d.TenantID != "t1" {
		t.Errorf("TenantID=%q", d.TenantID)
	}
	if d.LearnersActive != 400 {
		t.Errorf("LearnersActive=%d", d.LearnersActive)
	}
}

func TestNewAdminDashboard_RejectsBlankTenant(t *testing.T) {
	if _, err := dashboard.NewAdmin(dashboard.AdminInput{TenantID: ""}); err == nil {
		t.Error("expected error")
	}
}

func TestNewInstructor_RejectsBlankTenantAndGCID(t *testing.T) {
	if _, err := dashboard.NewInstructor(dashboard.InstructorInput{TenantID: ""}); err == nil {
		t.Error("expected blank tenant error")
	}
	if _, err := dashboard.NewInstructor(dashboard.InstructorInput{TenantID: "t1", GCID: ""}); err == nil {
		t.Error("expected blank gcid error")
	}
}

func TestNewInstructor_RejectsNegativeCounts(t *testing.T) {
	if _, err := dashboard.NewInstructor(dashboard.InstructorInput{TenantID: "t1", GCID: "g", CoursesOwned: -1}); err == nil {
		t.Error("expected negative CoursesOwned error")
	}
	if _, err := dashboard.NewInstructor(dashboard.InstructorInput{TenantID: "t1", GCID: "g", StudentsTotal: -1}); err == nil {
		t.Error("expected negative StudentsTotal error")
	}
	if _, err := dashboard.NewInstructor(dashboard.InstructorInput{TenantID: "t1", GCID: "g", StudentsCompleted: -1}); err == nil {
		t.Error("expected negative StudentsCompleted error")
	}
}

func TestNewAdmin_RejectsNegativeCounts(t *testing.T) {
	if _, err := dashboard.NewAdmin(dashboard.AdminInput{TenantID: "t1", LearnersActive: -1}); err == nil {
		t.Error("expected negative LearnersActive error")
	}
}

func TestComputedAt_IsRecent(t *testing.T) {
	before := time.Now().UTC()
	d, _ := dashboard.NewAdmin(dashboard.AdminInput{TenantID: "t1"})
	after := time.Now().UTC().Add(time.Second)
	if d.ComputedAt.Before(before.Add(-time.Second)) || d.ComputedAt.After(after) {
		t.Errorf("ComputedAt outside expected window: %v", d.ComputedAt)
	}
}
