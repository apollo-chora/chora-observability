// Package dashboard holds pre-computed dashboard view types for the
// analytics slice of chora-observability (consolidated at M12.2.E.4 from
// chora-analytics). Dashboards are tenant-scoped and role-conditioned
// (learner / instructor / admin) per the integrative-UI principle
// (CLAUDE.md §6).
//
// Read-only by design: a dashboard view represents the materialised result
// of a roll-up query against the eventaggregator + domain repos at a
// point in time (ComputedAt). The HTTP layer hands these out untouched.
package dashboard

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrInvalidArgument is the validation sentinel.
var ErrInvalidArgument = errors.New("invalid argument")

// LearnerDashboard is the learner-role analytics view.
type LearnerDashboard struct {
	GCID              string    `json:"gcid"`
	TenantID          string    `json:"tenant_id"`
	AtomsCompleted    int       `json:"atoms_completed"`
	SessionsCount     int       `json:"sessions_count"`
	CurrentStreakDays int       `json:"current_streak_days"`
	ComputedAt        time.Time `json:"computed_at"`
}

// LearnerInput is the constructor input.
type LearnerInput struct {
	TenantID          string
	GCID              string
	AtomsCompleted    int
	SessionsCount     int
	CurrentStreakDays int
}

// NewLearner validates + constructs the learner dashboard.
func NewLearner(in LearnerInput) (*LearnerDashboard, error) {
	if strings.TrimSpace(in.TenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if strings.TrimSpace(in.GCID) == "" {
		return nil, fmt.Errorf("%w: gcid required", ErrInvalidArgument)
	}
	if in.AtomsCompleted < 0 {
		return nil, fmt.Errorf("%w: AtomsCompleted negative", ErrInvalidArgument)
	}
	if in.SessionsCount < 0 {
		return nil, fmt.Errorf("%w: SessionsCount negative", ErrInvalidArgument)
	}
	if in.CurrentStreakDays < 0 {
		return nil, fmt.Errorf("%w: CurrentStreakDays negative", ErrInvalidArgument)
	}
	return &LearnerDashboard{
		GCID:              in.GCID,
		TenantID:          in.TenantID,
		AtomsCompleted:    in.AtomsCompleted,
		SessionsCount:     in.SessionsCount,
		CurrentStreakDays: in.CurrentStreakDays,
		ComputedAt:        time.Now().UTC(),
	}, nil
}

// InstructorDashboard is the instructor-role analytics view.
type InstructorDashboard struct {
	GCID              string    `json:"gcid"`
	TenantID          string    `json:"tenant_id"`
	CoursesOwned      int       `json:"courses_owned"`
	StudentsTotal     int       `json:"students_total"`
	CompletionRatePct float64   `json:"completion_rate_pct"`
	ComputedAt        time.Time `json:"computed_at"`
}

// InstructorInput is the constructor input.
type InstructorInput struct {
	TenantID          string
	GCID              string
	CoursesOwned      int
	StudentsTotal     int
	StudentsCompleted int
}

// NewInstructor validates + constructs the instructor dashboard. Computes
// CompletionRatePct = 100 * StudentsCompleted / StudentsTotal (or 0 when
// StudentsTotal == 0 to avoid divide-by-zero).
func NewInstructor(in InstructorInput) (*InstructorDashboard, error) {
	if strings.TrimSpace(in.TenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if strings.TrimSpace(in.GCID) == "" {
		return nil, fmt.Errorf("%w: gcid required", ErrInvalidArgument)
	}
	if in.CoursesOwned < 0 || in.StudentsTotal < 0 || in.StudentsCompleted < 0 {
		return nil, fmt.Errorf("%w: counts must be non-negative", ErrInvalidArgument)
	}
	rate := 0.0
	if in.StudentsTotal > 0 {
		rate = 100.0 * float64(in.StudentsCompleted) / float64(in.StudentsTotal)
	}
	return &InstructorDashboard{
		GCID:              in.GCID,
		TenantID:          in.TenantID,
		CoursesOwned:      in.CoursesOwned,
		StudentsTotal:     in.StudentsTotal,
		CompletionRatePct: rate,
		ComputedAt:        time.Now().UTC(),
	}, nil
}

// AdminDashboard is the tenant-wide admin analytics view.
type AdminDashboard struct {
	TenantID          string    `json:"tenant_id"`
	LearnersActive    int       `json:"learners_active"`
	AtomsPublished    int       `json:"atoms_published"`
	SessionsCompleted int       `json:"sessions_completed"`
	ComputedAt        time.Time `json:"computed_at"`
}

// AdminInput is the constructor input.
type AdminInput struct {
	TenantID          string
	LearnersActive    int
	AtomsPublished    int
	SessionsCompleted int
}

// NewAdmin validates + constructs the admin tenant dashboard.
func NewAdmin(in AdminInput) (*AdminDashboard, error) {
	if strings.TrimSpace(in.TenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if in.LearnersActive < 0 || in.AtomsPublished < 0 || in.SessionsCompleted < 0 {
		return nil, fmt.Errorf("%w: counts must be non-negative", ErrInvalidArgument)
	}
	return &AdminDashboard{
		TenantID:          in.TenantID,
		LearnersActive:    in.LearnersActive,
		AtomsPublished:    in.AtomsPublished,
		SessionsCompleted: in.SessionsCompleted,
		ComputedAt:        time.Now().UTC(),
	}, nil
}
