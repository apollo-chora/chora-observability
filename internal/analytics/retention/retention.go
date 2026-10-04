// Package retention computes retention curves for cohorts and theoretical
// recall via the Ebbinghaus forgetting curve.
//
// Migrated from services/chora-analytics/internal/domain/retention at
// M12.2.E.4 — consolidated under the chora-observability domain.
//
// Two distinct flavours are exposed:
//
//  1. Cohort-relative retention: a curve of (day_offset, retained_count,
//     retention_pct) showing what fraction of a cohort's members had a
//     learner-active event within the day_offset day window after their
//     individual JoinedAt timestamp.
//
//  2. Ebbinghaus forgetting curve: theoretical R(t) = exp(-t / S) for time
//     t (days) since last review and item stability S (days). Used as a
//     primitive for spaced-repetition scheduling per CLAUDE.md §1
//     ("Ebbinghaus Forgetting Curve as primitive").
package retention

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"
)

// EbbinghausRecall returns the theoretical recall R(t, S) = exp(-t / S).
//
// stabilityDays MUST be positive — non-positive stability is undefined.
// We panic rather than return an error because callers always know the
// stability up front (typically picked from a per-atom calibration).
func EbbinghausRecall(daysSinceReview, stabilityDays float64) float64 {
	if stabilityDays <= 0 {
		panic(fmt.Sprintf("retention: stabilityDays must be > 0, got %v", stabilityDays))
	}
	return math.Exp(-daysSinceReview / stabilityDays)
}

// Member is a cohort participant with their individual join timestamp.
type Member struct {
	GCID     string
	JoinedAt time.Time
}

// Activity is a learner-active event for retention computation.
type Activity struct {
	GCID string
	At   time.Time
}

// RetentionPoint is one (day_offset, retained_count, retention_pct) point.
type RetentionPoint struct {
	DayOffset     int     `json:"day_offset"`
	RetainedCount int     `json:"retained_count"`
	RetentionPct  float64 `json:"retention_pct"`
}

// Curve is the full retention curve.
type Curve struct {
	TotalSize int              `json:"total_size"`
	Points    []RetentionPoint `json:"points"`
}

// ErrInvalidArgument is the validation sentinel.
var ErrInvalidArgument = errors.New("invalid argument")

// ComputeCohortCurve computes the cohort-relative retention curve.
//
// Algorithm:
//   - For each member m, find activity events with GCID=m.GCID where
//     m.JoinedAt < a.At ≤ m.JoinedAt + (offset+1) days  AND
//     a.At ≥ m.JoinedAt + offset days.
//   - That window captures "the day-offset window" — we count a member as
//     retained if at least one activity falls in the window after the
//     offset boundary.
//   - retention_pct = 100 * retained_count / total_size.
//
// Activity events before JoinedAt are ignored. Per-member multiple
// events in the same window are deduplicated.
func ComputeCohortCurve(members []Member, activity []Activity, offsets []int) (Curve, error) {
	if len(members) == 0 {
		return Curve{}, fmt.Errorf("%w: members empty", ErrInvalidArgument)
	}
	if len(offsets) == 0 {
		return Curve{}, fmt.Errorf("%w: offsets empty", ErrInvalidArgument)
	}
	for _, off := range offsets {
		if off <= 0 {
			return Curve{}, fmt.Errorf("%w: offset must be > 0, got %d", ErrInvalidArgument, off)
		}
	}

	// Index activity by gcid for O(1) per-member lookup.
	byGCID := make(map[string][]time.Time, len(members))
	for _, a := range activity {
		byGCID[a.GCID] = append(byGCID[a.GCID], a.At.UTC())
	}

	// Sort offsets ascending (so curve points are emitted in ascending order).
	sortedOffsets := append([]int(nil), offsets...)
	sort.Ints(sortedOffsets)

	points := make([]RetentionPoint, 0, len(sortedOffsets))
	for _, off := range sortedOffsets {
		retained := 0
		for _, m := range members {
			windowStart := m.JoinedAt.Add(time.Duration(off) * 24 * time.Hour)
			windowEnd := m.JoinedAt.Add(time.Duration(off+1) * 24 * time.Hour)
			for _, ts := range byGCID[m.GCID] {
				if ts.Before(m.JoinedAt) {
					continue
				}
				if !ts.Before(windowStart) && ts.Before(windowEnd) {
					retained++
					break // dedupe per-member per-offset
				}
			}
		}
		pct := 100.0 * float64(retained) / float64(len(members))
		points = append(points, RetentionPoint{
			DayOffset:     off,
			RetainedCount: retained,
			RetentionPct:  pct,
		})
	}
	return Curve{TotalSize: len(members), Points: points}, nil
}
