// Package cost_test exercises the cumulative cost calculator.
//
// The cumulative ticker mirrors the recurring "Budget Counter" gag in
// docs/comic/Sequel_Chora-Agentic-Customer-journeys.md — every act ticks
// the running total. Canonical seed (15 acts in the comic; Act 16 is
// epilogue with $0.00 added):
//
//	$0.12, $0.04, $0.05, $0.03, $0.04, $0.06, $0.02, $0.03,
//	$0.07, $0.02, $0.03, $0.01, $0.04, $0.12, $0.08
//
// Sum = $0.76. We store as int64 micros (1e-6 USD) so values are
// 120000, 40000, 50000, ..., summing to 760000.
package cost_test

import (
	"testing"
	"time"

	"github.com/apollo-chora/chora-observability/internal/domain/cost"
)

// SequelSeed is the canonical 15-act tick sequence from the comic, in micros.
var SequelSeed = []int64{
	120000, 40000, 50000, 30000, 40000, 60000, 20000, 30000,
	70000, 20000, 30000, 10000, 40000, 120000, 80000,
}

const SequelTotalMicros int64 = 760000 // $0.76

func TestCumulative_Empty(t *testing.T) {
	t.Parallel()
	res := cost.Cumulative(nil)
	if res.TotalUsdMicros != 0 {
		t.Errorf("empty total = %d; want 0", res.TotalUsdMicros)
	}
	if len(res.Ticks) != 0 {
		t.Errorf("empty ticks = %d; want 0", len(res.Ticks))
	}
}

func TestCumulative_SequelSeed_ExactlySeventySixCents(t *testing.T) {
	t.Parallel()
	// THE Sequel test: drop the canonical 15-tick seed in, get $0.76 out.
	now := time.Date(2026, 5, 8, 8, 30, 0, 0, time.UTC)
	acts := make([]cost.Act, len(SequelSeed))
	for i, c := range SequelSeed {
		acts[i] = cost.Act{
			Name:          string(rune('A' + i)),
			CostUsdMicros: c,
			OccurredAt:    now.Add(time.Duration(i) * 30 * time.Minute),
		}
	}
	res := cost.Cumulative(acts)
	if res.TotalUsdMicros != SequelTotalMicros {
		t.Errorf("total = %d; want %d ($0.76)", res.TotalUsdMicros, SequelTotalMicros)
	}
	if len(res.Ticks) != len(SequelSeed) {
		t.Fatalf("ticks = %d; want %d", len(res.Ticks), len(SequelSeed))
	}
	// Each tick must be the running sum.
	var running int64
	for i, tick := range res.Ticks {
		running += SequelSeed[i]
		if tick.CumulativeUsdMicros != running {
			t.Errorf("tick[%d] cumulative = %d; want %d",
				i, tick.CumulativeUsdMicros, running)
		}
	}
}

func TestCumulative_PrecisionAtSequelMilestones(t *testing.T) {
	t.Parallel()
	// Spot-check the comic's marquee budget callouts:
	//   Act 1:  $0.12   ($120000 micros)
	//   Act 11: $0.51   ($510000)
	//   Act 14: $0.68   ($680000)
	//   Act 15: $0.76   ($760000)
	now := time.Now().UTC()
	acts := make([]cost.Act, len(SequelSeed))
	for i, c := range SequelSeed {
		acts[i] = cost.Act{
			Name:          "act",
			CostUsdMicros: c,
			OccurredAt:    now,
		}
	}
	res := cost.Cumulative(acts)
	checkpoints := map[int]int64{
		0:  120000, // Act 1: $0.12
		10: 510000, // Act 11: $0.51
		13: 680000, // Act 14: $0.68
		14: 760000, // Act 15: $0.76
	}
	for idx, want := range checkpoints {
		if got := res.Ticks[idx].CumulativeUsdMicros; got != want {
			t.Errorf("tick[%d] = %d; want %d", idx, got, want)
		}
	}
}

func TestCumulative_RejectsNegativeAct(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic for negative act cost")
		}
	}()
	cost.Cumulative([]cost.Act{
		{Name: "neg", CostUsdMicros: -1, OccurredAt: time.Now()},
	})
}

func TestCumulative_PreservesActOrder(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	acts := []cost.Act{
		{Name: "first", CostUsdMicros: 100, OccurredAt: now},
		{Name: "second", CostUsdMicros: 200, OccurredAt: now.Add(time.Minute)},
		{Name: "third", CostUsdMicros: 50, OccurredAt: now.Add(2 * time.Minute)},
	}
	res := cost.Cumulative(acts)
	if res.Ticks[0].Act.Name != "first" {
		t.Errorf("tick[0] name = %q; want first", res.Ticks[0].Act.Name)
	}
	if res.Ticks[2].CumulativeUsdMicros != 350 {
		t.Errorf("tick[2] cum = %d; want 350", res.Ticks[2].CumulativeUsdMicros)
	}
}

func TestCumulative_FormattedUsd(t *testing.T) {
	t.Parallel()
	// Formatting helper renders micros as $X.YZ (two decimals) for human
	// display matching the comic's ticker.
	if got := cost.FormatUsd(120000); got != "$0.12" {
		t.Errorf("FormatUsd(120000) = %q; want $0.12", got)
	}
	if got := cost.FormatUsd(760000); got != "$0.76" {
		t.Errorf("FormatUsd(760000) = %q; want $0.76", got)
	}
	if got := cost.FormatUsd(0); got != "$0.00" {
		t.Errorf("FormatUsd(0) = %q; want $0.00", got)
	}
	if got := cost.FormatUsd(1500000); got != "$1.50" {
		t.Errorf("FormatUsd(1500000) = %q; want $1.50", got)
	}
}
