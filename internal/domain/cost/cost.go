// Package cost is the cumulative + by-act breakdown calculator over a
// stream of "acts" (cost-bearing events).
//
// Mirrors the recurring "Budget Counter" gag from the Sequel comic: each act
// ticks a running total. Stored as int64 micros (1e-6 USD) to avoid float
// drift in billing-grade arithmetic.
//
// PURE: input slice -> output struct. No I/O, no time-now. The act caller
// is responsible for ordering acts before passing them in (Cumulative
// preserves input order; GroupByAct sorts by name asc for stable JSON).
package cost

import (
	"fmt"
	"sort"
	"time"
)

// Act is one cost-bearing event in the stream. Name is the value-stream label
// used by the by-act roll-up.
type Act struct {
	Name          string    `json:"name"`
	CostUsdMicros int64     `json:"cost_usd_micros"`
	OccurredAt    time.Time `json:"occurred_at"`
}

// Tick is a row of the cumulative output: per-act cumulative running total.
type Tick struct {
	Act                 Act    `json:"act"`
	CumulativeUsdMicros int64  `json:"cumulative_cost_usd_micros"`
	CumulativeUsd       string `json:"cumulative_cost_usd"`
}

// CumulativeResult is the full cumulative response.
type CumulativeResult struct {
	TotalUsdMicros int64  `json:"total_cost_usd_micros"`
	TotalUsd       string `json:"total_cost_usd"`
	Ticks          []Tick `json:"ticks"`
}

// Cumulative builds the running-total ticker over the input acts (in input
// order). Empty input -> zero total + empty ticks.
//
// Panics on negative cost (acts must be non-negative; this is a programmer
// error, not a runtime input).
func Cumulative(acts []Act) CumulativeResult {
	if len(acts) == 0 {
		return CumulativeResult{TotalUsdMicros: 0, TotalUsd: FormatUsd(0), Ticks: []Tick{}}
	}

	ticks := make([]Tick, 0, len(acts))
	var running int64
	for _, a := range acts {
		if a.CostUsdMicros < 0 {
			panic(fmt.Sprintf("Cumulative: negative cost in act %q: %d", a.Name, a.CostUsdMicros))
		}
		running += a.CostUsdMicros
		ticks = append(ticks, Tick{
			Act:                 a,
			CumulativeUsdMicros: running,
			CumulativeUsd:       FormatUsd(running),
		})
	}
	return CumulativeResult{
		TotalUsdMicros: running,
		TotalUsd:       FormatUsd(running),
		Ticks:          ticks,
	}
}

// ActGroup is one row of the by-act breakdown.
type ActGroup struct {
	Name           string `json:"name"`
	TotalUsdMicros int64  `json:"total_cost_usd_micros"`
	TotalUsd       string `json:"total_cost_usd"`
	EntryCount     int    `json:"entry_count"`
}

// GroupByAct sums acts by Name. Output sorted by Name asc.
//
// Panics on negative cost (programmer error).
func GroupByAct(acts []Act) []ActGroup {
	if len(acts) == 0 {
		return []ActGroup{}
	}
	buckets := make(map[string]*ActGroup)
	for _, a := range acts {
		if a.CostUsdMicros < 0 {
			panic(fmt.Sprintf("GroupByAct: negative cost in act %q: %d", a.Name, a.CostUsdMicros))
		}
		g, ok := buckets[a.Name]
		if !ok {
			g = &ActGroup{Name: a.Name}
			buckets[a.Name] = g
		}
		g.TotalUsdMicros += a.CostUsdMicros
		g.EntryCount++
	}
	out := make([]ActGroup, 0, len(buckets))
	for _, g := range buckets {
		g.TotalUsd = FormatUsd(g.TotalUsdMicros)
		out = append(out, *g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// FormatUsd renders an int64 micros value as $X.YZ (two-decimal). Used for
// human display alongside the raw int micros field.
func FormatUsd(micros int64) string {
	// micros is 1e-6 USD; we render two decimals truncating.
	dollars := micros / 1_000_000
	// Compute the cents in micros: micros mod 1e6, then div by 1e4 (truncate).
	cents := (micros % 1_000_000) / 10_000
	return fmt.Sprintf("$%d.%02d", dollars, cents)
}
