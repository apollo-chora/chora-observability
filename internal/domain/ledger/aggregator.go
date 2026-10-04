// Aggregator: pure function over append-only ledger entries.
//
// Aggregator groups entries by model_id / agid / gcid and returns deterministic
// totals. Reused by the in-memory repository AND the eventual Cloud SQL
// adapter. Output is sorted by group key ascending for stable JSON.
package ledger

import (
	"sort"
)

// GroupBy is the dimension to roll up by.
type GroupBy string

const (
	GroupByModel GroupBy = "model"
	GroupByAgent GroupBy = "agent"
	GroupByGcid  GroupBy = "gcid"
)

// AggregateGroup is one row of the aggregation output.
type AggregateGroup struct {
	Group              string `json:"group"`
	TotalCostUsdMicros int64  `json:"total_cost_usd_micros"`
	PromptTokens       int    `json:"prompt_tokens"`
	CompletionTokens   int    `json:"completion_tokens"`
	EntryCount         int    `json:"entry_count"`
}

// AggregateBy returns groups rolled up by the given dimension. Unknown
// dimensions fall back to GroupByModel.
//
// Empty input returns empty slice. Output is sorted by Group ascending.
func AggregateBy(entries []*Entry, by GroupBy) []AggregateGroup {
	if len(entries) == 0 {
		return []AggregateGroup{}
	}

	keyer := keyByModel
	switch by {
	case GroupByAgent:
		keyer = keyByAgent
	case GroupByGcid:
		keyer = keyByGcid
	case GroupByModel:
		keyer = keyByModel
	default:
		// Unknown — fall back to model (per test contract).
		keyer = keyByModel
	}

	buckets := make(map[string]*AggregateGroup)
	for _, e := range entries {
		k := keyer(e)
		g, ok := buckets[k]
		if !ok {
			g = &AggregateGroup{Group: k}
			buckets[k] = g
		}
		g.TotalCostUsdMicros += e.CostUsdMicros
		g.PromptTokens += e.PromptTokens
		g.CompletionTokens += e.CompletionTokens
		g.EntryCount++
	}

	out := make([]AggregateGroup, 0, len(buckets))
	for _, g := range buckets {
		out = append(out, *g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Group < out[j].Group })
	return out
}

func keyByModel(e *Entry) string { return e.ModelID }
func keyByAgent(e *Entry) string { return e.Agid }
func keyByGcid(e *Entry) string  { return e.Gcid }
