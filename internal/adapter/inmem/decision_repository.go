package inmem

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/decision"
)

// DecisionRepository is a goroutine-safe append-only repository for
// AgentDecisionLog.
type DecisionRepository struct {
	mu   sync.RWMutex
	logs []*decision.Log // append-only slice
}

// NewDecisionRepository constructs an initialised repository.
func NewDecisionRepository() *DecisionRepository {
	return &DecisionRepository{logs: make([]*decision.Log, 0)}
}

// Append persists the log with defensive deep-copy.
func (r *DecisionRepository) Append(_ context.Context, l *decision.Log) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logs = append(r.logs, cloneLog(l))
	return nil
}

// cloneLog returns a defensive deep-copy of l so external mutation of the
// reference-typed fields (Reasoning pointer, PromptConditions map) does not leak
// into the stored append-only slice.
func cloneLog(l *decision.Log) *decision.Log {
	clone := *l
	if l.Reasoning != nil {
		rsClone := *l.Reasoning
		clone.Reasoning = &rsClone
	}
	if l.PromptConditions != nil {
		pc := make(map[string]string, len(l.PromptConditions))
		for k, v := range l.PromptConditions {
			pc[k] = v
		}
		clone.PromptConditions = pc
	}
	return &clone
}

// GetByID returns the log by ID for the tenant. Defensively deep-copies.
func (r *DecisionRepository) GetByID(_ context.Context, tenantID, logID string) (*decision.Log, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, l := range r.logs {
		if l.TenantID == tenantID && l.LogID == logID {
			return cloneLog(l), nil
		}
	}
	return nil, decision.ErrNotFound
}

// List returns logs for the tenant matching the filter, sorted CreatedAt
// ascending by default; when f.Descending is set the order is reversed to
// newest-first (so a Limit takes the most-recent rows). Default ASC mirrors the
// pg adapter and the /o/agents collectStats contract.
func (r *DecisionRepository) List(_ context.Context, tenantID string, f decision.ListFilter) ([]*decision.Log, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*decision.Log, 0)
	for _, l := range r.logs {
		if l.TenantID != tenantID {
			continue
		}
		if f.Agid != "" && l.Agid != f.Agid {
			continue
		}
		if !f.From.IsZero() && l.CreatedAt.Before(f.From) {
			continue
		}
		if !f.To.IsZero() && !l.CreatedAt.Before(f.To) {
			continue
		}
		out = append(out, cloneLog(l))
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	if f.Descending {
		// Reverse to newest-first BEFORE Offset/Limit so the limit takes the
		// most-recent rows (the O+ Decision Traces audit view).
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
	}

	if f.Offset > 0 && f.Offset < len(out) {
		out = out[f.Offset:]
	} else if f.Offset >= len(out) {
		out = nil
	}
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// Count returns the number of decision logs for the tenant in the half-open
// window [since, until). Zero-value since/until disable the respective bound.
func (r *DecisionRepository) Count(_ context.Context, tenantID string, since, until time.Time) (int64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var n int64
	for _, l := range r.logs {
		if l.TenantID != tenantID {
			continue
		}
		if !since.IsZero() && l.CreatedAt.Before(since) {
			continue
		}
		if !until.IsZero() && !l.CreatedAt.Before(until) {
			continue
		}
		n++
	}
	return n, nil
}
