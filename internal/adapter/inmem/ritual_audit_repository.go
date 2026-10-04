// ritual_audit_repository.go — in-memory ritualaudit.Repository.
//
// Hermetic fixture for tests + the dev path before chora_observability is
// wired. Production wiring uses the pgx-backed pg.RitualAuditRepository (also
// dispatched from cmd/server/main.go when the DB pool is set).
package inmem

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	ra "github.com/apollo-chora/chora-observability/internal/domain/ritualaudit"
)

// RitualAuditRepository is the in-memory implementation of
// ritualaudit.Repository.
type RitualAuditRepository struct {
	mu sync.RWMutex

	// rows — keyed by audit_id.
	rows map[string]ra.RitualRunAuditRow

	// seenEvents — source_event_id index for idempotency.
	seenEvents map[string]struct{}
}

// NewRitualAuditRepository constructs an empty in-memory repo.
func NewRitualAuditRepository() *RitualAuditRepository {
	return &RitualAuditRepository{
		rows:       make(map[string]ra.RitualRunAuditRow),
		seenEvents: make(map[string]struct{}),
	}
}

// Ingest inserts one audit row. A repeat source_event_id maps to
// ra.ErrDuplicateSourceEvent (idempotent replay).
func (r *RitualAuditRepository) Ingest(_ context.Context, row ra.RitualRunAuditRow) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if strings.TrimSpace(row.AuditID) == "" ||
		strings.TrimSpace(row.TenantID) == "" ||
		strings.TrimSpace(row.SourceEventID) == "" ||
		strings.TrimSpace(row.RunID) == "" {
		return ra.ErrInvalidArgument
	}
	if _, dup := r.seenEvents[row.SourceEventID]; dup {
		return ra.ErrDuplicateSourceEvent
	}
	if row.ReceivedAt.IsZero() {
		row.ReceivedAt = time.Now().UTC()
	}
	if row.Stamps == nil {
		row.Stamps = []map[string]any{}
	}
	r.rows[row.AuditID] = row
	r.seenEvents[row.SourceEventID] = struct{}{}
	return nil
}

// List returns the tenant's rows sorted received_at DESC, capped at limit
// (limit <= 0 returns all).
func (r *RitualAuditRepository) List(_ context.Context, tenantID string, limit int) ([]ra.RitualRunAuditRow, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ra.RitualRunAuditRow, 0, 8)
	for _, v := range r.rows {
		if v.TenantID != tenantID {
			continue
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ReceivedAt.After(out[j].ReceivedAt)
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// Compile-time check.
var _ ra.Repository = (*RitualAuditRepository)(nil)
