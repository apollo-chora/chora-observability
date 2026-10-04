// familiar_growth_repository.go — in-memory FamiliarGrowthRepository.
//
// Hermetic fixture for tests + the M10/dev path before chora_observability
// is wired. Production wiring uses the pgx-backed pg.FamiliarGrowthRepository
// (also dispatched from cmd/server/main.go when the DB pool is set).
package inmem

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	fg "github.com/5007-Capstone/chora/services/chora-observability/internal/domain/familiargrowth"
)

// FamiliarGrowthRepository is the in-memory implementation of
// familiargrowth.Repository.
type FamiliarGrowthRepository struct {
	mu sync.RWMutex

	// audit_ledger — keyed by audit_id.
	ledger map[string]fg.AuditLedgerRow

	// source_event_id index for idempotency.
	seenEvents map[string]struct{}

	// daily_metrics — keyed by (tenant_id, day_bucket_iso, source).
	daily map[string]fg.DailyMetricsRow

	// breed_roll_audit — keyed by audit_id.
	breedRolls map[string]fg.BreedRollAuditRow

	// egg_funnel_metrics — keyed by (tenant_id, day_bucket_iso).
	funnel map[string]fg.EggFunnelRow
}

// NewFamiliarGrowthRepository constructs an empty in-memory repo.
func NewFamiliarGrowthRepository() *FamiliarGrowthRepository {
	return &FamiliarGrowthRepository{
		ledger:     make(map[string]fg.AuditLedgerRow),
		seenEvents: make(map[string]struct{}),
		daily:      make(map[string]fg.DailyMetricsRow),
		breedRolls: make(map[string]fg.BreedRollAuditRow),
		funnel:     make(map[string]fg.EggFunnelRow),
	}
}

// Ingest atomically writes the audit ledger row + optional side effects.
func (r *FamiliarGrowthRepository) Ingest(_ context.Context, req fg.IngestRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if strings.TrimSpace(req.Ledger.AuditID) == "" {
		return fg.ErrInvalidArgument
	}
	if strings.TrimSpace(req.Ledger.TenantID) == "" {
		return fg.ErrInvalidArgument
	}
	if strings.TrimSpace(req.Ledger.SourceEventID) == "" {
		return fg.ErrInvalidArgument
	}
	if _, dup := r.seenEvents[req.Ledger.SourceEventID]; dup {
		return fg.ErrDuplicateSourceEvent
	}

	row := req.Ledger
	if row.ReceivedAt.IsZero() {
		row.ReceivedAt = time.Now().UTC()
	}
	if row.Payload == nil {
		row.Payload = map[string]any{}
	}
	r.ledger[row.AuditID] = row
	r.seenEvents[row.SourceEventID] = struct{}{}

	if req.BreedRoll != nil {
		b := *req.BreedRoll
		if strings.TrimSpace(b.AuditID) == "" {
			b.AuditID = row.AuditID
		}
		r.breedRolls[b.AuditID] = b
	}

	if req.MetricsDelta != nil {
		key := r.dailyKey(row.TenantID, req.MetricsDelta.DayBucket, req.MetricsDelta.Source)
		cur := r.daily[key]
		if cur.TenantID == "" {
			cur = fg.DailyMetricsRow{
				TenantID:  row.TenantID,
				DayBucket: req.MetricsDelta.DayBucket,
				Source:    req.MetricsDelta.Source,
			}
		}
		cur.TotalExpAwarded += req.MetricsDelta.ExpAwardedDelta
		cur.EventCount += req.MetricsDelta.EventCountDelta
		cur.StageUpsCount += req.MetricsDelta.StageUpsDelta
		cur.UpdatedAt = row.ReceivedAt
		r.daily[key] = cur
	}

	if req.FunnelDelta != nil {
		key := r.funnelKey(row.TenantID, req.FunnelDelta.DayBucket)
		cur := r.funnel[key]
		if cur.TenantID == "" {
			cur = fg.EggFunnelRow{
				TenantID:  row.TenantID,
				DayBucket: req.FunnelDelta.DayBucket,
			}
		}
		cur.EggsPurchased += req.FunnelDelta.PurchasedDelta
		cur.EggsHatched += req.FunnelDelta.HatchedDelta
		cur.EggsExpiredUnhatched += req.FunnelDelta.ExpiredDelta
		cur.UpdatedAt = row.ReceivedAt
		r.funnel[key] = cur
	}
	return nil
}

// ListLedger returns audit_ledger rows for the tenant, sorted received_at DESC.
func (r *FamiliarGrowthRepository) ListLedger(_ context.Context, tenantID string, f fg.AuditFilter) ([]fg.AuditLedgerRow, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]fg.AuditLedgerRow, 0, 8)
	for _, v := range r.ledger {
		if v.TenantID != tenantID {
			continue
		}
		if !f.From.IsZero() && v.ReceivedAt.Before(f.From) {
			continue
		}
		if !f.To.IsZero() && !v.ReceivedAt.Before(f.To) {
			continue
		}
		if f.Source != "" && v.SourceTopic != f.Source {
			continue
		}
		if f.FamiliarID != "" && v.FamiliarID != f.FamiliarID {
			continue
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ReceivedAt.After(out[j].ReceivedAt)
	})
	return paginate(out, f), nil
}

// ListMetrics returns daily-rollup rows for tenant + window.
func (r *FamiliarGrowthRepository) ListMetrics(_ context.Context, tenantID string, f fg.AuditFilter) ([]fg.DailyMetricsRow, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]fg.DailyMetricsRow, 0, 8)
	for _, v := range r.daily {
		if v.TenantID != tenantID {
			continue
		}
		if !f.From.IsZero() && v.DayBucket.Before(f.From) {
			continue
		}
		if !f.To.IsZero() && !v.DayBucket.Before(f.To) {
			continue
		}
		if f.Source != "" && v.Source != f.Source {
			continue
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].DayBucket.Equal(out[j].DayBucket) {
			return out[i].Source < out[j].Source
		}
		return out[i].DayBucket.After(out[j].DayBucket)
	})
	return out, nil
}

// ListBreedRolls returns breed_roll_audit rows for tenant + filter.
func (r *FamiliarGrowthRepository) ListBreedRolls(_ context.Context, tenantID string, f fg.AuditFilter) ([]fg.BreedRollAuditRow, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]fg.BreedRollAuditRow, 0, 8)
	for _, v := range r.breedRolls {
		if v.TenantID != tenantID {
			continue
		}
		if !f.From.IsZero() && v.RevealedAt.Before(f.From) {
			continue
		}
		if !f.To.IsZero() && !v.RevealedAt.Before(f.To) {
			continue
		}
		if f.EggSKU != "" && v.EggSKU != f.EggSKU {
			continue
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].RevealedAt.After(out[j].RevealedAt)
	})
	return out, nil
}

// ListEggFunnel returns egg_funnel_metrics rows for tenant + window.
func (r *FamiliarGrowthRepository) ListEggFunnel(_ context.Context, tenantID string, f fg.AuditFilter) ([]fg.EggFunnelRow, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]fg.EggFunnelRow, 0, 8)
	for _, v := range r.funnel {
		if v.TenantID != tenantID {
			continue
		}
		if !f.From.IsZero() && v.DayBucket.Before(f.From) {
			continue
		}
		if !f.To.IsZero() && !v.DayBucket.Before(f.To) {
			continue
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].DayBucket.After(out[j].DayBucket)
	})
	return out, nil
}

func (r *FamiliarGrowthRepository) dailyKey(tenantID string, day time.Time, source string) string {
	return tenantID + "|" + day.UTC().Format("2006-01-02") + "|" + source
}

func (r *FamiliarGrowthRepository) funnelKey(tenantID string, day time.Time) string {
	return tenantID + "|" + day.UTC().Format("2006-01-02")
}

func paginate[T any](in []T, f fg.AuditFilter) []T {
	if f.Limit <= 0 && f.Offset <= 0 {
		return in
	}
	off := f.Offset
	if off < 0 {
		off = 0
	}
	if off > len(in) {
		return []T{}
	}
	end := len(in)
	if f.Limit > 0 && off+f.Limit < end {
		end = off + f.Limit
	}
	out := make([]T, end-off)
	copy(out, in[off:end])
	return out
}

// Compile-time check.
var _ fg.Repository = (*FamiliarGrowthRepository)(nil)
