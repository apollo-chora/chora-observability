// Package familiargrowth holds the audit ledger + IMDA D1/D2 governance
// dashboard types for the ADR-149 Familiar Growth rollout (PROD-H).
//
// The 4 audit tables in chora_observability are projections of the 6
// inbound event topics (chora.consumption.familiar.* + chora.tenancy.familiar_egg.*).
// chora-observability is the canonical owner of the audit ledger; this
// package defines the domain types + repository port so adapter packages
// can build pgx-backed implementations + in-memory test doubles.
//
// IMDA dimensions per ADR-141:
//   - D1 accountability  : exp_awarded / stage_up / source_revelation / egg_purchased / payment_succeeded
//   - D2 transparency    : breed_revealed (lootbox roll outcome — auditable distribution)
//   - hatched (user-visible lifecycle transition; Fix-D 2026-05-16)
package familiargrowth

import (
	"context"
	"errors"
	"time"
)

// Source topic constants for the 7 ADR-149 Familiar Growth events
// (Fix-D 2026-05-16 added hatched.v1 — was previously proxied through
// breed_revealed.v1 in the egg-funnel rollup; now sourced directly so the
// IMDA D2 audit log records the actual lifecycle transition).
//
// The familiar.* names are the LEGACY subjects: ADR-254 renamed the aggregate
// to `companion` and the producers now emit the companion.* subjects below.
// Both families are subscribed — the companion.* subjects carry the live
// traffic, the familiar.* subjects are retained for any straggler producer
// still on the legacy name.
const (
	TopicExpAwarded       = "chora.consumption.familiar.exp_awarded.v1"
	TopicStageUp          = "chora.consumption.familiar.stage_up.v1"
	TopicBreedRevealed    = "chora.consumption.familiar.breed_revealed.v1"
	TopicHatched          = "chora.consumption.familiar.hatched.v1"
	TopicSourceRevelation = "chora.consumption.familiar.source_revelation.v1"
	TopicEggPurchased     = "chora.consumption.familiar.egg_purchased.v1"
	TopicPaymentSucceeded = "chora.tenancy.familiar_egg.payment_succeeded.v1"
)

// ADR-254 canonical companion source topics. Same event types, same payloads,
// same projection — only the aggregate token in the subject changed
// (familiar -> companion). chora-contracts is the source of truth for these
// names (proto/events/consumption/companion.proto,
// proto/events/tenancy/companion_egg.proto).
const (
	TopicExpAwardedCompanion       = "chora.consumption.companion.exp_awarded.v1"
	TopicStageUpCompanion          = "chora.consumption.companion.stage_up.v1"
	TopicBreedRevealedCompanion    = "chora.consumption.companion.breed_revealed.v1"
	TopicHatchedCompanion          = "chora.consumption.companion.hatched.v1"
	TopicSourceRevelationCompanion = "chora.consumption.companion.source_revelation.v1"
	TopicEggPurchasedCompanion     = "chora.consumption.companion.egg_purchased.v1"
	TopicPaymentSucceededCompanion = "chora.tenancy.companion_egg.payment_succeeded.v1"
)

// IMDA dimensions per ADR-141 canonical taxonomy.
const (
	IMDADimensionAccountability = "accountability" // D1
	IMDADimensionTransparency   = "transparency"   // D2
	IMDALifecycleStageRuntime   = "runtime"
)

// AuditLedgerRow mirrors familiar_growth_audit_ledger one-row-per-event.
type AuditLedgerRow struct {
	AuditID       string         `json:"audit_id"`
	TenantID      string         `json:"tenant_id"`
	SourceTopic   string         `json:"source_topic"`
	SourceEventID string         `json:"source_event_id"`
	FamiliarID    string         `json:"familiar_id,omitempty"`
	OwnerGCID     string         `json:"owner_gcid,omitempty"`
	EventType     string         `json:"event_type"`
	Payload       map[string]any `json:"payload"`
	ReceivedAt    time.Time      `json:"received_at"`
}

// DailyMetricsRow mirrors familiar_growth_daily_metrics — per-tenant
// per-day per-source rollup.
type DailyMetricsRow struct {
	TenantID        string    `json:"tenant_id"`
	DayBucket       time.Time `json:"day_bucket"`
	Source          string    `json:"source"`
	TotalExpAwarded int64     `json:"total_exp_awarded"`
	EventCount      int64     `json:"event_count"`
	StageUpsCount   int       `json:"stage_ups_count"`
	UpdatedAt       time.Time `json:"updated_at,omitempty"`
}

// BreedRollAuditRow mirrors breed_roll_audit — per breed_revealed event
// (IMDA D2 transparency).
type BreedRollAuditRow struct {
	AuditID              string         `json:"audit_id"`
	TenantID             string         `json:"tenant_id"`
	FamiliarID           string         `json:"familiar_id"`
	OwnerGCID            string         `json:"owner_gcid,omitempty"`
	EggSKU               string         `json:"egg_sku"`
	Species              string         `json:"species"`
	Shiny                bool           `json:"shiny"`
	Rarity               string         `json:"rarity"`
	RolledProbability    float64        `json:"rolled_probability"`
	DistributionSnapshot map[string]any `json:"distribution_snapshot,omitempty"`
	RevealedAt           time.Time      `json:"revealed_at"`
}

// EggFunnelRow mirrors egg_funnel_metrics — per-tenant per-day egg funnel.
type EggFunnelRow struct {
	TenantID             string    `json:"tenant_id"`
	DayBucket            time.Time `json:"day_bucket"`
	EggsPurchased        int       `json:"eggs_purchased"`
	EggsHatched          int       `json:"eggs_hatched"`
	EggsExpiredUnhatched int       `json:"eggs_expired_unhatched"`
	UpdatedAt            time.Time `json:"updated_at,omitempty"`
}

// AuditFilter is the common ListLedger / ListMetrics filter.
type AuditFilter struct {
	From       time.Time
	To         time.Time
	Source     string // optional source_topic filter
	FamiliarID string // optional familiar_id filter
	EggSKU     string // optional egg_sku filter (breed-roll endpoint only)
	Limit      int
	Offset     int
}

// IngestRequest is what the subscriber hands to the repository to record
// one event end-to-end (audit ledger + per-source-table side effects).
type IngestRequest struct {
	// LedgerRow is always required.
	Ledger AuditLedgerRow

	// BreedRoll is set when SourceTopic == TopicBreedRevealed.
	BreedRoll *BreedRollAuditRow

	// MetricsDelta is set for exp_awarded / stage_up — drives the daily
	// metrics UPSERT.
	MetricsDelta *DailyMetricsDelta

	// FunnelDelta is set for egg_purchased / breed_revealed (proxy for
	// hatch) — drives the egg-funnel UPSERT.
	FunnelDelta *EggFunnelDelta
}

// DailyMetricsDelta is the increment applied to familiar_growth_daily_metrics
// on a single ingest.
type DailyMetricsDelta struct {
	DayBucket       time.Time
	Source          string
	ExpAwardedDelta int64
	EventCountDelta int64
	StageUpsDelta   int
}

// EggFunnelDelta is the increment applied to egg_funnel_metrics on a
// single ingest.
type EggFunnelDelta struct {
	DayBucket      time.Time
	PurchasedDelta int
	HatchedDelta   int
	ExpiredDelta   int
}

// ErrDuplicateSourceEvent signals a UNIQUE collision on source_event_id —
// the subscriber treats this as a no-op (idempotent replay).
var ErrDuplicateSourceEvent = errors.New("familiargrowth: duplicate source_event_id")

// ErrInvalidArgument is returned for malformed inputs.
var ErrInvalidArgument = errors.New("familiargrowth: invalid argument")

// Repository is the persistence port for the 4 audit tables. Adapters
// implement this against pgx (production) or in-memory (tests + dev).
type Repository interface {
	// Ingest atomically inserts the audit ledger row + side-effects
	// (breed-roll / daily metrics upsert / funnel upsert) for one event.
	// Returns ErrDuplicateSourceEvent when SourceEventID already exists —
	// the caller MAY treat this as a no-op.
	Ingest(ctx context.Context, req IngestRequest) error

	// ListLedger returns audit_ledger rows for the tenant matching the
	// filter, sorted received_at DESC.
	ListLedger(ctx context.Context, tenantID string, f AuditFilter) ([]AuditLedgerRow, error)

	// ListMetrics returns daily-rollup rows for the tenant + window.
	ListMetrics(ctx context.Context, tenantID string, f AuditFilter) ([]DailyMetricsRow, error)

	// ListBreedRolls returns breed_roll_audit rows for the tenant + filter.
	ListBreedRolls(ctx context.Context, tenantID string, f AuditFilter) ([]BreedRollAuditRow, error)

	// ListEggFunnel returns egg_funnel_metrics rows for the tenant + window.
	ListEggFunnel(ctx context.Context, tenantID string, f AuditFilter) ([]EggFunnelRow, error)
}

// ChiSquareResult is the IMDA D2 transparency goodness-of-fit summary.
type ChiSquareResult struct {
	// Chi-square statistic.
	Statistic float64 `json:"statistic"`
	// Degrees of freedom (k-1 where k = number of categories with non-zero
	// expected frequency).
	DegreesOfFreedom int `json:"degrees_of_freedom"`
	// Approximate p-value (Wilson-Hilferty cube-root normal approximation).
	// Range [0, 1]. Higher = empirical fits claimed distribution well.
	PValue float64 `json:"p_value"`
	// SampleSize is the total empirical count.
	SampleSize int `json:"sample_size"`
	// Categories under consideration (deterministic order).
	Categories []string `json:"categories"`
	// Expected counts per category (under the claimed distribution).
	Expected []float64 `json:"expected_counts"`
	// Observed counts per category.
	Observed []int `json:"observed_counts"`
	// True when sample is too small to compute (n < 5 across all bins).
	InsufficientSample bool `json:"insufficient_sample"`
}

// BreedDistributionReport is the response payload for the D2 dashboard
// endpoint. Empirical = observed counts in the audit window. Claimed =
// the egg SKU's published breed_distribution at the most recent roll in
// the window (read from distribution_snapshot).
type BreedDistributionReport struct {
	TenantID   string    `json:"tenant_id"`
	EggSKU     string    `json:"egg_sku"`
	From       time.Time `json:"from"`
	To         time.Time `json:"to"`
	SampleSize int       `json:"sample_size"`

	// Empirical observed-frequency table (sum = SampleSize).
	Empirical map[string]int `json:"empirical"`

	// Empirical observed-probability table (sum ≈ 1.0).
	EmpiricalProbabilities map[string]float64 `json:"empirical_probabilities"`

	// Claimed = distribution_snapshot from the most recent roll in the
	// window. May be empty when no roll has occurred.
	Claimed map[string]float64 `json:"claimed"`

	// ChiSquare is populated when both Empirical and Claimed are non-empty.
	ChiSquare *ChiSquareResult `json:"chi_square,omitempty"`
}
