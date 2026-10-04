// Package eventaggregator is the cross-domain Pub/Sub event aggregator
// for the analytics slice of chora-observability (formerly chora-analytics,
// consolidated at M12.2.E.4 per docs/architecture/m12-2-consolidation-plan-2026-05-12.md).
//
// SCOPE — Analytics is a READ-ONLY leaf consumer of every `chora.*.v1`
// event published by the 11 domains. It NEVER republishes (per the
// "read-only on event ingestion" constraint).
//
// Aggregator semantics:
//   - Per-period buckets: hour | day | week | month (UTC truncated).
//   - Bucket key: tenant + (period, period_start) + (domain, aggregate, event_type).
//   - Idempotent on EnvelopeIdempotencyKey: re-delivering the same event
//     does NOT inflate counts (Pub/Sub at-least-once is the norm).
//
// Multi-tenant isolation: queries always scope by TenantID; cross-tenant
// results are never returned. Use literal "platform" for cross-tenant
// platform-level events (per envelope.proto §tenant_id).
//
// Topic taxonomy validated: chora.{domain}.{aggregate}.{event_type}.v{N}
// (5 dotted segments after the chora prefix; v{N} is the major version).
package eventaggregator

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ===== Envelope =====

// Envelope is the analytics-internal projection of the platform-wide
// EventEnvelope (chora-contracts/proto/common/envelope.proto). We only
// retain the fields needed for aggregation; payloads are intentionally
// dropped (analytics counts events, it does not store payloads).
type Envelope struct {
	EventID        string
	IdempotencyKey string
	TenantID       string
	GCID           string
	Topic          string // raw chora.{domain}.{aggregate}.{event_type}.v{N}
	Domain         string // parsed from Topic
	Aggregate      string // parsed from Topic
	EventType      string // parsed from Topic
	SchemaVersion  int32
	OccurredAt     time.Time
	PublishedAt    time.Time
}

// EnvelopeParams is the constructor input.
type EnvelopeParams struct {
	EventID        string
	IdempotencyKey string
	TenantID       string
	GCID           string
	Topic          string
	OccurredAt     time.Time
	PublishedAt    time.Time
	SchemaVersion  int32
}

// ErrInvalidEnvelope is the sentinel for envelope validation failures.
var ErrInvalidEnvelope = errors.New("invalid envelope")

// NewEnvelope constructs a validated Envelope.
//
// Topic format: chora.{domain}.{aggregate}.{event_type}.v{N}
//
//	segment 0: literal "chora"
//	segment 1: domain ∈ 11 known names
//	segment 2: aggregate (snake_case)
//	segment 3: event_type (snake_case past tense)
//	segment 4: v{N} version (N ≥ 1)
func NewEnvelope(p EnvelopeParams) (Envelope, error) {
	if strings.TrimSpace(p.TenantID) == "" {
		return Envelope{}, fmt.Errorf("%w: tenant_id required", ErrInvalidEnvelope)
	}
	if strings.TrimSpace(p.EventID) == "" {
		return Envelope{}, fmt.Errorf("%w: event_id required", ErrInvalidEnvelope)
	}
	if strings.TrimSpace(p.IdempotencyKey) == "" {
		return Envelope{}, fmt.Errorf("%w: idempotency_key required", ErrInvalidEnvelope)
	}
	domain, aggregate, eventType, err := parseTopic(p.Topic)
	if err != nil {
		return Envelope{}, fmt.Errorf("%w: %v", ErrInvalidEnvelope, err)
	}
	return Envelope{
		EventID:        p.EventID,
		IdempotencyKey: p.IdempotencyKey,
		TenantID:       p.TenantID,
		GCID:           p.GCID,
		Topic:          p.Topic,
		Domain:         domain,
		Aggregate:      aggregate,
		EventType:      eventType,
		SchemaVersion:  p.SchemaVersion,
		OccurredAt:     p.OccurredAt,
		PublishedAt:    p.PublishedAt,
	}, nil
}

// validDomains is the closed set per CLAUDE.md §3 + chora-contracts §2.
var validDomains = map[string]struct{}{
	// 5 core
	"creation": {}, "consumption": {}, "delivery": {}, "sharing": {}, "a2a": {},
	// 6 supporting (M12.2 adds chora_support as 7th — accept it preemptively)
	"identity": {}, "tenancy": {}, "governance": {}, "observability": {},
	"notifications": {}, "ai_kernel": {}, "support": {},
}

func parseTopic(topic string) (string, string, string, error) {
	if !strings.HasPrefix(topic, "chora.") {
		return "", "", "", fmt.Errorf("topic %q lacks chora. prefix", topic)
	}
	parts := strings.Split(topic, ".")
	if len(parts) != 5 {
		return "", "", "", fmt.Errorf("topic %q must have 5 dotted segments", topic)
	}
	domain, aggregate, eventType, version := parts[1], parts[2], parts[3], parts[4]
	if _, ok := validDomains[domain]; !ok {
		return "", "", "", fmt.Errorf("unknown domain %q", domain)
	}
	if aggregate == "" || eventType == "" {
		return "", "", "", fmt.Errorf("aggregate / event_type empty")
	}
	if !strings.HasPrefix(version, "v") {
		return "", "", "", fmt.Errorf("version %q must start with v", version)
	}
	n, err := strconv.Atoi(version[1:])
	if err != nil || n < 1 {
		return "", "", "", fmt.Errorf("invalid version %q", version)
	}
	return domain, aggregate, eventType, nil
}

// ===== Period =====

// Period is the aggregation granularity.
type Period string

const (
	PeriodHour  Period = "hour"
	PeriodDay   Period = "day"
	PeriodWeek  Period = "week"
	PeriodMonth Period = "month"
)

// truncate returns the start of the period containing t (UTC).
func (p Period) truncate(t time.Time) (time.Time, error) {
	t = t.UTC()
	switch p {
	case PeriodHour:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, time.UTC), nil
	case PeriodDay:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC), nil
	case PeriodWeek:
		// ISO week: Monday-anchored.
		wd := int(t.Weekday())
		// Go: Sunday=0..Saturday=6. Convert to Mon=0..Sun=6.
		wd = (wd + 6) % 7
		monday := time.Date(t.Year(), t.Month(), t.Day()-wd, 0, 0, 0, 0, time.UTC)
		return monday, nil
	case PeriodMonth:
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC), nil
	default:
		return time.Time{}, fmt.Errorf("unknown period %q", p)
	}
}

// ===== Bucket =====

// Bucket is the per-(tenant, period, period_start) counter view.
type Bucket struct {
	TenantID    string
	Period      Period
	PeriodStart time.Time
	Counts      map[string]int // key = domain|aggregate|event_type
}

// CountFor reads the count for a (domain, aggregate, event_type) triple.
// Returns 0 for unknown keys.
func (b Bucket) CountFor(domain, aggregate, eventType string) int {
	if b.Counts == nil {
		return 0
	}
	return b.Counts[counterKey(domain, aggregate, eventType)]
}

func counterKey(domain, aggregate, eventType string) string {
	return domain + "|" + aggregate + "|" + eventType
}

// ===== Aggregator =====

// Aggregator is the in-memory fold of all ingested envelopes.
//
// Production deployment will swap the in-memory fold for a Postgres
// materialised view (chora_observability — analytics is now consolidated
// under the observability domain at M12.2.E.4); the aggregation domain
// logic stays here.
type Aggregator struct {
	mu      sync.RWMutex
	buckets map[bucketKey]*Bucket
	seen    map[string]struct{} // idempotency_keys observed
}

type bucketKey struct {
	TenantID    string
	Period      Period
	PeriodStart time.Time
}

// NewAggregator returns a fresh, empty Aggregator.
func NewAggregator() *Aggregator {
	return &Aggregator{
		buckets: make(map[bucketKey]*Bucket),
		seen:    make(map[string]struct{}),
	}
}

// Ingest folds an envelope into the per-period buckets.
//
// Idempotent: if this IdempotencyKey has been seen, the call is a no-op.
// At-least-once Pub/Sub delivery means the same event_id can arrive
// multiple times; aggregation MUST not double-count.
func (a *Aggregator) Ingest(e Envelope) error {
	if strings.TrimSpace(e.TenantID) == "" {
		return fmt.Errorf("%w: tenant_id required", ErrInvalidEnvelope)
	}
	if strings.TrimSpace(e.IdempotencyKey) == "" {
		return fmt.Errorf("%w: idempotency_key required", ErrInvalidEnvelope)
	}
	if strings.TrimSpace(e.Domain) == "" || strings.TrimSpace(e.Aggregate) == "" || strings.TrimSpace(e.EventType) == "" {
		return fmt.Errorf("%w: topic segments missing — use NewEnvelope", ErrInvalidEnvelope)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if _, already := a.seen[e.IdempotencyKey]; already {
		return nil
	}
	a.seen[e.IdempotencyKey] = struct{}{}

	occurred := e.OccurredAt
	if occurred.IsZero() {
		occurred = e.PublishedAt
	}

	for _, p := range []Period{PeriodHour, PeriodDay, PeriodWeek, PeriodMonth} {
		start, err := p.truncate(occurred)
		if err != nil {
			return err
		}
		k := bucketKey{TenantID: e.TenantID, Period: p, PeriodStart: start}
		b, ok := a.buckets[k]
		if !ok {
			b = &Bucket{
				TenantID:    e.TenantID,
				Period:      p,
				PeriodStart: start,
				Counts:      make(map[string]int),
			}
			a.buckets[k] = b
		}
		b.Counts[counterKey(e.Domain, e.Aggregate, e.EventType)]++
	}
	return nil
}

// QueryParams scopes a Query call.
type QueryParams struct {
	TenantID string
	Period   Period
	At       time.Time
}

// Query returns the bucket containing At for (TenantID, Period). An empty
// bucket (no events ingested for that key) is returned with empty Counts.
func (a *Aggregator) Query(p QueryParams) (Bucket, error) {
	if strings.TrimSpace(p.TenantID) == "" {
		return Bucket{}, fmt.Errorf("tenant_id required")
	}
	start, err := p.Period.truncate(p.At)
	if err != nil {
		return Bucket{}, err
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	k := bucketKey{TenantID: p.TenantID, Period: p.Period, PeriodStart: start}
	if b, ok := a.buckets[k]; ok {
		// defensive copy to avoid handing out a mutable internal map
		out := Bucket{
			TenantID:    b.TenantID,
			Period:      b.Period,
			PeriodStart: b.PeriodStart,
			Counts:      make(map[string]int, len(b.Counts)),
		}
		for k, v := range b.Counts {
			out.Counts[k] = v
		}
		return out, nil
	}
	return Bucket{
		TenantID:    p.TenantID,
		Period:      p.Period,
		PeriodStart: start,
		Counts:      map[string]int{},
	}, nil
}
