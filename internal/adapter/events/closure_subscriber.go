// ClosureSubscriber for the federated account-closure saga (Tier 3 D11).
//
// Receives chora.observability.pii.pseudonymise.requested.v1 from the
// chora-closure-orchestrator. Applies pseudonymisation per the per-domain
// PII_Closure_Map.yaml and emits chora.observability.account.pseudonymised.v1
// (or pii.pseudonymise.failed.v1 on compensation path).
//
// Hexagonal:
//   - INBOUND ADAPTER from Pub/Sub
//   - depends on a small ClosurePublisher port (not the full events.Publisher)
//   - depends on a small ClosureRepository port (this service's DB only —
//     cross-DB queries forbidden per ddd-enforcement.md HARD RULE)
//
// PII fields per chora-observability/config/PII_Closure_Map.yaml:
//   - token_usage_ledger: subject_display_name (cost columns preserved for billing)
//   - agent_decision_log: subject_display_name (decision facts preserved — IMDA D4 evidence)
//   - trace_correlation_index: subject_display_name (trace_id preserved for OTLP)
//   - budgets: tenant_admin_display_name
//   - mana_ledger: subject_display_name (per-user ledger cascades)
//   - cost_anomaly_alerts: subject_display_name
//
// Cost ledger reconciliation works post-closure (numeric columns intact).
package events

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/5007-Capstone/chora/libs/chora-go-common/idempotent"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/config"
)

// Topic constants for the federated closure saga, observability domain.
const (
	TopicPseudonymiseRequested = "chora.observability.pii.pseudonymise.requested.v1"
	// TopicPseudonymiseCompleted is the taxonomy-correct
	// chora.{domain}.{aggregate}.{event_type} ack name (CHO-1719) — aligned
	// with the Terraform closure-orchestrator module IAM + the
	// account-pseudonymised-v1.yaml AsyncAPI contract.
	TopicPseudonymiseCompleted = "chora.observability.account.pseudonymised.v1"
	TopicPseudonymiseFailed    = "chora.observability.pii.pseudonymise.failed.v1"
)
// InboxTTL is the dedupe-key retention window for closure_subscriber's
// inbox. 24h covers Pub/Sub max redelivery window (7d default) reduced
// for the closure saga's typical end-to-end latency.
const InboxTTL = 24 * time.Hour


// PseudonymiseRequestedPayload mirrors the per-domain fan-out payload from
// the closure orchestrator's PseudonymiseRequested event.
type PseudonymiseRequestedPayload struct {
	SagaID      string `json:"saga_id"`
	Gcid        string `json:"gcid"`
	TenantID    string `json:"tenant_id"`
	SubjectKind string `json:"subject_kind,omitempty"` // "gcid" (default) | "agid"
	Traceparent string `json:"traceparent,omitempty"`
	Tracestate  string `json:"tracestate,omitempty"`
}

// ClosurePublisher is the port the subscriber uses to emit completion /
// failure acks back to the saga.
//
// Production: a ClosurePub adapter (M12) that wraps the local events
// package's Publisher and projects the payload onto the canonical
// envelope.Envelope.
//
// Tests: an in-process recorder (see InMemoryClosurePublisher).
type ClosurePublisher interface {
	Publish(topic string, tenantID, gcid, traceparent string, payload map[string]interface{}) error
}

// ClosureRepository is the local-domain port the subscriber uses to apply
// pseudonymisation. Implementations operate ONLY on the service's own DB.
type ClosureRepository interface {
	// Pseudonymise applies the provided fields to all rows owned by the
	// given GCID inside the tenant. Returns the number of rows touched.
	// Implementation MUST be idempotent.
	Pseudonymise(ctx context.Context, tenantID, gcid string, spec []config.TableSpec) (int, error)

	// IsPseudonymised reports whether the (tenantID, gcid) pair has already
	// been processed.
	//
	// Widened (W0-F5 error-honesty, CHO-2198) from the original bare
	// `IsPseudonymised(gcid string) bool`: that signature carried no
	// context.Context and had no way to report a backing-store failure, so
	// a durable (pg) implementation would have been forced to swallow a
	// real DB error into a false "not yet pseudonymised" result — the
	// exact swallowed-error trap this saga cannot afford (a guard read
	// that fails OPEN reads as "safe to reprocess" to any future caller).
	// tenantID was also added: pseudonymisation state is tenant-scoped
	// (RLS, like every other table in this domain's DB), matching
	// Pseudonymise's existing (tenantID, gcid) shape. Every caller MUST
	// treat a non-nil error as "unknown" — never coerce it to false.
	IsPseudonymised(ctx context.Context, tenantID, gcid string) (bool, error)
}

// ClosureSubscriber processes pseudonymise.requested.v1 messages from the
// federated closure saga.
type ClosureSubscriber struct {
	repo ClosureRepository
	pub  ClosurePublisher
	pii  *config.PIIClosureMap

	inbox idempotent.Store
	ttl   time.Duration

	mu       sync.Mutex
}

// NewClosureSubscriber wires the local repo + publisher + per-domain PII map.
func NewClosureSubscriber(repo ClosureRepository, pub ClosurePublisher, pii *config.PIIClosureMap, inbox idempotent.Store) *ClosureSubscriber {
	if inbox == nil {
		inbox = idempotent.NewMemoryStore()
	}
	return &ClosureSubscriber{
		repo:     repo,
		pub:      pub,
		pii:      pii,
		inbox: inbox,
		ttl:   InboxTTL,
	}
}

// SubscribedTopic returns the inbound topic this subscriber binds to.
func (s *ClosureSubscriber) SubscribedTopic() string { return TopicPseudonymiseRequested }

// Handle processes one pseudonymise.requested.v1 message.
func (s *ClosureSubscriber) Handle(ctx context.Context, payload PseudonymiseRequestedPayload) error {
	if s == nil || s.repo == nil || s.pub == nil || s.pii == nil {
		return errors.New("events: ClosureSubscriber not initialised")
	}
	if err := validatePayload(payload); err != nil {
		return err
	}

	// Idempotency guard on (saga_id, gcid).
	key := payload.SagaID + ":" + payload.Gcid
	return s.inbox.Process(ctx, key, s.ttl, func() error {

	// AGID handling: skipped_agid_closure if domain does not apply.
	if strings.EqualFold(payload.SubjectKind, "agid") && !s.pii.AGIDApplicable {
		return s.publishCompleted(payload, 0, "skipped_agid_closure")
	}

	// Apply per-table tokenisation declared in PII_Closure_Map.yaml.
	count, err := s.repo.Pseudonymise(ctx, payload.TenantID, payload.Gcid, s.pii.FieldsToTokenize)
	if err != nil {
		_ = s.publishFailed(payload, err.Error())
		return fmt.Errorf("events: closure pseudonymise failed: %w", err)
	}
	return s.publishCompleted(payload, count, "ok")
	})
}

func (s *ClosureSubscriber) publishCompleted(payload PseudonymiseRequestedPayload, rowsTouched int, status string) error {
	return s.pub.Publish(
		TopicPseudonymiseCompleted,
		payload.TenantID,
		payload.Gcid,
		payload.Traceparent,
		map[string]interface{}{
			"saga_id":              payload.SagaID,
			"gcid":                 payload.Gcid,
			"tenant_id":            payload.TenantID,
			"domain":               "observability",
			"rows_touched":         rowsTouched,
			"status":               status,
			"completed_at":         time.Now().UTC().Format(time.RFC3339Nano),
			"chora_imda_dimension": "accountability",
			"imda_lifecycle_stage": "runtime",
		},
	)
}

func (s *ClosureSubscriber) publishFailed(payload PseudonymiseRequestedPayload, errMsg string) error {
	return s.pub.Publish(
		TopicPseudonymiseFailed,
		payload.TenantID,
		payload.Gcid,
		payload.Traceparent,
		map[string]interface{}{
			"saga_id":              payload.SagaID,
			"gcid":                 payload.Gcid,
			"tenant_id":            payload.TenantID,
			"domain":               "observability",
			"error":                errMsg,
			"failed_at":            time.Now().UTC().Format(time.RFC3339Nano),
			"chora_imda_dimension": "safety_and_robustness",
			"imda_lifecycle_stage": "runtime",
		},
	)
}

func validatePayload(p PseudonymiseRequestedPayload) error {
	if strings.TrimSpace(p.SagaID) == "" {
		return errors.New("events: closure payload saga_id required")
	}
	if strings.TrimSpace(p.Gcid) == "" {
		return errors.New("events: closure payload gcid required")
	}
	if strings.TrimSpace(p.TenantID) == "" {
		return errors.New("events: closure payload tenant_id required")
	}
	return nil
}

// -----------------------------------------------------------------------------
// InMemoryClosurePublisher — test double for ClosurePublisher.
// -----------------------------------------------------------------------------

// PublishedClosureEvent records one publish call.
type PublishedClosureEvent struct {
	Topic       string
	TenantID    string
	GCID        string
	Traceparent string
	Payload     map[string]interface{}
}

// InMemoryClosurePublisher is an in-memory ClosurePublisher for tests + MVP.
type InMemoryClosurePublisher struct {
	mu     sync.Mutex
	events []PublishedClosureEvent
}

// NewInMemoryClosurePublisher returns a fresh in-memory publisher.
func NewInMemoryClosurePublisher() *InMemoryClosurePublisher {
	return &InMemoryClosurePublisher{events: make([]PublishedClosureEvent, 0, 4)}
}

// Publish records the event in memory.
func (p *InMemoryClosurePublisher) Publish(topic, tenantID, gcid, traceparent string, payload map[string]interface{}) error {
	if strings.TrimSpace(topic) == "" {
		return errors.New("publisher: topic required")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, PublishedClosureEvent{
		Topic:       topic,
		TenantID:    tenantID,
		GCID:        gcid,
		Traceparent: traceparent,
		Payload:     payload,
	})
	return nil
}

// ClosureRecorded returns a defensive copy of all recorded events.
func (p *InMemoryClosurePublisher) ClosureRecorded() []PublishedClosureEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]PublishedClosureEvent, len(p.events))
	copy(out, p.events)
	return out
}

// ClosureRecordedByTopic returns the events matching the given topic.
func (p *InMemoryClosurePublisher) ClosureRecordedByTopic(topic string) []PublishedClosureEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]PublishedClosureEvent, 0)
	for _, e := range p.events {
		if e.Topic == topic {
			out = append(out, e)
		}
	}
	return out
}

// -----------------------------------------------------------------------------
// InMemoryClosureRepo — test double for ClosureRepository.
// -----------------------------------------------------------------------------

// InMemoryClosureRepo is an in-memory ClosureRepository for tests + MVP.
type InMemoryClosureRepo struct {
	mu          sync.Mutex
	subjects    map[string]bool
	pseudonymed map[string]bool
	failNext    bool
}

// NewInMemoryClosureRepo returns a fresh in-memory repo.
func NewInMemoryClosureRepo() *InMemoryClosureRepo {
	return &InMemoryClosureRepo{
		subjects:    make(map[string]bool),
		pseudonymed: make(map[string]bool),
	}
}

// SeedSubject registers a (tenant, gcid) pair that exists locally.
func (r *InMemoryClosureRepo) SeedSubject(_, gcid string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.subjects[gcid] = true
}

// closureKey composes the InMemoryClosureRepo dedup-map key. Tenant-scoped
// (fixed alongside the W0-F5 port widening, CHO-2198): the prior version
// keyed `pseudonymed` by gcid ALONE, so a GCID that is a member of two
// tenants would have its second tenant's Pseudonymise call incorrectly
// short-circuited as "already done" the moment the first tenant's call
// completed — a latent cross-tenant dedup collision. The durable pg
// adapter (internal/adapter/pg.ClosureRepository) was written correctly
// scoped from the start; this brings the in-memory fallback in line so the
// two implementations agree on what "already pseudonymised" means.
func closureKey(tenantID, gcid string) string { return tenantID + "|" + gcid }

// Pseudonymise marks the (tenantID, gcid) pair as pseudonymised. Idempotent.
func (r *InMemoryClosureRepo) Pseudonymise(_ context.Context, tenantID, gcid string, spec []config.TableSpec) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failNext {
		r.failNext = false
		return 0, errors.New("inmem: forced failure")
	}
	key := closureKey(tenantID, gcid)
	if r.pseudonymed[key] {
		return 0, nil
	}
	r.pseudonymed[key] = true
	rows := 0
	for _, t := range spec {
		rows += len(t.Columns)
	}
	return rows, nil
}

// IsPseudonymised reports whether the (tenantID, gcid) pair has been
// pseudonymised. Signature widened per W0-F5 (CHO-2198) — see the
// ClosureRepository doc comment. The in-memory store cannot itself fail,
// so it always returns a nil error; the pg adapter is where the widened
// error return earns its keep.
func (r *InMemoryClosureRepo) IsPseudonymised(_ context.Context, tenantID, gcid string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pseudonymed[closureKey(tenantID, gcid)], nil
}

// SetFailNext makes the next Pseudonymise call return an error.
func (r *InMemoryClosureRepo) SetFailNext(b bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failNext = b
}

// -----------------------------------------------------------------------------
// Bootstrap helper for cmd/server/main.go
// -----------------------------------------------------------------------------

// BootstrapClosureSubscriber loads the per-domain PII_Closure_Map.yaml from
// the given path and builds a fully-wired ClosureSubscriber against the
// supplied repo + publisher. Returns an error if the map fails to load or
// validate.
//
// Caller is responsible for binding the subscriber to its Pub/Sub source
// (M12+: cgcpubsub.CloudSubscriber). In dev, the in-memory bus calls
// Handle directly.
func BootstrapClosureSubscriber(piiMapPath string, repo ClosureRepository, pub ClosurePublisher, inbox idempotent.Store) (*ClosureSubscriber, error) {
	pii, err := loadPIIMap(piiMapPath)
	if err != nil {
		return nil, err
	}
	return NewClosureSubscriber(repo, pub, pii, inbox), nil
}

// loadPIIMap is a thin shim around config.LoadFromFile to keep the events
// package free of direct config import in tests that don't need it. Defined
// inline to dodge an import cycle when subpackages embed events.
func loadPIIMap(path string) (*config.PIIClosureMap, error) {
	return config.LoadFromFile(path)
}
