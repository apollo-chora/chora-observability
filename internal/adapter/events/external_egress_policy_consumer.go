// external_egress_policy_consumer.go — projects
// chora.tenancy.external_egress_policy.updated.v1 into the model-gateway's
// fail-closed read-copy (CHO-2148).
//
// chora-tenancy owns the external web-egress entitlement. The model-gateway
// enforces it on every grounded call, but it reads from
// chora_observability.external_egress_policy — a different database. Cross-DB
// writes are forbidden, so THIS SUBSCRIBER IS THE ONLY BRIDGE. If it drops an
// event, the gateway keeps serving a stale entitlement with nothing to alert on.
//
// Delivery semantics, and why each matters here:
//
//   - AT-LEAST-ONCE → the inbox dedupes on event_id, so a redelivery cannot
//     re-apply a change.
//   - NOT ORDER-PRESERVING → the repository's monotonic `version` guard discards
//     any event that is not strictly newer. A late "egress ON" must never
//     overwrite a newer "egress OFF" and resurrect a revoked entitlement.
//     A discarded-as-stale event is ACKed (nil error): it is a normal outcome,
//     not a fault, and NACKing it would poison-loop a valid-but-late redelivery
//     into the DLQ.
//   - MALFORMED → fail loud (error → NACK → retry → DLQ). Never silently drop.
package events

import (
	"context"
	"fmt"
	"log"

	"github.com/5007-Capstone/chora/libs/chora-go-common/idempotent"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/externalegress"
)

// TopicExternalEgressPolicyUpdated is the source topic (chora-tenancy publishes).
const TopicExternalEgressPolicyUpdated = "chora.tenancy.external_egress_policy.updated.v1"

// ExternalEgressPolicyConsumerConfig wires the projection subscriber.
type ExternalEgressPolicyConsumerConfig struct {
	// Repo projects into external_egress_policy + the audit trail (local DB only).
	Repo externalegress.Repository

	// Inbox gives exactly-once subscriber semantics keyed on event_id.
	Inbox idempotent.Store
}

// ExternalEgressPolicyConsumer projects tenant egress-policy changes.
type ExternalEgressPolicyConsumer struct {
	repo  externalegress.Repository
	inbox idempotent.Store
}

// NewExternalEgressPolicyConsumer constructs the consumer. Panics on a missing
// dependency so a wiring bug fails loud at boot rather than silently dropping
// every egress change at runtime.
func NewExternalEgressPolicyConsumer(cfg ExternalEgressPolicyConsumerConfig) *ExternalEgressPolicyConsumer {
	if cfg.Repo == nil {
		panic("events.NewExternalEgressPolicyConsumer: nil Repo")
	}
	if cfg.Inbox == nil {
		panic("events.NewExternalEgressPolicyConsumer: nil Inbox")
	}
	return &ExternalEgressPolicyConsumer{repo: cfg.Repo, inbox: cfg.Inbox}
}

// Handle projects one policy change.
func (c *ExternalEgressPolicyConsumer) Handle(ctx context.Context, ev externalegress.PolicyChanged) error {
	if err := ev.Validate(); err != nil {
		// FAIL LOUD → NACK → retry → DLQ. A malformed egress event is a producer
		// bug; swallowing it would diverge the gateway from the tenant's decision.
		return fmt.Errorf("external_egress_policy_consumer: invalid event: %w", err)
	}

	return c.inbox.Process(ctx, "external_egress_policy_updated:"+ev.EventID, externalegress.InboxTTL, func() error {
		applied, err := c.repo.Project(ctx, ev)
		if err != nil {
			// Not claimed by the inbox (Process only records the key on success),
			// so the broker's retry re-runs the write.
			return fmt.Errorf("external_egress_policy_consumer: project tenant=%s version=%d: %w",
				ev.TenantID, ev.Version, err)
		}
		if !applied {
			// Out-of-order redelivery: the projection already holds a version at
			// least as new. Discarding is the CORRECT outcome — ACK it.
			log.Printf(
				"observability: external_egress projection SKIPPED stale event "+
					"(tenant=%s version=%d event_id=%s) — a newer policy is already projected",
				ev.TenantID, ev.Version, ev.EventID,
			)
		}
		return nil
	})
}
