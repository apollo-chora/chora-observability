// Package protodecode decodes inbound Pub/Sub message bytes into a
// snake_case map[string]any compatible with the legacy json.Unmarshal flow.
//
// Why this package exists
// -----------------------
// Producer-side wave (task #33 / #38) flipped outbox payloads to binary
// protobuf on Schema-Registry-attached topics. The 5 ADR-149 familiar-growth
// topics chora-observability subscribes to are candidates for the same flip
// upstream; subscribers MUST accept both shapes — proto.Unmarshal first,
// json.Unmarshal fallback.
//
// Decode strategy
// ---------------
// 1. If the topic is registered for binary decoding (see binaryDecoders),
//    attempt proto.Unmarshal first. On success, project the proto message
//    into a snake_case map[string]any compatible with the legacy JSON shape.
// 2. On binary failure (or unregistered topic), fall back to json.Unmarshal.
// 3. On both-fail, return a wrapped error.
package protodecode

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/common/v1"
	consumptionv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/consumption/v1"
	tenancyv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/tenancy/v1"
)

// ErrEmptyPayload is returned when the inbound bytes are empty.
var ErrEmptyPayload = errors.New("protodecode: empty payload")

type binaryDecoder func(payload []byte) (proto.Message, error)
type projector func(msg proto.Message, out map[string]any)

var binaryDecoders = map[string]struct {
	decode  binaryDecoder
	project projector
}{
	"chora.consumption.familiar.exp_awarded.v1": {
		decode: func(payload []byte) (proto.Message, error) {
			var m consumptionv1.CompanionExpAwarded
			if err := proto.Unmarshal(payload, &m); err != nil {
				return nil, err
			}
			return &m, nil
		},
		project: projectExpAwarded,
	},
	"chora.consumption.familiar.stage_up.v1": {
		decode: func(payload []byte) (proto.Message, error) {
			var m consumptionv1.CompanionStageUp
			if err := proto.Unmarshal(payload, &m); err != nil {
				return nil, err
			}
			return &m, nil
		},
		project: projectStageUp,
	},
	"chora.consumption.familiar.breed_revealed.v1": {
		decode: func(payload []byte) (proto.Message, error) {
			var m consumptionv1.CompanionBreedRevealed
			if err := proto.Unmarshal(payload, &m); err != nil {
				return nil, err
			}
			return &m, nil
		},
		project: projectBreedRevealed,
	},
	"chora.consumption.familiar.hatched.v1": {
		decode: func(payload []byte) (proto.Message, error) {
			var m consumptionv1.CompanionHatched
			if err := proto.Unmarshal(payload, &m); err != nil {
				return nil, err
			}
			return &m, nil
		},
		project: projectHatched,
	},
	"chora.consumption.familiar.source_revelation.v1": {
		decode: func(payload []byte) (proto.Message, error) {
			var m consumptionv1.CompanionSourceRevelation
			if err := proto.Unmarshal(payload, &m); err != nil {
				return nil, err
			}
			return &m, nil
		},
		project: projectSourceRevelation,
	},
	"chora.consumption.familiar.egg_purchased.v1": {
		decode: func(payload []byte) (proto.Message, error) {
			var m consumptionv1.CompanionEggPurchased
			if err := proto.Unmarshal(payload, &m); err != nil {
				return nil, err
			}
			return &m, nil
		},
		project: projectEggPurchased,
	},
	"chora.tenancy.familiar_egg.payment_succeeded.v1": {
		decode: func(payload []byte) (proto.Message, error) {
			var m tenancyv1.CompanionEggPaymentSucceeded
			if err := proto.Unmarshal(payload, &m); err != nil {
				return nil, err
			}
			return &m, nil
		},
		project: projectEggPaymentSucceeded,
	},
}

// DecodePayloadMap decodes inbound Pub/Sub message bytes into a snake_case
// map[string]any. Binary protobuf preferred via per-topic decoders; falls
// back to json.Unmarshal for unregistered or transitioning topics.
//
// Use DecodePayloadMapWithAttrs when the caller has Pub/Sub msg.Attributes
// available — the publisher places envelope fields (event_id / tenant_id /
// gcid / traceparent) there, not in the payload body, so the JSON fallback
// path returns empty envelope fields without them.
func DecodePayloadMap(topic string, payload []byte) (map[string]any, error) {
	return DecodePayloadMapWithAttrs(topic, payload, nil)
}

// DecodePayloadMapWithAttrs decodes inbound Pub/Sub message bytes + the
// publisher-supplied msg.Attributes into a snake_case map[string]any.
//
// Field precedence (high → low):
//  1. Binary proto Envelope (when payload is binary-decodable for this topic)
//  2. Pub/Sub msg.Attributes (publisher's canonical envelope projection per
//     libs/chora-go-common/pubsub.envelopeAttributes)
//  3. JSON payload body
//
// Binary always wins because the producer flipped to binary AS the canonical
// wire shape (#33 / #38). Attributes win over the JSON body during the
// transition window. nil attrs ⇒ legacy DecodePayloadMap behaviour. Empty
// payload ⇒ ErrEmptyPayload regardless of attrs.
func DecodePayloadMapWithAttrs(topic string, payload []byte, attrs map[string]string) (map[string]any, error) {
	if len(payload) == 0 {
		return nil, ErrEmptyPayload
	}

	if entry, ok := binaryDecoders[topic]; ok {
		msg, err := entry.decode(payload)
		if err == nil && msg != nil && entry.project != nil {
			out := make(map[string]any)
			// Lay attrs down first; binary projection overrides them.
			mergeAttrsEnvelope(attrs, out)
			entry.project(msg, out)
			return out, nil
		}
		warnBinaryFallback(topic, err)
	} else {
		warnUnknownTopic(topic)
	}

	var body map[string]any
	if err := json.Unmarshal(payload, &body); err != nil {
		return nil, fmt.Errorf("protodecode: topic %q neither binary-decodable nor JSON-decodable: %w", topic, err)
	}
	out := make(map[string]any, len(body)+len(attrs))
	for k, v := range body {
		out[k] = v
	}
	// Attrs override the JSON body for envelope fields.
	mergeAttrsEnvelope(attrs, out)
	return out, nil
}

// mergeAttrsEnvelope projects Pub/Sub msg.Attributes onto the decoded map's
// canonical envelope keys. Empty / missing attrs are no-ops.
//
// Attribute key mapping (publisher → consumer field name):
//   - attrs["event_id"]      → out["event_id"]
//   - attrs["tenant_id"]     → out["tenant_id"]
//   - attrs["gcid"]          → out["owner_gcid"] (Familiar events use
//                              owner_gcid; gcid is the canonical envelope
//                              field name on the wire)
//   - attrs["traceparent"]   → out["traceparent"]
//   - attrs["tracestate"]    → out["tracestate"]
//   - attrs["occurred_at"]   → out["occurred_at"]
func mergeAttrsEnvelope(attrs map[string]string, out map[string]any) {
	if len(attrs) == 0 {
		return
	}
	if v := attrs["event_id"]; v != "" {
		out["event_id"] = v
	}
	if v := attrs["tenant_id"]; v != "" {
		out["tenant_id"] = v
	}
	if v := attrs["gcid"]; v != "" {
		out["owner_gcid"] = v
		if _, ok := out["gcid"]; !ok {
			out["gcid"] = v
		}
	}
	if v := attrs["traceparent"]; v != "" {
		out["traceparent"] = v
	}
	if v := attrs["tracestate"]; v != "" {
		out["tracestate"] = v
	}
	if v := attrs["occurred_at"]; v != "" {
		out["occurred_at"] = v
	}
	if v := attrs["published_at"]; v != "" {
		out["published_at"] = v
	}
	if v := attrs["chora_imda_dimension"]; v != "" {
		out["chora_imda_dimension"] = v
	}
	if v := attrs["imda_lifecycle_stage"]; v != "" {
		out["imda_lifecycle_stage"] = v
	}
}

// -----------------------------------------------------------------------------
// Projectors
// -----------------------------------------------------------------------------

func mergeEnvelope(env *commonv1.EventEnvelope, out map[string]any) {
	if env == nil {
		return
	}
	if v := env.GetEventId(); v != "" {
		out["event_id"] = v
	}
	if v := env.GetTenantId(); v != "" {
		out["tenant_id"] = v
	}
	if v := env.GetGcid(); v != "" {
		out["gcid"] = v
	}
	if v := env.GetTraceparent(); v != "" {
		out["traceparent"] = v
	}
	if t := env.GetOccurredAt(); t != nil {
		out["occurred_at"] = t.AsTime().UTC().Format(time.RFC3339Nano)
	}
	if v := env.GetChoraImdaDimension(); v != "" {
		out["chora_imda_dimension"] = v
	}
	if v := env.GetImdaLifecycleStage(); v != "" {
		out["imda_lifecycle_stage"] = v
	}
}

func projectExpAwarded(msg proto.Message, out map[string]any) {
	m, ok := msg.(*consumptionv1.CompanionExpAwarded)
	if !ok || m == nil {
		return
	}
	mergeEnvelope(m.GetEnvelope(), out)
	if v := m.GetCompanionId(); v != "" {
		out["familiar_id"] = v
	}
	if v := m.GetOwnerGcid(); v != "" {
		out["owner_gcid"] = v
	}
	if v := m.GetExpDelta(); v != 0 {
		out["exp_delta"] = int(v)
	}
	// Proto field `source` ↔ legacy JSON key `exp_source` per the audit
	// subscriber's existing handler.
	if v := m.GetSource(); v != "" {
		out["exp_source"] = v
	}
}

func projectStageUp(msg proto.Message, out map[string]any) {
	m, ok := msg.(*consumptionv1.CompanionStageUp)
	if !ok || m == nil {
		return
	}
	mergeEnvelope(m.GetEnvelope(), out)
	if v := m.GetCompanionId(); v != "" {
		out["familiar_id"] = v
	}
	if v := m.GetOwnerGcid(); v != "" {
		out["owner_gcid"] = v
	}
	if v := m.GetStageFrom(); v != 0 {
		out["stage_from"] = int(v)
	}
	if v := m.GetStageTo(); v != 0 {
		out["stage_to"] = int(v)
	}
}

func projectBreedRevealed(msg proto.Message, out map[string]any) {
	m, ok := msg.(*consumptionv1.CompanionBreedRevealed)
	if !ok || m == nil {
		return
	}
	mergeEnvelope(m.GetEnvelope(), out)
	if v := m.GetCompanionId(); v != "" {
		out["familiar_id"] = v
	}
	if v := m.GetOwnerGcid(); v != "" {
		out["owner_gcid"] = v
	}
	if v := m.GetSpecies(); v != 0 {
		out["species"] = v.String()
	}
	out["shiny"] = m.GetShinyVariant()
	if v := m.GetRarity(); v != "" {
		out["rarity"] = v
	}
	if v := m.GetEggSku(); v != "" {
		out["egg_sku"] = v
	}
	if v := m.GetRolledProbability(); v != 0 {
		out["rolled_probability"] = v
	}
}

func projectHatched(msg proto.Message, out map[string]any) {
	m, ok := msg.(*consumptionv1.CompanionHatched)
	if !ok || m == nil {
		return
	}
	mergeEnvelope(m.GetEnvelope(), out)
	if v := m.GetCompanionId(); v != "" {
		out["familiar_id"] = v
	}
	if v := m.GetOwnerGcid(); v != "" {
		out["owner_gcid"] = v
	}
	if v := m.GetDisplayName(); v != "" {
		out["display_name"] = v
	}
	if v := m.GetTone(); v != "" {
		out["tone"] = v
	}
	if v := m.GetLearnerPersona(); v != "" {
		out["learner_persona"] = v
	}
	if v := m.GetResonantAtomId(); v != "" {
		out["resonant_atom_id"] = v
	}
	if v := m.GetSpecialization(); v != "" {
		out["specialization"] = v
	}
	if v := m.GetSpecies(); v != 0 {
		out["species"] = v.String()
	}
	out["shiny"] = m.GetShinyVariant()
	if t := m.GetHatchedAt(); t != nil {
		out["hatched_at"] = t.AsTime().UTC().Format(time.RFC3339Nano)
	}
}

func projectSourceRevelation(msg proto.Message, out map[string]any) {
	m, ok := msg.(*consumptionv1.CompanionSourceRevelation)
	if !ok || m == nil {
		return
	}
	mergeEnvelope(m.GetEnvelope(), out)
	if v := m.GetCompanionId(); v != "" {
		out["familiar_id"] = v
	}
	if v := m.GetOwnerGcid(); v != "" {
		out["owner_gcid"] = v
	}
	if v := m.GetWindowDurationSeconds(); v != 0 {
		out["window_duration_seconds"] = int(v)
	}
}

func projectEggPurchased(msg proto.Message, out map[string]any) {
	m, ok := msg.(*consumptionv1.CompanionEggPurchased)
	if !ok || m == nil {
		return
	}
	mergeEnvelope(m.GetEnvelope(), out)
	if v := m.GetCompanionId(); v != "" {
		out["familiar_id"] = v
	}
	if v := m.GetOwnerGcid(); v != "" {
		out["owner_gcid"] = v
	}
	if v := m.GetEggSku(); v != "" {
		out["egg_sku"] = v
	}
	// Proto field `source` ↔ legacy JSON key `purchase_source` per the
	// audit subscriber's existing handler.
	if v := m.GetSource(); v != "" {
		out["purchase_source"] = v
	}
}

func projectEggPaymentSucceeded(msg proto.Message, out map[string]any) {
	m, ok := msg.(*tenancyv1.CompanionEggPaymentSucceeded)
	if !ok || m == nil {
		return
	}
	mergeEnvelope(m.GetEnvelope(), out)
	if v := m.GetEggSku(); v != "" {
		out["egg_sku"] = v
	}
	if v := m.GetAmountCentsPaid(); v != 0 {
		out["amount_cents"] = v
	}
	if v := m.GetCurrency(); v != "" {
		out["currency"] = v
	}
}

// -----------------------------------------------------------------------------
// One-shot WARN logging
// -----------------------------------------------------------------------------

var (
	warnedFallbackMu sync.Mutex
	warnedFallback   = map[string]bool{}

	warnedUnknownMu sync.Mutex
	warnedUnknown   = map[string]bool{}
)

func warnBinaryFallback(topic string, err error) {
	warnedFallbackMu.Lock()
	defer warnedFallbackMu.Unlock()
	if warnedFallback[topic] {
		return
	}
	warnedFallback[topic] = true
	log.Printf("WARN protodecode: topic %q registered for binary but binary unmarshal failed (%v) — falling back to JSON. Expected during producer-side flip; investigate if persistent.", topic, err)
}

func warnUnknownTopic(topic string) {
	warnedUnknownMu.Lock()
	defer warnedUnknownMu.Unlock()
	if warnedUnknown[topic] {
		return
	}
	warnedUnknown[topic] = true
	log.Printf("WARN protodecode: topic %q has no binary decoder registered — using JSON fallback. Add an entry to internal/adapter/events/protodecode/protodecode.go when the producer flips to binary.", topic)
}
