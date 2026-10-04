// protodecode_test verifies binary-protobuf decode + JSON fallback for the 6
// chora-observability inbound topics.
package protodecode_test

import (
	"encoding/json"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"

	commonv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/common/v1"
	consumptionv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/consumption/v1"
	tenancyv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/tenancy/v1"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/events/protodecode"
)

func TestDecode_ExpAwarded_Binary(t *testing.T) {
	t0 := time.Date(2026, 5, 16, 9, 30, 0, 0, time.UTC)
	msg := &consumptionv1.CompanionExpAwarded{
		Envelope: &commonv1.EventEnvelope{
			EventId:       "01971a90-0000-7000-8000-000000000abc",
			TenantId:      "tenant-acme",
			Gcid:          "gcid-phyllis",
			OccurredAt:    timestamppb.New(t0),
			SchemaVersion: 1,
		},
		CompanionId: "fam-1",
		OwnerGcid:   "gcid-phyllis",
		ExpDelta:    3,
		Source:      "atom_session",
	}
	bz, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	got, err := protodecode.DecodePayloadMap("chora.consumption.familiar.exp_awarded.v1", bz)
	if err != nil {
		t.Fatalf("DecodePayloadMap: %v", err)
	}
	if v, _ := got["familiar_id"].(string); v != "fam-1" {
		t.Errorf("familiar_id = %q", v)
	}
	if v, _ := got["exp_source"].(string); v != "atom_session" {
		t.Errorf("exp_source = %q, expected atom_session (proto.source mapped)", v)
	}
	if v, _ := got["tenant_id"].(string); v != "tenant-acme" {
		t.Errorf("tenant_id from envelope = %q", v)
	}
}

func TestDecode_ExpAwarded_JSONFallback(t *testing.T) {
	payload := map[string]any{
		"familiar_id": "fam-2",
		"owner_gcid":  "gcid-x",
		"exp_delta":   2,
		"exp_source":  "atom_session",
	}
	bz, _ := json.Marshal(payload)
	got, err := protodecode.DecodePayloadMap("chora.consumption.familiar.exp_awarded.v1", bz)
	if err != nil {
		t.Fatalf("json fallback: %v", err)
	}
	if v, _ := got["familiar_id"].(string); v != "fam-2" {
		t.Errorf("familiar_id = %q", v)
	}
	if v, _ := got["exp_source"].(string); v != "atom_session" {
		t.Errorf("exp_source = %q", v)
	}
}

func TestDecode_EmptyPayload_FailsLoud(t *testing.T) {
	if _, err := protodecode.DecodePayloadMap("chora.consumption.familiar.exp_awarded.v1", nil); err == nil {
		t.Fatal("expected error on nil")
	}
}

func TestDecode_UnknownTopic_FallsBackToJSON(t *testing.T) {
	bz, _ := json.Marshal(map[string]any{"foo": "bar"})
	got, err := protodecode.DecodePayloadMap("chora.unknown.x.v1", bz)
	if err != nil {
		t.Fatalf("unknown topic: %v", err)
	}
	if v, _ := got["foo"].(string); v != "bar" {
		t.Errorf("foo = %q", v)
	}
}

// -----------------------------------------------------------------------------
// Debt #3 — JSON-fallback envelope-from-attributes fix
// -----------------------------------------------------------------------------

// TestDecodeWithAttrs_JSONPayload_AttrsPopulateEnvelope asserts the JSON
// fallback path is enriched with attribute-sourced envelope fields.
func TestDecodeWithAttrs_JSONPayload_AttrsPopulateEnvelope(t *testing.T) {
	payload := map[string]any{
		"familiar_id": "fam-2",
		"owner_gcid":  "gcid-x",
		"exp_delta":   2,
		"exp_source":  "atom_session",
	}
	bz, _ := json.Marshal(payload)
	attrs := map[string]string{
		"event_id":    "01971a90-0000-7000-8000-attrs-source",
		"tenant_id":   "tenant-acme",
		"gcid":        "gcid-phyllis",
		"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		"topic":       "chora.consumption.familiar.exp_awarded.v1",
		"occurred_at": "2026-05-16T09:30:00Z",
	}
	got, err := protodecode.DecodePayloadMapWithAttrs("chora.consumption.familiar.exp_awarded.v1", bz, attrs)
	if err != nil {
		t.Fatalf("DecodePayloadMapWithAttrs: %v", err)
	}
	if v, _ := got["event_id"].(string); v != "01971a90-0000-7000-8000-attrs-source" {
		t.Errorf("event_id = %q (want attrs-sourced)", v)
	}
	if v, _ := got["tenant_id"].(string); v != "tenant-acme" {
		t.Errorf("tenant_id = %q", v)
	}
	if v, _ := got["owner_gcid"].(string); v != "gcid-phyllis" {
		t.Errorf("owner_gcid = %q (want attrs.gcid)", v)
	}
	if v, _ := got["traceparent"].(string); v == "" {
		t.Errorf("traceparent missing from JSON+attrs path")
	}
	if v, _ := got["exp_source"].(string); v != "atom_session" {
		t.Errorf("exp_source = %q (JSON body field)", v)
	}
}

// TestDecodeWithAttrs_BinaryPayload_EnvelopeWinsOverAttrs asserts binary is
// canonical.
func TestDecodeWithAttrs_BinaryPayload_EnvelopeWinsOverAttrs(t *testing.T) {
	t0 := time.Date(2026, 5, 16, 9, 30, 0, 0, time.UTC)
	msg := &consumptionv1.CompanionExpAwarded{
		Envelope: &commonv1.EventEnvelope{
			EventId:       "01971a90-0000-7000-8000-binary-CANON",
			TenantId:      "tenant-from-binary",
			Gcid:          "gcid-from-binary",
			OccurredAt:    timestamppb.New(t0),
			SchemaVersion: 1,
		},
		CompanionId: "fam-1",
		OwnerGcid:   "gcid-from-binary",
		ExpDelta:    3,
		Source:      "atom_session",
	}
	bz, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	attrs := map[string]string{
		"event_id":  "DIFFERENT-event-id-from-attrs",
		"tenant_id": "tenant-from-attrs",
		"gcid":      "gcid-from-attrs",
	}
	got, err := protodecode.DecodePayloadMapWithAttrs("chora.consumption.familiar.exp_awarded.v1", bz, attrs)
	if err != nil {
		t.Fatalf("DecodePayloadMapWithAttrs binary: %v", err)
	}
	if v, _ := got["event_id"].(string); v != "01971a90-0000-7000-8000-binary-CANON" {
		t.Errorf("event_id = %q, expected binary canonical to win", v)
	}
	if v, _ := got["tenant_id"].(string); v != "tenant-from-binary" {
		t.Errorf("tenant_id = %q, expected binary canonical", v)
	}
}

// TestDecodeWithAttrs_BinaryPayload_EmptyAttrs ensures binary path still
// works with nil/empty attrs.
func TestDecodeWithAttrs_BinaryPayload_EmptyAttrs(t *testing.T) {
	t0 := time.Date(2026, 5, 16, 9, 30, 0, 0, time.UTC)
	msg := &consumptionv1.CompanionExpAwarded{
		Envelope: &commonv1.EventEnvelope{
			EventId:       "01971a90-0000-7000-8000-onlybinary",
			TenantId:      "tenant-acme",
			Gcid:          "gcid-phyllis",
			OccurredAt:    timestamppb.New(t0),
			SchemaVersion: 1,
		},
		CompanionId: "fam-1",
		OwnerGcid:   "gcid-phyllis",
		ExpDelta:    1,
	}
	bz, _ := proto.Marshal(msg)
	got, err := protodecode.DecodePayloadMapWithAttrs("chora.consumption.familiar.exp_awarded.v1", bz, nil)
	if err != nil {
		t.Fatalf("DecodePayloadMapWithAttrs binary nil attrs: %v", err)
	}
	if v, _ := got["event_id"].(string); v != "01971a90-0000-7000-8000-onlybinary" {
		t.Errorf("event_id = %q (binary envelope must populate)", v)
	}
}

// TestDecodeWithAttrs_EmptyPayload_FailsLoud ensures empty fails loud.
func TestDecodeWithAttrs_EmptyPayload_FailsLoud(t *testing.T) {
	if _, err := protodecode.DecodePayloadMapWithAttrs("chora.consumption.familiar.exp_awarded.v1", nil, map[string]string{"event_id": "x"}); err == nil {
		t.Fatal("expected error on nil payload")
	}
}

// -----------------------------------------------------------------------------
// Coverage tests for the 5 unexercised projectors (StageUp, BreedRevealed,
// SourceRevelation, EggPurchased, EggPaymentSucceeded).
// -----------------------------------------------------------------------------

func TestDecode_FamiliarStageUp_Binary(t *testing.T) {
	t0 := time.Date(2026, 5, 16, 9, 30, 0, 0, time.UTC)
	msg := &consumptionv1.CompanionStageUp{
		Envelope: &commonv1.EventEnvelope{
			EventId: "01971a90-0000-7000-8000-stage", TenantId: "tenant-acme",
			Gcid: "gcid-phyllis", OccurredAt: timestamppb.New(t0), SchemaVersion: 1,
		},
		CompanionId: "fam-1", OwnerGcid: "gcid-phyllis",
		StageFrom: 3, StageTo: 4,
	}
	bz, _ := proto.Marshal(msg)
	got, err := protodecode.DecodePayloadMap("chora.consumption.familiar.stage_up.v1", bz)
	if err != nil {
		t.Fatalf("DecodePayloadMap stage_up: %v", err)
	}
	if v, _ := got["stage_to"].(int); v != 4 {
		t.Errorf("stage_to = %v", got["stage_to"])
	}
}

func TestDecode_FamiliarBreedRevealed_Binary(t *testing.T) {
	t0 := time.Date(2026, 5, 16, 9, 30, 0, 0, time.UTC)
	msg := &consumptionv1.CompanionBreedRevealed{
		Envelope: &commonv1.EventEnvelope{
			EventId: "01971a90-0000-7000-8000-breed", TenantId: "tenant-acme",
			Gcid: "gcid-phyllis", OccurredAt: timestamppb.New(t0), SchemaVersion: 1,
		},
		CompanionId: "fam-1", OwnerGcid: "gcid-phyllis",
		ShinyVariant: true, Rarity: "rare", EggSku: "egg-1", RolledProbability: 0.07,
	}
	bz, _ := proto.Marshal(msg)
	got, err := protodecode.DecodePayloadMap("chora.consumption.familiar.breed_revealed.v1", bz)
	if err != nil {
		t.Fatalf("DecodePayloadMap breed_revealed: %v", err)
	}
	if v, _ := got["rarity"].(string); v != "rare" {
		t.Errorf("rarity = %q", v)
	}
	if v, _ := got["shiny"].(bool); !v {
		t.Errorf("shiny = %v (observability projector uses 'shiny' key)", v)
	}
}

// TestDecode_FamiliarHatched_Binary covers the new binary decoder + projector
// for chora.consumption.familiar.hatched.v1 added in Fix-D 2026-05-16.
func TestDecode_FamiliarHatched_Binary(t *testing.T) {
	t0 := time.Date(2026, 5, 16, 9, 30, 0, 0, time.UTC)
	msg := &consumptionv1.CompanionHatched{
		Envelope: &commonv1.EventEnvelope{
			EventId: "01971a90-0000-7000-8000-hatch", TenantId: "tenant-acme",
			Gcid: "gcid-phyllis", OccurredAt: timestamppb.New(t0), SchemaVersion: 1,
		},
		CompanionId:    "fam-1",
		OwnerGcid:      "gcid-phyllis",
		DisplayName:    "Spark",
		Tone:           "encouraging",
		LearnerPersona: "curious-explorer",
		Specialization: "math",
		ShinyVariant:   true,
		HatchedAt:      timestamppb.New(t0),
	}
	bz, _ := proto.Marshal(msg)
	got, err := protodecode.DecodePayloadMap("chora.consumption.familiar.hatched.v1", bz)
	if err != nil {
		t.Fatalf("DecodePayloadMap hatched: %v", err)
	}
	if v, _ := got["familiar_id"].(string); v != "fam-1" {
		t.Errorf("familiar_id = %q", v)
	}
	if v, _ := got["owner_gcid"].(string); v != "gcid-phyllis" {
		t.Errorf("owner_gcid = %q", v)
	}
	if v, _ := got["tenant_id"].(string); v != "tenant-acme" {
		t.Errorf("tenant_id = %q (envelope-sourced)", v)
	}
}

func TestDecode_FamiliarSourceRevelation_Binary(t *testing.T) {
	t0 := time.Date(2026, 5, 16, 9, 30, 0, 0, time.UTC)
	msg := &consumptionv1.CompanionSourceRevelation{
		Envelope: &commonv1.EventEnvelope{
			EventId: "01971a90-0000-7000-8000-rev", TenantId: "tenant-acme",
			Gcid: "gcid-phyllis", OccurredAt: timestamppb.New(t0), SchemaVersion: 1,
		},
		CompanionId: "fam-1", OwnerGcid: "gcid-phyllis",
		WindowDurationSeconds: 172800,
	}
	bz, _ := proto.Marshal(msg)
	got, err := protodecode.DecodePayloadMap("chora.consumption.familiar.source_revelation.v1", bz)
	if err != nil {
		t.Fatalf("DecodePayloadMap source_revelation: %v", err)
	}
	if v, _ := got["window_duration_seconds"].(int); v != 172800 {
		t.Errorf("window_duration_seconds = %v", got["window_duration_seconds"])
	}
}

func TestDecode_FamiliarEggPurchased_Binary(t *testing.T) {
	t0 := time.Date(2026, 5, 16, 9, 30, 0, 0, time.UTC)
	msg := &consumptionv1.CompanionEggPurchased{
		Envelope: &commonv1.EventEnvelope{
			EventId: "01971a90-0000-7000-8000-egg", TenantId: "tenant-acme",
			Gcid: "gcid-phyllis", OccurredAt: timestamppb.New(t0), SchemaVersion: 1,
		},
		CompanionId: "fam-1", OwnerGcid: "gcid-phyllis",
		EggSku: "egg-mystic", Source: "stripe",
	}
	bz, _ := proto.Marshal(msg)
	got, err := protodecode.DecodePayloadMap("chora.consumption.familiar.egg_purchased.v1", bz)
	if err != nil {
		t.Fatalf("DecodePayloadMap egg_purchased: %v", err)
	}
	if v, _ := got["egg_sku"].(string); v != "egg-mystic" {
		t.Errorf("egg_sku = %q", v)
	}
	if v, _ := got["purchase_source"].(string); v != "stripe" {
		t.Errorf("purchase_source = %q (proto.source mapped)", v)
	}
}

func TestDecode_TenancyFamiliarEggPaymentSucceeded_Binary(t *testing.T) {
	t0 := time.Date(2026, 5, 16, 9, 30, 0, 0, time.UTC)
	msg := &tenancyv1.CompanionEggPaymentSucceeded{
		Envelope: &commonv1.EventEnvelope{
			EventId: "01971a90-0000-7000-8000-pay", TenantId: "tenant-acme",
			Gcid: "gcid-phyllis", OccurredAt: timestamppb.New(t0), SchemaVersion: 1,
		},
		EggSku: "egg-mystic", AmountCentsPaid: 999, Currency: "SGD",
	}
	bz, _ := proto.Marshal(msg)
	got, err := protodecode.DecodePayloadMap("chora.tenancy.familiar_egg.payment_succeeded.v1", bz)
	if err != nil {
		t.Fatalf("DecodePayloadMap payment_succeeded: %v", err)
	}
	if v, _ := got["currency"].(string); v != "SGD" {
		t.Errorf("currency = %q", v)
	}
}
