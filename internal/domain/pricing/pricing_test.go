// Package pricing_test exercises the pricing-config loader.
//
// Tests assert the canonical YAML at services/chora-observability/config/
// pricing.yaml parses to the expected schema and that the cost calculator
// is deterministic for managed models + GPU-amortized for self-hosted.
package pricing_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apollo-chora/chora-observability/internal/domain/pricing"
)

func TestLoadCanonicalPricingYAML(t *testing.T) {
	t.Parallel()

	// Resolve relative to the test file so it works under any cwd.
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	yamlPath := filepath.Join(wd, "..", "..", "..", "config", "pricing.yaml")

	cfg, err := pricing.LoadFile(yamlPath)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if cfg.Version == "" {
		t.Errorf("version empty")
	}
	if cfg.EffectiveFrom.IsZero() {
		t.Errorf("effective_from zero")
	}

	// Spot-check a managed model.
	//
	// CHO-2220: this keyed off "vertex_ai/gemini-2.5-flash" and asserted
	// $0.15/1M input. Both were wrong, and being wrong together is why it stayed
	// green: no producer has ever emitted a provider-qualified model id (agents
	// send the bare logical_model_id), so the prefixed key existed only in
	// fixtures like this one — every production lookup missed and the O+ cost
	// column was blank for every model. The rate was Gemini 2.5 Flash's pre-GA
	// price. Key by what the wire carries; take rates from the Billing Catalog.
	gemini, ok := cfg.Prices["gemini-2.5-flash"]
	if !ok {
		t.Fatal("gemini-2.5-flash missing — the key must be the BARE model id " +
			"production sends, not a {provider}/{model} form nothing emits")
	}
	if gemini.InputPer1k != 0.00030 { // SKU: Gemini 2.5 Flash GA Text Input, $0.30/1M
		t.Errorf("gemini input price = %v; want 0.00030", gemini.InputPer1k)
	}
	if gemini.OutputPer1k != 0.00250 { // SKU: Gemini 2.5 Flash GA Text Output, $2.50/1M
		t.Errorf("gemini output price = %v; want 0.00250", gemini.OutputPer1k)
	}

	// Spot-check a self-hosted model is amortized.
	gemma, ok := cfg.Prices["gemma-4-9b-instruct"]
	if !ok {
		t.Fatal("gemma-4-9b-instruct missing")
	}
	if gemma.PricingModel != "gpu_hours_amortized" {
		t.Errorf("gemma pricing_model = %q; want gpu_hours_amortized", gemma.PricingModel)
	}
}

func TestComputeCostMicros_ManagedModel(t *testing.T) {
	t.Parallel()
	p := pricing.Price{InputPer1k: 0.00015, OutputPer1k: 0.00060, CachePer1k: 0.0000375}
	// 1000 input + 500 output + 0 cached.
	got, err := pricing.ComputeCostMicros(p, 1000, 500, 0)
	if err != nil {
		t.Fatalf("ComputeCostMicros: %v", err)
	}
	// 1k * 0.00015 + 0.5k * 0.00060 = 0.00015 + 0.00030 = 0.00045 USD = 450 micros.
	if got != 450 {
		t.Errorf("cost = %d micros; want 450", got)
	}
}

func TestComputeCostMicros_WithCachedDiscount(t *testing.T) {
	t.Parallel()
	p := pricing.Price{InputPer1k: 0.00015, OutputPer1k: 0.00060, CachePer1k: 0.0000375}
	// 1000 input (1000 cached, 0 fresh) + 500 output.
	got, err := pricing.ComputeCostMicros(p, 1000, 500, 1000)
	if err != nil {
		t.Fatalf("ComputeCostMicros: %v", err)
	}
	// 1k cached * 0.0000375 + 0.5k out * 0.00060 = 0.0000375 + 0.00030 = 0.0003375 USD = 337.5 micros => 337.
	if got != 337 {
		t.Errorf("cost = %d micros; want 337", got)
	}
}

func TestComputeCostMicros_PassThrough(t *testing.T) {
	t.Parallel()
	p := pricing.Price{PassThrough: true}
	got, err := pricing.ComputeCostMicros(p, 1000, 500, 0)
	if err != nil {
		t.Fatalf("pass-through: %v", err)
	}
	if got != 0 {
		t.Errorf("pass-through cost = %d; want 0", got)
	}
}

func TestComputeCostMicros_RejectsNegative(t *testing.T) {
	t.Parallel()
	p := pricing.Price{InputPer1k: 0.0001}
	_, err := pricing.ComputeCostMicros(p, -1, 0, 0)
	if err == nil {
		t.Error("expected error for negative tokens")
	}
}

func TestLoadFile_RejectsMissingFile(t *testing.T) {
	t.Parallel()
	_, err := pricing.LoadFile("/no/such/path.yaml")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestLoadFile_RejectsBadYAML(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	path := filepath.Join(tmp, "bad.yaml")
	if err := os.WriteFile(path, []byte("not\n  valid:\n yaml: ["), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, err := pricing.LoadFile(path)
	if err == nil {
		t.Error("expected error for malformed yaml")
	}
}

func TestLoadFile_RejectsMissingVersion(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	path := filepath.Join(tmp, "no-version.yaml")
	body := []byte("effective_from: 2026-05-09T00:00:00Z\nprices:\n  m1:\n    input_per_1k: 0.1\n")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, err := pricing.LoadFile(path)
	if err == nil {
		t.Error("expected error for missing version")
	}
}

func TestLoadFile_RejectsMissingEffectiveFrom(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	path := filepath.Join(tmp, "no-eff.yaml")
	body := []byte("version: \"1\"\nprices:\n  m1:\n    input_per_1k: 0.1\n")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, err := pricing.LoadFile(path)
	if err == nil {
		t.Error("expected error for missing effective_from")
	}
}

func TestComputeCostMicros_GPUAmortizedReturnsZero(t *testing.T) {
	t.Parallel()
	p := pricing.Price{PricingModel: "gpu_hours_amortized"}
	got, err := pricing.ComputeCostMicros(p, 100, 50, 0)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != 0 {
		t.Errorf("expected 0 (caller should use ComputeGPUCostMicros); got %d", got)
	}
}

func TestComputeCostMicros_RejectsCachedExceedsPrompt(t *testing.T) {
	t.Parallel()
	p := pricing.Price{InputPer1k: 0.1}
	_, err := pricing.ComputeCostMicros(p, 100, 50, 200)
	if err == nil {
		t.Error("expected error for cached > prompt")
	}
}

func TestComputeGPUCostMicros_Basic(t *testing.T) {
	t.Parallel()
	p := pricing.Price{
		PricingModel:        "gpu_hours_amortized",
		AmortizedPerGPUHour: 0.95,
	}
	// 3600 seconds = 1 GPU-hour @ $0.95 = 950000 micros
	got, err := pricing.ComputeGPUCostMicros(p, 3600)
	if err != nil {
		t.Fatalf("ComputeGPUCostMicros: %v", err)
	}
	if got != 950000 {
		t.Errorf("got %d; want 950000", got)
	}
}

func TestComputeGPUCostMicros_RejectsNegative(t *testing.T) {
	t.Parallel()
	p := pricing.Price{PricingModel: "gpu_hours_amortized", AmortizedPerGPUHour: 0.95}
	_, err := pricing.ComputeGPUCostMicros(p, -1)
	if err == nil {
		t.Error("expected error for negative seconds")
	}
}

func TestComputeGPUCostMicros_RejectsNonGPUModel(t *testing.T) {
	t.Parallel()
	p := pricing.Price{InputPer1k: 0.1}
	_, err := pricing.ComputeGPUCostMicros(p, 60)
	if err == nil {
		t.Error("expected error for non-gpu pricing model")
	}
}
