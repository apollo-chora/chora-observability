// Package pricing is the pricing-config loader + cost calculator for the
// TokenUsageLedger.
//
// Source-of-truth: ai-cost-tracking skill ("Pricing config (versioned YAML) —
// Pricing is data, not code"). The YAML is at
// services/chora-observability/config/pricing.yaml.
//
// The Model Gateway calls ComputeCostMicros at every LLM invocation BEFORE
// writing the ledger row, using the version active at occurred_at. The
// ledger row records pricing_version so retroactive price corrections can
// be reconciled by the nightly Cloud Run Job.
package pricing

import (
	"errors"
	"fmt"
	"math"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the parsed pricing-config YAML.
type Config struct {
	Version       string             `yaml:"version"`
	EffectiveFrom time.Time          `yaml:"effective_from"`
	Prices        map[string]Price   `yaml:"prices"`
	Reconciliation Reconciliation    `yaml:"reconciliation"`
}

// Price is one model's pricing. Either per-1k-token (managed) or
// gpu_hours_amortized (self-hosted) or pass_through (BYOA).
type Price struct {
	InputPer1k  float64 `yaml:"input_per_1k"`
	OutputPer1k float64 `yaml:"output_per_1k"`
	CachePer1k  float64 `yaml:"cache_per_1k"`

	PricingModel              string  `yaml:"pricing_model"`
	GPUClass                  string  `yaml:"gpu_class"`
	AmortizedPerGPUHour       float64 `yaml:"amortized_per_gpu_hour"`
	TokensPerGPUHourEstimate  int64   `yaml:"tokens_per_gpu_hour_estimate"`

	PassThrough bool `yaml:"pass_through"`
}

// Reconciliation captures the nightly job parameters.
type Reconciliation struct {
	Cron             string `yaml:"cron"`
	LookbackDays     int    `yaml:"lookback_days"`
	BigQueryDataset  string `yaml:"bigquery_dataset"`
	BigQueryTable    string `yaml:"bigquery_table"`
}

// LoadFile reads + parses the YAML at path.
func LoadFile(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, fmt.Errorf("yaml unmarshal: %w", err)
	}
	if cfg.Version == "" {
		return nil, errors.New("pricing yaml missing version")
	}
	if cfg.EffectiveFrom.IsZero() {
		return nil, errors.New("pricing yaml missing effective_from")
	}
	return &cfg, nil
}

// ComputeCostMicros returns the cost in int64 micros (1e-6 USD) for the
// given token counts.
//
// For managed models (input/output/cache per-1k):
//
//	cost = ((input - cached) * input_per_1k +
//	         cached         * cache_per_1k +
//	         output         * output_per_1k) * 1000_000 / 1000
//
// For self-hosted (gpu_hours_amortized): callers MUST use ComputeGPUCostMicros
// directly with measured GPU-seconds — this function returns 0 for those.
//
// For pass_through: returns 0 (tenant pays provider directly; we track tokens
// only for quota).
//
// Negative inputs are rejected.
func ComputeCostMicros(p Price, promptTokens, completionTokens, cachedTokens int64) (int64, error) {
	if promptTokens < 0 || completionTokens < 0 || cachedTokens < 0 {
		return 0, fmt.Errorf("negative token count: prompt=%d completion=%d cached=%d",
			promptTokens, completionTokens, cachedTokens)
	}
	if cachedTokens > promptTokens {
		return 0, fmt.Errorf("cached_tokens (%d) > prompt_tokens (%d)", cachedTokens, promptTokens)
	}
	if p.PassThrough {
		return 0, nil
	}
	if p.PricingModel == "gpu_hours_amortized" {
		// Caller should use ComputeGPUCostMicros with measured seconds; we
		// return 0 to signal "no per-token cost". (Tracking can use
		// p.AmortizedPerGPUHour / p.TokensPerGPUHourEstimate as a fallback
		// estimate if seconds aren't measured — that's a future enhancement.)
		return 0, nil
	}
	freshInput := promptTokens - cachedTokens
	costUSD := float64(freshInput)*p.InputPer1k/1000.0 +
		float64(cachedTokens)*p.CachePer1k/1000.0 +
		float64(completionTokens)*p.OutputPer1k/1000.0
	micros := int64(math.Trunc(costUSD * 1_000_000))
	if micros < 0 {
		return 0, fmt.Errorf("cost overflow")
	}
	return micros, nil
}

// ComputeGPUCostMicros returns the int64 micros cost of a self-hosted Gemma
// inference based on measured GPU-seconds.
func ComputeGPUCostMicros(p Price, gpuSeconds float64) (int64, error) {
	if gpuSeconds < 0 {
		return 0, fmt.Errorf("negative gpu_seconds: %v", gpuSeconds)
	}
	if p.PricingModel != "gpu_hours_amortized" {
		return 0, fmt.Errorf("ComputeGPUCostMicros only supports pricing_model=gpu_hours_amortized")
	}
	costUSD := gpuSeconds / 3600.0 * p.AmortizedPerGPUHour
	return int64(math.Trunc(costUSD * 1_000_000)), nil
}
