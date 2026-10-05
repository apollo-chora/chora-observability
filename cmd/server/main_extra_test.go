// main_extra_test.go — completes cmd/server coverage for the pure /
// env-driven helpers the composition root's exhaustive test suite skips:
// the quarantine logging + duplicate path, the nil-pool bootstrap fallbacks,
// the pricing-backed decision-cost calculator, and the agents registry
// loader. Nothing here opens a real DB / pubsub connection: every DSN-or-client
// path that would log.Fatalf (unreachable in a test process) is left untested.
package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/eventbus"
	obsoutbox "github.com/apollo-chora/chora-observability/internal/adapter/outbox"
)

// ---------------------------------------------------------------------------
// Quarantine helpers
// ---------------------------------------------------------------------------

func TestIsDuplicateHelper(t *testing.T) {
	t.Parallel()
	if !isDuplicate(obsoutbox.ErrDuplicateIdempotencyKey) {
		t.Error("isDuplicate(ErrDuplicateIdempotencyKey) = false; want true")
	}
	if isDuplicate(errors.New("boom")) {
		t.Error("isDuplicate(other) = true; want false")
	}
	if isDuplicate(nil) {
		t.Error("isDuplicate(nil) = true; want false")
	}
}

func TestTruncateStr(t *testing.T) {
	t.Parallel()
	short := "abc"
	if got := truncateStr(short, 10); got != "abc" {
		t.Errorf("truncateStr(short) = %q; want unchanged", got)
	}
	long := "abcdefghij"
	if got := truncateStr(long, 3); got != "abc" {
		t.Errorf("truncateStr(long,3) = %q; want abc", got)
	}
	if got := truncateStr(long, len(long)); got != long {
		t.Errorf("truncateStr(len) = %q; want unchanged", got)
	}
}

func TestDefaultQuarantineLogger(t *testing.T) {
	t.Parallel()
	var l defaultQuarantineLogger
	// These only log; they must not panic.
	l.Infof("info %d", 1)
	l.Warnf("warn %s", "x")
	l.Errorf("error %v", errors.New("boom"))
}

func TestQuarantineIdentity_SynthesisesWhenAttrsAbsent(t *testing.T) {
	t.Parallel()
	rowID, tenantID := quarantineIdentity("token_usage", eventbus.Message{})
	if rowID == "" {
		t.Error("expected synthesised row id")
	}
	if tenantID != "platform" {
		t.Errorf("tenant = %q; want platform", tenantID)
	}
	// Empty envelope carries no attrs.
	rowID, tenantID = quarantineIdentity("agent_decision", eventbus.Message{})
	if rowID == "" || tenantID != "platform" {
		t.Errorf("empty envelope -> (%q, %q)", rowID, tenantID)
	}
}

// dupStore rejects every Insert as a duplicate idempotency key.
type dupStore struct{}

func (dupStore) Insert(context.Context, obsoutbox.Row) error {
	return obsoutbox.ErrDuplicateIdempotencyKey
}
func (dupStore) FetchPending(context.Context, int) ([]obsoutbox.Row, error) { return nil, nil }
func (dupStore) MarkPublished(context.Context, string) error                { return nil }
func (dupStore) MarkFailed(context.Context, string, string) error           { return nil }
func (dupStore) Deadletter(context.Context, string, string, int) error      { return nil }

func TestQuarantine_StoreDuplicateInsertIsIdempotentHappyPath(t *testing.T) {
	t.Parallel()
	inner := func(_ context.Context, _ eventbus.Message) error { return errors.New("decode error") }
	h := withQuarantine(inner, quarantineDeps{
		ConsumerName: "token_usage",
		Topic:        "chora.observability.token_usage.recorded.v1",
		Store:        dupStore{},
		Now:          func() time.Time { return time.Unix(1_700_000_000, 0).UTC() },
	})
	err := h(context.Background(), eventbus.Message{})
	if err == nil {
		t.Fatal("duplicate insert must still return the original error (broker Nak)")
	}
}

// ---------------------------------------------------------------------------
// Bootstrap nil / env fallbacks (no live connections)
// ---------------------------------------------------------------------------

func TestBootstrapDBPool_NoEnv_FallsBackToNil(t *testing.T) {
	t.Setenv("CHORA_DB_DSN", "")
	t.Setenv("CHORA_DB_DSN_SECRET_ID", "")
	pool, shutdown := bootstrapDBPool(context.Background())
	if pool != nil {
		t.Error("expected nil pool when no DSN env is set")
	}
	if shutdown != nil {
		t.Error("expected nil shutdown when no DSN env is set")
	}
}

func TestObservabilityDBRuntimeParams(t *testing.T) {
	t.Parallel()
	params := observabilityDBRuntimeParams()
	if params["lock_timeout"] != "3s" {
		t.Errorf("lock_timeout = %q; want 3s", params["lock_timeout"])
	}
	if params["idle_in_transaction_session_timeout"] != "60s" {
		t.Errorf("idle timeout = %q; want 60s", params["idle_in_transaction_session_timeout"])
	}
}

func TestBootstrapInbox_NilPool_UsesMemoryStore(t *testing.T) {
	t.Parallel()
	store := bootstrapInbox(nil)
	if store == nil {
		t.Fatal("bootstrapInbox(nil) returned nil store")
	}
}

func TestBootstrapOutboxStore_NilPool_UsesMemoryStore(t *testing.T) {
	t.Parallel()
	store := bootstrapOutboxStore(nil)
	if store == nil {
		t.Fatal("bootstrapOutboxStore(nil) returned nil store")
	}
}

// ---------------------------------------------------------------------------
// Pricing-backed decision cost calculator
// ---------------------------------------------------------------------------

const testPricingYAML = `
version: "test-1"
effective_from: 2026-01-01T00:00:00Z
prices:
  gemini-3-pro:
    input_per_1k: 1.25
    output_per_1k: 5.0
    cache_per_1k: 0.125
    pricing_model: managed
reconciliation:
  cron: "0 4 * * *"
  lookback_days: 1
`

func TestNewDecisionCostCalculatorFromFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "pricing.yaml")
	if err := os.WriteFile(path, []byte(testPricingYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	calc, err := newDecisionCostCalculatorFromFile(path)
	if err != nil {
		t.Fatalf("newDecisionCostCalculatorFromFile: %v", err)
	}
	if calc == nil {
		t.Fatal("expected a calculator")
	}
	if _, err := newDecisionCostCalculatorFromFile(filepath.Join(dir, "missing.yaml")); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestComputeMicros(t *testing.T) {
	t.Parallel()
	calc, err := newDecisionCostCalculatorFromFile(newTestPricingPath(t))
	if err != nil {
		t.Fatalf("calculator: %v", err)
	}

	// zero tokens -> (0, false)
	if micros, ok := calc.ComputeMicros("gemini-3-pro", 0, 0, 0); ok || micros != 0 {
		t.Errorf("zero tokens = (%d, %v); want (0, false)", micros, ok)
	}
	// unknown model -> (0, false)
	if _, ok := calc.ComputeMicros("nope-model", 100, 0, 0); ok {
		t.Error("unknown model should not price")
	}
	// empty model + nil calculator -> (0, false)
	if _, ok := calc.ComputeMicros("", 100, 0, 0); ok {
		t.Error("empty model should not price")
	}
	var nilCalc *decisionCostCalculator
	if _, ok := nilCalc.ComputeMicros("gemini-3-pro", 100, 0, 0); ok {
		t.Error("nil calculator should not price")
	}
	// priced path — 1000 prompt tokens at $1.25/1k = 1250 micros
	micros, ok := calc.ComputeMicros("gemini-3-pro", 1000, 0, 0)
	if !ok {
		t.Fatal("expected priced cost")
	}
	if micros <= 0 {
		t.Errorf("micros = %d; want > 0", micros)
	}
}

// newTestPricingPath writes the fixture YAML once per call (cheap, hermetic).
func newTestPricingPath(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pricing.yaml")
	if err := os.WriteFile(path, []byte(testPricingYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// ---------------------------------------------------------------------------
// Agents registry loader
// ---------------------------------------------------------------------------

func TestLoadAgentsRegistry_EnvPath(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "registry.json")
	if err := os.WriteFile(good, []byte(`{"version":"v1","crews":[{"name":"qgen","domain":"obs"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CHORA_AGENTS_REGISTRY_PATH", good)
	r := loadAgentsRegistry()
	if r == nil || len(r.Crews) != 1 {
		t.Fatalf("loadAgentsRegistry = %+v; want 1 crew", r)
	}

	bad := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(bad, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CHORA_AGENTS_REGISTRY_PATH", bad)
	if r := loadAgentsRegistry(); r != nil {
		t.Errorf("parse failure should yield nil registry, got %+v", r)
	}

	t.Setenv("CHORA_AGENTS_REGISTRY_PATH", "")
	if r := loadAgentsRegistry(); r != nil {
		t.Errorf("no candidates should yield nil registry, got %+v", r)
	}
}
