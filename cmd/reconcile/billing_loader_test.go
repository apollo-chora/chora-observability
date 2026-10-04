// billing_loader_test.go — RED-phase TDD specs for env-driven Vertex
// Billing client construction in cmd/reconcile.
//
// Per ai-cost-tracking + secrets-and-env, the cmd/reconcile binary
// chooses between mock + real billing clients via BILLING_CLIENT_MODE,
// and sources every external URL/token from env vars (no inline). The
// real client uses a TokenSource the binary builds from env (ENV-driven
// static-token seam during build-out; ADC/WIF in production rollout).
package main

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/billing"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/reconcile"
)

func withEnv(t *testing.T, kv map[string]string) {
	t.Helper()
	prev := make(map[string]string, len(kv))
	for k := range kv {
		prev[k] = os.Getenv(k)
	}
	for k, v := range kv {
		if v == "" {
			_ = os.Unsetenv(k)
			continue
		}
		_ = os.Setenv(k, v)
	}
	t.Cleanup(func() {
		for k, v := range prev {
			if v == "" {
				_ = os.Unsetenv(k)
				continue
			}
			_ = os.Setenv(k, v)
		}
	})
}

func TestNewBillingClientFromEnv_DefaultsToMock(t *testing.T) {
	withEnv(t, map[string]string{
		"BILLING_CLIENT_MODE": "",
	})
	c, err := newBillingClientFromEnv()
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if _, ok := c.(reconcile.BillingClient); !ok {
		t.Errorf("must satisfy reconcile.BillingClient")
	}
	if _, ok := c.(*billing.MockClient); !ok {
		t.Errorf("default must be *billing.MockClient; got %T", c)
	}
}

func TestNewBillingClientFromEnv_VertexModeRequiresBaseURL(t *testing.T) {
	withEnv(t, map[string]string{
		"BILLING_CLIENT_MODE":     "vertex",
		"VERTEX_BILLING_BASE_URL": "",
		"VERTEX_BILLING_TOKEN":    "tok",
	})
	_, err := newBillingClientFromEnv()
	if err == nil {
		t.Fatal("expected error when VERTEX_BILLING_BASE_URL blank")
	}
	if !strings.Contains(err.Error(), "VERTEX_BILLING_BASE_URL") {
		t.Errorf("err must reference env var; got %v", err)
	}
}

func TestNewBillingClientFromEnv_VertexModeRequiresToken(t *testing.T) {
	withEnv(t, map[string]string{
		"BILLING_CLIENT_MODE":     "vertex",
		"VERTEX_BILLING_BASE_URL": "https://cloudbilling.googleapis.com",
		"VERTEX_BILLING_TOKEN":    "",
	})
	_, err := newBillingClientFromEnv()
	if err == nil {
		t.Fatal("expected error when VERTEX_BILLING_TOKEN blank")
	}
}

func TestNewBillingClientFromEnv_VertexModeBuildsRealClient(t *testing.T) {
	withEnv(t, map[string]string{
		"BILLING_CLIENT_MODE":     "vertex",
		"VERTEX_BILLING_BASE_URL": "https://cloudbilling.googleapis.com",
		"VERTEX_BILLING_TOKEN":    "ya29-test-token",
	})
	c, err := newBillingClientFromEnv()
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if _, ok := c.(*billing.VertexClient); !ok {
		t.Errorf("expected *billing.VertexClient; got %T", c)
	}
}

func TestNewBillingClientFromEnv_RejectsUnknownMode(t *testing.T) {
	withEnv(t, map[string]string{
		"BILLING_CLIENT_MODE": "weatherwax",
	})
	_, err := newBillingClientFromEnv()
	if err == nil {
		t.Fatal("expected error on unknown mode")
	}
	if !errors.Is(err, errUnknownBillingMode) {
		t.Errorf("err = %v; want errUnknownBillingMode", err)
	}
}
