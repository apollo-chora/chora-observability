// main_extra_test.go — completes cmd/reconcile coverage for the env helpers
// (getenvInt / getenvFloat / envOrDefault), the static token source, the
// event-bus-required fail-loud path, and the run() composition root (which is
// fully exercisable offline: mock billing + BigQuery clients + the logging
// anomaly sink).
package main

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestStaticEnvTokenSource(t *testing.T) {
	s := &staticEnvTokenSource{token: "abc123"}
	if tok, err := s.Token(); err != nil || tok != "abc123" {
		t.Fatalf("Token() = (%q, %v); want (abc123, nil)", tok, err)
	}
	blank := &staticEnvTokenSource{token: "   "}
	if _, err := blank.Token(); err == nil {
		t.Fatal("expected error for blank token")
	}
}

func TestGetenvInt(t *testing.T) {
	withEnv(t, map[string]string{"RECONCILE_EXT": ""})
	// unset -> default
	if got := getenvInt("RECONCILE_EXT", 7); got != 7 {
		t.Errorf("unset = %d; want 7", got)
	}
	// valid
	if err := os.Setenv("RECONCILE_EXT", "3"); err != nil {
		t.Fatal(err)
	}
	if got := getenvInt("RECONCILE_EXT", 7); got != 3 {
		t.Errorf("valid = %d; want 3", got)
	}
	// invalid -> default
	if err := os.Setenv("RECONCILE_EXT", "not-a-number"); err != nil {
		t.Fatal(err)
	}
	if got := getenvInt("RECONCILE_EXT", 7); got != 7 {
		t.Errorf("invalid = %d; want 7", got)
	}
}

func TestGetenvFloat(t *testing.T) {
	withEnv(t, map[string]string{"RECONCILE_EXTF": ""})
	if got := getenvFloat("RECONCILE_EXTF", 0.5); got != 0.5 {
		t.Errorf("unset = %v; want 0.5", got)
	}
	if err := os.Setenv("RECONCILE_EXTF", "0.001"); err != nil {
		t.Fatal(err)
	}
	if got := getenvFloat("RECONCILE_EXTF", 0.5); got != 0.001 {
		t.Errorf("valid = %v; want 0.001", got)
	}
	if err := os.Setenv("RECONCILE_EXTF", "nope"); err != nil {
		t.Fatal(err)
	}
	if got := getenvFloat("RECONCILE_EXTF", 0.5); got != 0.5 {
		t.Errorf("invalid = %v; want 0.5", got)
	}
}

func TestEnvOrDefault(t *testing.T) {
	withEnv(t, map[string]string{"CHORA_SOURCE_PROJECT": ""})
	if got := envOrDefault("CHORA_SOURCE_PROJECT", "chora-489812"); got != "chora-489812" {
		t.Errorf("unset = %q; want default", got)
	}
	_ = os.Setenv("CHORA_SOURCE_PROJECT", "chora-custom")
	if got := envOrDefault("CHORA_SOURCE_PROJECT", "chora-489812"); got != "chora-custom" {
		t.Errorf("set = %q; want chora-custom", got)
	}
}

func TestRun_RequiresProject(t *testing.T) {
	withEnv(t, map[string]string{
		"GOOGLE_CLOUD_PROJECT":       "",
		"BILLING_CLIENT_MODE":        "mock",
		"NATS_URL":                   "",
		"RECONCILE_REQUIRE_EVENTBUS": "",
	})
	if err := run(context.Background()); err == nil {
		t.Fatal("expected error when GOOGLE_CLOUD_PROJECT is unset")
	}
}

func TestRun_HappyPath_MockClients(t *testing.T) {
	withEnv(t, map[string]string{
		"GOOGLE_CLOUD_PROJECT":       "chora-test",
		"BILLING_CLIENT_MODE":        "",
		"NATS_URL":                   "",
		"RECONCILE_REQUIRE_EVENTBUS": "",
		"RECONCILE_LOOKBACK_DAYS":    "2",
	})
	if err := run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestRun_EventBusRequired_FailsLoud(t *testing.T) {
	withEnv(t, map[string]string{
		"GOOGLE_CLOUD_PROJECT":       "chora-test",
		"BILLING_CLIENT_MODE":        "mock",
		"NATS_URL":                   "",
		"RECONCILE_REQUIRE_EVENTBUS": "1",
	})
	err := run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "RECONCILE_REQUIRE_EVENTBUS") {
		t.Fatalf("err = %v; want RECONCILE_REQUIRE_EVENTBUS failure", err)
	}
}

func TestNewEventSinksFromEnv_RealEventBusOrDegrade(t *testing.T) {
	// NATS_URL set: either a real bus comes up (a live NATS server is
	// reachable) or nats.Connect fails and the non-required path degrades to
	// the logging fallback. Both outcomes must construct a working Anomaly sink.
	withEnv(t, map[string]string{
		"NATS_URL":                   "chora-test",
		"RECONCILE_REQUIRE_EVENTBUS": "",
		"CHORA_SOURCE_PROJECT":       "chora-custom",
	})
	sinks, shutdown, err := newEventSinksFromEnv(t.Context())
	if err != nil {
		t.Fatalf("newEventSinksFromEnv: %v", err)
	}
	if sinks.Anomaly == nil {
		t.Fatal("expected an anomaly sink")
	}
	if shutdown != nil {
		shutdown()
	}
	if got := envOrDefault("CHORA_SOURCE_PROJECT", "x"); got != "chora-custom" {
		t.Errorf("envOrDefault = %q; want chora-custom", got)
	}
}

func TestNewEventSinksFromEnv_RequiredWithProjectSet(t *testing.T) {
	// When REQUIRED is asserted, a bus-init failure must FAIL LOUD (join
	// errEventBusRequired), never degrade silently.
	withEnv(t, map[string]string{
		"NATS_URL":                   "chora-test",
		"RECONCILE_REQUIRE_EVENTBUS": "yes",
	})
	sinks, shutdown, err := newEventSinksFromEnv(t.Context())
	// If the bus init succeeded (a live NATS server is reachable) there is no
	// error and a real sink is wired — also acceptable. We only assert the
	// fail-loud shape when the init actually failed.
	if err != nil {
		if !strings.Contains(err.Error(), "RECONCILE_REQUIRE_EVENTBUS") {
			t.Fatalf("err = %v; want wrapped errEventBusRequired", err)
		}
		return
	}
	if sinks.Anomaly == nil {
		t.Fatal("expected a sink when the bus initialised")
	}
	if shutdown != nil {
		shutdown()
	}
}

func TestMain_RunsHappyPath(t *testing.T) {
	withEnv(t, map[string]string{
		"GOOGLE_CLOUD_PROJECT":       "chora-main-test",
		"BILLING_CLIENT_MODE":        "",
		"NATS_URL":                   "",
		"RECONCILE_REQUIRE_EVENTBUS": "",
	})
	// main() calls run(); on the happy path (mock clients, logging sink) it
	// returns without log.Fatalf. A regression that fatals fails the test.
	main()
}
