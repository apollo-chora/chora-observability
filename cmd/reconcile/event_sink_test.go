// event_sink_test.go — env-driven EventSink resolution for cmd/reconcile.
//
// Verifies the ADR-167 Tier 2 wiring decision tree:
//   - NATS_URL unset + not-required → logging fallback (dev).
//   - NATS_URL unset + RECONCILE_REQUIRE_EVENTBUS → fail loud.
//
// The "real JetStream bus wired" branch needs a live NATS server (network),
// so it is exercised by the reconcilepublish adapter unit test with a stub
// publisher instead — here we only assert the env decision tree + the
// fail-loud guard, which need no broker.
package main

import (
	"context"
	"errors"
	"testing"

	"github.com/apollo-chora/chora-observability/internal/domain/reconcile"
)

func TestNewEventSinksFromEnv_UnsetProject_LoggingFallback(t *testing.T) {
	withEnv(t, map[string]string{
		"NATS_URL":                   "",
		"RECONCILE_REQUIRE_EVENTBUS": "",
	})
	sinks, shutdown, err := newEventSinksFromEnv(context.Background())
	if err != nil {
		t.Fatalf("err = %v; want nil (dev fallback)", err)
	}
	defer shutdown()
	if _, ok := sinks.Anomaly.(loggingEventSink); !ok {
		t.Errorf("anomaly sink = %T; want loggingEventSink fallback", sinks.Anomaly)
	}
	// Degraded is intentionally nil in the logging fallback.
	if sinks.Degraded != nil {
		t.Errorf("degraded sink = %T; want nil in logging fallback", sinks.Degraded)
	}
	// Sanity: the fallback still satisfies the port + emits without error.
	if err := sinks.Anomaly.Emit(context.Background(), reconcile.Verdict{IsAnomaly: true}.AsEvent()); err != nil {
		t.Errorf("logging fallback Emit = %v; want nil", err)
	}
}

func TestNewEventSinksFromEnv_RequiredButUnset_FailsLoud(t *testing.T) {
	withEnv(t, map[string]string{
		"NATS_URL":                   "",
		"RECONCILE_REQUIRE_EVENTBUS": "1",
	})
	_, _, err := newEventSinksFromEnv(context.Background())
	if err == nil {
		t.Fatal("expected fail-loud error when the event bus is required but NATS_URL is unset")
	}
	if !errors.Is(err, errEventBusRequired) {
		t.Errorf("err = %v; want errEventBusRequired", err)
	}
}

func TestIsTruthy(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want bool
	}{
		{"1", true}, {"true", true}, {"TRUE", true}, {"yes", true}, {"on", true},
		{"", false}, {"0", false}, {"false", false}, {"weatherwax", false},
	} {
		if got := isTruthy(tc.in); got != tc.want {
			t.Errorf("isTruthy(%q) = %v; want %v", tc.in, got, tc.want)
		}
	}
}
