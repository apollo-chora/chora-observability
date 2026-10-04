// event_sink_test.go — env-driven EventSink resolution for cmd/reconcile.
//
// Verifies the ADR-167 Tier 2 wiring decision tree:
//   - CHORA_PUBSUB_PROJECT unset + not-required → logging fallback (dev).
//   - CHORA_PUBSUB_PROJECT unset + RECONCILE_REQUIRE_PUBSUB → fail loud.
//
// The "real CloudPublisher wired" branch needs a live GCP client (network),
// so it is exercised by the reconcilepublish adapter unit test with a stub
// publisher instead — here we only assert the env decision tree + the
// fail-loud guard, which need no broker.
package main

import (
	"context"
	"errors"
	"testing"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/reconcile"
)

func TestNewEventSinksFromEnv_UnsetProject_LoggingFallback(t *testing.T) {
	withEnv(t, map[string]string{
		"CHORA_PUBSUB_PROJECT":     "",
		"RECONCILE_REQUIRE_PUBSUB": "",
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
		"CHORA_PUBSUB_PROJECT":     "",
		"RECONCILE_REQUIRE_PUBSUB": "1",
	})
	_, _, err := newEventSinksFromEnv(context.Background())
	if err == nil {
		t.Fatal("expected fail-loud error when Pub/Sub required but project unset")
	}
	if !errors.Is(err, errPubSubRequired) {
		t.Errorf("err = %v; want errPubSubRequired", err)
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
