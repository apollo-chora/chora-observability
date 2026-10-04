// bootstrap_test.go — production wiring helper tests for chora-observability.
package main

import (
	"context"
	"os"
	"testing"
)

func TestBootstrapDBPool_NoEnvReturnsNil(t *testing.T) {
	t.Setenv("CHORA_DB_DSN", "")
	t.Setenv("CHORA_DB_DSN_SECRET_ID", "")
	pool, shutdown := bootstrapDBPool(context.Background())
	if pool != nil {
		t.Fatalf("expected nil pool when DB env unset; got %v", pool)
	}
	if shutdown != nil {
		t.Fatalf("expected nil shutdown when pool unwired")
	}
}

func TestBootstrapPubSubClient_NoEnvReturnsNil(t *testing.T) {
	t.Setenv("CHORA_PUBSUB_PROJECT", "")
	cli, shutdown := bootstrapPubSubClient(context.Background())
	if cli != nil {
		t.Fatalf("expected nil client when CHORA_PUBSUB_PROJECT unset; got %v", cli)
	}
	if shutdown != nil {
		t.Fatalf("expected nil shutdown when client unwired")
	}
}

func TestBootstrapOutboxStore_NilPoolReturnsInMemory(t *testing.T) {
	store := bootstrapOutboxStore(nil)
	if store == nil {
		t.Fatalf("expected non-nil InMemoryStore when pool is nil; got nil")
	}
}

func TestOutboxWorkerID_PrefersEnvOverHostname(t *testing.T) {
	t.Setenv("CHORA_OUTBOX_WORKER_ID", "test-worker-42")
	if got := outboxWorkerID(); got != "test-worker-42" {
		t.Errorf("outboxWorkerID = %q; want test-worker-42", got)
	}
}

func TestOutboxWorkerID_FallsBackToHostname(t *testing.T) {
	t.Setenv("CHORA_OUTBOX_WORKER_ID", "")
	t.Setenv("HOSTNAME", "my-pod-x7q")
	if got := outboxWorkerID(); got != "my-pod-x7q" {
		t.Errorf("outboxWorkerID = %q; want my-pod-x7q", got)
	}
}

func TestEnvOrDefault_ReturnsDefaultOnEmpty(t *testing.T) {
	t.Setenv("MISSING_KEY_XYZ", "")
	if got := envOrDefault("MISSING_KEY_XYZ", "fallback"); got != "fallback" {
		t.Errorf("envOrDefault = %q; want fallback", got)
	}
}

func TestEnvOrDefault_PrefersEnvWhenSet(t *testing.T) {
	t.Setenv("PRESENT_KEY_XYZ", "set-value")
	if got := envOrDefault("PRESENT_KEY_XYZ", "fallback"); got != "set-value" {
		t.Errorf("envOrDefault = %q; want set-value", got)
	}
}

func TestLoadDotEnv_LoadsValuesWithoutOverwritingExistingEnv(t *testing.T) {
	t.Setenv("DOTENV_TEST_EXISTING", "process-value")

	path := t.TempDir() + "/.env"
	content := "DOTENV_TEST_NEW=loaded-value\nDOTENV_TEST_EXISTING=file-value\nexport DOTENV_TEST_QUOTED="hello world"\n# comment\n"
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}

	if err := loadDotEnv(path); err != nil {
		t.Fatalf("loadDotEnv: %v", err)
	}
	if got := os.Getenv("DOTENV_TEST_NEW"); got != "loaded-value" {
		t.Fatalf("new value = %q; want loaded-value", got)
	}
	if got := os.Getenv("DOTENV_TEST_EXISTING"); got != "process-value" {
		t.Fatalf("existing value = %q; want process-value", got)
	}
	if got := os.Getenv("DOTENV_TEST_QUOTED"); got != "hello world" {
		t.Fatalf("quoted value = %q; want hello world", got)
	}
}

func TestLoadDotEnv_RejectsMalformedEntry(t *testing.T) {
	path := t.TempDir() + "/.env"
	if err := os.WriteFile(path, []byte("NOT_A_PAIR\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := loadDotEnv(path); err == nil {
		t.Fatal("expected malformed dotenv entry to fail")
	}
}
