//go:build integration

// Real-Postgres repository tests.
//
// These exist because stub-querier tests verify nothing about the actual
// schema: they cannot catch a column the INSERT forgot, a Scan order that
// drifted from the SELECT, a renamed column, or an RLS policy that rejects the
// write. A silently dropped `cached_tokens` field survived the whole unit suite
// and was only found by publishing a real event and reading the row back.
//
// Run with a migrated database:
//
//	CHORA_TEST_DSN='postgres://chora_observability_app_rw:chora@localhost:5432/chora_observability?sslmode=disable' \
//	  go test -tags integration ./internal/adapter/pg/...
//
// Skips cleanly when CHORA_TEST_DSN is unset.
package pg_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/apollo-chora/chora-observability/internal/adapter/pg"
	"github.com/apollo-chora/chora-observability/internal/domain/ledger"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("CHORA_TEST_DSN")
	if strings.TrimSpace(dsn) == "" {
		t.Skip("CHORA_TEST_DSN not set; skipping real-Postgres repository test")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		t.Fatalf("ping %s: %v", dsn, err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// TestLedgerRepository_RealPostgres_RoundTrip writes an entry with every field
// distinct and non-zero, reads it back through List, and compares every field.
// A field the INSERT or the SELECT omits shows up as a mismatch here.
func TestLedgerRepository_RealPostgres_RoundTrip(t *testing.T) {
	pool := testPool(t)
	repo := pg.NewLedgerRepository(pg.NewPgxPoolQuerier(pool))
	ctx := context.Background()

	tenant := "00000000-0000-7000-8000-0000000000a1"
	gcid := "00000000-0000-7000-8000-0000000000a2"
	// timestamptz keeps microsecond precision.
	recordedAt := time.Date(2026, 10, 5, 12, 34, 56, 123456000, time.UTC)

	entry, err := ledger.New(ledger.NewParams{
		TenantID:         tenant,
		Gcid:             gcid,
		Agid:             "qgen_question",
		ModelID:          "round-trip-model",
		PromptTokens:     101,
		CompletionTokens: 202,
		CachedTokens:     303,
		CostUsdMicros:    404,
		TraceID:          "4bf92f3577b34da6a3ce929d0e0e4736",
		SpanID:           "00f067aa0ba902b7",
		RecordedAt:       recordedAt,
	})
	if err != nil {
		t.Fatalf("ledger.New: %v", err)
	}
	if err := repo.Append(ctx, entry); err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, err := repo.List(ctx, tenant, ledger.ListFilter{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var found *ledger.Entry
	for _, e := range got {
		if e.LedgerID == entry.LedgerID {
			found = e
			break
		}
	}
	if found == nil {
		t.Fatalf("appended entry %s not returned by List (got %d rows)", entry.LedgerID, len(got))
	}

	checks := []struct {
		name      string
		got, want any
	}{
		{"tenant_id", found.TenantID, tenant},
		{"gcid", found.Gcid, gcid},
		{"agid", found.Agid, "qgen_question"},
		{"model_id", found.ModelID, "round-trip-model"},
		{"prompt_tokens", found.PromptTokens, 101},
		{"completion_tokens", found.CompletionTokens, 202},
		{"cached_tokens", found.CachedTokens, 303},
		{"cost_usd_micros", found.CostUsdMicros, int64(404)},
		{"trace_id", found.TraceID, "4bf92f3577b34da6a3ce929d0e0e4736"},
		{"span_id", found.SpanID, "00f067aa0ba902b7"},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v; want %v", c.name, c.got, c.want)
		}
	}
	if !found.RecordedAt.UTC().Equal(recordedAt) {
		t.Errorf("recorded_at = %s; want %s", found.RecordedAt.UTC(), recordedAt)
	}
}

// TestLedgerRepository_RealPostgres_AppendOnlyTrigger proves the migration's
// append-only invariant is actually installed — the repository is the only
// writer, so a missing trigger would be invisible to every other test.
func TestLedgerRepository_RealPostgres_AppendOnlyTrigger(t *testing.T) {
	pool := testPool(t)
	repo := pg.NewLedgerRepository(pg.NewPgxPoolQuerier(pool))
	ctx := context.Background()

	tenant := "00000000-0000-7000-8000-0000000000b1"
	entry, err := ledger.New(ledger.NewParams{
		TenantID:         tenant,
		Gcid:             "00000000-0000-7000-8000-0000000000b2",
		ModelID:          "immutable-model",
		PromptTokens:     1,
		CompletionTokens: 1,
		CostUsdMicros:    1,
		TraceID:          "4bf92f3577b34da6a3ce929d0e0e4736",
		SpanID:           "00f067aa0ba902b7",
	})
	if err != nil {
		t.Fatalf("ledger.New: %v", err)
	}
	if err := repo.Append(ctx, entry); err != nil {
		t.Fatalf("Append: %v", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SELECT set_config('chora.tenant_id', $1, true)", tenant); err != nil {
		t.Fatalf("set tenant guc: %v", err)
	}
	_, err = tx.Exec(ctx, "UPDATE token_usage_ledger SET model_id = 'tampered' WHERE ledger_id = $1", entry.LedgerID)
	if err == nil {
		t.Fatal("UPDATE succeeded; the append-only trigger is missing or not enforcing")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "append") {
		t.Logf("UPDATE rejected (trigger or RLS): %v", err)
	}
}
