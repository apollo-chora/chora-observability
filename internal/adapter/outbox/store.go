// Package outbox is chora-observability's D6.2 producer-side transactional
// outbox adapter (M12.3 Wave 2, subagent w2c).
//
// Why it exists
// -------------
// chora-observability emits chora.observability.token_usage.recorded.v1
// (and future observability domain events) whenever the LedgerHook records
// a token usage entry on behalf of the Model Gateway. The pre-D6.2 path
// wrote to an in-memory recorder + relied on a later relay process — a
// crash between Append and Publish silently dropped the event.
//
// The D6.2 pattern (per `feedback_d6_resilience_first_class` +
// `.claude/skills/agentic-resilience-d6/SKILL.md` Pillar 2) replaces this
// with a canonical transactional outbox:
//
//  1. The OutboxPublisher writes the event to outbox_events
//     (status='pending') inside the same request handler. The HTTP request
//     returns successfully once the row is durably committed.
//  2. A separate background Dispatcher polls pending rows, publishes
//     them to Cloud Pub/Sub, and marks success/failure/deadletter.
//
// The handler crash window now never loses an event: on restart the
// dispatcher resumes from the pending row.
//
// Three actors:
//
//   - Store — port that wraps the outbox_events table. InMemoryStore +
//     PostgresStore implementations.
//   - Publisher — satisfies ledger.OutboxRecorder by writing to Store.
//     Drop-in for the existing inmem.OutboxRecorder.
//   - Dispatcher — drains Store to a Bus with retry + DLQ.
//
// Mirrored from:
//   - services/chora-guardrail/internal/adapter/outbox/*.go (canonical)
//
// Schema in:
//   - services/chora-observability/migrations/0003_outbox.sql (legacy shape)
//   - services/chora-observability/migrations/0006_outbox_d6_canonical.up.sql
//     (D6 canonical augmentation — tenant_id top-level + RLS + idempotency idx)
//
// Per `feedback_no_inline_config`: no inline URLs/secrets — every dependency
// is injected via the public Config structs.
package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// ErrDuplicateIdempotencyKey is returned by Store.Insert when the supplied
// row collides on idempotency_key. Callers MAY treat this as a no-op
// (the prior emission is already durably queued).
var ErrDuplicateIdempotencyKey = errors.New("outbox: duplicate idempotency_key")

// -----------------------------------------------------------------------------
// Row
// -----------------------------------------------------------------------------

// Row mirrors one row of outbox_events. Field names align with the
// migration column names so SQL marshalling is mechanical.
type Row struct {
	ID             string
	TenantID       string
	GCID           string
	AgentID        string // mapped to envelope agent_id; column = agid (canonical column name)
	EventType      string
	Topic          string
	Payload        []byte
	Envelope       map[string]string // JSONB; subscriber attribute set
	IdempotencyKey string
	OccurredAt     time.Time
	RetryCount     int
	LastError      string
}

// DeadLetter mirrors one row of outbox_dead_letters.
type DeadLetter struct {
	RowID          string
	FailureReason  string
	AttemptCount   int
	WorkerID       string
	DeadletteredAt time.Time
}

// -----------------------------------------------------------------------------
// Store port
// -----------------------------------------------------------------------------

// Store is the port the Publisher + Dispatcher use to talk to outbox_events.
// Two implementations:
//
//   - InMemoryStore — hermetic for tests + local dev.
//   - PostgresStore — production; wraps a database/sql connection on
//     chora_observability (where the table lives).
type Store interface {
	// Insert writes a new pending row. Returns ErrDuplicateIdempotencyKey
	// when the row collides on idempotency_key.
	Insert(ctx context.Context, row Row) error

	// FetchPending claims up to limit rows in status='pending' ordered by
	// occurred_at ASC. Postgres uses SELECT ... FOR UPDATE SKIP LOCKED so
	// concurrent workers do not collide.
	FetchPending(ctx context.Context, limit int) ([]Row, error)

	// MarkPublished transitions the row to status='published'.
	MarkPublished(ctx context.Context, id string) error

	// MarkFailed records a transient publish failure: bumps retry_count,
	// stamps last_error + last_attempt_at, sets status='failed'.
	MarkFailed(ctx context.Context, id string, errMsg string) error

	// Deadletter inserts an outbox_dead_letters row + transitions the events
	// row to status='deadlettered'. attemptCount is captured for audit /
	// runbook context.
	Deadletter(ctx context.Context, id string, failureReason string, attemptCount int) error
}

// -----------------------------------------------------------------------------
// InMemoryStore
// -----------------------------------------------------------------------------

// InMemoryStore is the hermetic fixture used by tests + by main()'s fallback
// when CHORA_OUTBOX_DSN is unset. NOT safe for production — does not persist
// across process restart.
type InMemoryStore struct {
	mu                 sync.Mutex
	rows               map[string]*Row // id → row
	idempotency        map[string]string
	published          []string
	deadLetters        []DeadLetter
	deadletterWorkerID string
}

// NewInMemoryStore constructs an empty InMemoryStore.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		rows:        make(map[string]*Row),
		idempotency: make(map[string]string),
	}
}

// Insert satisfies Store.
func (s *InMemoryStore) Insert(_ context.Context, row Row) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.idempotency[row.IdempotencyKey]; dup {
		return fmt.Errorf("%w: %s", ErrDuplicateIdempotencyKey, row.IdempotencyKey)
	}
	cp := row
	if cp.Envelope != nil {
		envCopy := make(map[string]string, len(cp.Envelope))
		for k, v := range cp.Envelope {
			envCopy[k] = v
		}
		cp.Envelope = envCopy
	}
	if cp.Payload != nil {
		cp.Payload = append([]byte(nil), cp.Payload...)
	}
	s.rows[cp.ID] = &cp
	s.idempotency[cp.IdempotencyKey] = cp.ID
	return nil
}

// FetchPending satisfies Store.
func (s *InMemoryStore) FetchPending(_ context.Context, limit int) ([]Row, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pending := make([]*Row, 0, len(s.rows))
	for _, r := range s.rows {
		pending = append(pending, r)
	}
	sort.SliceStable(pending, func(i, j int) bool {
		return pending[i].OccurredAt.Before(pending[j].OccurredAt)
	})
	if limit > 0 && len(pending) > limit {
		pending = pending[:limit]
	}
	out := make([]Row, 0, len(pending))
	for _, r := range pending {
		out = append(out, *r)
	}
	return out, nil
}

// MarkPublished satisfies Store.
func (s *InMemoryStore) MarkPublished(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rows[id]; !ok {
		return fmt.Errorf("outbox: MarkPublished: row %q not found", id)
	}
	delete(s.rows, id)
	s.published = append(s.published, id)
	return nil
}

// MarkFailed satisfies Store.
func (s *InMemoryStore) MarkFailed(_ context.Context, id string, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[id]
	if !ok {
		return fmt.Errorf("outbox: MarkFailed: row %q not found", id)
	}
	r.RetryCount++
	r.LastError = errMsg
	return nil
}

// Deadletter satisfies Store.
func (s *InMemoryStore) Deadletter(_ context.Context, id, failureReason string, attemptCount int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[id]
	if !ok {
		return fmt.Errorf("outbox: Deadletter: row %q not found", id)
	}
	s.deadLetters = append(s.deadLetters, DeadLetter{
		RowID:          id,
		FailureReason:  failureReason,
		AttemptCount:   attemptCount,
		WorkerID:       s.deadletterWorkerID,
		DeadletteredAt: time.Now().UTC(),
	})
	delete(s.rows, r.ID)
	return nil
}

// SetWorkerID stamps the worker_id captured in DeadLetters created by this
// in-memory store. The Dispatcher calls this once at construction so the
// store can record the worker on Deadletter() rows without an extra
// parameter on the Store port (PostgresStore stores worker_id on its own
// struct).
func (s *InMemoryStore) SetWorkerID(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deadletterWorkerID = id
}

// Published returns the IDs of successfully MarkPublished'd rows in order.
func (s *InMemoryStore) Published() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]string(nil), s.published...)
	return out
}

// DeadLetters returns the dead-letter rows recorded by Deadletter.
func (s *InMemoryStore) DeadLetters() []DeadLetter {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]DeadLetter(nil), s.deadLetters...)
	return out
}

// Compile-time check.
var _ Store = (*InMemoryStore)(nil)

// -----------------------------------------------------------------------------
// PostgresStore
// -----------------------------------------------------------------------------

// SQLRows is the minimal database/sql Rows surface used by PostgresStore.
type SQLRows interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
	Err() error
}

// SQLDB is the minimal database/sql surface required by PostgresStore.
// Production wires *sql.DB (or a *sql.Conn) pointing at chora_observability.
// Tests stub via the SQLDB interface.
type SQLDB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (SQLRows, error)
}

// PostgresStoreOptions tunes a PostgresStore at construction time.
// defaultClaimReservationSeconds is the window during which a claimed-but-not-
// yet-published row is hidden from other workers' FetchPending. It must exceed
// the worst-case time to publish one DrainOnce batch; it also bounds how long a
// row claimed by a crashed worker waits before re-claim (the built-in reaper).
const defaultClaimReservationSeconds = 60

type PostgresStoreOptions struct {
	// WorkerID identifies the dispatcher worker that owns Deadletter rows.
	// Required. Typically derived from the pod name (HOSTNAME).
	WorkerID string

	// ClaimReservationSeconds is the FetchPending claim reservation window (see
	// defaultClaimReservationSeconds). Defaults to 60 when <= 0.
	ClaimReservationSeconds int
}

// PostgresStore is the production Store backed by the outbox_events
// table in chora_observability (per migration 0003_outbox.sql +
// 0006_outbox_d6_canonical.up.sql).
type PostgresStore struct {
	db              SQLDB
	workerID        string
	claimReservSecs int
}

// NewPostgresStore constructs a PostgresStore. Panics on missing WorkerID.
func NewPostgresStore(db SQLDB, opts PostgresStoreOptions) *PostgresStore {
	if opts.WorkerID == "" {
		panic("outbox: NewPostgresStore: WorkerID required")
	}
	resv := opts.ClaimReservationSeconds
	if resv <= 0 {
		resv = defaultClaimReservationSeconds
	}
	return &PostgresStore{db: db, workerID: opts.WorkerID, claimReservSecs: resv}
}

// Insert satisfies Store.
func (s *PostgresStore) Insert(ctx context.Context, row Row) error {
	envJSON, err := json.Marshal(row.Envelope)
	if err != nil {
		return fmt.Errorf("outbox: marshal envelope: %w", err)
	}
	const q = `INSERT INTO outbox_events (
        id, tenant_id, gcid, agid, event_type, topic,
        payload, envelope, idempotency_key, occurred_at,
        aggregate_type, aggregate_id, status
    ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11, $12, 'pending'
    )`
	aggregateType := row.AggregateType()
	aggregateID := row.AggregateID()
	_, err = s.db.ExecContext(ctx, q,
		row.ID, row.TenantID, row.GCID, row.AgentID, row.EventType, row.Topic,
		row.Payload, string(envJSON), row.IdempotencyKey, row.OccurredAt,
		aggregateType, aggregateID,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: %s", ErrDuplicateIdempotencyKey, row.IdempotencyKey)
		}
		return fmt.Errorf("outbox: Insert: %w", err)
	}
	return nil
}

// FetchPending satisfies Store.
// FetchPending atomically CLAIMS up to `limit` pending rows and returns them.
// CHO-1615: claiming UPDATE (not a bare SELECT) so two dispatcher workers draining
// the same table (a prod + a *-dev co-deployment, or HPA >1 replica) never both
// publish the same row. The committed last_attempt_at bump + reservation-window
// predicate hide just-claimed rows from a peer; the window doubles as the
// crashed-worker reaper. No migration (last_attempt_at + status already exist).
func (s *PostgresStore) FetchPending(ctx context.Context, limit int) ([]Row, error) {
	const q = `UPDATE outbox_events
        SET last_attempt_at = now()
        WHERE id IN (
            SELECT id FROM outbox_events
            -- includes 'failed' so a transient publish error retries, not strands (see chora-creation outbox)
            WHERE status IN ('pending', 'failed')
              AND (last_attempt_at IS NULL
                   OR last_attempt_at < now() - make_interval(secs => $2))
            ORDER BY occurred_at ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id, tenant_id, gcid, agid,
               event_type, topic, payload, envelope::TEXT,
               idempotency_key, retry_count, occurred_at`
	rows, err := s.db.QueryContext(ctx, q, limit, s.claimReservSecs)
	if err != nil {
		return nil, fmt.Errorf("outbox: FetchPending: %w", err)
	}
	defer rows.Close()

	// poison collects rows whose envelope is genuinely un-parseable JSON. We
	// quarantine them AFTER the cursor is exhausted (below) rather than mid-
	// iteration, so the Deadletter ExecContext never races the open result set.
	type poisonRow struct{ id, reason string }
	var (
		out    []Row
		poison []poisonRow
	)
	for rows.Next() {
		var (
			r       Row
			envStr  string
			occured time.Time
		)
		if err := rows.Scan(
			&r.ID, &r.TenantID, &r.GCID, &r.AgentID,
			&r.EventType, &r.Topic, &r.Payload, &envStr,
			&r.IdempotencyKey, &r.RetryCount, &occured,
		); err != nil {
			return nil, fmt.Errorf("outbox: FetchPending scan: %w", err)
		}
		r.OccurredAt = occured

		// Tolerant envelope decode (HANDOFF 2026-05-29 Fix 1). A single
		// un-parseable or mixed-shape envelope must NEVER fail the whole batch
		// — that stalls every pending row forever (per
		// feedback_resilience_priority + D6 fail-loud-not-stall). chora-model-
		// gateway shares this table and writes a nested `"event"` OBJECT into
		// its camelCase envelope; the protobuf body already lives in the
		// payload BYTEA, so any non-string envelope value is redundant and is
		// dropped here rather than crashing the reader.
		env := map[string]string{}
		if envStr != "" {
			var raw map[string]json.RawMessage
			if err := json.Unmarshal([]byte(envStr), &raw); err != nil {
				// Not even valid JSON → quarantine this row, keep draining the rest.
				poison = append(poison, poisonRow{id: r.ID, reason: fmt.Sprintf("envelope decode: %v", err)})
				continue
			}
			for k, v := range raw {
				var sv string
				if json.Unmarshal(v, &sv) == nil {
					env[k] = sv // scalar string → keep as a Pub/Sub attribute
				}
				// non-string value (e.g. nested "event" object) → skip; the
				// payload BYTEA carries the canonical event body.
			}
		}
		r.Envelope = env
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox: FetchPending iter: %w", err)
	}
	// Cursor exhausted; safe to dead-letter the poison rows out-of-band.
	_ = rows.Close()
	for _, p := range poison {
		if dlErr := s.Deadletter(ctx, p.id, p.reason, 0); dlErr != nil {
			// Best-effort: a failed quarantine must not stall the drainable batch.
			return out, nil
		}
	}
	return out, nil
}

// MarkPublished satisfies Store.
func (s *PostgresStore) MarkPublished(ctx context.Context, id string) error {
	const q = `UPDATE outbox_events
        SET status='published', published_at=now()
        WHERE id = $1`
	if _, err := s.db.ExecContext(ctx, q, id); err != nil {
		return fmt.Errorf("outbox: MarkPublished: %w", err)
	}
	return nil
}

// MarkFailed satisfies Store.
func (s *PostgresStore) MarkFailed(ctx context.Context, id string, errMsg string) error {
	const q = `UPDATE outbox_events
        SET status='failed',
            retry_count = retry_count + 1,
            last_error = $2,
            last_attempt_at = now()
        WHERE id = $1`
	if _, err := s.db.ExecContext(ctx, q, id, truncate(errMsg, 1000)); err != nil {
		return fmt.Errorf("outbox: MarkFailed: %w", err)
	}
	return nil
}

// Deadletter satisfies Store.
func (s *PostgresStore) Deadletter(ctx context.Context, id, failureReason string, attemptCount int) error {
	const insertQ = `INSERT INTO outbox_dead_letters
        (outbox_event_id, failure_reason, attempt_count, worker_id)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (outbox_event_id) DO NOTHING`
	if _, err := s.db.ExecContext(ctx, insertQ, id, truncate(failureReason, 1000), attemptCount, s.workerID); err != nil {
		return fmt.Errorf("outbox: Deadletter insert: %w", err)
	}
	const updateQ = `UPDATE outbox_events
        SET status='deadlettered',
            last_attempt_at = now()
        WHERE id = $1`
	if _, err := s.db.ExecContext(ctx, updateQ, id); err != nil {
		return fmt.Errorf("outbox: Deadletter update: %w", err)
	}
	return nil
}

// Compile-time check.
var _ Store = (*PostgresStore)(nil)

// -----------------------------------------------------------------------------
// Row helpers — chora-observability outbox_events keeps the legacy
// aggregate_type / aggregate_id columns from migration 0003 (NOT NULL).
// The canonical Row struct doesn't carry them, so we derive sensible
// defaults from EventType.
// -----------------------------------------------------------------------------

// AggregateType derives the aggregate-type column value from EventType.
// Canonical event_type is "{domain}.{aggregate}.{event_type}" so the
// aggregate is the second dotted segment.
func (r Row) AggregateType() string {
	if r.EventType == "" {
		return "event"
	}
	parts := strings.Split(r.EventType, ".")
	if len(parts) >= 2 {
		return parts[1]
	}
	return parts[0]
}

// AggregateID derives the aggregate-id column value. Falls back to the
// row id when the producer doesn't carry a separate aggregate identifier
// (the LedgerHook uses ledger_id which is identical to event_id).
func (r Row) AggregateID() string {
	if r.ID != "" {
		return r.ID
	}
	return r.IdempotencyKey
}

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

// isUniqueViolation returns true if the error is a Postgres unique violation.
// Driver-specific check is deliberately loose (string match on SQLSTATE
// 23505 / "duplicate key value violates unique constraint") so we don't
// bind to pq/pgx at compile time.
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "23505") || strings.Contains(msg, "duplicate key value violates unique constraint")
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
