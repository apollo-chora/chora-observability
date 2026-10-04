// Package cohort is the Cohort aggregate of the analytics slice of
// chora-observability (consolidated at M12.2.E.4 from chora-analytics).
//
// A Cohort is a tenant-scoped, named, criteria-tagged set of GCIDs (learners).
// Membership is materialised by upstream consumers (e.g., the eventaggregator
// or a future cohort-rules engine) and stored here as an idempotent set per
// (tenant_id, cohort_id, gcid).
//
// Multi-tenant isolation: cohort lookups + member queries are tenant-scoped;
// cross-tenant access returns an error.
//
// IDs use UUIDv7 per .claude/rules/ddd-enforcement.md aggregate-invariant #7.
package cohort

import (
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Cohort is the aggregate root.
type Cohort struct {
	ID        string            `json:"id"`
	TenantID  string            `json:"tenant_id"`
	Name      string            `json:"name"`
	Criteria  map[string]string `json:"criteria"`
	CreatedAt time.Time         `json:"created_at"`
}

// ErrInvalidArgument signals validation failures.
var ErrInvalidArgument = errors.New("invalid argument")

// ErrNotFound signals lookup misses.
var ErrNotFound = errors.New("cohort not found")

// New constructs a Cohort with a UUIDv7 id.
func New(tenantID, name string, criteria map[string]string) (*Cohort, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("%w: name required", ErrInvalidArgument)
	}
	copied := make(map[string]string, len(criteria))
	for k, v := range criteria {
		copied[k] = v
	}
	return &Cohort{
		ID:        newUUIDv7(),
		TenantID:  tenantID,
		Name:      name,
		Criteria:  copied,
		CreatedAt: time.Now().UTC(),
	}, nil
}

// Registry is the in-memory tenant-scoped cohort store + membership.
//
// M12+ swaps to chora_observability (analytics tables migrated alongside
// observability migrations at M12.2.E.4).
type Registry struct {
	mu       sync.RWMutex
	byID     map[string]*Cohort              // cohort_id -> cohort
	members  map[string]map[string]time.Time // cohort_id -> gcid -> joined_at
	tenantOf map[string]string               // cohort_id -> tenant_id (cross-tenant guard)
}

// NewRegistry constructs an empty Registry.
func NewRegistry() *Registry {
	return &Registry{
		byID:     make(map[string]*Cohort),
		members:  make(map[string]map[string]time.Time),
		tenantOf: make(map[string]string),
	}
}

// Save inserts or upserts a cohort.
func (r *Registry) Save(c *Cohort) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byID[c.ID] = c
	r.tenantOf[c.ID] = c.TenantID
	if _, ok := r.members[c.ID]; !ok {
		r.members[c.ID] = make(map[string]time.Time)
	}
}

// Get returns a cohort scoped to (tenantID, id). Returns ok=false on miss
// or cross-tenant access.
func (r *Registry) Get(tenantID, id string) (*Cohort, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.byID[id]
	if !ok {
		return nil, false
	}
	if c.TenantID != tenantID {
		return nil, false
	}
	return c, true
}

// AddMember adds a GCID to a cohort. Idempotent (re-adding same gcid is a no-op).
func (r *Registry) AddMember(tenantID, cohortID, gcid string, joinedAt time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.byID[cohortID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, cohortID)
	}
	if c.TenantID != tenantID {
		return fmt.Errorf("%w: cross-tenant access", ErrInvalidArgument)
	}
	if strings.TrimSpace(gcid) == "" {
		return fmt.Errorf("%w: gcid required", ErrInvalidArgument)
	}
	if _, exists := r.members[cohortID][gcid]; exists {
		return nil
	}
	r.members[cohortID][gcid] = joinedAt.UTC()
	return nil
}

// Members returns the (gcid, joined_at) tuples for a cohort.
func (r *Registry) Members(tenantID, cohortID string) ([]Member, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.byID[cohortID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, cohortID)
	}
	if c.TenantID != tenantID {
		return nil, fmt.Errorf("%w: cross-tenant access", ErrInvalidArgument)
	}
	m := r.members[cohortID]
	out := make([]Member, 0, len(m))
	for gcid, ts := range m {
		out = append(out, Member{GCID: gcid, JoinedAt: ts})
	}
	return out, nil
}

// Member is a (gcid, joined_at) tuple.
type Member struct {
	GCID     string
	JoinedAt time.Time
}

// newUUIDv7 returns a freshly generated UUIDv7. Mirrors the format used in
// chora-sharing's domain helper to keep this slice dependency-free.
func newUUIDv7() string {
	const buflen = 16
	var b [buflen]byte
	now := uint64(time.Now().UnixMilli())
	b[0] = byte(now >> 40)
	b[1] = byte(now >> 32)
	b[2] = byte(now >> 24)
	b[3] = byte(now >> 16)
	b[4] = byte(now >> 8)
	b[5] = byte(now)
	if _, err := rand.Read(b[6:]); err != nil {
		for i := 6; i < buflen; i++ {
			b[i] = byte(now >> uint(8*(i-6)))
		}
	}
	b[6] = (b[6] & 0x0F) | 0x70
	b[8] = (b[8] & 0x3F) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
