// Package cohort_test exercises the cohort definition + membership query
// domain. Cohorts are tenant-scoped sets of GCIDs sharing membership criteria.
//
// Migrated from services/chora-analytics/internal/domain/cohort at M12.2.E.4.
package cohort_test

import (
	"testing"
	"time"

	"github.com/apollo-chora/chora-observability/internal/analytics/cohort"
)

func TestNewCohort_Validates(t *testing.T) {
	cases := []struct {
		name   string
		tenant string
		cName  string
		wantOK bool
	}{
		{"happy", "t1", "Q2 Onboarders", true},
		{"blank tenant", "", "x", false},
		{"blank name", "t1", "", false},
		{"whitespace name", "t1", "   ", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := cohort.New(tc.tenant, tc.cName, map[string]string{"min_atoms": "5"})
			if tc.wantOK {
				if err != nil {
					t.Fatalf("New: %v", err)
				}
				if c.ID == "" {
					t.Error("ID empty")
				}
				if c.TenantID != tc.tenant {
					t.Errorf("TenantID=%q want %q", c.TenantID, tc.tenant)
				}
				if c.Name != tc.cName {
					t.Errorf("Name=%q want %q", c.Name, tc.cName)
				}
				if c.CreatedAt.IsZero() {
					t.Error("CreatedAt zero")
				}
			} else if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestNewCohort_AssignsUUIDv7(t *testing.T) {
	c, err := cohort.New("t1", "x", nil)
	if err != nil {
		t.Fatal(err)
	}
	// UUIDv7 is 36 chars: 8-4-4-4-12
	if len(c.ID) != 36 {
		t.Errorf("ID length=%d want 36", len(c.ID))
	}
	// version nibble at idx 14 is '7'
	if c.ID[14] != '7' {
		t.Errorf("ID[14]=%q want '7' (UUIDv7 version)", c.ID[14])
	}
}

func TestNewCohort_DefensiveCopiesCriteria(t *testing.T) {
	src := map[string]string{"a": "1"}
	c, err := cohort.New("t1", "x", src)
	if err != nil {
		t.Fatal(err)
	}
	src["a"] = "MUTATED"
	if c.Criteria["a"] != "1" {
		t.Errorf("criteria mutation leaked: got %q", c.Criteria["a"])
	}
}

func TestRegistry_AddThenGet(t *testing.T) {
	r := cohort.NewRegistry()
	c, err := cohort.New("t1", "Q2 cohort", map[string]string{"min_atoms": "5"})
	if err != nil {
		t.Fatal(err)
	}
	r.Save(c)
	got, ok := r.Get("t1", c.ID)
	if !ok {
		t.Fatal("not found")
	}
	if got.ID != c.ID {
		t.Errorf("ID mismatch")
	}
}

func TestRegistry_TenantIsolation(t *testing.T) {
	r := cohort.NewRegistry()
	c, _ := cohort.New("t1", "x", nil)
	r.Save(c)
	if _, ok := r.Get("t2", c.ID); ok {
		t.Error("tenant isolation breach")
	}
}

func TestRegistry_AddMembersAndQuery(t *testing.T) {
	r := cohort.NewRegistry()
	c, _ := cohort.New("t1", "x", nil)
	r.Save(c)

	now := time.Now().UTC()
	if err := r.AddMember(c.TenantID, c.ID, "01900000-0000-7000-8000-000000000001", now); err != nil {
		t.Fatalf("AddMember: %v", err)
	}
	if err := r.AddMember(c.TenantID, c.ID, "01900000-0000-7000-8000-000000000002", now); err != nil {
		t.Fatalf("AddMember: %v", err)
	}
	// Re-add same member is idempotent
	if err := r.AddMember(c.TenantID, c.ID, "01900000-0000-7000-8000-000000000001", now); err != nil {
		t.Fatalf("AddMember dup: %v", err)
	}

	members, err := r.Members(c.TenantID, c.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 2 {
		t.Errorf("len=%d want 2", len(members))
	}
}

func TestRegistry_AddMember_RejectsUnknownCohort(t *testing.T) {
	r := cohort.NewRegistry()
	err := r.AddMember("t1", "00000000-0000-0000-0000-000000000000", "g", time.Now())
	if err == nil {
		t.Fatal("expected error for unknown cohort")
	}
}

func TestRegistry_Members_RejectsCrossTenant(t *testing.T) {
	r := cohort.NewRegistry()
	c, _ := cohort.New("t1", "x", nil)
	r.Save(c)
	_ = r.AddMember("t1", c.ID, "g", time.Now())
	if _, err := r.Members("t-other", c.ID); err == nil {
		t.Error("expected cross-tenant error")
	}
}
