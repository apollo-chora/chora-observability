// runtime_internal_test.go — in-package tests for the RLS tenant helpers.
// Package pg (not pg_test) so the unexported validateTenantIDLiteral is
// reachable.
package pg

import "testing"

func TestNormalizeTenantForRLS(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"platform":                             NilTenantUUID,
		"":                                     NilTenantUUID,
		"01963e7a-0000-7000-8000-000000000abc": "01963e7a-0000-7000-8000-000000000abc",
	}
	for in, want := range cases {
		if got := NormalizeTenantForRLS(in); got != want {
			t.Errorf("NormalizeTenantForRLS(%q) = %q; want %q", in, got, want)
		}
	}
}

func TestValidateTenantIDLiteral(t *testing.T) {
	t.Parallel()
	ok := []string{
		NilTenantUUID,
		"01963e7a-0000-7000-8000-000000000abc",
		"abcDEF123",
		// "platform" is char-valid (lowercase letters) — it's a literal-
		// safety check, not a UUID-shape check. Callers MUST
		// NormalizeTenantForRLS first; an un-normalised "platform" would
		// only fail later at the `::uuid` RLS cast, never here.
		"platform",
	}
	for _, id := range ok {
		if err := validateTenantIDLiteral(id); err != nil {
			t.Errorf("validateTenantIDLiteral(%q) = %v; want nil", id, err)
		}
	}
	bad := []string{
		"",                       // empty
		"'; DROP TABLE x; --",    // injection
		"00000000 0000",          // space
		"tenant_with_underscore", // underscore not allowed
	}
	for _, id := range bad {
		if err := validateTenantIDLiteral(id); err == nil {
			t.Errorf("validateTenantIDLiteral(%q) = nil; want error", id)
		}
	}
}
