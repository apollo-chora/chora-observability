// role_gate.go — the first role gate in chora-observability (CHO-2148).
//
// Until now this service had NO role concept: its middleware read X-Tenant-Id
// and traceparent and nothing else. The platform egress kill-switch changes
// that, because it disables external web egress for EVERY tenant at once and so
// must be reachable by a PLATFORM_OPERATOR and by nobody else.
//
// The gate reads the gateway-propagated `x-mesh-user-roles` mesh header
// (servicemesh.HeaderUserRoles) and FAILS CLOSED. That header choice is not
// arbitrary — see [[reusable_tenancy_authz_mesh_roles_not_xrole_failopen]]:
// chora-tenancy's twelve admin gates were built on `X-Role`, which the gateway
// never stamps, and their helper returned TRUE on an absent header. Every one of
// those gates was silently unenforced in production until CHO-2072. Do not
// reintroduce that shape here: an absent or unrecognised role DENIES.
//
// Note the asymmetry with chora-tenancy's callerHoldsHPlusAdminRole, which
// deliberately does NOT admit platform_operator: that gate protects TENANT data,
// and the operator is non-tenant-scoped (CHO-2076). This gate protects a
// PLATFORM control, which is precisely what the operator role is for.
package httpadapter

import (
	"net/http"
	"strings"

	"github.com/5007-Capstone/chora/libs/chora-go-common/auth/servicemesh"
)

// RolePlatformOperator is the sole role admitted to platform-wide runtime
// controls.
const RolePlatformOperator = "platform_operator"

// callerIsPlatformOperator reports whether the validated mesh roles carry
// platform_operator. Fail-CLOSED: an absent or empty header denies.
// Case-insensitive, exact per-entry match (a superset name like
// "platform_operator_readonly" does NOT pass).
func callerIsPlatformOperator(h http.Header) bool {
	raw := strings.TrimSpace(h.Get(servicemesh.HeaderUserRoles))
	if raw == "" {
		return false
	}
	for _, part := range strings.Split(raw, ",") {
		role := strings.ToLower(strings.TrimSpace(part))
		role = strings.ReplaceAll(role, "-", "_")
		if role == RolePlatformOperator {
			return true
		}
	}
	return false
}

// callerRoles returns the validated mesh roles as a slice (trimmed, raw
// spelling; the domain matrix normalises). Empty when the header is absent, so
// any matrix check fails closed.
func callerRoles(h http.Header) []string {
	raw := strings.TrimSpace(h.Get(servicemesh.HeaderUserRoles))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// callerGCID returns the acting principal's GCID from the mesh headers. An
// egress kill-switch flip is never anonymous, so the handler refuses when this
// is empty.
func callerGCID(h http.Header) string {
	if v := strings.TrimSpace(h.Get(servicemesh.HeaderGCID)); v != "" {
		return v
	}
	// Legacy/compat spellings the gateway also stamps on some paths.
	if v := strings.TrimSpace(h.Get("X-GCID")); v != "" {
		return v
	}
	return strings.TrimSpace(h.Get("gcid"))
}
