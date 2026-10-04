// Package httpadapter wires net/http handlers to the Observability domain.
//
// Middleware chain (outermost first):
//
//	logging -> tenantContext -> traceparent -> mux
//
//   - logging: per-request method/path log + structured trace correlation
//   - tenantContext: extracts X-Tenant-Id; rejects /api/* without it
//   - traceparent: extracts and echoes the W3C traceparent header
package httpadapter

import (
	"context"
	"log"
	"net/http"
	"strings"
)

type ctxKey string

const (
	ctxKeyTenantID    ctxKey = "tenant_id"
	ctxKeyTraceparent ctxKey = "traceparent"
)

// tenantContext extracts the X-Tenant-Id header and stores it on the request
// context. Rejects protected paths without the header.
func tenantContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isPublicPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		tenantID := strings.TrimSpace(r.Header.Get("X-Tenant-Id"))
		if tenantID == "" {
			writeError(w, http.StatusBadRequest, "OBS_TENANT_REQUIRED",
				"X-Tenant-Id header is required")
			return
		}
		ctx := context.WithValue(r.Context(), ctxKeyTenantID, tenantID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// traceparent extracts the W3C traceparent header (if present) and echoes it
// back on the response so clients can correlate with their own trace ID. Also
// stores it on context for downstream use.
func traceparent(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tp := strings.TrimSpace(r.Header.Get("traceparent"))
		if tp != "" {
			w.Header().Set("traceparent", tp)
			ctx := context.WithValue(r.Context(), ctxKeyTraceparent, tp)
			r = r.WithContext(ctx)
		}
		next.ServeHTTP(w, r)
	})
}

// logging logs each dispatched request.
func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenant := r.Header.Get("X-Tenant-Id")
		if tenant != "" {
			log.Printf("method=%s path=%s tenant=%s", r.Method, r.URL.Path, tenant)
		} else {
			log.Printf("method=%s path=%s", r.Method, r.URL.Path)
		}
		next.ServeHTTP(w, r)
	})
}

func isPublicPath(p string) bool {
	switch p {
	case "/", "/healthz", "/healthz/", "/health", "/readyz":
		return true
	}
	return false
}

// tenantFromContext returns the tenant_id stored by tenantContext.
func tenantFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeyTenantID).(string)
	return v
}

// traceparentFromContext returns the traceparent header value stored on ctx.
func traceparentFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeyTraceparent).(string)
	return v
}
