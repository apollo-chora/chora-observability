// Package outbox exercises the unexported helpers the external suite cannot
// reach directly: parseTime with every fallback branch and the stdlib-log
// default logger methods.
package outbox

import (
	"testing"
	"time"
)

func TestParseTime(t *testing.T) {
	t.Parallel()
	fallback := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)

	// empty -> fallback
	if got := parseTime("", fallback); !got.Equal(fallback) {
		t.Errorf("parseTime(empty) = %v; want fallback", got)
	}
	// RFC3339Nano with fractional seconds
	nanos := "2026-05-01T12:00:00.123456789Z"
	if got := parseTime(nanos, fallback); got.Format("2006-01-02T15:04:05.999999999Z07:00") != "2026-05-01T12:00:00.123456789Z" {
		t.Errorf("parseTime(nanos) = %v", got)
	}
	// RFC3339 (no fractional)
	if got := parseTime("2026-05-01T12:00:00Z", fallback); got.Hour() != 12 {
		t.Errorf("parseTime(rfc3339) = %v", got)
	}
	// invalid -> fallback
	if got := parseTime("not-a-time", fallback); !got.Equal(fallback) {
		t.Errorf("parseTime(invalid) = %v; want fallback", got)
	}
}

func TestDefaultLogger_Methods(t *testing.T) {
	t.Parallel()
	var l defaultLogger
	l.Infof("info %d", 1)
	l.Warnf("warn %s", "x")
	l.Errorf("error %v", "y")
}