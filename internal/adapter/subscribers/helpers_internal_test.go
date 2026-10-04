// Package subscribers exercises the unexported event-projection helpers:
// deriveEventType topic parsing, fallbackString, truncateToDay and cloneMap.
package subscribers

import (
	"testing"
	"time"
)

func TestDeriveEventType(t *testing.T) {
	t.Parallel()
	if got := deriveEventType("chora.consumption.familiar.exp_awarded.v1"); got != "exp_awarded" {
		t.Errorf("deriveEventType = %q; want exp_awarded", got)
	}
	if got := deriveEventType("short.topic"); got != "short.topic" {
		t.Errorf("short topic = %q; want identity", got)
	}
}

func TestFallbackString(t *testing.T) {
	t.Parallel()
	if got := fallbackString("", "def"); got != "def" {
		t.Errorf("empty = %q; want def", got)
	}
	if got := fallbackString("   ", "def"); got != "def" {
		t.Errorf("blank = %q; want def", got)
	}
	if got := fallbackString("value", "def"); got != "value" {
		t.Errorf("set = %q; want value", got)
	}
}

func TestTruncateToDay(t *testing.T) {
	t.Parallel()
	in := time.Date(2026, 5, 13, 14, 45, 30, 123, time.UTC)
	got := truncateToDay(in)
	want := time.Date(2026, 5, 13, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("truncateToDay = %v; want %v", got, want)
	}
}

func TestCloneMap(t *testing.T) {
	t.Parallel()
	if got := cloneMap(nil); got == nil || len(got) != 0 {
		t.Errorf("cloneMap(nil) = %v; want empty non-nil map", got)
	}
	in := map[string]any{"a": 1, "b": "x"}
	got := cloneMap(in)
	got["a"] = 99
	if in["a"] != 1 {
		t.Error("cloneMap must deep-copy the top-level map")
	}
	if len(got) != 2 {
		t.Errorf("cloneMap size = %d; want 2", len(got))
	}
}
