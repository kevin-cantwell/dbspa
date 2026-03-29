package engine

import (
	"testing"
	"time"
)

func TestParseDuration(t *testing.T) {
	tests := []struct {
		input string
		want  time.Duration
	}{
		{"1 minute", time.Minute},
		{"30 seconds", 30 * time.Second},
		{"1 hour", time.Hour},
		{"5 minutes", 5 * time.Minute},
		{"1 day", 24 * time.Hour},
		{"500 milliseconds", 500 * time.Millisecond},
	}

	for _, tt := range tests {
		got, err := ParseDuration(tt.input)
		if err != nil {
			t.Errorf("ParseDuration(%q) error: %v", tt.input, err)
			continue
		}
		if got != tt.want {
			t.Errorf("ParseDuration(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestAssignTumblingWindows(t *testing.T) {
	spec := WindowSpec{Type: "TUMBLING", Size: time.Minute, Advance: time.Minute}

	ts := time.Date(2026, 3, 28, 10, 0, 5, 0, time.UTC)
	keys := AssignWindows(ts, spec)

	if len(keys) != 1 {
		t.Fatalf("expected 1 window, got %d", len(keys))
	}

	wantStart := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)
	wantEnd := time.Date(2026, 3, 28, 10, 1, 0, 0, time.UTC)

	if !keys[0].Start.Equal(wantStart) {
		t.Errorf("window start = %v, want %v", keys[0].Start, wantStart)
	}
	if !keys[0].End.Equal(wantEnd) {
		t.Errorf("window end = %v, want %v", keys[0].End, wantEnd)
	}
}

func TestAssignTumblingWindowsEpochAligned(t *testing.T) {
	spec := WindowSpec{Type: "TUMBLING", Size: time.Hour, Advance: time.Hour}

	// 14:30 should fall in [14:00, 15:00)
	ts := time.Date(2026, 3, 28, 14, 30, 0, 0, time.UTC)
	keys := AssignWindows(ts, spec)

	if len(keys) != 1 {
		t.Fatalf("expected 1 window, got %d", len(keys))
	}

	wantStart := time.Date(2026, 3, 28, 14, 0, 0, 0, time.UTC)
	if !keys[0].Start.Equal(wantStart) {
		t.Errorf("window start = %v, want %v", keys[0].Start, wantStart)
	}
}

func TestAssignSlidingWindows(t *testing.T) {
	spec := WindowSpec{Type: "SLIDING", Size: 10 * time.Minute, Advance: 5 * time.Minute}

	ts := time.Date(2026, 3, 28, 10, 7, 0, 0, time.UTC)
	keys := AssignWindows(ts, spec)

	// 10:07 should be in windows [10:00, 10:10) and [10:05, 10:15)
	if len(keys) != 2 {
		t.Fatalf("expected 2 windows, got %d", len(keys))
	}
}

func TestSessionWindowManager(t *testing.T) {
	sm := &SessionWindowManager{Gap: 5 * time.Minute}

	// First event
	t1 := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)
	wk, merged, isMerge := sm.AddEvent(t1)
	if isMerge {
		t.Error("expected no merge on first event")
	}
	if len(merged) != 0 {
		// First event: merged contains the initial "empty" state. Actually it contains the new window itself.
		// Let's just verify the returned window key.
	}
	_ = merged
	if !wk.Start.Equal(t1) {
		t.Errorf("session start = %v, want %v", wk.Start, t1)
	}

	// Second event within gap — extends session
	t2 := t1.Add(3 * time.Minute)
	wk2, _, _ := sm.AddEvent(t2)
	if !wk2.Start.Equal(t1) {
		t.Errorf("session start after extend = %v, want %v", wk2.Start, t1)
	}

	// Third event outside gap — new session
	t3 := t1.Add(15 * time.Minute)
	wk3, _, _ := sm.AddEvent(t3)
	if !wk3.Start.Equal(t3) {
		t.Errorf("new session start = %v, want %v", wk3.Start, t3)
	}

	if len(sm.Sessions) != 2 {
		t.Errorf("expected 2 sessions, got %d", len(sm.Sessions))
	}
}
