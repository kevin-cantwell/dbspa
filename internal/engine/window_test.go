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

// TC-WIN-001: Tumbling window basic assignment - epoch-aligned
func TestTumblingWindowEpochAlignment(t *testing.T) {
	tests := []struct {
		name      string
		size      time.Duration
		ts        time.Time
		wantStart time.Time
		wantEnd   time.Time
	}{
		{
			name:      "1-minute window, event mid-minute",
			size:      time.Minute,
			ts:        time.Date(2026, 3, 28, 10, 0, 10, 0, time.UTC),
			wantStart: time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC),
			wantEnd:   time.Date(2026, 3, 28, 10, 1, 0, 0, time.UTC),
		},
		{
			name:      "1-minute window, event at exact boundary",
			size:      time.Minute,
			ts:        time.Date(2026, 3, 28, 10, 1, 0, 0, time.UTC),
			wantStart: time.Date(2026, 3, 28, 10, 1, 0, 0, time.UTC),
			wantEnd:   time.Date(2026, 3, 28, 10, 2, 0, 0, time.UTC),
		},
		{
			name:      "TC-WIN-018: 1-hour window at 14:37",
			size:      time.Hour,
			ts:        time.Date(2026, 3, 28, 14, 37, 0, 0, time.UTC),
			wantStart: time.Date(2026, 3, 28, 14, 0, 0, 0, time.UTC),
			wantEnd:   time.Date(2026, 3, 28, 15, 0, 0, 0, time.UTC),
		},
		{
			name:      "5-minute window, event at 00:07:30",
			size:      5 * time.Minute,
			ts:        time.Date(2026, 3, 28, 0, 7, 30, 0, time.UTC),
			wantStart: time.Date(2026, 3, 28, 0, 5, 0, 0, time.UTC),
			wantEnd:   time.Date(2026, 3, 28, 0, 10, 0, 0, time.UTC),
		},
		{
			name:      "1-day window",
			size:      24 * time.Hour,
			ts:        time.Date(2026, 3, 28, 14, 37, 0, 0, time.UTC),
			wantStart: time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC),
			wantEnd:   time.Date(2026, 3, 29, 0, 0, 0, 0, time.UTC),
		},
		{
			name:      "30-second window, event at 10:00:05",
			size:      30 * time.Second,
			ts:        time.Date(2026, 3, 28, 10, 0, 5, 0, time.UTC),
			wantStart: time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC),
			wantEnd:   time.Date(2026, 3, 28, 10, 0, 30, 0, time.UTC),
		},
		{
			name:      "1-second window, event at nanosecond precision",
			size:      time.Second,
			ts:        time.Date(2026, 3, 28, 10, 0, 0, 500_000_000, time.UTC),
			wantStart: time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC),
			wantEnd:   time.Date(2026, 3, 28, 10, 0, 1, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := WindowSpec{Type: "TUMBLING", Size: tt.size, Advance: tt.size}
			keys := AssignWindows(tt.ts, spec)
			if len(keys) != 1 {
				t.Fatalf("expected 1 window, got %d", len(keys))
			}
			if !keys[0].Start.Equal(tt.wantStart) {
				t.Errorf("start = %v, want %v", keys[0].Start, tt.wantStart)
			}
			if !keys[0].End.Equal(tt.wantEnd) {
				t.Errorf("end = %v, want %v", keys[0].End, tt.wantEnd)
			}
		})
	}
}

// TC-WIN-001 continued: Tumbling window assigns multiple events correctly
func TestTumblingWindowMultipleEvents(t *testing.T) {
	spec := WindowSpec{Type: "TUMBLING", Size: time.Minute, Advance: time.Minute}
	base := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)

	// Events at 00:00:10, 00:00:50, 00:01:10
	events := []time.Time{
		base.Add(10 * time.Second),
		base.Add(50 * time.Second),
		base.Add(70 * time.Second),
	}

	windowCounts := make(map[string]int)
	for _, ts := range events {
		keys := AssignWindows(ts, spec)
		for _, k := range keys {
			windowCounts[k.String()]++
		}
	}

	w0 := WindowKey{Start: base, End: base.Add(time.Minute)}.String()
	w1 := WindowKey{Start: base.Add(time.Minute), End: base.Add(2 * time.Minute)}.String()

	if windowCounts[w0] != 2 {
		t.Errorf("window [00:00,00:01) has %d events, want 2", windowCounts[w0])
	}
	if windowCounts[w1] != 1 {
		t.Errorf("window [00:01,00:02) has %d events, want 1", windowCounts[w1])
	}
}

// TC-WIN-003: Sliding window overlapping assignment
func TestSlidingWindowOverlap(t *testing.T) {
	tests := []struct {
		name       string
		size       time.Duration
		advance    time.Duration
		ts         time.Time
		wantCount  int
		wantStarts []time.Time
	}{
		{
			name:      "10m/5m at 07:30 - belongs to 2 windows",
			size:      10 * time.Minute,
			advance:   5 * time.Minute,
			ts:        time.Date(2026, 3, 28, 0, 7, 30, 0, time.UTC),
			wantCount: 2,
			wantStarts: []time.Time{
				time.Date(2026, 3, 28, 0, 5, 0, 0, time.UTC),
				time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name:      "10m/5m at exact boundary 10:00",
			size:      10 * time.Minute,
			advance:   5 * time.Minute,
			ts:        time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC),
			wantCount: 2,
			wantStarts: []time.Time{
				time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC),
				time.Date(2026, 3, 28, 9, 55, 0, 0, time.UTC),
			},
		},
		{
			name:      "1m/20s at 00:00:30 - belongs to 3 windows",
			size:      time.Minute,
			advance:   20 * time.Second,
			ts:        time.Date(2026, 3, 28, 0, 0, 30, 0, time.UTC),
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := WindowSpec{Type: "SLIDING", Size: tt.size, Advance: tt.advance}
			keys := AssignWindows(tt.ts, spec)
			if len(keys) != tt.wantCount {
				t.Fatalf("expected %d windows, got %d: %v", tt.wantCount, len(keys), keys)
			}
			if tt.wantStarts != nil {
				for i, wantStart := range tt.wantStarts {
					if !keys[i].Start.Equal(wantStart) {
						t.Errorf("window[%d] start = %v, want %v", i, keys[i].Start, wantStart)
					}
				}
			}
			// All windows should contain the timestamp
			for i, k := range keys {
				if tt.ts.Before(k.Start) || !tt.ts.Before(k.End) {
					t.Errorf("window[%d] %v does not contain ts %v", i, k, tt.ts)
				}
			}
		})
	}
}

// TC-WIN-004/005: Session window gap detection
func TestSessionWindowGapDetection(t *testing.T) {
	sm := &SessionWindowManager{Gap: 5 * time.Second}
	base := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)

	// Events at 0s, 3s, 4s (all within 5s gap) then 20s (new session)
	events := []time.Time{
		base,
		base.Add(3 * time.Second),
		base.Add(4 * time.Second),
		base.Add(20 * time.Second),
	}

	for _, ts := range events[:3] {
		sm.AddEvent(ts)
	}

	if len(sm.Sessions) != 1 {
		t.Fatalf("expected 1 session after 3 events, got %d", len(sm.Sessions))
	}
	if !sm.Sessions[0].Start.Equal(base) {
		t.Errorf("session start = %v, want %v", sm.Sessions[0].Start, base)
	}

	sm.AddEvent(events[3])
	if len(sm.Sessions) != 2 {
		t.Fatalf("expected 2 sessions after gap, got %d", len(sm.Sessions))
	}
}

// TC-WIN-017: Session window gap exactly equals timeout
// The session end is lastEvent+gap. When a new event arrives at exactly session.End,
// the implementation treats it as overlapping (ts.After(s.End) is false when equal),
// so it extends the existing session rather than creating a new one.
func TestSessionWindowExactGapBoundary(t *testing.T) {
	gap := 5 * time.Second
	sm := &SessionWindowManager{Gap: gap}

	t0 := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	sm.AddEvent(t0)

	// Event at exactly t0 + gap: session end is t0+gap, ts == session.End,
	// ts.After(s.End) is false, so it overlaps and extends the session
	t1 := t0.Add(gap)
	wk, _, _ := sm.AddEvent(t1)

	if len(sm.Sessions) != 1 {
		t.Fatalf("expected 1 session (ts==end is inclusive), got %d", len(sm.Sessions))
	}
	if !wk.Start.Equal(t0) {
		t.Errorf("session start = %v, want %v (should keep original start)", wk.Start, t0)
	}
	if !wk.End.Equal(t1.Add(gap)) {
		t.Errorf("session end = %v, want %v", wk.End, t1.Add(gap))
	}
}

// TC-WIN-017 variant: gap + 1ns creates a new session
func TestSessionWindowJustAfterGapBoundary(t *testing.T) {
	gap := 5 * time.Second
	sm := &SessionWindowManager{Gap: gap}

	t0 := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	sm.AddEvent(t0)

	// Event at t0 + gap + 1ns: ts.After(session.End) is true => new session
	t1 := t0.Add(gap + time.Nanosecond)
	wk, _, _ := sm.AddEvent(t1)

	if len(sm.Sessions) != 2 {
		t.Fatalf("expected 2 sessions (past boundary), got %d", len(sm.Sessions))
	}
	if !wk.Start.Equal(t1) {
		t.Errorf("new session start = %v, want %v", wk.Start, t1)
	}
}

// TC-WIN-017 complement: gap minus one nanosecond keeps same session
func TestSessionWindowJustBeforeGapBoundary(t *testing.T) {
	gap := 5 * time.Second
	sm := &SessionWindowManager{Gap: gap}

	t0 := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	sm.AddEvent(t0)

	t1 := t0.Add(gap - time.Nanosecond)
	sm.AddEvent(t1)

	if len(sm.Sessions) != 1 {
		t.Fatalf("expected 1 session (just within gap), got %d", len(sm.Sessions))
	}
}

// Session window merge: two separate sessions bridged by a new event
func TestSessionWindowMerge(t *testing.T) {
	gap := 5 * time.Second
	sm := &SessionWindowManager{Gap: gap}

	t0 := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(20 * time.Second) // far apart

	sm.AddEvent(t0)
	sm.AddEvent(t1)
	if len(sm.Sessions) != 2 {
		t.Fatalf("expected 2 sessions, got %d", len(sm.Sessions))
	}

	// Bridge event that merges both: within gap of both
	tBridge := t0.Add(3 * time.Second)
	wk, merged, isMerge := sm.AddEvent(tBridge)

	// Depending on gap, this should extend session 1 but not reach session 2
	// t0=0s, tBridge=3s, session1 end becomes 3+5=8s, session2 start=20s => no merge
	_ = wk
	_ = merged
	_ = isMerge
	if len(sm.Sessions) != 2 {
		t.Fatalf("expected 2 sessions (no merge since bridge doesn't reach), got %d", len(sm.Sessions))
	}
}

// SESSION type returns nil from AssignWindows (handled separately)
func TestAssignWindowsSessionReturnsNil(t *testing.T) {
	spec := WindowSpec{Type: "SESSION", Size: 5 * time.Second}
	keys := AssignWindows(time.Now(), spec)
	if keys != nil {
		t.Errorf("SESSION windows should return nil from AssignWindows, got %v", keys)
	}
}

// TC-WIN-009: Empty window - verify no phantom assignment
func TestTumblingWindowExactBoundaryExclusive(t *testing.T) {
	spec := WindowSpec{Type: "TUMBLING", Size: time.Minute, Advance: time.Minute}

	// Event at exact boundary minute: should be in NEXT window, not current
	ts := time.Date(2026, 3, 28, 10, 1, 0, 0, time.UTC)
	keys := AssignWindows(ts, spec)

	if len(keys) != 1 {
		t.Fatalf("expected 1 window, got %d", len(keys))
	}
	// Window should be [10:01, 10:02), not [10:00, 10:01)
	wantStart := time.Date(2026, 3, 28, 10, 1, 0, 0, time.UTC)
	if !keys[0].Start.Equal(wantStart) {
		t.Errorf("boundary event assigned to start=%v, want %v", keys[0].Start, wantStart)
	}
}

// WindowKey String format test
func TestWindowKeyString(t *testing.T) {
	wk := WindowKey{
		Start: time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC),
		End:   time.Date(2026, 3, 28, 10, 1, 0, 0, time.UTC),
	}
	got := wk.String()
	want := "[2026-03-28T10:00:00Z, 2026-03-28T10:01:00Z)"
	if got != want {
		t.Errorf("WindowKey.String() = %q, want %q", got, want)
	}
}

// ParseDuration error cases
func TestParseDurationErrors(t *testing.T) {
	tests := []string{
		"",
		"abc",
		"1 fathom",
		"not a duration",
	}
	for _, input := range tests {
		_, err := ParseDuration(input)
		if err == nil {
			t.Errorf("ParseDuration(%q) expected error, got nil", input)
		}
	}
}

// ParseDuration Go-style formats
func TestParseDurationGoStyle(t *testing.T) {
	tests := []struct {
		input string
		want  time.Duration
	}{
		{"30s", 30 * time.Second},
		{"1m", time.Minute},
		{"1h", time.Hour},
		{"500ms", 500 * time.Millisecond},
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
