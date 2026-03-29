package engine

import (
	"testing"
	"time"
)

func TestWatermarkTracker(t *testing.T) {
	wt := NewWatermarkTracker(5 * time.Second)

	base := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)

	// Advance with first event
	wm := wt.Advance(0, base)
	// watermark = base - 5s
	expected := base.Add(-5 * time.Second)
	if !wm.Equal(expected) {
		t.Errorf("watermark = %v, want %v", wm, expected)
	}

	// Advance further
	wm = wt.Advance(0, base.Add(10*time.Second))
	expected = base.Add(5 * time.Second)
	if !wm.Equal(expected) {
		t.Errorf("watermark = %v, want %v", wm, expected)
	}

	// Watermark should be monotonically non-decreasing
	// Sending an older event time should not decrease the watermark
	wm = wt.Advance(0, base.Add(3*time.Second))
	if wm.Before(expected) {
		t.Errorf("watermark went backward: %v < %v", wm, expected)
	}
}

func TestWatermarkShouldClose(t *testing.T) {
	wt := NewWatermarkTracker(1 * time.Millisecond) // tiny delay

	base := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)
	windowEnd := base.Add(time.Minute)

	// Before advancing, watermark is zero time — should not close
	if wt.ShouldClose(windowEnd) {
		t.Error("should not close before any events")
	}

	// Advance to just before window end (with 1ms delay, need to be just before)
	wt.Advance(0, windowEnd.Add(-time.Second))
	if wt.ShouldClose(windowEnd) {
		t.Error("should not close when watermark < window end")
	}

	// Advance well past window end
	wt.Advance(0, windowEnd.Add(10*time.Second))
	if !wt.ShouldClose(windowEnd) {
		t.Error("should close when watermark >= window end")
	}
}

// TC-WIN-013: Multi-partition watermark (minimum across partitions)
// Note: watermark is monotonically non-decreasing. If partition 0 advances first
// to a high value, the watermark will be set high and never go back down.
// To test the "min across partitions" property, we must advance the lagging
// partition first so the watermark starts low.
func TestWatermarkMultiPartition(t *testing.T) {
	delay := 2 * time.Second
	wt := NewWatermarkTracker(delay)

	base := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)

	// Advance lagging partition first
	wt.Advance(1, base.Add(80*time.Second))
	wm := wt.Advance(0, base.Add(100*time.Second))

	// Watermark should be min(100, 80) - delay = base + 78s
	want := base.Add(78 * time.Second)
	if !wm.Equal(want) {
		t.Errorf("multi-partition watermark = %v, want %v (min across partitions)", wm, want)
	}
}

// Multi-partition: watermark advances when lagging partition catches up
func TestWatermarkMultiPartitionAdvance(t *testing.T) {
	delay := 5 * time.Second
	wt := NewWatermarkTracker(delay)

	base := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)

	// Advance lagging partition first to keep watermark low
	wt.Advance(1, base.Add(50*time.Second))
	wm1 := wt.Advance(0, base.Add(100*time.Second))

	// watermark = min(100,50) - 5 = 45s
	want1 := base.Add(45 * time.Second)
	if !wm1.Equal(want1) {
		t.Errorf("watermark after first round = %v, want %v", wm1, want1)
	}

	// Partition 1 advances to 120s
	wm2 := wt.Advance(1, base.Add(120*time.Second))

	// watermark = min(100,120) - 5 = 95s
	want2 := base.Add(95 * time.Second)
	if !wm2.Equal(want2) {
		t.Errorf("watermark after partition1 advance = %v, want %v", wm2, want2)
	}
}

// Monotonicity: watermark never decreases
func TestWatermarkMonotonicity(t *testing.T) {
	wt := NewWatermarkTracker(2 * time.Second)
	base := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)

	wm1 := wt.Advance(0, base.Add(100*time.Second))
	wm2 := wt.Advance(0, base.Add(50*time.Second)) // older event
	wm3 := wt.Advance(0, base.Add(30*time.Second)) // even older

	if wm2.Before(wm1) {
		t.Errorf("watermark decreased: %v < %v", wm2, wm1)
	}
	if wm3.Before(wm2) {
		t.Errorf("watermark decreased: %v < %v", wm3, wm2)
	}
	// All should equal the first watermark (monotonic non-decreasing)
	if !wm1.Equal(wm2) || !wm2.Equal(wm3) {
		t.Errorf("watermark should stay constant: wm1=%v, wm2=%v, wm3=%v", wm1, wm2, wm3)
	}
}

// TC-WIN-015: Default watermark delay is 5 seconds
func TestWatermarkDefaultDelay(t *testing.T) {
	wt := NewWatermarkTracker(0) // 0 means use default

	base := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)
	wm := wt.Advance(0, base.Add(30*time.Second))

	// Default delay is 5s, so watermark = 30s - 5s = 25s
	want := base.Add(25 * time.Second)
	if !wm.Equal(want) {
		t.Errorf("default delay watermark = %v, want %v (default 5s delay)", wm, want)
	}
}

// ShouldClose: exact boundary
func TestWatermarkShouldCloseExactBoundary(t *testing.T) {
	wt := NewWatermarkTracker(0)

	base := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)
	windowEnd := base.Add(time.Minute)

	// Advance watermark to exactly window end + delay
	// With default 5s delay: need event at windowEnd + 5s to get watermark = windowEnd
	wt.Advance(0, windowEnd.Add(5*time.Second))
	if !wt.ShouldClose(windowEnd) {
		t.Error("ShouldClose should return true when watermark == window end")
	}
}

// Watermark returns zero before any events
func TestWatermarkInitialZero(t *testing.T) {
	wt := NewWatermarkTracker(5 * time.Second)
	wm := wt.Watermark()
	if !wm.IsZero() {
		t.Errorf("initial watermark should be zero, got %v", wm)
	}
}

// Three partitions: watermark is min across all active partitions
func TestWatermarkThreePartitions(t *testing.T) {
	delay := 3 * time.Second
	wt := NewWatermarkTracker(delay)
	base := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)

	// Advance lowest partition first to keep watermark low
	wt.Advance(2, base.Add(50*time.Second))
	wt.Advance(0, base.Add(100*time.Second))
	wm := wt.Advance(1, base.Add(200*time.Second))

	// min is partition 2 at 50s, delay 3s => 47s
	want := base.Add(47 * time.Second)
	if !wm.Equal(want) {
		t.Errorf("3-partition watermark = %v, want %v", wm, want)
	}
}
