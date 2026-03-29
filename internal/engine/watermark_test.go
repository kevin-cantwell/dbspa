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
