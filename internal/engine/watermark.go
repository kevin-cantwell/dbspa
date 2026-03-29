package engine

import (
	"sync"
	"time"
)

// WatermarkTracker tracks event-time watermarks across partitions.
// The watermark is the minimum event time across all active partitions
// minus the watermark delay.
type WatermarkTracker struct {
	mu    sync.Mutex
	delay time.Duration

	// partitions tracks the max event time seen per partition.
	partitions map[int]*partitionState

	// watermark is the current global watermark (monotonically non-decreasing).
	watermark time.Time

	// stallTimeout is how long a partition can be idle before being excluded
	// from the minimum calculation.
	stallTimeout time.Duration
}

type partitionState struct {
	maxEventTime time.Time
	lastSeen     time.Time // wall-clock time of last record
}

// NewWatermarkTracker creates a watermark tracker with the given delay.
// If delay is 0, defaults to 5 seconds per spec.
func NewWatermarkTracker(delay time.Duration) *WatermarkTracker {
	if delay == 0 {
		delay = 5 * time.Second
	}
	return &WatermarkTracker{
		delay:        delay,
		partitions:   make(map[int]*partitionState),
		stallTimeout: 30 * time.Second,
	}
}

// Advance updates the watermark given a new event time from a partition.
// For stdin (single partition), use partitionID=0.
// Returns the new watermark value.
func (wt *WatermarkTracker) Advance(partitionID int, eventTime time.Time) time.Time {
	wt.mu.Lock()
	defer wt.mu.Unlock()

	now := time.Now()

	ps, ok := wt.partitions[partitionID]
	if !ok {
		ps = &partitionState{}
		wt.partitions[partitionID] = ps
	}

	if eventTime.After(ps.maxEventTime) {
		ps.maxEventTime = eventTime
	}
	ps.lastSeen = now

	// Compute minimum across active partitions
	var minTime time.Time
	first := true
	for _, p := range wt.partitions {
		// Exclude stalled partitions (no data for > stallTimeout)
		if now.Sub(p.lastSeen) > wt.stallTimeout {
			continue
		}
		if first || p.maxEventTime.Before(minTime) {
			minTime = p.maxEventTime
		}
		first = false
	}

	if first {
		// No active partitions — don't advance
		return wt.watermark
	}

	candidate := minTime.Add(-wt.delay)

	// Monotonically non-decreasing
	if candidate.After(wt.watermark) {
		wt.watermark = candidate
	}

	return wt.watermark
}

// Watermark returns the current watermark value.
func (wt *WatermarkTracker) Watermark() time.Time {
	wt.mu.Lock()
	defer wt.mu.Unlock()
	return wt.watermark
}

// ShouldClose returns true if the watermark has advanced past windowEnd,
// meaning the window should be closed.
func (wt *WatermarkTracker) ShouldClose(windowEnd time.Time) bool {
	wt.mu.Lock()
	defer wt.mu.Unlock()
	return !wt.watermark.Before(windowEnd) // watermark >= windowEnd
}
