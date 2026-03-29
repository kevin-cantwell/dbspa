package engine

import (
	"fmt"
	"math"
	"time"
)

// WindowKey identifies a specific window instance.
type WindowKey struct {
	Start time.Time
	End   time.Time
}

// String returns a human-readable window key.
func (wk WindowKey) String() string {
	return fmt.Sprintf("[%s, %s)", wk.Start.Format(time.RFC3339), wk.End.Format(time.RFC3339))
}

// WindowSpec describes the window configuration parsed from the WINDOW clause.
type WindowSpec struct {
	Type    string        // "TUMBLING", "SLIDING", "SESSION"
	Size    time.Duration // window size
	Advance time.Duration // slide interval (SLIDING only); for TUMBLING, equals Size
}

// AssignWindows assigns a record to one or more windows based on its timestamp.
// For TUMBLING: exactly one window.
// For SLIDING: ceil(size/advance) windows.
// For SESSION: not handled here (session windows are managed by SessionWindowManager).
func AssignWindows(ts time.Time, spec WindowSpec) []WindowKey {
	switch spec.Type {
	case "TUMBLING":
		return assignTumbling(ts, spec.Size)
	case "SLIDING":
		return assignSliding(ts, spec.Size, spec.Advance)
	default:
		// SESSION windows are handled differently
		return nil
	}
}

// assignTumbling assigns the record to a single epoch-aligned tumbling window.
// Window: [floor(ts/size)*size, floor(ts/size)*size + size)
func assignTumbling(ts time.Time, size time.Duration) []WindowKey {
	sizeNanos := size.Nanoseconds()
	tsNanos := ts.UnixNano()
	windowStart := (tsNanos / sizeNanos) * sizeNanos
	start := time.Unix(0, windowStart).UTC()
	end := start.Add(size)
	return []WindowKey{{Start: start, End: end}}
}

// assignSliding assigns the record to ceil(size/advance) overlapping windows.
func assignSliding(ts time.Time, size, advance time.Duration) []WindowKey {
	numWindows := int(math.Ceil(float64(size) / float64(advance)))
	advNanos := advance.Nanoseconds()
	tsNanos := ts.UnixNano()

	// The last window that contains this timestamp has start <= ts < start + size.
	// The earliest possible window start is: floor((ts - size + advance) / advance) * advance
	// but we iterate from the window that ts falls into, backward.
	lastWindowStart := (tsNanos / advNanos) * advNanos

	var keys []WindowKey
	for i := 0; i < numWindows; i++ {
		wStart := lastWindowStart - int64(i)*advNanos
		start := time.Unix(0, wStart).UTC()
		end := start.Add(size)
		// Only include windows where ts is actually in [start, end)
		if !ts.Before(start) && ts.Before(end) {
			keys = append(keys, WindowKey{Start: start, End: end})
		}
	}
	return keys
}

// SessionWindowManager tracks session windows for a given group key.
// Session windows merge when a new record arrives within the gap timeout
// of an existing session.
type SessionWindowManager struct {
	Gap      time.Duration
	Sessions []*SessionWindow
}

// SessionWindow is a single session window with its time bounds.
type SessionWindow struct {
	Start time.Time
	End   time.Time // exclusive: last event time + gap
}

// AddEvent adds a record timestamp to the session manager.
// Returns: the resulting window key, merged sessions (to retract), and whether a merge occurred.
func (sm *SessionWindowManager) AddEvent(ts time.Time) (result WindowKey, merged []WindowKey, isMerge bool) {
	eventEnd := ts.Add(sm.Gap)

	// Find overlapping or adjacent sessions.
	// A session overlaps if: session.Start <= ts + gap AND session.End >= ts
	// (i.e., the new event's "extended range" [ts, ts+gap) touches [session.Start, session.End))
	var overlapping []*SessionWindow
	var nonOverlapping []*SessionWindow

	for _, s := range sm.Sessions {
		// Session boundary: gap > timeout means new session (spec 12.3: boundary exclusive)
		if ts.After(s.End) || eventEnd.Before(s.Start) {
			nonOverlapping = append(nonOverlapping, s)
		} else {
			overlapping = append(overlapping, s)
		}
	}

	// Compute merged bounds
	newStart := ts
	newEnd := eventEnd
	for _, s := range overlapping {
		if s.Start.Before(newStart) {
			newStart = s.Start
		}
		if s.End.After(newEnd) {
			newEnd = s.End
		}
	}

	// Collect merged keys for retraction
	for _, s := range overlapping {
		merged = append(merged, WindowKey{Start: s.Start, End: s.End})
	}

	isMerge = len(overlapping) > 1 || (len(overlapping) == 1 &&
		(overlapping[0].Start != newStart || overlapping[0].End != newEnd))

	// Replace sessions
	resultSession := &SessionWindow{Start: newStart, End: newEnd}
	sm.Sessions = append(nonOverlapping, resultSession)

	return WindowKey{Start: newStart, End: newEnd}, merged, isMerge
}

// ParseDuration parses a SQL duration literal like "1 minute", "30 seconds", "1 hour".
func ParseDuration(s string) (time.Duration, error) {
	var n float64
	var unit string
	_, err := fmt.Sscanf(s, "%f %s", &n, &unit)
	if err != nil {
		// Try single-word forms like "30s", "1m", "1h"
		d, err2 := time.ParseDuration(s)
		if err2 != nil {
			return 0, fmt.Errorf("cannot parse duration %q: expected format like '1 minute' or '30 seconds'", s)
		}
		return d, nil
	}

	switch unit {
	case "second", "seconds", "sec", "secs", "s":
		return time.Duration(n * float64(time.Second)), nil
	case "minute", "minutes", "min", "mins", "m":
		return time.Duration(n * float64(time.Minute)), nil
	case "hour", "hours", "hr", "hrs", "h":
		return time.Duration(n * float64(time.Hour)), nil
	case "day", "days", "d":
		return time.Duration(n * 24 * float64(time.Hour)), nil
	case "millisecond", "milliseconds", "ms":
		return time.Duration(n * float64(time.Millisecond)), nil
	default:
		return 0, fmt.Errorf("unknown duration unit %q in %q", unit, s)
	}
}
