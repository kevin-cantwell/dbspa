package engine

import (
	"fmt"
	"log"
	"strconv"
	"sync"
)

// SchemaTracker tracks column types across records and detects drift.
// It records the first-seen type for each column and reports when subsequent
// records contain a different type for the same column. Compatible changes
// (e.g., INT → FLOAT) produce a warning; incompatible changes (e.g., INT → BOOL)
// produce an error.
type SchemaTracker struct {
	columns map[string]string // column name → first-seen type
	warned  map[string]bool   // columns we've already warned about (to rate-limit)
	mu      sync.Mutex
}

// NewSchemaTracker creates a new SchemaTracker.
func NewSchemaTracker() *SchemaTracker {
	return &SchemaTracker{
		columns: make(map[string]string),
		warned:  make(map[string]bool),
	}
}

// Track checks a record's columns against the expected schema.
// Returns an error for incompatible type changes. Compatible changes
// are logged as warnings to stderr (rate-limited: one warning per column).
func (t *SchemaTracker) Track(rec Record) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for name, val := range rec.Columns {
		if val == nil || val.IsNull() {
			continue // NULL is compatible with any type
		}
		newType := val.Type()
		firstType, seen := t.columns[name]
		if !seen {
			t.columns[name] = newType
			continue
		}
		if newType == firstType {
			continue
		}

		// Type changed — check compatibility
		if isCompatibleDrift(firstType, newType, val) {
			if !t.warned[name] {
				t.warned[name] = true
				log.Printf("Warning: column %q type changed from %s to %s (compatible, continuing)", name, firstType, newType)
			}
			continue
		}

		return fmt.Errorf("schema drift: column %q changed from %s to %s (incompatible)", name, firstType, newType)
	}
	return nil
}

// isCompatibleDrift returns true if changing from oldType to newType is a
// compatible (lossy but acceptable) drift. The val parameter is the actual
// value, used to check whether TEXT values are parseable as numbers.
func isCompatibleDrift(oldType, newType string, val Value) bool {
	// BOOL is incompatible with everything else
	if oldType == "BOOL" || newType == "BOOL" {
		return false
	}
	// JSON is incompatible with everything else
	if oldType == "JSON" || newType == "JSON" {
		return false
	}

	switch {
	case oldType == "INT" && newType == "FLOAT":
		return true
	case oldType == "FLOAT" && newType == "INT":
		return true // precision loss, but acceptable
	case oldType == "INT" && newType == "TEXT":
		return isNumericText(val)
	case oldType == "FLOAT" && newType == "TEXT":
		return isNumericText(val)
	case oldType == "TEXT" && (newType == "INT" || newType == "FLOAT"):
		return true // numeric types from what was text — fine
	case oldType == "TEXT" && newType == "TIMESTAMP":
		return true
	case oldType == "TIMESTAMP" && newType == "TEXT":
		return true
	default:
		return false
	}
}

// isNumericText returns true if the value is a TextValue that can be parsed
// as a number.
func isNumericText(val Value) bool {
	tv, ok := val.(TextValue)
	if !ok {
		return false
	}
	_, err := strconv.ParseFloat(tv.V, 64)
	return err == nil
}
