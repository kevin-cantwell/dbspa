package engine

import (
	"encoding/json"
	"sort"
	"time"
)

// Record represents a single data record flowing through the pipeline.
// Every record carries a diff (+1 for insert, -1 for retract) and a timestamp.
type Record struct {
	// Columns maps column names to their values.
	Columns map[string]Value
	// Timestamp is the record's processing or event time.
	Timestamp time.Time
	// Diff is +1 for insert, -1 for retract.
	Diff int8
}

// NewRecord creates a new insert record with the given columns and current time.
func NewRecord(columns map[string]Value) Record {
	return Record{
		Columns:   columns,
		Timestamp: time.Now(),
		Diff:      1,
	}
}

// Get returns the value for the given column name, or NullValue if not found.
func (r Record) Get(name string) Value {
	if v, ok := r.Columns[name]; ok {
		return v
	}
	return NullValue{}
}

// RecordFingerprint returns a deterministic string fingerprint of the record's columns,
// suitable for use as a deduplication key.
func RecordFingerprint(r Record) string {
	keys := make([]string, 0, len(r.Columns))
	for k := range r.Columns {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]any, 0, len(keys)*2)
	for _, k := range keys {
		parts = append(parts, k)
		v := r.Columns[k]
		if v == nil || v.IsNull() {
			parts = append(parts, nil)
		} else {
			parts = append(parts, v.ToJSON())
		}
	}
	data, _ := json.Marshal(parts)
	return string(data)
}
