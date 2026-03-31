package engine

import (
	"encoding/json"
	"sort"
	"time"
)

// Record represents a single entry in a Z-set flowing through the pipeline.
// The Weight field is the Z-set multiplicity: positive for insertions,
// negative for retractions. Weight=+1 means "one copy inserted", Weight=-1
// means "one copy retracted". Weights greater than 1 represent multiset
// semantics (multiple identical rows).
//
// This is the DBSP/Z-set model from Feldera: every relational operator has
// a provably correct incremental version when applied to weighted records.
type Record struct {
	// Columns maps column names to their values.
	Columns map[string]Value
	// Timestamp is the record's processing or event time.
	Timestamp time.Time
	// Weight is the Z-set multiplicity. +N = insert N copies, -N = retract N copies.
	Weight int
}

// NewRecord creates a new insert record with the given columns and current time.
func NewRecord(columns map[string]Value) Record {
	return Record{
		Columns:   columns,
		Timestamp: time.Now(),
		Weight:    1,
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
