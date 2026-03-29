package engine

import "time"

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
