package sink

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"github.com/kevin-cantwell/folddb/internal/engine"
)

// ChangelogSink writes accumulating query results as changelog NDJSON.
// Each line includes an "op" field: "+" for insert, "-" for retract.
// Retraction/insertion pairs for a single key are always adjacent.
type ChangelogSink struct {
	Writer io.Writer
	// ColumnOrder determines the output column order. If nil, columns are sorted.
	ColumnOrder []string
}

// Write serializes the record as a changelog JSON line with an "op" field.
func (s *ChangelogSink) Write(rec engine.Record) error {
	op := "+"
	if rec.Diff < 0 {
		op = "-"
	}

	// Build key-value pairs with "op" first
	var kvs []orderedKV
	kvs = append(kvs, orderedKV{Key: "op", Value: op})

	if len(s.ColumnOrder) > 0 {
		for _, col := range s.ColumnOrder {
			if v, ok := rec.Columns[col]; ok {
				kvs = append(kvs, orderedKV{Key: col, Value: v.ToJSON()})
			}
		}
	} else {
		// Sort column names for deterministic output
		keys := make([]string, 0, len(rec.Columns))
		for k := range rec.Columns {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			kvs = append(kvs, orderedKV{Key: k, Value: rec.Columns[k].ToJSON()})
		}
	}

	data, err := json.Marshal(orderedMap(kvs))
	if err != nil {
		return fmt.Errorf("changelog JSON encode error: %w", err)
	}

	_, err = fmt.Fprintf(s.Writer, "%s\n", data)
	return err
}

// Close is a no-op for the changelog sink.
func (s *ChangelogSink) Close() error {
	return nil
}
