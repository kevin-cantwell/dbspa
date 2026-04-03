package format

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/kevin-cantwell/folddb/internal/engine"
)

// FoldDBChangelogDecoder decodes JSON records with an optional _weight field.
// The _weight field controls the record's diff weight (1 for insertion, -1 for
// retraction). If _weight is absent, it defaults to 1 (insertion).
// The _weight field is consumed and not included in the record's columns.
type FoldDBChangelogDecoder struct{}

// Decode parses a JSON object, extracts the _weight field if present, and
// returns a record with the remaining columns.
func (d *FoldDBChangelogDecoder) Decode(data []byte) (engine.Record, error) {
	if len(data) == 0 {
		return engine.Record{}, fmt.Errorf("FoldDB changelog decode error: empty input")
	}

	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return engine.Record{}, fmt.Errorf("FoldDB changelog decode error: %w", err)
	}

	// Extract _weight (default: 1)
	weight := 1
	if w, ok := raw["_weight"]; ok {
		delete(raw, "_weight")
		switch v := w.(type) {
		case float64:
			weight = int(v)
		case json.Number:
			if i, err := v.Int64(); err == nil {
				weight = int(i)
			}
		}
	}

	cols := make(map[string]engine.Value, len(raw))
	for k, v := range raw {
		cols[k] = inferValue(v)
	}

	return engine.Record{
		Columns:   cols,
		Timestamp: time.Now(),
		Weight:    weight,
	}, nil
}
