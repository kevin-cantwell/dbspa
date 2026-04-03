package format

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/kevin-cantwell/dbspa/internal/engine"
)

// DBSPAChangelogDecoder decodes JSON records in the DBSPA weighted format:
// {"weight": N, "data": {"col1": val, ...}}
//
// The "weight" field controls the record's Z-set weight (1 for insertion, -1 for
// retraction). If "weight" is absent, it defaults to 1. The "data" field contains
// the actual record columns. If "data" is absent, the top-level fields (excluding
// "weight") are used as columns for backwards compatibility.
type DBSPAChangelogDecoder struct{}

// Decode parses a weighted JSON envelope and returns a record.
func (d *DBSPAChangelogDecoder) Decode(data []byte) (engine.Record, error) {
	if len(data) == 0 {
		return engine.Record{}, fmt.Errorf("DBSPA changelog decode error: empty input")
	}

	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return engine.Record{}, fmt.Errorf("DBSPA changelog decode error: %w", err)
	}

	// Extract weight (default: 1)
	weight := 1
	if w, ok := raw["weight"]; ok {
		switch v := w.(type) {
		case float64:
			weight = int(v)
		case json.Number:
			if i, err := v.Int64(); err == nil {
				weight = int(i)
			}
		}
	}

	// Extract data envelope — if "data" is present, use it; otherwise fall back
	// to top-level fields (minus "weight") for backwards compatibility.
	var fields map[string]any
	if d, ok := raw["data"]; ok {
		if dm, ok := d.(map[string]any); ok {
			fields = dm
		} else {
			fields = make(map[string]any)
		}
	} else {
		fields = raw
		delete(fields, "weight")
	}

	cols := make(map[string]engine.Value, len(fields))
	for k, v := range fields {
		cols[k] = inferValue(v)
	}

	return engine.Record{
		Columns:   cols,
		Timestamp: time.Now(),
		Weight:    weight,
	}, nil
}
