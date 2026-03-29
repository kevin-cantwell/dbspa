package format

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kevin-cantwell/folddb/internal/engine"
)

// JSONDecoder decodes NDJSON lines into Records.
type JSONDecoder struct{}

// Decode parses a JSON object and maps keys to column names with type inference:
// number -> IntValue or FloatValue, string -> TextValue,
// bool -> BoolValue, null -> NullValue, object/array -> JsonValue.
func (d *JSONDecoder) Decode(data []byte) (engine.Record, error) {
	var raw map[string]any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&raw); err != nil {
		return engine.Record{}, fmt.Errorf("JSON decode error: %w", err)
	}

	cols := make(map[string]engine.Value, len(raw))
	for k, v := range raw {
		cols[k] = inferValue(v)
	}

	return engine.Record{
		Columns:   cols,
		Timestamp: time.Now(),
		Diff:      1,
	}, nil
}

func inferValue(v any) engine.Value {
	if v == nil {
		return engine.NullValue{}
	}
	switch val := v.(type) {
	case json.Number:
		if i, err := val.Int64(); err == nil {
			return engine.IntValue{V: i}
		}
		if f, err := val.Float64(); err == nil {
			return engine.FloatValue{V: f}
		}
		return engine.TextValue{V: val.String()}
	case string:
		return engine.TextValue{V: val}
	case bool:
		return engine.BoolValue{V: val}
	case map[string]any:
		return engine.JsonValue{V: val}
	case []any:
		return engine.JsonValue{V: val}
	default:
		return engine.TextValue{V: fmt.Sprintf("%v", val)}
	}
}
