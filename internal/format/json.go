package format

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/kevin-cantwell/folddb/internal/engine"
)

// JSONDecoder decodes NDJSON lines into Records.
type JSONDecoder struct{}

// Decode parses a JSON object and maps keys to column names with type inference:
// number -> IntValue or FloatValue, string -> TextValue,
// bool -> BoolValue, null -> NullValue, object/array -> JsonValue.
func (d *JSONDecoder) Decode(data []byte) (engine.Record, error) {
	if len(data) == 0 {
		return engine.Record{}, fmt.Errorf("JSON decode error: empty input")
	}

	// Use Unmarshal (faster than NewDecoder for single objects).
	// Without UseNumber, Go decodes JSON numbers as float64.
	// We recover int vs float by checking if the float64 is a whole number.
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
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
	case float64:
		// Recover integer type: if the value is a whole number within int64 range, use IntValue
		if val == math.Trunc(val) && val >= math.MinInt64 && val <= math.MaxInt64 {
			return engine.IntValue{V: int64(val)}
		}
		return engine.FloatValue{V: val}
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
