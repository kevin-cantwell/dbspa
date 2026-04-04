package format

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	gojson "github.com/goccy/go-json"
	"github.com/kevin-cantwell/dbspa/internal/engine"
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

	// Use go-json (goccy/go-json) — drop-in replacement for encoding/json,
	// 2-4x faster for unmarshaling. Numbers decoded as float64 by default;
	// we recover int vs float by checking for whole-number values below.
	var raw map[string]any
	if err := gojson.Unmarshal(data, &raw); err != nil {
		return engine.Record{}, fmt.Errorf("JSON decode error: %w", err)
	}

	cols := make(map[string]engine.Value, len(raw))
	for k, v := range raw {
		cols[k] = inferValue(v)
	}

	return engine.Record{
		Columns:   cols,
		Timestamp: time.Now(),
		Weight:      1,
	}, nil
}

func inferValue(v any) engine.Value {
	if v == nil {
		return engine.NullValue{}
	}
	switch val := v.(type) {
	case json.Number:
		// json.Number is produced when UseNumber() is set on the decoder.
		// Try integer first, then float.
		if i, err := val.Int64(); err == nil {
			return engine.IntValue{V: i}
		}
		if f, err := val.Float64(); err == nil {
			return engine.FloatValue{V: f}
		}
		return engine.TextValue{V: val.String()}
	case float64:
		// Recover integer type: if the value is a whole number that fits in int64 exactly.
		// float64 can only represent integers exactly up to 2^53. Beyond that,
		// float64→int64 conversion may overflow or lose precision.
		if val == math.Trunc(val) && val >= -1<<53 && val <= 1<<53 {
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
