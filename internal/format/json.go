package format

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"time"

	gojson "github.com/goccy/go-json"
	"github.com/kevin-cantwell/dbspa/internal/engine"
)

// JSONDecoder decodes NDJSON lines into Records.
// If AllowCols is non-nil, only columns present in the set are decoded;
// all others are skipped at the token level — no engine.Value is allocated
// for them, and nested objects/arrays are depth-walked without heap copies.
type JSONDecoder struct {
	AllowCols map[string]bool
}

// Decode parses a JSON object and maps keys to column names with type inference:
// number -> IntValue or FloatValue, string -> TextValue,
// bool -> BoolValue, null -> NullValue, object/array -> JsonValue.
func (d *JSONDecoder) Decode(data []byte) (engine.Record, error) {
	if len(data) == 0 {
		return engine.Record{}, fmt.Errorf("JSON decode error: empty input")
	}

	var cols map[string]engine.Value
	var err error

	if d.AllowCols != nil {
		// Selective path: stream-parse and skip unused fields before allocation.
		cols, err = decodeSelectiveCols(data, d.AllowCols)
		if err != nil {
			return engine.Record{}, fmt.Errorf("JSON decode error: %w", err)
		}
	} else {
		// Fast path: bulk unmarshal. Numbers decoded as float64 by default;
		// we recover int vs float by checking for whole-number values below.
		var raw map[string]any
		if err := gojson.Unmarshal(data, &raw); err != nil {
			return engine.Record{}, fmt.Errorf("JSON decode error: %w", err)
		}
		cols = make(map[string]engine.Value, len(raw))
		for k, v := range raw {
			cols[k] = inferValue(v)
		}
	}

	return engine.Record{
		Columns:   cols,
		Timestamp: time.Now(),
		Weight:    1,
	}, nil
}

// decodeSelectiveCols streams the JSON object token-by-token, decoding only
// keys present in allow. Keys absent from allow are skipped by advancing the
// token stream — no heap allocation occurs for their values.
func decodeSelectiveCols(data []byte, allow map[string]bool) (map[string]engine.Value, error) {
	dec := gojson.NewDecoder(bytes.NewReader(data))

	// Consume opening '{'
	if _, err := dec.Token(); err != nil {
		return nil, err
	}

	cols := make(map[string]engine.Value, len(allow))
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, _ := keyTok.(string)

		if !allow[key] {
			if err := skipJSONValue(dec); err != nil {
				return nil, err
			}
			continue
		}

		var v any
		if err := dec.Decode(&v); err != nil {
			return nil, err
		}
		cols[key] = inferValue(v)
	}
	return cols, nil
}

// skipJSONValue advances dec past one JSON value without allocating it.
// Primitives are consumed by a single Token() call. Objects and arrays are
// depth-tracked until their matching close delimiter, handling arbitrary
// nesting (e.g. {"a": [{"b": 1}]}) correctly.
func skipJSONValue(dec *gojson.Decoder) error {
	t, err := dec.Token()
	if err != nil {
		return err
	}
	if _, ok := t.(json.Delim); !ok {
		return nil // primitive (number, string, bool, null) — already consumed
	}
	// Opening delimiter consumed; read tokens until depth returns to zero.
	depth := 1
	for depth > 0 {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		if d, ok := tok.(json.Delim); ok {
			switch d {
			case '{', '[':
				depth++
			case '}', ']':
				depth--
			}
		}
	}
	return nil
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
