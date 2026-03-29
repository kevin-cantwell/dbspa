// Package engine provides the core execution types and operators for FoldDB.
package engine

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// Value represents a typed value in the FoldDB engine.
type Value interface {
	// Type returns the type name of this value (e.g., "NULL", "BOOL", "INT", "FLOAT", "TEXT", "TIMESTAMP", "JSON").
	Type() string
	// IsNull returns true if this value represents SQL NULL.
	IsNull() bool
	// String returns a human-readable representation.
	String() string
	// ToJSON returns the value suitable for JSON marshaling.
	ToJSON() any
}

// NullValue represents SQL NULL.
type NullValue struct{}

func (NullValue) Type() string          { return "NULL" }
func (NullValue) IsNull() bool          { return true }
func (NullValue) String() string        { return "NULL" }
func (NullValue) ToJSON() any   { return nil }

// BoolValue represents a boolean value.
type BoolValue struct{ V bool }

func (BoolValue) Type() string          { return "BOOL" }
func (BoolValue) IsNull() bool          { return false }
func (v BoolValue) String() string      { return strconv.FormatBool(v.V) }
func (v BoolValue) ToJSON() any { return v.V }

// IntValue represents a 64-bit integer.
type IntValue struct{ V int64 }

func (IntValue) Type() string          { return "INT" }
func (IntValue) IsNull() bool          { return false }
func (v IntValue) String() string      { return strconv.FormatInt(v.V, 10) }
func (v IntValue) ToJSON() any { return v.V }

// FloatValue represents a 64-bit float.
type FloatValue struct{ V float64 }

func (FloatValue) Type() string     { return "FLOAT" }
func (FloatValue) IsNull() bool     { return false }
func (v FloatValue) String() string { return strconv.FormatFloat(v.V, 'f', -1, 64) }
func (v FloatValue) ToJSON() any {
	if math.IsInf(v.V, 1) || math.IsInf(v.V, -1) || math.IsNaN(v.V) {
		// NaN and Infinity are not valid JSON numbers; emit null with a warning.
		fmt.Fprintf(os.Stderr, "Warning: float value %v is not representable in JSON, emitting null\n", v.V)
		return nil
	}
	// Use jsonFloat to ensure float values always serialize with a decimal point,
	// preserving type information (e.g., 123.0 instead of 123).
	return jsonFloat(v.V)
}

// jsonFloat is a float64 wrapper that always serializes with a decimal point in JSON.
type jsonFloat float64

func (f jsonFloat) MarshalJSON() ([]byte, error) {
	v := float64(f)
	// If the float is a whole number, format with .0 suffix
	if v == math.Trunc(v) && !math.IsInf(v, 0) && !math.IsNaN(v) {
		return []byte(strconv.FormatFloat(v, 'f', 1, 64)), nil
	}
	return []byte(strconv.FormatFloat(v, 'f', -1, 64)), nil
}

// TextValue represents a UTF-8 string.
type TextValue struct{ V string }

func (TextValue) Type() string          { return "TEXT" }
func (TextValue) IsNull() bool          { return false }
func (v TextValue) String() string      { return v.V }
func (v TextValue) ToJSON() any { return v.V }

// TimestampValue represents a UTC timestamp.
type TimestampValue struct{ V time.Time }

func (TimestampValue) Type() string          { return "TIMESTAMP" }
func (TimestampValue) IsNull() bool          { return false }
func (v TimestampValue) String() string      { return v.V.Format(time.RFC3339Nano) }
func (v TimestampValue) ToJSON() any { return v.V.Format(time.RFC3339Nano) }

// JsonValue represents an opaque JSON value (object, array, or scalar from JSON).
type JsonValue struct{ V any }

func (JsonValue) Type() string     { return "JSON" }
func (JsonValue) IsNull() bool     { return false }
func (v JsonValue) String() string {
	b, _ := json.Marshal(v.V)
	return string(b)
}
func (v JsonValue) ToJSON() any { return v.V }

// CastValue attempts to cast a value to the named type.
func CastValue(v Value, typeName string) (Value, error) {
	if v.IsNull() {
		return NullValue{}, nil
	}

	switch strings.ToUpper(typeName) {
	case "INT", "BIGINT", "INTEGER":
		return castToInt(v)
	case "FLOAT", "DOUBLE", "REAL", "NUMERIC", "DECIMAL":
		return castToFloat(v)
	case "TEXT", "VARCHAR", "STRING":
		return castToText(v), nil
	case "BOOLEAN", "BOOL":
		return castToBool(v)
	case "TIMESTAMP":
		return castToTimestamp(v)
	case "JSON":
		return castToJSON(v)
	default:
		return nil, fmt.Errorf("unsupported cast type: %s", typeName)
	}
}

func castToInt(v Value) (Value, error) {
	switch val := v.(type) {
	case IntValue:
		return val, nil
	case FloatValue:
		return IntValue{V: int64(val.V)}, nil
	case TextValue:
		n, err := strconv.ParseInt(val.V, 10, 64)
		if err != nil {
			// Try parsing as float then truncating
			f, ferr := strconv.ParseFloat(val.V, 64)
			if ferr != nil {
				return nil, fmt.Errorf("cannot cast %q to INT", val.V)
			}
			return IntValue{V: int64(f)}, nil
		}
		return IntValue{V: n}, nil
	case BoolValue:
		if val.V {
			return IntValue{V: 1}, nil
		}
		return IntValue{V: 0}, nil
	default:
		return nil, fmt.Errorf("cannot cast %s to INT", v.Type())
	}
}

func castToFloat(v Value) (Value, error) {
	switch val := v.(type) {
	case FloatValue:
		return val, nil
	case IntValue:
		return FloatValue{V: float64(val.V)}, nil
	case TextValue:
		f, err := strconv.ParseFloat(val.V, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot cast %q to FLOAT", val.V)
		}
		return FloatValue{V: f}, nil
	case BoolValue:
		if val.V {
			return FloatValue{V: 1}, nil
		}
		return FloatValue{V: 0}, nil
	default:
		return nil, fmt.Errorf("cannot cast %s to FLOAT", v.Type())
	}
}

func castToText(v Value) Value {
	return TextValue{V: v.String()}
}

func castToBool(v Value) (Value, error) {
	switch val := v.(type) {
	case BoolValue:
		return val, nil
	case TextValue:
		s := strings.ToLower(val.V)
		switch s {
		case "true", "t", "yes", "y", "1":
			return BoolValue{V: true}, nil
		case "false", "f", "no", "n", "0":
			return BoolValue{V: false}, nil
		default:
			return nil, fmt.Errorf("cannot cast %q to BOOLEAN", val.V)
		}
	case IntValue:
		return BoolValue{V: val.V != 0}, nil
	default:
		return nil, fmt.Errorf("cannot cast %s to BOOLEAN", v.Type())
	}
}

func castToTimestamp(v Value) (Value, error) {
	switch val := v.(type) {
	case TimestampValue:
		return val, nil
	case TextValue:
		t, err := time.Parse(time.RFC3339, val.V)
		if err != nil {
			t, err = time.Parse("2006-01-02T15:04:05", val.V)
			if err != nil {
				return nil, fmt.Errorf("cannot cast %q to TIMESTAMP", val.V)
			}
		}
		return TimestampValue{V: t.UTC()}, nil
	case IntValue:
		return TimestampValue{V: time.Unix(val.V, 0).UTC()}, nil
	case FloatValue:
		sec := int64(val.V)
		nsec := int64((val.V - float64(sec)) * 1e9)
		return TimestampValue{V: time.Unix(sec, nsec).UTC()}, nil
	default:
		return nil, fmt.Errorf("cannot cast %s to TIMESTAMP", v.Type())
	}
}

func castToJSON(v Value) (Value, error) {
	switch val := v.(type) {
	case JsonValue:
		return val, nil
	case TextValue:
		var j any
		if err := json.Unmarshal([]byte(val.V), &j); err != nil {
			return nil, fmt.Errorf("cannot cast %q to JSON: %v", val.V, err)
		}
		return JsonValue{V: j}, nil
	default:
		return JsonValue{V: v.ToJSON()}, nil
	}
}
