package format

import (
	"testing"

	"github.com/kevin-cantwell/dbspa/internal/engine"
)

func TestJSONDecodeSimpleObject(t *testing.T) {
	d := &JSONDecoder{}
	rec, err := d.Decode([]byte(`{"name":"alice","age":30}`))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(rec.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(rec.Columns))
	}
	name, ok := rec.Columns["name"].(engine.TextValue)
	if !ok {
		t.Fatalf("expected TextValue for name, got %T", rec.Columns["name"])
	}
	if name.V != "alice" {
		t.Errorf("name: got %q, want %q", name.V, "alice")
	}
}

func TestJSONDecodeTypeInference(t *testing.T) {
	d := &JSONDecoder{}
	rec, err := d.Decode([]byte(`{"i":42,"f":3.14,"s":"hello","b":true,"n":null}`))
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	tests := []struct {
		key      string
		wantType string
	}{
		{"i", "INT"},
		{"f", "FLOAT"},
		{"s", "TEXT"},
		{"b", "BOOL"},
		{"n", "NULL"},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			v := rec.Columns[tt.key]
			if v.Type() != tt.wantType {
				t.Errorf("key %q: got type %q, want %q", tt.key, v.Type(), tt.wantType)
			}
		})
	}
}

func TestJSONDecodeIntegerVsFloat(t *testing.T) {
	d := &JSONDecoder{}
	// 42 should be INT, 3.14 should be FLOAT
	// Note: JSON 42.0 is indistinguishable from 42 without UseNumber (both become float64(42))
	// so we use 3.14 to test actual float detection
	rec, err := d.Decode([]byte(`{"a":42,"b":3.14}`))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if rec.Columns["a"].Type() != "INT" {
		t.Errorf("42 should be INT, got %q", rec.Columns["a"].Type())
	}
	if rec.Columns["b"].Type() != "FLOAT" {
		t.Errorf("3.14 should be FLOAT, got %q", rec.Columns["b"].Type())
	}
}

func TestJSONDecodeNestedObject(t *testing.T) {
	d := &JSONDecoder{}
	rec, err := d.Decode([]byte(`{"user":{"name":"alice","age":30}}`))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	jv, ok := rec.Columns["user"].(engine.JsonValue)
	if !ok {
		t.Fatalf("expected JsonValue for nested object, got %T", rec.Columns["user"])
	}
	obj, ok := jv.V.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", jv.V)
	}
	if obj["name"] != "alice" {
		t.Errorf("nested name: got %v", obj["name"])
	}
}

func TestJSONDecodeArray(t *testing.T) {
	d := &JSONDecoder{}
	rec, err := d.Decode([]byte(`{"items":[1,2,3]}`))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	jv, ok := rec.Columns["items"].(engine.JsonValue)
	if !ok {
		t.Fatalf("expected JsonValue for array, got %T", rec.Columns["items"])
	}
	arr, ok := jv.V.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", jv.V)
	}
	if len(arr) != 3 {
		t.Errorf("expected 3 items, got %d", len(arr))
	}
}

func TestJSONDecodeMalformed(t *testing.T) {
	d := &JSONDecoder{}
	tests := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"not json", "hello world"},
		{"truncated", `{"name":"ali`},
		{"array not object", `[1,2,3]`},
		{"bare string", `"hello"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := d.Decode([]byte(tt.input))
			if err == nil {
				t.Error("expected error for malformed JSON")
			}
		})
	}
}

func TestJSONDecodeNullField(t *testing.T) {
	d := &JSONDecoder{}
	rec, err := d.Decode([]byte(`{"x":null}`))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !rec.Columns["x"].IsNull() {
		t.Errorf("expected NULL for null field, got %v", rec.Columns["x"])
	}
}

func TestJSONDecodeBooleans(t *testing.T) {
	d := &JSONDecoder{}
	rec, err := d.Decode([]byte(`{"t":true,"f":false}`))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !rec.Columns["t"].(engine.BoolValue).V {
		t.Error("expected true")
	}
	if rec.Columns["f"].(engine.BoolValue).V {
		t.Error("expected false")
	}
}

func TestJSONDecodeDiffAlwaysPositive(t *testing.T) {
	d := &JSONDecoder{}
	rec, err := d.Decode([]byte(`{"x":1}`))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if rec.Weight != 1 {
		t.Errorf("expected diff=1, got %d", rec.Weight)
	}
}

func TestJSONDecodeLargeNumber(t *testing.T) {
	d := &JSONDecoder{}

	// Numbers within float64 safe integer range (2^53) round-trip as IntValue
	rec, err := d.Decode([]byte(`{"safe":9007199254740992}`))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	iv, ok := rec.Columns["safe"].(engine.IntValue)
	if !ok {
		t.Fatalf("expected IntValue for 2^53, got %T", rec.Columns["safe"])
	}
	if iv.V != 9007199254740992 {
		t.Errorf("got %d, want 2^53", iv.V)
	}

	// Numbers beyond 2^53 can't be represented exactly in float64,
	// so they come back as FloatValue (json.Unmarshal without UseNumber
	// decodes all numbers as float64)
	rec2, err := d.Decode([]byte(`{"big":9223372036854775807}`))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	fv, ok := rec2.Columns["big"].(engine.FloatValue)
	if !ok {
		t.Fatalf("expected FloatValue for max int64, got %T", rec2.Columns["big"])
	}
	// float64 can't represent max int64 exactly, so we just check it's close
	if fv.V < 9.2e18 || fv.V > 9.3e18 {
		t.Errorf("got %v, want ~9.22e18", fv.V)
	}
}

func TestJSONDecodeEmptyObject(t *testing.T) {
	d := &JSONDecoder{}
	rec, err := d.Decode([]byte(`{}`))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(rec.Columns) != 0 {
		t.Errorf("expected 0 columns, got %d", len(rec.Columns))
	}
}

func TestJSONDecodeStringWithSpecialChars(t *testing.T) {
	d := &JSONDecoder{}
	rec, err := d.Decode([]byte(`{"msg":"hello\nworld","path":"a/b/c"}`))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	msg := rec.Columns["msg"].(engine.TextValue).V
	if msg != "hello\nworld" {
		t.Errorf("msg: got %q, want %q", msg, "hello\nworld")
	}
}
