package format

import (
	"bytes"
	"testing"

	"github.com/kevin-cantwell/dbspa/internal/engine"
	"github.com/linkedin/goavro/v2"
)

func TestAvroOCFDecoder(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "TestRecord",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "price", "type": "double"},
			{"name": "active", "type": "boolean"}
		]
	}`

	// Encode records into an OCF buffer
	var buf bytes.Buffer
	writer, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      &buf,
		Schema: schema,
	})
	if err != nil {
		t.Fatalf("OCF writer error: %v", err)
	}

	records := []map[string]any{
		{"id": int32(1), "name": "widget", "price": 9.99, "active": true},
		{"id": int32(2), "name": "gadget", "price": 19.50, "active": false},
		{"id": int32(3), "name": "doohickey", "price": 5.00, "active": true},
	}

	for _, rec := range records {
		if err := writer.Append([]any{rec}); err != nil {
			t.Fatalf("OCF append error: %v", err)
		}
	}

	// Decode using AvroOCFDecoder
	dec := &AvroOCFDecoder{}
	ch := make(chan engine.Record, 10)
	if err := dec.DecodeStream(&buf, ch); err != nil {
		t.Fatalf("DecodeStream error: %v", err)
	}

	var decoded []engine.Record
	for rec := range ch {
		decoded = append(decoded, rec)
	}

	if len(decoded) != 3 {
		t.Fatalf("expected 3 records, got %d", len(decoded))
	}

	// Check first record
	rec := decoded[0]
	if v, ok := rec.Columns["id"].(engine.IntValue); !ok || v.V != 1 {
		t.Errorf("id: got %v, want 1", rec.Columns["id"])
	}
	if v, ok := rec.Columns["name"].(engine.TextValue); !ok || v.V != "widget" {
		t.Errorf("name: got %v, want widget", rec.Columns["name"])
	}
	if v, ok := rec.Columns["price"].(engine.FloatValue); !ok || v.V != 9.99 {
		t.Errorf("price: got %v, want 9.99", rec.Columns["price"])
	}
	if v, ok := rec.Columns["active"].(engine.BoolValue); !ok || v.V != true {
		t.Errorf("active: got %v, want true", rec.Columns["active"])
	}

	// Check diff is always +1
	for i, r := range decoded {
		if r.Weight != 1 {
			t.Errorf("record %d: diff=%d, want 1", i, r.Weight)
		}
	}
}

func TestAvroOCFDecoderNullableFields(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "NullableRecord",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "label", "type": ["null", "string"], "default": null}
		]
	}`

	var buf bytes.Buffer
	writer, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      &buf,
		Schema: schema,
	})
	if err != nil {
		t.Fatalf("OCF writer error: %v", err)
	}

	// One record with value, one with null
	err = writer.Append([]any{
		map[string]any{"id": int32(1), "label": goavro.Union("string", "hello")},
		map[string]any{"id": int32(2), "label": nil},
	})
	if err != nil {
		t.Fatalf("OCF append error: %v", err)
	}

	dec := &AvroOCFDecoder{}
	ch := make(chan engine.Record, 10)
	if err := dec.DecodeStream(&buf, ch); err != nil {
		t.Fatalf("DecodeStream error: %v", err)
	}

	var decoded []engine.Record
	for rec := range ch {
		decoded = append(decoded, rec)
	}

	if len(decoded) != 2 {
		t.Fatalf("expected 2 records, got %d", len(decoded))
	}

	// First record: label should be unwrapped from union
	if v, ok := decoded[0].Columns["label"].(engine.TextValue); !ok || v.V != "hello" {
		t.Errorf("record 0 label: got %v (%T), want hello", decoded[0].Columns["label"], decoded[0].Columns["label"])
	}

	// Second record: null label
	if !decoded[1].Columns["label"].IsNull() {
		t.Errorf("record 1 label: got %v, want null", decoded[1].Columns["label"])
	}
}

func TestAvroOCFDecoderEmpty(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "Empty",
		"fields": [
			{"name": "x", "type": "int"}
		]
	}`

	var buf bytes.Buffer
	_, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      &buf,
		Schema: schema,
	})
	if err != nil {
		t.Fatalf("OCF writer error: %v", err)
	}

	dec := &AvroOCFDecoder{}
	ch := make(chan engine.Record, 10)
	if err := dec.DecodeStream(&buf, ch); err != nil {
		t.Fatalf("DecodeStream error: %v", err)
	}

	var count int
	for range ch {
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 records from empty OCF, got %d", count)
	}
}
