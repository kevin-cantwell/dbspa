package format

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/kevin-cantwell/dbspa/internal/engine"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// writeDelimited writes a length-delimited protobuf message to buf.
func writeDelimited(buf *bytes.Buffer, msg proto.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
	buf.Write(lenBuf[:])
	buf.Write(data)
	return nil
}

func TestProtobufDecoder(t *testing.T) {
	var buf bytes.Buffer

	records := []map[string]any{
		{"id": float64(1), "name": "widget", "price": 9.99, "active": true},
		{"id": float64(2), "name": "gadget", "price": 19.50, "active": false},
		{"id": float64(3), "name": "doohickey", "price": 5.0, "active": true},
	}

	for _, rec := range records {
		s, err := structpb.NewStruct(rec)
		if err != nil {
			t.Fatalf("NewStruct error: %v", err)
		}
		if err := writeDelimited(&buf, s); err != nil {
			t.Fatalf("write error: %v", err)
		}
	}

	dec := &ProtobufDecoder{}
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
		t.Errorf("id: got %v (%T), want 1", rec.Columns["id"], rec.Columns["id"])
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

	// All diffs should be +1
	for i, r := range decoded {
		if r.Weight != 1 {
			t.Errorf("record %d: diff=%d, want 1", i, r.Weight)
		}
	}
}

func TestProtobufDecoderNullFields(t *testing.T) {
	var buf bytes.Buffer

	s, err := structpb.NewStruct(map[string]any{
		"id":    float64(1),
		"label": nil,
	})
	if err != nil {
		t.Fatalf("NewStruct error: %v", err)
	}
	if err := writeDelimited(&buf, s); err != nil {
		t.Fatalf("write error: %v", err)
	}

	dec := &ProtobufDecoder{}
	ch := make(chan engine.Record, 10)
	if err := dec.DecodeStream(&buf, ch); err != nil {
		t.Fatalf("DecodeStream error: %v", err)
	}

	var decoded []engine.Record
	for rec := range ch {
		decoded = append(decoded, rec)
	}

	if len(decoded) != 1 {
		t.Fatalf("expected 1 record, got %d", len(decoded))
	}

	if !decoded[0].Columns["label"].IsNull() {
		t.Errorf("label: got %v, want null", decoded[0].Columns["label"])
	}
}

func TestProtobufDecoderEmpty(t *testing.T) {
	var buf bytes.Buffer // empty

	dec := &ProtobufDecoder{}
	ch := make(chan engine.Record, 10)
	if err := dec.DecodeStream(&buf, ch); err != nil {
		t.Fatalf("DecodeStream error: %v", err)
	}

	var count int
	for range ch {
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 records from empty stream, got %d", count)
	}
}

func TestProtobufDecoderNestedStruct(t *testing.T) {
	var buf bytes.Buffer

	s, err := structpb.NewStruct(map[string]any{
		"id":   float64(1),
		"meta": map[string]any{"region": "us-east", "tier": "premium"},
		"tags": []any{"a", "b"},
	})
	if err != nil {
		t.Fatalf("NewStruct error: %v", err)
	}
	if err := writeDelimited(&buf, s); err != nil {
		t.Fatalf("write error: %v", err)
	}

	dec := &ProtobufDecoder{}
	ch := make(chan engine.Record, 10)
	if err := dec.DecodeStream(&buf, ch); err != nil {
		t.Fatalf("DecodeStream error: %v", err)
	}

	var decoded []engine.Record
	for rec := range ch {
		decoded = append(decoded, rec)
	}

	if len(decoded) != 1 {
		t.Fatalf("expected 1 record, got %d", len(decoded))
	}

	// Nested struct -> JsonValue
	if _, ok := decoded[0].Columns["meta"].(engine.JsonValue); !ok {
		t.Errorf("meta: got %T, want JsonValue", decoded[0].Columns["meta"])
	}

	// Array -> JsonValue
	if _, ok := decoded[0].Columns["tags"].(engine.JsonValue); !ok {
		t.Errorf("tags: got %T, want JsonValue", decoded[0].Columns["tags"])
	}
}
