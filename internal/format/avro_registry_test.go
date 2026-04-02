package format

import (
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kevin-cantwell/folddb/internal/engine"
	"github.com/kevin-cantwell/folddb/internal/registry"
	"github.com/linkedin/goavro/v2"
)

const testOrderSchema = `{"type":"record","name":"Order","namespace":"com.example","fields":[{"name":"id","type":"int"},{"name":"status","type":"string"}]}`

func makeConfluentMessage(schemaID int32, payload []byte) []byte {
	msg := make([]byte, 5+len(payload))
	msg[0] = 0x00 // magic byte
	binary.BigEndian.PutUint32(msg[1:5], uint32(schemaID))
	copy(msg[5:], payload)
	return msg
}

func startMockRegistry(schema string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"schema":     schema,
			"schemaType": "AVRO",
		})
	}))
}

func assertIntValue(t *testing.T, v engine.Value, name string, want int64) {
	t.Helper()
	iv, ok := v.(engine.IntValue)
	if !ok {
		t.Errorf("%s: expected IntValue, got %T (%v)", name, v, v)
		return
	}
	if iv.V != want {
		t.Errorf("%s = %d, want %d", name, iv.V, want)
	}
}

func assertTextValue(t *testing.T, v engine.Value, name string, want string) {
	t.Helper()
	tv, ok := v.(engine.TextValue)
	if !ok {
		t.Errorf("%s: expected TextValue, got %T (%v)", name, v, v)
		return
	}
	if tv.V != want {
		t.Errorf("%s = %q, want %q", name, tv.V, want)
	}
}

func TestConfluentAvroDecoder_Basic(t *testing.T) {
	srv := startMockRegistry(testOrderSchema)
	defer srv.Close()

	codec, err := goavro.NewCodec(testOrderSchema)
	if err != nil {
		t.Fatalf("NewCodec: %v", err)
	}

	payload, err := codec.BinaryFromNative(nil, map[string]any{
		"id":     int32(42),
		"status": "pending",
	})
	if err != nil {
		t.Fatalf("BinaryFromNative: %v", err)
	}

	msg := makeConfluentMessage(1, payload)

	dec := NewConfluentAvroDecoder(registry.NewClient(srv.URL))
	rec, err := dec.Decode(msg)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	assertIntValue(t, rec.Get("id"), "id", 42)
	assertTextValue(t, rec.Get("status"), "status", "pending")
	if rec.Weight != 1 {
		t.Errorf("Weight = %d, want 1", rec.Weight)
	}
}

func TestConfluentAvroDecoder_InvalidMagicByte(t *testing.T) {
	srv := startMockRegistry(testOrderSchema)
	defer srv.Close()

	dec := NewConfluentAvroDecoder(registry.NewClient(srv.URL))
	_, err := dec.Decode([]byte{0x01, 0, 0, 0, 1, 0})
	if err == nil {
		t.Fatal("expected error for invalid magic byte")
	}
}

func TestConfluentAvroDecoder_TooShort(t *testing.T) {
	srv := startMockRegistry(testOrderSchema)
	defer srv.Close()

	dec := NewConfluentAvroDecoder(registry.NewClient(srv.URL))
	_, err := dec.Decode([]byte{0x00, 0, 0})
	if err == nil {
		t.Fatal("expected error for too-short message")
	}
}

func TestConfluentAvroDecoder_MultipleSchemaIDs(t *testing.T) {
	schema2 := `{"type":"record","name":"User","namespace":"com.example","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/schemas/ids/1":
			json.NewEncoder(w).Encode(map[string]any{
				"schema":     testOrderSchema,
				"schemaType": "AVRO",
			})
		case "/schemas/ids/2":
			json.NewEncoder(w).Encode(map[string]any{
				"schema":     schema2,
				"schemaType": "AVRO",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	dec := NewConfluentAvroDecoder(registry.NewClient(srv.URL))

	// Encode with schema 1
	codec1, _ := goavro.NewCodec(testOrderSchema)
	payload1, _ := codec1.BinaryFromNative(nil, map[string]any{
		"id":     int32(1),
		"status": "active",
	})
	rec1, err := dec.Decode(makeConfluentMessage(1, payload1))
	if err != nil {
		t.Fatalf("Decode schema 1: %v", err)
	}
	assertIntValue(t, rec1.Get("id"), "schema1.id", 1)

	// Encode with schema 2
	codec2, _ := goavro.NewCodec(schema2)
	payload2, _ := codec2.BinaryFromNative(nil, map[string]any{
		"name": "Alice",
		"age":  int32(30),
	})
	rec2, err := dec.Decode(makeConfluentMessage(2, payload2))
	if err != nil {
		t.Fatalf("Decode schema 2: %v", err)
	}
	assertTextValue(t, rec2.Get("name"), "schema2.name", "Alice")
	assertIntValue(t, rec2.Get("age"), "schema2.age", 30)
}

func TestIsConfluentWireFormat(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want bool
	}{
		{"valid", []byte{0x00, 0, 0, 0, 1, 0x01}, true},
		{"too short", []byte{0x00, 0, 0}, false},
		{"wrong magic", []byte{0x01, 0, 0, 0, 1}, false},
		{"empty", []byte{}, false},
		{"nil", nil, false},
		{"exactly 5 bytes", []byte{0x00, 0, 0, 0, 1}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsConfluentWireFormat(tt.data); got != tt.want {
				t.Errorf("IsConfluentWireFormat = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfluentAvroDecoder_DecodeMulti(t *testing.T) {
	srv := startMockRegistry(testOrderSchema)
	defer srv.Close()

	codec, _ := goavro.NewCodec(testOrderSchema)
	payload, _ := codec.BinaryFromNative(nil, map[string]any{
		"id":     int32(99),
		"status": "shipped",
	})

	dec := NewConfluentAvroDecoder(registry.NewClient(srv.URL))
	recs, err := dec.DecodeMulti(makeConfluentMessage(1, payload))
	if err != nil {
		t.Fatalf("DecodeMulti: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1", len(recs))
	}
	assertTextValue(t, recs[0].Get("status"), "status", "shipped")
}

func TestConfluentDebeziumAvroDecoder(t *testing.T) {
	debeziumSchema := `{
		"type": "record",
		"name": "Envelope",
		"namespace": "com.example",
		"fields": [
			{"name": "op", "type": "string"},
			{"name": "before", "type": ["null", {"type": "record", "name": "Value", "fields": [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}]}], "default": null},
			{"name": "after", "type": ["null", "Value"], "default": null}
		]
	}`

	srv := startMockRegistry(debeziumSchema)
	defer srv.Close()

	codec, err := goavro.NewCodec(debeziumSchema)
	if err != nil {
		t.Fatalf("NewCodec: %v", err)
	}

	// Create event (op=c, before=null, after={id:1, name:"Alice"})
	payload, err := codec.BinaryFromNative(nil, map[string]any{
		"op":     "c",
		"before": nil,
		"after": map[string]any{
			"com.example.Value": map[string]any{
				"id":   int32(1),
				"name": "Alice",
			},
		},
	})
	if err != nil {
		t.Fatalf("BinaryFromNative: %v", err)
	}

	msg := makeConfluentMessage(1, payload)

	dec := NewConfluentDebeziumAvroDecoder(registry.NewClient(srv.URL))
	recs, err := dec.DecodeMulti(msg)
	if err != nil {
		t.Fatalf("DecodeMulti: %v", err)
	}

	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1", len(recs))
	}

	rec := recs[0]
	assertIntValue(t, rec.Get("id"), "id", 1)
	assertTextValue(t, rec.Get("name"), "name", "Alice")
	if rec.Weight != 1 {
		t.Errorf("Weight = %d, want 1", rec.Weight)
	}
	assertTextValue(t, rec.Get("_op"), "_op", "c")
}

func TestConfluentDebeziumAvroDecoder_Update(t *testing.T) {
	debeziumSchema := `{
		"type": "record",
		"name": "Envelope",
		"namespace": "com.example",
		"fields": [
			{"name": "op", "type": "string"},
			{"name": "before", "type": ["null", {"type": "record", "name": "Value", "fields": [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}]}], "default": null},
			{"name": "after", "type": ["null", "Value"], "default": null}
		]
	}`

	srv := startMockRegistry(debeziumSchema)
	defer srv.Close()

	codec, _ := goavro.NewCodec(debeziumSchema)

	// Update event (op=u, before={id:1, name:"Alice"}, after={id:1, name:"Bob"})
	payload, _ := codec.BinaryFromNative(nil, map[string]any{
		"op": "u",
		"before": map[string]any{
			"com.example.Value": map[string]any{
				"id":   int32(1),
				"name": "Alice",
			},
		},
		"after": map[string]any{
			"com.example.Value": map[string]any{
				"id":   int32(1),
				"name": "Bob",
			},
		},
	})

	msg := makeConfluentMessage(1, payload)
	dec := NewConfluentDebeziumAvroDecoder(registry.NewClient(srv.URL))
	recs, err := dec.DecodeMulti(msg)
	if err != nil {
		t.Fatalf("DecodeMulti: %v", err)
	}

	// Update should produce 2 records: retraction (-1) and insertion (+1)
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2", len(recs))
	}

	// First: retraction of old value
	if recs[0].Weight != -1 {
		t.Errorf("retraction Weight = %d, want -1", recs[0].Weight)
	}
	assertTextValue(t, recs[0].Get("name"), "retraction.name", "Alice")

	// Second: insertion of new value
	if recs[1].Weight != 1 {
		t.Errorf("insertion Weight = %d, want 1", recs[1].Weight)
	}
	assertTextValue(t, recs[1].Get("name"), "insertion.name", "Bob")
}
