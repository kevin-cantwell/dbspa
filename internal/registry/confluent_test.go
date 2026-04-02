package registry

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

const testSchema = `{"type":"record","name":"Order","namespace":"com.example","fields":[{"name":"id","type":"int"},{"name":"status","type":"string"}]}`

func TestGetSchema(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/schemas/ids/1" {
			http.NotFound(w, r)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"schema":     testSchema,
			"schemaType": "AVRO",
		})
	}))
	defer srv.Close()

	client := NewClient(srv.URL)

	schema, err := client.GetSchema(1)
	if err != nil {
		t.Fatalf("GetSchema: %v", err)
	}
	if schema.ID != 1 {
		t.Errorf("ID = %d, want 1", schema.ID)
	}
	if schema.SchemaType != "AVRO" {
		t.Errorf("SchemaType = %q, want AVRO", schema.SchemaType)
	}
	if schema.Codec == nil {
		t.Fatal("Codec is nil")
	}
	if schema.Schema != testSchema {
		t.Errorf("Schema = %q, want %q", schema.Schema, testSchema)
	}
}

func TestGetSchema_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	_, err := client.GetSchema(999)
	if err == nil {
		t.Fatal("expected error for missing schema")
	}
}

func TestGetSchema_Caching(t *testing.T) {
	var fetchCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetchCount.Add(1)
		json.NewEncoder(w).Encode(map[string]any{
			"schema":     testSchema,
			"schemaType": "AVRO",
		})
	}))
	defer srv.Close()

	client := NewClient(srv.URL)

	// Fetch three times — should only hit the server once
	for i := 0; i < 3; i++ {
		_, err := client.GetSchema(1)
		if err != nil {
			t.Fatalf("GetSchema attempt %d: %v", i, err)
		}
	}

	if got := fetchCount.Load(); got != 1 {
		t.Errorf("fetch count = %d, want 1 (schema should be cached)", got)
	}
}

func TestGetCodec(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"schema":     testSchema,
			"schemaType": "AVRO",
		})
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	codec, err := client.GetCodec(1)
	if err != nil {
		t.Fatalf("GetCodec: %v", err)
	}
	if codec == nil {
		t.Fatal("codec is nil")
	}
}

func TestGetSchema_DefaultsToAvro(t *testing.T) {
	// Older registries may not return schemaType
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"schema": testSchema,
		})
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	schema, err := client.GetSchema(1)
	if err != nil {
		t.Fatalf("GetSchema: %v", err)
	}
	if schema.SchemaType != "AVRO" {
		t.Errorf("SchemaType = %q, want AVRO (should default)", schema.SchemaType)
	}
	if schema.Codec == nil {
		t.Fatal("Codec should be parsed for AVRO")
	}
}

func TestGetSchema_Retry5xx(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"schema":     testSchema,
			"schemaType": "AVRO",
		})
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	schema, err := client.GetSchema(1)
	if err != nil {
		t.Fatalf("GetSchema after retries: %v", err)
	}
	if schema.Codec == nil {
		t.Fatal("Codec is nil")
	}
	if got := attempts.Load(); got != 3 {
		t.Errorf("attempts = %d, want 3", got)
	}
}
