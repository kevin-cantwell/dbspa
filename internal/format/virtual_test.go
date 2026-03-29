package format

import (
	"testing"
	"time"

	"github.com/kevin-cantwell/folddb/internal/engine"
	"github.com/kevin-cantwell/folddb/internal/source"
)

func TestInjectKafkaVirtuals(t *testing.T) {
	ts := time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC)
	kr := source.KafkaRecord{
		Value:     []byte(`{"x":1}`),
		Key:       []byte("my-key"),
		Offset:    42,
		Partition: 3,
		Timestamp: ts,
	}

	rec := engine.Record{
		Columns: map[string]engine.Value{
			"x": engine.IntValue{V: 1},
		},
		Diff: 1,
	}

	recs := InjectKafkaVirtuals([]engine.Record{rec}, kr)
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}
	r := recs[0]

	// Check virtual columns
	if v, ok := r.Columns["_offset"].(engine.IntValue); !ok || v.V != 42 {
		t.Errorf("_offset: got %v, want 42", r.Columns["_offset"])
	}
	if v, ok := r.Columns["_partition"].(engine.IntValue); !ok || v.V != 3 {
		t.Errorf("_partition: got %v, want 3", r.Columns["_partition"])
	}
	if v, ok := r.Columns["_timestamp"].(engine.TimestampValue); !ok || !v.V.Equal(ts) {
		t.Errorf("_timestamp: got %v, want %v", r.Columns["_timestamp"], ts)
	}
	if v, ok := r.Columns["_key"].(engine.TextValue); !ok || v.V != "my-key" {
		t.Errorf("_key: got %v, want my-key", r.Columns["_key"])
	}

	// Original column preserved
	if v, ok := r.Columns["x"].(engine.IntValue); !ok || v.V != 1 {
		t.Errorf("x: got %v, want 1", r.Columns["x"])
	}
}

func TestInjectKafkaVirtualsNilKey(t *testing.T) {
	kr := source.KafkaRecord{
		Value:     []byte(`{}`),
		Offset:    0,
		Partition: 0,
		Timestamp: time.Now(),
	}
	rec := engine.Record{
		Columns: map[string]engine.Value{},
		Diff:    1,
	}
	recs := InjectKafkaVirtuals([]engine.Record{rec}, kr)
	if !recs[0].Columns["_key"].IsNull() {
		t.Errorf("expected _key to be null for nil key, got %v", recs[0].Columns["_key"])
	}
}

func TestInjectKafkaVirtualsNonUTF8Key(t *testing.T) {
	kr := source.KafkaRecord{
		Value:     []byte(`{}`),
		Key:       []byte{0xFF, 0xFE},
		Offset:    0,
		Partition: 0,
		Timestamp: time.Now(),
	}
	rec := engine.Record{
		Columns: map[string]engine.Value{},
		Diff:    1,
	}
	recs := InjectKafkaVirtuals([]engine.Record{rec}, kr)
	keyVal := recs[0].Columns["_key"].(engine.TextValue).V
	if keyVal[:4] != "b64:" {
		t.Errorf("expected b64: prefix for non-UTF-8 key, got %q", keyVal)
	}
}
