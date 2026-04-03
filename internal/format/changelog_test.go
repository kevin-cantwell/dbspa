package format

import (
	"testing"
)

func TestFoldDBChangelogDecoder_WeightedFormat(t *testing.T) {
	dec := &FoldDBChangelogDecoder{}
	data := []byte(`{"weight":1,"data":{"name":"alice","age":30}}`)

	rec, err := dec.Decode(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Weight != 1 {
		t.Errorf("weight: got %d, want 1", rec.Weight)
	}
	if len(rec.Columns) != 2 {
		t.Errorf("columns: got %d, want 2", len(rec.Columns))
	}
	// "weight" and "data" should not appear as columns
	if _, ok := rec.Columns["weight"]; ok {
		t.Error("weight should not appear in columns")
	}
	if _, ok := rec.Columns["data"]; ok {
		t.Error("data should not appear in columns")
	}
}

func TestFoldDBChangelogDecoder_Retraction(t *testing.T) {
	dec := &FoldDBChangelogDecoder{}
	data := []byte(`{"weight":-1,"data":{"name":"alice","age":30}}`)

	rec, err := dec.Decode(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Weight != -1 {
		t.Errorf("weight: got %d, want -1", rec.Weight)
	}
	if _, ok := rec.Columns["weight"]; ok {
		t.Error("weight should not appear in columns")
	}
}

func TestFoldDBChangelogDecoder_DefaultWeight(t *testing.T) {
	dec := &FoldDBChangelogDecoder{}
	// Missing weight defaults to 1
	data := []byte(`{"data":{"name":"alice"}}`)

	rec, err := dec.Decode(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Weight != 1 {
		t.Errorf("weight: got %d, want 1 (default)", rec.Weight)
	}
	if len(rec.Columns) != 1 {
		t.Errorf("columns: got %d, want 1", len(rec.Columns))
	}
}

func TestFoldDBChangelogDecoder_MultisetWeight(t *testing.T) {
	dec := &FoldDBChangelogDecoder{}
	data := []byte(`{"weight":5,"data":{"x":1}}`)

	rec, err := dec.Decode(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Weight != 5 {
		t.Errorf("weight: got %d, want 5", rec.Weight)
	}
}

func TestFoldDBChangelogDecoder_EmptyInput(t *testing.T) {
	dec := &FoldDBChangelogDecoder{}
	_, err := dec.Decode([]byte{})
	if err == nil {
		t.Error("expected error on empty input")
	}
}

func TestFoldDBChangelogDecoder_InvalidJSON(t *testing.T) {
	dec := &FoldDBChangelogDecoder{}
	_, err := dec.Decode([]byte(`not json`))
	if err == nil {
		t.Error("expected error on invalid JSON")
	}
}

func TestFoldDBChangelogDecoder_EmptyData(t *testing.T) {
	dec := &FoldDBChangelogDecoder{}
	data := []byte(`{"weight":1,"data":{}}`)

	rec, err := dec.Decode(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Weight != 1 {
		t.Errorf("weight: got %d, want 1", rec.Weight)
	}
	if len(rec.Columns) != 0 {
		t.Errorf("columns: got %d, want 0", len(rec.Columns))
	}
}
