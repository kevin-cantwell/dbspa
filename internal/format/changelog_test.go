package format

import (
	"testing"
)

func TestFoldDBChangelogDecoder_InsertDefault(t *testing.T) {
	dec := &FoldDBChangelogDecoder{}
	data := []byte(`{"name":"alice","age":30}`)

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
}

func TestFoldDBChangelogDecoder_InsertExplicit(t *testing.T) {
	dec := &FoldDBChangelogDecoder{}
	data := []byte(`{"name":"alice","age":30,"_weight":1}`)

	rec, err := dec.Decode(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Weight != 1 {
		t.Errorf("weight: got %d, want 1", rec.Weight)
	}
	// _weight should be stripped from columns
	if _, ok := rec.Columns["_weight"]; ok {
		t.Error("_weight should not appear in columns")
	}
	if len(rec.Columns) != 2 {
		t.Errorf("columns: got %d, want 2", len(rec.Columns))
	}
}

func TestFoldDBChangelogDecoder_Retraction(t *testing.T) {
	dec := &FoldDBChangelogDecoder{}
	data := []byte(`{"name":"alice","age":30,"_weight":-1}`)

	rec, err := dec.Decode(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Weight != -1 {
		t.Errorf("weight: got %d, want -1", rec.Weight)
	}
	if _, ok := rec.Columns["_weight"]; ok {
		t.Error("_weight should not appear in columns")
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
