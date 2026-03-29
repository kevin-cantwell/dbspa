package engine

import (
	"testing"
	"time"

	"github.com/kevin-cantwell/folddb/internal/sql/ast"
)

func TestDedupFilter(t *testing.T) {
	keyExpr := &ast.ColumnRef{Name: "id"}
	df := NewDedupFilter(keyExpr, 0, 100)

	rec1 := Record{
		Columns: map[string]Value{"id": TextValue{V: "abc"}},
		Diff:    1,
	}
	rec2 := Record{
		Columns: map[string]Value{"id": TextValue{V: "abc"}},
		Diff:    1,
	}
	rec3 := Record{
		Columns: map[string]Value{"id": TextValue{V: "def"}},
		Diff:    1,
	}

	// First occurrence should pass
	if df.ShouldDrop(rec1) {
		t.Error("first occurrence should not be dropped")
	}

	// Duplicate should be dropped
	if !df.ShouldDrop(rec2) {
		t.Error("duplicate should be dropped")
	}

	// Different key should pass
	if df.ShouldDrop(rec3) {
		t.Error("different key should not be dropped")
	}
}

func TestDedupFilterRetractionBypass(t *testing.T) {
	keyExpr := &ast.ColumnRef{Name: "id"}
	df := NewDedupFilter(keyExpr, 0, 100)

	rec := Record{
		Columns: map[string]Value{"id": TextValue{V: "abc"}},
		Diff:    -1, // retraction
	}

	// Retractions always pass through
	if df.ShouldDrop(rec) {
		t.Error("retractions should never be dropped")
	}
}

func TestDedupFilterWithin(t *testing.T) {
	keyExpr := &ast.ColumnRef{Name: "id"}
	df := NewDedupFilter(keyExpr, 1*time.Millisecond, 100)

	rec := Record{
		Columns: map[string]Value{"id": TextValue{V: "abc"}},
		Diff:    1,
	}

	// First should pass
	if df.ShouldDrop(rec) {
		t.Error("first occurrence should not be dropped")
	}

	// Wait for the within window to expire
	time.Sleep(5 * time.Millisecond)

	// Same key should now pass (expired)
	if df.ShouldDrop(rec) {
		t.Error("expired key should not be dropped")
	}
}

func TestDedupFilterCapacity(t *testing.T) {
	keyExpr := &ast.ColumnRef{Name: "id"}
	df := NewDedupFilter(keyExpr, 0, 2) // capacity of 2

	r1 := Record{Columns: map[string]Value{"id": TextValue{V: "a"}}, Diff: 1}
	r2 := Record{Columns: map[string]Value{"id": TextValue{V: "b"}}, Diff: 1}
	r3 := Record{Columns: map[string]Value{"id": TextValue{V: "c"}}, Diff: 1}

	df.ShouldDrop(r1) // adds "a"
	df.ShouldDrop(r2) // adds "b", capacity full
	df.ShouldDrop(r3) // adds "c", evicts "a"

	// "a" was evicted, should pass again
	if df.ShouldDrop(r1) {
		t.Error("evicted key should not be dropped")
	}
}

func TestDedupFilterNullKey(t *testing.T) {
	keyExpr := &ast.ColumnRef{Name: "id"}
	df := NewDedupFilter(keyExpr, 0, 100)

	rec1 := Record{
		Columns: map[string]Value{"id": NullValue{}},
		Diff:    1,
	}
	rec2 := Record{
		Columns: map[string]Value{"id": NullValue{}},
		Diff:    1,
	}

	// First NULL key passes
	if df.ShouldDrop(rec1) {
		t.Error("first NULL key should not be dropped")
	}

	// Second NULL key is duplicate
	if !df.ShouldDrop(rec2) {
		t.Error("duplicate NULL key should be dropped")
	}
}
