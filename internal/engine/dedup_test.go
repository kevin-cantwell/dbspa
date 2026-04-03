package engine

import (
	"testing"
	"time"

	"github.com/kevin-cantwell/dbspa/internal/sql/ast"
)

func TestDedupFilter(t *testing.T) {
	keyExpr := &ast.ColumnRef{Name: "id"}
	df := NewDedupFilter(keyExpr, 0, 100)

	rec1 := Record{
		Columns: map[string]Value{"id": TextValue{V: "abc"}},
		Weight:    1,
	}
	rec2 := Record{
		Columns: map[string]Value{"id": TextValue{V: "abc"}},
		Weight:    1,
	}
	rec3 := Record{
		Columns: map[string]Value{"id": TextValue{V: "def"}},
		Weight:    1,
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
		Weight:    -1, // retraction
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
		Weight:    1,
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

	r1 := Record{Columns: map[string]Value{"id": TextValue{V: "a"}}, Weight: 1}
	r2 := Record{Columns: map[string]Value{"id": TextValue{V: "b"}}, Weight: 1}
	r3 := Record{Columns: map[string]Value{"id": TextValue{V: "c"}}, Weight: 1}

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
		Weight:    1,
	}
	rec2 := Record{
		Columns: map[string]Value{"id": NullValue{}},
		Weight:    1,
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

// TC-DEDUP-001/002: Basic dedup within and after WITHIN window (table-driven)
func TestDedupFilterWithinWindow(t *testing.T) {
	tests := []struct {
		name        string
		within      time.Duration
		sleepBefore time.Duration
		wantDrop    bool
	}{
		{"duplicate within window", 100 * time.Millisecond, 0, true},
		{"duplicate after window expires", 10 * time.Millisecond, 20 * time.Millisecond, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keyExpr := &ast.ColumnRef{Name: "order_id"}
			df := NewDedupFilter(keyExpr, tt.within, 100)

			rec := Record{
				Columns: map[string]Value{"order_id": TextValue{V: "ORD-1"}},
				Weight:    1,
			}

			if df.ShouldDrop(rec) {
				t.Error("first occurrence should not be dropped")
			}

			if tt.sleepBefore > 0 {
				time.Sleep(tt.sleepBefore)
			}

			got := df.ShouldDrop(rec)
			if got != tt.wantDrop {
				t.Errorf("ShouldDrop = %v, want %v", got, tt.wantDrop)
			}
		})
	}
}

// TC-DEDUP-003: LRU eviction under capacity pressure
func TestDedupFilterLRUEviction(t *testing.T) {
	keyExpr := &ast.ColumnRef{Name: "id"}
	cap := 5
	df := NewDedupFilter(keyExpr, 0, cap)

	// Insert cap+2 distinct keys; first 2 should be evicted
	for i := 0; i < cap+2; i++ {
		rec := Record{
			Columns: map[string]Value{"id": IntValue{V: int64(i)}},
			Weight:    1,
		}
		df.ShouldDrop(rec)
	}

	// Key 0 and 1 should be evicted
	r0 := Record{Columns: map[string]Value{"id": IntValue{V: 0}}, Weight: 1}
	if df.ShouldDrop(r0) {
		t.Error("key 0 should have been evicted and pass through")
	}

	r1 := Record{Columns: map[string]Value{"id": IntValue{V: 1}}, Weight: 1}
	if df.ShouldDrop(r1) {
		t.Error("key 1 should have been evicted and pass through")
	}

	// Key cap+1 should still be in cache
	rLast := Record{Columns: map[string]Value{"id": IntValue{V: int64(cap + 1)}}, Weight: 1}
	if !df.ShouldDrop(rLast) {
		t.Error("most recent key should still be cached as duplicate")
	}
}

// TC-DEDUP-006/012: Retractions always bypass dedup regardless of key
func TestDedupFilterRetractionAlwaysPasses(t *testing.T) {
	keyExpr := &ast.ColumnRef{Name: "order_id"}
	df := NewDedupFilter(keyExpr, 0, 100)

	rec := Record{
		Columns: map[string]Value{"order_id": TextValue{V: "ORD-1"}},
		Weight:    1,
	}
	// Insert first
	df.ShouldDrop(rec)

	// Retraction for same key
	retract := Record{
		Columns: map[string]Value{"order_id": TextValue{V: "ORD-1"}},
		Weight:    -1,
	}
	if df.ShouldDrop(retract) {
		t.Error("retraction should never be dropped")
	}

	// Multiple retractions in a row
	if df.ShouldDrop(retract) {
		t.Error("second retraction should also pass through")
	}
}

// TC-DEDUP-007: Composite key expression
func TestDedupFilterCompositeKey(t *testing.T) {
	// Composite key: user_id || '-' || session_id
	keyExpr := &ast.BinaryExpr{
		Left: &ast.BinaryExpr{
			Left:  &ast.ColumnRef{Name: "user_id"},
			Op:    "||",
			Right: &ast.StringLiteral{Value: "-"},
		},
		Op:    "||",
		Right: &ast.ColumnRef{Name: "session_id"},
	}
	df := NewDedupFilter(keyExpr, 0, 100)

	rec1 := Record{
		Columns: map[string]Value{
			"user_id":    TextValue{V: "alice"},
			"session_id": TextValue{V: "s1"},
		},
		Weight: 1,
	}
	rec2 := Record{
		Columns: map[string]Value{
			"user_id":    TextValue{V: "alice"},
			"session_id": TextValue{V: "s1"},
		},
		Weight: 1,
	}
	rec3 := Record{
		Columns: map[string]Value{
			"user_id":    TextValue{V: "alice"},
			"session_id": TextValue{V: "s2"},
		},
		Weight: 1,
	}

	if df.ShouldDrop(rec1) {
		t.Error("first occurrence should pass")
	}
	if !df.ShouldDrop(rec2) {
		t.Error("same composite key should be dropped")
	}
	if df.ShouldDrop(rec3) {
		t.Error("different session_id should pass")
	}
}

// TC-DEDUP-011: NULL keys treated as equal for dedup
func TestDedupFilterNullKeyEquality(t *testing.T) {
	keyExpr := &ast.ColumnRef{Name: "nullable_col"}
	df := NewDedupFilter(keyExpr, 0, 100)

	rec1 := Record{
		Columns: map[string]Value{"nullable_col": NullValue{}},
		Weight:    1,
	}
	rec2 := Record{
		Columns: map[string]Value{"nullable_col": NullValue{}},
		Weight:    1,
	}

	if df.ShouldDrop(rec1) {
		t.Error("first NULL key should pass")
	}
	if !df.ShouldDrop(rec2) {
		t.Error("second NULL key should be treated as duplicate")
	}
}

// TC-DEDUP-010: Default capacity is 100000
func TestDedupFilterDefaultCapacity(t *testing.T) {
	keyExpr := &ast.ColumnRef{Name: "id"}
	df := NewDedupFilter(keyExpr, 0, 0) // 0 should use default
	if df.capacity != 100000 {
		t.Errorf("default capacity = %d, want 100000", df.capacity)
	}
}

// Multiple distinct keys, then re-check each
func TestDedupFilterManyDistinctKeys(t *testing.T) {
	keyExpr := &ast.ColumnRef{Name: "id"}
	df := NewDedupFilter(keyExpr, 0, 1000)

	for i := 0; i < 100; i++ {
		rec := Record{
			Columns: map[string]Value{"id": IntValue{V: int64(i)}},
			Weight:    1,
		}
		if df.ShouldDrop(rec) {
			t.Errorf("first occurrence of key %d should not be dropped", i)
		}
	}

	// All 100 should now be duplicates
	for i := 0; i < 100; i++ {
		rec := Record{
			Columns: map[string]Value{"id": IntValue{V: int64(i)}},
			Weight:    1,
		}
		if !df.ShouldDrop(rec) {
			t.Errorf("key %d should be detected as duplicate", i)
		}
	}
}

// Eval error on key expression: record passes through
func TestDedupFilterEvalError(t *testing.T) {
	keyExpr := &ast.ColumnRef{Name: "nonexistent"}
	df := NewDedupFilter(keyExpr, 0, 100)

	rec := Record{
		Columns: map[string]Value{"id": TextValue{V: "abc"}},
		Weight:    1,
	}
	// Column does not exist, eval may return null or error
	// Either way, record should not be dropped
	_ = df.ShouldDrop(rec) // just ensure no panic
}
