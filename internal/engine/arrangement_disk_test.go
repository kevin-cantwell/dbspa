package engine

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kevin-cantwell/dbspa/internal/sql/ast"
)

func TestDiskArrangement_MemOnlyMode(t *testing.T) {
	// memLimit=0 means purely in-memory — should behave identically to Arrangement
	dir := t.TempDir()
	keyExpr := &ast.ColumnRef{Name: "id"}
	da := NewDiskArrangement(keyExpr, 0, filepath.Join(dir, "badger"))
	defer da.Close()

	// Insert some records
	batch := Batch{
		{Columns: map[string]Value{"id": IntValue{V: 1}, "name": TextValue{V: "alice"}}, Weight: 1, Timestamp: time.Now()},
		{Columns: map[string]Value{"id": IntValue{V: 2}, "name": TextValue{V: "bob"}}, Weight: 1, Timestamp: time.Now()},
		{Columns: map[string]Value{"id": IntValue{V: 1}, "name": TextValue{V: "carol"}}, Weight: 1, Timestamp: time.Now()},
	}
	applied := da.Apply(batch)
	if len(applied) != 3 {
		t.Fatalf("expected 3 applied, got %d", len(applied))
	}

	// Lookup
	results := da.LookupValue(IntValue{V: 1})
	if len(results) != 2 {
		t.Fatalf("expected 2 results for key=1, got %d", len(results))
	}

	results = da.LookupValue(IntValue{V: 2})
	if len(results) != 1 {
		t.Fatalf("expected 1 result for key=2, got %d", len(results))
	}

	// Badger should never have been opened
	if da.db != nil {
		t.Error("expected Badger DB to be nil in mem-only mode")
	}
}

func TestDiskArrangement_SpillToDisk(t *testing.T) {
	// memLimit=10, insert 100 records, verify all 100 are findable
	dir := t.TempDir()
	keyExpr := &ast.ColumnRef{Name: "id"}
	da := NewDiskArrangement(keyExpr, 10, filepath.Join(dir, "badger"))
	defer da.Close()

	now := time.Now()
	for i := 0; i < 100; i++ {
		rec := Record{
			Columns:   map[string]Value{"id": IntValue{V: int64(i % 10)}, "seq": IntValue{V: int64(i)}},
			Weight:    1,
			Timestamp: now.Add(time.Duration(i) * time.Second),
		}
		da.Apply(Batch{rec})
	}

	// Badger should have been opened due to spill
	if da.db == nil {
		t.Error("expected Badger DB to be open after exceeding memLimit")
	}

	// Every key (0-9) should have 10 records total (across mem + disk)
	for k := int64(0); k < 10; k++ {
		results := da.LookupValue(IntValue{V: k})
		if len(results) != 10 {
			t.Errorf("key=%d: expected 10 results, got %d", k, len(results))
		}
	}

	// Non-existent key should return nil
	results := da.LookupValue(IntValue{V: 99})
	if len(results) != 0 {
		t.Errorf("expected 0 results for non-existent key, got %d", len(results))
	}
}

func TestDiskArrangement_EvictionBothStores(t *testing.T) {
	dir := t.TempDir()
	keyExpr := &ast.ColumnRef{Name: "id"}
	da := NewDiskArrangement(keyExpr, 5, filepath.Join(dir, "badger"))
	defer da.Close()

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Insert 20 records with varying timestamps
	for i := 0; i < 20; i++ {
		rec := Record{
			Columns:   map[string]Value{"id": IntValue{V: 1}, "seq": IntValue{V: int64(i)}},
			Weight:    1,
			Timestamp: base.Add(time.Duration(i) * time.Minute),
		}
		da.Apply(Batch{rec})
	}

	// Should have spilled to disk
	if da.db == nil {
		t.Fatal("expected Badger DB to be open")
	}

	// All 20 should be findable
	results := da.LookupValue(IntValue{V: 1})
	if len(results) != 20 {
		t.Fatalf("expected 20 results before eviction, got %d", len(results))
	}

	// Evict records older than 10 minutes
	cutoff := base.Add(10 * time.Minute)
	evicted := da.EvictBefore(cutoff)

	// Should have evicted 10 records (minutes 0-9)
	if len(evicted) != 10 {
		t.Fatalf("expected 10 evicted, got %d", len(evicted))
	}

	// Evicted records should have negative weights
	for _, e := range evicted {
		if e.Weight >= 0 {
			t.Errorf("expected negative weight on evicted record, got %d", e.Weight)
		}
	}

	// Only 10 should remain
	results = da.LookupValue(IntValue{V: 1})
	if len(results) != 10 {
		t.Errorf("expected 10 results after eviction, got %d", len(results))
	}
}

func TestDiskArrangement_StringKeys(t *testing.T) {
	dir := t.TempDir()
	keyExpr := &ast.ColumnRef{Name: "region"}
	da := NewDiskArrangement(keyExpr, 3, filepath.Join(dir, "badger"))
	defer da.Close()

	now := time.Now()
	regions := []string{"us-east", "us-west", "eu-west"}
	for i := 0; i < 30; i++ {
		rec := Record{
			Columns: map[string]Value{
				"region": TextValue{V: regions[i%3]},
				"seq":    IntValue{V: int64(i)},
			},
			Weight:    1,
			Timestamp: now.Add(time.Duration(i) * time.Second),
		}
		da.Apply(Batch{rec})
	}

	for _, region := range regions {
		results := da.LookupValue(TextValue{V: region})
		if len(results) != 10 {
			t.Errorf("region=%s: expected 10 results, got %d", region, len(results))
		}
	}
}

func TestDiskArrangement_Close_Cleanup(t *testing.T) {
	dir := t.TempDir()
	badgerDir := filepath.Join(dir, "badger")
	keyExpr := &ast.ColumnRef{Name: "id"}
	da := NewDiskArrangement(keyExpr, 2, badgerDir)

	// Insert enough to trigger spill
	now := time.Now()
	for i := 0; i < 10; i++ {
		rec := Record{
			Columns:   map[string]Value{"id": IntValue{V: int64(i)}, "val": IntValue{V: int64(i)}},
			Weight:    1,
			Timestamp: now.Add(time.Duration(i) * time.Second),
		}
		da.Apply(Batch{rec})
	}

	// Verify Badger dir exists
	if _, err := os.Stat(badgerDir); os.IsNotExist(err) {
		t.Fatal("expected badger dir to exist before close")
	}

	// Close should clean up
	if err := da.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify Badger dir is removed
	if _, err := os.Stat(badgerDir); !os.IsNotExist(err) {
		t.Error("expected badger dir to be removed after close")
	}
}

func TestDiskArrangement_DDJoinOpTransparent(t *testing.T) {
	// Verify DDJoinOp works transparently with DiskArrangement
	dir := t.TempDir()
	leftKeyExpr := &ast.ColumnRef{Name: "order_id"}
	rightKeyExpr := &ast.ColumnRef{Name: "order_id"}

	op := &DDJoinOp{
		Left:         NewDiskArrangement(leftKeyExpr, 5, filepath.Join(dir, "left")),
		Right:        NewDiskArrangement(rightKeyExpr, 5, filepath.Join(dir, "right")),
		LeftKeyExpr:  leftKeyExpr,
		RightKeyExpr: rightKeyExpr,
		LeftAlias:    "o",
		RightAlias:   "c",
	}
	defer op.Left.Close()
	defer op.Right.Close()

	// Seed right side with customers
	now := time.Now()
	rightBatch := Batch{
		{Columns: map[string]Value{"order_id": IntValue{V: 1}, "customer": TextValue{V: "Alice"}}, Weight: 1, Timestamp: now},
		{Columns: map[string]Value{"order_id": IntValue{V: 2}, "customer": TextValue{V: "Bob"}}, Weight: 1, Timestamp: now},
		{Columns: map[string]Value{"order_id": IntValue{V: 3}, "customer": TextValue{V: "Carol"}}, Weight: 1, Timestamp: now},
	}
	op.Right.Apply(rightBatch)

	// Process left deltas
	leftBatch := Batch{
		{Columns: map[string]Value{"order_id": IntValue{V: 1}, "amount": IntValue{V: 100}}, Weight: 1, Timestamp: now},
		{Columns: map[string]Value{"order_id": IntValue{V: 2}, "amount": IntValue{V: 200}}, Weight: 1, Timestamp: now},
		{Columns: map[string]Value{"order_id": IntValue{V: 4}, "amount": IntValue{V: 400}}, Weight: 1, Timestamp: now}, // no match
	}

	results := op.ProcessLeftDeltaSlice(leftBatch)

	// Should get 2 joined results (order_id 1 and 2 match)
	if len(results) != 2 {
		t.Fatalf("expected 2 join results, got %d", len(results))
	}

	// Verify join produced correct columns
	for _, r := range results {
		orderID := r.Get("order_id")
		customer := r.Get("customer")
		amount := r.Get("amount")
		if orderID.IsNull() || customer.IsNull() || amount.IsNull() {
			t.Errorf("unexpected null in joined record: order_id=%v customer=%v amount=%v", orderID, customer, amount)
		}
	}
}

func TestDiskArrangement_WeightNetting(t *testing.T) {
	// Test that Z-set weight netting works correctly with disk spill
	dir := t.TempDir()
	keyExpr := &ast.ColumnRef{Name: "id"}
	da := NewDiskArrangement(keyExpr, 3, filepath.Join(dir, "badger"))
	defer da.Close()

	now := time.Now()

	// Insert a record
	da.Apply(Batch{
		{Columns: map[string]Value{"id": IntValue{V: 1}, "name": TextValue{V: "alice"}}, Weight: 1, Timestamp: now},
	})

	// Retract it
	da.Apply(Batch{
		{Columns: map[string]Value{"id": IntValue{V: 1}, "name": TextValue{V: "alice"}}, Weight: -1, Timestamp: now},
	})

	// Should be gone from memory
	results := da.LookupValue(IntValue{V: 1})
	if len(results) != 0 {
		t.Errorf("expected 0 results after retraction, got %d", len(results))
	}
}

func TestDiskArrangement_LookupValueUnsafe(t *testing.T) {
	dir := t.TempDir()
	keyExpr := &ast.ColumnRef{Name: "id"}
	da := NewDiskArrangement(keyExpr, 5, filepath.Join(dir, "badger"))
	defer da.Close()

	now := time.Now()
	for i := 0; i < 20; i++ {
		rec := Record{
			Columns:   map[string]Value{"id": IntValue{V: int64(i % 5)}, "seq": IntValue{V: int64(i)}},
			Weight:    1,
			Timestamp: now.Add(time.Duration(i) * time.Second),
		}
		da.Apply(Batch{rec})
	}

	// LookupValueUnsafe should return same results as LookupValue
	for k := int64(0); k < 5; k++ {
		safe := da.LookupValue(IntValue{V: k})
		unsafe := da.LookupValueUnsafe(IntValue{V: k})
		if len(safe) != len(unsafe) {
			t.Errorf("key=%d: safe=%d unsafe=%d", k, len(safe), len(unsafe))
		}
	}
}

func TestDiskArrangement_NullKeySkipped(t *testing.T) {
	dir := t.TempDir()
	keyExpr := &ast.ColumnRef{Name: "id"}
	da := NewDiskArrangement(keyExpr, 0, filepath.Join(dir, "badger"))
	defer da.Close()

	// NULL lookups should return nil
	results := da.LookupValue(NullValue{})
	if results != nil {
		t.Errorf("expected nil for NULL key, got %v", results)
	}

	results = da.Lookup(NullValue{})
	if results != nil {
		t.Errorf("expected nil for NULL key via Lookup, got %v", results)
	}
}

func TestDiskArrangement_ColumnNames(t *testing.T) {
	dir := t.TempDir()
	keyExpr := &ast.ColumnRef{Name: "id"}
	da := NewDiskArrangement(keyExpr, 0, filepath.Join(dir, "badger"))
	defer da.Close()

	da.Apply(Batch{
		{Columns: map[string]Value{"id": IntValue{V: 1}, "name": TextValue{V: "alice"}, "age": IntValue{V: 30}}, Weight: 1, Timestamp: time.Now()},
	})

	names := da.ColumnNames()
	nameSet := make(map[string]bool)
	for _, n := range names {
		nameSet[n] = true
	}

	for _, expected := range []string{"id", "name", "age"} {
		if !nameSet[expected] {
			t.Errorf("expected column %q in ColumnNames", expected)
		}
	}
}

func TestDiskArrangement_RecordSerialization(t *testing.T) {
	// Test that all value types survive marshaling round-trip
	now := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	rec := Record{
		Columns: map[string]Value{
			"null_col":  NullValue{},
			"bool_col":  BoolValue{V: true},
			"int_col":   IntValue{V: 42},
			"float_col": FloatValue{V: 3.14},
			"text_col":  TextValue{V: "hello world"},
			"ts_col":    TimestampValue{V: now},
		},
		Timestamp: now,
		Weight:    3,
	}

	data, err := marshalRecord(rec)
	if err != nil {
		t.Fatalf("marshalRecord: %v", err)
	}

	got, err := unmarshalRecord(data)
	if err != nil {
		t.Fatalf("unmarshalRecord: %v", err)
	}

	if got.Weight != rec.Weight {
		t.Errorf("weight: got %d want %d", got.Weight, rec.Weight)
	}
	if !got.Timestamp.Equal(rec.Timestamp) {
		t.Errorf("timestamp: got %v want %v", got.Timestamp, rec.Timestamp)
	}
	if len(got.Columns) != len(rec.Columns) {
		t.Fatalf("columns: got %d want %d", len(got.Columns), len(rec.Columns))
	}

	// Check each value type
	checks := map[string]string{
		"null_col":  "NULL",
		"bool_col":  "true",
		"int_col":   "42",
		"float_col": "3.14",
		"text_col":  "hello world",
		"ts_col":    now.Format(time.RFC3339Nano),
	}
	for col, expected := range checks {
		v := got.Columns[col]
		if v == nil {
			t.Errorf("column %s: nil", col)
			continue
		}
		if v.String() != expected {
			t.Errorf("column %s: got %q want %q", col, v.String(), expected)
		}
	}
}
