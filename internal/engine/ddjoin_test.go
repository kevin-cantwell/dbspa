package engine

import (
	"sync"
	"testing"
	"time"

	"github.com/kevin-cantwell/folddb/internal/sql/ast"
)

// Helper to create a record with the given columns and weight.
func ddRec(cols map[string]Value, weight int) Record {
	return Record{
		Columns:   cols,
		Timestamp: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		Weight:    weight,
	}
}

// Helper to create a record with a specific timestamp.
func ddRecAt(cols map[string]Value, weight int, ts time.Time) Record {
	return Record{
		Columns:   cols,
		Timestamp: ts,
		Weight:    weight,
	}
}

func TestDDJoinOp_InnerJoin_StreamToFile(t *testing.T) {
	// Simulate the current stream-to-file use case through DD join.
	// Right side is loaded once (file), left side receives stream deltas.
	op := NewDDJoinOp(
		&ast.QualifiedRef{Qualifier: "e", Name: "user_id"},
		&ast.QualifiedRef{Qualifier: "u", Name: "id"},
		"e", "u", false,
	)

	// Load right (table) side
	op.Right.Apply(Batch{
		ddRec(map[string]Value{"id": IntValue{V: 1}, "name": TextValue{V: "Alice"}}, 1),
		ddRec(map[string]Value{"id": IntValue{V: 2}, "name": TextValue{V: "Bob"}}, 1),
		ddRec(map[string]Value{"id": IntValue{V: 3}, "name": TextValue{V: "Charlie"}}, 1),
	})

	// Stream record that matches
	results := op.ProcessLeftDeltaSlice(Batch{
		ddRec(map[string]Value{"user_id": IntValue{V: 1}, "action": TextValue{V: "login"}}, 1),
	})
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	r := results[0]
	if v := r.Columns["e.user_id"]; v == nil || v.String() != "1" {
		t.Errorf("expected e.user_id=1, got %v", v)
	}
	if v := r.Columns["u.name"]; v == nil || v.String() != "Alice" {
		t.Errorf("expected u.name=Alice, got %v", v)
	}
	if v := r.Columns["e.action"]; v == nil || v.String() != "login" {
		t.Errorf("expected e.action=login, got %v", v)
	}
	if r.Weight != 1 {
		t.Errorf("expected weight=1, got %d", r.Weight)
	}

	// Stream record that does NOT match (inner join: no result)
	results = op.ProcessLeftDeltaSlice(Batch{
		ddRec(map[string]Value{"user_id": IntValue{V: 99}}, 1),
	})
	if len(results) != 0 {
		t.Fatalf("expected 0 results for unmatched key, got %d", len(results))
	}
}

func TestDDJoinOp_LeftJoin(t *testing.T) {
	op := NewDDJoinOp(
		&ast.QualifiedRef{Qualifier: "e", Name: "user_id"},
		&ast.QualifiedRef{Qualifier: "u", Name: "id"},
		"e", "u", true, // LEFT JOIN
	)

	// Load right side
	op.Right.Apply(Batch{
		ddRec(map[string]Value{"id": IntValue{V: 1}, "name": TextValue{V: "Alice"}}, 1),
	})

	// Match
	results := op.ProcessLeftDeltaSlice(Batch{
		ddRec(map[string]Value{"user_id": IntValue{V: 1}}, 1),
	})
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if v := results[0].Columns["u.name"]; v.String() != "Alice" {
		t.Errorf("expected Alice, got %v", v)
	}

	// No match — LEFT JOIN should emit NULLs
	results = op.ProcessLeftDeltaSlice(Batch{
		ddRec(map[string]Value{"user_id": IntValue{V: 99}}, 1),
	})
	if len(results) != 1 {
		t.Fatalf("expected 1 result for LEFT JOIN unmatched, got %d", len(results))
	}
	if v := results[0].Columns["u.name"]; !v.IsNull() {
		t.Errorf("expected NULL for unmatched table column, got %v", v)
	}
	if v := results[0].Columns["e.user_id"]; v == nil || v.String() != "99" {
		t.Errorf("expected stream column preserved, got %v", v)
	}
}

func TestDDJoinOp_WeightMultiplication(t *testing.T) {
	op := NewDDJoinOp(
		&ast.ColumnRef{Name: "user_id"},
		&ast.ColumnRef{Name: "id"},
		"", "", false,
	)

	// Right side with weight=2
	op.Right.Apply(Batch{
		ddRec(map[string]Value{"id": IntValue{V: 1}, "name": TextValue{V: "Alice"}}, 2),
	})

	// Left delta with weight=3 → output weight = 3 * 2 = 6
	results := op.ProcessLeftDeltaSlice(Batch{
		ddRec(map[string]Value{"user_id": IntValue{V: 1}, "action": TextValue{V: "click"}}, 3),
	})
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Weight != 6 {
		t.Errorf("expected weight=6, got %d", results[0].Weight)
	}
}

func TestDDJoinOp_StreamRetraction(t *testing.T) {
	op := NewDDJoinOp(
		&ast.ColumnRef{Name: "user_id"},
		&ast.ColumnRef{Name: "id"},
		"", "", false,
	)

	// Load right side
	op.Right.Apply(Batch{
		ddRec(map[string]Value{"id": IntValue{V: 1}, "name": TextValue{V: "Alice"}}, 1),
	})

	// Insert stream record
	results := op.ProcessLeftDeltaSlice(Batch{
		ddRec(map[string]Value{"user_id": IntValue{V: 1}, "action": TextValue{V: "login"}}, 1),
	})
	if len(results) != 1 || results[0].Weight != 1 {
		t.Fatalf("expected 1 result with weight=1, got %d results", len(results))
	}

	// Retract stream record (weight=-1) → output should also be weight=-1
	results = op.ProcessLeftDeltaSlice(Batch{
		ddRec(map[string]Value{"user_id": IntValue{V: 1}, "action": TextValue{V: "login"}}, -1),
	})
	if len(results) != 1 {
		t.Fatalf("expected 1 retraction result, got %d", len(results))
	}
	if results[0].Weight != -1 {
		t.Errorf("expected weight=-1 for retraction, got %d", results[0].Weight)
	}
	if v := results[0].Columns["name"]; v.String() != "Alice" {
		t.Errorf("retraction should carry joined columns, got name=%v", v)
	}
}

func TestDDJoinOp_RightSideChanges_CDC(t *testing.T) {
	// This is the killer feature: right-side CDC updates propagate through the join.
	//
	// Scenario:
	// 1. Right arrangement has {id:1, name:"Alice"}
	// 2. Left stream: {user_id:1, action:"login"} → emits joined row
	// 3. Right CDC delta: retract Alice, insert Alicia
	// 4. ProcessRightDelta should retract old joined row and emit new one

	op := NewDDJoinOp(
		&ast.ColumnRef{Name: "user_id"},
		&ast.ColumnRef{Name: "id"},
		"", "", false,
	)

	// Step 1: Load initial right side
	op.Right.Apply(Batch{
		ddRec(map[string]Value{"id": IntValue{V: 1}, "name": TextValue{V: "Alice"}}, 1),
	})

	// Step 2: Left stream insert
	results := op.ProcessLeftDeltaSlice(Batch{
		ddRec(map[string]Value{"user_id": IntValue{V: 1}, "action": TextValue{V: "login"}}, 1),
	})
	if len(results) != 1 {
		t.Fatalf("step 2: expected 1 result, got %d", len(results))
	}
	if results[0].Weight != 1 {
		t.Errorf("step 2: expected weight=+1, got %d", results[0].Weight)
	}
	if v := results[0].Columns["name"]; v.String() != "Alice" {
		t.Errorf("step 2: expected name=Alice, got %v", v)
	}

	// Step 3+4: Right CDC update — name changes from "Alice" to "Alicia"
	cdcDelta := Batch{
		ddRec(map[string]Value{"id": IntValue{V: 1}, "name": TextValue{V: "Alice"}}, -1),  // retract old
		ddRec(map[string]Value{"id": IntValue{V: 1}, "name": TextValue{V: "Alicia"}}, +1), // insert new
	}
	results = op.ProcessRightDeltaSlice(cdcDelta)

	// Should produce 2 output records: retraction of old, insertion of new
	if len(results) != 2 {
		t.Fatalf("step 4: expected 2 results, got %d: %+v", len(results), results)
	}

	// Find retraction and insertion
	var retract, insert *Record
	for i := range results {
		if results[i].Weight < 0 {
			retract = &results[i]
		} else if results[i].Weight > 0 {
			insert = &results[i]
		}
	}

	if retract == nil {
		t.Fatal("step 4: expected a retraction (weight < 0)")
	}
	if insert == nil {
		t.Fatal("step 4: expected an insertion (weight > 0)")
	}

	if retract.Weight != -1 {
		t.Errorf("step 4: retraction weight should be -1, got %d", retract.Weight)
	}
	if v := retract.Columns["name"]; v.String() != "Alice" {
		t.Errorf("step 4: retraction should reference Alice, got %v", v)
	}
	if v := retract.Columns["action"]; v.String() != "login" {
		t.Errorf("step 4: retraction should carry left columns, got action=%v", v)
	}

	if insert.Weight != 1 {
		t.Errorf("step 4: insertion weight should be +1, got %d", insert.Weight)
	}
	if v := insert.Columns["name"]; v.String() != "Alicia" {
		t.Errorf("step 4: insertion should reference Alicia, got %v", v)
	}
	if v := insert.Columns["action"]; v.String() != "login" {
		t.Errorf("step 4: insertion should carry left columns, got action=%v", v)
	}
}

func TestDDJoinOp_MultipleMatches(t *testing.T) {
	op := NewDDJoinOp(
		&ast.ColumnRef{Name: "user_id"},
		&ast.ColumnRef{Name: "id"},
		"", "", false,
	)

	// Two right records with the same key
	op.Right.Apply(Batch{
		ddRec(map[string]Value{"id": IntValue{V: 1}, "item": TextValue{V: "A"}}, 1),
		ddRec(map[string]Value{"id": IntValue{V: 1}, "item": TextValue{V: "B"}}, 1),
	})

	results := op.ProcessLeftDeltaSlice(Batch{
		ddRec(map[string]Value{"user_id": IntValue{V: 1}}, 1),
	})
	if len(results) != 2 {
		t.Fatalf("expected 2 results for key with 2 right matches, got %d", len(results))
	}
}

func TestDDJoinOp_NullKey(t *testing.T) {
	op := NewDDJoinOp(
		&ast.ColumnRef{Name: "user_id"},
		&ast.ColumnRef{Name: "id"},
		"", "", false,
	)

	op.Right.Apply(Batch{
		ddRec(map[string]Value{"id": IntValue{V: 1}, "name": TextValue{V: "Alice"}}, 1),
	})

	// NULL key should not match
	results := op.ProcessLeftDeltaSlice(Batch{
		ddRec(map[string]Value{"user_id": NullValue{}}, 1),
	})
	if len(results) != 0 {
		t.Fatalf("expected 0 results for NULL key, got %d", len(results))
	}
}

func TestArrangement_Apply_WeightNetting(t *testing.T) {
	a := NewArrangement(&ast.ColumnRef{Name: "id"})

	// Insert
	a.Apply(Batch{
		ddRec(map[string]Value{"id": IntValue{V: 1}, "name": TextValue{V: "Alice"}}, 1),
	})
	entries := a.LookupString("1")
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry after insert, got %d", len(entries))
	}
	if entries[0].Weight != 1 {
		t.Errorf("expected weight=1, got %d", entries[0].Weight)
	}

	// Insert again (same record) → weight should be 2
	a.Apply(Batch{
		ddRec(map[string]Value{"id": IntValue{V: 1}, "name": TextValue{V: "Alice"}}, 1),
	})
	entries = a.LookupString("1")
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry after duplicate insert, got %d", len(entries))
	}
	if entries[0].Weight != 2 {
		t.Errorf("expected weight=2, got %d", entries[0].Weight)
	}

	// Retract → weight should be 1
	a.Apply(Batch{
		ddRec(map[string]Value{"id": IntValue{V: 1}, "name": TextValue{V: "Alice"}}, -1),
	})
	entries = a.LookupString("1")
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry after partial retraction, got %d", len(entries))
	}
	if entries[0].Weight != 1 {
		t.Errorf("expected weight=1, got %d", entries[0].Weight)
	}

	// Fully retract → entry should be removed
	a.Apply(Batch{
		ddRec(map[string]Value{"id": IntValue{V: 1}, "name": TextValue{V: "Alice"}}, -1),
	})
	entries = a.LookupString("1")
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries after full retraction, got %d", len(entries))
	}
}

func TestArrangement_DifferentRecordsSameKey(t *testing.T) {
	a := NewArrangement(&ast.ColumnRef{Name: "id"})

	// Two different records with the same key
	a.Apply(Batch{
		ddRec(map[string]Value{"id": IntValue{V: 1}, "name": TextValue{V: "Alice"}}, 1),
		ddRec(map[string]Value{"id": IntValue{V: 1}, "name": TextValue{V: "Bob"}}, 1),
	})
	entries := a.LookupString("1")
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries for same key different records, got %d", len(entries))
	}
}

func TestDDJoinOp_RightSideChanges_MultipleLeftRecords(t *testing.T) {
	// When the right side changes, ALL matching left records should emit updates.
	op := NewDDJoinOp(
		&ast.ColumnRef{Name: "user_id"},
		&ast.ColumnRef{Name: "id"},
		"", "", false,
	)

	// Initial right side
	op.Right.Apply(Batch{
		ddRec(map[string]Value{"id": IntValue{V: 1}, "name": TextValue{V: "Alice"}}, 1),
	})

	// Two left stream records for the same key
	op.ProcessLeftDeltaSlice(Batch{
		ddRec(map[string]Value{"user_id": IntValue{V: 1}, "action": TextValue{V: "login"}}, 1),
	})
	op.ProcessLeftDeltaSlice(Batch{
		ddRec(map[string]Value{"user_id": IntValue{V: 1}, "action": TextValue{V: "click"}}, 1),
	})

	// Right side update: Alice → Alicia
	results := op.ProcessRightDeltaSlice(Batch{
		ddRec(map[string]Value{"id": IntValue{V: 1}, "name": TextValue{V: "Alice"}}, -1),
		ddRec(map[string]Value{"id": IntValue{V: 1}, "name": TextValue{V: "Alicia"}}, +1),
	})

	// Should emit retractions and insertions for BOTH left records
	// 2 left records × 2 right deltas = 4 output records
	if len(results) != 4 {
		t.Fatalf("expected 4 results (2 retractions + 2 insertions), got %d", len(results))
	}

	retractions := 0
	insertions := 0
	for _, r := range results {
		if r.Weight < 0 {
			retractions++
		} else if r.Weight > 0 {
			insertions++
		}
	}
	if retractions != 2 {
		t.Errorf("expected 2 retractions, got %d", retractions)
	}
	if insertions != 2 {
		t.Errorf("expected 2 insertions, got %d", insertions)
	}
}

func TestArrangement_EvictBefore(t *testing.T) {
	a := NewArrangement(&ast.ColumnRef{Name: "id"})

	t0 := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	t1 := time.Date(2025, 1, 1, 10, 5, 0, 0, time.UTC)
	t2 := time.Date(2025, 1, 1, 10, 10, 0, 0, time.UTC)

	// Insert records at different times
	a.Apply(Batch{
		ddRecAt(map[string]Value{"id": IntValue{V: 1}, "name": TextValue{V: "Alice"}}, 1, t0),
		ddRecAt(map[string]Value{"id": IntValue{V: 2}, "name": TextValue{V: "Bob"}}, 1, t1),
		ddRecAt(map[string]Value{"id": IntValue{V: 3}, "name": TextValue{V: "Charlie"}}, 1, t2),
	})

	// Evict before t1 — should remove Alice (t0 < t1)
	cutoff := t1
	evicted := a.EvictBefore(cutoff)
	if len(evicted) != 1 {
		t.Fatalf("expected 1 evicted record, got %d", len(evicted))
	}
	if evicted[0].Weight != -1 {
		t.Errorf("evicted weight should be -1, got %d", evicted[0].Weight)
	}
	if v := evicted[0].Columns["name"]; v.String() != "Alice" {
		t.Errorf("expected evicted record to be Alice, got %v", v)
	}

	// Verify arrangement state: Bob and Charlie remain
	if entries := a.LookupString("1"); len(entries) != 0 {
		t.Errorf("expected Alice removed from arrangement, got %d entries", len(entries))
	}
	if entries := a.LookupString("2"); len(entries) != 1 {
		t.Errorf("expected Bob still in arrangement, got %d entries", len(entries))
	}
	if entries := a.LookupString("3"); len(entries) != 1 {
		t.Errorf("expected Charlie still in arrangement, got %d entries", len(entries))
	}
}

func TestArrangement_EvictBefore_Empty(t *testing.T) {
	a := NewArrangement(&ast.ColumnRef{Name: "id"})
	evicted := a.EvictBefore(time.Now())
	if len(evicted) != 0 {
		t.Errorf("expected 0 evicted from empty arrangement, got %d", len(evicted))
	}
}

func TestArrangement_EvictBefore_NothingExpired(t *testing.T) {
	a := NewArrangement(&ast.ColumnRef{Name: "id"})
	future := time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC)
	a.Apply(Batch{
		ddRecAt(map[string]Value{"id": IntValue{V: 1}}, 1, future),
	})
	evicted := a.EvictBefore(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	if len(evicted) != 0 {
		t.Errorf("expected 0 evicted (all in future), got %d", len(evicted))
	}
}

func TestDDJoinOp_StreamStream_BothSides(t *testing.T) {
	// Two streams, matching records arrive on both sides → joined output emitted
	op := NewDDJoinOp(
		&ast.ColumnRef{Name: "order_id"},
		&ast.ColumnRef{Name: "order_id"},
		"o", "p", false,
	)

	// Left stream: order arrives
	results := op.ProcessLeftDeltaSlice(Batch{
		ddRec(map[string]Value{"order_id": IntValue{V: 1}, "amount": IntValue{V: 100}}, 1),
	})
	// No right-side records yet → no output
	if len(results) != 0 {
		t.Fatalf("expected 0 results before right arrives, got %d", len(results))
	}

	// Right stream: payment arrives for same order
	results = op.ProcessRightDeltaSlice(Batch{
		ddRec(map[string]Value{"order_id": IntValue{V: 1}, "payment_id": TextValue{V: "pay-1"}}, 1),
	})
	// Now both sides have a match → should produce 1 joined row
	if len(results) != 1 {
		t.Fatalf("expected 1 result after both sides arrive, got %d", len(results))
	}
	if results[0].Weight != 1 {
		t.Errorf("expected weight=1, got %d", results[0].Weight)
	}
	if v := results[0].Columns["o.amount"]; v == nil || v.String() != "100" {
		t.Errorf("expected o.amount=100, got %v", v)
	}
	if v := results[0].Columns["p.payment_id"]; v == nil || v.String() != "pay-1" {
		t.Errorf("expected p.payment_id=pay-1, got %v", v)
	}
}

func TestDDJoinOp_StreamStream_EvictionRetractions(t *testing.T) {
	// Eviction removes old entries and produces join retractions
	op := NewDDJoinOp(
		&ast.ColumnRef{Name: "order_id"},
		&ast.ColumnRef{Name: "order_id"},
		"o", "p", false,
	)

	t0 := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	t1 := time.Date(2025, 1, 1, 10, 5, 0, 0, time.UTC)

	// Left stream: order at t0
	op.ProcessLeftDeltaSlice(Batch{
		ddRecAt(map[string]Value{"order_id": IntValue{V: 1}, "amount": IntValue{V: 100}}, 1, t0),
	})

	// Right stream: payment at t1
	results := op.ProcessRightDeltaSlice(Batch{
		ddRecAt(map[string]Value{"order_id": IntValue{V: 1}, "payment_id": TextValue{V: "pay-1"}}, 1, t1),
	})
	if len(results) != 1 {
		t.Fatalf("expected 1 join result, got %d", len(results))
	}

	// Evict left entries before t1 (removes the order)
	cutoff := t1
	leftEvicted := op.Left.EvictBefore(cutoff)
	if len(leftEvicted) != 1 {
		t.Fatalf("expected 1 evicted from left, got %d", len(leftEvicted))
	}

	// Process the eviction through the join → should produce retraction
	retractions := op.ProcessLeftDeltaSlice(leftEvicted)
	if len(retractions) != 1 {
		t.Fatalf("expected 1 retraction from eviction, got %d", len(retractions))
	}
	if retractions[0].Weight >= 0 {
		t.Errorf("expected negative weight (retraction), got %d", retractions[0].Weight)
	}
}

func TestDDJoinOp_StreamStream_ExpiredDoesNotJoin(t *testing.T) {
	// After eviction, a record arriving for the evicted key should not join
	op := NewDDJoinOp(
		&ast.ColumnRef{Name: "order_id"},
		&ast.ColumnRef{Name: "order_id"},
		"o", "p", false,
	)

	t0 := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	tEvict := time.Date(2025, 1, 1, 10, 11, 0, 0, time.UTC)

	// Left stream: order at t0
	op.ProcessLeftDeltaSlice(Batch{
		ddRecAt(map[string]Value{"order_id": IntValue{V: 1}, "amount": IntValue{V: 100}}, 1, t0),
	})

	// Evict everything before tEvict
	op.Left.EvictBefore(tEvict)

	// Right stream: payment arrives after eviction — no match
	results := op.ProcessRightDeltaSlice(Batch{
		ddRecAt(map[string]Value{"order_id": IntValue{V: 1}, "payment_id": TextValue{V: "pay-late"}}, 1, tEvict),
	})
	if len(results) != 0 {
		t.Fatalf("expected 0 results after eviction, got %d", len(results))
	}
}

func TestDDJoinOp_ConcurrentLeftRight(t *testing.T) {
	// Verify concurrent left+right deltas don't race
	op := NewDDJoinOp(
		&ast.ColumnRef{Name: "id"},
		&ast.ColumnRef{Name: "id"},
		"l", "r", false,
	)

	out := make(chan Record, 1000)
	var wg sync.WaitGroup

	// Left goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			op.ProcessLeftDelta(Batch{
				ddRec(map[string]Value{"id": IntValue{V: int64(i)}, "side": TextValue{V: "left"}}, 1),
			}, out)
		}
	}()

	// Right goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			op.ProcessRightDelta(Batch{
				ddRec(map[string]Value{"id": IntValue{V: int64(i)}, "side": TextValue{V: "right"}}, 1),
			}, out)
		}
	}()

	wg.Wait()
	close(out)

	// Count results — some matches will occur depending on timing
	count := 0
	for range out {
		count++
	}
	// With 100 left and 100 right records on overlapping keys,
	// some joins will fire. The exact count depends on scheduling,
	// but the important thing is no panic/race.
	t.Logf("concurrent test produced %d joined records", count)
}

func TestDDJoinOp_StreamStream_WeightMultiplication(t *testing.T) {
	op := NewDDJoinOp(
		&ast.ColumnRef{Name: "id"},
		&ast.ColumnRef{Name: "id"},
		"", "", false,
	)

	// Left with weight=3
	op.ProcessLeftDeltaSlice(Batch{
		ddRec(map[string]Value{"id": IntValue{V: 1}, "a": TextValue{V: "x"}}, 3),
	})

	// Right with weight=2 → output weight = 3 * 2 = 6
	results := op.ProcessRightDeltaSlice(Batch{
		ddRec(map[string]Value{"id": IntValue{V: 1}, "b": TextValue{V: "y"}}, 2),
	})
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Weight != 6 {
		t.Errorf("expected weight=6, got %d", results[0].Weight)
	}
}

func TestDDJoinOp_EvictAndRetract(t *testing.T) {
	op := NewDDJoinOp(
		&ast.ColumnRef{Name: "order_id"},
		&ast.ColumnRef{Name: "order_id"},
		"o", "p", false,
	)

	t0 := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	t1 := time.Date(2025, 1, 1, 10, 5, 0, 0, time.UTC)
	tLate := time.Date(2025, 1, 1, 10, 15, 0, 0, time.UTC)

	// Both sides at t0
	op.ProcessLeftDeltaSlice(Batch{
		ddRecAt(map[string]Value{"order_id": IntValue{V: 1}, "amount": IntValue{V: 100}}, 1, t0),
	})
	op.ProcessRightDeltaSlice(Batch{
		ddRecAt(map[string]Value{"order_id": IntValue{V: 1}, "payment_id": TextValue{V: "pay-1"}}, 1, t0),
	})

	// Record at t1 that should survive eviction
	op.ProcessLeftDeltaSlice(Batch{
		ddRecAt(map[string]Value{"order_id": IntValue{V: 2}, "amount": IntValue{V: 200}}, 1, tLate),
	})
	op.ProcessRightDeltaSlice(Batch{
		ddRecAt(map[string]Value{"order_id": IntValue{V: 2}, "payment_id": TextValue{V: "pay-2"}}, 1, tLate),
	})

	// Evict before t1 — should evict order_id=1 from both sides
	out := make(chan Record, 100)
	op.EvictAndRetract(t1, out)
	close(out)

	var retractions []Record
	for rec := range out {
		retractions = append(retractions, rec)
	}

	// Should have retractions for the joined pair (order_id=1)
	// Left eviction produces retraction against right, right eviction produces retraction against left
	// But since left is evicted first and removed, right eviction won't find left matches.
	// So we get 1 retraction (from left eviction joined against right).
	// Then right is evicted but left arrangement no longer has order_id=1, so 0 from right eviction.
	if len(retractions) != 1 {
		t.Fatalf("expected 1 retraction from EvictAndRetract, got %d", len(retractions))
	}
	if retractions[0].Weight >= 0 {
		t.Errorf("expected negative weight, got %d", retractions[0].Weight)
	}

	// Verify order_id=2 still in both arrangements
	if entries := op.Left.LookupString("2"); len(entries) != 1 {
		t.Errorf("expected order_id=2 in left arrangement, got %d entries", len(entries))
	}
	if entries := op.Right.LookupString("2"); len(entries) != 1 {
		t.Errorf("expected order_id=2 in right arrangement, got %d entries", len(entries))
	}
}

func TestDDJoinOp_EvictAndRetract_Concurrent(t *testing.T) {
	// Verify that EvictAndRetract is safe to call concurrently with Process* methods
	op := NewDDJoinOp(
		&ast.ColumnRef{Name: "id"},
		&ast.ColumnRef{Name: "id"},
		"", "", false,
	)

	out := make(chan Record, 10000)
	var wg sync.WaitGroup

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			ts := time.Date(2025, 1, 1, 10, 0, i, 0, time.UTC)
			op.ProcessLeftDelta(Batch{
				ddRecAt(map[string]Value{"id": IntValue{V: int64(i)}}, 1, ts),
			}, out)
			op.ProcessRightDelta(Batch{
				ddRecAt(map[string]Value{"id": IntValue{V: int64(i)}}, 1, ts),
			}, out)
		}
	}()

	// Evictor goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			cutoff := time.Date(2025, 1, 1, 10, 0, i, 0, time.UTC)
			op.EvictAndRetract(cutoff, out)
		}
	}()

	wg.Wait()
	close(out)
	count := 0
	for range out {
		count++
	}
	t.Logf("concurrent evict test produced %d records", count)
}

func BenchmarkDDJoin_StreamToFile(b *testing.B) {
	// Simulate stream-to-file join: right side is static, left side streams.
	op := NewDDJoinOp(
		&ast.QualifiedRef{Qualifier: "e", Name: "customer_id"},
		&ast.QualifiedRef{Qualifier: "c", Name: "id"},
		"e", "c", false,
	)
	op.RightIsStatic = true

	// Load 1000 right-side records
	rightBatch := make(Batch, 1000)
	for i := 0; i < 1000; i++ {
		rightBatch[i] = ddRec(map[string]Value{
			"id":   IntValue{V: int64(i)},
			"tier": TextValue{V: "gold"},
			"name": TextValue{V: "Customer"},
		}, 1)
	}
	op.Right.Apply(rightBatch)

	// Pre-build left records
	leftRecords := make([]Record, b.N)
	for i := 0; i < b.N; i++ {
		leftRecords[i] = ddRec(map[string]Value{
			"customer_id": IntValue{V: int64(i % 1000)},
			"order_id":    IntValue{V: int64(i)},
			"amount":      FloatValue{V: 99.99},
			"product":     TextValue{V: "widget"},
			"region":      TextValue{V: "us-west"},
		}, 1)
	}

	out := make(chan Record, 256)
	go func() {
		for range out {
		}
	}()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key, _ := EvalKeyExpr(op.LeftKeyExpr, leftRecords[i])
		rightMatches := op.Right.LookupStringUnsafe(key.String())
		for _, rightRec := range rightMatches {
			merged := op.merge(leftRecords[i], rightRec)
			merged.Weight = leftRecords[i].Weight * rightRec.Weight
			out <- merged
		}
	}
}
