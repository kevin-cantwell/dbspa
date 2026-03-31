package engine

import (
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
