package engine

import (
	"testing"

	"github.com/kevin-cantwell/folddb/internal/sql/ast"
)

func TestExtractEquiJoinKeys(t *testing.T) {
	// e.user_id = u.id
	cond := &ast.BinaryExpr{
		Left:  &ast.QualifiedRef{Qualifier: "e", Name: "user_id"},
		Op:    "=",
		Right: &ast.QualifiedRef{Qualifier: "u", Name: "id"},
	}

	stream, table, err := ExtractEquiJoinKeys(cond, "e", "u")
	if err != nil {
		t.Fatal(err)
	}
	sRef, ok := stream.(*ast.QualifiedRef)
	if !ok || sRef.Qualifier != "e" || sRef.Name != "user_id" {
		t.Fatalf("expected e.user_id, got %#v", stream)
	}
	tRef, ok := table.(*ast.QualifiedRef)
	if !ok || tRef.Qualifier != "u" || tRef.Name != "id" {
		t.Fatalf("expected u.id, got %#v", table)
	}
}

func TestExtractEquiJoinKeys_Reversed(t *testing.T) {
	// u.id = e.user_id (reversed order)
	cond := &ast.BinaryExpr{
		Left:  &ast.QualifiedRef{Qualifier: "u", Name: "id"},
		Op:    "=",
		Right: &ast.QualifiedRef{Qualifier: "e", Name: "user_id"},
	}

	stream, table, err := ExtractEquiJoinKeys(cond, "e", "u")
	if err != nil {
		t.Fatal(err)
	}
	sRef, ok := stream.(*ast.QualifiedRef)
	if !ok || sRef.Name != "user_id" {
		t.Fatalf("expected stream key user_id, got %#v", stream)
	}
	tRef, ok := table.(*ast.QualifiedRef)
	if !ok || tRef.Name != "id" {
		t.Fatalf("expected table key id, got %#v", table)
	}
}

func TestHashJoinOp_InnerJoin(t *testing.T) {
	tableRecords := []Record{
		{Columns: map[string]Value{"id": IntValue{V: 1}, "name": TextValue{V: "Alice"}}, Weight: 1},
		{Columns: map[string]Value{"id": IntValue{V: 2}, "name": TextValue{V: "Bob"}}, Weight: 1},
		{Columns: map[string]Value{"id": IntValue{V: 3}, "name": TextValue{V: "Charlie"}}, Weight: 1},
	}

	op := &HashJoinOp{
		StreamKeyExpr: &ast.QualifiedRef{Qualifier: "e", Name: "user_id"},
		TableKeyExpr:  &ast.QualifiedRef{Qualifier: "u", Name: "id"},
		LeftJoin:      false,
		StreamAlias:   "e",
		TableAlias:    "u",
	}
	if err := op.BuildIndex(tableRecords); err != nil {
		t.Fatal(err)
	}

	// Match
	results := op.Probe(Record{Columns: map[string]Value{"user_id": IntValue{V: 1}, "action": TextValue{V: "login"}}, Weight: 1})
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	// Check that both stream and table columns are present
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

	// No match
	results = op.Probe(Record{Columns: map[string]Value{"user_id": IntValue{V: 99}}, Weight: 1})
	if len(results) != 0 {
		t.Fatalf("expected 0 results for unmatched key, got %d", len(results))
	}
}

func TestHashJoinOp_LeftJoin(t *testing.T) {
	tableRecords := []Record{
		{Columns: map[string]Value{"id": IntValue{V: 1}, "name": TextValue{V: "Alice"}}, Weight: 1},
	}

	op := &HashJoinOp{
		StreamKeyExpr: &ast.QualifiedRef{Qualifier: "e", Name: "user_id"},
		TableKeyExpr:  &ast.QualifiedRef{Qualifier: "u", Name: "id"},
		LeftJoin:      true,
		StreamAlias:   "e",
		TableAlias:    "u",
	}
	if err := op.BuildIndex(tableRecords); err != nil {
		t.Fatal(err)
	}

	// Match
	results := op.Probe(Record{Columns: map[string]Value{"user_id": IntValue{V: 1}}, Weight: 1})
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if v := results[0].Columns["u.name"]; v.String() != "Alice" {
		t.Errorf("expected Alice, got %v", v)
	}

	// No match — LEFT JOIN should emit NULLs
	results = op.Probe(Record{Columns: map[string]Value{"user_id": IntValue{V: 99}}, Weight: 1})
	if len(results) != 1 {
		t.Fatalf("expected 1 result for LEFT JOIN, got %d", len(results))
	}
	if v := results[0].Columns["u.name"]; !v.IsNull() {
		t.Errorf("expected NULL for unmatched table column, got %v", v)
	}
	if v := results[0].Columns["e.user_id"]; v == nil || v.String() != "99" {
		t.Errorf("expected stream column preserved, got %v", v)
	}
}

func TestHashJoinOp_MultipleMatches(t *testing.T) {
	tableRecords := []Record{
		{Columns: map[string]Value{"id": IntValue{V: 1}, "item": TextValue{V: "A"}}, Weight: 1},
		{Columns: map[string]Value{"id": IntValue{V: 1}, "item": TextValue{V: "B"}}, Weight: 1},
	}

	op := &HashJoinOp{
		StreamKeyExpr: &ast.ColumnRef{Name: "user_id"},
		TableKeyExpr:  &ast.ColumnRef{Name: "id"},
		LeftJoin:      false,
		StreamAlias:   "",
		TableAlias:    "",
	}
	if err := op.BuildIndex(tableRecords); err != nil {
		t.Fatal(err)
	}

	results := op.Probe(Record{Columns: map[string]Value{"user_id": IntValue{V: 1}}, Weight: 1})
	if len(results) != 2 {
		t.Fatalf("expected 2 results for key with 2 table matches, got %d", len(results))
	}
}

func TestHashJoinOp_NullKey(t *testing.T) {
	tableRecords := []Record{
		{Columns: map[string]Value{"id": IntValue{V: 1}, "name": TextValue{V: "Alice"}}, Weight: 1},
	}

	op := &HashJoinOp{
		StreamKeyExpr: &ast.ColumnRef{Name: "user_id"},
		TableKeyExpr:  &ast.ColumnRef{Name: "id"},
		LeftJoin:      false,
	}
	if err := op.BuildIndex(tableRecords); err != nil {
		t.Fatal(err)
	}

	// NULL key should not match anything
	results := op.Probe(Record{Columns: map[string]Value{"user_id": NullValue{}}, Weight: 1})
	if len(results) != 0 {
		t.Fatalf("expected 0 results for NULL key, got %d", len(results))
	}
}
