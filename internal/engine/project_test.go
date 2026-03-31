package engine

import (
	"testing"
	"time"

	"github.com/kevin-cantwell/folddb/internal/sql/ast"
)

func TestProjectSpecificColumns(t *testing.T) {
	rec := Record{
		Columns: map[string]Value{
			"name": TextValue{V: "alice"},
			"age":  IntValue{V: 30},
			"city": TextValue{V: "NYC"},
		},
		Timestamp: time.Now(),
		Weight:      1,
	}
	cols := []ast.Column{
		{Expr: &ast.ColumnRef{Name: "name"}},
		{Expr: &ast.ColumnRef{Name: "age"}},
	}
	out, err := Project(cols, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(out.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(out.Columns))
	}
	if out.Columns["name"].(TextValue).V != "alice" {
		t.Errorf("name: got %v", out.Columns["name"])
	}
	if out.Columns["age"].(IntValue).V != 30 {
		t.Errorf("age: got %v", out.Columns["age"])
	}
	if _, ok := out.Columns["city"]; ok {
		t.Error("city should not be in output")
	}
}

func TestProjectWithAlias(t *testing.T) {
	rec := Record{
		Columns:   map[string]Value{"name": TextValue{V: "alice"}},
		Timestamp: time.Now(),
		Weight:      1,
	}
	cols := []ast.Column{
		{Expr: &ast.ColumnRef{Name: "name"}, Alias: "username"},
	}
	out, err := Project(cols, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if _, ok := out.Columns["username"]; !ok {
		t.Error("expected 'username' column in output")
	}
	if out.Columns["username"].(TextValue).V != "alice" {
		t.Errorf("got %v", out.Columns["username"])
	}
}

func TestProjectStar(t *testing.T) {
	rec := Record{
		Columns: map[string]Value{
			"a": IntValue{V: 1},
			"b": IntValue{V: 2},
			"c": IntValue{V: 3},
		},
		Timestamp: time.Now(),
		Weight:      1,
	}
	cols := []ast.Column{
		{Expr: &ast.StarExpr{}},
	}
	out, err := Project(cols, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(out.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(out.Columns))
	}
	for _, k := range []string{"a", "b", "c"} {
		if _, ok := out.Columns[k]; !ok {
			t.Errorf("expected column %q in output", k)
		}
	}
}

func TestProjectWithExpression(t *testing.T) {
	rec := Record{
		Columns:   map[string]Value{"a": IntValue{V: 10}, "b": IntValue{V: 3}},
		Timestamp: time.Now(),
		Weight:      1,
	}
	cols := []ast.Column{
		{
			Expr: &ast.BinaryExpr{
				Left:  &ast.ColumnRef{Name: "a"},
				Op:    "+",
				Right: &ast.ColumnRef{Name: "b"},
			},
			Alias: "sum",
		},
	}
	out, err := Project(cols, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	iv := out.Columns["sum"].(IntValue)
	if iv.V != 13 {
		t.Errorf("got %d, want 13", iv.V)
	}
}

func TestProjectJsonAccess(t *testing.T) {
	rec := Record{
		Columns: map[string]Value{
			"obj": JsonValue{V: map[string]any{"email": "a@b.com"}},
		},
		Timestamp: time.Now(),
		Weight:      1,
	}
	cols := []ast.Column{
		{
			Expr: &ast.JsonAccessExpr{
				Left:   &ast.ColumnRef{Name: "obj"},
				Key:    &ast.StringLiteral{Value: "email"},
				AsText: true,
			},
			Alias: "email",
		},
	}
	out, err := Project(cols, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	tv := out.Columns["email"].(TextValue)
	if tv.V != "a@b.com" {
		t.Errorf("got %q, want %q", tv.V, "a@b.com")
	}
}

func TestProjectColumnNameInference(t *testing.T) {
	rec := Record{
		Columns:   map[string]Value{"name": TextValue{V: "alice"}},
		Timestamp: time.Now(),
		Weight:      1,
	}
	// No alias - should infer from ColumnRef
	cols := []ast.Column{
		{Expr: &ast.ColumnRef{Name: "name"}},
	}
	out, err := Project(cols, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if _, ok := out.Columns["name"]; !ok {
		t.Error("expected inferred column name 'name'")
	}
}

func TestProjectFunctionNameInference(t *testing.T) {
	rec := Record{
		Columns:   map[string]Value{"x": TextValue{V: "hello"}},
		Timestamp: time.Now(),
		Weight:      1,
	}
	cols := []ast.Column{
		{Expr: &ast.FunctionCall{Name: "LENGTH", Args: []ast.Expr{&ast.ColumnRef{Name: "x"}}}},
	}
	out, err := Project(cols, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if _, ok := out.Columns["LENGTH"]; !ok {
		t.Errorf("expected inferred name 'LENGTH', got columns: %v", keys(out.Columns))
	}
}

func TestProjectJsonAccessNameInference(t *testing.T) {
	rec := Record{
		Columns: map[string]Value{
			"obj": JsonValue{V: map[string]any{"email": "a@b.com"}},
		},
		Timestamp: time.Now(),
		Weight:      1,
	}
	// No alias - should infer "email" from the JSON key
	cols := []ast.Column{
		{Expr: &ast.JsonAccessExpr{
			Left:   &ast.ColumnRef{Name: "obj"},
			Key:    &ast.StringLiteral{Value: "email"},
			AsText: true,
		}},
	}
	out, err := Project(cols, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if _, ok := out.Columns["email"]; !ok {
		t.Errorf("expected inferred name 'email', got columns: %v", keys(out.Columns))
	}
}

func TestProjectPreservesTimestampAndDiff(t *testing.T) {
	ts := time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC)
	rec := Record{
		Columns:   map[string]Value{"x": IntValue{V: 1}},
		Timestamp: ts,
		Weight:      -1,
	}
	cols := []ast.Column{{Expr: &ast.ColumnRef{Name: "x"}}}
	out, err := Project(cols, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !out.Timestamp.Equal(ts) {
		t.Errorf("timestamp not preserved")
	}
	if out.Weight != -1 {
		t.Errorf("diff not preserved: got %d, want -1", out.Weight)
	}
}

func keys(m map[string]Value) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}
