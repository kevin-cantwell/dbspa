package engine

import (
	"testing"
	"time"

	"github.com/kevin-cantwell/folddb/internal/sql/ast"
)

func TestFilterSimplePredicate(t *testing.T) {
	rec := Record{
		Columns:   map[string]Value{"age": IntValue{V: 30}},
		Timestamp: time.Now(),
		Diff:      1,
	}
	where := &ast.BinaryExpr{
		Left:  &ast.ColumnRef{Name: "age"},
		Op:    ">",
		Right: &ast.NumberLiteral{Value: "25"},
	}
	pass, err := Filter(where, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !pass {
		t.Error("expected record to pass filter")
	}
}

func TestFilterSimplePredicateFails(t *testing.T) {
	rec := Record{
		Columns:   map[string]Value{"age": IntValue{V: 20}},
		Timestamp: time.Now(),
		Diff:      1,
	}
	where := &ast.BinaryExpr{
		Left:  &ast.ColumnRef{Name: "age"},
		Op:    ">",
		Right: &ast.NumberLiteral{Value: "25"},
	}
	pass, err := Filter(where, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if pass {
		t.Error("expected record to fail filter")
	}
}

func TestFilterAnd(t *testing.T) {
	rec := Record{
		Columns:   map[string]Value{"a": IntValue{V: 5}, "b": IntValue{V: 10}},
		Timestamp: time.Now(),
		Diff:      1,
	}
	where := &ast.BinaryExpr{
		Left: &ast.BinaryExpr{
			Left:  &ast.ColumnRef{Name: "a"},
			Op:    ">",
			Right: &ast.NumberLiteral{Value: "3"},
		},
		Op: "AND",
		Right: &ast.BinaryExpr{
			Left:  &ast.ColumnRef{Name: "b"},
			Op:    "<",
			Right: &ast.NumberLiteral{Value: "20"},
		},
	}
	pass, err := Filter(where, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !pass {
		t.Error("expected AND condition to pass")
	}
}

func TestFilterOr(t *testing.T) {
	rec := Record{
		Columns:   map[string]Value{"a": IntValue{V: 1}, "b": IntValue{V: 10}},
		Timestamp: time.Now(),
		Diff:      1,
	}
	where := &ast.BinaryExpr{
		Left: &ast.BinaryExpr{
			Left:  &ast.ColumnRef{Name: "a"},
			Op:    ">",
			Right: &ast.NumberLiteral{Value: "3"},
		},
		Op: "OR",
		Right: &ast.BinaryExpr{
			Left:  &ast.ColumnRef{Name: "b"},
			Op:    ">",
			Right: &ast.NumberLiteral{Value: "5"},
		},
	}
	pass, err := Filter(where, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !pass {
		t.Error("expected OR condition to pass (second clause true)")
	}
}

func TestFilterNot(t *testing.T) {
	rec := Record{
		Columns:   map[string]Value{"x": BoolValue{V: false}},
		Timestamp: time.Now(),
		Diff:      1,
	}
	where := &ast.UnaryExpr{
		Op:   "NOT",
		Expr: &ast.ColumnRef{Name: "x"},
	}
	pass, err := Filter(where, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !pass {
		t.Error("expected NOT false to pass")
	}
}

func TestFilterNullFiltersOut(t *testing.T) {
	// NULL in WHERE should be treated as not passing (three-valued logic)
	rec := Record{
		Columns:   map[string]Value{"a": NullValue{}},
		Timestamp: time.Now(),
		Diff:      1,
	}
	where := &ast.BinaryExpr{
		Left:  &ast.ColumnRef{Name: "a"},
		Op:    "=",
		Right: &ast.NumberLiteral{Value: "5"},
	}
	pass, err := Filter(where, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if pass {
		t.Error("NULL = 5 should not pass filter (NULL is not truthy)")
	}
}

func TestFilterNilWherePassesAll(t *testing.T) {
	rec := Record{
		Columns:   map[string]Value{"x": IntValue{V: 1}},
		Timestamp: time.Now(),
		Diff:      1,
	}
	pass, err := Filter(nil, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !pass {
		t.Error("nil WHERE should pass all records")
	}
}

func TestFilterNullAndTrue(t *testing.T) {
	// NULL AND TRUE = NULL, which should not pass
	rec := Record{
		Columns:   map[string]Value{"a": NullValue{}, "b": BoolValue{V: true}},
		Timestamp: time.Now(),
		Diff:      1,
	}
	where := &ast.BinaryExpr{
		Left:  &ast.ColumnRef{Name: "a"},
		Op:    "AND",
		Right: &ast.ColumnRef{Name: "b"},
	}
	pass, err := Filter(where, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if pass {
		t.Error("NULL AND TRUE should not pass filter")
	}
}

func TestFilterNullOrTrue(t *testing.T) {
	// NULL OR TRUE = TRUE, should pass
	rec := Record{
		Columns:   map[string]Value{"a": NullValue{}, "b": BoolValue{V: true}},
		Timestamp: time.Now(),
		Diff:      1,
	}
	where := &ast.BinaryExpr{
		Left:  &ast.ColumnRef{Name: "a"},
		Op:    "OR",
		Right: &ast.ColumnRef{Name: "b"},
	}
	pass, err := Filter(where, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !pass {
		t.Error("NULL OR TRUE should pass filter")
	}
}
