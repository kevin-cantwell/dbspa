package engine

import (
	"testing"

	"github.com/kevin-cantwell/dbspa/internal/sql/ast"
)

func TestExtractReferencedColumns_Basic(t *testing.T) {
	// SELECT c.tier, c.region, COUNT(*), SUM(total::float)
	// FROM stdin e JOIN '...' c ON e.customer_id = c.id
	// GROUP BY c.tier, c.region
	stmt := &ast.SelectStatement{
		Columns: []ast.Column{
			{Expr: &ast.QualifiedRef{Qualifier: "c", Name: "tier"}},
			{Expr: &ast.QualifiedRef{Qualifier: "c", Name: "region"}},
			{Expr: &ast.FunctionCall{Name: "COUNT", Args: []ast.Expr{&ast.StarExpr{}}}},
			{Expr: &ast.FunctionCall{Name: "SUM", Args: []ast.Expr{
				&ast.CastExpr{Expr: &ast.ColumnRef{Name: "total"}, TypeName: "FLOAT"},
			}}},
		},
		GroupBy: []ast.Expr{
			&ast.QualifiedRef{Qualifier: "c", Name: "tier"},
			&ast.QualifiedRef{Qualifier: "c", Name: "region"},
		},
		Join: &ast.JoinClause{
			Condition: &ast.BinaryExpr{
				Left:  &ast.QualifiedRef{Qualifier: "e", Name: "customer_id"},
				Op:    "=",
				Right: &ast.QualifiedRef{Qualifier: "c", Name: "id"},
			},
		},
	}

	refs := ExtractReferencedColumns(stmt)
	if refs == nil {
		t.Fatal("expected non-nil refs")
	}

	expected := map[string]bool{
		"c.tier":        true,
		"tier":          true,
		"c.region":      true,
		"region":        true,
		"total":         true,
		"e.customer_id": true,
		"customer_id":   true,
		"c.id":          true,
		"id":            true,
	}

	for col := range expected {
		if !refs[col] {
			t.Errorf("expected column %q to be referenced", col)
		}
	}

	// Should NOT contain columns not in the query
	if refs["name"] {
		t.Error("did not expect 'name' to be referenced")
	}
}

func TestExtractReferencedColumns_StarExpr(t *testing.T) {
	// SELECT * should return nil (no projection pushdown)
	stmt := &ast.SelectStatement{
		Columns: []ast.Column{
			{Expr: &ast.StarExpr{}},
		},
	}

	refs := ExtractReferencedColumns(stmt)
	if refs != nil {
		t.Fatalf("expected nil for SELECT *, got %v", refs)
	}
}

func TestExtractReferencedColumns_CaseExpression(t *testing.T) {
	// SELECT SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END)
	stmt := &ast.SelectStatement{
		Columns: []ast.Column{
			{Expr: &ast.FunctionCall{
				Name: "SUM",
				Args: []ast.Expr{
					&ast.CaseExpr{
						Whens: []ast.CaseWhen{
							{
								Condition: &ast.BinaryExpr{
									Left:  &ast.ColumnRef{Name: "status"},
									Op:    "=",
									Right: &ast.StringLiteral{Value: "delivered"},
								},
								Result: &ast.NumberLiteral{Value: "1"},
							},
						},
						Else: &ast.NumberLiteral{Value: "0"},
					},
				},
			}},
		},
	}

	refs := ExtractReferencedColumns(stmt)
	if refs == nil {
		t.Fatal("expected non-nil refs")
	}
	if !refs["status"] {
		t.Error("expected 'status' to be referenced from CASE expression")
	}
}

func TestExtractReferencedColumns_Where(t *testing.T) {
	// SELECT a FROM ... WHERE b > 10 AND c IN ('x', 'y')
	stmt := &ast.SelectStatement{
		Columns: []ast.Column{
			{Expr: &ast.ColumnRef{Name: "a"}},
		},
		Where: &ast.BinaryExpr{
			Left: &ast.BinaryExpr{
				Left:  &ast.ColumnRef{Name: "b"},
				Op:    ">",
				Right: &ast.NumberLiteral{Value: "10"},
			},
			Op: "AND",
			Right: &ast.InExpr{
				Expr: &ast.ColumnRef{Name: "c"},
				Values: []ast.Expr{
					&ast.StringLiteral{Value: "x"},
					&ast.StringLiteral{Value: "y"},
				},
			},
		},
	}

	refs := ExtractReferencedColumns(stmt)
	if refs == nil {
		t.Fatal("expected non-nil refs")
	}
	for _, col := range []string{"a", "b", "c"} {
		if !refs[col] {
			t.Errorf("expected column %q to be referenced", col)
		}
	}
}

func TestExtractReferencedColumns_OrderByAndHaving(t *testing.T) {
	// SELECT x GROUP BY x HAVING COUNT(*) > 10 ORDER BY y
	stmt := &ast.SelectStatement{
		Columns: []ast.Column{
			{Expr: &ast.ColumnRef{Name: "x"}},
		},
		GroupBy: []ast.Expr{
			&ast.ColumnRef{Name: "x"},
		},
		Having: &ast.BinaryExpr{
			Left:  &ast.FunctionCall{Name: "COUNT", Args: []ast.Expr{&ast.StarExpr{}}},
			Op:    ">",
			Right: &ast.NumberLiteral{Value: "10"},
		},
		OrderBy: []ast.OrderByItem{
			{Expr: &ast.ColumnRef{Name: "y"}},
		},
	}

	refs := ExtractReferencedColumns(stmt)
	if refs == nil {
		t.Fatal("expected non-nil refs")
	}
	if !refs["x"] {
		t.Error("expected 'x' to be referenced")
	}
	if !refs["y"] {
		t.Error("expected 'y' to be referenced from ORDER BY")
	}
}
