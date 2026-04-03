package engine

import (
	"github.com/kevin-cantwell/dbspa/internal/sql/ast"
)

// ExtractReferencedColumns walks a SelectStatement and returns all column
// names that are referenced in SELECT, WHERE, GROUP BY, HAVING, ORDER BY.
// Returns both qualified ("c.tier") and unqualified ("tier") forms.
// Returns nil if SELECT * is used (meaning all columns are needed).
func ExtractReferencedColumns(stmt *ast.SelectStatement) map[string]bool {
	// Check for SELECT * — means we need all columns, no projection pushdown.
	for _, col := range stmt.Columns {
		if _, ok := col.Expr.(*ast.StarExpr); ok {
			return nil
		}
	}

	refs := make(map[string]bool)

	// Walk SELECT list expressions
	for _, col := range stmt.Columns {
		walkExpr(col.Expr, refs)
	}

	// Walk WHERE
	if stmt.Where != nil {
		walkExpr(stmt.Where, refs)
	}

	// Walk GROUP BY
	for _, expr := range stmt.GroupBy {
		walkExpr(expr, refs)
	}

	// Walk HAVING
	if stmt.Having != nil {
		walkExpr(stmt.Having, refs)
	}

	// Walk ORDER BY
	for _, item := range stmt.OrderBy {
		walkExpr(item.Expr, refs)
	}

	// Walk JOIN condition — join keys must always be included
	if stmt.Join != nil && stmt.Join.Condition != nil {
		walkExpr(stmt.Join.Condition, refs)
	}

	// Walk EVENT TIME BY
	if stmt.EventTime != nil {
		walkExpr(stmt.EventTime.Expr, refs)
	}

	// Walk DEDUPLICATE BY
	if stmt.Deduplicate != nil {
		walkExpr(stmt.Deduplicate.Key, refs)
	}

	return refs
}

// walkExpr recursively walks an AST expression and collects all column references.
func walkExpr(expr ast.Expr, refs map[string]bool) {
	if expr == nil {
		return
	}

	switch e := expr.(type) {
	case *ast.ColumnRef:
		refs[e.Name] = true

	case *ast.QualifiedRef:
		refs[e.QualifiedName()] = true
		refs[e.Name] = true

	case *ast.BinaryExpr:
		walkExpr(e.Left, refs)
		walkExpr(e.Right, refs)

	case *ast.UnaryExpr:
		walkExpr(e.Expr, refs)

	case *ast.FunctionCall:
		for _, arg := range e.Args {
			walkExpr(arg, refs)
		}

	case *ast.CastExpr:
		walkExpr(e.Expr, refs)

	case *ast.CaseExpr:
		for _, when := range e.Whens {
			walkExpr(when.Condition, refs)
			walkExpr(when.Result, refs)
		}
		if e.Else != nil {
			walkExpr(e.Else, refs)
		}

	case *ast.InExpr:
		walkExpr(e.Expr, refs)
		for _, v := range e.Values {
			walkExpr(v, refs)
		}

	case *ast.BetweenExpr:
		walkExpr(e.Expr, refs)
		walkExpr(e.Low, refs)
		walkExpr(e.High, refs)

	case *ast.IsExpr:
		walkExpr(e.Expr, refs)
		if e.From != nil {
			walkExpr(e.From, refs)
		}

	case *ast.JsonAccessExpr:
		walkExpr(e.Left, refs)
		walkExpr(e.Key, refs)

	case *ast.StarExpr:
		// Handled at the top level in ExtractReferencedColumns

	case *ast.NumberLiteral, *ast.StringLiteral, *ast.BoolLiteral, *ast.NullLiteral, *ast.IntervalLiteral:
		// Literals have no column references
	}
}
