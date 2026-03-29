package engine

import (
	"fmt"
	"maps"

	"github.com/kevin-cantwell/folddb/internal/sql/ast"
)

// Project evaluates the SELECT list expressions against a record, producing
// a new record with only the projected columns.
func Project(columns []ast.Column, rec Record) (Record, error) {
	out := Record{
		Columns:   make(map[string]Value, len(columns)),
		Timestamp: rec.Timestamp,
		Diff:      rec.Diff,
	}

	for i, col := range columns {
		// Handle SELECT *
		if _, ok := col.Expr.(*ast.StarExpr); ok {
			maps.Copy(out.Columns, rec.Columns)
			continue
		}

		val, err := Eval(col.Expr, rec)
		if err != nil {
			return Record{}, fmt.Errorf("projection error on column %d: %w", i+1, err)
		}

		name := col.Alias
		if name == "" {
			name = inferColumnName(col.Expr)
		}
		if name == "" {
			name = fmt.Sprintf("col%d", i+1)
		}

		out.Columns[name] = val
	}

	return out, nil
}

// inferColumnName attempts to derive a meaningful name from an expression.
func inferColumnName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.ColumnRef:
		return e.Name
	case *ast.FunctionCall:
		return e.Name
	case *ast.JsonAccessExpr:
		if key, ok := e.Key.(*ast.StringLiteral); ok {
			return key.Value
		}
	case *ast.CastExpr:
		return inferColumnName(e.Expr)
	}
	return ""
}
