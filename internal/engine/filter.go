package engine

import (
	"fmt"

	"github.com/kevin-cantwell/folddb/internal/sql/ast"
)

// Filter evaluates a WHERE clause expression against a record.
// Returns true if the record passes the filter (or if there is no filter).
func Filter(where ast.Expr, rec Record) (bool, error) {
	if where == nil {
		return true, nil
	}

	val, err := Eval(where, rec)
	if err != nil {
		return false, fmt.Errorf("filter evaluation error: %w", err)
	}

	// NULL and FALSE both fail the filter (three-valued logic)
	if val.IsNull() {
		return false, nil
	}
	b, ok := val.(BoolValue)
	if !ok {
		return false, fmt.Errorf("WHERE clause must evaluate to BOOL, got %s", val.Type())
	}
	return b.V, nil
}
