package parser

import (
	"testing"

	"github.com/kevin-cantwell/folddb/internal/sql/ast"
)

func TestPositiveSQL(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		check func(t *testing.T, stmt *ast.SelectStatement)
	}{
		// =====================================================================
		// SELECT clause
		// =====================================================================
		{
			name: "select single column",
			sql:  "SELECT name",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if len(stmt.Columns) != 1 {
					t.Fatalf("expected 1 column, got %d", len(stmt.Columns))
				}
				ref, ok := stmt.Columns[0].Expr.(*ast.ColumnRef)
				if !ok {
					t.Fatalf("expected ColumnRef, got %T", stmt.Columns[0].Expr)
				}
				if ref.Name != "name" {
					t.Errorf("column: got %q, want %q", ref.Name, "name")
				}
			},
		},
		{
			name: "select multiple columns",
			sql:  "SELECT a, b, c",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if len(stmt.Columns) != 3 {
					t.Errorf("expected 3 columns, got %d", len(stmt.Columns))
				}
			},
		},
		{
			name: "select with alias AS",
			sql:  "SELECT name AS username, age AS years",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if len(stmt.Columns) != 2 {
					t.Fatalf("expected 2 columns, got %d", len(stmt.Columns))
				}
				if stmt.Columns[0].Alias != "username" {
					t.Errorf("alias 0: got %q, want %q", stmt.Columns[0].Alias, "username")
				}
				if stmt.Columns[1].Alias != "years" {
					t.Errorf("alias 1: got %q, want %q", stmt.Columns[1].Alias, "years")
				}
			},
		},
		{
			name: "select with implicit alias",
			sql:  "SELECT name username",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.Columns[0].Alias != "username" {
					t.Errorf("alias: got %q, want %q", stmt.Columns[0].Alias, "username")
				}
			},
		},
		{
			name: "select star",
			sql:  "SELECT *",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if len(stmt.Columns) != 1 {
					t.Fatalf("expected 1 column, got %d", len(stmt.Columns))
				}
				if _, ok := stmt.Columns[0].Expr.(*ast.StarExpr); !ok {
					t.Errorf("expected StarExpr, got %T", stmt.Columns[0].Expr)
				}
			},
		},
		{
			name: "select distinct",
			sql:  "SELECT DISTINCT region",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if !stmt.Distinct {
					t.Error("expected Distinct=true")
				}
				if len(stmt.Columns) != 1 {
					t.Errorf("expected 1 column, got %d", len(stmt.Columns))
				}
			},
		},
		{
			name: "select with addition",
			sql:  "SELECT a + b AS total",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				bin, ok := stmt.Columns[0].Expr.(*ast.BinaryExpr)
				if !ok {
					t.Fatalf("expected BinaryExpr, got %T", stmt.Columns[0].Expr)
				}
				if bin.Op != "+" {
					t.Errorf("op: got %q, want %q", bin.Op, "+")
				}
			},
		},
		{
			name: "select with multiplication",
			sql:  "SELECT price * 2 AS doubled",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				bin, ok := stmt.Columns[0].Expr.(*ast.BinaryExpr)
				if !ok {
					t.Fatalf("expected BinaryExpr, got %T", stmt.Columns[0].Expr)
				}
				if bin.Op != "*" {
					t.Errorf("op: got %q, want %q", bin.Op, "*")
				}
			},
		},
		{
			name: "select with string concat",
			sql:  "SELECT first || ' ' || last AS full_name",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				bin, ok := stmt.Columns[0].Expr.(*ast.BinaryExpr)
				if !ok {
					t.Fatalf("expected BinaryExpr, got %T", stmt.Columns[0].Expr)
				}
				if bin.Op != "||" {
					t.Errorf("top op: got %q, want %q", bin.Op, "||")
				}
			},
		},
		{
			name: "select COUNT(*)",
			sql:  "SELECT COUNT(*)",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				fc, ok := stmt.Columns[0].Expr.(*ast.FunctionCall)
				if !ok {
					t.Fatalf("expected FunctionCall, got %T", stmt.Columns[0].Expr)
				}
				if fc.Name != "COUNT" {
					t.Errorf("func: got %q, want %q", fc.Name, "COUNT")
				}
				if len(fc.Args) != 1 {
					t.Fatalf("expected 1 arg, got %d", len(fc.Args))
				}
				if _, ok := fc.Args[0].(*ast.StarExpr); !ok {
					t.Errorf("expected StarExpr arg, got %T", fc.Args[0])
				}
			},
		},
		{
			name: "select SUM",
			sql:  "SELECT SUM(amount) AS total",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				fc, ok := stmt.Columns[0].Expr.(*ast.FunctionCall)
				if !ok {
					t.Fatalf("expected FunctionCall, got %T", stmt.Columns[0].Expr)
				}
				if fc.Name != "SUM" {
					t.Errorf("func: got %q, want %q", fc.Name, "SUM")
				}
			},
		},
		{
			name: "select AVG",
			sql:  "SELECT AVG(score)",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				fc, ok := stmt.Columns[0].Expr.(*ast.FunctionCall)
				if !ok {
					t.Fatalf("expected FunctionCall, got %T", stmt.Columns[0].Expr)
				}
				if fc.Name != "AVG" {
					t.Errorf("func: got %q, want %q", fc.Name, "AVG")
				}
			},
		},
		{
			name: "select MIN and MAX",
			sql:  "SELECT MIN(price), MAX(price)",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if len(stmt.Columns) != 2 {
					t.Fatalf("expected 2 columns, got %d", len(stmt.Columns))
				}
				fc0 := stmt.Columns[0].Expr.(*ast.FunctionCall)
				fc1 := stmt.Columns[1].Expr.(*ast.FunctionCall)
				if fc0.Name != "MIN" {
					t.Errorf("func 0: got %q, want MIN", fc0.Name)
				}
				if fc1.Name != "MAX" {
					t.Errorf("func 1: got %q, want MAX", fc1.Name)
				}
			},
		},
		{
			name: "select MEDIAN",
			sql:  "SELECT MEDIAN(latency)",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				fc, ok := stmt.Columns[0].Expr.(*ast.FunctionCall)
				if !ok {
					t.Fatalf("expected FunctionCall, got %T", stmt.Columns[0].Expr)
				}
				if fc.Name != "MEDIAN" {
					t.Errorf("func: got %q, want %q", fc.Name, "MEDIAN")
				}
			},
		},
		{
			name: "select FIRST and LAST",
			sql:  "SELECT FIRST(val), LAST(val)",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if len(stmt.Columns) != 2 {
					t.Fatalf("expected 2 columns, got %d", len(stmt.Columns))
				}
				fc0 := stmt.Columns[0].Expr.(*ast.FunctionCall)
				fc1 := stmt.Columns[1].Expr.(*ast.FunctionCall)
				if fc0.Name != "FIRST" {
					t.Errorf("func 0: got %q, want FIRST", fc0.Name)
				}
				if fc1.Name != "LAST" {
					t.Errorf("func 1: got %q, want LAST", fc1.Name)
				}
			},
		},
		{
			name: "select ARRAY_AGG",
			sql:  "SELECT ARRAY_AGG(tag)",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				fc := stmt.Columns[0].Expr.(*ast.FunctionCall)
				if fc.Name != "ARRAY_AGG" {
					t.Errorf("func: got %q, want ARRAY_AGG", fc.Name)
				}
			},
		},
		{
			name: "select APPROX_COUNT_DISTINCT",
			sql:  "SELECT APPROX_COUNT_DISTINCT(user_id)",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				fc := stmt.Columns[0].Expr.(*ast.FunctionCall)
				if fc.Name != "APPROX_COUNT_DISTINCT" {
					t.Errorf("func: got %q, want APPROX_COUNT_DISTINCT", fc.Name)
				}
			},
		},
		{
			name: "select COUNT(DISTINCT x)",
			sql:  "SELECT COUNT(DISTINCT user_id)",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				fc, ok := stmt.Columns[0].Expr.(*ast.FunctionCall)
				if !ok {
					t.Fatalf("expected FunctionCall, got %T", stmt.Columns[0].Expr)
				}
				if !fc.Distinct {
					t.Error("expected Distinct=true")
				}
				if fc.Name != "COUNT" {
					t.Errorf("func: got %q, want COUNT", fc.Name)
				}
			},
		},
		{
			name: "select COALESCE",
			sql:  "SELECT COALESCE(a, b, c)",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				fc, ok := stmt.Columns[0].Expr.(*ast.FunctionCall)
				if !ok {
					t.Fatalf("expected FunctionCall, got %T", stmt.Columns[0].Expr)
				}
				if fc.Name != "COALESCE" || len(fc.Args) != 3 {
					t.Errorf("expected COALESCE(3 args), got %s(%d)", fc.Name, len(fc.Args))
				}
			},
		},
		{
			name: "select NULLIF",
			sql:  "SELECT NULLIF(a, b)",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				fc := stmt.Columns[0].Expr.(*ast.FunctionCall)
				if fc.Name != "NULLIF" || len(fc.Args) != 2 {
					t.Errorf("expected NULLIF(2 args), got %s(%d)", fc.Name, len(fc.Args))
				}
			},
		},
		{
			name: "select CASE WHEN THEN ELSE END",
			sql:  "SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END AS flag",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				ce, ok := stmt.Columns[0].Expr.(*ast.CaseExpr)
				if !ok {
					t.Fatalf("expected CaseExpr, got %T", stmt.Columns[0].Expr)
				}
				if len(ce.Whens) != 1 {
					t.Errorf("expected 1 WHEN, got %d", len(ce.Whens))
				}
				if ce.Else == nil {
					t.Error("expected ELSE clause")
				}
				if stmt.Columns[0].Alias != "flag" {
					t.Errorf("alias: got %q, want %q", stmt.Columns[0].Alias, "flag")
				}
			},
		},
		{
			name: "select CASE with multiple WHENs",
			sql:  "SELECT CASE WHEN x > 10 THEN 'huge' WHEN x > 5 THEN 'big' WHEN x > 0 THEN 'small' ELSE 'zero' END",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				ce := stmt.Columns[0].Expr.(*ast.CaseExpr)
				if len(ce.Whens) != 3 {
					t.Errorf("expected 3 WHENs, got %d", len(ce.Whens))
				}
			},
		},
		{
			name: "select CASE without ELSE",
			sql:  "SELECT CASE WHEN x > 0 THEN 'positive' END",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				ce := stmt.Columns[0].Expr.(*ast.CaseExpr)
				if ce.Else != nil {
					t.Error("expected no ELSE clause")
				}
			},
		},
		{
			name: "select type cast shorthand",
			sql:  "SELECT val::INT AS num",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				cast, ok := stmt.Columns[0].Expr.(*ast.CastExpr)
				if !ok {
					t.Fatalf("expected CastExpr, got %T", stmt.Columns[0].Expr)
				}
				if cast.TypeName != "INT" {
					t.Errorf("type: got %q, want %q", cast.TypeName, "INT")
				}
			},
		},
		{
			name: "select type cast to float",
			sql:  "SELECT val::FLOAT",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				cast := stmt.Columns[0].Expr.(*ast.CastExpr)
				if cast.TypeName != "FLOAT" {
					t.Errorf("type: got %q, want %q", cast.TypeName, "FLOAT")
				}
			},
		},
		{
			name: "select CAST function syntax",
			sql:  "SELECT CAST(val AS TEXT)",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				cast, ok := stmt.Columns[0].Expr.(*ast.CastExpr)
				if !ok {
					t.Fatalf("expected CastExpr, got %T", stmt.Columns[0].Expr)
				}
				if cast.TypeName != "TEXT" {
					t.Errorf("type: got %q, want %q", cast.TypeName, "TEXT")
				}
			},
		},
		{
			name: "select json access arrow text",
			sql:  "SELECT col->>'field' AS val",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				ja, ok := stmt.Columns[0].Expr.(*ast.JsonAccessExpr)
				if !ok {
					t.Fatalf("expected JsonAccessExpr, got %T", stmt.Columns[0].Expr)
				}
				if !ja.AsText {
					t.Error("expected AsText=true for ->>")
				}
			},
		},
		{
			name: "select json access arrow json",
			sql:  "SELECT col->'nested'",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				ja, ok := stmt.Columns[0].Expr.(*ast.JsonAccessExpr)
				if !ok {
					t.Fatalf("expected JsonAccessExpr, got %T", stmt.Columns[0].Expr)
				}
				if ja.AsText {
					t.Error("expected AsText=false for ->")
				}
			},
		},
		{
			name: "select json access array index",
			sql:  "SELECT col->0",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				ja, ok := stmt.Columns[0].Expr.(*ast.JsonAccessExpr)
				if !ok {
					t.Fatalf("expected JsonAccessExpr, got %T", stmt.Columns[0].Expr)
				}
				_, isNum := ja.Key.(*ast.NumberLiteral)
				if !isNum {
					t.Errorf("expected NumberLiteral key, got %T", ja.Key)
				}
			},
		},
		{
			name: "select chained json access",
			sql:  "SELECT payload->'user'->>'email' AS email",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				outer, ok := stmt.Columns[0].Expr.(*ast.JsonAccessExpr)
				if !ok {
					t.Fatalf("expected outer JsonAccessExpr, got %T", stmt.Columns[0].Expr)
				}
				if !outer.AsText {
					t.Error("expected outer AsText=true")
				}
				inner, ok := outer.Left.(*ast.JsonAccessExpr)
				if !ok {
					t.Fatalf("expected inner JsonAccessExpr, got %T", outer.Left)
				}
				if inner.AsText {
					t.Error("expected inner AsText=false")
				}
			},
		},
		{
			name: "select dot notation two level",
			sql:  "SELECT data.name",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				ref, ok := stmt.Columns[0].Expr.(*ast.QualifiedRef)
				if !ok {
					t.Fatalf("expected QualifiedRef, got %T", stmt.Columns[0].Expr)
				}
				if ref.Qualifier != "data" || ref.Name != "name" {
					t.Errorf("got %s.%s, want data.name", ref.Qualifier, ref.Name)
				}
			},
		},
		{
			name: "select dot notation three level",
			sql:  "SELECT user.address.city",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				// user.address.city => JsonAccessExpr(QualifiedRef(user, address), "city", AsText=true)
				ja, ok := stmt.Columns[0].Expr.(*ast.JsonAccessExpr)
				if !ok {
					t.Fatalf("expected JsonAccessExpr, got %T", stmt.Columns[0].Expr)
				}
				if !ja.AsText {
					t.Error("expected AsText=true for final segment")
				}
				qr, ok := ja.Left.(*ast.QualifiedRef)
				if !ok {
					t.Fatalf("expected QualifiedRef inside, got %T", ja.Left)
				}
				if qr.Qualifier != "user" || qr.Name != "address" {
					t.Errorf("got %s.%s, want user.address", qr.Qualifier, qr.Name)
				}
			},
		},
		{
			name: "select nested expressions with parentheses",
			sql:  "SELECT (a + b) * c",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				bin, ok := stmt.Columns[0].Expr.(*ast.BinaryExpr)
				if !ok {
					t.Fatalf("expected BinaryExpr, got %T", stmt.Columns[0].Expr)
				}
				if bin.Op != "*" {
					t.Errorf("top op: got %q, want %q", bin.Op, "*")
				}
				left, ok := bin.Left.(*ast.BinaryExpr)
				if !ok {
					t.Fatalf("expected BinaryExpr on left, got %T", bin.Left)
				}
				if left.Op != "+" {
					t.Errorf("left op: got %q, want %q", left.Op, "+")
				}
			},
		},
		{
			name: "select quoted identifier",
			sql:  `SELECT "last" AS surname`,
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				ref, ok := stmt.Columns[0].Expr.(*ast.ColumnRef)
				if !ok {
					t.Fatalf("expected ColumnRef, got %T", stmt.Columns[0].Expr)
				}
				if ref.Name != "last" {
					t.Errorf("column: got %q, want %q", ref.Name, "last")
				}
			},
		},
		{
			name: "select quoted identifier keyword",
			sql:  `SELECT "select"`,
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				ref, ok := stmt.Columns[0].Expr.(*ast.ColumnRef)
				if !ok {
					t.Fatalf("expected ColumnRef, got %T", stmt.Columns[0].Expr)
				}
				if ref.Name != "select" {
					t.Errorf("column: got %q, want %q", ref.Name, "select")
				}
			},
		},
		{
			name: "select unary minus",
			sql:  "SELECT -x",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				ue, ok := stmt.Columns[0].Expr.(*ast.UnaryExpr)
				if !ok {
					t.Fatalf("expected UnaryExpr, got %T", stmt.Columns[0].Expr)
				}
				if ue.Op != "-" {
					t.Errorf("op: got %q, want %q", ue.Op, "-")
				}
			},
		},
		{
			name: "select NULL literal",
			sql:  "SELECT NULL",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if _, ok := stmt.Columns[0].Expr.(*ast.NullLiteral); !ok {
					t.Errorf("expected NullLiteral, got %T", stmt.Columns[0].Expr)
				}
			},
		},
		{
			name: "select boolean literals",
			sql:  "SELECT TRUE, FALSE",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				b1, ok := stmt.Columns[0].Expr.(*ast.BoolLiteral)
				if !ok || !b1.Value {
					t.Error("expected TRUE")
				}
				b2, ok := stmt.Columns[1].Expr.(*ast.BoolLiteral)
				if !ok || b2.Value {
					t.Error("expected FALSE")
				}
			},
		},
		{
			name: "select integer and float literals",
			sql:  "SELECT 42, 3.14",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				n1, ok := stmt.Columns[0].Expr.(*ast.NumberLiteral)
				if !ok {
					t.Fatalf("expected NumberLiteral, got %T", stmt.Columns[0].Expr)
				}
				if n1.Value != "42" || n1.IsFloat {
					t.Errorf("expected integer 42, got %q (float=%v)", n1.Value, n1.IsFloat)
				}
				n2, ok := stmt.Columns[1].Expr.(*ast.NumberLiteral)
				if !ok {
					t.Fatalf("expected NumberLiteral, got %T", stmt.Columns[1].Expr)
				}
				if n2.Value != "3.14" || !n2.IsFloat {
					t.Errorf("expected float 3.14, got %q (float=%v)", n2.Value, n2.IsFloat)
				}
			},
		},
		{
			name: "select string literal",
			sql:  "SELECT 'hello world'",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				sl, ok := stmt.Columns[0].Expr.(*ast.StringLiteral)
				if !ok {
					t.Fatalf("expected StringLiteral, got %T", stmt.Columns[0].Expr)
				}
				if sl.Value != "hello world" {
					t.Errorf("value: got %q, want %q", sl.Value, "hello world")
				}
			},
		},
		{
			name: "select NOW function",
			sql:  "SELECT NOW()",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				fc := stmt.Columns[0].Expr.(*ast.FunctionCall)
				if fc.Name != "NOW" {
					t.Errorf("func: got %q, want NOW", fc.Name)
				}
				if len(fc.Args) != 0 {
					t.Errorf("expected 0 args, got %d", len(fc.Args))
				}
			},
		},
		{
			name: "select LENGTH function",
			sql:  "SELECT LENGTH(name)",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				fc := stmt.Columns[0].Expr.(*ast.FunctionCall)
				if fc.Name != "LENGTH" {
					t.Errorf("func: got %q, want LENGTH", fc.Name)
				}
			},
		},
		{
			name: "select UPPER and LOWER",
			sql:  "SELECT UPPER(name), LOWER(name)",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				fc0 := stmt.Columns[0].Expr.(*ast.FunctionCall)
				fc1 := stmt.Columns[1].Expr.(*ast.FunctionCall)
				if fc0.Name != "UPPER" {
					t.Errorf("func 0: got %q, want UPPER", fc0.Name)
				}
				if fc1.Name != "LOWER" {
					t.Errorf("func 1: got %q, want LOWER", fc1.Name)
				}
			},
		},
		{
			name: "select TRIM LTRIM RTRIM",
			sql:  "SELECT TRIM(s), LTRIM(s), RTRIM(s)",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if len(stmt.Columns) != 3 {
					t.Fatalf("expected 3 columns, got %d", len(stmt.Columns))
				}
				for i, want := range []string{"TRIM", "LTRIM", "RTRIM"} {
					fc := stmt.Columns[i].Expr.(*ast.FunctionCall)
					if fc.Name != want {
						t.Errorf("func %d: got %q, want %q", i, fc.Name, want)
					}
				}
			},
		},
		{
			name: "select SUBSTR",
			sql:  "SELECT SUBSTR(name, 1, 3)",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				fc := stmt.Columns[0].Expr.(*ast.FunctionCall)
				if fc.Name != "SUBSTR" || len(fc.Args) != 3 {
					t.Errorf("expected SUBSTR(3 args), got %s(%d)", fc.Name, len(fc.Args))
				}
			},
		},
		{
			name: "select REPLACE",
			sql:  "SELECT REPLACE(name, 'old', 'new')",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				fc := stmt.Columns[0].Expr.(*ast.FunctionCall)
				if fc.Name != "REPLACE" || len(fc.Args) != 3 {
					t.Errorf("expected REPLACE(3 args), got %s(%d)", fc.Name, len(fc.Args))
				}
			},
		},
		{
			name: "select SPLIT_PART",
			sql:  "SELECT SPLIT_PART(path, '/', 2)",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				fc := stmt.Columns[0].Expr.(*ast.FunctionCall)
				if fc.Name != "SPLIT_PART" || len(fc.Args) != 3 {
					t.Errorf("expected SPLIT_PART(3 args), got %s(%d)", fc.Name, len(fc.Args))
				}
			},
		},
		{
			// NOTE: Standard SQL EXTRACT(field FROM expr) is not supported by
			// the parser because FROM is a clause keyword. EXTRACT is registered
			// as a callable keyword so it parses as a regular function call.
			name: "select EXTRACT as regular function",
			sql:  "SELECT EXTRACT(ts)",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				fc := stmt.Columns[0].Expr.(*ast.FunctionCall)
				if fc.Name != "EXTRACT" {
					t.Errorf("func: got %q, want EXTRACT", fc.Name)
				}
			},
		},
		{
			name: "select json_keys",
			sql:  "SELECT json_keys(payload)",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				fc := stmt.Columns[0].Expr.(*ast.FunctionCall)
				if fc.Name != "JSON_KEYS" {
					t.Errorf("func: got %q, want JSON_KEYS", fc.Name)
				}
			},
		},
		{
			name: "select modulo operator",
			sql:  "SELECT x % 2",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				bin := stmt.Columns[0].Expr.(*ast.BinaryExpr)
				if bin.Op != "%" {
					t.Errorf("op: got %q, want %%", bin.Op)
				}
			},
		},
		{
			name: "select division operator",
			sql:  "SELECT total / count",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				bin := stmt.Columns[0].Expr.(*ast.BinaryExpr)
				if bin.Op != "/" {
					t.Errorf("op: got %q, want /", bin.Op)
				}
			},
		},

		// =====================================================================
		// FROM clause
		// =====================================================================
		{
			name: "from kafka URI",
			sql:  "SELECT * FROM 'kafka://broker:9092/topic'",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.From == nil {
					t.Fatal("expected FROM clause")
				}
				if stmt.From.URI != "kafka://broker:9092/topic" {
					t.Errorf("URI: got %q, want %q", stmt.From.URI, "kafka://broker:9092/topic")
				}
			},
		},
		{
			name: "from stdin bare identifier",
			sql:  "SELECT * FROM stdin",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.From == nil {
					t.Fatal("expected FROM clause")
				}
				if stmt.From.URI != "stdin://" {
					t.Errorf("URI: got %q, want %q", stmt.From.URI, "stdin://")
				}
			},
		},
		{
			name: "from with alias",
			sql:  "SELECT * FROM stdin e",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.FromAlias != "e" {
					t.Errorf("alias: got %q, want %q", stmt.FromAlias, "e")
				}
			},
		},
		{
			name: "from with FORMAT DEBEZIUM",
			sql:  "SELECT * FROM 'kafka://b/t' FORMAT DEBEZIUM",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.From.Format != "DEBEZIUM" {
					t.Errorf("format: got %q, want %q", stmt.From.Format, "DEBEZIUM")
				}
			},
		},
		{
			name: "from with FORMAT CSV and options",
			sql:  "SELECT * FROM 'kafka://b/t' FORMAT CSV(delimiter='|', header=true)",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.From.Format != "CSV" {
					t.Errorf("format: got %q, want CSV", stmt.From.Format)
				}
				if stmt.From.FormatOptions["delimiter"] != "|" {
					t.Errorf("delimiter: got %q, want %q", stmt.From.FormatOptions["delimiter"], "|")
				}
				if stmt.From.FormatOptions["header"] != "true" {
					t.Errorf("header: got %q, want %q", stmt.From.FormatOptions["header"], "true")
				}
			},
		},
		{
			name: "from with FORMAT AVRO",
			sql:  "SELECT * FROM 'kafka://b/t' FORMAT AVRO",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.From.Format != "AVRO" {
					t.Errorf("format: got %q, want AVRO", stmt.From.Format)
				}
			},
		},
		{
			name: "from with FORMAT PROTOBUF with option",
			sql:  "SELECT * FROM 'kafka://b/t' FORMAT PROTOBUF(message='Order')",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.From.Format != "PROTOBUF" {
					t.Errorf("format: got %q, want PROTOBUF", stmt.From.Format)
				}
				if stmt.From.FormatOptions["message"] != "Order" {
					t.Errorf("message opt: got %q", stmt.From.FormatOptions["message"])
				}
			},
		},
		{
			name: "from with FORMAT PARQUET",
			sql:  "SELECT * FROM 'kafka://b/t' FORMAT PARQUET",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.From.Format != "PARQUET" {
					t.Errorf("format: got %q, want PARQUET", stmt.From.Format)
				}
			},
		},
		{
			name: "from with FORMAT DEBEZIUM_AVRO",
			sql:  "SELECT * FROM 'kafka://b/t' FORMAT DEBEZIUM_AVRO",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.From.Format != "DEBEZIUM_AVRO" {
					t.Errorf("format: got %q, want DEBEZIUM_AVRO", stmt.From.Format)
				}
			},
		},
		{
			name: "from kafka with URI params",
			sql:  "SELECT * FROM 'kafka://broker/topic?offset=earliest&registry=http://reg:8081'",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				want := "kafka://broker/topic?offset=earliest&registry=http://reg:8081"
				if stmt.From.URI != want {
					t.Errorf("URI: got %q, want %q", stmt.From.URI, want)
				}
			},
		},
		{
			name: "from omitted means stdin",
			sql:  "SELECT name",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.From != nil {
					t.Error("expected From to be nil (stdin implied)")
				}
			},
		},
		{
			name: "from stdin with FORMAT DEBEZIUM",
			sql:  "SELECT * FROM stdin FORMAT DEBEZIUM",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.From.URI != "stdin://" {
					t.Errorf("URI: got %q, want stdin://", stmt.From.URI)
				}
				if stmt.From.Format != "DEBEZIUM" {
					t.Errorf("format: got %q, want DEBEZIUM", stmt.From.Format)
				}
			},
		},

		// =====================================================================
		// JOIN clause
		// =====================================================================
		{
			name: "join with file path and alias",
			sql:  "SELECT * FROM stdin e JOIN '/tmp/users.ndjson' u ON e.user_id = u.id",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.Join == nil {
					t.Fatal("expected JOIN clause")
				}
				if stmt.Join.Type != "JOIN" {
					t.Errorf("join type: got %q, want JOIN", stmt.Join.Type)
				}
				if stmt.Join.Source.URI != "/tmp/users.ndjson" {
					t.Errorf("join source: got %q", stmt.Join.Source.URI)
				}
				if stmt.Join.Alias != "u" {
					t.Errorf("join alias: got %q, want u", stmt.Join.Alias)
				}
				if stmt.Join.Condition == nil {
					t.Error("expected ON condition")
				}
			},
		},
		{
			name: "left join",
			sql:  "SELECT * FROM stdin e LEFT JOIN '/tmp/users.csv' u ON e.uid = u.id",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.Join == nil {
					t.Fatal("expected JOIN clause")
				}
				if stmt.Join.Type != "LEFT JOIN" {
					t.Errorf("join type: got %q, want LEFT JOIN", stmt.Join.Type)
				}
			},
		},
		{
			name: "join with FORMAT on join source",
			sql:  "SELECT e.id FROM stdin e JOIN '/tmp/data.csv' FORMAT CSV u ON e.id = u.id",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.Join.Source.Format != "CSV" {
					t.Errorf("join format: got %q, want CSV", stmt.Join.Source.Format)
				}
			},
		},
		{
			name: "join with qualified refs in ON",
			sql:  "SELECT e.user_id, u.name FROM stdin e JOIN '/tmp/users.ndjson' u ON e.user_id = u.id",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				bin, ok := stmt.Join.Condition.(*ast.BinaryExpr)
				if !ok || bin.Op != "=" {
					t.Fatalf("expected = condition")
				}
				left, ok := bin.Left.(*ast.QualifiedRef)
				if !ok {
					t.Fatalf("expected QualifiedRef on left of ON, got %T", bin.Left)
				}
				if left.Qualifier != "e" || left.Name != "user_id" {
					t.Errorf("ON left: got %s.%s", left.Qualifier, left.Name)
				}
			},
		},
		{
			name: "join with WITHIN INTERVAL (stream-stream)",
			sql:  "SELECT o.id, p.id FROM 'kafka://b/orders' o JOIN 'kafka://b/payments' p ON o.order_id = p.order_id WITHIN INTERVAL '10 minutes'",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.Join.Within == nil {
					t.Fatal("expected WITHIN clause")
				}
				if *stmt.Join.Within != "10 minutes" {
					t.Errorf("within: got %q, want %q", *stmt.Join.Within, "10 minutes")
				}
			},
		},
		{
			name: "join without WITHIN (file join)",
			sql:  "SELECT * FROM stdin o JOIN '/tmp/users.ndjson' u ON o.id = u.id",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.Join.Within != nil {
					t.Errorf("expected nil Within for file join, got %q", *stmt.Join.Within)
				}
			},
		},

		// =====================================================================
		// SEED FROM
		// =====================================================================
		{
			name: "seed from with file path",
			sql:  "SELECT region, COUNT(*) FROM 'kafka://b/t' FORMAT DEBEZIUM SEED FROM '/path/to/snapshot.parquet' GROUP BY region",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.Seed == nil {
					t.Fatal("expected SEED clause")
				}
				if stmt.Seed.Source.URI != "/path/to/snapshot.parquet" {
					t.Errorf("seed URI: got %q", stmt.Seed.Source.URI)
				}
			},
		},
		{
			name: "seed from with FORMAT",
			sql:  "SELECT status, COUNT(*) FROM stdin SEED FROM '/tmp/seed.csv' FORMAT CSV GROUP BY status",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.Seed == nil {
					t.Fatal("expected SEED clause")
				}
				if stmt.Seed.Source.Format != "CSV" {
					t.Errorf("seed format: got %q, want CSV", stmt.Seed.Source.Format)
				}
			},
		},

		// =====================================================================
		// WHERE clause
		// =====================================================================
		{
			name: "where simple comparison",
			sql:  "SELECT x WHERE x > 25",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.Where == nil {
					t.Fatal("expected WHERE clause")
				}
				bin, ok := stmt.Where.(*ast.BinaryExpr)
				if !ok {
					t.Fatalf("expected BinaryExpr, got %T", stmt.Where)
				}
				if bin.Op != ">" {
					t.Errorf("op: got %q, want >", bin.Op)
				}
			},
		},
		{
			name: "where equality",
			sql:  "SELECT x WHERE status = 'active'",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				bin := stmt.Where.(*ast.BinaryExpr)
				if bin.Op != "=" {
					t.Errorf("op: got %q, want =", bin.Op)
				}
			},
		},
		{
			name: "where with AND",
			sql:  "SELECT x WHERE a > 1 AND b < 10",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				bin := stmt.Where.(*ast.BinaryExpr)
				if bin.Op != "AND" {
					t.Errorf("op: got %q, want AND", bin.Op)
				}
			},
		},
		{
			name: "where with OR",
			sql:  "SELECT x WHERE a = 1 OR b = 2",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				bin := stmt.Where.(*ast.BinaryExpr)
				if bin.Op != "OR" {
					t.Errorf("op: got %q, want OR", bin.Op)
				}
			},
		},
		{
			name: "where with NOT",
			sql:  "SELECT x WHERE NOT active",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				ue, ok := stmt.Where.(*ast.UnaryExpr)
				if !ok {
					t.Fatalf("expected UnaryExpr, got %T", stmt.Where)
				}
				if ue.Op != "NOT" {
					t.Errorf("op: got %q, want NOT", ue.Op)
				}
			},
		},
		{
			name: "where IN list",
			sql:  "SELECT s WHERE s IN ('a', 'b', 'c')",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				ie, ok := stmt.Where.(*ast.InExpr)
				if !ok {
					t.Fatalf("expected InExpr, got %T", stmt.Where)
				}
				if len(ie.Values) != 3 {
					t.Errorf("expected 3 IN values, got %d", len(ie.Values))
				}
				if ie.Not {
					t.Error("expected Not=false")
				}
			},
		},
		{
			name: "where NOT IN",
			sql:  "SELECT s WHERE s NOT IN (1, 2)",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				ie := stmt.Where.(*ast.InExpr)
				if !ie.Not {
					t.Error("expected Not=true")
				}
			},
		},
		{
			name: "where BETWEEN",
			sql:  "SELECT x WHERE x BETWEEN 10 AND 100",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				be, ok := stmt.Where.(*ast.BetweenExpr)
				if !ok {
					t.Fatalf("expected BetweenExpr, got %T", stmt.Where)
				}
				if be.Not {
					t.Error("expected Not=false")
				}
			},
		},
		{
			name: "where IS NULL",
			sql:  "SELECT x WHERE x IS NULL",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				is := stmt.Where.(*ast.IsExpr)
				if is.What != "NULL" || is.Not {
					t.Errorf("expected IS NULL")
				}
			},
		},
		{
			name: "where IS NOT NULL",
			sql:  "SELECT x WHERE x IS NOT NULL",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				is := stmt.Where.(*ast.IsExpr)
				if is.What != "NULL" || !is.Not {
					t.Errorf("expected IS NOT NULL")
				}
			},
		},
		{
			name: "where IS DISTINCT FROM",
			sql:  "SELECT x WHERE x IS DISTINCT FROM y",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				is := stmt.Where.(*ast.IsExpr)
				if is.What != "DISTINCT FROM" || is.Not {
					t.Error("expected IS DISTINCT FROM")
				}
				if is.From == nil {
					t.Error("expected From expression")
				}
			},
		},
		{
			name: "where IS NOT DISTINCT FROM",
			sql:  "SELECT x WHERE x IS NOT DISTINCT FROM y",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				is := stmt.Where.(*ast.IsExpr)
				if is.What != "DISTINCT FROM" || !is.Not {
					t.Error("expected IS NOT DISTINCT FROM")
				}
			},
		},
		{
			name: "where LIKE",
			sql:  "SELECT n WHERE n LIKE 'A%'",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				bin := stmt.Where.(*ast.BinaryExpr)
				if bin.Op != "LIKE" {
					t.Errorf("op: got %q, want LIKE", bin.Op)
				}
			},
		},
		{
			name: "where ILIKE",
			sql:  "SELECT n WHERE n ILIKE '%alice%'",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				bin := stmt.Where.(*ast.BinaryExpr)
				if bin.Op != "ILIKE" {
					t.Errorf("op: got %q, want ILIKE", bin.Op)
				}
			},
		},
		{
			name: "where NOT LIKE",
			sql:  "SELECT n WHERE n NOT LIKE 'test%'",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				ue, ok := stmt.Where.(*ast.UnaryExpr)
				if !ok {
					t.Fatalf("expected UnaryExpr(NOT), got %T", stmt.Where)
				}
				if ue.Op != "NOT" {
					t.Errorf("op: got %q, want NOT", ue.Op)
				}
				bin := ue.Expr.(*ast.BinaryExpr)
				if bin.Op != "LIKE" {
					t.Errorf("inner op: got %q, want LIKE", bin.Op)
				}
			},
		},
		{
			name: "where with qualified ref",
			sql:  "SELECT e.name FROM stdin e WHERE e.status = 'active'",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				bin := stmt.Where.(*ast.BinaryExpr)
				qr, ok := bin.Left.(*ast.QualifiedRef)
				if !ok {
					t.Fatalf("expected QualifiedRef, got %T", bin.Left)
				}
				if qr.Qualifier != "e" || qr.Name != "status" {
					t.Errorf("got %s.%s, want e.status", qr.Qualifier, qr.Name)
				}
			},
		},
		{
			name: "where with json access",
			sql:  "SELECT x WHERE payload->>'type' = 'click'",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				bin := stmt.Where.(*ast.BinaryExpr)
				if bin.Op != "=" {
					t.Errorf("op: got %q, want =", bin.Op)
				}
				ja, ok := bin.Left.(*ast.JsonAccessExpr)
				if !ok {
					t.Fatalf("expected JsonAccessExpr on left, got %T", bin.Left)
				}
				if !ja.AsText {
					t.Error("expected AsText=true")
				}
			},
		},
		{
			name: "where with nested boolean logic",
			sql:  "SELECT x WHERE (a > 1 AND b < 2) OR c = 3",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				bin := stmt.Where.(*ast.BinaryExpr)
				if bin.Op != "OR" {
					t.Errorf("top op: got %q, want OR", bin.Op)
				}
				left, ok := bin.Left.(*ast.BinaryExpr)
				if !ok {
					t.Fatalf("expected BinaryExpr on left, got %T", bin.Left)
				}
				if left.Op != "AND" {
					t.Errorf("left op: got %q, want AND", left.Op)
				}
			},
		},
		{
			name: "where with comparison operators",
			sql:  "SELECT x WHERE a != 1",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				bin := stmt.Where.(*ast.BinaryExpr)
				if bin.Op != "!=" {
					t.Errorf("op: got %q, want !=", bin.Op)
				}
			},
		},
		{
			name: "where with lte and gte",
			sql:  "SELECT x WHERE a <= 10 AND b >= 20",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				and := stmt.Where.(*ast.BinaryExpr)
				if and.Op != "AND" {
					t.Fatalf("top op: got %q, want AND", and.Op)
				}
				left := and.Left.(*ast.BinaryExpr)
				right := and.Right.(*ast.BinaryExpr)
				if left.Op != "<=" {
					t.Errorf("left op: got %q, want <=", left.Op)
				}
				if right.Op != ">=" {
					t.Errorf("right op: got %q, want >=", right.Op)
				}
			},
		},

		// =====================================================================
		// GROUP BY
		// =====================================================================
		{
			name: "group by single column",
			sql:  "SELECT region, COUNT(*) FROM 'kafka://b/t' GROUP BY region",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if len(stmt.GroupBy) != 1 {
					t.Fatalf("expected 1 GROUP BY, got %d", len(stmt.GroupBy))
				}
				ref, ok := stmt.GroupBy[0].(*ast.ColumnRef)
				if !ok {
					t.Fatalf("expected ColumnRef, got %T", stmt.GroupBy[0])
				}
				if ref.Name != "region" {
					t.Errorf("group key: got %q, want region", ref.Name)
				}
			},
		},
		{
			name: "group by multiple columns",
			sql:  "SELECT a, b, COUNT(*) FROM 'kafka://b/t' GROUP BY a, b",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if len(stmt.GroupBy) != 2 {
					t.Errorf("expected 2 GROUP BY, got %d", len(stmt.GroupBy))
				}
			},
		},
		{
			name: "group by ordinal",
			sql:  "SELECT a, b, COUNT(*) FROM 'kafka://b/t' GROUP BY 1, 2",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if len(stmt.GroupBy) != 2 {
					t.Fatalf("expected 2 GROUP BY, got %d", len(stmt.GroupBy))
				}
				n, ok := stmt.GroupBy[0].(*ast.NumberLiteral)
				if !ok {
					t.Fatalf("expected NumberLiteral, got %T", stmt.GroupBy[0])
				}
				if n.Value != "1" {
					t.Errorf("ordinal: got %q, want 1", n.Value)
				}
			},
		},
		{
			name: "group by expression json access",
			sql:  "SELECT _after->>'region', COUNT(*) FROM 'kafka://b/t' FORMAT DEBEZIUM GROUP BY _after->>'region'",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if len(stmt.GroupBy) != 1 {
					t.Fatalf("expected 1 GROUP BY, got %d", len(stmt.GroupBy))
				}
				ja, ok := stmt.GroupBy[0].(*ast.JsonAccessExpr)
				if !ok {
					t.Fatalf("expected JsonAccessExpr, got %T", stmt.GroupBy[0])
				}
				if !ja.AsText {
					t.Error("expected AsText=true")
				}
			},
		},

		// =====================================================================
		// HAVING
		// =====================================================================
		{
			name: "having with aggregate condition",
			sql:  "SELECT region, COUNT(*) AS cnt FROM 'kafka://b/t' GROUP BY region HAVING COUNT(*) > 100",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.Having == nil {
					t.Fatal("expected HAVING clause")
				}
				bin, ok := stmt.Having.(*ast.BinaryExpr)
				if !ok {
					t.Fatalf("expected BinaryExpr, got %T", stmt.Having)
				}
				if bin.Op != ">" {
					t.Errorf("having op: got %q, want >", bin.Op)
				}
			},
		},
		{
			name: "having with SUM",
			sql:  "SELECT region, SUM(total) FROM 'kafka://b/t' GROUP BY region HAVING SUM(total) >= 1000",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.Having == nil {
					t.Fatal("expected HAVING clause")
				}
			},
		},

		// =====================================================================
		// ORDER BY
		// =====================================================================
		{
			name: "order by single column",
			sql:  "SELECT region, COUNT(*) AS cnt FROM 'kafka://b/t' GROUP BY region ORDER BY cnt",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if len(stmt.OrderBy) != 1 {
					t.Fatalf("expected 1 ORDER BY, got %d", len(stmt.OrderBy))
				}
				if stmt.OrderBy[0].Desc {
					t.Error("expected ASC (default)")
				}
			},
		},
		{
			name: "order by DESC",
			sql:  "SELECT region, COUNT(*) AS cnt FROM 'kafka://b/t' GROUP BY region ORDER BY cnt DESC",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if !stmt.OrderBy[0].Desc {
					t.Error("expected DESC")
				}
			},
		},
		{
			name: "order by ASC explicit",
			sql:  "SELECT region, COUNT(*) AS cnt FROM 'kafka://b/t' GROUP BY region ORDER BY cnt ASC",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.OrderBy[0].Desc {
					t.Error("expected ASC")
				}
			},
		},
		{
			name: "order by multiple columns",
			sql:  "SELECT region, status, COUNT(*) FROM 'kafka://b/t' GROUP BY region, status ORDER BY region ASC, status DESC",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if len(stmt.OrderBy) != 2 {
					t.Fatalf("expected 2 ORDER BY items, got %d", len(stmt.OrderBy))
				}
				if stmt.OrderBy[0].Desc {
					t.Error("item 0: expected ASC")
				}
				if !stmt.OrderBy[1].Desc {
					t.Error("item 1: expected DESC")
				}
			},
		},

		// =====================================================================
		// LIMIT
		// =====================================================================
		{
			name: "limit N",
			sql:  "SELECT x LIMIT 100",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.Limit == nil || *stmt.Limit != 100 {
					t.Errorf("expected LIMIT 100, got %v", stmt.Limit)
				}
			},
		},
		{
			name: "limit 0",
			sql:  "SELECT x LIMIT 0",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.Limit == nil || *stmt.Limit != 0 {
					t.Errorf("expected LIMIT 0, got %v", stmt.Limit)
				}
			},
		},

		// =====================================================================
		// WINDOW
		// =====================================================================
		{
			name: "window tumbling",
			sql:  "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1 minute'",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.Window == nil {
					t.Fatal("expected WINDOW clause")
				}
				if stmt.Window.Type != "TUMBLING" {
					t.Errorf("type: got %q, want TUMBLING", stmt.Window.Type)
				}
				if stmt.Window.Size != "1 minute" {
					t.Errorf("size: got %q, want %q", stmt.Window.Size, "1 minute")
				}
			},
		},
		{
			name: "window sliding",
			sql:  "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW SLIDING '10 minutes' BY '5 minutes'",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.Window == nil {
					t.Fatal("expected WINDOW clause")
				}
				if stmt.Window.Type != "SLIDING" {
					t.Errorf("type: got %q, want SLIDING", stmt.Window.Type)
				}
				if stmt.Window.Size != "10 minutes" {
					t.Errorf("size: got %q", stmt.Window.Size)
				}
				if stmt.Window.SlideBy != "5 minutes" {
					t.Errorf("slide: got %q, want %q", stmt.Window.SlideBy, "5 minutes")
				}
			},
		},
		{
			name: "window session",
			sql:  "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW SESSION '5 minutes'",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.Window == nil {
					t.Fatal("expected WINDOW clause")
				}
				if stmt.Window.Type != "SESSION" {
					t.Errorf("type: got %q, want SESSION", stmt.Window.Type)
				}
			},
		},

		// =====================================================================
		// EMIT
		// =====================================================================
		{
			name: "emit final",
			sql:  "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1 minute' EMIT FINAL",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.Emit == nil {
					t.Fatal("expected EMIT clause")
				}
				if stmt.Emit.Type != "FINAL" {
					t.Errorf("type: got %q, want FINAL", stmt.Emit.Type)
				}
			},
		},
		{
			name: "emit early",
			sql:  "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1 minute' EMIT EARLY '10 seconds'",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.Emit == nil {
					t.Fatal("expected EMIT clause")
				}
				if stmt.Emit.Type != "EARLY" {
					t.Errorf("type: got %q, want EARLY", stmt.Emit.Type)
				}
				if stmt.Emit.Interval != "10 seconds" {
					t.Errorf("interval: got %q, want %q", stmt.Emit.Interval, "10 seconds")
				}
			},
		},

		// =====================================================================
		// EVENT TIME BY
		// =====================================================================
		{
			name: "event time by column",
			sql:  "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1 minute' EVENT TIME BY ts",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.EventTime == nil {
					t.Fatal("expected EVENT TIME clause")
				}
				ref, ok := stmt.EventTime.Expr.(*ast.ColumnRef)
				if !ok {
					t.Fatalf("expected ColumnRef, got %T", stmt.EventTime.Expr)
				}
				if ref.Name != "ts" {
					t.Errorf("event time col: got %q, want ts", ref.Name)
				}
			},
		},

		// =====================================================================
		// WATERMARK
		// =====================================================================
		{
			name: "watermark duration",
			sql:  "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1 minute' EVENT TIME BY ts WATERMARK '30 seconds'",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.Watermark == nil {
					t.Fatal("expected WATERMARK clause")
				}
				if stmt.Watermark.Duration != "30 seconds" {
					t.Errorf("duration: got %q, want %q", stmt.Watermark.Duration, "30 seconds")
				}
			},
		},

		// =====================================================================
		// DEDUPLICATE BY
		// =====================================================================
		{
			name: "deduplicate by column within",
			sql:  "SELECT * DEDUPLICATE BY order_id WITHIN '10 minutes'",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.Deduplicate == nil {
					t.Fatal("expected DEDUPLICATE clause")
				}
				ref := stmt.Deduplicate.Key.(*ast.ColumnRef)
				if ref.Name != "order_id" {
					t.Errorf("key: got %q, want order_id", ref.Name)
				}
				if stmt.Deduplicate.Within != "10 minutes" {
					t.Errorf("within: got %q", stmt.Deduplicate.Within)
				}
				if stmt.Deduplicate.Capacity != nil {
					t.Errorf("expected nil capacity, got %d", *stmt.Deduplicate.Capacity)
				}
			},
		},
		{
			name: "deduplicate by with capacity",
			sql:  "SELECT * DEDUPLICATE BY id WITHIN '5 minutes' CAPACITY 500000",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.Deduplicate.Capacity == nil || *stmt.Deduplicate.Capacity != 500000 {
					t.Errorf("capacity: got %v, want 500000", stmt.Deduplicate.Capacity)
				}
			},
		},

		// =====================================================================
		// Standalone FORMAT (trailing)
		// =====================================================================
		{
			name: "trailing format on implicit stdin",
			sql:  "SELECT * FORMAT CSV(delimiter=',', header=true)",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.From == nil {
					t.Fatal("expected FROM to be populated by standalone FORMAT")
				}
				if stmt.From.Format != "CSV" {
					t.Errorf("format: got %q, want CSV", stmt.From.Format)
				}
				if stmt.From.FormatOptions["delimiter"] != "," {
					t.Errorf("delimiter: got %q", stmt.From.FormatOptions["delimiter"])
				}
			},
		},

		// =====================================================================
		// Complex combinations
		// =====================================================================
		{
			name: "select with join where group_by having order_by",
			sql: `SELECT u.name, COUNT(*) AS cnt
				FROM stdin e
				JOIN '/tmp/users.ndjson' u ON e.user_id = u.id
				WHERE e.action = 'login'
				GROUP BY u.name
				HAVING COUNT(*) > 5
				ORDER BY cnt DESC`,
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if len(stmt.Columns) != 2 {
					t.Errorf("expected 2 columns, got %d", len(stmt.Columns))
				}
				if stmt.Join == nil {
					t.Error("expected JOIN")
				}
				if stmt.Where == nil {
					t.Error("expected WHERE")
				}
				if stmt.GroupBy == nil {
					t.Error("expected GROUP BY")
				}
				if stmt.Having == nil {
					t.Error("expected HAVING")
				}
				if len(stmt.OrderBy) != 1 {
					t.Errorf("expected 1 ORDER BY, got %d", len(stmt.OrderBy))
				}
			},
		},
		{
			name: "window with event_time watermark emit",
			sql: `SELECT region, COUNT(*) AS cnt
				FROM 'kafka://broker/events'
				GROUP BY region
				WINDOW TUMBLING '5 minutes'
				EVENT TIME BY event_time
				WATERMARK '30 seconds'
				EMIT EARLY '10 seconds'`,
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.Window == nil {
					t.Error("expected WINDOW")
				}
				if stmt.EventTime == nil {
					t.Error("expected EVENT TIME")
				}
				if stmt.Watermark == nil {
					t.Error("expected WATERMARK")
				}
				if stmt.Emit == nil {
					t.Error("expected EMIT")
				}
			},
		},
		{
			name: "seed from with join and group_by",
			sql: `SELECT e.status, COUNT(*)
				FROM stdin e
				JOIN '/tmp/users.ndjson' u ON e.uid = u.id
				SEED FROM '/tmp/seed.ndjson'
				GROUP BY e.status`,
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.Join == nil {
					t.Error("expected JOIN")
				}
				if stmt.Seed == nil {
					t.Error("expected SEED")
				}
				if stmt.GroupBy == nil {
					t.Error("expected GROUP BY")
				}
			},
		},
		{
			name: "format debezium with group_by having",
			sql:  "SELECT _after->>'region', COUNT(*) AS cnt FROM 'kafka://b/t' FORMAT DEBEZIUM GROUP BY _after->>'region' HAVING COUNT(*) > 10",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.From.Format != "DEBEZIUM" {
					t.Errorf("format: got %q, want DEBEZIUM", stmt.From.Format)
				}
				if stmt.GroupBy == nil {
					t.Error("expected GROUP BY")
				}
				if stmt.Having == nil {
					t.Error("expected HAVING")
				}
			},
		},
		{
			name: "complex cdc query with join aggregates case having order",
			sql: `SELECT
				u.name,
				COUNT(*) AS total_events,
				SUM(CASE WHEN e.action = 'purchase' THEN 1 ELSE 0 END) AS purchases,
				AVG((e.payload->>'amount')::FLOAT) AS avg_amount,
				MIN((e.payload->>'amount')::FLOAT) AS min_amount,
				MAX((e.payload->>'amount')::FLOAT) AS max_amount,
				COUNT(DISTINCT e.session_id) AS sessions,
				FIRST(e.action) AS first_action
				FROM 'kafka://broker/events' e
				JOIN '/data/users.parquet' u ON e.user_id = u.id
				WHERE e.action != 'heartbeat'
				GROUP BY u.name
				HAVING COUNT(*) > 10
				ORDER BY total_events DESC
				LIMIT 50`,
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if len(stmt.Columns) != 8 {
					t.Errorf("expected 8 columns, got %d", len(stmt.Columns))
				}
				if stmt.Join == nil {
					t.Error("expected JOIN")
				}
				if stmt.Where == nil {
					t.Error("expected WHERE")
				}
				if len(stmt.GroupBy) != 1 {
					t.Errorf("expected 1 GROUP BY, got %d", len(stmt.GroupBy))
				}
				if stmt.Having == nil {
					t.Error("expected HAVING")
				}
				if len(stmt.OrderBy) != 1 {
					t.Errorf("expected 1 ORDER BY, got %d", len(stmt.OrderBy))
				}
				if stmt.Limit == nil || *stmt.Limit != 50 {
					t.Errorf("expected LIMIT 50")
				}
			},
		},
		{
			name: "stream-stream join with within and window",
			sql: `SELECT o.order_id, COUNT(*) AS matches
				FROM 'kafka://broker/orders' o
				JOIN 'kafka://broker/payments' p ON o.order_id = p.order_id
				WITHIN INTERVAL '5 minutes'
				GROUP BY o.order_id
				WINDOW TUMBLING '1 minute'
				EVENT TIME BY o.created_at
				WATERMARK '10 seconds'
				EMIT FINAL`,
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.Join == nil || stmt.Join.Within == nil {
					t.Error("expected JOIN with WITHIN")
				}
				if stmt.Window == nil {
					t.Error("expected WINDOW")
				}
				if stmt.EventTime == nil {
					t.Error("expected EVENT TIME")
				}
				if stmt.Watermark == nil {
					t.Error("expected WATERMARK")
				}
				if stmt.Emit == nil {
					t.Error("expected EMIT")
				}
			},
		},
		{
			name: "select distinct with from and where and limit",
			sql:  "SELECT DISTINCT region FROM 'kafka://broker/events' WHERE status = 'active' LIMIT 10",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if !stmt.Distinct {
					t.Error("expected Distinct=true")
				}
				if stmt.From == nil {
					t.Error("expected FROM")
				}
				if stmt.Where == nil {
					t.Error("expected WHERE")
				}
				if stmt.Limit == nil || *stmt.Limit != 10 {
					t.Error("expected LIMIT 10")
				}
			},
		},
		{
			name: "left join with format csv options",
			sql:  "SELECT e.id, u.name FROM stdin e LEFT JOIN '/data/users.csv' FORMAT CSV(header=true, delimiter=',') u ON e.uid = u.id",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.Join == nil {
					t.Fatal("expected JOIN")
				}
				if stmt.Join.Type != "LEFT JOIN" {
					t.Errorf("type: got %q, want LEFT JOIN", stmt.Join.Type)
				}
				if stmt.Join.Source.Format != "CSV" {
					t.Errorf("format: got %q, want CSV", stmt.Join.Source.Format)
				}
				if stmt.Join.Source.FormatOptions["header"] != "true" {
					t.Errorf("header opt: got %q", stmt.Join.Source.FormatOptions["header"])
				}
			},
		},
		{
			name: "deduplicate with where and limit",
			sql:  "SELECT * WHERE status = 'ok' DEDUPLICATE BY request_id WITHIN '1 minute' LIMIT 1000",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.Where == nil {
					t.Error("expected WHERE")
				}
				if stmt.Deduplicate == nil {
					t.Error("expected DEDUPLICATE")
				}
				if stmt.Limit == nil || *stmt.Limit != 1000 {
					t.Error("expected LIMIT 1000")
				}
			},
		},
		{
			name: "multiple aggregates no group by (implicit single group)",
			sql:  "SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount)",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if len(stmt.Columns) != 5 {
					t.Errorf("expected 5 columns, got %d", len(stmt.Columns))
				}
				if stmt.GroupBy != nil {
					t.Error("expected nil GROUP BY for implicit single group")
				}
			},
		},
		{
			name: "from with alias and trailing format",
			sql:  "SELECT * FROM stdin e FORMAT DEBEZIUM",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.FromAlias != "e" {
					t.Errorf("alias: got %q, want e", stmt.FromAlias)
				}
				if stmt.From.Format != "DEBEZIUM" {
					t.Errorf("format: got %q, want DEBEZIUM", stmt.From.Format)
				}
			},
		},
		{
			name: "cast in where clause",
			sql:  "SELECT x WHERE (payload->>'score')::INT > 90",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				bin := stmt.Where.(*ast.BinaryExpr)
				if bin.Op != ">" {
					t.Errorf("op: got %q, want >", bin.Op)
				}
				cast, ok := bin.Left.(*ast.CastExpr)
				if !ok {
					t.Fatalf("expected CastExpr, got %T", bin.Left)
				}
				if cast.TypeName != "INT" {
					t.Errorf("cast type: got %q, want INT", cast.TypeName)
				}
			},
		},
		{
			name: "arithmetic expression in select with subtraction",
			sql:  "SELECT revenue - cost AS profit",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				bin := stmt.Columns[0].Expr.(*ast.BinaryExpr)
				if bin.Op != "-" {
					t.Errorf("op: got %q, want -", bin.Op)
				}
				if stmt.Columns[0].Alias != "profit" {
					t.Errorf("alias: got %q, want profit", stmt.Columns[0].Alias)
				}
			},
		},
		{
			name: "interval literal in expression",
			sql:  "SELECT INTERVAL '1 hour'",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				il, ok := stmt.Columns[0].Expr.(*ast.IntervalLiteral)
				if !ok {
					t.Fatalf("expected IntervalLiteral, got %T", stmt.Columns[0].Expr)
				}
				if il.Value != "1 hour" {
					t.Errorf("value: got %q, want %q", il.Value, "1 hour")
				}
			},
		},
		{
			name: "from stdin explicit URI",
			sql:  "SELECT * FROM 'stdin://'",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.From == nil || stmt.From.URI != "stdin://" {
					t.Errorf("expected URI stdin://, got %v", stmt.From)
				}
			},
		},
		{
			name: "format avro with registry option",
			sql:  "SELECT * FROM 'kafka://b/t' FORMAT AVRO(registry='http://registry:8081')",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				if stmt.From.Format != "AVRO" {
					t.Errorf("format: got %q, want AVRO", stmt.From.Format)
				}
				if stmt.From.FormatOptions["registry"] != "http://registry:8081" {
					t.Errorf("registry: got %q", stmt.From.FormatOptions["registry"])
				}
			},
		},
		{
			name: "csv format with all options",
			sql:  `SELECT * FROM 'kafka://b/t' FORMAT CSV(delimiter='|', header=true, quote='"', null_string='')`,
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				opts := stmt.From.FormatOptions
				if opts["delimiter"] != "|" {
					t.Errorf("delimiter: got %q", opts["delimiter"])
				}
				if opts["header"] != "true" {
					t.Errorf("header: got %q", opts["header"])
				}
				if opts["quote"] != `"` {
					t.Errorf("quote: got %q", opts["quote"])
				}
				if opts["null_string"] != "" {
					t.Errorf("null_string: got %q", opts["null_string"])
				}
			},
		},
		{
			name: "precedence mul before add",
			sql:  "SELECT a + b * c",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				bin := stmt.Columns[0].Expr.(*ast.BinaryExpr)
				if bin.Op != "+" {
					t.Errorf("top op: got %q, want +", bin.Op)
				}
				right := bin.Right.(*ast.BinaryExpr)
				if right.Op != "*" {
					t.Errorf("right op: got %q, want *", right.Op)
				}
			},
		},
		{
			name: "AND binds tighter than OR",
			sql:  "SELECT a OR b AND c",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				bin := stmt.Columns[0].Expr.(*ast.BinaryExpr)
				if bin.Op != "OR" {
					t.Errorf("top op: got %q, want OR", bin.Op)
				}
				right := bin.Right.(*ast.BinaryExpr)
				if right.Op != "AND" {
					t.Errorf("right op: got %q, want AND", right.Op)
				}
			},
		},
		{
			name: "NOT binds tighter than AND",
			sql:  "SELECT NOT a AND b",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				bin := stmt.Columns[0].Expr.(*ast.BinaryExpr)
				if bin.Op != "AND" {
					t.Errorf("top op: got %q, want AND", bin.Op)
				}
				left := bin.Left.(*ast.UnaryExpr)
				if left.Op != "NOT" {
					t.Errorf("left op: got %q, want NOT", left.Op)
				}
			},
		},
		{
			name: "kafka with sasl params",
			sql:  "SELECT * FROM 'kafka://broker/topic?sasl_mechanism=PLAIN&sasl_username=user&sasl_password=pass'",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				want := "kafka://broker/topic?sasl_mechanism=PLAIN&sasl_username=user&sasl_password=pass"
				if stmt.From.URI != want {
					t.Errorf("URI: got %q", stmt.From.URI)
				}
			},
		},
		{
			name: "qualified ref in select and group by",
			sql:  "SELECT e.region, COUNT(*) FROM stdin e GROUP BY e.region",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				qr := stmt.Columns[0].Expr.(*ast.QualifiedRef)
				if qr.Qualifier != "e" || qr.Name != "region" {
					t.Errorf("col: got %s.%s", qr.Qualifier, qr.Name)
				}
				gb := stmt.GroupBy[0].(*ast.QualifiedRef)
				if gb.Qualifier != "e" || gb.Name != "region" {
					t.Errorf("group: got %s.%s", gb.Qualifier, gb.Name)
				}
			},
		},
		{
			name: "is true and is false",
			sql:  "SELECT x WHERE x IS TRUE",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				is := stmt.Where.(*ast.IsExpr)
				if is.What != "TRUE" {
					t.Errorf("what: got %q, want TRUE", is.What)
				}
			},
		},
		{
			name: "not between",
			sql:  "SELECT x WHERE x NOT BETWEEN 1 AND 10",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				be := stmt.Where.(*ast.BetweenExpr)
				if !be.Not {
					t.Error("expected Not=true")
				}
			},
		},
		{
			name: "not ilike",
			sql:  "SELECT n WHERE n NOT ILIKE '%test%'",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				ue := stmt.Where.(*ast.UnaryExpr)
				if ue.Op != "NOT" {
					t.Errorf("op: got %q, want NOT", ue.Op)
				}
				bin := ue.Expr.(*ast.BinaryExpr)
				if bin.Op != "ILIKE" {
					t.Errorf("inner: got %q, want ILIKE", bin.Op)
				}
			},
		},
		{
			name: "json arrow text then cast",
			sql:  "SELECT (col->>'amount')::FLOAT",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				cast := stmt.Columns[0].Expr.(*ast.CastExpr)
				if cast.TypeName != "FLOAT" {
					t.Errorf("type: got %q, want FLOAT", cast.TypeName)
				}
				ja := cast.Expr.(*ast.JsonAccessExpr)
				if !ja.AsText {
					t.Error("expected AsText=true")
				}
			},
		},
		{
			name: "parse_timestamp function",
			sql:  "SELECT PARSE_TIMESTAMP(ts, 'RFC3339')",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				fc := stmt.Columns[0].Expr.(*ast.FunctionCall)
				if fc.Name != "PARSE_TIMESTAMP" || len(fc.Args) != 2 {
					t.Errorf("expected PARSE_TIMESTAMP(2 args), got %s(%d)", fc.Name, len(fc.Args))
				}
			},
		},
		{
			name: "format_timestamp function",
			sql:  "SELECT FORMAT_TIMESTAMP(ts, 'RFC3339')",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				fc := stmt.Columns[0].Expr.(*ast.FunctionCall)
				if fc.Name != "FORMAT_TIMESTAMP" || len(fc.Args) != 2 {
					t.Errorf("expected FORMAT_TIMESTAMP(2 args), got %s(%d)", fc.Name, len(fc.Args))
				}
			},
		},
		{
			name: "coalesce with cast",
			sql:  "SELECT COALESCE((payload->>'amount')::FLOAT, 0)",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				fc := stmt.Columns[0].Expr.(*ast.FunctionCall)
				if fc.Name != "COALESCE" || len(fc.Args) != 2 {
					t.Errorf("expected COALESCE(2 args), got %s(%d)", fc.Name, len(fc.Args))
				}
			},
		},
		{
			name: "case when in aggregation",
			sql:  "SELECT SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) AS ok_count",
			check: func(t *testing.T, stmt *ast.SelectStatement) {
				fc := stmt.Columns[0].Expr.(*ast.FunctionCall)
				if fc.Name != "SUM" || len(fc.Args) != 1 {
					t.Errorf("expected SUM(1 arg), got %s(%d)", fc.Name, len(fc.Args))
				}
				_, ok := fc.Args[0].(*ast.CaseExpr)
				if !ok {
					t.Fatalf("expected CaseExpr inside SUM, got %T", fc.Args[0])
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New(tt.sql)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}
			if tt.check != nil {
				tt.check(t, stmt)
			}
		})
	}
}
