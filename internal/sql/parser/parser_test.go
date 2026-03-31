package parser

import (
	"testing"

	"github.com/kevin-cantwell/folddb/internal/sql/ast"
)

func TestBasicSelectWithWhere(t *testing.T) {
	p := New("SELECT name WHERE age > 25")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(stmt.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(stmt.Columns))
	}
	ref, ok := stmt.Columns[0].Expr.(*ast.ColumnRef)
	if !ok {
		t.Fatalf("expected ColumnRef, got %T", stmt.Columns[0].Expr)
	}
	if ref.Name != "name" {
		t.Errorf("column name: got %q, want %q", ref.Name, "name")
	}
	if stmt.Where == nil {
		t.Fatal("expected WHERE clause")
	}
}

func TestSelectWithAlias(t *testing.T) {
	p := New("SELECT name AS username")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stmt.Columns[0].Alias != "username" {
		t.Errorf("alias: got %q, want %q", stmt.Columns[0].Alias, "username")
	}
}

func TestSelectStar(t *testing.T) {
	p := New("SELECT *")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(stmt.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(stmt.Columns))
	}
	if _, ok := stmt.Columns[0].Expr.(*ast.StarExpr); !ok {
		t.Errorf("expected StarExpr, got %T", stmt.Columns[0].Expr)
	}
}

func TestSelectMultipleColumns(t *testing.T) {
	p := New("SELECT a, b, c")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(stmt.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(stmt.Columns))
	}
}

func TestSelectDistinct(t *testing.T) {
	p := New("SELECT DISTINCT x")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !stmt.Distinct {
		t.Error("expected Distinct to be true")
	}
}

func TestFromSourceURI(t *testing.T) {
	p := New("SELECT * FROM 'kafka://localhost:9092/topic'")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stmt.From == nil {
		t.Fatal("expected FROM clause")
	}
	if stmt.From.URI != "kafka://localhost:9092/topic" {
		t.Errorf("URI: got %q, want %q", stmt.From.URI, "kafka://localhost:9092/topic")
	}
}

func TestFromOmittedMeansStdin(t *testing.T) {
	p := New("SELECT name WHERE age > 30")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stmt.From != nil {
		t.Error("expected From to be nil (stdin implied)")
	}
}

func TestFormatClause(t *testing.T) {
	p := New("SELECT * FROM 'kafka://b/t' FORMAT DEBEZIUM")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stmt.From.Format != "DEBEZIUM" {
		t.Errorf("format: got %q, want %q", stmt.From.Format, "DEBEZIUM")
	}
}

func TestGroupBy(t *testing.T) {
	p := New("SELECT g, COUNT(*) FROM 'kafka://b/t' GROUP BY g")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(stmt.GroupBy) != 1 {
		t.Fatalf("expected 1 GROUP BY expr, got %d", len(stmt.GroupBy))
	}
}

func TestGroupByMultiple(t *testing.T) {
	p := New("SELECT a, b, COUNT(*) FROM 'kafka://b/t' GROUP BY a, b")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(stmt.GroupBy) != 2 {
		t.Errorf("expected 2 GROUP BY exprs, got %d", len(stmt.GroupBy))
	}
}

func TestHaving(t *testing.T) {
	p := New("SELECT g, COUNT(*) AS c FROM 'kafka://b/t' GROUP BY g HAVING c > 1")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stmt.Having == nil {
		t.Error("expected HAVING clause")
	}
}

func TestOrderByOnAccumulating(t *testing.T) {
	p := New("SELECT g, COUNT(*) AS c FROM 'kafka://b/t' GROUP BY g ORDER BY c DESC")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(stmt.OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY item, got %d", len(stmt.OrderBy))
	}
	if !stmt.OrderBy[0].Desc {
		t.Error("expected DESC")
	}
}

func TestOrderByRejectedOnNonAccumulating(t *testing.T) {
	// TC-PARSER-013
	p := New("SELECT x ORDER BY x DESC")
	_, err := p.Parse()
	if err == nil {
		t.Fatal("expected error for ORDER BY on non-accumulating query")
	}
	expected := "ORDER BY is not supported on non-accumulating streaming queries"
	if !containsStr(err.Error(), expected) {
		t.Errorf("error message: got %q, want substring %q", err.Error(), expected)
	}
}

func TestLimit(t *testing.T) {
	p := New("SELECT x LIMIT 10")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stmt.Limit == nil || *stmt.Limit != 10 {
		t.Errorf("expected LIMIT 10, got %v", stmt.Limit)
	}
}

func TestWindowTumbling(t *testing.T) {
	p := New("SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1 minute'")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stmt.Window == nil {
		t.Fatal("expected WINDOW clause")
	}
	if stmt.Window.Type != "TUMBLING" {
		t.Errorf("window type: got %q, want %q", stmt.Window.Type, "TUMBLING")
	}
	if stmt.Window.Size != "1 minute" {
		t.Errorf("window size: got %q, want %q", stmt.Window.Size, "1 minute")
	}
}

func TestWindowSliding(t *testing.T) {
	p := New("SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW SLIDING '10 minutes' BY '5 minutes'")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stmt.Window == nil {
		t.Fatal("expected WINDOW clause")
	}
	if stmt.Window.Type != "SLIDING" {
		t.Errorf("window type: got %q, want %q", stmt.Window.Type, "SLIDING")
	}
	if stmt.Window.SlideBy != "5 minutes" {
		t.Errorf("slide: got %q, want %q", stmt.Window.SlideBy, "5 minutes")
	}
}

func TestWindowSession(t *testing.T) {
	p := New("SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW SESSION '5 minutes'")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stmt.Window.Type != "SESSION" {
		t.Errorf("window type: got %q, want %q", stmt.Window.Type, "SESSION")
	}
}

func TestDeduplicateBy(t *testing.T) {
	p := New("SELECT * DEDUPLICATE BY order_id WITHIN '10 minutes'")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stmt.Deduplicate == nil {
		t.Fatal("expected DEDUPLICATE clause")
	}
	ref, ok := stmt.Deduplicate.Key.(*ast.ColumnRef)
	if !ok {
		t.Fatalf("expected ColumnRef for dedup key, got %T", stmt.Deduplicate.Key)
	}
	if ref.Name != "order_id" {
		t.Errorf("dedup key: got %q, want %q", ref.Name, "order_id")
	}
	if stmt.Deduplicate.Within != "10 minutes" {
		t.Errorf("within: got %q, want %q", stmt.Deduplicate.Within, "10 minutes")
	}
}

func TestDeduplicateByWithCapacity(t *testing.T) {
	p := New("SELECT * DEDUPLICATE BY id WITHIN '5 minutes' CAPACITY 500000")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stmt.Deduplicate.Capacity == nil || *stmt.Deduplicate.Capacity != 500000 {
		t.Errorf("capacity: got %v, want 500000", stmt.Deduplicate.Capacity)
	}
}

func TestJsonAccessExpression(t *testing.T) {
	p := New("SELECT payload->'user'->>'email' AS email")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(stmt.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(stmt.Columns))
	}
	// The outer expression should be a JsonAccessExpr with AsText=true
	ja, ok := stmt.Columns[0].Expr.(*ast.JsonAccessExpr)
	if !ok {
		t.Fatalf("expected JsonAccessExpr, got %T", stmt.Columns[0].Expr)
	}
	if !ja.AsText {
		t.Error("expected AsText=true for ->>")
	}
}

func TestTypeCast(t *testing.T) {
	p := New("SELECT val::INT AS num")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cast, ok := stmt.Columns[0].Expr.(*ast.CastExpr)
	if !ok {
		t.Fatalf("expected CastExpr, got %T", stmt.Columns[0].Expr)
	}
	if cast.TypeName != "INT" {
		t.Errorf("type: got %q, want %q", cast.TypeName, "INT")
	}
}

func TestCastFunction(t *testing.T) {
	p := New("SELECT CAST(val AS INT) AS num")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cast, ok := stmt.Columns[0].Expr.(*ast.CastExpr)
	if !ok {
		t.Fatalf("expected CastExpr, got %T", stmt.Columns[0].Expr)
	}
	if cast.TypeName != "INT" {
		t.Errorf("type: got %q, want %q", cast.TypeName, "INT")
	}
}

func TestOperatorPrecedenceMultBeforeAdd(t *testing.T) {
	// a + b * c should parse as a + (b * c)
	p := New("SELECT a + b * c")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bin, ok := stmt.Columns[0].Expr.(*ast.BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", stmt.Columns[0].Expr)
	}
	if bin.Op != "+" {
		t.Errorf("top op: got %q, want %q", bin.Op, "+")
	}
	// Right side should be b * c
	right, ok := bin.Right.(*ast.BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr on right, got %T", bin.Right)
	}
	if right.Op != "*" {
		t.Errorf("right op: got %q, want %q", right.Op, "*")
	}
}

func TestParenthesesOverridePrecedence(t *testing.T) {
	// (a + b) * c
	p := New("SELECT (a + b) * c")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
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
}

func TestAndOrPrecedence(t *testing.T) {
	// a OR b AND c should parse as a OR (b AND c)
	p := New("SELECT a OR b AND c")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bin, ok := stmt.Columns[0].Expr.(*ast.BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", stmt.Columns[0].Expr)
	}
	if bin.Op != "OR" {
		t.Errorf("top op: got %q, want %q", bin.Op, "OR")
	}
	right, ok := bin.Right.(*ast.BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr on right, got %T", bin.Right)
	}
	if right.Op != "AND" {
		t.Errorf("right op: got %q, want %q", right.Op, "AND")
	}
}

func TestNotPrecedence(t *testing.T) {
	// NOT a AND b should parse as (NOT a) AND b
	p := New("SELECT NOT a AND b")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bin, ok := stmt.Columns[0].Expr.(*ast.BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr (AND), got %T", stmt.Columns[0].Expr)
	}
	if bin.Op != "AND" {
		t.Errorf("top op: got %q, want %q", bin.Op, "AND")
	}
	left, ok := bin.Left.(*ast.UnaryExpr)
	if !ok {
		t.Fatalf("expected UnaryExpr (NOT) on left, got %T", bin.Left)
	}
	if left.Op != "NOT" {
		t.Errorf("left op: got %q, want %q", left.Op, "NOT")
	}
}

func TestCaseWhenExpression(t *testing.T) {
	p := New("SELECT CASE WHEN x > 3 THEN 'big' ELSE 'small' END AS label")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
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
}

func TestNestedCaseWhen(t *testing.T) {
	p := New("SELECT CASE WHEN x > 10 THEN 'huge' WHEN x > 3 THEN 'big' ELSE 'small' END")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ce, ok := stmt.Columns[0].Expr.(*ast.CaseExpr)
	if !ok {
		t.Fatalf("expected CaseExpr, got %T", stmt.Columns[0].Expr)
	}
	if len(ce.Whens) != 2 {
		t.Errorf("expected 2 WHENs, got %d", len(ce.Whens))
	}
}

func TestBetweenExpression(t *testing.T) {
	p := New("SELECT x WHERE x BETWEEN 3 AND 8")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	be, ok := stmt.Where.(*ast.BetweenExpr)
	if !ok {
		t.Fatalf("expected BetweenExpr, got %T", stmt.Where)
	}
	if be.Not {
		t.Error("expected Not=false")
	}
}

func TestNotBetweenExpression(t *testing.T) {
	p := New("SELECT x WHERE x NOT BETWEEN 3 AND 8")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	be, ok := stmt.Where.(*ast.BetweenExpr)
	if !ok {
		t.Fatalf("expected BetweenExpr, got %T", stmt.Where)
	}
	if !be.Not {
		t.Error("expected Not=true")
	}
}

func TestInExpression(t *testing.T) {
	p := New("SELECT s WHERE s IN ('a', 'c')")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ie, ok := stmt.Where.(*ast.InExpr)
	if !ok {
		t.Fatalf("expected InExpr, got %T", stmt.Where)
	}
	if len(ie.Values) != 2 {
		t.Errorf("expected 2 IN values, got %d", len(ie.Values))
	}
}

func TestIsNullExpression(t *testing.T) {
	p := New("SELECT x WHERE x IS NULL")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	is, ok := stmt.Where.(*ast.IsExpr)
	if !ok {
		t.Fatalf("expected IsExpr, got %T", stmt.Where)
	}
	if is.What != "NULL" || is.Not {
		t.Errorf("expected IS NULL, got IS%s %s", map[bool]string{true: " NOT", false: ""}[is.Not], is.What)
	}
}

func TestIsNotNullExpression(t *testing.T) {
	p := New("SELECT x WHERE x IS NOT NULL")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	is, ok := stmt.Where.(*ast.IsExpr)
	if !ok {
		t.Fatalf("expected IsExpr, got %T", stmt.Where)
	}
	if is.What != "NULL" || !is.Not {
		t.Errorf("expected IS NOT NULL")
	}
}

func TestIsDistinctFrom(t *testing.T) {
	p := New("SELECT x WHERE x IS DISTINCT FROM y")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	is, ok := stmt.Where.(*ast.IsExpr)
	if !ok {
		t.Fatalf("expected IsExpr, got %T", stmt.Where)
	}
	if is.What != "DISTINCT FROM" || is.Not {
		t.Error("expected IS DISTINCT FROM")
	}
	if is.From == nil {
		t.Error("expected From expression")
	}
}

func TestIsNotDistinctFrom(t *testing.T) {
	p := New("SELECT x WHERE x IS NOT DISTINCT FROM y")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	is, ok := stmt.Where.(*ast.IsExpr)
	if !ok {
		t.Fatalf("expected IsExpr, got %T", stmt.Where)
	}
	if is.What != "DISTINCT FROM" || !is.Not {
		t.Error("expected IS NOT DISTINCT FROM")
	}
}

func TestFunctionCallCountStar(t *testing.T) {
	p := New("SELECT COUNT(*)")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fc, ok := stmt.Columns[0].Expr.(*ast.FunctionCall)
	if !ok {
		t.Fatalf("expected FunctionCall, got %T", stmt.Columns[0].Expr)
	}
	if fc.Name != "COUNT" {
		t.Errorf("func name: got %q, want %q", fc.Name, "COUNT")
	}
	if len(fc.Args) != 1 {
		t.Fatalf("expected 1 arg, got %d", len(fc.Args))
	}
	if _, ok := fc.Args[0].(*ast.StarExpr); !ok {
		t.Errorf("expected StarExpr arg, got %T", fc.Args[0])
	}
}

func TestFunctionCallCountDistinct(t *testing.T) {
	p := New("SELECT COUNT(DISTINCT x)")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fc, ok := stmt.Columns[0].Expr.(*ast.FunctionCall)
	if !ok {
		t.Fatalf("expected FunctionCall, got %T", stmt.Columns[0].Expr)
	}
	if !fc.Distinct {
		t.Error("expected Distinct=true")
	}
}

func TestCoalesceFunction(t *testing.T) {
	p := New("SELECT COALESCE(a, b, c)")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fc, ok := stmt.Columns[0].Expr.(*ast.FunctionCall)
	if !ok {
		t.Fatalf("expected FunctionCall, got %T", stmt.Columns[0].Expr)
	}
	if fc.Name != "COALESCE" || len(fc.Args) != 3 {
		t.Errorf("expected COALESCE with 3 args, got %s with %d", fc.Name, len(fc.Args))
	}
}

func TestEmitClause(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantType string
		wantInt  string
	}{
		{"final", "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1 minute' EMIT FINAL", "FINAL", ""},
		{"early", "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1 minute' EMIT EARLY '10 seconds'", "EARLY", "10 seconds"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New(tt.input)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if stmt.Emit == nil {
				t.Fatal("expected EMIT clause")
			}
			if stmt.Emit.Type != tt.wantType {
				t.Errorf("emit type: got %q, want %q", stmt.Emit.Type, tt.wantType)
			}
			if stmt.Emit.Interval != tt.wantInt {
				t.Errorf("emit interval: got %q, want %q", stmt.Emit.Interval, tt.wantInt)
			}
		})
	}
}

func TestEventTimeAndWatermark(t *testing.T) {
	p := New("SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1 minute' EVENT TIME BY ts WATERMARK '30 seconds'")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stmt.EventTime == nil {
		t.Fatal("expected EVENT TIME clause")
	}
	ref, ok := stmt.EventTime.Expr.(*ast.ColumnRef)
	if !ok {
		t.Fatalf("expected ColumnRef, got %T", stmt.EventTime.Expr)
	}
	if ref.Name != "ts" {
		t.Errorf("event time col: got %q, want %q", ref.Name, "ts")
	}
	if stmt.Watermark == nil {
		t.Fatal("expected WATERMARK clause")
	}
	if stmt.Watermark.Duration != "30 seconds" {
		t.Errorf("watermark: got %q, want %q", stmt.Watermark.Duration, "30 seconds")
	}
}

func TestErrorMissingSelect(t *testing.T) {
	p := New("name WHERE age > 25")
	_, err := p.Parse()
	if err == nil {
		t.Fatal("expected error for missing SELECT")
	}
}

func TestErrorUnclosedParenthesis(t *testing.T) {
	p := New("SELECT COUNT(* FROM 'kafka://b/t'")
	_, err := p.Parse()
	if err == nil {
		t.Fatal("expected error for unclosed parenthesis")
	}
}

func TestErrorUnexpectedTokenAfterStatement(t *testing.T) {
	// "GARBAGE" after a column gets parsed as an implicit alias, so use
	// something that can't be consumed: two identifiers in a row after a comma-less expr list
	p := New("SELECT x y z")
	_, err := p.Parse()
	if err == nil {
		t.Fatal("expected error for trailing garbage")
	}
}

func TestLikeExpression(t *testing.T) {
	p := New("SELECT n WHERE n LIKE 'ali%'")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bin, ok := stmt.Where.(*ast.BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", stmt.Where)
	}
	if bin.Op != "LIKE" {
		t.Errorf("op: got %q, want %q", bin.Op, "LIKE")
	}
}

func TestIlikeExpression(t *testing.T) {
	p := New("SELECT n WHERE n ILIKE 'alice'")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bin, ok := stmt.Where.(*ast.BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", stmt.Where)
	}
	if bin.Op != "ILIKE" {
		t.Errorf("op: got %q, want %q", bin.Op, "ILIKE")
	}
}

func TestConcatOperator(t *testing.T) {
	p := New("SELECT first || ' ' || last AS full_name")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should parse as (first || ' ') || last
	bin, ok := stmt.Columns[0].Expr.(*ast.BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", stmt.Columns[0].Expr)
	}
	if bin.Op != "||" {
		t.Errorf("top op: got %q, want %q", bin.Op, "||")
	}
}

func TestUnaryMinus(t *testing.T) {
	p := New("SELECT -x")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ue, ok := stmt.Columns[0].Expr.(*ast.UnaryExpr)
	if !ok {
		t.Fatalf("expected UnaryExpr, got %T", stmt.Columns[0].Expr)
	}
	if ue.Op != "-" {
		t.Errorf("op: got %q, want %q", ue.Op, "-")
	}
}

func TestNullLiteral(t *testing.T) {
	p := New("SELECT NULL")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := stmt.Columns[0].Expr.(*ast.NullLiteral); !ok {
		t.Errorf("expected NullLiteral, got %T", stmt.Columns[0].Expr)
	}
}

func TestBoolLiterals(t *testing.T) {
	p := New("SELECT TRUE, FALSE")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	b1, ok := stmt.Columns[0].Expr.(*ast.BoolLiteral)
	if !ok || !b1.Value {
		t.Error("expected TRUE")
	}
	b2, ok := stmt.Columns[1].Expr.(*ast.BoolLiteral)
	if !ok || b2.Value {
		t.Error("expected FALSE")
	}
}

func TestNullIfFunction(t *testing.T) {
	p := New("SELECT NULLIF(a, b)")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fc, ok := stmt.Columns[0].Expr.(*ast.FunctionCall)
	if !ok {
		t.Fatalf("expected FunctionCall, got %T", stmt.Columns[0].Expr)
	}
	if fc.Name != "NULLIF" || len(fc.Args) != 2 {
		t.Errorf("expected NULLIF(2 args), got %s(%d)", fc.Name, len(fc.Args))
	}
}

func TestGroupByWithOrdinals(t *testing.T) {
	p := New("SELECT a, b, COUNT(*) FROM 'kafka://b/t' GROUP BY 1, 2")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(stmt.GroupBy) != 2 {
		t.Fatalf("expected 2 GROUP BY, got %d", len(stmt.GroupBy))
	}
	n, ok := stmt.GroupBy[0].(*ast.NumberLiteral)
	if !ok {
		t.Fatalf("expected NumberLiteral, got %T", stmt.GroupBy[0])
	}
	if n.Value != "1" {
		t.Errorf("ordinal: got %q, want %q", n.Value, "1")
	}
}

func TestJoinClause(t *testing.T) {
	p := New("SELECT e.user_id, u.name FROM stdin e JOIN '/tmp/users.ndjson' u ON e.user_id = u.id")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// FROM
	if stmt.From == nil {
		t.Fatal("expected FROM clause")
	}
	if stmt.FromAlias != "e" {
		t.Errorf("from alias: got %q, want %q", stmt.FromAlias, "e")
	}

	// JOIN
	if stmt.Join == nil {
		t.Fatal("expected JOIN clause")
	}
	if stmt.Join.Type != "JOIN" {
		t.Errorf("join type: got %q, want %q", stmt.Join.Type, "JOIN")
	}
	if stmt.Join.Source.URI != "/tmp/users.ndjson" {
		t.Errorf("join source: got %q, want %q", stmt.Join.Source.URI, "/tmp/users.ndjson")
	}
	if stmt.Join.Alias != "u" {
		t.Errorf("join alias: got %q, want %q", stmt.Join.Alias, "u")
	}
	if stmt.Join.Condition == nil {
		t.Fatal("expected ON condition")
	}

	// Check columns are QualifiedRefs
	col0, ok := stmt.Columns[0].Expr.(*ast.QualifiedRef)
	if !ok {
		t.Fatalf("expected QualifiedRef, got %T", stmt.Columns[0].Expr)
	}
	if col0.Qualifier != "e" || col0.Name != "user_id" {
		t.Errorf("column 0: got %s.%s, want e.user_id", col0.Qualifier, col0.Name)
	}
	col1, ok := stmt.Columns[1].Expr.(*ast.QualifiedRef)
	if !ok {
		t.Fatalf("expected QualifiedRef, got %T", stmt.Columns[1].Expr)
	}
	if col1.Qualifier != "u" || col1.Name != "name" {
		t.Errorf("column 1: got %s.%s, want u.name", col1.Qualifier, col1.Name)
	}

	// ON condition
	bin, ok := stmt.Join.Condition.(*ast.BinaryExpr)
	if !ok || bin.Op != "=" {
		t.Fatalf("expected = condition, got %T", stmt.Join.Condition)
	}
}

func TestLeftJoinClause(t *testing.T) {
	p := New("SELECT e.user_id, u.name FROM stdin e LEFT JOIN '/tmp/users.ndjson' u ON e.user_id = u.id")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stmt.Join == nil {
		t.Fatal("expected JOIN clause")
	}
	if stmt.Join.Type != "LEFT JOIN" {
		t.Errorf("join type: got %q, want %q", stmt.Join.Type, "LEFT JOIN")
	}
}

func TestJoinWithWhereAndGroupBy(t *testing.T) {
	p := New("SELECT u.name, COUNT(*) AS cnt FROM stdin e JOIN '/tmp/users.ndjson' u ON e.user_id = u.id WHERE e.action = 'login' GROUP BY u.name")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stmt.Join == nil {
		t.Fatal("expected JOIN clause")
	}
	if stmt.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	if stmt.GroupBy == nil {
		t.Fatal("expected GROUP BY clause")
	}
}

func TestQualifiedColumnRef(t *testing.T) {
	p := New("SELECT t.name")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ref, ok := stmt.Columns[0].Expr.(*ast.QualifiedRef)
	if !ok {
		t.Fatalf("expected QualifiedRef, got %T", stmt.Columns[0].Expr)
	}
	if ref.Qualifier != "t" || ref.Name != "name" {
		t.Errorf("got %s.%s, want t.name", ref.Qualifier, ref.Name)
	}
}

func TestJoinWithFormat(t *testing.T) {
	p := New("SELECT e.user_id FROM stdin e JOIN '/tmp/data.csv' FORMAT CSV u ON e.id = u.id")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stmt.Join == nil {
		t.Fatal("expected JOIN clause")
	}
	if stmt.Join.Source.Format != "CSV" {
		t.Errorf("join format: got %q, want %q", stmt.Join.Source.Format, "CSV")
	}
}

func TestSeedFromClause(t *testing.T) {
	p := New("SELECT region, COUNT(*) AS orders FROM 'kafka://broker/orders.cdc' FORMAT DEBEZIUM SEED FROM '/path/to/snapshot.parquet' GROUP BY region")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if stmt.Seed == nil {
		t.Fatal("expected SEED clause")
	}
	if stmt.Seed.Source.URI != "/path/to/snapshot.parquet" {
		t.Errorf("seed source URI: got %q, want %q", stmt.Seed.Source.URI, "/path/to/snapshot.parquet")
	}
}

func TestSeedFromWithFormat(t *testing.T) {
	p := New("SELECT status, COUNT(*) FROM stdin SEED FROM '/tmp/seed.csv' FORMAT CSV GROUP BY status")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if stmt.Seed == nil {
		t.Fatal("expected SEED clause")
	}
	if stmt.Seed.Source.URI != "/tmp/seed.csv" {
		t.Errorf("seed source URI: got %q, want %q", stmt.Seed.Source.URI, "/tmp/seed.csv")
	}
	if stmt.Seed.Source.Format != "CSV" {
		t.Errorf("seed format: got %q, want %q", stmt.Seed.Source.Format, "CSV")
	}
}

func TestSeedFromWithJoin(t *testing.T) {
	p := New("SELECT e.status, COUNT(*) FROM stdin e JOIN '/tmp/users.ndjson' u ON e.uid = u.id SEED FROM '/tmp/seed.ndjson' GROUP BY e.status")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if stmt.Join == nil {
		t.Fatal("expected JOIN clause")
	}
	if stmt.Seed == nil {
		t.Fatal("expected SEED clause")
	}
	if stmt.Seed.Source.URI != "/tmp/seed.ndjson" {
		t.Errorf("seed source URI: got %q, want %q", stmt.Seed.Source.URI, "/tmp/seed.ndjson")
	}
}

func TestJoinWithinInterval(t *testing.T) {
	p := New("SELECT o.order_id, p.payment_id FROM 'kafka://broker/orders' o JOIN 'kafka://broker/payments' p ON o.order_id = p.order_id WITHIN INTERVAL '10 minutes'")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if stmt.Join == nil {
		t.Fatal("expected JOIN clause")
	}
	if stmt.Join.Within == nil {
		t.Fatal("expected WITHIN clause on JOIN")
	}
	if *stmt.Join.Within != "10 minutes" {
		t.Errorf("within: got %q, want %q", *stmt.Join.Within, "10 minutes")
	}
	if stmt.Join.Source.URI != "kafka://broker/payments" {
		t.Errorf("join source: got %q, want %q", stmt.Join.Source.URI, "kafka://broker/payments")
	}
}

func TestJoinWithoutWithin(t *testing.T) {
	// Without WITHIN, the clause should parse fine (Within is nil)
	p := New("SELECT * FROM stdin o JOIN '/tmp/users.ndjson' u ON o.id = u.id")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stmt.Join.Within != nil {
		t.Errorf("expected nil Within for file join, got %q", *stmt.Join.Within)
	}
}

func TestJoinWithinWithGroupBy(t *testing.T) {
	p := New("SELECT o.order_id, COUNT(*) FROM 'kafka://broker/orders' o JOIN 'kafka://broker/payments' p ON o.order_id = p.order_id WITHIN INTERVAL '5 minutes' GROUP BY o.order_id")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stmt.Join == nil || stmt.Join.Within == nil {
		t.Fatal("expected JOIN with WITHIN clause")
	}
	if *stmt.Join.Within != "5 minutes" {
		t.Errorf("within: got %q, want %q", *stmt.Join.Within, "5 minutes")
	}
	if stmt.GroupBy == nil {
		t.Fatal("expected GROUP BY clause")
	}
}

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
