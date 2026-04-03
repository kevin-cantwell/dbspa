package engine

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/kevin-cantwell/dbspa/internal/sql/ast"
)

func makeRec(cols map[string]Value) Record {
	return Record{
		Columns:   cols,
		Timestamp: time.Now(),
		Weight:      1,
	}
}

// --- Arithmetic Tests ---

func TestArithmeticIntInt(t *testing.T) {
	tests := []struct {
		op   string
		a, b int64
		want int64
	}{
		{"+", 10, 3, 13},
		{"-", 10, 3, 7},
		{"*", 10, 3, 30},
		{"/", 10, 3, 3}, // integer division
		{"%", 10, 3, 1},
	}
	rec := makeRec(nil)
	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			expr := &ast.BinaryExpr{
				Left:  &ast.NumberLiteral{Value: intStr(tt.a)},
				Op:    tt.op,
				Right: &ast.NumberLiteral{Value: intStr(tt.b)},
			}
			val, err := Eval(expr, rec)
			if err != nil {
				t.Fatalf("error: %v", err)
			}
			iv, ok := val.(IntValue)
			if !ok {
				t.Fatalf("expected IntValue, got %T (%v)", val, val)
			}
			if iv.V != tt.want {
				t.Errorf("got %d, want %d", iv.V, tt.want)
			}
		})
	}
}

func TestArithmeticIntFloat(t *testing.T) {
	rec := makeRec(nil)
	expr := &ast.BinaryExpr{
		Left:  &ast.NumberLiteral{Value: "5"},
		Op:    "/",
		Right: &ast.NumberLiteral{Value: "2.0", IsFloat: true},
	}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	fv, ok := val.(FloatValue)
	if !ok {
		t.Fatalf("expected FloatValue, got %T", val)
	}
	if fv.V != 2.5 {
		t.Errorf("got %f, want 2.5", fv.V)
	}
}

func TestArithmeticFloatFloat(t *testing.T) {
	rec := makeRec(nil)
	expr := &ast.BinaryExpr{
		Left:  &ast.NumberLiteral{Value: "3.5", IsFloat: true},
		Op:    "+",
		Right: &ast.NumberLiteral{Value: "1.5", IsFloat: true},
	}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	fv := val.(FloatValue)
	if fv.V != 5.0 {
		t.Errorf("got %f, want 5.0", fv.V)
	}
}

// --- Comparison Tests ---

func TestComparisons(t *testing.T) {
	tests := []struct {
		name string
		op   string
		l, r Value
		want bool
	}{
		{"int eq true", "=", IntValue{5}, IntValue{5}, true},
		{"int eq false", "=", IntValue{5}, IntValue{6}, false},
		{"int neq", "!=", IntValue{5}, IntValue{6}, true},
		{"int lt", "<", IntValue{3}, IntValue{5}, true},
		{"int gt", ">", IntValue{5}, IntValue{3}, true},
		{"int lteq", "<=", IntValue{5}, IntValue{5}, true},
		{"int gteq", ">=", IntValue{5}, IntValue{4}, true},
		{"float eq", "=", FloatValue{1.5}, FloatValue{1.5}, true},
		{"text eq", "=", TextValue{"abc"}, TextValue{"abc"}, true},
		{"text lt", "<", TextValue{"abc"}, TextValue{"def"}, true},
		{"bool eq", "=", BoolValue{true}, BoolValue{true}, true},
		{"int float cross", "=", IntValue{5}, FloatValue{5.0}, true},
		{"int float lt", "<", IntValue{5}, FloatValue{5.5}, true},
	}
	rec := makeRec(nil)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build expression using column refs
			rec := makeRec(map[string]Value{"l": tt.l, "r": tt.r})
			expr := &ast.BinaryExpr{
				Left:  &ast.ColumnRef{Name: "l"},
				Op:    tt.op,
				Right: &ast.ColumnRef{Name: "r"},
			}
			val, err := Eval(expr, rec)
			_ = rec
			if err != nil {
				t.Fatalf("error: %v", err)
			}
			bv, ok := val.(BoolValue)
			if !ok {
				t.Fatalf("expected BoolValue, got %T", val)
			}
			if bv.V != tt.want {
				t.Errorf("got %v, want %v", bv.V, tt.want)
			}
		})
	}
	_ = rec
}

// --- NULL Propagation Tests ---

func TestNullPropagation(t *testing.T) {
	rec := makeRec(map[string]Value{"a": IntValue{5}, "b": NullValue{}})

	tests := []struct {
		name string
		expr ast.Expr
	}{
		{"NULL + 1", &ast.BinaryExpr{Left: &ast.ColumnRef{Name: "b"}, Op: "+", Right: &ast.NumberLiteral{Value: "1"}}},
		{"1 + NULL", &ast.BinaryExpr{Left: &ast.NumberLiteral{Value: "1"}, Op: "+", Right: &ast.ColumnRef{Name: "b"}}},
		{"NULL = NULL", &ast.BinaryExpr{Left: &ast.NullLiteral{}, Op: "=", Right: &ast.NullLiteral{}}},
		{"NULL != 5", &ast.BinaryExpr{Left: &ast.NullLiteral{}, Op: "!=", Right: &ast.NumberLiteral{Value: "5"}}},
		{"NULL < 5", &ast.BinaryExpr{Left: &ast.NullLiteral{}, Op: "<", Right: &ast.NumberLiteral{Value: "5"}}},
		{"NULL || 'x'", &ast.BinaryExpr{Left: &ast.NullLiteral{}, Op: "||", Right: &ast.StringLiteral{Value: "x"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := Eval(tt.expr, rec)
			if err != nil {
				t.Fatalf("error: %v", err)
			}
			if !val.IsNull() {
				t.Errorf("expected NULL, got %v", val)
			}
		})
	}
}

// --- Three-Valued Logic ---

func TestThreeValuedLogicAND(t *testing.T) {
	T := BoolValue{V: true}
	F := BoolValue{V: false}
	N := NullValue{}

	tests := []struct {
		name string
		l, r Value
		want string // "true", "false", "null"
	}{
		{"T AND T", T, T, "true"},
		{"T AND F", T, F, "false"},
		{"T AND N", T, N, "null"},
		{"F AND T", F, T, "false"},
		{"F AND F", F, F, "false"},
		{"F AND N", F, N, "false"},
		{"N AND T", N, T, "null"},
		{"N AND F", N, F, "false"},
		{"N AND N", N, N, "null"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := evalAnd(tt.l, tt.r)
			if err != nil {
				t.Fatalf("error: %v", err)
			}
			got := valToStr(val)
			if got != tt.want {
				t.Errorf("got %s, want %s", got, tt.want)
			}
		})
	}
}

func TestThreeValuedLogicOR(t *testing.T) {
	T := BoolValue{V: true}
	F := BoolValue{V: false}
	N := NullValue{}

	tests := []struct {
		name string
		l, r Value
		want string
	}{
		{"T OR T", T, T, "true"},
		{"T OR F", T, F, "true"},
		{"T OR N", T, N, "true"},
		{"F OR T", F, T, "true"},
		{"F OR F", F, F, "false"},
		{"F OR N", F, N, "null"},
		{"N OR T", N, T, "true"},
		{"N OR F", N, F, "null"},
		{"N OR N", N, N, "null"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := evalOr(tt.l, tt.r)
			if err != nil {
				t.Fatalf("error: %v", err)
			}
			got := valToStr(val)
			if got != tt.want {
				t.Errorf("got %s, want %s", got, tt.want)
			}
		})
	}
}

func TestNOTNull(t *testing.T) {
	rec := makeRec(nil)
	expr := &ast.UnaryExpr{Op: "NOT", Expr: &ast.NullLiteral{}}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !val.IsNull() {
		t.Errorf("NOT NULL should be NULL, got %v", val)
	}
}

// --- JSON Access Tests ---

func TestJsonAccessObject(t *testing.T) {
	rec := makeRec(map[string]Value{
		"obj": JsonValue{V: map[string]any{"name": "alice", "age": float64(30)}},
	})
	// obj->>'name'
	expr := &ast.JsonAccessExpr{
		Left:   &ast.ColumnRef{Name: "obj"},
		Key:    &ast.StringLiteral{Value: "name"},
		AsText: true,
	}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	tv, ok := val.(TextValue)
	if !ok {
		t.Fatalf("expected TextValue, got %T", val)
	}
	if tv.V != "alice" {
		t.Errorf("got %q, want %q", tv.V, "alice")
	}
}

func TestJsonAccessArray(t *testing.T) {
	rec := makeRec(map[string]Value{
		"arr": JsonValue{V: []any{float64(10), float64(20), float64(30)}},
	})
	// arr->0
	expr := &ast.JsonAccessExpr{
		Left:   &ast.ColumnRef{Name: "arr"},
		Key:    &ast.NumberLiteral{Value: "0"},
		AsText: false,
	}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	jv, ok := val.(JsonValue)
	if !ok {
		t.Fatalf("expected JsonValue, got %T", val)
	}
	if jv.V != float64(10) {
		t.Errorf("got %v, want 10", jv.V)
	}
}

func TestJsonAccessOnNull(t *testing.T) {
	rec := makeRec(map[string]Value{"x": NullValue{}})
	expr := &ast.JsonAccessExpr{
		Left:   &ast.ColumnRef{Name: "x"},
		Key:    &ast.StringLiteral{Value: "key"},
		AsText: false,
	}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !val.IsNull() {
		t.Errorf("expected NULL, got %v", val)
	}
}

func TestJsonAccessNonexistentKey(t *testing.T) {
	rec := makeRec(map[string]Value{
		"obj": JsonValue{V: map[string]any{"a": float64(1)}},
	})
	expr := &ast.JsonAccessExpr{
		Left:   &ast.ColumnRef{Name: "obj"},
		Key:    &ast.StringLiteral{Value: "nonexistent"},
		AsText: true,
	}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !val.IsNull() {
		t.Errorf("expected NULL for nonexistent key, got %v", val)
	}
}

func TestJsonAccessNullValue(t *testing.T) {
	// obj->>'key' where the JSON value is null
	rec := makeRec(map[string]Value{
		"obj": JsonValue{V: map[string]any{"key": nil}},
	})
	expr := &ast.JsonAccessExpr{
		Left:   &ast.ColumnRef{Name: "obj"},
		Key:    &ast.StringLiteral{Value: "key"},
		AsText: true,
	}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !val.IsNull() {
		t.Errorf("expected NULL for JSON null value, got %v", val)
	}
}

// --- Type Casts ---

func TestTypeCasts(t *testing.T) {
	tests := []struct {
		name     string
		val      Value
		typeName string
		wantType string
		wantStr  string
	}{
		{"text to int", TextValue{V: "42"}, "INT", "INT", "42"},
		{"text to float", TextValue{V: "3.14"}, "FLOAT", "FLOAT", "3.14"},
		{"int to text", IntValue{V: 42}, "TEXT", "TEXT", "42"},
		{"int to float", IntValue{V: 5}, "FLOAT", "FLOAT", "5"},
		{"float to int", FloatValue{V: 3.7}, "INT", "INT", "3"},
		{"text to bool true", TextValue{V: "true"}, "BOOLEAN", "BOOL", "true"},
		{"text to bool false", TextValue{V: "false"}, "BOOLEAN", "BOOL", "false"},
		{"int to bool nonzero", IntValue{V: 1}, "BOOLEAN", "BOOL", "true"},
		{"int to bool zero", IntValue{V: 0}, "BOOLEAN", "BOOL", "false"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := CastValue(tt.val, tt.typeName)
			if err != nil {
				t.Fatalf("error: %v", err)
			}
			if result.Type() != tt.wantType {
				t.Errorf("type: got %q, want %q", result.Type(), tt.wantType)
			}
			if result.String() != tt.wantStr {
				t.Errorf("string: got %q, want %q", result.String(), tt.wantStr)
			}
		})
	}
}

func TestCastNullReturnsNull(t *testing.T) {
	types := []string{"INT", "FLOAT", "TEXT", "BOOLEAN"}
	for _, typ := range types {
		result, err := CastValue(NullValue{}, typ)
		if err != nil {
			t.Fatalf("error casting NULL to %s: %v", typ, err)
		}
		if !result.IsNull() {
			t.Errorf("NULL::%s should be NULL, got %v", typ, result)
		}
	}
}

func TestCastInvalidText(t *testing.T) {
	_, err := CastValue(TextValue{V: "not_a_number"}, "INT")
	if err == nil {
		t.Error("expected error casting 'not_a_number' to INT")
	}
}

// --- LIKE and ILIKE ---

func TestLike(t *testing.T) {
	tests := []struct {
		text    string
		pattern string
		want    bool
	}{
		{"alice", "ali%", true},
		{"alicia", "ali%", true},
		{"bob", "ali%", false},
		{"alice", "a_ice", true},
		{"alice", "alice", true},
		{"alice", "%", true},
		{"", "%", true},
		{"abc", "a%c", true},
		{"abc", "a_c", true},
		{"abdc", "a_c", false},
	}
	rec := makeRec(nil)
	for _, tt := range tests {
		t.Run(tt.text+"~"+tt.pattern, func(t *testing.T) {
			expr := &ast.BinaryExpr{
				Left:  &ast.StringLiteral{Value: tt.text},
				Op:    "LIKE",
				Right: &ast.StringLiteral{Value: tt.pattern},
			}
			val, err := Eval(expr, rec)
			if err != nil {
				t.Fatalf("error: %v", err)
			}
			bv := val.(BoolValue)
			if bv.V != tt.want {
				t.Errorf("got %v, want %v", bv.V, tt.want)
			}
		})
	}
}

func TestIlike(t *testing.T) {
	rec := makeRec(nil)
	expr := &ast.BinaryExpr{
		Left:  &ast.StringLiteral{Value: "Alice"},
		Op:    "ILIKE",
		Right: &ast.StringLiteral{Value: "alice"},
	}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !val.(BoolValue).V {
		t.Error("ILIKE should match case-insensitively")
	}
}

// --- BETWEEN and IN ---

func TestBetween(t *testing.T) {
	rec := makeRec(nil)
	tests := []struct {
		val      int64
		low      int64
		high     int64
		not      bool
		expected bool
	}{
		{5, 3, 8, false, true},
		{1, 3, 8, false, false},
		{3, 3, 8, false, true},
		{8, 3, 8, false, true},
		{5, 3, 8, true, false},
		{1, 3, 8, true, true},
	}
	for _, tt := range tests {
		expr := &ast.BetweenExpr{
			Expr: &ast.NumberLiteral{Value: intStr(tt.val)},
			Low:  &ast.NumberLiteral{Value: intStr(tt.low)},
			High: &ast.NumberLiteral{Value: intStr(tt.high)},
			Not:  tt.not,
		}
		val, err := Eval(expr, rec)
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		bv := val.(BoolValue)
		if bv.V != tt.expected {
			t.Errorf("BETWEEN(%d, %d, %d, not=%v): got %v, want %v", tt.val, tt.low, tt.high, tt.not, bv.V, tt.expected)
		}
	}
}

func TestBetweenWithNull(t *testing.T) {
	rec := makeRec(nil)
	expr := &ast.BetweenExpr{
		Expr: &ast.NullLiteral{},
		Low:  &ast.NumberLiteral{Value: "1"},
		High: &ast.NumberLiteral{Value: "10"},
	}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !val.IsNull() {
		t.Errorf("NULL BETWEEN should be NULL, got %v", val)
	}
}

func TestIn(t *testing.T) {
	rec := makeRec(nil)
	expr := &ast.InExpr{
		Expr: &ast.StringLiteral{Value: "a"},
		Values: []ast.Expr{
			&ast.StringLiteral{Value: "a"},
			&ast.StringLiteral{Value: "c"},
		},
	}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !val.(BoolValue).V {
		t.Error("expected 'a' IN ('a','c') to be true")
	}
}

func TestInNotFound(t *testing.T) {
	rec := makeRec(nil)
	expr := &ast.InExpr{
		Expr: &ast.StringLiteral{Value: "b"},
		Values: []ast.Expr{
			&ast.StringLiteral{Value: "a"},
			&ast.StringLiteral{Value: "c"},
		},
	}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if val.(BoolValue).V {
		t.Error("expected 'b' IN ('a','c') to be false")
	}
}

func TestInWithNull(t *testing.T) {
	rec := makeRec(nil)
	// 'b' IN ('a', NULL) -> NULL (no match, but NULL present)
	expr := &ast.InExpr{
		Expr: &ast.StringLiteral{Value: "b"},
		Values: []ast.Expr{
			&ast.StringLiteral{Value: "a"},
			&ast.NullLiteral{},
		},
	}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !val.IsNull() {
		t.Errorf("expected NULL, got %v", val)
	}
}

func TestInNullExpr(t *testing.T) {
	rec := makeRec(nil)
	expr := &ast.InExpr{
		Expr:   &ast.NullLiteral{},
		Values: []ast.Expr{&ast.NumberLiteral{Value: "1"}},
	}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !val.IsNull() {
		t.Errorf("NULL IN (...) should be NULL, got %v", val)
	}
}

// --- IS NULL, IS NOT NULL ---

func TestIsNull(t *testing.T) {
	rec := makeRec(map[string]Value{"x": NullValue{}})
	expr := &ast.IsExpr{Expr: &ast.ColumnRef{Name: "x"}, What: "NULL"}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !val.(BoolValue).V {
		t.Error("NULL IS NULL should be true")
	}
}

func TestIsNotNull(t *testing.T) {
	rec := makeRec(map[string]Value{"x": IntValue{V: 5}})
	expr := &ast.IsExpr{Expr: &ast.ColumnRef{Name: "x"}, What: "NULL", Not: true}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !val.(BoolValue).V {
		t.Error("5 IS NOT NULL should be true")
	}
}

// --- IS DISTINCT FROM ---

func TestIsDistinctFrom(t *testing.T) {
	rec := makeRec(nil)
	tests := []struct {
		name string
		l, r ast.Expr
		not  bool
		want bool
	}{
		{"NULL DISTINCT NULL", &ast.NullLiteral{}, &ast.NullLiteral{}, false, false},
		{"NULL NOT DISTINCT NULL", &ast.NullLiteral{}, &ast.NullLiteral{}, true, true},
		{"1 DISTINCT NULL", &ast.NumberLiteral{Value: "1"}, &ast.NullLiteral{}, false, true},
		{"NULL DISTINCT 1", &ast.NullLiteral{}, &ast.NumberLiteral{Value: "1"}, false, true},
		{"1 DISTINCT 1", &ast.NumberLiteral{Value: "1"}, &ast.NumberLiteral{Value: "1"}, false, false},
		{"1 DISTINCT 2", &ast.NumberLiteral{Value: "1"}, &ast.NumberLiteral{Value: "2"}, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := &ast.IsExpr{Expr: tt.l, What: "DISTINCT FROM", Not: tt.not, From: tt.r}
			val, err := Eval(expr, rec)
			if err != nil {
				t.Fatalf("error: %v", err)
			}
			bv := val.(BoolValue)
			if bv.V != tt.want {
				t.Errorf("got %v, want %v", bv.V, tt.want)
			}
		})
	}
}

// --- Division by Zero ---

func TestDivisionByZeroInt(t *testing.T) {
	rec := makeRec(nil)
	expr := &ast.BinaryExpr{
		Left:  &ast.NumberLiteral{Value: "10"},
		Op:    "/",
		Right: &ast.NumberLiteral{Value: "0"},
	}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !val.IsNull() {
		t.Errorf("integer div by zero should be NULL, got %v", val)
	}
}

func TestDivisionByZeroFloatPositive(t *testing.T) {
	rec := makeRec(nil)
	expr := &ast.BinaryExpr{
		Left:  &ast.NumberLiteral{Value: "10.0", IsFloat: true},
		Op:    "/",
		Right: &ast.NumberLiteral{Value: "0.0", IsFloat: true},
	}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	fv := val.(FloatValue)
	if !math.IsInf(fv.V, 1) {
		t.Errorf("expected +Inf, got %v", fv.V)
	}
}

func TestDivisionByZeroFloatNegative(t *testing.T) {
	rec := makeRec(map[string]Value{"a": FloatValue{V: -10.0}, "b": FloatValue{V: 0.0}})
	expr2 := &ast.BinaryExpr{
		Left:  &ast.ColumnRef{Name: "a"},
		Op:    "/",
		Right: &ast.ColumnRef{Name: "b"},
	}
	val, err := Eval(expr2, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	fv := val.(FloatValue)
	if !math.IsInf(fv.V, -1) {
		t.Errorf("expected -Inf, got %v", fv.V)
	}
}

func TestDivisionByZeroFloatNaN(t *testing.T) {
	rec := makeRec(map[string]Value{"a": FloatValue{V: 0.0}, "b": FloatValue{V: 0.0}})
	expr := &ast.BinaryExpr{
		Left:  &ast.ColumnRef{Name: "a"},
		Op:    "/",
		Right: &ast.ColumnRef{Name: "b"},
	}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	fv := val.(FloatValue)
	if !math.IsNaN(fv.V) {
		t.Errorf("expected NaN, got %v", fv.V)
	}
}

// --- String Concat ---

func TestStringConcat(t *testing.T) {
	rec := makeRec(map[string]Value{
		"first": TextValue{V: "Alice"},
		"last":  TextValue{V: "Smith"},
	})
	expr := &ast.BinaryExpr{
		Left: &ast.BinaryExpr{
			Left:  &ast.ColumnRef{Name: "first"},
			Op:    "||",
			Right: &ast.StringLiteral{Value: " "},
		},
		Op:    "||",
		Right: &ast.ColumnRef{Name: "last"},
	}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	tv := val.(TextValue)
	if tv.V != "Alice Smith" {
		t.Errorf("got %q, want %q", tv.V, "Alice Smith")
	}
}

// --- CASE WHEN ---

func TestCaseWhen(t *testing.T) {
	rec := makeRec(map[string]Value{"x": IntValue{V: 5}})
	expr := &ast.CaseExpr{
		Whens: []ast.CaseWhen{
			{
				Condition: &ast.BinaryExpr{Left: &ast.ColumnRef{Name: "x"}, Op: ">", Right: &ast.NumberLiteral{Value: "3"}},
				Result:    &ast.StringLiteral{Value: "big"},
			},
		},
		Else: &ast.StringLiteral{Value: "small"},
	}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	tv := val.(TextValue)
	if tv.V != "big" {
		t.Errorf("got %q, want %q", tv.V, "big")
	}
}

func TestCaseWhenElse(t *testing.T) {
	rec := makeRec(map[string]Value{"x": IntValue{V: 1}})
	expr := &ast.CaseExpr{
		Whens: []ast.CaseWhen{
			{
				Condition: &ast.BinaryExpr{Left: &ast.ColumnRef{Name: "x"}, Op: ">", Right: &ast.NumberLiteral{Value: "3"}},
				Result:    &ast.StringLiteral{Value: "big"},
			},
		},
		Else: &ast.StringLiteral{Value: "small"},
	}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	tv := val.(TextValue)
	if tv.V != "small" {
		t.Errorf("got %q, want %q", tv.V, "small")
	}
}

func TestCaseWhenNoElse(t *testing.T) {
	rec := makeRec(map[string]Value{"x": IntValue{V: 1}})
	expr := &ast.CaseExpr{
		Whens: []ast.CaseWhen{
			{
				Condition: &ast.BinaryExpr{Left: &ast.ColumnRef{Name: "x"}, Op: ">", Right: &ast.NumberLiteral{Value: "3"}},
				Result:    &ast.StringLiteral{Value: "big"},
			},
		},
	}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !val.IsNull() {
		t.Errorf("CASE with no match and no ELSE should return NULL, got %v", val)
	}
}

// --- COALESCE and NULLIF ---

func TestCoalesce(t *testing.T) {
	rec := makeRec(map[string]Value{"a": NullValue{}, "b": NullValue{}, "c": IntValue{V: 42}})
	expr := &ast.FunctionCall{
		Name: "COALESCE",
		Args: []ast.Expr{
			&ast.ColumnRef{Name: "a"},
			&ast.ColumnRef{Name: "b"},
			&ast.ColumnRef{Name: "c"},
		},
	}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	iv := val.(IntValue)
	if iv.V != 42 {
		t.Errorf("got %d, want 42", iv.V)
	}
}

func TestCoalesceAllNull(t *testing.T) {
	rec := makeRec(map[string]Value{"a": NullValue{}, "b": NullValue{}})
	expr := &ast.FunctionCall{
		Name: "COALESCE",
		Args: []ast.Expr{&ast.ColumnRef{Name: "a"}, &ast.ColumnRef{Name: "b"}},
	}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !val.IsNull() {
		t.Errorf("COALESCE(NULL, NULL) should be NULL, got %v", val)
	}
}

func TestNullIf(t *testing.T) {
	rec := makeRec(map[string]Value{"a": IntValue{V: 5}, "b": IntValue{V: 5}})
	expr := &ast.FunctionCall{
		Name: "NULLIF",
		Args: []ast.Expr{&ast.ColumnRef{Name: "a"}, &ast.ColumnRef{Name: "b"}},
	}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !val.IsNull() {
		t.Errorf("NULLIF(5,5) should be NULL, got %v", val)
	}
}

func TestNullIfDifferent(t *testing.T) {
	rec := makeRec(map[string]Value{"a": IntValue{V: 5}, "b": IntValue{V: 3}})
	expr := &ast.FunctionCall{
		Name: "NULLIF",
		Args: []ast.Expr{&ast.ColumnRef{Name: "a"}, &ast.ColumnRef{Name: "b"}},
	}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	iv := val.(IntValue)
	if iv.V != 5 {
		t.Errorf("NULLIF(5,3) should be 5, got %d", iv.V)
	}
}

// --- String Functions ---

func TestStringFunctions(t *testing.T) {
	tests := []struct {
		name string
		fn   string
		arg  string
		want string
	}{
		{"LENGTH", "LENGTH", "hello", "5"},
		{"UPPER", "UPPER", "hello", "HELLO"},
		{"LOWER", "LOWER", "HELLO", "hello"},
		{"TRIM", "TRIM", "  hello  ", "hello"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := makeRec(nil)
			expr := &ast.FunctionCall{
				Name: tt.fn,
				Args: []ast.Expr{&ast.StringLiteral{Value: tt.arg}},
			}
			val, err := Eval(expr, rec)
			if err != nil {
				t.Fatalf("error: %v", err)
			}
			if val.String() != tt.want {
				t.Errorf("got %q, want %q", val.String(), tt.want)
			}
		})
	}
}

func TestStringFunctionsOnNull(t *testing.T) {
	rec := makeRec(map[string]Value{"x": NullValue{}})
	for _, fn := range []string{"UPPER", "LOWER", "TRIM", "LENGTH"} {
		expr := &ast.FunctionCall{
			Name: fn,
			Args: []ast.Expr{&ast.ColumnRef{Name: "x"}},
		}
		val, err := Eval(expr, rec)
		if err != nil {
			t.Fatalf("%s(NULL) error: %v", fn, err)
		}
		if !val.IsNull() {
			t.Errorf("%s(NULL) should be NULL, got %v", fn, val)
		}
	}
}

// --- Modulo by zero ---

func TestModuloByZero(t *testing.T) {
	rec := makeRec(nil)
	expr := &ast.BinaryExpr{
		Left:  &ast.NumberLiteral{Value: "10"},
		Op:    "%",
		Right: &ast.NumberLiteral{Value: "0"},
	}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !val.IsNull() {
		t.Errorf("10 %% 0 should be NULL, got %v", val)
	}
}

// --- Unary minus ---

func TestUnaryMinus(t *testing.T) {
	rec := makeRec(map[string]Value{"x": IntValue{V: 5}})
	expr := &ast.UnaryExpr{Op: "-", Expr: &ast.ColumnRef{Name: "x"}}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if val.(IntValue).V != -5 {
		t.Errorf("got %v, want -5", val)
	}
}

func TestUnaryMinusNull(t *testing.T) {
	rec := makeRec(nil)
	expr := &ast.UnaryExpr{Op: "-", Expr: &ast.NullLiteral{}}
	val, err := Eval(expr, rec)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !val.IsNull() {
		t.Errorf("-(NULL) should be NULL, got %v", val)
	}
}

// --- Helpers ---

func intStr(v int64) string {
	return fmt.Sprintf("%d", v)
}

func valToStr(v Value) string {
	if v.IsNull() {
		return "null"
	}
	if b, ok := v.(BoolValue); ok {
		if b.V {
			return "true"
		}
		return "false"
	}
	return v.String()
}
