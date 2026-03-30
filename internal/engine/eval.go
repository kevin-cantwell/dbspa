package engine

import (
	"fmt"
	"math"
	"strings"

	"github.com/kevin-cantwell/folddb/internal/sql/ast"
)

// Eval evaluates an AST expression against a record and returns the resulting value.
func Eval(expr ast.Expr, rec Record) (Value, error) {
	switch e := expr.(type) {
	case *ast.ColumnRef:
		return rec.Get(e.Name), nil

	case *ast.QualifiedRef:
		// Try qualified name first: "qualifier.name"
		qualName := e.Qualifier + "." + e.Name
		if v, ok := rec.Columns[qualName]; ok {
			return v, nil
		}
		// Fall back to unqualified name
		return rec.Get(e.Name), nil

	case *ast.NumberLiteral:
		return parseNumber(e)

	case *ast.StringLiteral:
		return TextValue{V: e.Value}, nil

	case *ast.BoolLiteral:
		return BoolValue{V: e.Value}, nil

	case *ast.NullLiteral:
		return NullValue{}, nil

	case *ast.StarExpr:
		// Star in expression context returns the whole record as JSON
		m := make(map[string]any, len(rec.Columns))
		for k, v := range rec.Columns {
			m[k] = v.ToJSON()
		}
		return JsonValue{V: m}, nil

	case *ast.UnaryExpr:
		return evalUnary(e, rec)

	case *ast.BinaryExpr:
		return evalBinary(e, rec)

	case *ast.JsonAccessExpr:
		return evalJsonAccess(e, rec)

	case *ast.CastExpr:
		val, err := Eval(e.Expr, rec)
		if err != nil {
			return nil, err
		}
		return CastValue(val, e.TypeName)

	case *ast.FunctionCall:
		return evalFunction(e, rec)

	case *ast.BetweenExpr:
		return evalBetween(e, rec)

	case *ast.InExpr:
		return evalIn(e, rec)

	case *ast.IsExpr:
		return evalIs(e, rec)

	case *ast.CaseExpr:
		return evalCase(e, rec)

	case *ast.IntervalLiteral:
		return TextValue{V: e.Value}, nil

	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

func parseNumber(e *ast.NumberLiteral) (Value, error) {
	if e.IsFloat {
		f, err := fmt.Sscanf(e.Value, "%f")
		_ = f
		var v float64
		_, err = fmt.Sscanf(e.Value, "%f", &v)
		if err != nil {
			return nil, fmt.Errorf("invalid float: %s", e.Value)
		}
		return FloatValue{V: v}, nil
	}
	var v int64
	_, err := fmt.Sscanf(e.Value, "%d", &v)
	if err != nil {
		return nil, fmt.Errorf("invalid integer: %s", e.Value)
	}
	return IntValue{V: v}, nil
}

func evalUnary(e *ast.UnaryExpr, rec Record) (Value, error) {
	val, err := Eval(e.Expr, rec)
	if err != nil {
		return nil, err
	}

	switch e.Op {
	case "-":
		if val.IsNull() {
			return NullValue{}, nil
		}
		switch v := val.(type) {
		case IntValue:
			return IntValue{V: -v.V}, nil
		case FloatValue:
			return FloatValue{V: -v.V}, nil
		default:
			return nil, fmt.Errorf("cannot negate %s", val.Type())
		}

	case "+":
		return val, nil

	case "NOT":
		if val.IsNull() {
			return NullValue{}, nil
		}
		b, ok := val.(BoolValue)
		if !ok {
			return nil, fmt.Errorf("NOT requires BOOL, got %s", val.Type())
		}
		return BoolValue{V: !b.V}, nil

	default:
		return nil, fmt.Errorf("unknown unary operator: %s", e.Op)
	}
}

func evalBinary(e *ast.BinaryExpr, rec Record) (Value, error) {
	left, err := Eval(e.Left, rec)
	if err != nil {
		return nil, err
	}
	right, err := Eval(e.Right, rec)
	if err != nil {
		return nil, err
	}

	// Short-circuit for AND/OR with three-valued logic
	switch e.Op {
	case "AND":
		return evalAnd(left, right)
	case "OR":
		return evalOr(left, right)
	}

	// NULL propagation for all other operators
	if left.IsNull() || right.IsNull() {
		return NullValue{}, nil
	}

	switch e.Op {
	case "+", "-", "*", "/", "%":
		return evalArithmetic(e.Op, left, right)
	case "=", "!=", "<", ">", "<=", ">=":
		return evalComparison(e.Op, left, right)
	case "||":
		return evalConcat(left, right)
	case "LIKE":
		return evalLike(left, right, false)
	case "ILIKE":
		return evalLike(left, right, true)
	default:
		return nil, fmt.Errorf("unknown binary operator: %s", e.Op)
	}
}

func evalAnd(left, right Value) (Value, error) {
	lb := toBool(left)
	rb := toBool(right)

	// FALSE AND anything = FALSE
	if lb != nil && !*lb {
		return BoolValue{V: false}, nil
	}
	if rb != nil && !*rb {
		return BoolValue{V: false}, nil
	}
	// TRUE AND TRUE = TRUE
	if lb != nil && *lb && rb != nil && *rb {
		return BoolValue{V: true}, nil
	}
	// Otherwise NULL
	return NullValue{}, nil
}

func evalOr(left, right Value) (Value, error) {
	lb := toBool(left)
	rb := toBool(right)

	// TRUE OR anything = TRUE
	if lb != nil && *lb {
		return BoolValue{V: true}, nil
	}
	if rb != nil && *rb {
		return BoolValue{V: true}, nil
	}
	// FALSE OR FALSE = FALSE
	if lb != nil && !*lb && rb != nil && !*rb {
		return BoolValue{V: false}, nil
	}
	// Otherwise NULL
	return NullValue{}, nil
}

func toBool(v Value) *bool {
	if v.IsNull() {
		return nil
	}
	if b, ok := v.(BoolValue); ok {
		val := b.V
		return &val
	}
	return nil
}

func evalArithmetic(op string, left, right Value) (Value, error) {
	// Promote to float if either side is float
	lf, lok := toFloat(left)
	rf, rok := toFloat(right)
	if !lok || !rok {
		return nil, fmt.Errorf("cannot perform arithmetic on %s and %s", left.Type(), right.Type())
	}

	_, leftIsFloat := left.(FloatValue)
	_, rightIsFloat := right.(FloatValue)
	useFloat := leftIsFloat || rightIsFloat

	switch op {
	case "+":
		if useFloat {
			return FloatValue{V: lf + rf}, nil
		}
		return IntValue{V: int64(lf) + int64(rf)}, nil
	case "-":
		if useFloat {
			return FloatValue{V: lf - rf}, nil
		}
		return IntValue{V: int64(lf) - int64(rf)}, nil
	case "*":
		if useFloat {
			return FloatValue{V: lf * rf}, nil
		}
		return IntValue{V: int64(lf) * int64(rf)}, nil
	case "/":
		if rf == 0 {
			if useFloat {
				if lf == 0 {
					return FloatValue{V: math.NaN()}, nil
				}
				if lf > 0 {
					return FloatValue{V: math.Inf(1)}, nil
				}
				return FloatValue{V: math.Inf(-1)}, nil
			}
			// Integer division by zero returns NULL per Postgres semantics
			return NullValue{}, nil
		}
		if useFloat {
			return FloatValue{V: lf / rf}, nil
		}
		return IntValue{V: int64(lf) / int64(rf)}, nil
	case "%":
		if rf == 0 {
			return NullValue{}, nil
		}
		if useFloat {
			return FloatValue{V: math.Mod(lf, rf)}, nil
		}
		return IntValue{V: int64(lf) % int64(rf)}, nil
	default:
		return nil, fmt.Errorf("unknown arithmetic operator: %s", op)
	}
}

func toFloat(v Value) (float64, bool) {
	switch val := v.(type) {
	case IntValue:
		return float64(val.V), true
	case FloatValue:
		return val.V, true
	default:
		return 0, false
	}
}

func evalComparison(op string, left, right Value) (Value, error) {
	cmp, err := compareValues(left, right)
	if err != nil {
		return nil, err
	}

	switch op {
	case "=":
		return BoolValue{V: cmp == 0}, nil
	case "!=":
		return BoolValue{V: cmp != 0}, nil
	case "<":
		return BoolValue{V: cmp < 0}, nil
	case ">":
		return BoolValue{V: cmp > 0}, nil
	case "<=":
		return BoolValue{V: cmp <= 0}, nil
	case ">=":
		return BoolValue{V: cmp >= 0}, nil
	default:
		return nil, fmt.Errorf("unknown comparison operator: %s", op)
	}
}

// compareValues returns -1, 0, or 1 for ordering.
func compareValues(left, right Value) (int, error) {
	// Same-type comparisons
	switch l := left.(type) {
	case IntValue:
		switch r := right.(type) {
		case IntValue:
			return cmpInt(l.V, r.V), nil
		case FloatValue:
			return cmpFloat(float64(l.V), r.V), nil
		}
	case FloatValue:
		switch r := right.(type) {
		case FloatValue:
			return cmpFloat(l.V, r.V), nil
		case IntValue:
			return cmpFloat(l.V, float64(r.V)), nil
		}
	case TextValue:
		if r, ok := right.(TextValue); ok {
			return strings.Compare(l.V, r.V), nil
		}
	case BoolValue:
		if r, ok := right.(BoolValue); ok {
			return cmpBool(l.V, r.V), nil
		}
	case TimestampValue:
		if r, ok := right.(TimestampValue); ok {
			if l.V.Before(r.V) {
				return -1, nil
			}
			if l.V.After(r.V) {
				return 1, nil
			}
			return 0, nil
		}
	}

	return 0, fmt.Errorf("cannot compare %s and %s", left.Type(), right.Type())
}

func cmpInt(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func cmpFloat(a, b float64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func cmpBool(a, b bool) int {
	if a == b {
		return 0
	}
	if !a {
		return -1
	}
	return 1
}

func evalConcat(left, right Value) (Value, error) {
	// || implicitly casts to text
	ls := left.String()
	rs := right.String()
	return TextValue{V: ls + rs}, nil
}

func evalLike(left, right Value, caseInsensitive bool) (Value, error) {
	l, ok := left.(TextValue)
	if !ok {
		return nil, fmt.Errorf("LIKE requires TEXT, got %s", left.Type())
	}
	r, ok := right.(TextValue)
	if !ok {
		return nil, fmt.Errorf("LIKE pattern must be TEXT, got %s", right.Type())
	}

	pattern := r.V
	text := l.V
	if caseInsensitive {
		pattern = strings.ToLower(pattern)
		text = strings.ToLower(text)
	}

	matched := matchLike(text, pattern)
	return BoolValue{V: matched}, nil
}

// matchLike implements SQL LIKE pattern matching with % and _ wildcards.
func matchLike(text, pattern string) bool {
	ti, pi := 0, 0
	starTi, starPi := -1, -1

	for ti < len(text) {
		if pi < len(pattern) && (pattern[pi] == '_' || pattern[pi] == text[ti]) {
			ti++
			pi++
		} else if pi < len(pattern) && pattern[pi] == '%' {
			starTi = ti
			starPi = pi
			pi++
		} else if starPi >= 0 {
			starTi++
			ti = starTi
			pi = starPi + 1
		} else {
			return false
		}
	}
	for pi < len(pattern) && pattern[pi] == '%' {
		pi++
	}
	return pi == len(pattern)
}

func evalJsonAccess(e *ast.JsonAccessExpr, rec Record) (Value, error) {
	left, err := Eval(e.Left, rec)
	if err != nil {
		return nil, err
	}

	if left.IsNull() {
		return NullValue{}, nil
	}

	key, err := Eval(e.Key, rec)
	if err != nil {
		return nil, err
	}

	jv, ok := left.(JsonValue)
	if !ok {
		return nil, fmt.Errorf("-> operator requires JSON, got %s", left.Type())
	}

	var result any

	switch k := key.(type) {
	case TextValue:
		obj, ok := jv.V.(map[string]any)
		if !ok {
			return NullValue{}, nil
		}
		result, ok = obj[k.V]
		if !ok {
			return NullValue{}, nil
		}
	case IntValue:
		arr, ok := jv.V.([]any)
		if !ok {
			return NullValue{}, nil
		}
		idx := int(k.V)
		if idx < 0 || idx >= len(arr) {
			return NullValue{}, nil
		}
		result = arr[idx]
	default:
		return nil, fmt.Errorf("JSON key must be TEXT or INT, got %s", key.Type())
	}

	if result == nil {
		return NullValue{}, nil
	}

	if e.AsText {
		// ->> returns text
		switch v := result.(type) {
		case string:
			return TextValue{V: v}, nil
		case float64:
			// Format without trailing zeros for integers
			if v == float64(int64(v)) {
				return TextValue{V: fmt.Sprintf("%d", int64(v))}, nil
			}
			return TextValue{V: fmt.Sprintf("%g", v)}, nil
		case bool:
			return TextValue{V: fmt.Sprintf("%t", v)}, nil
		default:
			// Object or array — serialize to JSON string
			return TextValue{V: fmt.Sprintf("%v", v)}, nil
		}
	}

	// -> returns JSON
	return JsonValue{V: result}, nil
}

func evalFunction(e *ast.FunctionCall, rec Record) (Value, error) {
	name := strings.ToUpper(e.Name)

	switch name {
	case "COALESCE":
		for _, arg := range e.Args {
			val, err := Eval(arg, rec)
			if err != nil {
				return nil, err
			}
			if !val.IsNull() {
				return val, nil
			}
		}
		return NullValue{}, nil

	case "NULLIF":
		if len(e.Args) != 2 {
			return nil, fmt.Errorf("NULLIF requires 2 arguments, got %d", len(e.Args))
		}
		a, err := Eval(e.Args[0], rec)
		if err != nil {
			return nil, err
		}
		b, err := Eval(e.Args[1], rec)
		if err != nil {
			return nil, err
		}
		if !a.IsNull() && !b.IsNull() {
			cmp, err := compareValues(a, b)
			if err == nil && cmp == 0 {
				return NullValue{}, nil
			}
		}
		return a, nil

	case "LENGTH":
		if len(e.Args) != 1 {
			return nil, fmt.Errorf("LENGTH requires 1 argument, got %d", len(e.Args))
		}
		val, err := Eval(e.Args[0], rec)
		if err != nil {
			return nil, err
		}
		if val.IsNull() {
			return NullValue{}, nil
		}
		s, ok := val.(TextValue)
		if !ok {
			return nil, fmt.Errorf("LENGTH requires TEXT, got %s", val.Type())
		}
		return IntValue{V: int64(len([]rune(s.V)))}, nil

	case "UPPER":
		return evalStringFunc1(e, rec, strings.ToUpper)
	case "LOWER":
		return evalStringFunc1(e, rec, strings.ToLower)
	case "TRIM":
		return evalStringFunc1(e, rec, strings.TrimSpace)
	case "LTRIM":
		return evalStringFunc1(e, rec, func(s string) string { return strings.TrimLeft(s, " \t\n\r") })
	case "RTRIM":
		return evalStringFunc1(e, rec, func(s string) string { return strings.TrimRight(s, " \t\n\r") })

	case "NOW":
		return TimestampValue{V: rec.Timestamp}, nil

	default:
		return nil, fmt.Errorf("unknown function: %s", name)
	}
}

func evalStringFunc1(e *ast.FunctionCall, rec Record, fn func(string) string) (Value, error) {
	if len(e.Args) != 1 {
		return nil, fmt.Errorf("%s requires 1 argument, got %d", e.Name, len(e.Args))
	}
	val, err := Eval(e.Args[0], rec)
	if err != nil {
		return nil, err
	}
	if val.IsNull() {
		return NullValue{}, nil
	}
	s, ok := val.(TextValue)
	if !ok {
		return nil, fmt.Errorf("%s requires TEXT, got %s", e.Name, val.Type())
	}
	return TextValue{V: fn(s.V)}, nil
}

func evalBetween(e *ast.BetweenExpr, rec Record) (Value, error) {
	val, err := Eval(e.Expr, rec)
	if err != nil {
		return nil, err
	}
	low, err := Eval(e.Low, rec)
	if err != nil {
		return nil, err
	}
	high, err := Eval(e.High, rec)
	if err != nil {
		return nil, err
	}

	if val.IsNull() || low.IsNull() || high.IsNull() {
		return NullValue{}, nil
	}

	cmpLow, err := compareValues(val, low)
	if err != nil {
		return nil, err
	}
	cmpHigh, err := compareValues(val, high)
	if err != nil {
		return nil, err
	}

	result := cmpLow >= 0 && cmpHigh <= 0
	if e.Not {
		result = !result
	}
	return BoolValue{V: result}, nil
}

func evalIn(e *ast.InExpr, rec Record) (Value, error) {
	val, err := Eval(e.Expr, rec)
	if err != nil {
		return nil, err
	}

	if val.IsNull() {
		return NullValue{}, nil
	}

	hasNull := false
	for _, vExpr := range e.Values {
		v, err := Eval(vExpr, rec)
		if err != nil {
			return nil, err
		}
		if v.IsNull() {
			hasNull = true
			continue
		}
		cmp, err := compareValues(val, v)
		if err != nil {
			continue
		}
		if cmp == 0 {
			if e.Not {
				return BoolValue{V: false}, nil
			}
			return BoolValue{V: true}, nil
		}
	}

	if hasNull {
		return NullValue{}, nil
	}

	if e.Not {
		return BoolValue{V: true}, nil
	}
	return BoolValue{V: false}, nil
}

func evalIs(e *ast.IsExpr, rec Record) (Value, error) {
	val, err := Eval(e.Expr, rec)
	if err != nil {
		return nil, err
	}

	var result bool
	switch e.What {
	case "NULL":
		result = val.IsNull()
	case "TRUE":
		if b, ok := val.(BoolValue); ok {
			result = b.V
		}
	case "FALSE":
		if b, ok := val.(BoolValue); ok {
			result = !b.V
		}
	case "DISTINCT FROM":
		from, err := Eval(e.From, rec)
		if err != nil {
			return nil, err
		}
		if val.IsNull() && from.IsNull() {
			result = false // NULL IS NOT DISTINCT FROM NULL
		} else if val.IsNull() || from.IsNull() {
			result = true // one is NULL, the other is not
		} else {
			cmp, err := compareValues(val, from)
			if err != nil {
				return nil, err
			}
			result = cmp != 0
		}
	default:
		return nil, fmt.Errorf("unknown IS predicate: %s", e.What)
	}

	if e.Not {
		result = !result
	}
	return BoolValue{V: result}, nil
}

func evalCase(e *ast.CaseExpr, rec Record) (Value, error) {
	for _, when := range e.Whens {
		cond, err := Eval(when.Condition, rec)
		if err != nil {
			return nil, err
		}
		if b, ok := cond.(BoolValue); ok && b.V {
			return Eval(when.Result, rec)
		}
	}
	if e.Else != nil {
		return Eval(e.Else, rec)
	}
	return NullValue{}, nil
}
