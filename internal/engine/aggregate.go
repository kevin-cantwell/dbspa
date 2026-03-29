package engine

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/kevin-cantwell/folddb/internal/sql/ast"
)

// AggColumn describes one column in the SELECT list of an accumulating query.
// It is either a GROUP BY key reference or an aggregate function call.
type AggColumn struct {
	// Alias is the output column name.
	Alias string
	// Expr is the AST expression (used for GROUP BY key evaluation).
	Expr ast.Expr
	// IsAggregate is true if this column is an aggregate function.
	IsAggregate bool
	// AggFunc is the uppercase aggregate function name (COUNT, SUM, etc.).
	AggFunc string
	// AggArg is the argument expression to the aggregate (nil for COUNT(*)).
	AggArg ast.Expr
	// IsStar is true for COUNT(*).
	IsStar bool
	// GroupByIdx is the index into the GROUP BY key slice for non-aggregate columns.
	// -1 if not applicable (aggregate columns).
	GroupByIdx int
}

// AggregateOp is the stateful aggregation operator.
// It maintains a hash map of group keys to accumulator slices.
type AggregateOp struct {
	// GroupByExprs are the expressions used for GROUP BY.
	GroupByExprs []ast.Expr
	// Columns describes each output column.
	Columns []AggColumn
	// Having is the optional HAVING clause expression.
	Having ast.Expr

	// groups maps composite key string -> group state.
	groups map[string]*groupState
	// keyOrder tracks insertion order of keys.
	keyOrder []string
}

type groupState struct {
	accumulators []Accumulator
	keyValues    []Value      // the GROUP BY key values for this group
	lastEmitted  []Value      // last emitted result values (for retraction)
	hasEmitted   bool         // whether we have emitted a result for this group
}

// NewAggregateOp creates a new AggregateOp from the query's SELECT columns and GROUP BY.
func NewAggregateOp(columns []AggColumn, groupBy []ast.Expr, having ast.Expr) *AggregateOp {
	return &AggregateOp{
		GroupByExprs: groupBy,
		Columns:      columns,
		Having:       having,
		groups:       make(map[string]*groupState),
	}
}

// Process reads records from in, applies aggregation, and sends changelog
// records to out. It closes out when in is exhausted.
func (op *AggregateOp) Process(in <-chan Record, out chan<- Record) error {
	defer close(out)

	for rec := range in {
		op.processRecord(rec, out)
	}
	return nil
}

func (op *AggregateOp) processRecord(rec Record, out chan<- Record) {
	// Evaluate GROUP BY key
	keyVals := make([]Value, len(op.GroupByExprs))
	for i, expr := range op.GroupByExprs {
		val, err := Eval(expr, rec)
		if err != nil {
			// Skip malformed records
			continue
		}
		keyVals[i] = val
	}

	keyStr := compositeKey(keyVals)

	// Get or create group
	gs, exists := op.groups[keyStr]
	if !exists {
		gs = &groupState{
			accumulators: op.newAccumulators(),
			keyValues:    keyVals,
		}
		op.groups[keyStr] = gs
		op.keyOrder = append(op.keyOrder, keyStr)
	}

	// Apply Add or Retract to each accumulator
	for i, col := range op.Columns {
		if !col.IsAggregate {
			continue
		}
		var argVal Value
		if col.IsStar {
			argVal = IntValue{V: 1} // placeholder for COUNT(*)
		} else {
			var err error
			argVal, err = Eval(col.AggArg, rec)
			if err != nil {
				argVal = NullValue{}
			}
		}

		if rec.Diff >= 0 {
			gs.accumulators[i].Add(argVal)
		} else {
			gs.accumulators[i].Retract(argVal)
		}
	}

	// Check if any accumulator changed
	anyChanged := false
	for i, col := range op.Columns {
		if col.IsAggregate && gs.accumulators[i].HasChanged() {
			anyChanged = true
		}
	}

	if !anyChanged {
		// Reset changed flags
		for i, col := range op.Columns {
			if col.IsAggregate {
				gs.accumulators[i].ResetChanged()
			}
		}
		return
	}

	// Check if this group's COUNT(*) hit 0 — if so, remove the group
	countZero := op.isGroupEmpty(gs)

	// Build current result record
	resultCols := op.buildResult(gs)

	// Apply HAVING filter if present
	havingPass := true
	if op.Having != nil && !countZero {
		havingResult, err := op.evalHaving(op.Having, gs, rec)
		if err != nil {
			havingPass = false
		} else if havingResult.IsNull() {
			havingPass = false
		} else if b, ok := havingResult.(BoolValue); ok {
			havingPass = b.V
		} else {
			havingPass = false
		}
	}

	// Emit retraction for previous result (if one was emitted)
	if gs.hasEmitted {
		retractCols := make(map[string]Value, len(gs.lastEmitted))
		for i, col := range op.Columns {
			retractCols[col.Alias] = gs.lastEmitted[i]
		}
		out <- Record{
			Columns:   retractCols,
			Timestamp: rec.Timestamp,
			Diff:      -1,
		}
	}

	if countZero {
		// Remove the group — do not emit a new insertion
		delete(op.groups, keyStr)
		for i, k := range op.keyOrder {
			if k == keyStr {
				op.keyOrder = append(op.keyOrder[:i], op.keyOrder[i+1:]...)
				break
			}
		}
	} else if havingPass {
		// Emit new result
		resultVals := make([]Value, len(op.Columns))
		for i, col := range op.Columns {
			resultVals[i] = resultCols[col.Alias]
		}
		gs.lastEmitted = resultVals
		gs.hasEmitted = true

		out <- Record{
			Columns:   resultCols,
			Timestamp: rec.Timestamp,
			Diff:      1,
		}
	} else {
		// HAVING failed — this group is suppressed (no emission)
		gs.hasEmitted = false
		gs.lastEmitted = nil
	}

	// Reset changed flags
	for i, col := range op.Columns {
		if col.IsAggregate {
			gs.accumulators[i].ResetChanged()
		}
	}
}

// evalHaving evaluates a HAVING expression, resolving aggregate function calls
// to their accumulator results.
func (op *AggregateOp) evalHaving(expr ast.Expr, gs *groupState, rec Record) (Value, error) {
	switch e := expr.(type) {
	case *ast.FunctionCall:
		if IsAggregateFunc(e.Name) {
			// Find the matching accumulator
			funcName := strings.ToUpper(e.Name)
			for i, col := range op.Columns {
				if col.IsAggregate && col.AggFunc == funcName {
					// Check if the arguments match
					if col.IsStar && len(e.Args) == 1 {
						if _, ok := e.Args[0].(*ast.StarExpr); ok {
							return gs.accumulators[i].Result(), nil
						}
					}
					if !col.IsStar && col.AggArg != nil && len(e.Args) == 1 {
						// Simple match: same function name and we assume args match
						// (for v0 this is sufficient since HAVING typically references
						// the same aggregates as the SELECT list)
						return gs.accumulators[i].Result(), nil
					}
				}
			}
			// Aggregate not found in SELECT list — this is an error for v0
			return NullValue{}, fmt.Errorf("HAVING references aggregate %s not in SELECT list", e.Name)
		}
		// Non-aggregate function — evaluate normally
		return Eval(expr, rec)

	case *ast.BinaryExpr:
		left, err := op.evalHaving(e.Left, gs, rec)
		if err != nil {
			return nil, err
		}
		right, err := op.evalHaving(e.Right, gs, rec)
		if err != nil {
			return nil, err
		}
		// Build a synthetic record with these values and evaluate the binary op
		syntheticRec := Record{
			Columns: map[string]Value{"_left": left, "_right": right},
		}
		syntheticExpr := &ast.BinaryExpr{
			Left:  &ast.ColumnRef{Name: "_left"},
			Op:    e.Op,
			Right: &ast.ColumnRef{Name: "_right"},
		}
		return Eval(syntheticExpr, syntheticRec)

	case *ast.UnaryExpr:
		val, err := op.evalHaving(e.Expr, gs, rec)
		if err != nil {
			return nil, err
		}
		syntheticRec := Record{
			Columns: map[string]Value{"_val": val},
		}
		syntheticExpr := &ast.UnaryExpr{
			Op:   e.Op,
			Expr: &ast.ColumnRef{Name: "_val"},
		}
		return Eval(syntheticExpr, syntheticRec)

	default:
		// For literals, column refs, etc. — evaluate normally
		return Eval(expr, rec)
	}
}

// isGroupEmpty checks if the group should be considered empty.
// We look for a CountStarAccumulator with count 0.
func (op *AggregateOp) isGroupEmpty(gs *groupState) bool {
	for i, col := range op.Columns {
		if col.IsAggregate && col.IsStar && col.AggFunc == "COUNT" {
			if v, ok := gs.accumulators[i].Result().(IntValue); ok && v.V == 0 {
				return true
			}
		}
	}
	return false
}

// buildResult constructs the output column map for a group.
func (op *AggregateOp) buildResult(gs *groupState) map[string]Value {
	cols := make(map[string]Value, len(op.Columns))
	for i, col := range op.Columns {
		if col.IsAggregate {
			cols[col.Alias] = gs.accumulators[i].Result()
		} else {
			// GROUP BY key column — use the mapped index
			idx := col.GroupByIdx
			if idx >= 0 && idx < len(gs.keyValues) {
				cols[col.Alias] = gs.keyValues[idx]
			} else {
				cols[col.Alias] = NullValue{}
			}
		}
	}
	return cols
}

// newAccumulators creates a fresh set of accumulators matching the column spec.
func (op *AggregateOp) newAccumulators() []Accumulator {
	accs := make([]Accumulator, len(op.Columns))
	for i, col := range op.Columns {
		if !col.IsAggregate {
			// Placeholder — GROUP BY key columns don't need accumulators.
			// We use a nil accumulator and skip it during processing.
			accs[i] = &noopAccumulator{}
			continue
		}
		accs[i] = newAccumulator(col.AggFunc, col.IsStar)
	}
	return accs
}

// newAccumulator creates the appropriate accumulator for a function name.
func newAccumulator(funcName string, isStar bool) Accumulator {
	switch strings.ToUpper(funcName) {
	case "COUNT":
		if isStar {
			return &CountStarAccumulator{}
		}
		return &CountAccumulator{}
	case "SUM":
		return &SumAccumulator{}
	case "AVG":
		return &AvgAccumulator{}
	case "MIN":
		return &MinAccumulator{}
	case "MAX":
		return &MaxAccumulator{}
	case "FIRST":
		return &FirstAccumulator{}
	case "LAST":
		return &LastAccumulator{}
	default:
		return &CountStarAccumulator{} // fallback
	}
}

// compositeKey produces a deterministic string key from a slice of Values.
func compositeKey(vals []Value) string {
	parts := make([]any, len(vals))
	for i, v := range vals {
		if v.IsNull() {
			parts[i] = nil
		} else {
			parts[i] = v.ToJSON()
		}
	}
	data, _ := json.Marshal(parts)
	return string(data)
}

// noopAccumulator is a placeholder for non-aggregate columns in the accumulator slice.
type noopAccumulator struct{}

func (a *noopAccumulator) Add(_ Value)              {}
func (a *noopAccumulator) Retract(_ Value)           {}
func (a *noopAccumulator) Result() Value             { return NullValue{} }
func (a *noopAccumulator) HasChanged() bool          { return false }
func (a *noopAccumulator) ResetChanged()             {}
func (a *noopAccumulator) CanMerge() bool            { return false }
func (a *noopAccumulator) Merge(_ Accumulator)       {}
func (a *noopAccumulator) Marshal() ([]byte, error)  { return nil, nil }
func (a *noopAccumulator) Unmarshal(_ []byte) error  { return nil }

// ParseAggColumns analyzes the SELECT list and GROUP BY expressions
// to produce AggColumn descriptors and validate the query structure.
func ParseAggColumns(columns []ast.Column, groupBy []ast.Expr) ([]AggColumn, error) {
	var result []AggColumn

	for i, col := range columns {
		alias := col.Alias
		if alias == "" {
			alias = inferColumnName(col.Expr)
		}
		if alias == "" {
			alias = fmt.Sprintf("col%d", i+1)
		}

		fc, isFunc := col.Expr.(*ast.FunctionCall)
		if isFunc && IsAggregateFunc(fc.Name) {
			ac := AggColumn{
				Alias:       alias,
				Expr:        col.Expr,
				IsAggregate: true,
				AggFunc:     strings.ToUpper(fc.Name),
				GroupByIdx:  -1,
			}
			if len(fc.Args) == 1 {
				if _, ok := fc.Args[0].(*ast.StarExpr); ok {
					ac.IsStar = true
				} else {
					ac.AggArg = fc.Args[0]
				}
			} else if len(fc.Args) == 0 {
				// e.g., COUNT() — treat like COUNT(*)
				ac.IsStar = true
			}
			result = append(result, ac)
		} else {
			// Non-aggregate — must be in GROUP BY
			gbIdx := findGroupByIndex(col.Expr, groupBy)
			if gbIdx < 0 {
				return nil, fmt.Errorf("column %q must appear in GROUP BY clause or be used in an aggregate function", alias)
			}
			result = append(result, AggColumn{
				Alias:      alias,
				Expr:       col.Expr,
				GroupByIdx: gbIdx,
			})
		}
	}

	return result, nil
}

// findGroupByIndex returns the index in groupBy that matches the given expression,
// or -1 if not found.
func findGroupByIndex(expr ast.Expr, groupBy []ast.Expr) int {
	for i, gb := range groupBy {
		if exprEqual(expr, gb) {
			return i
		}
	}
	return -1
}

// exprEqual checks if two AST expressions are structurally equal.
func exprEqual(a, b ast.Expr) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	switch av := a.(type) {
	case *ast.ColumnRef:
		bv, ok := b.(*ast.ColumnRef)
		return ok && av.Name == bv.Name
	case *ast.NumberLiteral:
		bv, ok := b.(*ast.NumberLiteral)
		return ok && av.Value == bv.Value && av.IsFloat == bv.IsFloat
	case *ast.StringLiteral:
		bv, ok := b.(*ast.StringLiteral)
		return ok && av.Value == bv.Value
	case *ast.BinaryExpr:
		bv, ok := b.(*ast.BinaryExpr)
		return ok && av.Op == bv.Op && exprEqual(av.Left, bv.Left) && exprEqual(av.Right, bv.Right)
	case *ast.FunctionCall:
		bv, ok := b.(*ast.FunctionCall)
		if !ok || av.Name != bv.Name || len(av.Args) != len(bv.Args) {
			return false
		}
		for i := range av.Args {
			if !exprEqual(av.Args[i], bv.Args[i]) {
				return false
			}
		}
		return true
	case *ast.StarExpr:
		_, ok := b.(*ast.StarExpr)
		return ok
	default:
		return fmt.Sprintf("%#v", a) == fmt.Sprintf("%#v", b)
	}
}

// IsAggregateFunc returns true if the function name is a known aggregate.
func IsAggregateFunc(name string) bool {
	switch strings.ToUpper(name) {
	case "COUNT", "SUM", "AVG", "MIN", "MAX", "MEDIAN", "FIRST", "LAST",
		"ARRAY_AGG", "APPROX_COUNT_DISTINCT":
		return true
	}
	return false
}
