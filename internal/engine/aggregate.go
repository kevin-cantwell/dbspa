package engine

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/kevin-cantwell/dbspa/internal/sql/ast"
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

	// mu protects groups and keyOrder for concurrent checkpoint saves.
	mu sync.RWMutex
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

// ProcessBatch applies aggregation to all records in a batch, emitting
// results to out.
//
// Phase 3 compaction: after exact-duplicate compaction, partition by GROUP BY
// key and process each group's sub-batch together, emitting one
// retraction+insertion per group per batch instead of one per record. For
// low-cardinality GROUP BY (e.g. 3 status values, batch of 1024) this
// reduces map lookups and changelog emissions by ~300x.
func (op *AggregateOp) ProcessBatch(batch Batch, out chan<- Record) {
	compacted := CompactBatch(batch)
	if len(op.GroupByExprs) == 0 {
		// No GROUP BY — single implicit group, use per-record path.
		for _, rec := range compacted {
			op.processRecord(rec, out)
		}
		return
	}
	groups := GroupByKey(compacted, op.GroupByExprs)
	// If every record landed in its own group (high-cardinality GROUP BY),
	// GroupByKey added overhead with no batching benefit. Fall back to the
	// per-record path to avoid the sub-batch allocation cost.
	if len(groups) == len(compacted) {
		for _, rec := range compacted {
			op.processRecord(rec, out)
		}
		return
	}
	for _, recs := range groups {
		op.processGroupBatch(recs, out)
	}
}

// ProcessBatches reads batches from in, applies aggregation to each batch,
// and sends changelog records to out. It closes out when in is exhausted.
func (op *AggregateOp) ProcessBatches(in <-chan Batch, out chan<- Record) error {
	defer close(out)

	for batch := range in {
		op.ProcessBatch(batch, out)
	}
	return nil
}

// ImportInitialState loads pre-computed group states from seed records.
// Each record's columns must match the GROUP BY keys + aggregate output aliases.
// For example, seed row {"region":"us-east","sum_amount":1000000} sets the
// SUM accumulator for group "us-east" to 1000000.
//
// Seed records are NOT processed through the Add/Retract pipeline — they set
// accumulator state directly via SetInitial. This avoids double-counting when
// the seed comes from a pre-aggregated source (e.g., BigQuery GROUP BY).
//
// Each seeded group's initial state is emitted to out so downstream sinks
// (TUI, changelog) show the data immediately.
func (op *AggregateOp) ImportInitialState(records []Record, out chan<- Record) {
	op.mu.Lock()
	defer op.mu.Unlock()

	for _, rec := range records {
		// Extract GROUP BY key values from the seed record by alias name
		keyVals := make([]Value, len(op.GroupByExprs))
		for _, col := range op.Columns {
			if col.IsAggregate {
				continue
			}
			// This is a GROUP BY key column — look it up by alias in the seed record
			if val, ok := rec.Columns[col.Alias]; ok {
				if col.GroupByIdx >= 0 && col.GroupByIdx < len(keyVals) {
					keyVals[col.GroupByIdx] = val
				}
			}
		}

		keyStr := compositeKey(keyVals)

		// Create the group (seed should not overlap with existing groups)
		gs := &groupState{
			accumulators: op.newAccumulators(),
			keyValues:    keyVals,
		}

		// Set each aggregate accumulator's initial value from the seed record
		for i, col := range op.Columns {
			if !col.IsAggregate {
				continue
			}
			seedVal, ok := rec.Columns[col.Alias]
			if !ok || seedVal == nil || seedVal.IsNull() {
				log.Printf("Warning: seed record missing column %q for group %s, accumulator starts at zero", col.Alias, keyStr)
				continue
			}
			gs.accumulators[i].SetInitial(seedVal)
			gs.accumulators[i].ResetChanged()
		}

		op.groups[keyStr] = gs
		op.keyOrder = append(op.keyOrder, keyStr)

		// Build and emit the initial result so TUI shows seeded data immediately
		resultCols := op.buildResult(gs)
		resultVals := make([]Value, len(op.Columns))
		for i, col := range op.Columns {
			resultVals[i] = resultCols[col.Alias]
		}
		gs.lastEmitted = resultVals
		gs.hasEmitted = true

		out <- Record{
			Columns:   resultCols,
			Timestamp: rec.Timestamp,
			Weight:    1,
		}
	}
}

// processGroupBatch processes all records belonging to a single GROUP BY key
// in one shot, emitting at most one retraction+insertion pair. This is Phase 3:
// all accumulator updates for a group happen before any emission, so a batch
// of N records with the same key produces one output record, not N.
func (op *AggregateOp) processGroupBatch(recs Batch, out chan<- Record) {
	if len(recs) == 0 {
		return
	}

	// Compute GROUP BY key from the first record (all records in this sub-batch
	// share the same key by construction from GroupByKey).
	keyVals := make([]Value, len(op.GroupByExprs))
	for i, expr := range op.GroupByExprs {
		if val, err := Eval(expr, recs[0]); err == nil {
			keyVals[i] = val
		}
	}
	keyStr := compositeKey(keyVals)

	op.mu.Lock()
	defer op.mu.Unlock()

	existed := true
	gs, ok := op.groups[keyStr]
	if !ok {
		existed = false
		gs = &groupState{
			accumulators: op.newAccumulators(),
			keyValues:    keyVals,
		}
		op.groups[keyStr] = gs
		op.keyOrder = append(op.keyOrder, keyStr)
	}

	for _, rec := range recs {
		op.applyToAccumulators(gs, rec)
	}
	op.emitGroupState(keyStr, gs, existed, recs[len(recs)-1], out)
}

// applyToAccumulators feeds a single record's weight into each aggregate
// accumulator. Uses AddN/RetractN to avoid the per-unit loop for weights > 1.
func (op *AggregateOp) applyToAccumulators(gs *groupState, rec Record) {
	absWeight := rec.Weight
	if absWeight < 0 {
		absWeight = -absWeight
	}
	for i, col := range op.Columns {
		if !col.IsAggregate {
			continue
		}
		var argVal Value
		if col.IsStar {
			argVal = IntValue{V: 1}
		} else {
			var err error
			argVal, err = Eval(col.AggArg, rec)
			if err != nil {
				argVal = NullValue{}
			}
		}
		if rec.Weight >= 0 {
			gs.accumulators[i].AddN(argVal, absWeight)
		} else {
			gs.accumulators[i].RetractN(argVal, absWeight)
		}
	}
}

// emitGroupState checks accumulators for changes and emits a retraction of the
// previous result followed by an insertion of the new result. It also handles
// group deletion when COUNT(*) reaches zero and HAVING suppression.
func (op *AggregateOp) emitGroupState(keyStr string, gs *groupState, existed bool, lastRec Record, out chan<- Record) {
	anyChanged := false
	hasAnyAggregate := false
	for i, col := range op.Columns {
		if col.IsAggregate {
			hasAnyAggregate = true
			if gs.accumulators[i].HasChanged() {
				anyChanged = true
			}
		}
	}
	if !hasAnyAggregate {
		anyChanged = !existed
	}
	if !anyChanged {
		for i, col := range op.Columns {
			if col.IsAggregate {
				gs.accumulators[i].ResetChanged()
			}
		}
		return
	}

	countZero := op.isGroupEmpty(gs)
	resultCols := op.buildResult(gs)

	havingPass := true
	if op.Having != nil && !countZero {
		havingResult, err := op.evalHaving(op.Having, gs, lastRec)
		if err != nil || havingResult.IsNull() {
			havingPass = false
		} else if b, ok := havingResult.(BoolValue); ok {
			havingPass = b.V
		} else {
			havingPass = false
		}
	}

	if gs.hasEmitted {
		retractCols := make(map[string]Value, len(gs.lastEmitted))
		for i, col := range op.Columns {
			retractCols[col.Alias] = gs.lastEmitted[i]
		}
		out <- Record{Columns: retractCols, Timestamp: lastRec.Timestamp, Weight: -1}
	}

	if countZero {
		delete(op.groups, keyStr)
		for i, k := range op.keyOrder {
			if k == keyStr {
				op.keyOrder = append(op.keyOrder[:i], op.keyOrder[i+1:]...)
				break
			}
		}
	} else if havingPass {
		resultVals := make([]Value, len(op.Columns))
		for i, col := range op.Columns {
			resultVals[i] = resultCols[col.Alias]
		}
		gs.lastEmitted = resultVals
		gs.hasEmitted = true
		out <- Record{Columns: resultCols, Timestamp: lastRec.Timestamp, Weight: 1}
	} else {
		gs.hasEmitted = false
		gs.lastEmitted = nil
	}

	for i, col := range op.Columns {
		if col.IsAggregate {
			gs.accumulators[i].ResetChanged()
		}
	}
}

func (op *AggregateOp) processRecord(rec Record, out chan<- Record) {
	keyVals := make([]Value, len(op.GroupByExprs))
	for i, expr := range op.GroupByExprs {
		if val, err := Eval(expr, rec); err == nil {
			keyVals[i] = val
		}
	}
	keyStr := compositeKey(keyVals)

	op.mu.Lock()
	defer op.mu.Unlock()

	existed := true
	gs, ok := op.groups[keyStr]
	if !ok {
		existed = false
		gs = &groupState{
			accumulators: op.newAccumulators(),
			keyValues:    keyVals,
		}
		op.groups[keyStr] = gs
		op.keyOrder = append(op.keyOrder, keyStr)
	}

	op.applyToAccumulators(gs, rec)
	op.emitGroupState(keyStr, gs, existed, rec, out)
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

// aggColumnMeta describes a column for checkpoint validation.
type aggColumnMeta struct {
	Alias       string `json:"alias"`
	IsAggregate bool   `json:"is_aggregate"`
	AggFunc     string `json:"agg_func,omitempty"`
	IsStar      bool   `json:"is_star,omitempty"`
	GroupByIdx  int    `json:"group_by_idx,omitempty"`
}

// aggStateEnvelope is the top-level serialized format for AggregateOp state.
type aggStateEnvelope struct {
	Columns []aggColumnMeta              `json:"columns"`
	Groups  map[string]json.RawMessage   `json:"groups"`
}

// groupStateData is the serialized form of a single group's state.
type groupStateData struct {
	KeyValues    []any             `json:"key_values"`
	Accumulators []json.RawMessage `json:"accumulators"`
	LastEmitted  []any             `json:"last_emitted,omitempty"`
	HasEmitted   bool              `json:"has_emitted"`
}

// MarshalState serializes all group keys and their accumulator states.
// Safe for concurrent use with processRecord.
func (op *AggregateOp) MarshalState() ([]byte, error) {
	op.mu.RLock()
	defer op.mu.RUnlock()

	// Build column metadata for validation on restore
	colMeta := make([]aggColumnMeta, len(op.Columns))
	for i, col := range op.Columns {
		colMeta[i] = aggColumnMeta{
			Alias:       col.Alias,
			IsAggregate: col.IsAggregate,
			AggFunc:     col.AggFunc,
			IsStar:      col.IsStar,
			GroupByIdx:  col.GroupByIdx,
		}
	}

	groups := make(map[string]json.RawMessage, len(op.groups))
	for keyStr, gs := range op.groups {
		// Serialize key values
		keyVals := make([]any, len(gs.keyValues))
		for i, v := range gs.keyValues {
			if v == nil || v.IsNull() {
				keyVals[i] = nil
			} else {
				keyVals[i] = v.ToJSON()
			}
		}

		// Serialize accumulators
		accData := make([]json.RawMessage, len(gs.accumulators))
		for i, acc := range gs.accumulators {
			b, err := acc.Marshal()
			if err != nil {
				return nil, fmt.Errorf("marshal accumulator %d for key %s: %w", i, keyStr, err)
			}
			accData[i] = b
		}

		// Serialize last emitted values
		var lastEmitted []any
		if gs.hasEmitted && gs.lastEmitted != nil {
			lastEmitted = make([]any, len(gs.lastEmitted))
			for i, v := range gs.lastEmitted {
				if v == nil || v.IsNull() {
					lastEmitted[i] = nil
				} else {
					lastEmitted[i] = v.ToJSON()
				}
			}
		}

		gsd := groupStateData{
			KeyValues:    keyVals,
			Accumulators: accData,
			LastEmitted:  lastEmitted,
			HasEmitted:   gs.hasEmitted,
		}
		b, err := json.Marshal(gsd)
		if err != nil {
			return nil, fmt.Errorf("marshal group %s: %w", keyStr, err)
		}
		groups[keyStr] = b
	}

	env := aggStateEnvelope{
		Columns: colMeta,
		Groups:  groups,
	}
	return json.Marshal(env)
}

// UnmarshalState restores group keys and accumulator states.
// Must be called before processing starts (not concurrency-safe with processRecord).
func (op *AggregateOp) UnmarshalState(data []byte) error {
	var env aggStateEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return fmt.Errorf("unmarshal state envelope: %w", err)
	}

	// Validate column metadata matches current query
	if len(env.Columns) != len(op.Columns) {
		return fmt.Errorf("column count mismatch: checkpoint has %d, query has %d", len(env.Columns), len(op.Columns))
	}
	for i, cm := range env.Columns {
		col := op.Columns[i]
		if cm.Alias != col.Alias || cm.IsAggregate != col.IsAggregate || cm.AggFunc != col.AggFunc {
			return fmt.Errorf("column %d mismatch: checkpoint has %s/%s, query has %s/%s",
				i, cm.Alias, cm.AggFunc, col.Alias, col.AggFunc)
		}
	}

	// Restore groups
	op.groups = make(map[string]*groupState, len(env.Groups))
	op.keyOrder = make([]string, 0, len(env.Groups))

	for keyStr, raw := range env.Groups {
		var gsd groupStateData
		if err := json.Unmarshal(raw, &gsd); err != nil {
			return fmt.Errorf("unmarshal group %s: %w", keyStr, err)
		}

		// Restore key values
		keyVals := make([]Value, len(gsd.KeyValues))
		for i, v := range gsd.KeyValues {
			keyVals[i] = anyToValue(v)
		}

		// Create and restore accumulators
		accs := op.newAccumulators()
		if len(gsd.Accumulators) != len(accs) {
			return fmt.Errorf("accumulator count mismatch for group %s: checkpoint has %d, expected %d",
				keyStr, len(gsd.Accumulators), len(accs))
		}
		for i, accData := range gsd.Accumulators {
			if accData != nil {
				if err := accs[i].Unmarshal(accData); err != nil {
					return fmt.Errorf("unmarshal accumulator %d for group %s: %w", i, keyStr, err)
				}
			}
		}

		// Restore last emitted
		var lastEmitted []Value
		if gsd.HasEmitted && gsd.LastEmitted != nil {
			lastEmitted = make([]Value, len(gsd.LastEmitted))
			for i, v := range gsd.LastEmitted {
				lastEmitted[i] = anyToValue(v)
			}
		}

		gs := &groupState{
			accumulators: accs,
			keyValues:    keyVals,
			lastEmitted:  lastEmitted,
			hasEmitted:   gsd.HasEmitted,
		}
		op.groups[keyStr] = gs
		op.keyOrder = append(op.keyOrder, keyStr)
	}

	return nil
}

// CurrentState returns one Record per group key with current accumulator values.
// This is used to re-emit state to the sink after restoring from a checkpoint.
func (op *AggregateOp) CurrentState() []Record {
	op.mu.RLock()
	defer op.mu.RUnlock()

	var records []Record
	for _, keyStr := range op.keyOrder {
		gs, ok := op.groups[keyStr]
		if !ok {
			continue
		}
		cols := op.buildResult(gs)
		records = append(records, Record{
			Columns:   cols,
			Timestamp: time.Now(),
			Weight:      1,
		})
	}
	return records
}

// compositeKey produces a deterministic string key from a slice of Values.
func compositeKey(vals []Value) string {
	if len(vals) == 1 {
		// Fast path: single key, no allocation needed for the builder
		v := vals[0]
		if v == nil || v.IsNull() {
			return "null"
		}
		return v.String()
	}
	var b strings.Builder
	b.Grow(len(vals) * 12) // rough estimate
	for i, v := range vals {
		if i > 0 {
			b.WriteByte('|')
		}
		if v == nil || v.IsNull() {
			b.WriteString("null")
		} else {
			b.WriteString(v.String())
		}
	}
	return b.String()
}

// noopAccumulator is a placeholder for non-aggregate columns in the accumulator slice.
type noopAccumulator struct{}

func (a *noopAccumulator) Add(_ Value)               {}
func (a *noopAccumulator) Retract(_ Value)            {}
func (a *noopAccumulator) AddN(_ Value, _ int)        {}
func (a *noopAccumulator) RetractN(_ Value, _ int)    {}
func (a *noopAccumulator) Result() Value              { return NullValue{} }
func (a *noopAccumulator) HasChanged() bool           { return false }
func (a *noopAccumulator) ResetChanged()              {}
func (a *noopAccumulator) CanMerge() bool             { return false }
func (a *noopAccumulator) Merge(_ Accumulator)        {}
func (a *noopAccumulator) SetInitial(_ Value)         {}
func (a *noopAccumulator) Marshal() ([]byte, error)   { return nil, nil }
func (a *noopAccumulator) Unmarshal(_ []byte) error   { return nil }

// ParseAggColumns analyzes the SELECT list and GROUP BY expressions
// to produce AggColumn descriptors and validate the query structure.
func ParseAggColumns(columns []ast.Column, groupBy []ast.Expr) ([]AggColumn, error) {
	var result []AggColumn

	// Track aliases to detect duplicates
	seenAliases := make(map[string]int) // alias -> count

	for i, col := range columns {
		alias := col.Alias
		if alias == "" {
			alias = inferColumnName(col.Expr)
		}
		if alias == "" {
			alias = fmt.Sprintf("col%d", i+1)
		}

		// Auto-deduplicate aliases by appending _N suffix
		if count, exists := seenAliases[alias]; exists {
			newAlias := fmt.Sprintf("%s_%d", alias, count+1)
			for {
				if _, taken := seenAliases[newAlias]; !taken {
					break
				}
				count++
				newAlias = fmt.Sprintf("%s_%d", alias, count+1)
			}
			seenAliases[alias] = count + 1
			alias = newAlias
		}
		seenAliases[alias] = 1

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
	case *ast.JsonAccessExpr:
		bv, ok := b.(*ast.JsonAccessExpr)
		return ok && av.AsText == bv.AsText && exprEqual(av.Left, bv.Left) && exprEqual(av.Key, bv.Key)
	case *ast.CastExpr:
		bv, ok := b.(*ast.CastExpr)
		return ok && av.TypeName == bv.TypeName && exprEqual(av.Expr, bv.Expr)
	case *ast.UnaryExpr:
		bv, ok := b.(*ast.UnaryExpr)
		return ok && av.Op == bv.Op && exprEqual(av.Expr, bv.Expr)
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
