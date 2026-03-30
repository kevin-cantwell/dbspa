package engine

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/kevin-cantwell/folddb/internal/sql/ast"
)

// WindowedAggregateOp performs windowed aggregation.
// Records are assigned to windows based on their event time, and
// results are emitted when windows close (watermark passes window end).
type WindowedAggregateOp struct {
	// GroupByExprs are the expressions used for GROUP BY.
	GroupByExprs []ast.Expr
	// Columns describes each output column.
	Columns []AggColumn
	// Having is the optional HAVING clause expression.
	Having ast.Expr

	// WindowSpec describes the window type and size.
	WindowSpec WindowSpec

	// EventTimeExpr extracts event time from records. Nil means processing time.
	EventTimeExpr ast.Expr

	// Watermark tracks event time watermarks.
	Watermark *WatermarkTracker

	// EmitMode: "FINAL" (default) or "EARLY"
	EmitMode string
	// EmitInterval for EARLY mode
	EmitInterval time.Duration

	// mu protects windows and windowBuckets for concurrent access
	// between the record-processing goroutine and the EMIT EARLY ticker.
	mu sync.Mutex

	// windows maps "windowKey|groupKey" -> windowGroupState
	windows map[string]*windowGroupState
	// windowBuckets maps WindowKey.String() -> list of group keys for that window
	windowBuckets map[string]*windowBucket
}

type windowBucket struct {
	key       WindowKey
	groupKeys map[string]bool
	closed    bool
}

type windowGroupState struct {
	windowKey    WindowKey
	accumulators []Accumulator
	keyValues    []Value
	lastEmitted  []Value
	hasEmitted   bool
	count        int64 // track records for empty window detection
}

// NewWindowedAggregateOp creates a windowed aggregate operator.
func NewWindowedAggregateOp(
	columns []AggColumn,
	groupBy []ast.Expr,
	having ast.Expr,
	windowSpec WindowSpec,
	eventTimeExpr ast.Expr,
	watermark *WatermarkTracker,
	emitMode string,
	emitInterval time.Duration,
) *WindowedAggregateOp {
	if emitMode == "" {
		emitMode = "FINAL"
	}
	return &WindowedAggregateOp{
		GroupByExprs:  groupBy,
		Columns:       columns,
		Having:        having,
		WindowSpec:    windowSpec,
		EventTimeExpr: eventTimeExpr,
		Watermark:     watermark,
		EmitMode:      emitMode,
		EmitInterval:  emitInterval,
		windows:       make(map[string]*windowGroupState),
		windowBuckets: make(map[string]*windowBucket),
	}
}

// Process reads records from in, applies windowed aggregation, and sends results to out.
func (op *WindowedAggregateOp) Process(in <-chan Record, out chan<- Record) error {
	defer close(out)

	// Start EMIT EARLY ticker goroutine if configured.
	var done chan struct{}
	if op.EmitMode == "EARLY" && op.EmitInterval > 0 {
		done = make(chan struct{})
		go op.earlyEmitLoop(done, out)
	}

	for rec := range in {
		op.mu.Lock()
		op.processRecord(rec, out)
		op.mu.Unlock()
	}

	// Stop the early emit ticker before flushing.
	if done != nil {
		close(done)
	}

	// Input exhausted (e.g., stdin EOF). Flush all open windows.
	op.mu.Lock()
	op.flushAllWindows(out)
	op.mu.Unlock()

	return nil
}

// earlyEmitLoop runs in a background goroutine, periodically emitting
// partial results for all open windows.
func (op *WindowedAggregateOp) earlyEmitLoop(done chan struct{}, out chan<- Record) {
	ticker := time.NewTicker(op.EmitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			op.mu.Lock()
			op.emitEarlyResults(out)
			op.mu.Unlock()
		}
	}
}

// emitEarlyResults emits partial results for all open windows.
// For each group in each open window, it emits a retraction of the
// previously emitted partial result (if any), then the current partial result.
// Must be called with op.mu held.
func (op *WindowedAggregateOp) emitEarlyResults(out chan<- Record) {
	for _, wb := range op.windowBuckets {
		if wb.closed {
			continue
		}
		wkStr := wb.key.String()

		for gk := range wb.groupKeys {
			compositeStr := wkStr + "|" + gk
			wgs, ok := op.windows[compositeStr]
			if !ok {
				continue
			}

			// Build the current partial result columns.
			cols := make(map[string]Value)
			cols["window_start"] = TextValue{V: wb.key.Start.Format(time.RFC3339)}
			cols["window_end"] = TextValue{V: wb.key.End.Format(time.RFC3339)}

			keyIdx := 0
			for i, col := range op.Columns {
				if col.IsAggregate {
					cols[col.Alias] = wgs.accumulators[i].Result()
				} else {
					if col.Alias == "window_start" || col.Alias == "window_end" {
						continue
					}
					if keyIdx < len(wgs.keyValues) {
						cols[col.Alias] = wgs.keyValues[keyIdx]
						keyIdx++
					}
				}
			}

			// If we previously emitted a partial result, retract it.
			if wgs.hasEmitted {
				retraction := make(map[string]Value, len(wgs.lastEmitted))
				retraction["window_start"] = TextValue{V: wb.key.Start.Format(time.RFC3339)}
				retraction["window_end"] = TextValue{V: wb.key.End.Format(time.RFC3339)}
				keyIdx2 := 0
				for i, col := range op.Columns {
					if col.IsAggregate {
						retraction[col.Alias] = wgs.lastEmitted[i]
					} else {
						if col.Alias == "window_start" || col.Alias == "window_end" {
							continue
						}
						if keyIdx2 < len(wgs.keyValues) {
							retraction[col.Alias] = wgs.keyValues[keyIdx2]
							keyIdx2++
						}
					}
				}
				out <- Record{
					Columns:   retraction,
					Timestamp: wb.key.End,
					Diff:      -1,
				}
			}

			// Emit the current partial result.
			out <- Record{
				Columns:   cols,
				Timestamp: wb.key.End,
				Diff:      1,
			}

			// Save the emitted values for future retraction.
			emitted := make([]Value, len(op.Columns))
			for i, col := range op.Columns {
				if col.IsAggregate {
					emitted[i] = wgs.accumulators[i].Result()
				}
			}
			wgs.lastEmitted = emitted
			wgs.hasEmitted = true
		}
	}
}

func (op *WindowedAggregateOp) processRecord(rec Record, out chan<- Record) {
	// Extract event time
	eventTime := ExtractEventTime(op.EventTimeExpr, rec)

	// Advance watermark
	op.Watermark.Advance(0, eventTime)

	// Assign record to windows
	var windowKeys []WindowKey
	if op.WindowSpec.Type == "SESSION" {
		// Session windows need special handling per group key
		// For now, we handle session as a single global session manager
		// A proper implementation would have per-group-key session managers
		// TODO: per-group-key session window managers
		windowKeys = []WindowKey{{
			Start: eventTime,
			End:   eventTime.Add(op.WindowSpec.Size),
		}}
	} else {
		windowKeys = AssignWindows(eventTime, op.WindowSpec)
	}

	// Evaluate GROUP BY key
	keyVals := make([]Value, len(op.GroupByExprs))
	for i, expr := range op.GroupByExprs {
		val, err := Eval(expr, rec)
		if err != nil {
			continue
		}
		keyVals[i] = val
	}
	groupKeyStr := compositeKey(keyVals)

	// For each window this record belongs to
	for _, wk := range windowKeys {
		// Check if this window is already closed (late data)
		if op.Watermark.ShouldClose(wk.End) {
			// Late data — drop it
			continue
		}

		wkStr := wk.String()
		compositeStr := wkStr + "|" + groupKeyStr

		// Get or create window bucket
		wb, ok := op.windowBuckets[wkStr]
		if !ok {
			wb = &windowBucket{
				key:       wk,
				groupKeys: make(map[string]bool),
			}
			op.windowBuckets[wkStr] = wb
		}
		wb.groupKeys[groupKeyStr] = true

		// Get or create window group state
		wgs, ok := op.windows[compositeStr]
		if !ok {
			wgs = &windowGroupState{
				windowKey:    wk,
				accumulators: op.newAccumulators(),
				keyValues:    keyVals,
			}
			op.windows[compositeStr] = wgs
		}

		// Apply the record to accumulators
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
			if rec.Diff >= 0 {
				wgs.accumulators[i].Add(argVal)
				wgs.count++
			} else {
				wgs.accumulators[i].Retract(argVal)
				wgs.count--
			}
		}
	}

	// Check if any windows should close after watermark advance
	op.closeReadyWindows(out)
}

// closeReadyWindows emits final results for windows whose end time
// has been passed by the watermark.
func (op *WindowedAggregateOp) closeReadyWindows(out chan<- Record) {
	var closedBuckets []string

	for wkStr, wb := range op.windowBuckets {
		if wb.closed {
			continue
		}
		if !op.Watermark.ShouldClose(wb.key.End) {
			continue
		}

		// Window is ready to close
		wb.closed = true
		op.emitWindowResults(wb, out)
		closedBuckets = append(closedBuckets, wkStr)
	}

	// Clean up closed window state
	for _, wkStr := range closedBuckets {
		wb := op.windowBuckets[wkStr]
		for gk := range wb.groupKeys {
			delete(op.windows, wkStr+"|"+gk)
		}
		delete(op.windowBuckets, wkStr)
	}
}

// flushAllWindows emits results for all remaining open windows (e.g., on EOF).
func (op *WindowedAggregateOp) flushAllWindows(out chan<- Record) {
	for _, wb := range op.windowBuckets {
		if wb.closed {
			continue
		}
		wb.closed = true
		op.emitWindowResults(wb, out)
	}
	// Clear all state
	op.windows = make(map[string]*windowGroupState)
	op.windowBuckets = make(map[string]*windowBucket)
}

// emitWindowResults emits final results for all groups in a window.
// If EMIT EARLY was active, it first retracts the last early-emitted partial result.
func (op *WindowedAggregateOp) emitWindowResults(wb *windowBucket, out chan<- Record) {
	wkStr := wb.key.String()

	for gk := range wb.groupKeys {
		compositeStr := wkStr + "|" + gk
		wgs, ok := op.windows[compositeStr]
		if !ok {
			continue
		}

		// If EMIT EARLY was active and we emitted a partial result, retract it.
		if op.EmitMode == "EARLY" && wgs.hasEmitted {
			retraction := make(map[string]Value)
			retraction["window_start"] = TextValue{V: wb.key.Start.Format(time.RFC3339)}
			retraction["window_end"] = TextValue{V: wb.key.End.Format(time.RFC3339)}
			keyIdx := 0
			for i, col := range op.Columns {
				if col.IsAggregate {
					retraction[col.Alias] = wgs.lastEmitted[i]
				} else {
					if col.Alias == "window_start" || col.Alias == "window_end" {
						continue
					}
					if keyIdx < len(wgs.keyValues) {
						retraction[col.Alias] = wgs.keyValues[keyIdx]
						keyIdx++
					}
				}
			}
			out <- Record{
				Columns:   retraction,
				Timestamp: wb.key.End,
				Diff:      -1,
			}
		}

		// Build result record with window_start and window_end
		cols := make(map[string]Value)

		// Add window_start and window_end as implicit columns
		cols["window_start"] = TextValue{V: wb.key.Start.Format(time.RFC3339)}
		cols["window_end"] = TextValue{V: wb.key.End.Format(time.RFC3339)}

		keyIdx := 0
		for i, col := range op.Columns {
			if col.IsAggregate {
				cols[col.Alias] = wgs.accumulators[i].Result()
			} else {
				if col.Alias == "window_start" || col.Alias == "window_end" {
					// Already set above
					continue
				}
				if keyIdx < len(wgs.keyValues) {
					cols[col.Alias] = wgs.keyValues[keyIdx]
					keyIdx++
				}
			}
		}

		// Emit the final result
		out <- Record{
			Columns:   cols,
			Timestamp: wb.key.End,
			Diff:      1,
		}
	}
}

// newAccumulators creates a fresh set of accumulators matching the column spec.
func (op *WindowedAggregateOp) newAccumulators() []Accumulator {
	accs := make([]Accumulator, len(op.Columns))
	for i, col := range op.Columns {
		if !col.IsAggregate {
			accs[i] = &noopAccumulator{}
			continue
		}
		accs[i] = newAccumulator(col.AggFunc, col.IsStar)
	}
	return accs
}

// WindowKeyForComposite creates the composite key used in the changelog output.
func WindowKeyForComposite(wk WindowKey, groupKeyVals []Value) string {
	parts := make([]any, 0, 2+len(groupKeyVals))
	parts = append(parts, wk.Start.Format(time.RFC3339), wk.End.Format(time.RFC3339))
	for _, v := range groupKeyVals {
		if v.IsNull() {
			parts = append(parts, nil)
		} else {
			parts = append(parts, v.ToJSON())
		}
	}
	data, _ := json.Marshal(parts)
	return string(data)
}

// inferColumnName is already in project.go

// ParseWindowSpec parses an AST WindowClause into a WindowSpec.
func ParseWindowSpec(wc *ast.WindowClause) (WindowSpec, error) {
	size, err := ParseDuration(wc.Size)
	if err != nil {
		return WindowSpec{}, fmt.Errorf("invalid window size: %w", err)
	}

	spec := WindowSpec{
		Type:    wc.Type,
		Size:    size,
		Advance: size, // default for tumbling
	}

	if wc.Type == "SLIDING" {
		if wc.SlideBy == "" {
			return WindowSpec{}, fmt.Errorf("SLIDING window requires BY clause")
		}
		advance, err := ParseDuration(wc.SlideBy)
		if err != nil {
			return WindowSpec{}, fmt.Errorf("invalid slide interval: %w", err)
		}
		spec.Advance = advance
	}

	return spec, nil
}
