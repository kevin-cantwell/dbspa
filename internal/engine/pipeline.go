package engine

import (
	"sync/atomic"

	"github.com/kevin-cantwell/dbspa/internal/sql/ast"
)

// Pipeline represents a streaming execution pipeline that reads records
// from a source channel, applies filter and projection, and writes to a sink.
type Pipeline struct {
	Columns []ast.Column
	Where   ast.Expr // may be nil
}

// Process reads records from in, applies filter and projection, and sends
// results to out. It closes out when in is exhausted.
func (p *Pipeline) Process(in <-chan Record, out chan<- Record) error {
	defer close(out)

	for rec := range in {
		// Apply WHERE filter
		pass, err := Filter(p.Where, rec)
		if err != nil {
			// Log and skip malformed records
			continue
		}
		if !pass {
			continue
		}

		// Apply projection
		projected, err := Project(p.Columns, rec)
		if err != nil {
			// Log and skip
			continue
		}

		out <- projected
	}
	return nil
}

// FilterProjectBatch applies WHERE filter and SELECT projection to a batch,
// returning a new batch with only matching, projected records. This is used
// by the fused aggregate path to avoid intermediate channel allocations.
func (p *Pipeline) FilterProjectBatch(batch Batch) Batch {
	result := make(Batch, 0, len(batch))
	for _, rec := range batch {
		if p.Where != nil {
			pass, err := Filter(p.Where, rec)
			if err != nil || !pass {
				continue
			}
		}
		projected, err := Project(p.Columns, rec)
		if err != nil {
			continue
		}
		result = append(result, projected)
	}
	return result
}

// ProcessBatches reads batches from in, applies filter and projection to each
// record in each batch, and sends results to out. It closes out when in is
// exhausted. This amortizes channel overhead for non-accumulating queries.
func (p *Pipeline) ProcessBatches(in <-chan Batch, out chan<- Record) error {
	defer close(out)

	for batch := range in {
		for _, rec := range batch {
			pass, err := Filter(p.Where, rec)
			if err != nil {
				continue
			}
			if !pass {
				continue
			}

			projected, err := Project(p.Columns, rec)
			if err != nil {
				continue
			}

			out <- projected
		}
	}
	return nil
}

// FusedAggregateProcessor combines WHERE filtering, column pruning, and
// aggregation into a single pass over each batch. This eliminates the
// intermediate chan Record and second BatchChannel that the non-fused path
// requires.
//
// Non-fused path:  decode → chan Record → filter goroutine → chan Record → BatchChannel → AggregateOp.ProcessBatches
// Fused path:      decode → chan Record → BatchChannel → FusedAggregateProcessor.ProcessBatches
//
// Phase 4: after filtering, records are pruned to only the columns referenced
// by WHERE, GROUP BY, and aggregate expressions. For wide records (many
// columns), this reduces per-record allocation size before aggregation.
type FusedAggregateProcessor struct {
	Where       ast.Expr      // WHERE clause (may be nil)
	AggregateOp *AggregateOp  // accumulator
	InputCount  *atomic.Int64 // optional counter for filtered input records
	neededCols  map[string]bool // nil = no pruning
}

// NewFusedAggregateProcessor creates a FusedAggregateProcessor with column
// pruning computed from all expressions that reference input columns.
func NewFusedAggregateProcessor(where ast.Expr, agg *AggregateOp, inputCount *atomic.Int64) *FusedAggregateProcessor {
	needed := make(map[string]bool)
	collectColRefs(where, needed)
	for _, expr := range agg.GroupByExprs {
		collectColRefs(expr, needed)
	}
	for _, col := range agg.Columns {
		if col.IsAggregate && !col.IsStar && col.AggArg != nil {
			collectColRefs(col.AggArg, needed)
		}
	}
	if agg.Having != nil {
		collectColRefs(agg.Having, needed)
	}
	return &FusedAggregateProcessor{
		Where:       where,
		AggregateOp: agg,
		InputCount:  inputCount,
		neededCols:  needed,
	}
}

// NeededColumnsFromAST computes the set of input column names referenced by a
// query's WHERE clause, GROUP BY expressions, aggregate arguments, and HAVING
// clause. The returned map can be passed to format.JSONDecoder.AllowCols to
// skip decoding unused columns at parse time.
func NeededColumnsFromAST(where ast.Expr, groupBy []ast.Expr, columns []AggColumn, having ast.Expr) map[string]bool {
	needed := make(map[string]bool)
	collectColRefs(where, needed)
	for _, expr := range groupBy {
		collectColRefs(expr, needed)
	}
	for _, col := range columns {
		if col.IsAggregate && !col.IsStar && col.AggArg != nil {
			collectColRefs(col.AggArg, needed)
		}
	}
	collectColRefs(having, needed)
	return needed
}

// collectColRefs recursively walks an AST expression, adding all referenced
// column names to into.
func collectColRefs(expr ast.Expr, into map[string]bool) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *ast.ColumnRef:
		into[e.Name] = true
	case *ast.QualifiedRef:
		into[e.Name] = true
	case *ast.JsonAccessExpr:
		collectColRefs(e.Left, into)
	case *ast.BinaryExpr:
		collectColRefs(e.Left, into)
		collectColRefs(e.Right, into)
	case *ast.UnaryExpr:
		collectColRefs(e.Expr, into)
	case *ast.FunctionCall:
		for _, arg := range e.Args {
			collectColRefs(arg, into)
		}
	case *ast.CastExpr:
		collectColRefs(e.Expr, into)
	case *ast.BetweenExpr:
		collectColRefs(e.Expr, into)
		collectColRefs(e.Low, into)
		collectColRefs(e.High, into)
	case *ast.InExpr:
		collectColRefs(e.Expr, into)
		for _, v := range e.Values {
			collectColRefs(v, into)
		}
	case *ast.IsExpr:
		collectColRefs(e.Expr, into)
		collectColRefs(e.From, into)
	case *ast.CaseExpr:
		for _, w := range e.Whens {
			collectColRefs(w.Condition, into)
			collectColRefs(w.Result, into)
		}
		collectColRefs(e.Else, into)
	}
}

// ProcessBatches reads batches from in, applies WHERE filter, column pruning,
// and aggregation in a single loop per batch, and sends changelog records to out.
// It closes out when in is exhausted.
func (f *FusedAggregateProcessor) ProcessBatches(in <-chan Batch, out chan<- Record) {
	defer close(out)
	for batch := range in {
		filtered := f.filterAndPruneBatch(batch)
		f.AggregateOp.ProcessBatch(filtered, out)
	}
}

// filterAndPruneBatch applies the WHERE clause and then prunes each passing
// record to only the columns needed by aggregation expressions.
func (f *FusedAggregateProcessor) filterAndPruneBatch(batch Batch) Batch {
	result := make(Batch, 0, len(batch))
	for _, rec := range batch {
		if f.InputCount != nil {
			f.InputCount.Add(1)
		}
		if f.Where != nil {
			pass, err := Filter(f.Where, rec)
			if err != nil || !pass {
				continue
			}
		}
		if len(f.neededCols) > 0 && len(rec.Columns) > len(f.neededCols) {
			rec = pruneRecord(rec, f.neededCols)
		}
		result = append(result, rec)
	}
	return result
}

// pruneRecord returns a copy of rec with only the columns present in needed.
func pruneRecord(rec Record, needed map[string]bool) Record {
	pruned := make(map[string]Value, len(needed))
	for k, v := range rec.Columns {
		if needed[k] {
			pruned[k] = v
		}
	}
	return Record{Columns: pruned, Timestamp: rec.Timestamp, Weight: rec.Weight}
}
