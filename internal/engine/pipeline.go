package engine

import (
	"sync/atomic"

	"github.com/kevin-cantwell/folddb/internal/sql/ast"
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

// FusedAggregateProcessor combines WHERE filtering and aggregation into a
// single pass over each batch. This eliminates the intermediate chan Record
// and second BatchChannel that the non-fused path requires.
//
// Non-fused path:  decode → chan Record → filter goroutine → chan Record → BatchChannel → AggregateOp.ProcessBatches
// Fused path:      decode → chan Record → BatchChannel → FusedAggregateProcessor.ProcessBatches
type FusedAggregateProcessor struct {
	Where       ast.Expr      // WHERE clause (may be nil)
	AggregateOp *AggregateOp  // accumulator
	InputCount  *atomic.Int64 // optional counter for filtered input records
}

// ProcessBatches reads batches from in, applies WHERE filter and aggregation
// in a single loop per batch, and sends changelog records to out.
// It closes out when in is exhausted.
func (f *FusedAggregateProcessor) ProcessBatches(in <-chan Batch, out chan<- Record) {
	defer close(out)
	for batch := range in {
		// Step 1: filter in one pass, counting input records
		filtered := f.filterBatch(batch)
		// Step 2: compact + aggregate
		f.AggregateOp.ProcessBatch(filtered, out)
	}
}

// filterBatch applies the WHERE clause to each record in the batch,
// returning a new batch with only matching records.
func (f *FusedAggregateProcessor) filterBatch(batch Batch) Batch {
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
		result = append(result, rec)
	}
	return result
}
