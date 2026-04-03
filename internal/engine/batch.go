package engine

import (
	"time"

	"github.com/kevin-cantwell/dbspa/internal/sql/ast"
)

// Batch is a slice of Z-set entries processed together through the pipeline.
// Batching amortizes channel send overhead, enables vectorized operations,
// and prepares for delta compaction in Phase 3.
type Batch []Record

// DefaultBatchSize is the number of records collected before sending a batch downstream.
const DefaultBatchSize = 1024

// batchFlushTimeout is the maximum time to wait before flushing a partial batch.
// This ensures low-latency delivery for low-throughput streams.
const batchFlushTimeout = 10 * time.Millisecond

// BatchChannel collects individual records into batches of up to batchSize,
// flushing either when the batch is full or after batchFlushTimeout elapses.
// The returned channel is closed when in is exhausted.
func BatchChannel(in <-chan Record, batchSize int) <-chan Batch {
	out := make(chan Batch, 4)
	go func() {
		defer close(out)
		batch := make(Batch, 0, batchSize)
		timer := time.NewTimer(batchFlushTimeout)
		defer timer.Stop()
		for {
			select {
			case rec, ok := <-in:
				if !ok {
					if len(batch) > 0 {
						out <- batch
					}
					return
				}
				batch = append(batch, rec)
				if len(batch) >= batchSize {
					out <- batch
					batch = make(Batch, 0, batchSize)
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					timer.Reset(batchFlushTimeout)
				}
			case <-timer.C:
				if len(batch) > 0 {
					out <- batch
					batch = make(Batch, 0, batchSize)
				}
				timer.Reset(batchFlushTimeout)
			}
		}
	}()
	return out
}

// CompactBatchThreshold is the minimum batch size before compaction is applied.
// Small batches (e.g., timer-flushed partial batches) skip compaction since
// the overhead isn't worth it.
const CompactBatchThreshold = 16

// CompactBatch groups records by full column fingerprint and nets out weights.
// Records with the same column values and opposite weights cancel each other.
// Returns a new batch with zero-weight entries removed. If the batch is smaller
// than CompactBatchThreshold, it is returned unchanged (compaction overhead
// exceeds savings for small batches).
func CompactBatch(batch Batch) Batch {
	if len(batch) < CompactBatchThreshold {
		return batch
	}

	type entry struct {
		record Record
		weight int
	}
	groups := make(map[string]*entry, len(batch))
	// Preserve order of first occurrence for deterministic output.
	order := make([]string, 0, len(batch))

	for _, rec := range batch {
		key := RecordFingerprint(rec)
		if e, ok := groups[key]; ok {
			e.weight += rec.Weight
		} else {
			groups[key] = &entry{record: rec, weight: rec.Weight}
			order = append(order, key)
		}
	}

	result := make(Batch, 0, len(groups))
	for _, key := range order {
		e := groups[key]
		if e.weight != 0 {
			e.record.Weight = e.weight
			result = append(result, e.record)
		}
	}
	return result
}

// GroupByKey partitions a batch by composite GROUP BY key.
// Returns a map of key string -> sub-batch. Processing each sub-batch
// sequentially improves cache locality for accumulator state access.
func GroupByKey(batch Batch, keyExprs []ast.Expr) map[string]Batch {
	groups := make(map[string]Batch, 64)
	for _, rec := range batch {
		keyVals := make([]Value, len(keyExprs))
		for i, expr := range keyExprs {
			val, err := Eval(expr, rec)
			if err != nil {
				continue
			}
			keyVals[i] = val
		}
		key := compositeKey(keyVals)
		groups[key] = append(groups[key], rec)
	}
	return groups
}

// UnbatchChannel expands batches back into individual records.
// The returned channel is closed when in is exhausted.
func UnbatchChannel(in <-chan Batch) <-chan Record {
	out := make(chan Record, 256)
	go func() {
		defer close(out)
		for batch := range in {
			for _, rec := range batch {
				out <- rec
			}
		}
	}()
	return out
}
