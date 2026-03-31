package engine

import "time"

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
