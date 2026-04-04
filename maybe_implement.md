# Maybe Implement

Features and behaviors that were documented as implemented but are not. Each item was caught during a docs audit and removed or corrected. Pick these up when ready to implement.

---

## Aggregate functions (unimplemented, silently fall back to COUNT(*))

The parser accepts these function names but `newAccumulator()` has no case for them — they fall through to `CountStarAccumulator`. Queries using them will silently compute wrong results.

- `COUNT(DISTINCT x)` — exact distinct count via hash set
- `APPROX_COUNT_DISTINCT(x)` — HyperLogLog approximation
- `MEDIAN(x)` — dual-heap or sorted structure
- `PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x)` — PostgreSQL syntax; `WITHIN` is not even a recognized token in the parser
- `ARRAY_AGG(x)` — collect values into an array, include NULLs

---

## MIN/MAX retraction efficiency

`MinAccumulator` and `MaxAccumulator` use a sorted `[]float64` slice. Retraction is an O(n) linear scan. Both `Add` and `Merge` call `sort.Float64s` after each mutation, making them O(n log n) per operation.

A proper heap (container/heap) would give O(log n) Add/Retract and O(1) Result. The code even imports `"container/heap"` at the top of accumulator.go but doesn't use it for MIN/MAX.

---

## Session window per-group-key merging

`WindowedAggregateOp.processRecord` has a TODO for this. The current implementation creates a standalone `[ts, ts+gap)` window for every record independently — it never merges or extends sessions. `SessionWindowManager` (in window.go) is fully implemented but never instantiated.

Proper behavior: each group key should have its own `SessionWindowManager` that extends the current session on arrival and merges two sessions when a record bridges the gap.

---

## Checkpoint: Kafka offset storage

The checkpoint file (`checkpoint.json`) only stores `query_hash`, `timestamp`, and serialized accumulator state. Kafka offsets are not persisted. On restart, Kafka position is determined by the consumer group commit (if `?group=` is set) or the `?offset=` URI parameter.

If reliable exactly-once replay is needed, the checkpoint should also store per-partition offsets so DBSPA can seek to the exact post-checkpoint position regardless of consumer group state.

---

## Checkpoint: dedup cache not persisted

The `DEDUPLICATE BY` dedup filter is constructed fresh on every run. Its seen-ID window is not saved to the checkpoint. After a restart, previously-seen IDs within the `WITHIN` window are forgotten, so Kafka redeliveries within that window will not be filtered.

---

## Checkpoint: time-based flush only

The docs claimed flush would also trigger after every 10,000 records. Only the time-based ticker (`--checkpoint-interval`, default 5s) is implemented. A record-count threshold would reduce data loss on crashes that happen between intervals during high-throughput bursts.

---

## `dbspa state inspect <hash>`

The command is registered and the CLI accepts it, but the implementation returns:

```
Inspect checkpoint <hash> (not yet implemented)
```

A useful implementation would print: query hash, timestamp, number of group keys, serialized state size, and (once Kafka offsets are stored) per-partition offsets.

---

## CSV `null_string` option

The `FORMAT CSV` clause parser does not support a `null_string` option. Empty strings and SQL NULLs are not distinguished in CSV input.

---

## Accumulator memory limit / warning

No memory threshold exists for accumulator state. There is no mechanism to warn when accumulator RAM exceeds a budget. The `--max-memory` flag only applies to join arrangements (spilling to Badger), not to the accumulator group map.
