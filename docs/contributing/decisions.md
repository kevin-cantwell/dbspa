# Design Decisions

This page tracks significant design decisions made during development. Each entry explains what was changed, why, and what alternatives were considered.

---

## 1. ORDER BY on accumulating queries

**Problem:** ORDER BY was parsed but silently ignored for accumulating (GROUP BY) queries. The TUI and changelog output showed rows in hash map insertion order, which is non-deterministic.

**Decision:** Sort in TUI always, sort at EOF for bounded changelog, warn for unbounded changelog.

- **TUI mode:** Sort the in-memory row map before rendering each frame. Cheap — we're already iterating all rows to render.
- **Changelog mode (bounded, stdin EOF):** Emit diffs unsorted during streaming. At EOF, emit one final sorted snapshot.
- **Changelog mode (unbounded, Kafka):** ORDER BY applies to window close emissions only. Non-windowed unbounded queries: ORDER BY is a no-op with a warning.

**Alternatives rejected:**
- Sort every changelog emission: O(n log n) per record, kills throughput.
- Reject ORDER BY on accumulating queries: too restrictive, TUI users expect sorted output.
- Buffer all output and sort at end: only works for bounded inputs.

**Result:** TUI sorts rows every frame. Changelog emits sorted final snapshot at Close(). CompareValues handles NULL-last, INT/FLOAT promotion, TEXT lexicographic.

---

## 2. Checkpointing in the hot path

**Problem:** The checkpoint manager was built but never called during streaming. `--stateful` accepted the flag but didn't save or restore state.

**Decision:** Checkpoint saves accumulator state + Kafka offsets + dedup cache atomically, on a periodic interval.

Save cadence: every `--checkpoint-interval` (default 5s) or every N records, whichever comes first.

On restart with `--stateful`:

1. Check for existing checkpoint matching query fingerprint.
2. If found: restore accumulator state, resume from saved offset.
3. If not found or fingerprint mismatch: start fresh.

The checkpoint is a performance optimization, not a correctness requirement. If corrupted, replay from scratch.

**Integration points:**
- `runAccumulatingFromFiltered()` — save after each checkpoint interval
- `runWindowedFromRecords()` — save after each window close
- AggregateOp has `MarshalState`/`UnmarshalState`/`CurrentState` methods with column metadata validation

**Result:** RWMutex protects group map for concurrent checkpoint saves. Pipeline wiring: restore on startup, periodic ticker save, final save at shutdown.

---

## 3. EMIT EARLY timer

**Problem:** `EMIT EARLY '10 seconds'` was parsed and the config flowed to the windowed aggregate operator, but the timer loop was never built.

**Decision:** Background goroutine with a `time.Ticker` at the specified interval. On each tick, iterate all open windows and emit current accumulator state as retraction+insertion pairs.

**Trade-offs:**
- Early emissions increase output volume significantly (every group in every open window emits per tick).
- For TUI mode this is fine (just redraws more often with fresher data).
- For changelog mode this creates retraction noise.
- This is the expected behavior — users opt into it explicitly with EMIT EARLY.

**Result:** Tracks last-emitted values per window+group for proper retraction pairs. Mutex protects window state for concurrent access. At window close, retracts last early-emitted value before emitting final result.

---

## 4. folddb serve (HTTP sidecar)

**Problem:** FoldDB was CLI-only. For sidecar deployments (Kubernetes, etc.), users needed an HTTP API to query accumulated state.

**Decision:** `folddb serve` starts an HTTP server that runs a streaming query in the background and exposes the result set via HTTP.

Endpoints:

| Endpoint | Description |
|---|---|
| `GET /` | Current result set as JSON array |
| `GET /stream` | SSE changelog stream |
| `GET /health` | Liveness check |
| `GET /schema` | Output schema |

**Design choices:**
- Simplest possible HTTP layer — just `net/http`, no framework.
- No authentication in v0 (assume trusted network / sidecar).
- Single query per server instance (not a multi-tenant query service).
- The HTTPSink maintains the result set in memory (like TUISink's row map), protected by RWMutex.

---

## 5. Quoted identifiers

**Problem:** JSON fields named `last`, `first`, `min`, `max`, `count` clash with SQL keywords.

**Decision:** Double-quoted identifiers (`"last"`) are always `TokenIdent`, never keywords. Follows PostgreSQL convention.

**Result:** `readQuotedIdent()` in the lexer handles this.

---

## 6. SEED FROM

**Problem:** Kafka retention is finite. Cold starts with `offset=earliest` may not have enough history to reconstruct full aggregation state.

**Decision:** `SEED FROM` bootstraps accumulators from a file before streaming begins.

```sql
SELECT region, COUNT(*) AS orders
FROM 'kafka://broker/orders.cdc' FORMAT DEBEZIUM
SEED FROM '/path/to/snapshot.parquet'
GROUP BY region
```

**Implementation:**
- Seed records loaded via `loadTableFile()`, filtered through WHERE, then prepended to the stream channel before aggregation starts.
- Seed loading is synchronous — all seed records are processed before the first stream record.
- Checkpoint interaction: if `--stateful` restores a checkpoint, seed is skipped (checkpoint is more recent).
- Works with both accumulating (GROUP BY) and windowed queries.

---

## 7. Z-set formalization

**Problem:** FoldDB's diff model (`Diff int8`, limited to +1/-1) was an informal version of Z-sets from DBSP (Feldera's foundation). The informal approach worked for simple operators but became fragile for complex queries (HAVING with retractions, multi-way joins, nested aggregation). It also prevented batch processing, which is needed to close the performance gap with DuckDB.

**Decision:** Formalize the data model as Z-sets with full integer weights (`Record.Weight int`), and introduce a batch pipeline.

- **Phase 1 -- Type change**: Replace `Record.Diff int8` with `Record.Weight int`. All operators use Weight. Existing behavior preserved (Weight=+1 and -1 are equivalent to old Diff).
- **Phase 2 -- Batch pipeline**: Replace `chan Record` with `chan []Record` (via `BatchChannel`). Operators process batches. Configurable batch size (default 1024), 10ms flush timeout.
- **Phase 3 -- Batch compaction** (planned): Before aggregation, compact per group key -- sum weights for identical keys, eliminating redundant accumulator updates.
- **Phase 4 -- Operator fusion** (planned): Fuse filter+project, filter+project+aggregate for common cases.

**Why now:** The whole point of FoldDB is to close the gap between streaming and batch. Z-sets are the mathematical foundation that makes this possible. Deferring it accumulates tech debt.

**Reference:** Budiu et al., "DBSP: Automatic Incremental View Maintenance" (VLDB 2023). Z-sets = multisets with integer weights. Every relational operator has a provably correct incremental version over Z-set deltas.

**Result:** Phase 1 and Phase 2 implemented. ~40% improvement for filter/project queries, ~11% for aggregation. Phases 3-4 planned.

---

## 8. At-least-once delivery

**Problem:** Should FoldDB provide exactly-once output semantics?

**Decision:** No. v0 is at-least-once.

The failure window:

1. Records processed, accumulator updated.
2. Output emitted to sink.
3. Checkpoint flushed to disk.
4. Kafka offsets committed.

A crash between steps 2 and 3 means output was emitted but checkpoint wasn't saved. On restart, records are replayed, producing duplicate output. For changelog consumers maintaining a key-value map, the duplicates are idempotent.

Exactly-once would require transactional coordination between the output sink and the checkpoint store. This is a v1 consideration for the `--state` SQLite mode specifically.

---

## 9. Single-goroutine accumulator

**Problem:** Should the accumulator be sharded across goroutines for parallelism?

**Decision:** No. Single goroutine is sufficient for v0.

At 200K records/sec with O(1) aggregates, the accumulator goroutine consumes ~20ms/sec of CPU. The bottleneck is JSON decoding and Kafka fetch, which are parallelized across partition goroutines. The single-goroutine design eliminates concurrency bugs and simplifies checkpoint serialization.

**When this breaks down:** O(n) aggregates (MEDIAN, PERCENTILE_CONT) with high-cardinality group keys. The v1 mitigation is to shard the accumulator map by group key hash.

---

## 10. PostgreSQL dialect alignment

**Problem:** Which SQL dialect should FoldDB follow?

**Decision:** PostgreSQL. It's the most widely known SQL dialect, and it's what DuckDB aligns to (relevant for future integration).

Streaming extensions (`WINDOW TUMBLING`, `EMIT`, `EVENT TIME BY`, etc.) are FoldDB-specific — neither Flink SQL nor Spark SQL syntax fits a zero-config CLI.

---

## 11. Pure Go SQLite

**Problem:** `github.com/mattn/go-sqlite3` requires CGo. Should we use it for performance?

**Decision:** Use `modernc.org/sqlite` (pure Go) for v0 to keep the build simple — single static binary, no C toolchain required. Revisit if SQLite write performance becomes a bottleneck.
