# FoldDB Decision Log

This file tracks design decisions made during development. Each entry explains what was changed, why, what alternatives were considered, and trade-offs.

---

## Loop: Post-Join Feature Pass

### 1. ORDER BY on accumulating queries

**Status:** In progress

**Problem:** ORDER BY is parsed but silently ignored for accumulating (GROUP BY) queries. The TUI and changelog output show rows in hash map insertion order, which is non-deterministic.

**Design:**

**Where to sort:**
- **TUI mode:** Sort the in-memory row map before rendering each frame. This is cheap — we're already iterating all rows to render. Just sort `rowOrder` by the ORDER BY columns before drawing.
- **Changelog mode (bounded, stdin EOF):** Emit diffs unsorted during streaming. At EOF, emit one final sorted snapshot. During streaming, ORDER BY is meaningless for changelog because diffs are temporal.
- **Changelog mode (unbounded, Kafka):** ORDER BY applies to window close emissions only. Each window's results are sorted before emitting. Non-windowed unbounded queries: ORDER BY is a no-op with a warning.

**Alternatives considered:**
- Sort every changelog emission: O(n log n) per record, kills throughput. Rejected.
- Reject ORDER BY on accumulating queries: too restrictive, TUI users expect sorted output.
- Buffer all output and sort at end: only works for bounded inputs.

**Decision:** Sort in TUI always, sort at EOF for bounded changelog, warn for unbounded changelog. This matches what the user expects in each context.

**Result:** Implemented in 3 commits. TUI sorts rows every frame. Changelog emits sorted final snapshot at Close(). CompareValues handles NULL-last, INT/FLOAT promotion, TEXT lexicographic. 4 new tests.

---

### 2. Wire checkpointing into hot path

**Status:** Implemented

**Problem:** The checkpoint manager (`internal/engine/checkpoint.go`) is built but never called during streaming. `--stateful` is accepted as a flag but doesn't save or restore state. The accumulator state, dedup cache, and Kafka offsets are lost on restart.

**Design:**

The checkpoint should save:
- Query fingerprint (hash of normalized SQL)
- Accumulator state (all group keys → serialized accumulators via Marshal())
- Last processed offset (for Kafka) or record count (for stdin)
- Dedup cache state
- Timestamp of last flush

Save cadence: every `--checkpoint-interval` (default 5s) or every N records, whichever comes first.

On restart with `--stateful`:
1. Check for existing checkpoint matching query fingerprint
2. If found: restore accumulator state, resume from saved offset
3. If not found or fingerprint mismatch: start fresh

The checkpoint is a performance optimization, not a correctness requirement. If corrupted, replay from scratch.

**Integration points:**
- `runAccumulatingFromFiltered()` — save after each checkpoint interval
- `runWindowedFromRecords()` — save after each window close (not yet wired)
- The aggregate operator needs `Marshal()/Unmarshal()` at the operator level (not just individual accumulators)

**Result:** Implemented in 4 commits. AggregateOp has MarshalState/UnmarshalState/CurrentState methods with column metadata validation. Pipeline wiring in runAccumulatingFromFiltered: restore on startup, periodic ticker save, final save at shutdown. RWMutex protects group map for concurrent checkpoint saves. 3 new integration tests.

---

### 3. EMIT EARLY timer

**Status:** Implemented

**Problem:** `EMIT EARLY '10 seconds'` is parsed and the config flows to the windowed aggregate operator, but the actual timer loop that triggers periodic partial result emissions was never built. Currently all windowed queries only emit on window close (EMIT FINAL).

**Design:**

In `WindowedAggregateOp.Process()`, when EMIT EARLY is configured:
- Start a background goroutine with a `time.Ticker` at the specified interval
- On each tick: iterate all open windows, emit current accumulator state for each group as a retraction+insertion pair
- The emission is the current partial result — it will be retracted and replaced on the next tick or at window close
- At window close: emit final result (same as EMIT FINAL), stop early emissions for that window

**Trade-offs:**
- Early emissions increase output volume significantly (every group in every open window emits per tick)
- For TUI mode this is fine (just redraws more often with fresher data)
- For changelog mode this creates a lot of retraction noise
- This is the expected behavior — users opt into it explicitly with EMIT EARLY

**Result:** Implemented in 3 commits. Background ticker goroutine in Process() emits partial results at EmitInterval for all open windows. Tracks last-emitted values per window+group for proper retraction pairs. Mutex protects window state for concurrent access. At window close, retracts last early-emitted value before emitting final result. 2 new tests.

---

### 4. folddb serve (HTTP sidecar)

**Status:** Implemented

**Problem:** FoldDB is CLI-only. For sidecar deployments (Kubernetes, etc.), users need an HTTP API to query the current accumulated state. The TUI sink redraws a terminal, but there's no programmatic access to the live result set.

**Design:**

`folddb serve` starts an HTTP server that:
- Runs a streaming query in the background (same pipeline as `folddb query`)
- Exposes the current accumulated state via HTTP endpoints:
  - `GET /` — current result set as JSON array
  - `GET /stream` — SSE stream of changelog diffs
  - `GET /health` — liveness check
  - `GET /metrics` — record count, groups, lag, uptime
- Accepts the SQL query and source as arguments (same as `query`)
- Uses `--state <file.db>` for SQLite persistence (optional)

**Syntax:**
```
folddb serve --port 8080 "SELECT region, COUNT(*) FROM 'kafka://broker/orders.cdc' FORMAT DEBEZIUM GROUP BY region"
```

**Architecture:**
- The serve command starts the normal pipeline but with a custom sink that maintains the result set in memory (like TUISink's row map)
- The HTTP handler reads from this in-memory map (protected by RWMutex)
- SSE endpoint writes changelog diffs as they arrive
- The pipeline runs in a background goroutine; the HTTP server is the main goroutine

**Trade-offs:**
- Simplest possible HTTP layer — no framework, just net/http
- No authentication in v0 (assume trusted network / sidecar)
- Single query per server instance (not a multi-tenant query service)

**Result:** Implemented in 3 commits. HTTPSink with 4 endpoints (/, /stream, /health, /schema). ServeCmd in Kong CLI. 6 tests.

---

### 5. Quoted identifiers

**Status:** Implemented

**Problem:** JSON fields named `last`, `first`, `min`, `max`, `count` clash with SQL keywords.

**Design:** Double-quoted identifiers (`"last"`) are always `TokenIdent`, never keywords. PostgreSQL convention.

**Result:** Single commit. `readQuotedIdent()` in lexer.

---

### 6. SEED FROM

**Status:** Implemented

**Problem:** Kafka retention is finite. SEED FROM bootstraps accumulators from a file before streaming.

**Design:** Load seed file → process through accumulators → start stream. Simple blocking load for v0 (no timestamp-based handoff).

**Syntax:**
```sql
SELECT region, COUNT(*) AS orders
FROM 'kafka://broker/orders.cdc' FORMAT DEBEZIUM
SEED FROM '/path/to/snapshot.parquet'
GROUP BY region
```

**Implementation:**
- Lexer: `TokenSeed` keyword. Parser: `parseSeedClause()` produces `SeedClause` AST node with `*TableSource` (reuses existing file path + FORMAT parsing).
- Pipeline: seed records loaded via `loadTableFile()`, filtered through WHERE, then prepended to the stream channel before aggregation starts. Seed loading is synchronous — all seed records are processed before the first stream record.
- Checkpoint interaction: if `--stateful` restores a checkpoint, seed is skipped (checkpoint is more recent). SEED FROM is the cold-start fallback.
- Works with both accumulating (GROUP BY) and windowed queries.

**Result:** Implemented in 3 commits. Parser + AST, pipeline wiring for both accumulating and windowed paths, 3 parser tests + 3 integration tests.

---

### 7. Z-set formalization

**Status:** In progress

**Problem:** FoldDB's diff model (Diff int8, +1/-1) is an informal version of Z-sets from DBSP (Feldera's foundation). The informal approach works for simple operators but becomes fragile for complex queries (HAVING with retractions, multi-way joins, nested aggregation). It also prevents batch processing, which is needed to close the performance gap with DuckDB.

**Design:**

Phase 1 — Type change: Replace `Record.Diff int8` with `Record.Weight int`. All operators use Weight. Existing behavior preserved (Weight=+1 and -1 are equivalent to old Diff). This is the foundation.

Phase 2 — Batch pipeline: Replace `chan Record` with `chan []Record`. Operators process batches. Configurable batch size (default 1024).

Phase 3 — Batch compaction: Before aggregation, compact per group key — sum weights for identical keys, eliminating redundant accumulator updates.

Phase 4 — Operator fusion: Fuse filter+project, filter+project+aggregate for common cases.

**Why now:** The whole point of FoldDB is to close the gap between streaming and batch. Z-sets are the mathematical foundation that makes this possible. Deferring it accumulates tech debt.

**Reference:** Budiu et al., "DBSP: Automatic Incremental View Maintenance" (VLDB 2023). Z-sets = multisets with integer weights. Every relational operator has a provably correct incremental version over Z-set deltas.

**Results:**
- Phase 1: Diff int8 → Weight int (mechanical rename, all tests pass)
- Phase 2: Batch pipeline with BatchChannel (1024 records / 10ms flush). ~40% faster filter/project.
- Phase 3: CompactBatch sums weights per fingerprint, drops zero-weight. Fixed processRecord to apply |weight| times.
- Phase 4: FusedAggregateProcessor eliminates intermediate channel + goroutine. 22% end-to-end improvement.
- Total: 2.2M record GROUP BY went from 10s → 7.8s (22% faster overall)
- Output format: changed from op:"+"/"-" to _weight:N (true Z-set deltas)

---

### 8. Differential Dataflow Join Operator

**Status:** In progress

**Problem:** The current join is a plain hash join — loads a file into a hash map, probes per stream record. It doesn't handle table-side changes. If reference data changes (via CDC), previously-emitted join results are stale and never corrected.

**Design:**

Implement the DBSP join formula:
```
delta(A JOIN B) = (delta_A JOIN B) UNION (A JOIN delta_B) UNION (delta_A JOIN delta_B)
```

**Core abstraction: Arrangement**

An indexed Z-set that supports:
- `Apply(delta []Record)` — merge weighted entries
- `Lookup(key) []Record` — find all entries matching a join key
- `Scan() []Record` — iterate all entries

Both sides of the join maintain an arrangement. When a delta arrives on either side, it's joined against the other side's arrangement and the results (with multiplied weights) are emitted.

**Architecture:**

```
Left source (stream) ──delta──▶ Left Arrangement ──┐
                                                    ├──▶ DD Join ──▶ output delta
Right source (file/CDC) ──delta──▶ Right Arrangement ──┘
```

**Weight multiplication:** output weight = left_weight × right_weight. This is what makes the algebra correct — a retraction on either side produces retractions in the output.

**Join types unified:**
- Stream-to-file: right arrangement loaded once, never receives deltas
- Stream-to-CDC: right arrangement seeded from initial load, receives Debezium deltas
- Stream-to-stream: both arrangements receive deltas (interval window bounds retention)

**LEFT JOIN:** When a left record has no match in the right arrangement, emit a NULL-filled row with the left record's weight. When a match later appears, retract the NULL row and emit the matched row.

**Result:** Implemented in 4 commits. Arrangement (indexed Z-set with Apply/Lookup/Scan), DDJoinOp with ProcessLeftDelta/ProcessRightDelta, weight multiplication (left*right), LEFT JOIN NULL transitions, pipeline wiring. 10 tests including CDC right-side change propagation: customer name change retracts old join results and emits corrected ones. Old HashJoinOp preserved as reference.

---

### 9. Stream-Stream Joins

**Status:** In progress

**Problem:** The current DD join only handles stream-to-file (right side is static). Stream-stream joins are needed for correlating two live data sources — e.g., matching orders to payments, correlating clicks to purchases.

**Design:**

The DD join operator already supports bidirectional deltas (ProcessLeftDelta + ProcessRightDelta). Stream-stream joins need:

1. **Two concurrent source readers** feeding into the same DDJoinOp
2. **Time-bounded retention** — arrangements can't grow forever. An interval bound limits how long entries are kept.
3. **Syntax** — `FROM 'kafka://broker/orders' o JOIN 'kafka://broker/payments' p ON o.order_id = p.order_id AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '10 minutes'`

**The interval bound is mandatory.** Without it, both arrangements grow indefinitely. The spec (Section 9.3) requires a time bound for stream-stream joins — FoldDB errors if none is specified.

**Architecture:**

```
Kafka topic A ──goroutine──▶ Left Arrangement  ──┐
                                                   ├──▶ DDJoinOp ──▶ output
Kafka topic B ──goroutine──▶ Right Arrangement ──┘
```

Both goroutines call ProcessLeftDelta/ProcessRightDelta respectively. The DDJoinOp is already thread-safe (arrangements have RWMutex).

**Retention/eviction:** A background goroutine periodically scans arrangements and evicts entries older than the interval bound. Evicted entries produce retractions through the join (weight=-1).

**For stdin, two-source joins aren't possible** (only one stdin). Both sources must be Kafka topics, files, or one of each.

---
