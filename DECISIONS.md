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

**Status:** In progress

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

---
