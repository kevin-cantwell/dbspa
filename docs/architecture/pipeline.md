# Execution Pipeline

Every FoldDB query flows through the same pipeline, connected by Go channels with natural backpressure.

## Pipeline stages

```
Source → Decode → [Seed] → [Dedup] → [Join] → Filter → [Batch] → [Aggregate] → Sink
```

Stages in brackets are optional, depending on the query.

```
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│  Source   │───▶│  Decode  │───▶│  Filter  │───▶│  Batch   │───▶│ Aggregate│───▶│   Sink   │
│          │    │          │    │ (WHERE)  │    │          │    │(GROUP BY)│    │          │
│ stdin    │    │ JSON     │    │          │    │ Collect  │    │          │    │ TUI      │
│ Kafka    │    │ Avro     │    │          │    │ 1024 or  │    │ COUNT    │    │ Changelog│
│ --input  │    │ Protobuf │    │          │    │ 10ms     │    │ SUM      │    │ SQLite   │
│          │    │ CSV      │    │          │    │ flush    │    │ AVG      │    │ HTTP     │
│          │    │ Debezium │    │          │    │          │    │ ...      │    │          │
│          │    │ Parquet  │    │          │    │          │    │          │    │          │
└──────────┘    └──────────┘    └──────────┘    └──────────┘    └──────────┘    └──────────┘
```

## Query classification

FoldDB classifies every query into one of three types, determining which pipeline stages are active:

### Non-accumulating (filter/project)

```sql
SELECT name, age WHERE age > 25
```

No GROUP BY, no aggregates. Pipeline: decode -> filter -> project -> sink. Records flow through one at a time. Terminates at EOF or LIMIT.

### Accumulating (GROUP BY)

```sql
SELECT status, COUNT(*) GROUP BY status
```

Pipeline: decode -> filter -> accumulate -> sink. The accumulator maintains a hash map of `group_key -> accumulators`. Each input record updates a group. Changed results emit retraction+insertion pairs to the [changelog](../concepts/changelog-output.md).

### Windowed (GROUP BY + WINDOW)

```sql
SELECT window_start, endpoint, COUNT(*)
GROUP BY endpoint
WINDOW TUMBLING '1 minute'
EVENT TIME BY timestamp
```

Like accumulating, but records are assigned to [time windows](../concepts/windowing.md). Each window has its own set of accumulators. Windows close when the watermark advances past the window end.

## Source layer

| Source | Implementation |
|---|---|
| stdin | Reads lines from `os.Stdin` or `--input` file. Line-based for JSON/CSV; raw reader for binary formats. |
| Kafka | Uses `franz-go`. One goroutine per partition. Fan-in to a shared channel. Offset control via URI params. |
| File (`--input`) | Opens the file, passes the reader to the decoder. Parquet requires a seekable file. |
| DuckDB | Embedded DuckDB engine for file queries. See [DuckDB Integration](#duckdb-integration) below. |

## Decoder layer

Two decoder interfaces:

| Interface | Used by | Framing |
|---|---|---|
| `Decoder` | JSON, CSV | Line-based: `Decode([]byte) -> Record` |
| `StreamDecoder` | Avro OCF, Protobuf, Parquet | Binary: `DecodeStream(io.Reader, chan Record)` |

The Debezium decoder implements `MultiDecoder` — it can emit 0-2 records per input (updates emit a retraction + insertion).

## Batch processing

Between the filter and the accumulator, the `BatchChannel` stage collects individual records into batches. This is a key part of FoldDB's [Z-set model](../concepts/diff-model.md) pipeline.

### How batching works

`BatchChannel` reads from a `chan Record` and writes to a `chan Batch` (where `Batch` is `[]Record`). It flushes on two conditions:

1. **Batch full**: When the batch reaches `DefaultBatchSize` (1024 records), it is sent downstream immediately.
2. **Timeout**: If 10ms elapses without filling a batch, the partial batch is flushed. This ensures low-latency delivery for low-throughput streams.

The output channel has a buffer of 4 batches to absorb bursts without blocking the producer.

### Why batching helps

- **Fewer channel sends**: 1024 records move through one channel send instead of 1024 separate sends. Channel operations have non-trivial overhead (mutex acquisition, goroutine scheduling).
- **Better cache locality**: Processing a contiguous slice of records keeps data in L1/L2 cache, compared to processing records one at a time with channel recv between each.
- **Enables compaction**: In a future phase, batches can be compacted before aggregation -- summing Z-set weights for identical group keys, eliminating redundant accumulator updates.

### Unbatching

After the accumulator, `UnbatchChannel` expands batches back into individual records for the sink layer, which processes records one at a time.

## Concurrency model

```
stdin goroutine ──chan Record──▶ decode+filter goroutine ──chan Record──▶ BatchChannel ──chan Batch──▶ accumulator ──chan Record──▶ sink
                                                                                                           │
                                                                                                           ├── checkpoint ticker
                                                                                                           └── TUI render (15fps)
```

For Kafka: one goroutine per partition -> fan-in channel -> `BatchChannel` -> single accumulator goroutine.

The accumulator is the serialization point. At 275K records/sec for O(1) aggregates, the accumulator goroutine uses ~20ms of CPU per second. The bottleneck is JSON decoding, not accumulation.

Non-accumulating queries bypass both the batch stage and the accumulator -- partition goroutines write filtered/projected records directly to the output channel.

## Sink layer

| Sink | When used | Behavior |
|---|---|---|
| `JSONSink` | Non-accumulating, piped | NDJSON, one line per record |
| `ChangelogSink` | Accumulating, piped | NDJSON with `"op":"+"/"−"`, sorted final snapshot at EOF |
| `TUISink` | Accumulating, TTY | Live-updating table at 15fps |
| `SQLiteSink` | `--state file.db` | UPSERT for accumulating, INSERT for non-accumulating |
| `HTTPSink` | `folddb serve` | In-memory state via HTTP + SSE |

## DuckDB Integration

FoldDB embeds [DuckDB](https://duckdb.org/) as the table query engine for file-based sources. DuckDB is a vectorized columnar engine optimized for batch analytics -- it handles Parquet, CSV, and JSON files orders of magnitude faster than FoldDB's own decoders.

### When DuckDB is used

File paths ending in `.parquet`, `.csv`, or `.json` are automatically routed to DuckDB. This applies to:

- **Direct file queries:** `FROM '/path/to/file.parquet'` executes in DuckDB.
- **Join table side:** `JOIN '/data/users.parquet' u ON ...` loads the file via DuckDB into a DD join arrangement.

### How it works

```
Stream source --> FoldDB pipeline --> DD Join --> Output
                                       ^
DuckDB query --> Result as Arrangement --+
```

1. FoldDB translates the relevant portion of the query (predicate pushdown, column pruning) into a DuckDB SQL query.
2. DuckDB executes the query natively, leveraging columnar storage, SIMD vectorization, and row group statistics.
3. Results are returned as `[]Record` and loaded into the pipeline (either directly to the sink for standalone queries, or into a DD join arrangement for joins).

### Predicate pushdown and column pruning

When FoldDB routes a query to DuckDB, it pushes down:

- **WHERE predicates** -- DuckDB skips entire row groups using column statistics, achieving nearly 2M records/sec on filters vs 277K for NDJSON.
- **Column references** -- only columns referenced by the query are read from the file. For wide Parquet files, this dramatically reduces I/O.

### DuckDB scans, FoldDB aggregates

For `GROUP BY` queries on files, DuckDB handles the scan and FoldDB handles the aggregation. This is because FoldDB's accumulator supports incremental updates and Z-set semantics, which DuckDB's batch engine does not. The scan is fast (DuckDB), and the aggregation throughput is dominated by accumulator updates regardless of the decoder (~110K records/sec).

### Syntax

```sql
-- DuckDB handles the file query natively
SELECT * FROM '/data/orders.parquet' WHERE region = 'us-east'

-- Stream joined against DuckDB-loaded table
SELECT e.*, u.name
FROM 'kafka://broker/events' e
JOIN '/data/users.parquet' u ON e.user_id = u.id

-- Works with CSV and JSON too
SELECT * FROM '/data/logs.csv' WHERE level = 'ERROR'
```

## Streaming Subquery Execution

When a JOIN subquery's FROM source is a Kafka topic, FoldDB runs the inner and outer queries concurrently. This avoids the materialization deadlock (Kafka never reaches EOF, so a materialized subquery would block forever).

### Concurrent pipeline

```
                    ┌─────────────────────────────────────────────────┐
                    │  Inner query goroutine                          │
                    │                                                 │
                    │  Kafka CDC ──▶ Decode ──▶ Filter ──▶ Accumulate │
                    │                                          │      │
                    └──────────────────────────────────────────┼──────┘
                                                               │
                                                        Z-set deltas
                                                        (chan Record)
                                                               │
                                                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  Outer query goroutine                                                   │
│                                                                          │
│  Kafka events ──▶ Decode ──▶ ProcessLeftDelta ──┐                        │
│                                                 ├──▶ DDJoinOp ──▶ Sink   │
│                      ProcessRightDelta ◀────────┘       │                │
│                      (from inner channel)            mutex-protected     │
└──────────────────────────────────────────────────────────────────────────┘
```

### Execution flow

1. `buildStreamingSubqueryJoinOp` creates a DDJoinOp with an empty right arrangement (`RightIsStatic = false`) and starts the inner query via `executeStreamingSubquery`.
2. `applyStreamingSubqueryJoin` spawns two goroutines:
    - **Right feeder:** reads from the inner query's output channel and calls `ProcessRightDelta` for each record.
    - **Left feeder:** reads from the outer query's source and calls `ProcessLeftDeltaStream`.
3. Both goroutines share the DDJoinOp, which is mutex-protected. When either side produces a delta, it is joined against the other side's arrangement.
4. The output channel is closed only when both goroutines finish (tracked by a `sync.WaitGroup`).

### Delta semantics

The inner query's accumulator emits retraction+insertion pairs for every group change. For example, if region "us-east" revenue goes from 100 to 105:

- Emit `{region: "us-east", revenue: 100, weight: -1}` (retraction)
- Emit `{region: "us-east", revenue: 105, weight: +1}` (insertion)

The DDJoinOp processes these as right-side deltas. The retraction removes the old revenue from the arrangement and produces retractions for all matching left records. The insertion adds the new revenue and produces updated join results. The output weight follows the standard DD formula: `output_weight = left_weight * right_weight`.

## SEED FROM

For cold starts when Kafka retention is insufficient:

```sql
FROM 'kafka://broker/topic' SEED FROM '/path/to/snapshot.parquet'
```

1. Load seed file and process through accumulators (synchronous, blocking).
2. Start streaming from Kafka.
3. Accumulators continue from seeded state.

If a checkpoint exists, it takes precedence over the seed (checkpoint is more recent).
