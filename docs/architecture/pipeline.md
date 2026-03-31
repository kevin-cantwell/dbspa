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

## SEED FROM

For cold starts when Kafka retention is insufficient:

```sql
FROM 'kafka://broker/topic' SEED FROM '/path/to/snapshot.parquet'
```

1. Load seed file and process through accumulators (synchronous, blocking).
2. Start streaming from Kafka.
3. Accumulators continue from seeded state.

If a checkpoint exists, it takes precedence over the seed (checkpoint is more recent).
