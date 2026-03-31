# Execution Pipeline

Every FoldDB query flows through the same pipeline, connected by Go channels with natural backpressure.

## Pipeline stages

```
Source → Decode → [Seed] → [Dedup] → [Join] → Filter → [Aggregate] → Sink
```

Stages in brackets are optional, depending on the query.

```
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│  Source   │───▶│  Decode  │───▶│  Filter  │───▶│ Aggregate│───▶│   Sink   │
│          │    │          │    │ (WHERE)  │    │(GROUP BY)│    │          │
│ stdin    │    │ JSON     │    │          │    │          │    │ TUI      │
│ Kafka    │    │ Avro     │    │          │    │ COUNT    │    │ Changelog│
│ --input  │    │ Protobuf │    │          │    │ SUM      │    │ SQLite   │
│          │    │ CSV      │    │          │    │ AVG      │    │ HTTP     │
│          │    │ Debezium │    │          │    │ ...      │    │          │
│          │    │ Parquet  │    │          │    │          │    │          │
└──────────┘    └──────────┘    └──────────┘    └──────────┘    └──────────┘
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

## Concurrency model

```
stdin goroutine ──chan──▶ decode+filter goroutine ──chan──▶ accumulator goroutine ──chan──▶ sink
                                                                │
                                                                ├── checkpoint ticker
                                                                └── TUI render (15fps)
```

For Kafka: one goroutine per partition -> fan-in channel -> single accumulator goroutine.

The accumulator is the serialization point. At 275K records/sec for O(1) aggregates, the accumulator goroutine uses ~20ms of CPU per second. The bottleneck is JSON decoding, not accumulation.

Non-accumulating queries bypass the accumulator entirely — partition goroutines write filtered/projected records directly to the output channel.

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
