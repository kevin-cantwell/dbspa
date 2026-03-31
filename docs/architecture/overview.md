# Architecture Overview

## The Pipeline

Every FoldDB query flows through the same pipeline:

```
Source → Decode → [Seed] → [Dedup] → [Join] → Filter → [Aggregate] → Sink
```

Each stage is a goroutine connected by Go channels. Backpressure propagates naturally — if the sink is slow, the upstream stages block.

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
                                     ▲               ▲
                                     │               │
                              ┌──────┴──┐     ┌──────┴──┐
                              │  Join   │     │ Window  │
                              │         │     │ Manager │
                              │Hash map │     │         │
                              │from file│     │Tumbling │
                              │         │     │Sliding  │
                              └─────────┘     │Session  │
                                              └─────────┘
```

## Query Types

FoldDB classifies every query into one of three types:

### Non-accumulating (filter/project)

```sql
SELECT name, age WHERE age > 25
```

No GROUP BY, no aggregates. Each input record produces zero or one output records. The pipeline is: decode → filter → project → output. Records flow through one at a time. The query terminates when input ends (EOF) or LIMIT is reached.

### Accumulating (GROUP BY)

```sql
SELECT status, COUNT(*) GROUP BY status
```

Has GROUP BY or aggregate functions. The accumulator maintains a hash map of `group_key → accumulators`. Each input record updates the relevant group's accumulators. When a result changes, the accumulator emits a retraction+insertion pair to the changelog.

The query runs until input ends. The output is a continuous stream of changelog diffs.

### Windowed (GROUP BY + WINDOW)

```sql
SELECT window_start, endpoint, COUNT(*)
GROUP BY endpoint
WINDOW TUMBLING '1 minute'
EVENT TIME BY timestamp
```

Like accumulating, but records are assigned to time windows. Each window has its own set of accumulators. Windows close when the watermark advances past the window end, at which point final results are emitted and the window's state is discarded.

## Record Flow

Here's what happens to a single record as it flows through a GROUP BY query:

```
1. Source reads raw bytes:
   b'{"status":"pending","amount":100}\n'

2. Decoder parses to Record:
   Record{Columns: {"status": TextValue("pending"), "amount": IntValue(100)}, Diff: +1}

3. Join (if present) probes hash map:
   Record{Columns: {"status": "pending", "amount": 100, "u.name": "Alice"}, Diff: +1}

4. Filter evaluates WHERE:
   WHERE amount > 50 → passes (amount=100)

5. Accumulator updates group "pending":
   key="pending": COUNT: 41→42, SUM: 4100→4200

6. Accumulator emits changelog:
   Record{Columns: {"status": "pending", "count": 41, "sum": 4100}, Diff: -1}  ← retraction
   Record{Columns: {"status": "pending", "count": 42, "sum": 4200}, Diff: +1}  ← insertion

7. Sink renders:
   TUI: updates the "pending" row in place
   Changelog: writes {"op":"-",...} then {"op":"+",...}
```

## Source Layer

| Source | How it works |
|---|---|
| **stdin** | Reads lines from os.Stdin (or `--input` file). Line-based for JSON/CSV; raw reader for Avro/Protobuf/Parquet. |
| **Kafka** | Uses `franz-go`. One goroutine per partition. Fan-in to shared channel. Offset control via URI params. |
| **File** (`--input`) | Opens the file, passes the reader to the decoder. Parquet requires seekable file (ReaderAt). |

## Decoder Layer

Decoders convert raw bytes to Records. Two interfaces:

- **Decoder** — line-based: `Decode([]byte) → Record`. Used for JSON, CSV.
- **StreamDecoder** — binary: `DecodeStream(io.Reader, chan Record)`. Used for Avro OCF, Protobuf, Parquet.

The Debezium decoder is special: it implements `MultiDecoder` and can emit 0-2 records per input (updates emit retraction + insertion).

## Accumulator Layer

The accumulator is a hash map: `composite_key → []Accumulator`.

Each aggregate function (COUNT, SUM, AVG, MIN, MAX, MEDIAN, FIRST, LAST) is an Accumulator:

```go
type Accumulator interface {
    Add(value Value)         // called for diff = +1
    Retract(value Value)     // called for diff = -1
    Result() Value           // current aggregate value
    HasChanged() bool        // did Result() change?
    Marshal() ([]byte, error)
    Unmarshal([]byte) error
}
```

The key insight: **Add and Retract are symmetric**. Every accumulator handles both. This is what makes the diff model work — you don't need special retraction handling per operator.

## Sink Layer

| Sink | When used | Behavior |
|---|---|---|
| **JSONSink** | Non-accumulating, piped | NDJSON, one line per record |
| **ChangelogSink** | Accumulating, piped | NDJSON with `"op":"+"/"−"`, sorted final snapshot at EOF |
| **TUISink** | Accumulating, TTY | Live-updating table at 15fps, sorted by ORDER BY |
| **SQLiteSink** | `--state file.db` | UPSERT for accumulating, INSERT for non-accumulating |
| **HTTPSink** | `folddb serve` | In-memory state served via HTTP + SSE |

## Concurrency Model

```
stdin reader goroutine ──channel──▶ decode+filter goroutine ──channel──▶ accumulator goroutine ──channel──▶ sink
                                                                              │
                                                                              ├── checkpoint ticker (periodic save)
                                                                              └── TUI render loop (15fps)
```

For Kafka: one goroutine per partition → fan-in channel → single accumulator goroutine.

The accumulator is the serialization point. At 275K records/sec for O(1) aggregates, the accumulator goroutine uses ~20ms of CPU per second. The bottleneck is JSON decoding, not accumulation.

## State Management

### Checkpointing (`--stateful`)

The accumulator state can be serialized (Marshal/Unmarshal) and saved to disk:

- **Periodic save**: a background ticker saves state every 5 seconds (configurable)
- **On shutdown**: final save before exit
- **On restart**: restore from checkpoint, resume from saved offset

The checkpoint file contains: query fingerprint, serialized group map, record count, timestamp.

### SEED FROM

For cold starts when Kafka retention is insufficient:

```sql
FROM 'kafka://broker/topic' SEED FROM '/path/to/snapshot.parquet'
```

1. Load seed file → process through accumulators (synchronous)
2. Start streaming from Kafka
3. Accumulators continue from seeded state

If a checkpoint exists, it takes precedence over the seed (checkpoint is more recent).

### SQLite state (`--state file.db`)

Writes the current accumulated state to a SQLite table via UPSERT. Other processes can read the SQLite file concurrently (WAL mode).
