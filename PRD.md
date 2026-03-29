# neql — Product Requirements Document

**Version:** 0.1 (Draft)
**Date:** 2026-03-28
**Status:** In Review

---

## 1. Executive Summary

`neql` (pronounced "neckle") is a zero-infrastructure CLI tool that lets engineers query any structured data source — streaming or static — using a single, unified SQL dialect. Point it at a Kafka topic, a CSV on S3, a Postgres table, or a pipe from `grep`, and write SQL against it. Joins across those sources just work. No servers, no config files, no scheduler. Just `neql`.

The guiding principle: **if you can describe your data, you can query it.**

---

## 2. Problem Statement

Data engineers today live in a fractured world:

- Query static files? Use `q`, `trdsql`, `duckdb`, or import into a database first.
- Query Kafka? Run ksqlDB — which means standing up a cluster, a schema registry, a Connect worker.
- Join a Kafka topic to a Postgres table? Write a Flink or Spark job, deploy it, wait for it.
- Query Parquet on S3? Either DuckDB (close, but no streams) or spin up Athena/Trino.

None of these tools are a CLI. All require infrastructure or have significant operational footprint. Most don't do streams at all, and none do streams *and* tables *and* joins in a single zero-setup experience.

`neql` eliminates the friction for the common case: ad-hoc exploration, lightweight ETL, operational debugging, and pipeline prototyping — all from the terminal.

---

## 3. Goals

### Primary Goals

1. **Zero infrastructure.** A single binary. No daemon, no server, no cluster.
2. **Unified SQL over streams and tables.** One query language for Kafka, files, stdin, databases, object stores, and warehouses.
3. **Joins across heterogeneous sources.** Stream-table, table-table, and stream-stream joins in SQL, without user-managed shuffle or state infrastructure.
4. **Broad format support.** CSV, JSON, NDJSON, Avro, Parquet, Protobuf, ORC, Arrow IPC.
5. **Intuitive source URIs.** Data sources are identified by URI in the `FROM` clause. No separate config step.
6. **Streaming-first semantics.** Proper handling of time, ordering, late data, and unbounded results for stream sources.

### Secondary Goals

- Output to files, streams, or databases (not just stdout).
- Scriptable — can be embedded in shell pipelines.
- Stateful aggregation over streams with configurable windowing.
- Schema inference for schemaless formats (JSON, CSV).
- Schema registry integration (Confluent, AWS Glue) for Avro/Protobuf.
- A `WATCH` mode that re-evaluates a table query on a timer (like `watch -n5` but SQL-aware).

### Non-Goals (v1)

- A persistent query service or daemon.
- A full ANSI SQL implementation (focus on the most useful 80%).
- Distributed execution across multiple machines.
- A UI or web interface.
- Kafka producer / data mutation (read-only initially).
- Schema evolution handling beyond best-effort forward compatibility.

---

## 4. User Personas

### Primary: The Data Engineering Manager / Senior Data Engineer

- Writes Go or Python fluently.
- Manages data pipelines: Kafka, Flink, dbt, Airflow.
- Frequent tasks: debugging live Kafka messages, spot-checking a file dropped by a partner, ad-hoc joining a stream to a reference table.
- Pain: every tool requires a setup step or is read-only or is too slow to iterate.

### Secondary: The Platform / Infrastructure Engineer

- Works with logs, metrics, and event streams.
- Wants to query structured logs from stdin or a file without importing into Splunk.
- Cares about zero footprint tools — will add to a Dockerfile or a `brew install`.

### Tertiary: The Analytics Engineer (power user of dbt, SQL-fluent)

- Comfortable with SQL, less comfortable with code.
- Wants DuckDB-style ad-hoc querying but extended to Kafka and live data.

---

## 5. Core Concepts

### 5.1 Tables vs. Streams

`neql` treats every data source as one of two logical types:

| Concept | Description | Examples |
|---|---|---|
| **Table** | Bounded or slowly-changing. Has a definite start and end (or is updated in-place). | CSV, Parquet, JSON file, Postgres table, BigQuery table, S3 prefix |
| **Stream** | Unbounded. Produces records continuously. Has an append-only log structure. | Kafka topic, stdin pipe, tail -f log file, CDC transaction log |

The distinction matters for:
- **Query termination**: table queries terminate; stream queries run until stopped (or a `LIMIT` / window boundary is reached).
- **Join semantics**: see Section 9.
- **Ordering**: tables are ordered by query plan; streams carry event-time and processing-time.

`neql` infers the type from the URI scheme. Users can override with a hint (`AS TABLE` / `AS STREAM`).

### 5.2 Time

Every stream record has two times:

- **Event time**: when the event actually happened. Extracted from a user-specified field or inferred heuristically.
- **Processing time**: when `neql` received the record. Always available.

Window functions and stream aggregations default to **processing time** but can be switched to **event time** via `EVENT TIME BY <column>`.

### 5.3 Watermarks

For event-time operations, `neql` maintains a watermark: an estimate of how far event time has progressed. Configurable allowed lateness (default 5s). Late records are either dropped or routed to a dead-letter output.

### 5.4 Source URIs

Data sources are referenced by URI in the `FROM` clause:

```
scheme://[auth@]host[:port]/path[?params]
```

Examples:

```sql
FROM 'kafka://broker:9092/events'
FROM 'file:///data/users.parquet'
FROM 'pg://user:pass@localhost/mydb/orders'
FROM 's3://my-bucket/data/2026/**/*.parquet'
FROM 'stdin://'
FROM 'mysql://user:pass@host/db/table'
FROM 'bigquery://project/dataset/table'
FROM 'iceberg://catalog/db/table'
FROM 'sqlite:///path/to/db.sqlite/tablename'
```

Bare file paths without a scheme are also accepted and the format is inferred from the extension:

```sql
FROM '/data/orders.csv'
FROM './events.ndjson'
```

### 5.5 Format Hints

When format cannot be inferred from extension or URI, the user provides a hint:

```sql
FROM 'kafka://broker:9092/events' FORMAT AVRO
FROM 'stdin://' FORMAT JSON
FROM '/data/dump' FORMAT CSV(delimiter='\t', header=false)
```

---

## 6. SQL Dialect

`neql` implements a pragmatic SQL subset. The goal is ANSI SQL compatibility where it doesn't conflict with stream semantics, with extensions for streaming.

### 6.1 Standard Clauses Supported

```sql
SELECT [DISTINCT] expr [AS alias], ...
FROM source [AS alias]
[JOIN source ON condition]
[WHERE condition]
[GROUP BY expr, ...]
[HAVING condition]
[ORDER BY expr [ASC|DESC], ...]
[LIMIT n]
[OFFSET n]
```

### 6.2 Stream-Specific Extensions

#### WINDOW clause (tumbling, hopping, session)

```sql
SELECT
    WINDOW_START,
    WINDOW_END,
    user_id,
    COUNT(*) AS event_count
FROM 'kafka://broker:9092/clickstream'
WINDOW TUMBLING(SIZE 1 MINUTE)
GROUP BY WINDOW_START, user_id
```

Window types:
- `TUMBLING(SIZE <duration>)` — non-overlapping fixed windows
- `HOPPING(SIZE <duration>, ADVANCE <duration>)` — overlapping sliding windows
- `SESSION(GAP <duration>)` — session windows (activity-based)

#### EMIT clause

Controls when window results are emitted:

```sql
EMIT ON WINDOW CLOSE          -- default: emit once when window closes
EMIT EVERY 10 SECONDS         -- emit partial results on a timer
EMIT ON EVERY RECORD          -- emit after each record (high volume, use carefully)
```

#### WATERMARK clause

```sql
FROM 'kafka://broker:9092/events'
EVENT TIME BY ts
WATERMARK 30 SECONDS          -- allow up to 30s of lateness
```

#### LATEST / AS OF

Snapshot a stream at a point in time (if source supports it):

```sql
FROM 'kafka://broker:9092/users' AS OF '2026-03-27T00:00:00Z'
```

### 6.3 Schema Inference & Virtual Columns

For schemaless formats (JSON, CSV), `neql` samples the first N records to infer schema. The user can inspect it:

```bash
neql schema 'kafka://broker:9092/events'
neql schema ./data.csv
```

For Kafka, each record exposes virtual columns automatically:

| Column | Description |
|---|---|
| `_offset` | Kafka partition offset |
| `_partition` | Kafka partition |
| `_timestamp` | Kafka log-append timestamp |
| `_key` | Kafka message key (raw bytes decoded as string or JSON) |

Similarly, every source exposes `_source_uri` and `_ingested_at`.

### 6.4 Type System

`neql` uses a rich scalar type system:

| Type | Notes |
|---|---|
| `BOOLEAN` | |
| `INT8/16/32/64` | |
| `FLOAT32/FLOAT64` | |
| `DECIMAL(p,s)` | |
| `VARCHAR` / `TEXT` | |
| `BYTES` | |
| `TIMESTAMP` | always UTC internally |
| `DATE` | |
| `INTERVAL` | |
| `JSON` | opaque JSON value; queryable via `->` / `->>` operators |
| `ARRAY<T>` | |
| `MAP<K,V>` | |
| `STRUCT(field T, ...)` | nested records |

JSON access follows PostgreSQL syntax:

```sql
SELECT payload->'user'->>'email' AS email
FROM 'kafka://broker:9092/events'
```

### 6.5 Functions

Standard SQL functions plus:
- `PARSE_JSON(str)` — coerce string to JSON
- `TO_JSON(val)` — serialize value to JSON string
- `EXTRACT_PATH(json, 'a.b.c')` — deep path extraction
- `PARSE_TIMESTAMP(str, fmt)` — flexible timestamp parsing
- `KAFKA_OFFSET()`, `KAFKA_PARTITION()` — shorthand virtual column accessors
- `WINDOW_START()`, `WINDOW_END()` — in windowed queries
- Standard aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `FIRST`, `LAST`, `APPROX_COUNT_DISTINCT`

---

## 7. Data Source Connectors

### 7.1 Connector Matrix (v1 Scope)

| Source | Scheme | Read | Write (sink) | Stream | Table | Notes |
|---|---|---|---|---|---|---|
| Kafka | `kafka://` | P2 | P4+ | yes | snapshot | |
| Debezium (Kafka) | `FORMAT DEBEZIUM` | P2 | — | yes | — | changelog stream over Kafka |
| stdin / pipe | `stdin://` | P1 | — | yes | — | |
| File (local) | `file://` or bare path | P1 | P1 | tail mode | yes | |
| CSV | (format) | P1 | P1 | — | yes | |
| NDJSON / JSON | (format) | P1 | P1 | yes | yes | |
| Parquet | (format) | P1 | P2 | — | yes | |
| Avro | (format) | P2 | P3 | yes | yes | |
| Protobuf | (format) | P3 | P4 | yes | yes | requires descriptor or registry |
| Postgres | `pg://` | P1 | P4+ | CDC P4+ | yes | P1 via DuckDB scanner |
| MySQL | `mysql://` | P1 | P4+ | CDC P4+ | yes | P1 via DuckDB scanner |
| SQLite | `sqlite://` | P1 | P1 | — | yes | |
| S3 | `s3://` | P1 | P3 | — | yes | |
| GCS | `gcs://` | P1 | P3 | — | yes | |
| BigQuery | `bigquery://` | P1 | P4+ | — | yes | via DuckDB BigQuery extension |
| Iceberg | `iceberg://` | P4+ | P4+ | — | yes | |
| ORC | (format) | P4+ | P4+ | — | yes | |
| Arrow IPC | (format) | P4+ | P4+ | yes | yes | |

### 7.2 Kafka Connector Details

- Connects to any Kafka-compatible broker (Apache Kafka, Redpanda, Confluent Cloud, MSK).
- Authentication: PLAINTEXT, SASL/PLAIN, SASL/SCRAM, SASL/OAUTHBEARER, mTLS.
- Credentials via URI userinfo, environment variables, or `~/.neql/credentials`.
- Schema registry integration: Confluent Schema Registry, AWS Glue Schema Registry (auto-detected via `?registry=` param).
- Consumer group optional — uses a unique ephemeral group by default so queries don't affect committed offsets.
- Partition assignment: consume all partitions by default; filterable via `?partition=0,1,2`.
- Offset control: `?offset=earliest` (default), `?offset=latest`, `?offset=<timestamp>`, `?offset=<number>`.

```sql
-- Start from earliest
FROM 'kafka://localhost:9092/orders?offset=earliest'

-- Start from specific timestamp
FROM 'kafka://localhost:9092/orders?offset=2026-03-27T00:00:00Z'

-- Confluent Cloud with schema registry
FROM 'kafka://pkc-xxx.us-east-1.aws.confluent.cloud:9092/events?registry=https://psrc-xxx.us-east-1.aws.confluent.cloud'
FORMAT AVRO
```

### 7.3 File & Object Storage

- Local files resolved relative to CWD.
- S3/GCS URIs use standard environment credentials (AWS_PROFILE, GOOGLE_APPLICATION_CREDENTIALS, etc.).
- Glob patterns in path are expanded server-side for object stores, locally for filesystem.
- Parquet and Avro files can be read with predicate pushdown (partition pruning for Hive-partitioned layouts).

### 7.4 stdin / Pipe Mode

```bash
# Pipe JSON logs through neql
kubectl logs my-pod | neql "SELECT level, message WHERE level = 'ERROR'"

# Pipe CSV
cat orders.csv | neql "SELECT customer_id, SUM(amount) GROUP BY customer_id"

# Explicit stdin URI
neql "SELECT * FROM 'stdin://' FORMAT JSON WHERE status = 'failed'"
```

When reading from stdin, `neql` defaults to line-delimited (NDJSON or CSV). Format is inferred or specified with `FORMAT`.

### 7.5 Relational Databases

- Postgres and MySQL connectors use Go standard `database/sql` with driver libraries.
- SQLite is embedded (no external daemon needed).
- For table sources, `neql` issues a `SELECT *` (or column subset if projectable) and streams results through the engine.
- Predicate pushdown for `WHERE` clauses against indexed columns is attempted via query rewrite.
- Join of a Postgres table to a Kafka stream will materialize the Postgres table in-memory (or spill to disk for large tables).

---

## 8. Format Support

| Format | Magic Detection | Schema Inference | Nested Types | Notes |
|---|---|---|---|---|
| CSV | header row / delimiter sniff | yes | no | configurable delimiter, quote char, encoding |
| TSV | extension | yes | no | shorthand for `CSV(delimiter='\t')` |
| JSON (array) | `[` prefix | yes | yes | reads entire file |
| NDJSON | line-by-line | yes (sampled) | yes | streaming-friendly |
| Avro OCF | magic bytes | schema embedded | yes | Object Container File |
| Avro Stream | (with registry) | registry | yes | raw Avro without container |
| Parquet | magic bytes | schema embedded | yes | column pruning, predicate pushdown |
| Protobuf | (with descriptor) | descriptor file | yes | requires `.proto` file or schema registry |
| ORC | magic bytes | schema embedded | yes | v2 scope |
| Arrow IPC | magic bytes | schema embedded | yes | v2 scope |

### 8.1 Format Parameters

Format parameters are passed via the `FORMAT` clause:

```sql
FORMAT CSV(
    delimiter = ',',
    quote = '"',
    escape = '"',
    header = true,
    null_string = 'NULL',
    encoding = 'utf-8'
)

FORMAT PROTOBUF(
    descriptor = '/path/to/schema.pb',
    message = 'com.example.OrderEvent'
)

FORMAT AVRO(
    registry = 'http://schema-registry:8081'
)
```

---

## 9. Join Semantics

Joins are the hardest part of a stream-table SQL engine. `neql` defines clear, predictable semantics.

### 9.1 Table-Table Join

Standard hash join or sort-merge join. Both sides are materialized (with pushdown; see Section 9.6). Behaves exactly like a traditional RDBMS join. Output is bounded.

```sql
SELECT o.order_id, u.email
FROM '/data/orders.parquet' o
JOIN 'pg://localhost/mydb/users' u ON o.user_id = u.id
```

### 9.2 Stream-Table Join (Lookup Join)

The stream drives the join. The planner selects one of three execution strategies based on estimated table size and source type:

**Strategy A — Batched Live Lookup (default for relational sources)**

Do not materialize the table. Buffer N stream records, issue a single parameterized query to the source, rejoin results, emit. The source never transfers rows that aren't needed.

```sql
-- neql issues: SELECT id, email FROM users WHERE id IN (batch_of_ids)
SELECT e.user_id, u.email, e.event_type
FROM 'kafka://broker:9092/events' e
JOIN 'pg://localhost/mydb/users' u ON e.user_id = u.id
```

Batch size is tunable (`LOOKUP BATCH SIZE 500`). This strategy works with tables of any size since the local memory footprint is bounded by the batch size, not the table size.

**Strategy B — Materialized Hash Index (for small tables or non-relational sources)**

Materialize the table once into an in-memory hash map, probe it for each stream record. Optional timer-based refresh. Preferred when: table is small (fits in memory), stream throughput is very high (amortizes materialization cost), or the source doesn't support parameterized queries (e.g., Parquet files).

```sql
JOIN 'pg://localhost/mydb/users' u
  ON e.user_id = u.id
  STRATEGY MATERIALIZE
  REFRESH EVERY 60 SECONDS
```

Spills to Badger on disk when the materialized index exceeds the memory limit.

**Strategy C — Change-Driven Index (for CDC sources)**

When the table side is a Debezium CDC stream, build and maintain a local hash index by replaying the changelog. No polling, no full scan — the index is always current. Memory footprint is proportional to the number of distinct keys that appear in join results, not the full table size.

```sql
SELECT e.user_id, u.email, e.event_type
FROM 'kafka://broker:9092/events' e
JOIN 'kafka://broker:9092/users.cdc' u FORMAT DEBEZIUM
  ON e.user_id = u.id
```

**Planner strategy selection:**

| Source type | Estimated size | Default strategy |
|---|---|---|
| Relational DB (Postgres, MySQL) | any | Batched Live Lookup |
| File / object store (Parquet, CSV) | ≤ memory limit | Materialized Hash Index |
| File / object store (Parquet, CSV) | > memory limit | Batched Live Lookup (partition scan) |
| Debezium CDC stream | any | Change-Driven Index |

User can always override with `STRATEGY MATERIALIZE`, `STRATEGY LOOKUP`, or `STRATEGY CDC`.

### 9.3 Stream-Stream Join (Interval Join)

Both streams must have a time window within which a match is possible. `neql` buffers records within the interval in an in-memory ring buffer (spills to disk). Matches are emitted as found.

```sql
SELECT a.order_id, b.payment_id
FROM 'kafka://broker:9092/orders' a
JOIN 'kafka://broker:9092/payments' b
  ON a.order_id = b.order_id
  AND b.event_time BETWEEN a.event_time AND a.event_time + INTERVAL 10 MINUTES
```

The join window must be bounded. `neql` will error if no time bound is specified for a stream-stream join.

### 9.4 LEFT / RIGHT / FULL Joins

Supported for table-table. Supported for stream-table (LEFT: stream records with no match emit NULLs for table columns). Stream-stream OUTER joins require a watermark advance to trigger NULL emission and are Phase 4 scope.

### 9.5 Cross-Source Type Coercion

When joining sources with different type systems (e.g., Avro `long` to Postgres `bigint`), `neql` applies automatic safe coercion. A coercion warning is printed if lossy coercion occurs. Explicit `CAST` is available.

### 9.6 Predicate and Projection Pushdown

The query planner analyzes every source reference and pushes as much computation as possible to the source system before any data is transferred. This is critical for large sources.

**Projection pushdown:** Only columns referenced in `SELECT`, `WHERE`, `JOIN ON`, `GROUP BY`, or `ORDER BY` are requested from the source. For columnar formats (Parquet, Avro), unneeded columns are never deserialized.

**Predicate pushdown:** Filter conditions on source columns are translated to the source's native query language and applied before transfer:
- Relational DBs: `WHERE` clause appended to the generated SQL
- Parquet: row group min/max statistics used to skip non-matching row groups
- Hive-partitioned object storage: partition column filters used to prune which files are read
- Kafka: no predicate pushdown (Kafka is a log; filtering must happen locally)

**Example — what gets pushed where:**

```sql
SELECT e.user_id, u.email
FROM 'kafka://broker:9092/events' e          -- no pushdown possible
JOIN 'pg://localhost/mydb/users' u
  ON e.user_id = u.id
WHERE u.region = 'us-east'                   -- pushed to Postgres: WHERE region='us-east'
  AND e.event_type = 'purchase'              -- applied locally to Kafka stream
```

The Postgres query neql actually issues: `SELECT id, email FROM users WHERE region = 'us-east' AND id IN (...batch...)` — combining static predicate pushdown with batched lookup.

---

## 10. Incremental Computation Model

### 10.1 Motivation

Traditional stream processors handle aggregations by maintaining per-key accumulators (`SUM`, `COUNT`, etc.) that are updated as new records arrive. This works for simple cases but fails for:

- **Retractions**: a Debezium `UPDATE` event is semantically a deletion of the old row plus an insertion of the new row. If that row contributed to a running aggregate, the aggregate must subtract the old contribution and add the new one. Most processors treat this as ad-hoc special casing.
- **Late data in large windows**: a 7-day sliding window that receives a late record must correctly update the result without rescanning the entire window.
- **Multi-hop joins**: if `A JOIN B JOIN C` and a row in `B` changes, the result must be updated — traditional processors require careful manual retraction handling.

**Differential dataflow** (pioneered by Frank McSherry; the foundation of Materialize and Feldera) solves this systematically: all data is represented as `(record, timestamp, diff)` triples where `diff` is `+1` (insert) or `-1` (retract/delete). Every operator is designed to process these deltas incrementally. A change to any input propagates correctly through the entire computation graph by processing only the delta — not recomputing from scratch.

neql adopts these principles progressively. The goal is to be diff-aware from day one so that incremental correctness is the default, not an afterthought.

### 10.2 The Diff-Aware Record

Starting in Phase 2, every record in neql's internal representation carries a `Diff` field:

```go
type Record struct {
    Values    []Value   // column data (Arrow-compatible)
    Timestamp time.Time // event time
    Diff      int8      // +1 = insert, -1 = retract
}
```

For sources that don't produce retractions (plain Kafka NDJSON, CSV files), `Diff` is always `+1`. For Debezium CDC sources, `Diff` is set from the `op` field:
- `op=c` (create) → `Diff=+1` on `_after`
- `op=d` (delete) → `Diff=-1` on `_before`
- `op=u` (update) → `Diff=-1` on `_before`, `Diff=+1` on `_after` (two records emitted)
- `op=r` (read/snapshot) → `Diff=+1`

This means a Debezium stream is a correct retraction stream out of the box.

### 10.3 Retraction-Aware Operators

Each operator handles `Diff` correctly:

**Filter:** pass through with original `Diff`. If a retracted record matches the filter, emit the retraction.

**Project:** pass through `Diff` unchanged.

**Aggregate (GROUP BY):** maintain per-key accumulators. For `Diff=+1`, add to accumulator; for `Diff=-1`, subtract. When an accumulator reaches zero, remove the key. Emit updated aggregate result (with a retraction of the previous result if the key already existed).

```
Input: (user_id=42, amount=100, Diff=+1)   → SUM(amount) for key 42 = 100  → emit (42, 100, +1)
Input: (user_id=42, amount=100, Diff=-1)   → SUM(amount) for key 42 = 0    → emit (42, 100, -1), (42, 0, +1)
Input: (user_id=42, amount=200, Diff=+1)   → SUM(amount) for key 42 = 200  → emit (42, 0, -1), (42, 200, +1)
```

**Join (stream-table, Change-Driven Index):** when the index receives a retraction for a key, emit retractions for all previously-emitted join results that referenced that key, then emit new results for the replacement value (if any).

**Window aggregation:** records entering a window contribute `Diff=+1`; records leaving a sliding window contribute `Diff=-1`. The aggregation operator handles both identically. A 7-day sliding window has the same per-record cost as a 1-minute window.

### 10.4 Incremental Aggregation — Supported Functions

| Aggregate | Incremental? | Notes |
|---|---|---|
| `COUNT(*)` | Yes | Increment/decrement a counter |
| `SUM(x)` | Yes | Add/subtract value |
| `AVG(x)` | Yes | Maintain sum + count separately |
| `MIN(x)`, `MAX(x)` | Partial | Efficient for insert-only; retractions require re-scan of candidates (use min-heap) |
| `COUNT(DISTINCT x)` | Approximate | HyperLogLog; exact requires full set, which is expensive |
| `FIRST(x)`, `LAST(x)` | Yes (with ordering) | Maintain ordered list of candidates |
| `ARRAY_AGG(x)` | Yes | Maintain list; remove on retraction |
| `APPROX_PERCENTILE(x, p)` | Yes | t-digest or DDSketch, both retraction-aware |

### 10.5 Roadmap Toward Full Differential Dataflow

neql approaches DD incrementally:

| Phase | DD Capability Added |
|---|---|
| Phase 2 | Diff-aware record type; retraction-aware filter/project/aggregate; Debezium retractions |
| Phase 3 | Change-Driven Index join (retraction-aware stream-table join) |
| Phase 4 | Sliding window retraction; multi-hop join retraction propagation |
| Phase 5+ | Full DD arrangements (sorted indexed `(key, value, time, diff)` traces); correct incremental multi-way joins; recursive queries |

Phase 5 is explicitly a future research goal, not a committed deliverable. The value of designing toward it now is that we don't paint ourselves into a corner — diff-aware records and retraction-capable operators are the foundation, and they cost almost nothing to add in Phase 2.

**Relationship to Feldera/Materialize:** These systems implement full DD and are excellent products. neql's position is complementary: zero infrastructure, CLI-first, for ad-hoc and operational use cases. We are not trying to replace a persistent streaming database. If a user's query is complex enough to need full DD with persistence and high availability, Materialize is the right tool. neql's DD-inspired model gives *most* of the correctness benefit for *most* common queries without requiring a server.

---

## 11. Execution Engine Architecture

### 10.1 High-Level Architecture

```
  CLI Input (SQL string)
        │
        ▼
  ┌─────────────┐
  │  SQL Parser │  (custom recursive descent parser in Go, ANSI SQL + neql extensions)
  └──────┬──────┘
         │ AST
         ▼
  ┌──────────────┐
  │  Planner /   │  (logical plan → physical plan, predicate pushdown, source pruning)
  │  Optimizer   │
  └──────┬───────┘
         │ Physical Plan (DAG)
         ▼
  ┌──────────────────────────────────────────┐
  │            Execution Engine              │
  │                                          │
  │  ┌─────────┐   ┌─────────┐  ┌────────┐  │
  │  │ Source  │   │ Source  │  │ Source │  │  (connector plugins)
  │  │ Reader  │   │ Reader  │  │ Reader │  │
  │  └────┬────┘   └────┬────┘  └───┬────┘  │
  │       │             │           │        │
  │       └──────┬──────┘           │        │
  │              │                  │        │
  │         ┌────▼────┐        ┌────▼────┐   │
  │         │  Join   │        │ Filter  │   │
  │         │Operator │        │Operator │   │
  │         └────┬────┘        └────┬────┘   │
  │              └────────┬─────────┘        │
  │                   ┌───▼───┐              │
  │                   │Project│              │
  │                   │  /    │              │
  │                   │Aggr.  │              │
  │                   └───┬───┘              │
  └───────────────────────┼──────────────────┘
                          │
                     ┌────▼────┐
                     │  Sink   │  (stdout, file, Kafka, DB)
                     └─────────┘
```

### 10.2 Execution Model

- **Pull-based (volcano model)** for table-table queries: operators call `Next()` on their inputs, producing batches of rows (columnar Arrow record batches internally for efficiency).
- **Push-based** for streaming queries: source readers push records into operator pipelines via channels.
- Hybrid: stream-table joins use push from the stream side and pull for table-side probes.

### 10.3 Internal Columnar Format

All intermediate data is represented as [Apache Arrow](https://arrow.apache.org/) record batches in memory. This:
- Enables SIMD-optimized operations on numeric columns.
- Provides zero-copy interchange between operators.
- Makes Parquet / Arrow IPC reads nearly zero-cost (no deserialization for compatible types).

### 10.4 State Backend

Streaming operators that require state (aggregations, stream-stream joins, deduplication) use a pluggable state backend:

- **Default**: in-memory hash maps and ring buffers.
- **Overflow**: automatic spill to local RocksDB instance (embedded, no separate install).
- State is keyed by query fingerprint and can optionally be persisted across restarts (`--stateful` flag).

### 10.5 Parallelism

- For multi-partition Kafka topics, each partition is consumed by a separate goroutine. Results are merged by a fan-in operator that respects event-time ordering.
- For multi-file glob patterns, files are read in parallel (configurable concurrency).
- Operators run in a goroutine pipeline with buffered channels. Backpressure is propagated upstream automatically.
- `--parallelism N` flag controls max goroutine concurrency (defaults to `GOMAXPROCS`).

### 10.6 Memory Management

- Default working set limit: 2GB (configurable via `--memory-limit`).
- When memory pressure is detected, intermediate state is spilled to disk.
- Large table materializations for joins are spilled proactively based on estimated size.

---

## 12. CLI Design

### 11.1 Primary Command

```
neql [flags] <SQL>
neql [flags] -f <query-file.sql>
```

### 11.2 Examples

```bash
# Query a local CSV
neql "SELECT * FROM orders.csv WHERE amount > 100 ORDER BY amount DESC LIMIT 20"

# Stream Kafka topic, format as table
neql "SELECT _timestamp, user_id, event_type FROM 'kafka://localhost:9092/events'" | head -100

# Join Kafka to Postgres
neql "
  SELECT e.user_id, u.email, COUNT(*) AS events
  FROM 'kafka://localhost:9092/clickstream' e
  JOIN 'pg://localhost/prod/users' u ON e.user_id = u.id
  WINDOW TUMBLING(SIZE 1 MINUTE)
  GROUP BY WINDOW_START, e.user_id, u.email
"

# Query S3 Parquet with glob
neql "SELECT date, SUM(revenue) FROM 's3://my-bucket/sales/2026/**/*.parquet' GROUP BY date"

# Pipe stdin
kubectl logs my-pod -f | neql "SELECT message WHERE level='ERROR'"

# Inspect schema
neql schema 'kafka://localhost:9092/events'
neql schema ./data.parquet

# Describe a source (partitions, offsets, row count estimate)
neql describe 'kafka://localhost:9092/events'
neql describe 's3://my-bucket/sales/'
```

### 11.3 Subcommands

| Subcommand | Description |
|---|---|
| `neql <sql>` | Execute a SQL query (default) |
| `neql -f <file>` | Execute SQL from a file |
| `neql schema <source>` | Print inferred schema for a source |
| `neql describe <source>` | Print metadata about a source |
| `neql formats` | List supported formats |
| `neql connectors` | List available connectors and status |
| `neql version` | Print version info |

### 11.4 Output Flags

| Flag | Description |
|---|---|
| `--format table` | Aligned ASCII table (default for TTY) |
| `--format csv` | CSV output |
| `--format json` | NDJSON output |
| `--format parquet` | Write to Parquet file (requires `--output`) |
| `--output <file-or-uri>` | Write output to file or data source URI |
| `--no-header` | Suppress header row in CSV/table output |
| `--pretty` | Pretty-print JSON output |

### 11.5 Control Flags

| Flag | Description |
|---|---|
| `--timeout <duration>` | Terminate after N seconds/minutes |
| `--limit N` | Terminate after N rows (overrides SQL LIMIT) |
| `--parallelism N` | Max goroutine concurrency |
| `--memory-limit <bytes>` | Working set memory limit |
| `--stateful` | Persist stream state across restarts |
| `--state-dir <path>` | Directory for spilled state (default: `~/.neql/state`) |
| `--log-level debug\|info\|warn\|error` | Logging verbosity |
| `--dry-run` | Print query plan without executing |
| `--explain` | Print physical query plan and estimated costs |

### 11.6 Credential Management

Credentials are never passed on the command line (security risk). Options:

1. **URI userinfo** (convenient, not recommended for prod): `pg://user:pass@host/db/table`
2. **Environment variables**: `NEQL_PG_PASSWORD`, `NEQL_KAFKA_SASL_PASSWORD`, etc.
3. **Credential file**: `~/.neql/credentials` (TOML format, 0600 permissions)
4. **OS keychain** (v2): integration with macOS Keychain / Linux Secret Service

```toml
# ~/.neql/credentials
[kafka.my-cluster]
brokers = ["pkc-xxx.confluent.cloud:9092"]
sasl_mechanism = "PLAIN"
sasl_username = "my-key"
sasl_password = "my-secret"

[pg.prod]
host = "prod-db.internal"
user = "readonly"
password = "hunter2"
```

Alias in query:

```sql
FROM 'kafka://my-cluster/events'     -- resolved from credentials file
FROM 'pg://prod/mydb/orders'         -- resolved from credentials file
```

### 11.7 Query File Support

```sql
-- query.sql
-- neql: timeout=5m
-- neql: format=csv

SELECT user_id, COUNT(*) AS events
FROM 'kafka://prod/clickstream'
WHERE event_type = 'purchase'
WINDOW TUMBLING(SIZE 1 HOUR)
GROUP BY WINDOW_START, user_id
EMIT ON WINDOW CLOSE
```

```bash
neql -f query.sql --output results.csv
```

---

## 13. Output Sinks

| Sink | URI / Flag | Notes |
|---|---|---|
| stdout | (default) | respects `--format` |
| File | `--output /path/to/file.csv` | format inferred from extension |
| S3 / GCS | `--output s3://bucket/prefix/out.parquet` | streaming writes via multipart upload |
| Kafka | `--output kafka://broker/topic` | v2, requires schema |
| Postgres / MySQL | `--output pg://host/db/table` | v2, INSERT or UPSERT |
| SQLite | `--output sqlite:///path/db.sqlite/table` | v1, useful for local storage of results |

---

## 14. Error Handling and Observability

### 13.1 Query Errors

- SQL parse errors: precise location + suggestion (e.g., "Did you mean `TUMBLING` instead of `TUMBLE`?")
- Schema mismatch: show inferred schema vs. query references
- Source unavailable: clear connectivity message with broker/host + port

### 13.2 Runtime Metrics

When `--log-level debug` or `--metrics` flag is set, `neql` emits:

- Records ingested per source per second
- Watermark lag
- Join buffer size
- Memory usage
- Spill events

### 13.3 Dead Letter Handling

Late records and deserialization errors are reported to stderr by default. Optionally routed:

```bash
neql "..." --dead-letter /tmp/dead-letters.ndjson
```

---

## 15. Implementation Phases

### Phase 1 — Table SQL via DuckDB (MVP)

**Goal:** Query local files and databases with standard SQL. No streaming. Ship fast by using DuckDB as the execution engine. neql's job is URI translation, credential management, and output formatting.

**Architecture:** neql parses just enough SQL to extract source URIs, rewrites them to DuckDB's native scanner functions (`read_parquet()`, `read_csv_auto()`, etc.), then hands the transformed query to an embedded DuckDB instance for execution. DuckDB returns Arrow record batches; neql formats and outputs them.

```
User SQL → neql URI rewriter → DuckDB SQL → DuckDB engine → Arrow batches → neql output formatter
```

**Deliverables:**
- neql URI translator: maps `'s3://bucket/file.parquet'` → `read_parquet('s3://...')`, bare paths → appropriate scanner, `pg://` → DuckDB's Postgres scanner, etc.
- DuckDB embedded executor (via `github.com/marcboeker/go-duckdb`)
- Output: stdout table (TTY), CSV, NDJSON
- `neql schema <source>` subcommand
- Credential file (`~/.neql/credentials`) for DB sources

**Connectors in scope:** local CSV, NDJSON, Parquet, SQLite, Postgres, MySQL, S3, GCS (all via DuckDB scanners)

**Done when:** `neql "SELECT * FROM orders.csv JOIN 'pg://localhost/db/users' u ON o.user_id = u.id"` works end-to-end, and `neql "SELECT * FROM 's3://bucket/data/**/*.parquet' WHERE amount > 100"` works against real S3.

**Note on DuckDB:** DuckDB is the execution engine for this phase only. We do not expose DuckDB's SQL syntax extensions to users — all input SQL is neql SQL, which happens to be expressible via DuckDB for table queries. In Phase 2 we introduce our own execution layer for streaming; DuckDB transitions to a table-side probe engine for stream-table joins.

---

### Phase 2 — Kafka Streams + Debezium

**Goal:** Stream Kafka topics with filtering, projection, and windowed aggregation. Support Debezium CDC format as a first-class format.

**Architecture:** Introduce neql's own push-based streaming execution layer alongside DuckDB. The SQL parser is extended with streaming clauses. Table queries still route to DuckDB; stream queries run through the new engine.

**Deliverables:**
- neql SQL parser (custom recursive-descent, Go) — covers `SELECT`, `FROM`, `WHERE`, `JOIN`, `GROUP BY`, `ORDER BY`, `LIMIT`, `WINDOW`, `EMIT`, `EVENT TIME BY`, `WATERMARK`
- Streaming execution engine: push-based goroutine pipeline, Arrow record batch boundaries
- Kafka connector (`franz-go`): SASL/PLAIN, SASL/SCRAM, SASL/OAUTHBEARER, mTLS; ephemeral consumer groups; offset control via URI params
- Formats: NDJSON, Avro (with Confluent Schema Registry auto-detection via magic byte `0x00`)
- `FORMAT DEBEZIUM`: unwraps Debezium envelope, exposes `_op`, `_before`, `_after`, `_db`, `_table`, `_ts` virtual columns; treats the stream as a changelog
- `WINDOW TUMBLING / HOPPING / SESSION` clauses
- `EMIT ON WINDOW CLOSE / EVERY <duration>` clause
- Virtual columns: `_offset`, `_partition`, `_timestamp`, `_key`
- `neql describe <source>` subcommand (partition count, watermarks, lag)
- `--output <file>` sink (local filesystem, format inferred from extension)

**Done when:**
- `neql "SELECT user_id, COUNT(*) FROM 'kafka://localhost:9092/clicks' WINDOW TUMBLING(SIZE 1 MINUTE) GROUP BY WINDOW_START, user_id"` streams live output.
- `neql "SELECT _op, _after->>'order_id' AS order_id FROM 'kafka://localhost:9092/orders.debezium' FORMAT DEBEZIUM WHERE _op IN ('c','u')"` works against a Debezium-produced topic.

---

### Phase 3 — Stream-Table Joins and Object Storage

**Goal:** Join Kafka streams to reference tables. Glob patterns for multi-file object storage reads.

**Architecture:** Stream-table lookup join: stream drives, DuckDB materializes and probes the table side. For each micro-batch of stream records, neql passes them to DuckDB as an Arrow record batch scan, runs the join, retrieves results. Table state is periodically refreshed.

**Deliverables:**
- Stream-table lookup join operator (DuckDB as probe engine via Arrow round-trip)
- Table materialization with optional `REFRESH EVERY <duration>`
- Spill-to-disk for large tables: Badger embedded KV store (pure Go, no CGo)
- Glob patterns in object storage URIs (`s3://bucket/2026/**/*.parquet`)
- Hive partition pruning
- Parquet predicate pushdown
- Protobuf format support (with `.proto` descriptor file or Confluent Schema Registry)
- `--output s3://` and `--output gcs://` sinks

**Done when:** `neql "SELECT e.*, u.email FROM 'kafka://...' e JOIN 's3://bucket/users.parquet' u ON e.user_id = u.id"` produces correct joined output from a live Kafka topic.

---

### Phase 4 — Stream-Stream Joins, Hardening, Performance

**Goal:** Stream-stream interval joins; production-grade reliability; replace DuckDB with native table engine if benchmarks warrant it.

**Deliverables:**
- Stream-stream interval join operator (bounded time window required; ring buffer with Badger spill)
- Watermark advancement and late data routing (`--dead-letter`)
- `EMIT ON EVERY RECORD` mode
- Full `LEFT JOIN` support for stream-table (NULL emission for unmatched stream records)
- Credential file: named alias support (`FROM 'kafka://my-cluster/topic'`)
- Integration test suite (testcontainers: Kafka, Postgres, MySQL, MinIO)
- Performance optimization pass against Phase 2/3 throughput targets
- DuckDB replacement evaluation: profile stream-table join hot path; replace if neql native engine is competitive

**Done when:** stream-stream join with two Kafka topics, bounded interval, correct watermark-driven output.

---

## 16. Performance Goals

| Scenario | Target |
|---|---|
| CSV scan (1M rows, 10 cols, SSD) | < 2 seconds |
| Parquet scan (100M rows, predicate pushdown, S3) | < 30 seconds |
| Kafka ingest throughput | > 500K records/sec (simple filter/project) |
| Stream-table join throughput | > 200K records/sec (table < 10M rows in-memory) |
| Memory for 1M row table materialization | < 500 MB |
| Cold start (binary launch to first output) | < 200 ms |

---

## 17. Technology Decisions

### Language: Go

- Single binary compilation, excellent concurrency primitives, strong ecosystem for Kafka (franz-go), databases, and Arrow.
- No Rust required for v1 — Go's performance is sufficient for the targeted throughput. Rust FFI considered for hot path SIMD operators in v2 if profiling indicates need.

### Key Libraries

| Phase | Purpose | Library |
|---|---|---|
| 1+ | Table execution engine | `github.com/marcboeker/go-duckdb` (embedded DuckDB via CGo) |
| 1+ | Arrow / columnar processing | `github.com/apache/arrow/go` |
| 1+ | CLI | `github.com/spf13/cobra` |
| 1+ | Table output formatting | `github.com/olekukonko/tablewriter` |
| 2+ | SQL parsing (custom) | internal `neql/sql/parser` package |
| 2+ | Kafka | `github.com/twmb/franz-go` |
| 2+ | Avro | `github.com/linkedin/goavro/v2` |
| 2+ | Protobuf | `google.golang.org/protobuf` |
| 3+ | Embedded state / spill | `github.com/dgraph-io/badger/v4` (pure Go, no CGo) |
| 3+ | Parquet (direct read) | `github.com/parquet-go/parquet-go` |
| 3+ | Object storage | `github.com/aws/aws-sdk-go-v2`, `cloud.google.com/go/storage` |
| 1+ | Postgres | `github.com/jackc/pgx/v5` (also used by DuckDB scanner) |
| 1+ | MySQL | `github.com/go-sql-driver/mysql` |

### Execution Architecture by Phase

**Phase 1:** DuckDB is the full engine. neql = URI translator + credential injector + output formatter. No custom parser needed yet.

**Phase 2:** neql custom parser introduced. For table-only queries, the AST is compiled to DuckDB SQL and executed via DuckDB. For stream queries, a new push-based engine handles execution. DuckDB is now a sub-component.

**Phase 3+:** DuckDB serves as the table-probe engine in stream-table joins, accessed via Arrow IPC round-trips. In Phase 4 we evaluate replacing it with a native engine if the Arrow round-trip overhead becomes a bottleneck.

### SQL Parser: Custom (Phase 2)

We write a custom recursive-descent parser rather than using an off-the-shelf SQL parser (Vitess sqlparser, pg_query_go, etc.). Reasons:
- We need streaming extensions (`WINDOW`, `EMIT`, `EVENT TIME BY`, `WATERMARK`) that don't exist in any existing parser grammar.
- Error message quality is a first-class concern — off-the-shelf parsers produce opaque errors.
- The grammar is intentionally a subset of SQL; we don't want to inherit the complexity of a full ANSI SQL parser.
- Writing a clean recursive-descent parser for our subset is a well-understood task, estimated at 2–3 weeks.

In Phase 1, neql does not need the custom parser — DuckDB's parser handles the table SQL. The custom parser is introduced at the start of Phase 2 alongside the streaming engine.

### State Store: Badger (Phase 3)

We choose `badger` (pure Go) over RocksDB (CGo) for the embedded state/spill store:
- No CGo: simpler cross-compilation, no system library dependency, easier Docker builds.
- Sufficient performance for our spill use cases (this is overflow, not the hot path).
- Re-evaluate in Phase 4 if profiling shows Badger is a bottleneck.

---

## 18. Decided Questions

| Question | Decision | Rationale |
|---|---|---|
| DuckDB vs. custom engine for Phase 1 | DuckDB | Ships faster; Arrow-native; table SQL coverage is excellent. Custom parser deferred to Phase 2 when streaming extensions are needed. |
| State store | Badger (pure Go) | No CGo, simpler builds. Re-evaluate in Phase 4 if benchmarks demand RocksDB. |
| Schema registry auto-detection | Yes | Auto-detect Confluent wire format magic byte (`0x00` + 4-byte schema ID). |
| Streaming output format on non-TTY | Yes | Auto-switch `--format table` to NDJSON when stdout is not a terminal. |
| WATCH mode | Deferred post-Phase 1 | Not blocking any known use case. |
| Plugin architecture | Compiled-in connectors for all phases | No .so hell. Revisit if third-party connector ecosystem emerges. |
| Windows support | Deferred | DuckDB CGo and Badger are both manageable on Windows, but not a priority. Target Linux + macOS. |
| Native CDC (Postgres/MySQL binlog) | Deferred (post-Phase 4) | Debezium-over-Kafka covers the practical CDC use case in Phase 2 without requiring binlog access. |
| Sinks / output targets | Filesystem only (Phase 1–2) | `--output <file>` to local disk with format inferred from extension. Object storage sinks in Phase 3. Kafka/DB sinks post-Phase 4. |

## 19. Open Questions

1. **Name**: `neql` ("Any Query Language") is the working name. No hard constraints — open to change before any public release.
2. **Debezium schema evolution**: When Debezium changes a table schema mid-stream (column added/removed), how do we handle schema transitions in the output Arrow batches? Options: fail with a clear error, drop mismatched columns, use a union schema. Need a decision before Phase 2 ships.
3. **DuckDB Postgres scanner vs. pgx directly**: DuckDB has a native Postgres scanner extension. Should we use it for `pg://` sources in Phase 1 (simpler), or use `pgx` directly (more control, no CGo dependency on the DuckDB extension)? Lean DuckDB scanner for Phase 1; switch to pgx in Phase 2 when we control the execution path.
4. **Debezium key handling**: Debezium keys are often composite (multi-field primary key serialized as JSON or Avro). Should `_key` auto-parse the Debezium key format, or remain raw bytes? Probably auto-parse — the raw bytes are rarely useful.

---

## 20. Success Metrics

- Time-to-first-query for a new user: < 5 minutes from `brew install` to first working Kafka query.
- GitHub stars / adoption as a proxy for developer love.
- P50 query latency for simple Kafka filter queries: < 500ms from query submission to first output record.
- Zero required config for local file and stdin queries.
- Support for the 5 most common Kafka auth mechanisms without documentation lookup.

---

*End of PRD v0.1*
