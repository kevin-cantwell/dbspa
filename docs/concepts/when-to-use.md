# When to Use DBSPA

DBSPA occupies a specific niche in the streaming data toolchain. This page is an honest comparison with other tools so you can pick the right one for your situation.

## The landscape

```
Capability
    ↑
    │   Flink / Materialize
    │   Production streaming. Complex joins. Large windows.
    │   Exactly-once. Horizontal scale. 24/7 operation.
    │
    │   ─────────── complexity ceiling ───────────
    │
    │   DBSPA
    │   Ad-hoc streaming SQL. Zero setup. Single machine.
    │   CDC + joins from the terminal. Prototyping.
    │
    │   ─────────── simplicity floor ─────────────
    │
    │   kafkacat + jq / kcat
    │   View raw messages. Simple text filtering.
    │
    └──────────────────────────────────────────→ Setup effort
```

## DBSPA vs Flink

[Apache Flink](https://flink.apache.org/) is a distributed stream processing engine designed for production pipelines that run 24/7.

| | DBSPA | Flink |
|---|---|---|
| Install | `brew install dbspa` | Cluster: JobManager + TaskManagers + ZooKeeper |
| First query | 10 seconds | 30+ minutes (write job, configure, deploy) |
| State backend | In-memory + checkpoint to disk | RocksDB (distributed, terabytes) |
| Exactly-once processing | Yes, with `DEDUPLICATE BY` | Yes (built-in) |
| Horizontal scale | No | Yes |
| Fault tolerance | Checkpoint to disk | Distributed snapshots |
| Throughput | ~100K events/sec | Millions/sec |
| Large windows (48h+) | Disk spill via `--spill-to-disk` / `--max-memory` | Handles via RocksDB |

**Use Flink when:** You need production-grade streaming with large state, exactly-once guarantees, horizontal scaling, or complex event processing at high throughput.

**Use DBSPA when:** You need to answer a question about streaming data right now, from your terminal, without deploying anything. Or you're prototyping a streaming query before implementing it in Flink.

## DBSPA vs Materialize

[Materialize](https://materialize.com/) is a streaming database that maintains incrementally-updated materialized views using DBSP (the same Z-set algebra that DBSPA uses internally).

| | DBSPA | Materialize |
|---|---|---|
| Install | Single binary, no server | Docker Compose or cloud service |
| Architecture | CLI tool (no daemon) | Server (PostgreSQL wire protocol) |
| First query | Pipe data in, get answer | Start server, CREATE SOURCE, CREATE VIEW |
| SQL dialect | PostgreSQL subset + streaming extensions | Full PostgreSQL |
| Incremental model | Z-set / DBSP (simplified) | Z-set / DBSP (complete) |
| Subqueries | Not yet | Full support |
| Recursive queries | No | Yes |
| State persistence | Checkpoint + SQLite | Persistent arrangements |
| Multi-user | No | Yes (Postgres protocol) |

**Use Materialize when:** You need a persistent streaming database with full SQL, multiple connected clients, complex view hierarchies, or recursive queries.

**Use DBSPA when:** You don't want to run a server. You need a quick answer from the terminal. You're exploring data interactively. You want to pipe structured data through SQL.

## DBSPA vs DuckDB

[DuckDB](https://duckdb.org/) is an in-process analytical database optimized for batch queries on files and tables.

| | DBSPA | DuckDB |
|---|---|---|
| Batch query speed | ~100K records/sec | ~5M+ records/sec (vectorized SIMD) |
| Kafka / streaming | Yes (native) | No |
| CDC / Debezium | Yes (retractions, Z-set) | No |
| Incremental aggregation | Yes (live-updating) | No (re-query each time) |
| Joins | DD join (stream-to-table, stream-to-stream) | Hash join, sort-merge (batch) |
| File formats | Parquet, CSV, JSON, Avro, Protobuf | Parquet, CSV, JSON, and many more |

**Use DuckDB when:** You have files and you want fast batch analytics. One-shot queries. Data exploration on static datasets.

**Use DBSPA when:** You're querying live Kafka streams or CDC data. You need aggregations that update in real time. You're joining a stream against a reference table. DBSPA actually uses DuckDB internally for file scanning — you get DuckDB's read performance with streaming semantics on top.

## DBSPA vs kafkacat / kcat + jq

[kcat](https://github.com/edenhill/kcat) is the standard CLI for reading and writing Kafka messages. Combined with `jq`, it covers basic message inspection.

| | DBSPA | kcat + jq |
|---|---|---|
| Read Kafka | Yes | Yes |
| Filter messages | SQL WHERE | jq expressions |
| Aggregate | GROUP BY, COUNT, SUM, etc. | No (manual scripting) |
| Join to reference data | Yes (DD join) | No |
| CDC retractions | Yes (Debezium format) | No |
| Windowed aggregation | Yes | No |

**Use kcat + jq when:** You just need to see raw Kafka messages or do simple field extraction.

**Use DBSPA when:** You need to aggregate, join, window, or apply SQL logic to the stream.

## DBSPA vs ksqlDB

[ksqlDB](https://ksqldb.io/) is Confluent's streaming SQL engine for Kafka.

| | DBSPA | ksqlDB |
|---|---|---|
| Install | Single binary | Requires Kafka Connect, Schema Registry, ksqlDB server |
| Infrastructure | Zero | Significant (JVM services) |
| Streaming SQL | Yes | Yes |
| Push queries | TUI / changelog / serve | Yes (native) |
| Connectors | Kafka, files, stdin | Kafka only (via Kafka Connect) |
| Local files | Yes | No |
| Production deployment | `dbspa serve` (lightweight) | Full cluster |

**Use ksqlDB when:** You're already running the Confluent Platform and need persistent streaming queries with Kafka Connect integration.

**Use DBSPA when:** You want streaming SQL without the Confluent Platform overhead. Or you need to query local files alongside Kafka topics.

## What DBSPA is best at

1. **Zero-to-answer in seconds.** No infrastructure to set up, no server to start, no source to register. Pipe data in, get SQL results out.

2. **CDC exploration.** "What's changing in my database right now?" is a one-liner with Debezium format support and correct retraction semantics.

3. **Stream + table joins from the terminal.** Enrich Kafka events with a Parquet reference table without writing a Flink job.

4. **Prototyping streaming queries.** Write and validate your SQL locally, then port to Flink or Materialize for production.

5. **Lightweight production serving.** `dbspa serve` as a Kubernetes sidecar for simple streaming aggregations that don't justify a Flink cluster.

6. **Universal SQL layer over CLI tools.** With `EXEC()`, DBSPA can query the output of any command-line tool -- `kubectl`, `psql`, `bq`, `aws`, `curl`, `jq` -- using SQL. Any tool that can produce JSON, CSV, or structured output becomes a queryable data source. This makes DBSPA a composable SQL layer for the entire Unix toolchain.

## What DBSPA is not designed for

- **Large state** (multi-GB sliding windows) — use Flink with RocksDB
- **Exactly-once processing** — use Flink with checkpointed state
- **Horizontal scaling** — use Flink or Kafka Streams
- **Complex SQL** (recursive CTEs, correlated subqueries) — use Materialize
- **Fast batch analytics on files** — use DuckDB directly
- **Multi-user access** — use Materialize (PostgreSQL protocol)
