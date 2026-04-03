# FoldDB

**Streaming SQL with incremental aggregation.**

FoldDB is a zero-infrastructure CLI that executes SQL queries against Kafka topics, CDC streams, and files — with correct incremental aggregation using [Z-set / DBSP](https://folddb.cantwell.dev/concepts/diff-model/) semantics.

```bash
# Count orders by status from a Kafka CDC stream, updating in real time
folddb "SELECT status, COUNT(*) AS orders
        FROM 'kafka://broker:9092/orders.cdc' CHANGELOG DEBEZIUM
        GROUP BY status"
```

## Features

- **Zero infrastructure.** Single binary. No server, no cluster, no daemon.
- **Streaming SQL.** Query Kafka topics, Debezium CDC streams, and stdin pipes.
- **Z-set model.** Correct incremental aggregation with retractions — when source data changes, results update correctly.
- **Joins.** Differential dataflow joins between streams and files (Parquet, CSV, JSON, Avro).
- **DuckDB inside.** File queries use embedded DuckDB for fast scanning with predicate pushdown.
- **Multiple formats.** NDJSON, CSV, Avro, Protobuf, Parquet, Debezium JSON/Avro.
- **Dot notation.** `data.user.email` — natural JSON field access alongside PostgreSQL `->>`/`->`.

## Install

```bash
go install github.com/kevin-cantwell/folddb/cmd/folddb@latest
```

## Quick Examples

```bash
# Filter API errors from a log stream
tail -f /var/log/api.json | folddb "SELECT endpoint, status_code WHERE status_code >= 400"

# Aggregate with windowing
folddb "SELECT window_start, endpoint, COUNT(*) AS reqs
        FROM 'kafka://broker/api_requests'
        GROUP BY endpoint
        WINDOW TUMBLING '1 minute'
        EVENT TIME BY timestamp"

# Join stream to Parquet reference table
cat events.ndjson | folddb "SELECT e.user_id, u.name, e.action
                             FROM stdin e
                             JOIN '/data/users.parquet' u ON e.user_id = u.id"

# Query a Parquet file directly (via DuckDB)
folddb "SELECT region, COUNT(*) FROM '/data/orders.parquet' GROUP BY region"

# Serve results via HTTP (sidecar mode)
folddb serve --port 8080 "SELECT region, COUNT(*)
                           FROM 'kafka://broker/orders.cdc' CHANGELOG DEBEZIUM
                           GROUP BY region"
```

## Performance

Stress test results on Apple M4:

| Scenario | Throughput |
|---|---|
| NDJSON passthrough (5M records) | 476K/sec |
| GROUP BY with 4 aggregates (5M records) | 389K/sec |
| CDC Debezium retractions (2M records) | 207K/sec |
| 100K unique group keys (1M records) | 382K/sec |
| 50-column wide records (500K records) | 37K/sec |
| JOIN with --max-memory spill-to-disk (1M records) | 297K/sec |

See [full benchmarks](https://folddb.cantwell.dev/architecture/performance/).

## Documentation

**[folddb.cantwell.dev](https://folddb.cantwell.dev)**

- [When to Use FoldDB](https://folddb.cantwell.dev/concepts/when-to-use/) — honest comparison with Flink, Materialize, DuckDB, ksqlDB
- [The Z-Set Model](https://folddb.cantwell.dev/concepts/diff-model/) — how incremental aggregation works
- [SQL Reference](https://folddb.cantwell.dev/reference/sql/) — full dialect documentation
- [Architecture](https://folddb.cantwell.dev/architecture/overview/) — pipeline, accumulators, joins, checkpointing

## License

[MIT](LICENSE)
