# DBSPA

**Streaming SQL with incremental aggregation.**

DBSPA is a zero-infrastructure CLI that executes SQL queries against Kafka topics, CDC streams, and files — with correct incremental aggregation using [Z-set / DBSP](https://kevin-cantwell.github.io/dbspa/docs/latest/concepts/diff-model/) semantics.

```bash
# Count orders by status from a Kafka CDC stream, updating in real time
dbspa "SELECT status, COUNT(*) AS orders
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
go install github.com/kevin-cantwell/dbspa/cmd/dbspa@latest
```

## Quick Examples

```bash
# Filter API errors from a log stream
tail -f /var/log/api.json | dbspa "SELECT endpoint, status_code WHERE status_code >= 400"

# Aggregate with windowing
dbspa "SELECT window_start, endpoint, COUNT(*) AS reqs
        FROM 'kafka://broker/api_requests'
        GROUP BY endpoint
        WINDOW TUMBLING '1 minute'
        EVENT TIME BY timestamp"

# Join stream to Parquet reference table
cat events.ndjson | dbspa "SELECT e.user_id, u.name, e.action
                             FROM stdin e
                             JOIN '/data/users.parquet' u ON e.user_id = u.id"

# Query a Parquet file directly (via DuckDB)
dbspa "SELECT region, COUNT(*) FROM '/data/orders.parquet' GROUP BY region"

# Serve results via HTTP (sidecar mode)
dbspa serve --port 8080 "SELECT region, COUNT(*)
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

See [full benchmarks](https://kevin-cantwell.github.io/dbspa/docs/latest/architecture/performance/).

## Documentation

**[kevin-cantwell.github.io/dbspa/docs/latest](https://kevin-cantwell.github.io/dbspa/docs/latest)**

- [When to Use DBSPA](https://kevin-cantwell.github.io/dbspa/docs/latest/concepts/when-to-use/) — honest comparison with Flink, Materialize, DuckDB, ksqlDB
- [The Z-Set Model](https://kevin-cantwell.github.io/dbspa/docs/latest/concepts/diff-model/) — how incremental aggregation works
- [SQL Reference](https://kevin-cantwell.github.io/dbspa/docs/latest/reference/sql/) — full dialect documentation
- [Architecture](https://kevin-cantwell.github.io/dbspa/docs/latest/architecture/overview/) — pipeline, accumulators, joins, checkpointing

## License

[MIT](LICENSE)
