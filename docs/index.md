# DBSPA

**Database Stream Processing Accumulator.**

DBSPA is a CLI tool that brings the power of [DBSP](https://arxiv.org/abs/2203.16684) — normally reserved for heavyweight deployments like [Feldera](https://feldera.com) and [Materialize](https://materialize.com) — to your command line.

```bash
dbspa "SELECT region, SUM(quantity) AS total_quantity
        FROM 'kafka://brokers:9092/orders.cdc' CHANGELOG DEBEZIUM
        GROUP BY region"
```

## What is DBSP?

DBSP (Database Stream Processors) is a mathematical framework for incremental computation over streams, introduced in the [VLDB 2023 paper](https://arxiv.org/abs/2203.16684) *"DBSP: Automatic Incremental View Maintenance for Rich Query Languages"* (Budiu, McSherry, Ryzhyk, Tannen — Best Paper Award). It is rooted in the theory of [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow). The key insight is that instead of recomputing query results from scratch on every new event, you can express any relational operator — filters, joins, aggregations, even recursive queries — as an *incremental* operator that processes only the *changes* (deltas) to its inputs.

Changes are represented as **Z-sets**: multisets of records with integer weights, where `+1` means an insertion and `-1` means a retraction. Operators compose over Z-sets the same way they compose over ordinary tables, which means you get correct, consistent query results as streams evolve — including accurate retractions when upstream data is corrected or deleted.

DBSPA implements this model for the command line: you write a SQL query, point it at a Kafka topic or a stdin pipe, and get a live, incrementally-maintained result set — the same guarantee that [Feldera](https://feldera.com) and [Materialize](https://materialize.com) provide at cluster scale, available as a single binary with no infrastructure required.

```bash
# Count orders by status from a Kafka CDC stream, updating in real time
dbspa "SELECT status, COUNT(*) AS orders
        FROM 'kafka://broker:9092/orders.cdc' CHANGELOG DEBEZIUM
        GROUP BY status"
```

```
STATUS    | ORDERS
----------+-------
pending   | 1,204
shipped   | 8,821
delivered | 12,048
(3 groups | 22,073 matched | 45,102 input)
```

## Why DBSPA?

- **Zero infrastructure.** Single binary. No server, no cluster, no daemon.
- **Streaming SQL.** Query Kafka topics, CDC streams, and stdin pipes with standard SQL.
- **Correct retractions.** When source data changes (via CDC), aggregations update correctly — no stale counts.
- **Joins.** Enrich streaming events with reference data from files (NDJSON, Parquet, CSV, Avro).
- **Fast.** 275K records/sec for NDJSON, 850K records/sec for Parquet filters.

## Quick taste

```bash
# Filter API errors from a log stream
tail -f /var/log/api.json | dbspa "SELECT endpoint, status_code, latency_ms
                                     WHERE status_code >= 400"

# Aggregate with windowing
dbspa "SELECT window_start, endpoint, COUNT(*) AS reqs, AVG(latency_ms) AS avg_lat
        FROM 'kafka://broker/api_requests'
        GROUP BY endpoint
        WINDOW TUMBLING '1 minute'
        EVENT TIME BY timestamp"

# Enrich stream with reference table
cat events.ndjson | dbspa "SELECT e.user_id, u.name, e.action
                             FROM stdin e
                             JOIN '/data/users.parquet' u ON e.user_id = u.id"

# Serve results via HTTP (sidecar mode)
dbspa serve --port 8080 "SELECT region, COUNT(*)
                           FROM 'kafka://broker/orders.cdc' CHANGELOG DEBEZIUM
                           GROUP BY region"
```
