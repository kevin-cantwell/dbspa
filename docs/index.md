# FoldDB

**Streaming SQL with incremental aggregation.**

FoldDB is a CLI tool that executes SQL queries against streaming data sources — Kafka topics, stdin pipes, and files — with correct incremental aggregation using differential dataflow principles.

```bash
# Count orders by status from a Kafka CDC stream, updating in real time
folddb "SELECT _after->>'status' AS status, COUNT(*) AS orders
        FROM 'kafka://broker:9092/orders.cdc' FORMAT DEBEZIUM
        GROUP BY _after->>'status'"
```

```
STATUS    | ORDERS
----------+-------
pending   | 1,204
shipped   | 8,821
delivered | 12,048
(3 groups | 22,073 matched | 45,102 input)
```

## Why FoldDB?

- **Zero infrastructure.** Single binary. No server, no cluster, no daemon.
- **Streaming SQL.** Query Kafka topics, CDC streams, and stdin pipes with standard SQL.
- **Correct retractions.** When source data changes (via CDC), aggregations update correctly — no stale counts.
- **Joins.** Enrich streaming events with reference data from files (NDJSON, Parquet, CSV, Avro).
- **Fast.** 275K records/sec for NDJSON, 850K records/sec for Parquet filters.

## Quick taste

```bash
# Filter API errors from a log stream
tail -f /var/log/api.json | folddb "SELECT endpoint, status_code, latency_ms
                                     WHERE status_code >= 400"

# Aggregate with windowing
folddb "SELECT window_start, endpoint, COUNT(*) AS reqs, AVG(latency_ms) AS avg_lat
        FROM 'kafka://broker/api_requests'
        GROUP BY endpoint
        WINDOW TUMBLING '1 minute'
        EVENT TIME BY timestamp"

# Enrich stream with reference table
cat events.ndjson | folddb "SELECT e.user_id, u.name, e.action
                             FROM stdin e
                             JOIN '/data/users.parquet' u ON e.user_id = u.id"

# Serve results via HTTP (sidecar mode)
folddb serve --port 8080 "SELECT region, COUNT(*)
                           FROM 'kafka://broker/orders.cdc' FORMAT DEBEZIUM
                           GROUP BY region"
```
