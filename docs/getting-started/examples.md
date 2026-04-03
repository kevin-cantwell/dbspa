# Examples

## Filtering and projection

```bash
# Filter API errors from a log stream
tail -f /var/log/api.json | dbspa "SELECT endpoint, status_code, latency_ms
                                     WHERE status_code >= 400"

# Project specific fields from Kubernetes logs
kubectl logs my-pod -f | dbspa "SELECT message WHERE level = 'ERROR'"

# JSON path extraction
cat events.ndjson | dbspa "SELECT payload.user.email AS email,
                                    payload.user.name AS name
                             WHERE payload.user.role = 'admin'"
```

## Aggregation

```bash
# Count by status
cat orders.ndjson | dbspa "SELECT status, COUNT(*) AS cnt GROUP BY status"

# Multiple aggregates
cat orders.ndjson | dbspa "SELECT region, COUNT(*) AS cnt,
                                    SUM(total) AS revenue,
                                    AVG(total) AS avg_order
                             GROUP BY region"

# Multi-key GROUP BY
cat orders.ndjson | dbspa "SELECT product, region,
                                    COUNT(*) AS cnt,
                                    SUM(total) AS revenue
                             GROUP BY product, region
                             ORDER BY revenue DESC"

# HAVING filter on aggregates
cat orders.ndjson | dbspa "SELECT status, COUNT(*) AS cnt
                             GROUP BY status
                             HAVING COUNT(*) > 100"
```

## Debezium CDC

```bash
# Live order counts from a CDC stream
dbspa "SELECT status, COUNT(*) AS orders
        FROM 'kafka://broker:9092/orders.cdc' CHANGELOG DEBEZIUM
        GROUP BY status"

# Revenue by region — updates correctly when orders change status
dbspa "SELECT region, SUM(total::float) AS revenue
        FROM 'kafka://broker:9092/orders.cdc' CHANGELOG DEBEZIUM
        GROUP BY region"

# Filter only updates — _before/_after virtuals let you compare old and new values
dbspa "SELECT order_id,
               _before.status AS old_status,
               _after.status AS new_status
        FROM 'kafka://broker:9092/orders.cdc' CHANGELOG DEBEZIUM
        WHERE _op = 'u'"
```

## Windowed aggregation

```bash
# Tumbling window: per-minute request counts
dbspa "SELECT window_start, endpoint, COUNT(*) AS reqs
        FROM 'kafka://broker/api_requests'
        GROUP BY endpoint
        WINDOW TUMBLING '1 minute'
        EVENT TIME BY timestamp"

# Sliding window: 10-minute average with 1-minute slide
dbspa "SELECT window_start, endpoint,
               COUNT(*) AS reqs,
               AVG(latency_ms) AS avg_lat
        FROM 'kafka://broker/api_requests'
        GROUP BY endpoint
        WINDOW SLIDING '10 minutes' BY '1 minute'
        EVENT TIME BY timestamp"

# Session window: user activity sessions with 5-minute gap
dbspa "SELECT window_start, window_end, user_id,
               COUNT(*) AS events
        FROM 'kafka://broker/clicks'
        GROUP BY user_id
        WINDOW SESSION '5 minutes'
        EVENT TIME BY event_time"

# Early emissions: see partial results every 10 seconds
dbspa "SELECT window_start, endpoint, COUNT(*) AS reqs
        FROM 'kafka://broker/api_requests'
        GROUP BY endpoint
        WINDOW TUMBLING '1 hour'
        EMIT EARLY '10 seconds'"
```

## Joins

Enrich streaming events with reference data from files:

```bash
# Join stream with a Parquet lookup table
cat events.ndjson | dbspa "SELECT e.user_id, u.name, e.action
                             FROM stdin e
                             JOIN '/data/users.parquet' u ON e.user_id = u.id"

# LEFT JOIN — keep events even if no user match
cat events.ndjson | dbspa "SELECT e.user_id,
                                    COALESCE(u.name, 'unknown') AS name,
                                    e.action
                             FROM stdin e
                             LEFT JOIN '/data/users.parquet' u ON e.user_id = u.id"
```

## Subqueries

Use subqueries as derived tables or as JOIN sources:

```bash
# Derived table: pre-aggregate then filter
dbspa -i orders.ndjson \
  "SELECT * FROM (SELECT status, COUNT(*) AS cnt GROUP BY status) t
   WHERE cnt > 10"

# Join stream against pre-aggregated file
cat events.ndjson | dbspa \
  "SELECT e.customer_id, e.action, r.order_count
   FROM stdin e
   JOIN (SELECT customer_id, COUNT(*) AS order_count
         FROM '/data/orders.parquet'
         GROUP BY customer_id) r
     ON e.customer_id = r.customer_id"

# Nested subquery: filter then project
dbspa -i data.ndjson \
  "SELECT name, total
   FROM (SELECT name, SUM(amount) AS total
         FROM (SELECT * FROM '/data/transactions.ndjson' WHERE status = 'complete') t1
         GROUP BY name) t2
   WHERE total > 1000"
```

## EXEC: shell commands as data sources

Use `EXEC()` to run any CLI tool and query its output with SQL:

```bash
# Query kubectl logs with SQL
dbspa "SELECT level, message FROM EXEC('kubectl logs my-pod --output=json') AS STREAM
        WHERE level = 'ERROR'"

# Join stream against a Postgres table via psql
cat events.json | dbspa \
  "SELECT e.*, u.name
   FROM stdin e
   JOIN EXEC('psql -c \"COPY users TO STDOUT WITH (FORMAT csv, HEADER)\"') u FORMAT CSV(header=true)
     ON e.user_id = u.id"

# Seed accumulator from BigQuery, then stream from Kafka
dbspa "SELECT region, SUM(amount) AS total
        FROM 'kafka://broker/orders.cdc' CHANGELOG DEBEZIUM
        SEED FROM EXEC('bq query --format=json \"SELECT * FROM orders_snapshot\"')
        GROUP BY region"

# Pipe shell commands together
dbspa "SELECT status, COUNT(*) AS cnt
        FROM EXEC('cat access.log | grep POST')
        GROUP BY status"
```

EXEC runs the command through `/bin/sh -c`, so pipes, redirects, and shell features work. For commands that run indefinitely (like `tail -f` or `kubectl logs -f`), use `AS STREAM` to process output concurrently instead of waiting for the command to exit.

!!! note
    EXEC is disabled in `dbspa serve` mode for security.

## Streaming subqueries

Join a live event stream against a concurrently-running CDC aggregation:

```bash
# Enrich events with live revenue-per-region from a CDC stream
dbspa "SELECT e.user_id, r.revenue
        FROM 'kafka://broker/events' e
        JOIN (
            SELECT region, SUM(amount) AS revenue
            FROM 'kafka://broker/orders.cdc' CHANGELOG DEBEZIUM
            GROUP BY region
        ) r ON e.region = r.region"
```

The inner subquery maintains a live aggregation of order revenue by region. The outer query joins each event against the current aggregation state. When an order changes in the source database, the CDC stream updates the inner aggregation, and the DD join retracts stale results and emits corrected ones.

### FROM streaming subquery

Use a Kafka-based subquery as a derived table with outer filtering:

```bash
# Live order status counts, filtered for statuses with > 100 orders
dbspa "SELECT * FROM (SELECT status, COUNT(*) AS cnt
                        FROM 'kafka://broker/orders.cdc' CHANGELOG DEBEZIUM
                        GROUP BY status) t
        WHERE cnt > 100"
```

The inner accumulating query streams Z-set deltas to the outer WHERE filter. As counts change, only groups currently exceeding the threshold appear in output.

### File left + streaming right

Use a static file as reference data with a live streaming subquery on the JOIN side:

```bash
# Enrich a static customer list with live order counts
dbspa "SELECT c.name, c.tier, r.order_count
        FROM '/data/customers.parquet' c
        JOIN (
            SELECT customer_id, COUNT(*) AS order_count
            FROM 'kafka://broker/orders.cdc' CHANGELOG DEBEZIUM
            GROUP BY customer_id
        ) r ON c.id = r.customer_id"
```

The customer file is loaded once. The query keeps running after the file is fully loaded -- right-side deltas produce corrected join results against all loaded left records.

### Spill to disk for large joins

```bash
# Explicit spill-to-disk
dbspa --spill-to-disk \
  "SELECT o.*, c.name FROM 'kafka://broker/orders' o
   JOIN '/data/customers.parquet' c ON o.customer_id = c.id"

# With memory budget
dbspa --max-memory 512MB \
  "SELECT o.*, c.name FROM 'kafka://broker/orders' o
   JOIN '/data/customers.parquet' c ON o.customer_id = c.id"
```

Stream-stream joins auto-enable spill-to-disk to prevent OOM.

## Deduplication

```bash
# Deduplicate by order_id within a 10-minute window
dbspa "SELECT order_id, status
        FROM 'kafka://broker/orders.cdc' CHANGELOG DEBEZIUM
        DEDUPLICATE BY order_id WITHIN '10 minutes'"

# With custom cache capacity
dbspa "SELECT *
        FROM 'kafka://broker/events'
        DEDUPLICATE BY event_id WITHIN '1 hour' CAPACITY 500000"
```

## Seeding from a file

Bootstrap aggregation state from a snapshot before streaming:

```bash
dbspa "SELECT region, COUNT(*) AS orders
        FROM 'kafka://broker/orders.cdc' CHANGELOG DEBEZIUM
        SEED FROM '/data/orders-snapshot.parquet'
        GROUP BY region"
```

### Seed from BigQuery, continue from Kafka CDC

Use EXEC to pull pre-aggregated state from a data warehouse, then continue with live CDC:

```bash
# Seed from BigQuery, continue from Kafka CDC
dbspa "SELECT region, SUM(amount) AS total
        FROM 'kafka://broker/orders.cdc?offset=2026-04-01' CHANGELOG DEBEZIUM
        SEED FROM EXEC('bq query --format=json \"SELECT region, SUM(amount) AS total FROM orders WHERE ts < ''2026-04-01'' GROUP BY region\"')
        GROUP BY region"
```

The seed query's columns (`region`, `total`) match the outer query's GROUP BY key and aggregate alias. DBSPA injects the pre-computed sums directly into the accumulators, then streaming picks up from the Kafka offset where the seed left off.

## Stateful checkpointing

Survive restarts without replaying from the beginning:

```bash
dbspa --stateful \
  "SELECT region, COUNT(*)
   FROM 'kafka://broker/orders.cdc' CHANGELOG DEBEZIUM
   GROUP BY region"

# Inspect checkpoint state
dbspa state list
dbspa state inspect a1b2c3d4
dbspa state reset a1b2c3d4
```

## Format examples

```bash
# CSV input
cat data.csv | dbspa "SELECT name, age WHERE age > 25 FORMAT CSV"

# CSV with options
cat data.csv | dbspa "SELECT * FORMAT CSV(delimiter='|', header=true)"

# Avro from Kafka with schema registry
dbspa "SELECT * FROM 'kafka://broker/events?registry=http://registry:8081' FORMAT AVRO"

# Avro-encoded Debezium CDC
dbspa "SELECT * FROM 'kafka://broker/orders.cdc?registry=http://registry:8081' FORMAT AVRO CHANGELOG DEBEZIUM"

# Protobuf with typed messages
cat data.pb | dbspa "SELECT * FORMAT PROTOBUF(message='Order')"

# Parquet file
dbspa -i users.parquet "SELECT * WHERE country = 'US' FORMAT PARQUET"
```

## DBSPA changelog piping

Compose DBSPA instances by piping one changelog into another using `CHANGELOG DBSPA`:

```bash
# Instance 1: aggregate orders by status, emit changelog
dbspa "SELECT status, COUNT(*) AS cnt
        FROM 'kafka://broker/orders' GROUP BY status" | \

# Instance 2: consume changelog, filter for high-count statuses
dbspa "SELECT * FROM stdin CHANGELOG DBSPA WHERE cnt > 100"
```

The `CHANGELOG DBSPA` envelope reads the `weight` field and unwraps the `data` object from the Feldera weighted format, preserving retraction/insertion semantics across the pipe.

## Output to SQLite

```bash
# Accumulating query: result table is UPSERTed
dbspa --state metrics.db \
  "SELECT endpoint, COUNT(*) AS reqs, AVG(latency_ms) AS avg_lat
   FROM 'kafka://broker/api_requests'
   GROUP BY endpoint"

# Non-accumulating query: records are appended
dbspa --state errors.db \
  "SELECT endpoint, status_code, latency_ms
   FROM 'kafka://broker/api_requests'
   WHERE status_code >= 500"
```

## Debug and inspect

```bash
# Print query plan without executing
dbspa --dry-run "SELECT status, COUNT(*) GROUP BY status"

# Print plan then execute
dbspa --explain "SELECT status, COUNT(*) GROUP BY status"

# Route bad records to a dead letter file
dbspa --dead-letter errors.ndjson \
  "SELECT * FROM 'kafka://broker/events'"

# Infer schema from a source
dbspa schema 'kafka://broker/orders.cdc'
```
