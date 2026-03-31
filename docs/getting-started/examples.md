# Examples

## Filtering and projection

```bash
# Filter API errors from a log stream
tail -f /var/log/api.json | folddb "SELECT endpoint, status_code, latency_ms
                                     WHERE status_code >= 400"

# Project specific fields from Kubernetes logs
kubectl logs my-pod -f | folddb "SELECT message WHERE level = 'ERROR'"

# JSON path extraction
cat events.ndjson | folddb "SELECT payload->'user'->>'email' AS email,
                                    payload->'user'->>'name' AS name
                             WHERE payload->'user'->>'role' = 'admin'"
```

## Aggregation

```bash
# Count by status
cat orders.ndjson | folddb "SELECT status, COUNT(*) AS cnt GROUP BY status"

# Multiple aggregates
cat orders.ndjson | folddb "SELECT region, COUNT(*) AS cnt,
                                    SUM(total) AS revenue,
                                    AVG(total) AS avg_order
                             GROUP BY region"

# Multi-key GROUP BY
cat orders.ndjson | folddb "SELECT product, region,
                                    COUNT(*) AS cnt,
                                    SUM(total) AS revenue
                             GROUP BY product, region
                             ORDER BY revenue DESC"

# HAVING filter on aggregates
cat orders.ndjson | folddb "SELECT status, COUNT(*) AS cnt
                             GROUP BY status
                             HAVING COUNT(*) > 100"
```

## Debezium CDC

```bash
# Live order counts from a CDC stream
folddb "SELECT _after->>'status' AS status, COUNT(*) AS orders
        FROM 'kafka://broker:9092/orders.cdc' FORMAT DEBEZIUM
        GROUP BY _after->>'status'"

# Revenue by region — updates correctly when orders change status
folddb "SELECT _after->>'region' AS region,
               SUM((_after->>'total')::float) AS revenue
        FROM 'kafka://broker:9092/orders.cdc' FORMAT DEBEZIUM
        GROUP BY _after->>'region'"

# Filter only updates
folddb "SELECT _after->>'order_id' AS id,
               _before->>'status' AS old_status,
               _after->>'status' AS new_status
        FROM 'kafka://broker:9092/orders.cdc' FORMAT DEBEZIUM
        WHERE _op = 'u'"
```

## Windowed aggregation

```bash
# Tumbling window: per-minute request counts
folddb "SELECT window_start, endpoint, COUNT(*) AS reqs
        FROM 'kafka://broker/api_requests'
        GROUP BY endpoint
        WINDOW TUMBLING '1 minute'
        EVENT TIME BY timestamp"

# Sliding window: 10-minute average with 1-minute slide
folddb "SELECT window_start, endpoint,
               COUNT(*) AS reqs,
               AVG(latency_ms) AS avg_lat
        FROM 'kafka://broker/api_requests'
        GROUP BY endpoint
        WINDOW SLIDING '10 minutes' BY '1 minute'
        EVENT TIME BY timestamp"

# Session window: user activity sessions with 5-minute gap
folddb "SELECT window_start, window_end, user_id,
               COUNT(*) AS events
        FROM 'kafka://broker/clicks'
        GROUP BY user_id
        WINDOW SESSION '5 minutes'
        EVENT TIME BY event_time"

# Early emissions: see partial results every 10 seconds
folddb "SELECT window_start, endpoint, COUNT(*) AS reqs
        FROM 'kafka://broker/api_requests'
        GROUP BY endpoint
        WINDOW TUMBLING '1 hour'
        EMIT EARLY '10 seconds'"
```

## Joins

Enrich streaming events with reference data from files:

```bash
# Join stream with a Parquet lookup table
cat events.ndjson | folddb "SELECT e.user_id, u.name, e.action
                             FROM stdin e
                             JOIN '/data/users.parquet' u ON e.user_id = u.id"

# LEFT JOIN — keep events even if no user match
cat events.ndjson | folddb "SELECT e.user_id,
                                    COALESCE(u.name, 'unknown') AS name,
                                    e.action
                             FROM stdin e
                             LEFT JOIN '/data/users.parquet' u ON e.user_id = u.id"
```

## Deduplication

```bash
# Deduplicate by order_id within a 10-minute window
folddb "SELECT _after->>'order_id' AS id, _after->>'status' AS status
        FROM 'kafka://broker/orders.cdc' FORMAT DEBEZIUM
        DEDUPLICATE BY _after->>'order_id' WITHIN '10 minutes'"

# With custom cache capacity
folddb "SELECT *
        FROM 'kafka://broker/events'
        DEDUPLICATE BY event_id WITHIN '1 hour' CAPACITY 500000"
```

## Seeding from a file

Bootstrap aggregation state from a snapshot before streaming:

```bash
folddb "SELECT region, COUNT(*) AS orders
        FROM 'kafka://broker/orders.cdc' FORMAT DEBEZIUM
        SEED FROM '/data/orders-snapshot.parquet'
        GROUP BY region"
```

## Stateful checkpointing

Survive restarts without replaying from the beginning:

```bash
folddb --stateful \
  "SELECT region, COUNT(*)
   FROM 'kafka://broker/orders.cdc' FORMAT DEBEZIUM
   GROUP BY region"

# Inspect checkpoint state
folddb state list
folddb state inspect a1b2c3d4
folddb state reset a1b2c3d4
```

## Format examples

```bash
# CSV input
cat data.csv | folddb "SELECT name, age WHERE age > 25 FORMAT CSV"

# CSV with options
cat data.csv | folddb "SELECT * FORMAT CSV(delimiter='|', header=true)"

# Avro from Kafka with schema registry
folddb "SELECT * FROM 'kafka://broker/events?registry=http://registry:8081' FORMAT AVRO"

# Protobuf with typed messages
cat data.pb | folddb "SELECT * FORMAT PROTOBUF(message='Order')"

# Parquet file
folddb -i users.parquet "SELECT * WHERE country = 'US' FORMAT PARQUET"
```

## Output to SQLite

```bash
# Accumulating query: result table is UPSERTed
folddb --state metrics.db \
  "SELECT endpoint, COUNT(*) AS reqs, AVG(latency_ms) AS avg_lat
   FROM 'kafka://broker/api_requests'
   GROUP BY endpoint"

# Non-accumulating query: records are appended
folddb --state errors.db \
  "SELECT endpoint, status_code, latency_ms
   FROM 'kafka://broker/api_requests'
   WHERE status_code >= 500"
```

## Debug and inspect

```bash
# Print query plan without executing
folddb --dry-run "SELECT status, COUNT(*) GROUP BY status"

# Print plan then execute
folddb --explain "SELECT status, COUNT(*) GROUP BY status"

# Route bad records to a dead letter file
folddb --dead-letter errors.ndjson \
  "SELECT * FROM 'kafka://broker/events'"

# Infer schema from a source
folddb schema 'kafka://broker/orders.cdc'
```
