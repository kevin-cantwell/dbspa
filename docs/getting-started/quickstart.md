# Quickstart

## Filter JSON from stdin

The simplest use: pipe JSON data through a SQL filter.

```bash
echo '{"name":"alice","age":30}
{"name":"bob","age":22}
{"name":"carol","age":35}' | dbspa "SELECT name, age WHERE age > 25"
```

Output:

```json
{"name":"alice","age":30}
{"name":"carol","age":35}
```

When reading from stdin, `FROM` is optional.

## Aggregate with GROUP BY

Add `GROUP BY` to get live-updating aggregations. In a terminal (TTY), DBSPA displays a TUI that redraws like `top`:

```bash
cat orders.ndjson | dbspa "SELECT status, COUNT(*) AS cnt GROUP BY status"
```

```
STATUS    | CNT
----------+-----
pending   | 42
shipped   | 108
delivered | 215
(3 groups | 365 matched | 365 input)
```

When piped (non-TTY), the output is [changelog NDJSON](../concepts/changelog-output.md):

```bash
cat orders.ndjson | dbspa "SELECT status, COUNT(*) AS cnt GROUP BY status" | head
```

```json
{"weight":1,"data":{"status":"pending","cnt":1}}
{"weight":1,"data":{"status":"shipped","cnt":1}}
{"weight":-1,"data":{"status":"pending","cnt":1}}
{"weight":1,"data":{"status":"pending","cnt":2}}
```

## Read from a file

Use `--input` (or `-i`) to read from a file instead of stdin:

```bash
dbspa -i data.ndjson "SELECT name, age WHERE age > 25"
```

This also works with Parquet files:

```bash
dbspa -i users.parquet "SELECT * WHERE country = 'US' FORMAT PARQUET"
```

## Query Kafka

Point DBSPA at a Kafka topic using a URI in the `FROM` clause:

```bash
dbspa "SELECT * FROM 'kafka://localhost:9092/events'"
```

With Debezium CDC:

```bash
dbspa "SELECT status, COUNT(*) AS orders
        FROM 'kafka://broker:9092/orders.cdc' CHANGELOG DEBEZIUM
        GROUP BY status"
```

## Save state to SQLite

Write the current aggregation result to a SQLite file that other processes can read concurrently:

```bash
dbspa --state orders.db \
  "SELECT region, COUNT(*) AS cnt
   FROM 'kafka://broker/orders.cdc' CHANGELOG DEBEZIUM
   GROUP BY region"
```

In another terminal:

```bash
sqlite3 orders.db "SELECT * FROM result ORDER BY cnt DESC"
```

## Serve results over HTTP

Run a query as an HTTP sidecar:

```bash
dbspa serve --port 8080 \
  "SELECT region, COUNT(*) AS orders
   FROM 'kafka://broker/orders.cdc' CHANGELOG DEBEZIUM
   GROUP BY region"
```

Then query it:

```bash
curl http://localhost:8080/          # Current state (JSON array)
curl http://localhost:8080/stream    # SSE changelog stream
curl http://localhost:8080/health    # Liveness check
```

## Query Parquet files with DuckDB

DBSPA routes file queries through an embedded DuckDB engine for fast columnar reads:

```bash
dbspa "SELECT region, COUNT(*) AS orders, SUM(total) AS revenue
        FROM '/data/orders.parquet'
        GROUP BY region"
```

DuckDB handles predicate pushdown and column pruning natively -- filtering a 100K-row Parquet file runs at nearly 2M records/sec.

## Access nested JSON with dot notation

Use dot notation to traverse nested JSON fields:

```bash
echo '{"user":{"name":"alice","address":{"city":"NYC"}}}
{"user":{"name":"bob","address":{"city":"SF"}}}' \
| dbspa "SELECT user.name, user.address.city WHERE user.address.city = 'NYC'"
```

Output:

```json
{"name":"alice","city":"NYC"}
```

Dot notation resolves aliases first (e.g., `e.user_id` means column `user_id` on alias `e`), then falls back to JSON field access (e.g., `user.name` means field `name` in column `user`). Multi-level access like `a.b.c` traverses nested objects.

## Join a stream against a file

Enrich streaming events with reference data from a Parquet file:

```bash
dbspa "SELECT e.user_id, u.name, e.action
        FROM 'kafka://broker/events' e
        JOIN '/data/users.parquet' u ON e.user_id = u.id"
```

The file is loaded once via DuckDB and indexed for fast lookups. If the reference data is a CDC stream, use a second Kafka topic with `WITHIN INTERVAL` for a stream-to-stream join.

## Next steps

- [Examples](examples.md) for more query patterns
- [SQL Dialect](../reference/sql.md) for the full language reference
- [CLI Reference](../reference/cli.md) for all flags and subcommands
