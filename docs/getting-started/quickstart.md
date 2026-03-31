# Quickstart

## Filter JSON from stdin

The simplest use: pipe JSON data through a SQL filter.

```bash
echo '{"name":"alice","age":30}
{"name":"bob","age":22}
{"name":"carol","age":35}' | folddb "SELECT name, age WHERE age > 25"
```

Output:

```json
{"name":"alice","age":30}
{"name":"carol","age":35}
```

When reading from stdin, `FROM` is optional.

## Aggregate with GROUP BY

Add `GROUP BY` to get live-updating aggregations. In a terminal (TTY), FoldDB displays a TUI that redraws like `top`:

```bash
cat orders.ndjson | folddb "SELECT status, COUNT(*) AS cnt GROUP BY status"
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
cat orders.ndjson | folddb "SELECT status, COUNT(*) AS cnt GROUP BY status" | head
```

```json
{"op":"+","status":"pending","cnt":1}
{"op":"+","status":"shipped","cnt":1}
{"op":"-","status":"pending","cnt":1}
{"op":"+","status":"pending","cnt":2}
```

## Read from a file

Use `--input` (or `-i`) to read from a file instead of stdin:

```bash
folddb -i data.ndjson "SELECT name, age WHERE age > 25"
```

This also works with Parquet files:

```bash
folddb -i users.parquet "SELECT * WHERE country = 'US' FORMAT PARQUET"
```

## Query Kafka

Point FoldDB at a Kafka topic using a URI in the `FROM` clause:

```bash
folddb "SELECT * FROM 'kafka://localhost:9092/events'"
```

With Debezium CDC:

```bash
folddb "SELECT _after->>'status' AS status, COUNT(*) AS orders
        FROM 'kafka://broker:9092/orders.cdc' FORMAT DEBEZIUM
        GROUP BY _after->>'status'"
```

## Save state to SQLite

Write the current aggregation result to a SQLite file that other processes can read concurrently:

```bash
folddb --state orders.db \
  "SELECT region, COUNT(*) AS cnt
   FROM 'kafka://broker/orders.cdc' FORMAT DEBEZIUM
   GROUP BY region"
```

In another terminal:

```bash
sqlite3 orders.db "SELECT * FROM result ORDER BY cnt DESC"
```

## Serve results over HTTP

Run a query as an HTTP sidecar:

```bash
folddb serve --port 8080 \
  "SELECT region, COUNT(*) AS orders
   FROM 'kafka://broker/orders.cdc' FORMAT DEBEZIUM
   GROUP BY region"
```

Then query it:

```bash
curl http://localhost:8080/          # Current state (JSON array)
curl http://localhost:8080/stream    # SSE changelog stream
curl http://localhost:8080/health    # Liveness check
```

## Next steps

- [Examples](examples.md) for more query patterns
- [SQL Dialect](../reference/sql.md) for the full language reference
- [CLI Reference](../reference/cli.md) for all flags and subcommands
