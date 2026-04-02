# SQL Dialect

FoldDB's SQL is aligned with **PostgreSQL** — the same dialect DuckDB aligns to. Streaming extensions are FoldDB-specific.

## Clause syntax

```sql
SELECT [DISTINCT] expr [AS alias], ...
FROM source [FORMAT format_spec]
[SEED FROM file_path]
[JOIN source alias [FORMAT format_spec] ON condition [WITHIN INTERVAL duration]]
[LEFT JOIN source alias [FORMAT format_spec] ON condition [WITHIN INTERVAL duration]]
[WHERE condition]
[GROUP BY expr, ...]
[HAVING condition]
[WINDOW window_spec]
[EVENT TIME BY expr]
[WATERMARK duration]
[EMIT emit_spec]
[DEDUPLICATE BY expr [WITHIN duration] [CAPACITY n]]
[ORDER BY expr [ASC|DESC], ...]
[LIMIT n]
```

!!! warning
    Clause ordering is rigid. Clauses must appear in the order shown above. The parser reports a clear error for out-of-order clauses.

## SELECT

```sql
SELECT *                          -- all columns
SELECT name, age                  -- specific columns
SELECT name AS n, age AS a        -- aliases
SELECT DISTINCT region            -- deduplicate output rows
SELECT COUNT(*), SUM(total)       -- aggregate functions (implicit single group if no GROUP BY)
```

`SELECT DISTINCT` on a non-accumulating query maintains an in-memory set of seen rows. For accumulating queries, `DISTINCT` is a no-op (output is already one row per group key).

## FROM

Specifies the data source. Optional when reading from stdin.

```sql
-- Kafka topic
FROM 'kafka://broker:9092/topic'
FROM 'kafka://broker/topic?offset=earliest'
FROM 'kafka://broker/topic?offset=2024-01-01T00:00:00Z'
FROM 'kafka://broker/topic?group=my-group&partition=0,1,2'

-- Kafka with authentication
FROM 'kafka://broker/topic?sasl_mechanism=PLAIN&sasl_username=X&sasl_password=Y'
FROM 'kafka://my-cluster/events'    -- resolved from ~/.folddb/credentials

-- stdin (implicit or explicit)
FROM 'stdin://'

-- Subquery (derived table)
FROM (SELECT status, SUM(amount) AS total FROM '/data/orders.parquet' GROUP BY status) t
```

### Subquery in FROM

A parenthesized `SELECT` statement can be used as the FROM source, producing a **derived table**. The inner query is executed to completion before the outer query begins. An alias is mandatory.

```sql
-- Pre-aggregate a file then filter in the outer query
SELECT *
FROM (SELECT region, COUNT(*) AS cnt FROM '/data/events.parquet' GROUP BY region) stats
WHERE cnt > 100

-- Nested subqueries
SELECT *
FROM (SELECT * FROM (SELECT id, name FROM '/data/users.ndjson') inner_t) outer_t
```

The inner query supports the full SQL dialect: WHERE, GROUP BY, HAVING, JOIN, LIMIT, etc. Kafka sources are not supported inside FROM subqueries (derived tables). For Kafka sources in JOIN subqueries, see [Streaming Subqueries](../concepts/joins.md#streaming-subqueries).

### Kafka URI parameters

| Param | Default | Description |
|---|---|---|
| `offset` | `latest` | `earliest`, `latest`, ISO timestamp, or integer offset |
| `partition` | all | Comma-separated partition IDs |
| `group` | (ephemeral) | Consumer group ID |
| `registry` | (none) | Schema registry URL for Avro/Protobuf |
| `sasl_mechanism` | (none) | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `sasl_username` | (none) | SASL username |
| `sasl_password` | (none) | SASL password |
| `tls_cert` | (none) | Path to client certificate (mTLS) |
| `tls_key` | (none) | Path to client private key (mTLS) |
| `tls_ca` | (none) | Path to CA certificate (mTLS) |

### Kafka virtual columns

Every Kafka record exposes:

| Column | Type | Description |
|---|---|---|
| `_offset` | BIGINT | Partition offset |
| `_partition` | INT | Partition number |
| `_timestamp` | TIMESTAMP | Kafka log-append timestamp |
| `_key` | TEXT | Message key (UTF-8, or `b64:` prefixed if not valid UTF-8) |

### Debezium virtual columns

When `FORMAT DEBEZIUM` is specified:

| Column | Type | Description |
|---|---|---|
| `_op` | TEXT | `c`, `u`, `d`, `r` |
| `_before` | JSON | Row state before the change |
| `_after` | JSON | Row state after the change |
| `_table` | TEXT | Source table name |
| `_db` | TEXT | Source database name |
| `_ts` | TIMESTAMP | Source database transaction timestamp |
| `_source` | JSON | Full Debezium source block |

## FORMAT

Declares the input format. Placed after the `FROM` clause.

```sql
FORMAT DEBEZIUM
FORMAT DEBEZIUM_AVRO
FORMAT CSV
FORMAT CSV(delimiter='|', header=true, quote='"', null_string='')
FORMAT AVRO
FORMAT AVRO(registry='http://registry:8081')
FORMAT PROTOBUF
FORMAT PROTOBUF(message='Order')
FORMAT PARQUET
```

Auto-detection: Kafka without a registry defaults to NDJSON. Kafka with a registry auto-detects via the Confluent wire format magic byte. stdin defaults to NDJSON.

## WHERE

Standard SQL filter expressions.

```sql
WHERE age > 25
WHERE status = 'active' AND region IN ('us-east', 'us-west')
WHERE name LIKE 'A%'
WHERE name ILIKE '%alice%'
WHERE amount BETWEEN 10 AND 100
WHERE status IS NOT NULL
WHERE x IS DISTINCT FROM y        -- NULL-safe inequality
```

## GROUP BY

```sql
GROUP BY status
GROUP BY region, status
GROUP BY 1, 2                     -- ordinal references to SELECT list
GROUP BY _after->>'region'        -- expressions
```

Column aliases from SELECT are **not** valid in GROUP BY. Use the original expression or an ordinal.

## HAVING

Filters groups after aggregation:

```sql
HAVING COUNT(*) > 100
HAVING SUM(total) >= 1000.0
```

## WINDOW

FoldDB extension for time-based windowed aggregation.

```sql
-- Tumbling (non-overlapping)
WINDOW TUMBLING '1 minute'

-- Sliding (overlapping)
WINDOW SLIDING '10 minutes' BY '5 minutes'

-- Session (activity-based)
WINDOW SESSION '5 minutes'
```

Duration literals: `'1 minute'`, `'30 seconds'`, `'1 hour'`, `'1 day'`.

See [Windowing](../concepts/windowing.md) for details.

## EVENT TIME BY

Declares which column carries event time for windowed queries.

```sql
EVENT TIME BY timestamp
EVENT TIME BY event_time
```

Parsed as ISO 8601 or Unix epoch (seconds). Timestamps without timezone offset are assumed UTC.

## WATERMARK

Sets allowed lateness for event-time windows. Records arriving behind the watermark by more than this duration are dropped.

```sql
WATERMARK '30 seconds'
```

Default: 5 seconds when `EVENT TIME BY` is specified.

## EMIT

Controls when windowed results are emitted.

```sql
EMIT FINAL              -- default: emit once when window closes
EMIT EARLY '10 seconds' -- periodic partial results
```

## SEED FROM

Bootstraps accumulator state from a file before streaming.

```sql
FROM 'kafka://broker/topic' FORMAT DEBEZIUM
SEED FROM '/path/to/snapshot.parquet'
GROUP BY region
```

See [Checkpointing](../architecture/checkpointing.md) for interaction with `--stateful`.

## DEDUPLICATE BY

Bounded deduplication on a key.

```sql
DEDUPLICATE BY order_id WITHIN '10 minutes'
DEDUPLICATE BY order_id WITHIN '10 minutes' CAPACITY 500000
```

Default cache capacity: 100,000 entries. LRU eviction when exceeded. Retractions (negative weight) always pass through unconditionally.

## ORDER BY

```sql
ORDER BY revenue DESC
ORDER BY region ASC, cnt DESC
ORDER BY 1, 2 DESC
```

Accepts expressions, ordinals, or aliases. For accumulating queries, ORDER BY controls TUI display order and SQLite row order. For non-accumulating streaming queries, ORDER BY is rejected at parse time.

## LIMIT

```sql
LIMIT 100
```

Terminates after N output records. For non-accumulating queries, this is straightforward. For accumulating queries, LIMIT applies to the total number of changelog emissions.

## JOIN

Equi-join between a stream and a table, a stream and a CDC source, two streams, or a stream and a subquery.

```sql
-- Stream-to-file
JOIN '/data/users.parquet' u ON e.user_id = u.id
LEFT JOIN '/data/users.csv' u FORMAT CSV(header=true) ON e.user_id = u.id

-- Stream-to-subquery (pre-aggregate a file, then join)
JOIN (SELECT customer_id, COUNT(*) AS cnt FROM '/data/orders.parquet' GROUP BY customer_id) r
  ON e.customer_id = r.customer_id

-- Stream-to-stream (WITHIN is mandatory)
FROM 'kafka://broker/orders' o
JOIN 'kafka://broker/payments' p ON o.order_id = p.order_id
WITHIN INTERVAL '10 minutes'
```

### Subquery as JOIN source

A parenthesized `SELECT` can replace a file path as the JOIN source. The subquery is materialized into an in-memory table before the stream begins. This is useful for joining a live stream against pre-aggregated or filtered reference data.

```sql
-- Enrich events with per-customer order counts
SELECT e.customer_id, e.action, r.order_count
FROM stdin e
JOIN (SELECT customer_id, COUNT(*) AS order_count
      FROM '/data/orders.parquet'
      GROUP BY customer_id) r
  ON e.customer_id = r.customer_id
```

An alias is mandatory for the subquery. The alias is used to qualify column references in the ON condition and SELECT list.

**Streaming subqueries:** When the inner query's FROM source is a Kafka topic (`kafka://`), the subquery runs concurrently with the outer query instead of being materialized. The inner query must have GROUP BY -- its accumulation results feed into the DD join as live Z-set deltas. The outer query sees live-updating results from the inner aggregation. See [Streaming Subqueries](../concepts/joins.md#streaming-subqueries) for details.

### WITHIN INTERVAL

Required for stream-to-stream joins. Bounds how long records are retained in the join arrangements. Records older than the interval are evicted and their join results are retracted.

```sql
WITHIN INTERVAL '10 minutes'
WITHIN INTERVAL '1 hour'
WITHIN INTERVAL '30 seconds'
```

Without `WITHIN`, a stream-to-stream join is rejected at parse time.

See [Joins](../concepts/joins.md) for full details on DD joins, CDC propagation, and stream-to-stream semantics.

## Expressions

### Operator precedence (highest to lowest)

| Precedence | Operators | Associativity |
|---|---|---|
| 1 | `::` (type cast) | Left |
| 2 | `->`, `->>` (JSON access) | Left |
| 3 | unary `-`, unary `+` | Right |
| 4 | `*`, `/`, `%` | Left |
| 5 | `+`, `-` (binary) | Left |
| 6 | `\|\|` (string concat) | Left |
| 7 | `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`, `LIKE`, `ILIKE`, `IS`, `IN`, `BETWEEN`, `IS DISTINCT FROM` | Non-associative |
| 8 | `NOT` | Right |
| 9 | `AND` | Left |
| 10 | `OR` | Left |

### JSON access

FoldDB supports two syntaxes for accessing nested JSON fields:

**Dot notation** (recommended for readability):
```sql
data.name               -- one level: extract field as text
user.address.city       -- multi-level: traverse nested objects
e.payload.score         -- works with table aliases (e is alias, payload is column)
```

**PostgreSQL arrow syntax** (full control over JSON vs text return type):
```sql
col->'key'              -- extract JSON field as JSON
col->>'key'             -- extract JSON field as TEXT
col->0                  -- extract JSON array element as JSON
col->>0                 -- extract JSON array element as TEXT
payload->'user'->>'email'  -- chaining
```

Dot notation always returns text for the final field (like `->>`). For intermediate levels in multi-level access, it preserves JSON (like `->`). If you need to keep a nested object as JSON, use the arrow syntax: `col->'key'`.

**Resolution order:** `a.b` first tries alias resolution (table `a`, column `b`). If no alias matches, it tries JSON field access (column `a`, field `b`). This means table aliases always take priority over JSON column names.

!!! warning
    Cast precedence: `col->>'amount'::float` parses as `col->>('amount'::float)`, not `(col->>'amount')::float`. Always use parentheses: `(col->>'amount')::float`.

### Type cast

```sql
expr::type              -- PostgreSQL shorthand
CAST(expr AS type)      -- standard SQL
```

### Conditional

```sql
CASE WHEN status = 'active' THEN 1 ELSE 0 END
CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 ELSE -1 END
```

## Types

| Type | Notes |
|---|---|
| `BOOLEAN` | |
| `INT` / `BIGINT` | 64-bit internally |
| `FLOAT` / `DOUBLE` | 64-bit internally |
| `TEXT` | UTF-8 strings |
| `TIMESTAMP` | UTC internally |
| `INTERVAL` | For window durations and time arithmetic |
| `JSON` | Opaque JSON value, queryable via `->` / `->>` |
| `NULL` | |

### Type coercion

| Context | Rule |
|---|---|
| Arithmetic between INT and FLOAT | INT promoted to FLOAT |
| Comparison between INT and FLOAT | INT promoted to FLOAT |
| TEXT compared to INT/FLOAT | Error — use explicit cast |
| `\|\|` with non-TEXT operand | Non-TEXT cast to TEXT |
| `->>` result in arithmetic | Error — `->>` returns TEXT, cast explicitly |
| TIMESTAMP compared to TEXT | TEXT parsed as ISO 8601 |

## Aggregate functions

| Function | State | Notes |
|---|---|---|
| `COUNT(*)` | O(1) | Counts all rows including NULLs |
| `COUNT(x)` | O(1) | Counts non-NULL values |
| `COUNT(DISTINCT x)` | O(n) | Exact count of distinct values |
| `APPROX_COUNT_DISTINCT(x)` | O(1) | HyperLogLog approximation |
| `SUM(x)` | O(1) | |
| `AVG(x)` | O(1) | |
| `MIN(x)` | O(n) | Retraction-aware via heap |
| `MAX(x)` | O(n) | Retraction-aware via heap |
| `MEDIAN(x)` | O(n) | Dual-heap |
| `PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x)` | O(n) | PostgreSQL syntax |
| `ARRAY_AGG(x)` | O(n) | Includes NULLs |
| `FIRST(x)` | O(1) | First non-NULL; not retractable |
| `LAST(x)` | O(1) | Most recent non-NULL; ignores retractions |

## Scalar functions

| Function | Description |
|---|---|
| `COALESCE(a, b, ...)` | First non-null argument |
| `NULLIF(a, b)` | NULL if a = b |
| `LENGTH(s)` | String length |
| `UPPER(s)` / `LOWER(s)` | Case conversion |
| `TRIM(s)` / `LTRIM(s)` / `RTRIM(s)` | Whitespace trim |
| `SUBSTR(s, start, len)` | Substring extraction |
| `REPLACE(s, from, to)` | String replacement |
| `SPLIT_PART(s, delim, idx)` | Split string and index into result |
| `PARSE_TIMESTAMP(s, fmt)` | Parse string to timestamp |
| `FORMAT_TIMESTAMP(ts, fmt)` | Format timestamp to string |
| `NOW()` | Current processing time |
| `EXTRACT(field FROM ts)` | Extract year/month/day/hour/minute/second |
| `json_keys(j)` | Array of keys in a JSON object |

## NULL semantics

FoldDB follows PostgreSQL three-valued logic.

- `NULL = NULL` is NULL, not TRUE. Use `IS NOT DISTINCT FROM` for NULL-safe equality.
- `NULL IN (1, 2, NULL)` is NULL, not TRUE.
- `NULL BETWEEN 1 AND 10` is NULL.
- NULL is a valid GROUP BY key value — all NULLs are grouped together.
- Division by zero returns NULL (integer) or `+Inf`/`-Inf`/`NaN` (float, per IEEE 754).
