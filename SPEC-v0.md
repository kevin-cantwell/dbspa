# FoldDB v0 Specification

**Date:** 2026-03-28
**Status:** Draft

---

## 1. What Is FoldDB

FoldDB is a CLI tool that executes SQL queries against Kafka topics and stdin streams, with correct incremental aggregation over change streams. It is a single binary with zero infrastructure dependencies.

The v0 scope is deliberately narrow: **streaming SQL with accumulation, Kafka and stdin only, no joins, no table sources.**

```bash
# Filter a Kafka CDC stream
folddb "SELECT _after->>'status', _after->>'region'
        FROM 'kafka://localhost:9092/orders.cdc' FORMAT DEBEZIUM
        WHERE _op = 'u'"

# Live aggregate over CDC — output updates in place as state changes
folddb "SELECT _after->>'region' AS region, COUNT(*) AS orders
        FROM 'kafka://localhost:9092/orders.cdc' FORMAT DEBEZIUM
        GROUP BY region"

# Windowed metrics over a plain Kafka topic
folddb "SELECT window_start, endpoint, COUNT(*) AS reqs, AVG(latency_ms) AS avg_lat
        FROM 'kafka://localhost:9092/api_requests'
        GROUP BY endpoint
        WINDOW TUMBLING '1 minute'"

# Pipe structured logs through SQL
kubectl logs my-pod -f | folddb "SELECT message WHERE level = 'ERROR'"
```

---

## 2. Core Concepts

### 2.1 Every Record Carries a Diff

All records inside FoldDB have the shape `(columns, timestamp, diff)` where `diff` is `+1` (insert) or `-1` (retract).

- **Insert-only sources** (plain Kafka JSON, stdin): `diff` is always `+1`.
- **Debezium CDC sources**: `diff` is derived from the `op` field:

| Debezium `op` | Records emitted |
|---|---|
| `c` (create) | `(_after, +1)` |
| `r` (snapshot read) | `(_after, +1)` |
| `u` (update) | `(_before, -1)` then `(_after, +1)` |
| `d` (delete) | `(_before, -1)` |
| `t` (truncate) | ignored (logged at `warn` level) |

**Debezium edge cases:**
- If `_before` is `null` on an `update` (common when Debezium `REPLICA IDENTITY` is not `FULL`), the retraction is **skipped** — only the `(_after, +1)` insert is emitted. This means accumulators may drift over time for sources without full replica identity. FoldDB logs a `warn` on the first occurrence per source and includes a count of skipped retractions in the TUI footer.
- If `_after` is `null` on an `update`, the record is treated as malformed and routed to `--dead-letter` (or logged and skipped).
- Debezium tombstone records (null value with non-null key) are silently dropped.
- If `_before` is `null` on a `delete` (`op=d`), the retraction is **skipped**. This occurs when the source table uses `REPLICA IDENTITY DEFAULT` (which only includes the primary key in `_before` for updates, and may send `null` for deletes). FoldDB logs a `warn` on the first occurrence per source, routes the original record to `--dead-letter` (if configured), and includes a count of skipped delete retractions in the TUI footer. The record is **not** silently dropped — it is explicitly surfaced as un-retractable.

This means all operators handle retractions correctly by default.

### 2.2 Two Query Types

**Non-accumulating** (no GROUP BY): output is a stream of records, one per line. Runs until killed or `LIMIT` is reached.

**Accumulating** (GROUP BY with aggregates): output is a changelog of accumulator updates. Each update is a diff — either a new/updated result (`+`) or a retraction of a previous result (`-`).

**DISTINCT:** `SELECT DISTINCT` on a non-accumulating query maintains an in-memory set of seen output rows. Duplicates are suppressed. This is bounded only by the data — there is no automatic eviction. For streams with high cardinality output, this can consume unbounded memory. FoldDB logs a warning when the distinct set exceeds `--memory-limit / 4`. For accumulating queries, `SELECT DISTINCT` is a no-op (the output is already one row per group key).

### 2.3 Output Modes

| Context | Accumulating | Non-accumulating |
|---|---|---|
| TTY | TUI: redraws in place like `top` | Scroll: one line per record |
| Piped / non-TTY | Changelog NDJSON (see 2.4) | NDJSON stream |
| `--state <file.db>` | SQLite table, updated in place | SQLite table, appended |

Override auto-detection with `--mode tui`, `--mode changelog`, `--mode json`.

### 2.4 Changelog NDJSON Format

When piped or explicitly requested, accumulating queries emit diffs:

```jsonl
{"op":"+","region":"us-east","orders":12048}
{"op":"+","region":"us-west","orders":8821}
{"op":"-","region":"us-east","orders":12048}
{"op":"+","region":"us-east","orders":12049}
```

A downstream consumer maintaining a map of `GROUP BY keys → latest "+" value` always has the current result set. A `"-"` retracts the previous value for that key; the immediately following `"+"` is the replacement.

**Ordering guarantee:** For a given group key, the retraction (`"-"`) for the old value is always emitted **immediately before** the insertion (`"+"`) for the new value, with no interleaving from other keys between them. Across different keys, no ordering is guaranteed. This means a consumer can process the changelog line-by-line and always have a consistent view of any single key, but may observe partial updates across keys at any given line.

For non-accumulating queries, every line is `op: "+"` and the `op` field is omitted for brevity — plain NDJSON.

### 2.5 SQLite State Store

`--state orders.db` writes the current aggregation result to a SQLite table named `result`, updated in place via UPSERT. The SQLite file uses WAL mode for concurrent read access — readers do not block writers and vice versa.

**File locking:** If another process holds a write lock on the SQLite file (e.g., another FoldDB instance or a manual `sqlite3` session with an open transaction), FoldDB retries with exponential backoff (100ms, 200ms, 400ms, ..., capped at 5s per retry). If the lock is not acquired within 30 seconds total, FoldDB exits with an error: `Error: cannot acquire write lock on <file> after 30s — is another process writing to it?`. In practice, WAL mode minimizes this: concurrent readers never conflict with FoldDB's writes, and write conflicts only occur if another process is actively writing.

```bash
# Terminal 1
folddb --state orders.db \
  "SELECT region, status, COUNT(*) AS cnt, SUM(total) AS revenue
   FROM 'kafka://broker/orders.cdc' FORMAT DEBEZIUM
   GROUP BY region, status"

# Terminal 2 (any time, while folddb is running)
sqlite3 orders.db "SELECT * FROM result ORDER BY revenue DESC"
```

Schema of the `result` table is derived from the query's output columns. GROUP BY columns become the composite primary key (for UPSERT).

For non-accumulating queries with `--state`, records are INSERTed (append-only). An auto-increment `_rowid` and `_ingested_at` column are added.

---

## 3. SQL Dialect (v0 Subset)

### 3.0 Dialect Alignment

FoldDB's SQL is **PostgreSQL** — the most widely known SQL dialect and the same one DuckDB aligns to (relevant for future table-query integration).

| Aspect | Aligned to | Notes |
|---|---|---|
| Expressions, operators, precedence | PostgreSQL | `||`, `ILIKE`, `IS DISTINCT FROM`, `::` casts |
| JSON operators | PostgreSQL | `->`, `->>`, not MySQL `JSON_EXTRACT()` |
| Type names and casting | PostgreSQL | `TEXT` not `VARCHAR`, `::int` shorthand |
| String functions | PostgreSQL | `SPLIT_PART`, `SUBSTR`, `TRIM`, `LENGTH` |
| Date/time | PostgreSQL | `EXTRACT()`, `INTERVAL '1' MINUTE`, `NOW()` |
| NULL handling | PostgreSQL | `IS DISTINCT FROM`, `COALESCE`, three-valued logic |
| Streaming semantics | FoldDB-specific | Simple, opinionated extensions designed for CLI ergonomics |

**Streaming extensions are FoldDB-specific.** We don't align to Flink SQL or Spark SQL for streaming because neither syntax is a good fit for a zero-config CLI. Instead, we define simple, readable extensions that prioritize ergonomics for the common case.

| Extension | Purpose |
|---|---|
| Source URIs in `FROM` | No catalog — sources are referenced inline |
| `FORMAT` clause | Format is declared per-query, not in a schema registry |
| `WINDOW TUMBLING / SLIDING / SESSION` | Time-based windowed aggregation (see 3.5) |
| `EMIT` clause | Controls when windowed results are emitted |
| `WATERMARK` clause | Declares event time column and allowed lateness |
| `DEDUPLICATE BY` clause | Bounded deduplication on a key |

**Postgres features NOT in v0:** CTEs (`WITH`), subqueries, `JOIN`, `UNION`, analytical window functions (`OVER`/`PARTITION BY`), `LATERAL`, `ARRAY` constructors, regex operators (`~`, `~*`), `RETURNING`. These may be added in future versions.

### 3.1 Supported Clauses

```sql
SELECT [DISTINCT] expr [AS alias], ...
FROM source [FORMAT format_spec]
[WHERE condition]
[GROUP BY expr, ...]
[HAVING condition]
[WINDOW window_spec]
[EVENT TIME BY expr]
[WATERMARK duration]
[EMIT emit_spec]
[DEDUPLICATE BY expr [WITHIN duration]]
[ORDER BY expr [ASC|DESC], ...]   -- accumulating: orders the TUI/state output
[LIMIT n]
```

**Clause ordering is rigid.** Clauses must appear in the order shown above. This eliminates ambiguity in the parser and produces clear error messages when clauses are out of order. The parser reports: `Error: WINDOW clause must appear after HAVING, found at position N`.

**GROUP BY references:** GROUP BY accepts expressions or integer ordinals (1-indexed, referencing SELECT list position). Column aliases defined in SELECT are **not** valid in GROUP BY — use the original expression or an ordinal. This follows PostgreSQL semantics and avoids ambiguity when an alias shadows a column name.

```sql
-- Valid:
GROUP BY _after->>'region'
GROUP BY 1, 2

-- Invalid (alias not allowed in GROUP BY):
SELECT _after->>'region' AS region ... GROUP BY region
-- Use instead:
SELECT _after->>'region' AS region ... GROUP BY 1
```

**ORDER BY** accepts expressions, ordinals, or aliases. For accumulating queries, ORDER BY controls the display order in TUI mode and the row order in SQLite state output. For non-accumulating streaming queries, ORDER BY is **rejected** at parse time with: `Error: ORDER BY is not supported on non-accumulating streaming queries (no GROUP BY). Records are emitted in arrival order.`

When reading from stdin, `FROM` is optional:

```bash
cat data.json | folddb "SELECT name, age WHERE age > 30"
```

### 3.2 Expressions

**Operator precedence** (highest to lowest):

| Precedence | Operators | Associativity |
|---|---|---|
| 1 (highest) | `::` (type cast) | Left |
| 2 | `->`, `->>` (JSON access) | Left |
| 3 | unary `-`, unary `+` | Right |
| 4 | `*`, `/`, `%` | Left |
| 5 | `+`, `-` (binary) | Left |
| 6 | `\|\|` (string concat) | Left |
| 7 | `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`, `LIKE`, `ILIKE`, `IS`, `IN`, `BETWEEN`, `IS DISTINCT FROM` | Non-associative |
| 8 | `NOT` | Right |
| 9 | `AND` | Left |
| 10 (lowest) | `OR` | Left |

Parentheses override precedence as expected.

**Arithmetic:** `+`, `-`, `*`, `/`, `%`

**Comparison:** `=`, `!=` / `<>`, `<`, `>`, `<=`, `>=`, `IS NULL`, `IS NOT NULL`, `IS DISTINCT FROM`, `IS NOT DISTINCT FROM`, `IN (...)`, `BETWEEN ... AND ...`

**Logical:** `AND`, `OR`, `NOT` — follows standard three-valued logic (see Section 13 for NULL truth tables).

**String:** `||` (concat), `LIKE`, `ILIKE`

**JSON access** (PostgreSQL syntax):
- `col->'key'` — extract JSON object field as JSON
- `col->>'key'` — extract JSON object field as text
- `col->0` — extract JSON array element as JSON
- `col->>0` — extract JSON array element as text
- Chaining: `payload->'user'->>'email'`
- Deep chaining: `a->'b'->'c'->>'d'` — each `->` returns JSON, final `->>` returns text

JSON access on a NULL value returns NULL (does not error). JSON access with a key that does not exist returns NULL.

**Type cast:** `expr::type` or `CAST(expr AS type)`

Casts bind tightly (precedence 1), so `_after->>'amount'::float` parses as `_after->>('amount'::float)`, which is almost certainly not what the user wants. The correct form is `(_after->>'amount')::float`. The parser emits a **warning** when `::` is applied to a string literal immediately following `->>`, suggesting parentheses.

**Mixed cast and JSON example:**
```sql
-- Correct:
SELECT (_after->>'amount')::float AS amount
-- Incorrect (casts the key, not the result):
SELECT _after->>'amount'::float AS amount
```

### 3.3 Types

| Type | Notes |
|---|---|
| `BOOLEAN` | |
| `INT` / `BIGINT` | 64-bit internally |
| `FLOAT` / `DOUBLE` | 64-bit internally |
| `DECIMAL(p,s)` | v0: mapped to float64; exact decimal in v1 |
| `TEXT` | UTF-8 strings |
| `TIMESTAMP` | UTC internally |
| `INTERVAL` | for window durations and time arithmetic |
| `JSON` | opaque JSON value, queryable via `->` / `->>` |
| `NULL` | |

### 3.4 Functions

**Aggregate (all fully retraction-aware per differential dataflow model):**

Every aggregate supports both `Add` (diff=+1) and `Retract` (diff=-1). The distinction is only in state cost — how much memory each group key requires:

| Function | State per key | Backing structure | Notes |
|---|---|---|---|
| `COUNT(*)` | O(1) — 1 integer | counter | +1/-1 |
| `SUM(x)` | O(1) — 1 number | running total | add/subtract |
| `AVG(x)` | O(1) — 2 numbers | sum + count | derived |
| `MIN(x)` | O(n) — all values | min-heap | retraction removes from heap; O(log n) |
| `MAX(x)` | O(n) — all values | max-heap | same as MIN |
| `MEDIAN(x)` | O(n) — all values | dual-heap | maintains two heaps split at median; O(log n) per update |
| `PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x)` | O(n) — all values | sorted set | Postgres syntax; recomputes on change |
| `COUNT(DISTINCT x)` | O(n) — distinct set | hash set | exact; use `APPROX_COUNT_DISTINCT(x)` for HyperLogLog (O(1), mergeable) |
| `ARRAY_AGG(x)` | O(n) — all values | list | append/remove |
| `FIRST(x)` | O(1) | single value | first seen; not retractable (see below) |
| `LAST(x)` | O(1) | single value | most recent; updates on each +1, ignores retractions |

**FIRST/LAST retraction semantics:** `FIRST(x)` records the first value seen for a group key and never changes. If that first record is retracted, the retraction is **ignored** — the `FIRST` value is frozen. This is a known semantic compromise: `FIRST` is order-dependent and not fully retraction-aware. `LAST(x)` updates on every `+1` diff but ignores `-1` diffs. Both functions log a `debug`-level message when a retraction is received. Users needing retractable first/last semantics should use `MIN`/`MAX` on a timestamp-correlated value.

**NULL handling in aggregates:** All aggregate functions skip NULL input values (following SQL standard). `COUNT(*)` counts all rows including NULLs; `COUNT(x)` counts only non-NULL values of `x`. If all input values for a group are NULL (or the group is empty after retractions), aggregates return NULL — except `COUNT(*)` and `COUNT(x)` which return 0.

**State cost matters for memory and checkpointing.** A `GROUP BY user_id` with `SUM(amount)` over 10M users needs ~80MB (8 bytes per key). The same query with `MEDIAN(amount)` and 100 values per user needs ~8GB (all values retained). FoldDB logs a warning when proportional-state aggregates exceed a configurable threshold.

**Scalar:**

| Function | Description |
|---|---|
| `COALESCE(a, b, ...)` | First non-null |
| `NULLIF(a, b)` | NULL if a = b |
| `CASE WHEN ... THEN ... ELSE ... END` | Conditional |
| `LENGTH(s)` | String length |
| `UPPER(s)` / `LOWER(s)` | Case conversion |
| `TRIM(s)` / `LTRIM(s)` / `RTRIM(s)` | Whitespace trim |
| `SUBSTR(s, start, len)` | Substring |
| `REPLACE(s, from, to)` | String replace |
| `SPLIT_PART(s, delim, idx)` | Split and index |
| `PARSE_TIMESTAMP(s, fmt)` | Parse string to timestamp |
| `FORMAT_TIMESTAMP(ts, fmt)` | Format timestamp to string |
| `NOW()` | Current processing time |
| `EXTRACT(field FROM ts)` | Extract year/month/day/hour/minute/second |
| `json_keys(j)` | Array of keys in a JSON object |

### 3.5 Window Clause (FoldDB Extension)

Windowed aggregation uses a `WINDOW` clause that specifies the window type after the `GROUP BY`. Two implicit columns — `window_start` and `window_end` — become available in `SELECT`.

**Tumbling window** (non-overlapping, fixed size):

```sql
SELECT window_start, window_end, user_id, COUNT(*) AS cnt
FROM 'kafka://broker/clicks'
GROUP BY user_id
WINDOW TUMBLING '1 minute'
```

**Sliding window** (overlapping, fixed size, fixed slide):

```sql
SELECT window_start, user_id, COUNT(*) AS cnt
FROM 'kafka://broker/clicks'
GROUP BY user_id
WINDOW SLIDING '10 minutes' BY '5 minutes'
```

**Session window** (activity-based, variable size):

```sql
SELECT window_start, window_end, user_id, COUNT(*) AS cnt
FROM 'kafka://broker/clicks'
GROUP BY user_id
WINDOW SESSION '5 minutes'
```

**Duration literals** follow Postgres `INTERVAL` syntax: `'1 minute'`, `'30 seconds'`, `'1 hour'`, `'1 day'`.

**Window assignment:** Each record is assigned to one or more windows based on its timestamp (processing time by default, or event time if `EVENT TIME BY` is specified). For tumbling windows, each record belongs to exactly one window. For sliding windows, a record may belong to multiple overlapping windows. For session windows, a record either extends an existing session or starts a new one.

**Window lifecycle:**
1. **Open:** A window is created when the first record for that `(window_start, ...group_keys)` arrives.
2. **Active:** Records are accumulated. If `EMIT EARLY` is configured, partial results are emitted periodically.
3. **Close:** The watermark advances past `window_end`. A final result is emitted (for `EMIT FINAL`) and the window's accumulator state is discarded.
4. **Late arrival:** A record arrives for a window that has already closed. See Section 13 for handling.

**Session window merge:** When a new record arrives within the session gap of an existing session for the same group key, the session is extended. If the record falls between two existing sessions and bridges the gap, the sessions are **merged** — their accumulators are combined and a retraction is emitted for the old sessions followed by an insertion for the merged session.

**Non-windowed streaming aggregation:**

Queries without `WINDOW` aggregate over the entire stream (running/cumulative):

```sql
-- Running count per region, no window boundary
SELECT region, COUNT(*) AS orders
FROM 'kafka://broker/orders.cdc' FORMAT DEBEZIUM
GROUP BY region
```

### 3.6 Emit Clause (FoldDB Extension)

Controls when windowed results are emitted:

```sql
EMIT FINAL              -- default: emit once when window closes
EMIT EARLY '10 seconds' -- periodic partial results before window closes
```

Non-windowed accumulating queries emit on every change by default (each input record that changes an accumulator triggers an output diff).

### 3.7 Event Time and Watermarks (FoldDB Extension)

By default, windows use processing time (when FoldDB receives the record). To use a field from the data as event time:

```sql
SELECT window_start, user_id, COUNT(*)
FROM 'kafka://broker/events'
GROUP BY user_id
WINDOW TUMBLING '1 minute'
EVENT TIME BY event_time
WATERMARK '30 seconds'
```

`EVENT TIME BY` declares which column carries event time. `WATERMARK` sets the allowed lateness — records arriving more than this amount behind the watermark are dropped (or routed to `--dead-letter`).

**Timestamp parsing:** Event time values are parsed using ISO 8601 format. If no timezone offset is present, the timestamp is assumed to be UTC. Timestamps with timezone offsets (e.g., `+05:30`, `-04:00`, `Z`) are converted to UTC internally. Unix epoch timestamps (integer or float) are interpreted as UTC seconds since epoch. Examples:
- `"2026-03-28T14:30:00Z"` — UTC
- `"2026-03-28T14:30:00+05:30"` — converted to `2026-03-28T09:00:00Z`
- `"2026-03-28T14:30:00"` — assumed UTC (no offset)
- `1743170400` — interpreted as UTC seconds since epoch
- `1743170400.123` — interpreted as UTC seconds since epoch with sub-second precision

If `EVENT TIME BY` is specified without `WATERMARK`, the default lateness is `'5 seconds'`.

**EVENT TIME BY edge cases:**
- **NULL event time:** If the event time column is NULL for a record, the record is assigned **processing time** and a `debug`-level log is emitted. This is a pragmatic choice — dropping records with NULL timestamps would silently lose data.
- **Unparseable event time:** If the column exists but cannot be parsed as a timestamp (e.g., a malformed string), the record is routed to `--dead-letter` (if configured) or dropped with a `warn`-level log. A count of dropped records is shown in the TUI footer.
- **EVENT TIME BY on a non-existent column:** This is a **parse-time error** if schema is known, or a **runtime error** on the first record if schema is inferred. The error message suggests available column names.
- **`EVENT TIME BY` without `WINDOW`:** Valid. The event time column is used for watermark tracking. Non-windowed accumulating queries use the watermark to determine when to discard dedup state or closed session state.

**Watermark advancement:** The watermark advances to `max(event_time_seen) - watermark_delay`. For multi-partition Kafka sources, FoldDB tracks the **minimum** watermark across all assigned partitions. The watermark does not advance until all partitions have produced at least one record past the threshold. A stalled partition (no records for > 30 seconds) is excluded from the minimum calculation to prevent watermark stalls; this is logged at `info` level.

### 3.8 Deduplication

```sql
DEDUPLICATE BY order_id WITHIN 10 MINUTES
```

Maintains a bounded LRU cache of seen keys. Records with a previously-seen key within the window are dropped. The dedup check happens before any operators (filter, project, aggregate).

**Default cache size:** 100,000 entries. Configurable via the `CAPACITY` keyword:

```sql
DEDUPLICATE BY order_id WITHIN '10 minutes' CAPACITY 500000
```

When the cache exceeds capacity, the least recently seen key is evicted. A `debug`-level log is emitted on each eviction.

**NULL keys in dedup:** NULL keys are treated as equal for deduplication purposes, following the SQL `GROUP BY` convention where all NULLs are grouped together. A record with a NULL dedup key will deduplicate against any other record with a NULL dedup key within the window.

**Retractions and dedup:** Deduplication only applies to insertions (`diff=+1`). Retractions (`diff=-1`) always pass through the dedup filter unconditionally. Deduplicating a retraction would cause incorrect accumulator state — the accumulator has already applied the corresponding insertion, so the retraction must reach it to maintain correctness.

For Kafka sources, infrastructure-level dedup (same partition+offset delivered twice) is handled automatically via offset checkpointing — no user syntax needed.

---

## 4. Source Connectors (v0)

### 4.1 Kafka

URI format:

```
kafka://[credentials@]host[:port]/topic[?params]
```

Parameters:

| Param | Default | Description |
|---|---|---|
| `offset` | `latest` | `earliest`, `latest`, ISO timestamp, or integer offset |
| `partition` | all | Comma-separated partition IDs |
| `group` | (ephemeral) | Consumer group ID; if set, offsets are committed |
| `registry` | (none) | Schema registry URL for Avro/Protobuf |

Authentication:

| Method | How to configure |
|---|---|
| PLAINTEXT | No auth params needed |
| SASL/PLAIN | `sasl_mechanism=PLAIN&sasl_username=X&sasl_password=Y` or credentials file |
| SASL/SCRAM-256/512 | `sasl_mechanism=SCRAM-SHA-256` + username/password |
| mTLS | `tls_cert=/path/cert.pem&tls_key=/path/key.pem&tls_ca=/path/ca.pem` |

Credentials file (`~/.folddb/credentials`):

```toml
[kafka.my-cluster]
brokers = ["pkc-xxx.confluent.cloud:9092"]
sasl_mechanism = "PLAIN"
sasl_username = "my-key"
sasl_password = "my-secret"
registry = "https://psrc-xxx.confluent.cloud"
```

Alias in query:

```sql
FROM 'kafka://my-cluster/events'   -- resolved from credentials file
```

#### Virtual Columns (Kafka)

Every Kafka record exposes:

| Column | Type | Description |
|---|---|---|
| `_offset` | BIGINT | Partition offset |
| `_partition` | INT | Partition number |
| `_timestamp` | TIMESTAMP | Kafka log-append timestamp |
| `_key` | TEXT | Message key (decoded as UTF-8; JSON-parsed if valid JSON) |

#### Virtual Columns (Debezium)

When `FORMAT DEBEZIUM` is specified, additionally:

| Column | Type | Description |
|---|---|---|
| `_op` | TEXT | `c`, `u`, `d`, `r` |
| `_before` | JSON | Row state before the change (null for inserts) |
| `_after` | JSON | Row state after the change (null for deletes) |
| `_table` | TEXT | Source table name |
| `_db` | TEXT | Source database name |
| `_ts` | TIMESTAMP | Source database transaction timestamp |
| `_source` | JSON | Full Debezium `source` block (for advanced dedup via `txId`/`lsn`) |

### 4.2 stdin

Implicit when no `FROM` clause, or explicit with `stdin://`:

```bash
cat data.json | folddb "SELECT name WHERE age > 30"
cat data.json | folddb "SELECT * FROM 'stdin://' FORMAT CSV"
```

stdin is always a stream source (`diff = +1` for all records).

**Empty stdin:** When stdin is empty (EOF immediately), FoldDB emits no output and exits with code 0. For accumulating queries, no result is emitted (not even an empty table). This is consistent with SQL behavior where an empty table produces no `GROUP BY` output rows.

### 4.3 Format Detection

| Source | Default format | Override |
|---|---|---|
| Kafka (no registry) | NDJSON | `FORMAT CSV`, `FORMAT AVRO`, `FORMAT DEBEZIUM` |
| Kafka (with registry) | Auto-detect via magic byte | `FORMAT AVRO`, `FORMAT PROTOBUF` |
| stdin | NDJSON | `FORMAT CSV(...)` |

#### CSV Format Options

```sql
FORMAT CSV(
    delimiter = ',',
    header = true,
    quote = '"',
    null_string = ''
)
```

#### Avro

When a schema registry URL is configured (via URI param or credentials file), FoldDB auto-detects the Confluent wire format (magic byte `0x00` + 4-byte schema ID) and fetches the schema from the registry. No `FORMAT` clause needed.

Explicit: `FORMAT AVRO` or `FORMAT AVRO(registry = 'http://registry:8081')`.

---

## 5. Stateful Checkpointing

### 5.1 `--stateful` Flag

```bash
folddb --stateful "SELECT region, COUNT(*) FROM 'kafka://broker/orders.cdc' FORMAT DEBEZIUM GROUP BY region"
```

When enabled:

**Checkpoint contents:**
- Query fingerprint (hash of normalized SQL)
- Per-partition Kafka offsets (last processed)
- Serialized accumulator state (the GROUP BY key → accumulator map)
- Dedup cache (if `DEDUPLICATE BY` is active)
- Checkpoint timestamp

**Flush cadence:** every 5 seconds or every 10,000 records, whichever comes first. Configurable via `--checkpoint-interval`.

**Flush mechanism:** atomic write (write to temp file, `fsync`, rename). A crash mid-flush never corrupts the checkpoint. **Disk full during checkpoint:** If the disk is full during the temp file write, the write fails and the previous checkpoint remains intact (the rename never occurs). FoldDB logs an error and continues processing without checkpointing. It retries on the next checkpoint interval. If the disk remains full for 3 consecutive checkpoint intervals, FoldDB exits with an error: `Error: disk full — unable to write checkpoint for 3 consecutive intervals. Exiting to prevent unbounded data loss on restart.`

**On restart:** load checkpoint, verify query fingerprint matches, resume from checkpointed offsets. The gap between the checkpoint and the current stream position is replayed (typically seconds of data).

**On query change:** query fingerprint won't match. FoldDB warns and starts fresh (or `--force-replay` to explicitly discard old state).

**Delivery semantics: at-least-once.** FoldDB provides at-least-once processing, not exactly-once. The failure window is:

1. FoldDB processes records and updates the in-memory accumulator.
2. Output records are emitted to the sink (stdout, SQLite, etc.).
3. On checkpoint flush: accumulator state + Kafka offsets are written atomically to disk.
4. After successful checkpoint flush: offsets are committed to Kafka (if consumer group is configured).

**Crash scenarios:**
- **Crash between step 2 and step 3:** Output was emitted but checkpoint was not saved. On restart, records between the last checkpoint and the crash point are **replayed**, producing **duplicate output** for those records. For changelog output, downstream consumers that maintain a key-value map will converge to the correct state after replay (the replay will re-emit the same retraction/insertion pairs). For non-accumulating queries, duplicates will appear in the output.
- **Crash between step 3 and step 4:** Checkpoint is saved but Kafka offsets are not committed. On restart, FoldDB resumes from the checkpointed offsets (stored locally), not from the Kafka-committed offsets. The Kafka consumer group offset commit is best-effort — it exists so that external monitoring tools (e.g., Burrow) can track lag, not for correctness.
- **Crash during step 3 (mid-flush):** The atomic write (temp file + fsync + rename) ensures the previous checkpoint is intact. FoldDB resumes from the previous checkpoint.

**Exactly-once is not a v0 goal.** Achieving exactly-once would require transactional coordination between the output sink and the checkpoint store (e.g., writing output and checkpoint to the same SQLite transaction). This is a v1 consideration for the `--state` SQLite mode specifically.

**Checkpoint and SQLite state store interaction:** When both `--stateful` and `--state <file.db>` are used, the SQLite UPSERT and the checkpoint flush are **not** in the same transaction. A crash between them can produce a SQLite state that is slightly ahead of or behind the checkpoint. On restart, the replayed records will re-UPSERT into SQLite, converging to the correct state. This is safe because UPSERT is idempotent for accumulating queries (same GROUP BY key overwrites the same row).

### 5.2 State Directory

Default: `~/.folddb/state/<query-hash>/`

Override: `--state-dir /path/to/dir`

### 5.3 State Inspection

```bash
folddb state list                      # list all checkpointed queries
folddb state inspect <query-hash>      # show checkpoint metadata
folddb state reset <query-hash>        # delete checkpoint (next run replays from scratch)
```

`folddb state inspect` output:

```
Query:       SELECT region, COUNT(*) FROM 'kafka://broker/orders.cdc' ...
Hash:        a1b2c3d4
Offsets:     partition 0 = 48291042, partition 1 = 47103821
Keys:        48
State size:  12 KB
Last flush:  2026-03-28T14:02:31Z
```

---

## 6. Execution Architecture

### 6.1 Pipeline

```
Source Reader (Kafka / stdin)
    │
    │  raw bytes
    ▼
Format Decoder (JSON / CSV / Avro / Debezium)
    │
    │  Record (columns, timestamp, diff)
    ▼
Dedup Filter (if DEDUPLICATE BY)
    │
    ▼
WHERE Filter
    │
    ▼
Projection / Expression Evaluation
    │
    ▼
┌─────────────────────────────────┐
│ Accumulator (if GROUP BY)       │
│                                 │
│  key → accumulator state map    │
│  emits diffs on state change    │
│  respects WINDOW boundaries     │
│  respects EMIT cadence          │
└────────────┬────────────────────┘
             │
             ▼
Output Sink (TUI / changelog NDJSON / SQLite)
```

### 6.2 Kafka Source Reader

- One goroutine per assigned partition (via `franz-go`).
- Each goroutine decodes, applies the pipeline, and sends output records to a shared output channel.
- Backpressure: if the output channel is full (slow sink), the reader blocks — Kafka consumer session timeout must be configured to tolerate this.
- Offset commit: after each checkpoint flush, commit offsets to Kafka (if a consumer group is configured).

**Consumer rebalance handling:** FoldDB uses an ephemeral consumer group by default (unique group ID per invocation), so rebalances only occur when a `group` parameter is explicitly set. When a rebalance occurs:
1. **Partitions revoked:** FoldDB immediately flushes a checkpoint for the revoked partitions' state. Goroutines for revoked partitions are stopped. Accumulator entries that were exclusively fed by revoked partitions remain in memory (they cannot be cleanly separated in v0 because the accumulator is a single shared map).
2. **Partitions assigned:** New goroutines are started. If `--stateful` is enabled and a checkpoint exists for the new partitions, state is restored. Otherwise, consumption starts from the configured offset.
3. **Accumulator correctness on rebalance:** Because the accumulator is a single shared map across all partitions, a rebalance with a shared consumer group can produce incorrect aggregate state if different FoldDB instances process overlapping group keys from different partitions. **Recommendation:** For accumulating queries with consumer groups, either consume all partitions (default) or ensure the GROUP BY key is aligned with the Kafka partition key.

**Multi-partition timestamp ordering:** Records from different partitions arrive independently and may have different timestamps. FoldDB does **not** merge-sort across partitions — records are processed in arrival order. For event-time windowed queries, the watermark mechanism handles out-of-order data. For non-windowed queries, output order reflects arrival order, not event-time order.

### 6.3 Accumulator

The accumulator is a hash map: `composite_key → []AccumulatorState`.

Each aggregate function has an `AccumulatorState` implementation:

```go
type Accumulator interface {
    // State mutation
    Add(value Value)         // called for diff = +1
    Retract(value Value)     // called for diff = -1

    // Result
    Result() Value           // current aggregate value
    HasChanged() bool        // did Result() change after the last Add/Retract?

    // Persistence
    Marshal() []byte         // serialize state for checkpoint
    Unmarshal([]byte)        // restore state from checkpoint

    // Merge (for future sidecar pattern — not all aggregates support this)
    CanMerge() bool          // true for SUM, COUNT, MIN, MAX, HLL; false for MEDIAN, PERCENTILE
    Merge(other Accumulator) // merge partial aggregate from another instance
}
```

The pipeline calls `Add`/`Retract`, then checks `HasChanged()`. If true:
1. Emit a retraction (`op: "-"`) for the previous result (if one was emitted)
2. Emit a new result (`op: "+"`) for the updated value

This is uniform across all aggregates — `SUM` and `MEDIAN` use the same emission logic. The only difference is internal state cost.

For windowed queries, the accumulator is keyed by `(window_start, ...group_by_keys)`. When a window closes (watermark advances past `window_end`), the final result is emitted and the window's accumulators are discarded.

### 6.4 Memory Management

- Accumulator state is in-memory by default.
- `--memory-limit` (default 1GB): if the accumulator map exceeds this, FoldDB logs a warning and continues (v0 does not spill to disk; Badger spill is v1).
- For `--state <file.db>`, the SQLite file acts as overflow — accumulators are periodically flushed to SQLite and evicted from memory for low-frequency keys. Hot keys remain in memory. (This is a v1 optimization; v0 keeps all accumulators in memory.)

### 6.5 Parallelism

- Kafka: one goroutine per partition. Partitions are independent; no cross-partition ordering guarantee.
- Accumulator: single goroutine receives from all partition goroutines via a fan-in channel. This is the serialization point. For v0, this is sufficient — the bottleneck is Kafka consumption and deserialization, not accumulation.
- Output: single goroutine reads from the accumulator output channel and writes to the sink.

**Single-goroutine accumulator justification:** The accumulator goroutine performs a hash map lookup, one or more `Add`/`Retract` calls (each O(1) for COUNT/SUM/AVG, O(log n) for MIN/MAX/MEDIAN), and a conditional output emit. For O(1) aggregates, this is ~100ns per record. At 200K records/sec target throughput, the accumulator consumes ~20ms/sec of CPU — well under the budget. The bottleneck is Kafka fetch + deserialization + JSON parsing, which is parallelized across partition goroutines. The single-goroutine design also eliminates concurrency bugs in accumulator state and simplifies checkpoint serialization (no locking required).

**When this breaks down:** If the query uses O(n) aggregates (MEDIAN, PERCENTILE_CONT) with high-cardinality group keys and many values per key, the per-record cost rises. At 10K values per key with MEDIAN, each update is ~O(log 10K) = ~13 comparisons + heap rebalance, which is still fast. The real bottleneck for O(n) aggregates is **memory**, not CPU. If profiling shows the accumulator goroutine at >50% CPU utilization, the v1 mitigation is to shard the accumulator map by group key hash across N goroutines.

**Non-accumulating queries bypass the accumulator entirely** — partition goroutines write filtered/projected records directly to the output channel, achieving full parallelism.

---

## 7. CLI Design

### 7.1 Primary Command

```
folddb [flags] <SQL>
folddb [flags] -f <query-file.sql>
```

### 7.2 Flags

| Flag | Default | Description |
|---|---|---|
| `--format json` | (auto) | Output format: `json`, `csv`, `table` |
| `--mode tui\|changelog\|json` | (auto) | Output mode override |
| `--state <file.db>` | (none) | Write accumulator state to SQLite |
| `--stateful` | false | Enable checkpoint persistence for fast restart |
| `--state-dir <path>` | `~/.folddb/state` | Checkpoint directory |
| `--checkpoint-interval <dur>` | `5s` | How often to flush checkpoints |
| `--timeout <duration>` | (none) | Terminate after duration |
| `--limit <n>` | (none) | Terminate after n output records |
| `--memory-limit <bytes>` | `1GB` | Accumulator memory warning threshold |
| `--log-level` | `warn` | `debug`, `info`, `warn`, `error` |
| `--dry-run` | false | Parse and print query plan, don't execute |
| `--explain` | false | Print query plan then execute |
| `-f <file>` | (none) | Read SQL from file |

### 7.3 Subcommands

| Command | Description |
|---|---|
| `folddb <sql>` | Execute query (default) |
| `folddb schema <source>` | Print inferred schema for a source |
| `folddb state list` | List checkpointed queries |
| `folddb state inspect <hash>` | Show checkpoint details |
| `folddb state reset <hash>` | Delete a checkpoint |
| `folddb version` | Print version |

### 7.4 Schema Subcommand

```bash
$ folddb schema 'kafka://localhost:9092/orders.cdc'
Source:    kafka://localhost:9092/orders.cdc
Format:    NDJSON (detected)
Partitions: 12
Sample:    100 records from partition 0

Inferred schema:
  op          TEXT        "c", "u", "d"
  before      JSON        nullable
  after       JSON        {order_id: INT, status: TEXT, region: TEXT, total: FLOAT, ...}
  source      JSON        {db: TEXT, table: TEXT, ts_ms: BIGINT, lsn: TEXT, ...}

With FORMAT DEBEZIUM, virtual columns:
  _op         TEXT
  _before     JSON
  _after      JSON
  _table      TEXT
  _db         TEXT
  _ts         TIMESTAMP
  _source     JSON
```

---

## 8. Error Handling

### 8.1 Parse Errors

Precise location with caret and suggestion:

```
Error: unexpected token 'GRUP' at position 84

  SELECT region, COUNT(*) FROM 'kafka://...' GRUP BY region
                                              ^^^^
  Did you mean: GROUP BY?
```

### 8.2 Schema Errors

```
Error: column 'regin' not found in source schema

  Available columns: region, status, total, _op, _before, _after, ...
  Did you mean: region?
```

### 8.3 Runtime Errors

- **Kafka connection failure**: `Error: cannot connect to kafka://localhost:9092 — connection refused. Is the broker running?`
- **Deserialization error**: logged to stderr, record skipped, processing continues. Count of skipped records shown in TUI footer or logged periodically.
- **Retraction on empty accumulator**: warn and skip. This can happen if the stream starts mid-history (e.g., `offset=latest` and the first event for a key is an update). Specifically: if `Retract` is called on a group key that does not exist in the accumulator map, the retraction is **dropped** (not applied). A `warn`-level log is emitted on the first occurrence; subsequent occurrences for the same key are logged at `debug` level to avoid log spam. The accumulator does **not** allow negative counts — `COUNT(*)` is clamped at 0, `SUM` does not go negative from phantom retractions.
- **Type errors in expressions**: e.g., `(_after->>'status')::int` where status is `"active"`. The record is skipped, counted as a deserialization error, and routed to `--dead-letter` if configured. The accumulator is not updated for skipped records.
- **Division by zero**: returns NULL (not an error). Follows PostgreSQL semantics for integer division by zero; float division by zero returns `+Inf`/`-Inf`/`NaN` per IEEE 754.
- **Maximum record/field size**: No hard limit on record or field size. Records are processed in memory. A single record larger than `--memory-limit` will cause the process to exceed the memory limit and may trigger OOM. For practical purposes, records larger than 10MB should be considered pathological. FoldDB does not stream-parse individual records — the entire record must fit in memory for deserialization.
- **JSON field type changes mid-stream**: If a JSON field changes type between records (e.g., `amount` is a string in record 1 and a number in record 2), FoldDB does not error. Each record is evaluated independently. The expression `(col->>'amount')::float` will succeed if the text representation is parseable as a float, regardless of the underlying JSON type. However, `col->'amount' + 1` will fail with a type error if `amount` is a string, since `->` preserves the JSON type and arithmetic on a JSON string is not defined.

### 8.4 Dead Letter Output

```bash
folddb --dead-letter errors.ndjson "..."
```

Records that fail deserialization or cause runtime errors are written to the dead letter file with error context:

```jsonl
{"error":"json parse error: unexpected EOF","offset":481029,"partition":3,"raw":"truncated..."}
```

---

## 9. Implementation Plan

### Phase 0a — Parser + Scaffold

**Goal:** Parse v0 SQL subset, build CLI skeleton, prove out the pipeline architecture with a trivial source.

- Go module init, project structure
- SQL lexer (tokenizer)
- SQL parser (recursive descent): SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, WINDOW, EMIT, DEDUPLICATE BY, FORMAT, EVENT TIME BY
- AST types
- CLI scaffold (cobra): `folddb <sql>`, `folddb schema`, `folddb version`
- Pipeline scaffold: Source → Decoder → Filter → Project → Accumulator → Sink (interfaces, wiring)
- stdin source + JSON decoder (simplest possible end-to-end)
- Non-accumulating output (NDJSON to stdout)
- Tests: parser unit tests, pipeline integration test with stdin JSON

**Done when:**
```bash
echo '{"name":"alice","age":30}' | folddb "SELECT name WHERE age > 25"
# → {"name":"alice"}
```

### Phase 0b — Accumulation Engine

**Goal:** GROUP BY with retraction-aware accumulators. TUI and changelog output modes.

- Accumulator framework: `Add()`, `Retract()`, `Result()`, `Marshal()`, `Unmarshal()`
- Implement: COUNT, SUM, AVG, MIN, MAX, FIRST, LAST
- Diff-aware pipeline: records carry `diff` field, accumulators handle +1/-1
- Changelog NDJSON output (piped mode)
- TUI output (TTY mode): terminal redraw, column alignment, footer with stats
- ORDER BY and LIMIT for accumulating queries (applied to output, not input)
- HAVING clause

**Done when:**
```bash
cat events.ndjson | folddb "SELECT status, COUNT(*) AS cnt GROUP BY status"
# TUI shows live-updating table; piping shows changelog diffs
```

### Phase 0c — Kafka Connector

**Goal:** Consume from Kafka topics. NDJSON and Avro formats. Debezium support.

- Kafka source reader (franz-go): multi-partition, parallel goroutines, fan-in
- SASL/PLAIN and SASL/SCRAM authentication
- mTLS authentication
- Confluent Schema Registry client (HTTP, schema caching)
- Avro deserialization (with registry auto-detect via magic byte)
- FORMAT DEBEZIUM: envelope unwrapping, virtual columns, diff derivation from `op`
- Credentials file (`~/.folddb/credentials`)
- `folddb schema <kafka-source>` subcommand
- Offset control: `earliest`, `latest`, timestamp, explicit offset
- Virtual columns: `_offset`, `_partition`, `_timestamp`, `_key`

**Done when:**
```bash
folddb "SELECT _after->>'region' AS region, COUNT(*) AS orders
        FROM 'kafka://localhost:9092/orders.cdc' FORMAT DEBEZIUM
        WHERE _op IN ('c','u','d')
        GROUP BY region"
# Live-updating output from a real Kafka topic with Debezium
```

### Phase 0d — Windowing, Dedup, Stateful Checkpoint

**Goal:** Tumbling/hopping/session windows. Deduplication. Persistent state for fast restart.

- Window manager: assigns records to windows based on event time or processing time
- `WINDOW TUMBLING / SLIDING / SESSION` clause implementations
- `window_start` / `window_end` implicit columns
- `EVENT TIME BY` and `WATERMARK` clauses
- Watermark tracking and window close detection
- `EMIT FINAL` / `EMIT EARLY` clauses
- DEDUPLICATE BY with bounded LRU cache
- Stateful checkpointing: accumulator serialization, offset persistence, atomic flush
- Checkpoint restore on startup
- `folddb state list/inspect/reset` subcommands
- `--state <file.db>` SQLite output mode

**Done when:**
```bash
folddb --stateful \
  "SELECT window_start, endpoint, COUNT(*) AS reqs, AVG(latency_ms) AS avg_lat
   FROM 'kafka://localhost:9092/api_requests'
   GROUP BY endpoint
   WINDOW TUMBLING '1 minute'
   EMIT FINAL"
# Outputs per-minute stats; survives restart with --stateful
```

### Phase 0e — Polish and Release

- CSV format support (Kafka + stdin)
- `--dead-letter` flag
- Error messages: caret positioning, suggestions
- `--dry-run` and `--explain` flags
- `--timeout` and `--limit` flags
- Performance profiling: target 200K records/sec for simple filter/project on Kafka
- Integration tests with testcontainers (Kafka + Schema Registry)
- Binary builds: Linux amd64/arm64, macOS amd64/arm64
- README, usage examples

---

## 10. Project Structure

```
folddb/
├── cmd/
│   └── folddb/
│       └── main.go              # CLI entrypoint
├── internal/
│   ├── sql/
│   │   ├── lexer/               # tokenizer
│   │   ├── parser/              # recursive descent parser
│   │   ├── ast/                 # AST node types
│   │   └── planner/             # AST → execution pipeline
│   ├── engine/
│   │   ├── record.go            # Record type (values, timestamp, diff)
│   │   ├── pipeline.go          # pipeline wiring (source → operators → sink)
│   │   ├── filter.go            # WHERE evaluation
│   │   ├── project.go           # SELECT expression evaluation
│   │   ├── accumulator.go       # accumulator interface + implementations
│   │   ├── window.go            # window assignment + watermark tracking
│   │   └── dedup.go             # dedup LRU cache
│   ├── source/
│   │   ├── kafka.go             # kafka consumer (franz-go)
│   │   ├── stdin.go             # stdin reader
│   │   └── source.go            # Source interface
│   ├── format/
│   │   ├── json.go              # JSON / NDJSON decoder
│   │   ├── csv.go               # CSV decoder
│   │   ├── avro.go              # Avro decoder (with registry)
│   │   ├── debezium.go          # Debezium envelope unwrapper
│   │   └── format.go            # Format interface
│   ├── sink/
│   │   ├── tui.go               # TUI output (terminal redraw)
│   │   ├── changelog.go         # changelog NDJSON output
│   │   ├── json.go              # plain NDJSON stream output
│   │   ├── sqlite.go            # SQLite state store output
│   │   └── sink.go              # Sink interface
│   ├── state/
│   │   ├── checkpoint.go        # stateful checkpoint: serialize/deserialize/flush
│   │   └── manager.go           # state directory management, inspect, reset
│   └── registry/
│       └── confluent.go         # Confluent Schema Registry HTTP client
├── go.mod
├── go.sum
├── Makefile
└── README.md
```

---

## 11. Key Dependencies

| Purpose | Library |
|---|---|
| Kafka client | `github.com/twmb/franz-go` |
| Avro deserialization | `github.com/linkedin/goavro/v2` |
| CLI framework | `github.com/spf13/cobra` |
| TUI rendering | `github.com/charmbracelet/bubbletea` (or `github.com/gdamore/tcell`) |
| SQLite driver | `github.com/mattn/go-sqlite3` (CGo) or `modernc.org/sqlite` (pure Go) |
| Table formatting | `github.com/olekukonko/tablewriter` |
| JSON (fast) | `github.com/bytedance/sonic` or `github.com/goccy/go-json` |

**CGo note:** `go-sqlite3` requires CGo. For pure Go builds, use `modernc.org/sqlite` (slower writes but zero CGo). Decision: use `modernc.org/sqlite` for v0 to keep the build simple. Revisit if SQLite write performance is a bottleneck.

---

## 12. Semantic Rules and Edge Cases

This section enumerates the precise behavior for every edge case that an implementer or user might encounter. Where a decision is a pragmatic compromise (not the "theoretically correct" answer), it is marked with **[pragmatic]**.

### 12.1 NULL Semantics

FoldDB follows PostgreSQL three-valued logic (TRUE / FALSE / NULL).

**Three-valued logic truth table:**

| A | B | A AND B | A OR B | NOT A |
|---|---|---|---|---|
| TRUE | TRUE | TRUE | TRUE | FALSE |
| TRUE | FALSE | FALSE | TRUE | FALSE |
| TRUE | NULL | NULL | TRUE | NULL |
| FALSE | FALSE | FALSE | FALSE | TRUE |
| FALSE | NULL | FALSE | NULL | TRUE |
| NULL | NULL | NULL | NULL | NULL |

**NULL in comparisons:**
- `NULL = NULL` is NULL (not TRUE). Use `IS NOT DISTINCT FROM` for NULL-safe equality.
- `NULL != anything` is NULL. Use `IS DISTINCT FROM` for NULL-safe inequality.
- `NULL IN (1, 2, NULL)` is NULL (not TRUE).
- `NULL BETWEEN 1 AND 10` is NULL.

**NULL in GROUP BY keys:** NULL is a valid group key value. All records with NULL in a GROUP BY column are grouped together into a single group (following SQL standard). This group appears in output with `null` for that key column.

**NULL in aggregate functions:**
| Function | NULL input behavior |
|---|---|
| `COUNT(*)` | Counts the row (NULLs don't affect `*`) |
| `COUNT(x)` | Skips NULL values of `x` |
| `SUM(x)` | Skips NULL values; returns NULL if all inputs are NULL |
| `AVG(x)` | Skips NULL values; returns NULL if all inputs are NULL |
| `MIN(x)` / `MAX(x)` | Skips NULL values; returns NULL if all inputs are NULL |
| `MEDIAN(x)` | Skips NULL values; returns NULL if all inputs are NULL |
| `ARRAY_AGG(x)` | Includes NULL values in the array |
| `FIRST(x)` | Returns the first non-NULL value **[pragmatic]** |
| `LAST(x)` | Returns the most recent non-NULL value **[pragmatic]** |

**NULL in JSON access:**
- `NULL->'key'` returns NULL.
- `col->'nonexistent_key'` returns NULL.
- `col->>'key'` where the value is JSON `null` returns SQL NULL (not the string `"null"`).

**NULL in type casts:**
- `NULL::int` returns NULL.
- `''::int` is a type error (empty string is not NULL).

### 12.2 Retraction Edge Cases

| Scenario | Behavior |
|---|---|
| Retraction for a key not in the accumulator | Dropped. `warn` on first occurrence per key, `debug` thereafter. |
| Retraction that would make COUNT negative | COUNT clamped at 0. The key is removed from the accumulator. |
| Retraction that would make SUM go below zero | SUM is allowed to go negative (this is mathematically correct). |
| Retraction of a NULL value from SUM | Ignored (NULLs were never added). |
| Retraction from FIRST/LAST | Ignored (FIRST/LAST are not retractable). |
| Retraction from MIN/MAX | Value removed from heap. If heap is empty, aggregate returns NULL. |
| Retraction from ARRAY_AGG | First matching value removed from list. If duplicates exist, only one is removed. |
| Retraction with different value than was inserted | Undefined behavior. FoldDB does not validate that retractions match prior insertions. This can produce incorrect aggregate state. The Debezium `_before` field is the mechanism that ensures retractions carry the correct prior value. |
| Double retraction (same value retracted twice) | Processed as-is. For COUNT, this can drive the count below zero (clamped at 0). For SUM, the value is subtracted twice. **Users should ensure their source does not produce double retractions.** |
| Insert after all values retracted (COUNT was 0) | The group key is re-created in the accumulator. A fresh `+` emission occurs (no retraction emitted since the previous state was already retracted when COUNT hit 0). |

### 12.3 Window Edge Cases

| Scenario | Behavior |
|---|---|
| Record arrives for a closed window (late data) | If within `WATERMARK` lateness, the window is **re-opened** and the record is accumulated. If beyond the watermark, the record is dropped and routed to `--dead-letter` if configured. A `warn`-level log is emitted periodically (not per record). |
| Window with zero records after retractions | The window emits a retraction for its last emitted result and is discarded. The final emission is: `op: "-"` for the last result. No `op: "+"` follows. |
| Session window: two sessions merge | Retraction emitted for both old sessions, insertion emitted for the merged session. Accumulator states are combined (SUM adds, COUNT adds, MIN/MAX take the extreme, MEDIAN/PERCENTILE rebuild from combined value sets). |
| Session window: gap exactly equals session timeout | The record starts a **new** session (the boundary is exclusive: `gap > timeout` means new session, `gap <= timeout` means extend). **[pragmatic]** — using `>=` would be equally valid; we choose `>` for consistency with tumbling window boundaries which are `[start, end)`. |
| Tumbling/sliding window boundary alignment | Windows are aligned to epoch. A `TUMBLING '1 hour'` window starting at any time produces windows `[00:00, 01:00)`, `[01:00, 02:00)`, etc. in UTC. |
| Event time goes backward (out-of-order) | Handled by watermark. The record is assigned to the correct window based on its event time, even if that window is "earlier" than the most recently seen window. |
| `EMIT EARLY` with no data in window | No emission. Early emissions only fire for windows that have at least one accumulated record. |
| `EMIT EARLY` interval shorter than input rate | Each early emission is a full retraction/insertion pair for the current state. High `EMIT EARLY` frequency with many group keys can produce substantial output volume. |

### 12.4 Type Coercion Rules

Implicit coercion is applied in expressions where types don't match, following PostgreSQL rules:

| Context | Rule |
|---|---|
| Arithmetic (`+`, `-`, `*`, `/`) between INT and FLOAT | INT promoted to FLOAT |
| Comparison between INT and FLOAT | INT promoted to FLOAT |
| Comparison between TEXT and INT/FLOAT | Error — no implicit coercion. Use explicit `::int` or `::float`. |
| `||` (concat) with non-TEXT operand | Non-TEXT operand cast to TEXT implicitly |
| JSON `->>` result used in arithmetic | Error — `->>` returns TEXT, must explicitly cast: `(col->>'x')::int` |
| BOOLEAN in arithmetic | Error — no implicit coercion |
| TIMESTAMP compared to TEXT | TEXT is parsed as timestamp using ISO 8601. Parse failure is a runtime error. |

### 12.5 Kafka-Specific Edge Cases

| Scenario | Behavior |
|---|---|
| Broker unreachable at startup | Retry with exponential backoff (1s, 2s, 4s, 8s, max 30s). After `--timeout` or 5 minutes (whichever is less), exit with error. |
| Broker becomes unreachable mid-stream | Kafka client retries internally (franz-go defaults). FoldDB continues processing buffered records. If reconnection fails after session timeout, exit with error. Checkpoint is flushed on exit if `--stateful`. |
| Topic does not exist | Immediate error: `Error: topic 'X' does not exist on broker Y. Available topics: ...` (lists up to 20 topics). |
| Partition produces no data for extended period | For watermark tracking: partition excluded from minimum watermark calculation after 30s of inactivity (see Section 3.7). For backpressure: idle partitions do not consume resources. |
| Message key is not valid UTF-8 | `_key` is set to the base64 encoding of the raw bytes, prefixed with `b64:`. Example: `b64:AQIDBA==`. |
| Message value is empty (null payload) | Record is skipped. Counted as a deserialization error. Debezium tombstones (null value, non-null key) are silently dropped without incrementing the error counter. |
| Schema registry unreachable | If schema registry is configured (via URI param or credentials) and unreachable, FoldDB retries 3 times, then exits with an error. Schema fetch failures mid-stream (e.g., new schema ID encountered) are logged and the record is routed to `--dead-letter`. |
| Consumer group rebalance | See Section 6.2. |

### 12.6 Output Determinism

**Accumulating queries (TUI mode):** Output order is determined by ORDER BY if specified, otherwise by insertion order of group keys into the accumulator map (which is **not deterministic** across runs due to hash map iteration order). For deterministic output, always specify ORDER BY.

**Accumulating queries (changelog mode):** Each output line is a retraction/insertion pair for a single key. The ordering of pairs across different keys is determined by the order in which input records are processed, which depends on Kafka partition assignment and arrival timing. This order is **not deterministic** across runs.

**Non-accumulating queries:** Output order matches input arrival order. For multi-partition Kafka sources, this is the interleaved arrival order across partitions (not event-time order, not partition order).

**SQLite state store:** The SQLite table has no inherent ordering. Queries against the `result` table should specify ORDER BY. The UPSERT order is the order in which the accumulator emits updates.

### 12.7 PRD Alignment Notes

The v0 spec (FoldDB) is the first implementation phase of the broader `neql` vision described in the PRD. The following syntactic differences exist between the v0 spec and the PRD and will be reconciled when FoldDB merges into neql:

| Aspect | v0 Spec (FoldDB) | PRD (neql) | Resolution |
|---|---|---|---|
| Window syntax | `WINDOW TUMBLING '1 minute'` | `WINDOW TUMBLING(SIZE 1 MINUTE)` | v0 uses the simpler form; the PRD form will be accepted as an alias in v1 |
| Sliding window keyword | `SLIDING` | `HOPPING` | v0 uses `SLIDING`; both will be accepted as aliases in v1 |
| Emit syntax | `EMIT FINAL` / `EMIT EARLY '10s'` | `EMIT ON WINDOW CLOSE` / `EMIT EVERY 10 SECONDS` / `EMIT ON EVERY RECORD` | v0 uses shorter forms; the PRD forms will be accepted as aliases in v1 |
| `window_start`/`window_end` | Implicit columns (lowercase) | `WINDOW_START`/`WINDOW_END` (uppercase) | v0 is case-insensitive for these; both forms work |
| State backend | In-memory only (v0), Badger spill (v1) | RocksDB mentioned, then Badger decided | Badger is the decided backend per PRD Section 18 |
| `OFFSET` clause | Not supported in v0 | Supported in PRD grammar | Deferred to v1 with table source support |
| `COUNT(DISTINCT x)` | Exact (hash set) + `APPROX_COUNT_DISTINCT` (HLL) | HyperLogLog only | v0 provides both; the exact variant is correct for low-cardinality streams |

---

## 13. Out of Scope (v0)

Explicitly not in v0, deferred to v1+:

- Table sources (files, databases, object storage)
- DuckDB integration
- Joins of any kind
- SEED FROM (warehouse bootstrap)
- `folddb serve` (HTTP/sidecar mode)
- Sidecar merge / ingest API
- Protobuf format
- Badger spill-to-disk for large accumulator state
- `WATCH` mode
- Write/sink to Kafka or databases
- CDC from Postgres/MySQL directly (only Debezium-over-Kafka)
- `OFFSET` clause (only meaningful with table sources)
- `UNION` / `INTERSECT` / `EXCEPT` set operations
- Exactly-once output semantics (v0 is at-least-once; see Section 5.1)
- Disk spill for DISTINCT sets or dedup caches
- Custom UDFs / UDAFs

---

*End of v0 Spec*
