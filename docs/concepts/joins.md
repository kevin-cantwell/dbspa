# Joins

FoldDB supports joins between streams and tables, between streams and CDC sources, and between two live streams. All joins use a **differential dataflow** (DD) operator built on the [Z-set model](diff-model.md).

## How it works

Both sides of a join maintain an **arrangement** -- an indexed Z-set that supports insertion, lookup, and scan. When a delta (new or changed record) arrives on either side, it is joined against the other side's arrangement and the results are emitted.

The core formula is from DBSP:

```
delta(A JOIN B) = (delta_A JOIN B) UNION (A JOIN delta_B)
```

When a new record arrives on the left, it is looked up against the full right arrangement. When a new record arrives on the right, it is looked up against the full left arrangement. The union of both results forms the output delta.

**Weight multiplication:** The output weight of a joined record is `left_weight * right_weight`. This is what makes the algebra correct -- a retraction on either side (negative weight) produces retractions in the output.

```sql
SELECT e.user_id, u.name, u.email, e.action
FROM stdin e
JOIN '/data/users.parquet' u ON e.user_id = u.id
```

1. FoldDB loads `users.parquet` into the right arrangement, indexed by `u.id`.
2. For each record from stdin, the DD join operator looks up matching rows in the right arrangement.
3. Matched records are merged (stream columns + table columns) with `output_weight = left_weight * right_weight`, and flow to the next pipeline stage.

## Supported join types

| Join type | Syntax | Unmatched stream records |
|---|---|---|
| INNER JOIN | `JOIN` | Dropped |
| LEFT JOIN | `LEFT JOIN` | Emitted with NULL for table columns |

```sql
-- LEFT JOIN: keep events even when no user match
SELECT e.user_id, COALESCE(u.name, 'unknown') AS name, e.action
FROM stdin e
LEFT JOIN '/data/users.parquet' u ON e.user_id = u.id
```

**LEFT JOIN with CDC:** When a left record initially has no match, it is emitted with NULLs for the right columns. If a matching right record later arrives (via CDC or stream), the NULL row is retracted and replaced with the fully joined row.

## Join variants

### Stream-to-file (static right side)

The simplest case. The right side is loaded from a file once at startup and never changes.

```sql
SELECT e.*, u.name
FROM 'kafka://broker/events' e
JOIN '/data/users.parquet' u ON e.user_id = u.id
```

FoldDB applies a **RightIsStatic** optimization: since the right arrangement never receives deltas, the left arrangement is skipped entirely. This eliminates `json.Marshal` fingerprinting on every stream record.

### Stream-to-CDC (mutable right side)

The right side is loaded from an initial snapshot and then updated via Debezium CDC. When a right-side record changes, the DD join retracts all previously emitted results that used the old value and re-emits corrected results with the new value.

```sql
SELECT o.order_id, o.total, c.name, c.tier
FROM 'kafka://broker/orders' o FORMAT DEBEZIUM
JOIN 'kafka://broker/customers.cdc' c FORMAT DEBEZIUM ON o.customer_id = c.id
```

If a customer's `tier` changes from "gold" to "platinum", every previously joined order for that customer is retracted (weight=-1) and re-emitted with the new tier (weight=+1).

### Stream-to-stream (WITHIN clause)

Both sides are live, unbounded sources. A `WITHIN INTERVAL` clause is **mandatory** to bound the arrangements -- without it, both sides would grow indefinitely.

```sql
SELECT o.order_id, o.total, p.amount, p.method
FROM 'kafka://broker/orders' o
JOIN 'kafka://broker/payments' p ON o.order_id = p.order_id
WITHIN INTERVAL '10 minutes'
```

**Architecture:**

```
Kafka topic A --goroutine--> Left Arrangement  --+
                                                  +--> DDJoinOp --> output
Kafka topic B --goroutine--> Right Arrangement --+
                                                  ^
Eviction ticker ----------------------------------+
```

Both source goroutines feed deltas into the DD join operator, which is protected by a `sync.Mutex` for thread safety.

**Retention and eviction:** A background goroutine calls `EvictAndRetract` at `withinDuration/10` intervals. Evicted entries produce retractions through the join (negated weights), so downstream accumulators stay correct. The eviction holds the join mutex to prevent concurrent modification.

!!! warning
    Stream-to-stream joins require both sources to be Kafka topics (or one Kafka topic and one file). Two-source joins are not possible with stdin.

## Table source formats

The table file can be any format FoldDB supports:

- Parquet (`.parquet`) -- routed through DuckDB for fast loading
- NDJSON (`.json`, `.ndjson`)
- CSV (`.csv`)
- Avro (`.avro`)

The format is detected from the file extension or specified explicitly:

```sql
JOIN '/data/users.csv' u FORMAT CSV(header=true) ON e.user_id = u.id
```

## Join condition

Only equi-joins are supported. The ON condition must be of the form `a.x = b.y`:

```sql
-- Valid
ON e.user_id = u.id
ON e.region = u.region_code

-- Invalid (not an equi-join)
ON e.user_id > u.id
ON e.user_id = u.id AND e.region = u.region
```

If column references are qualified (prefixed with an alias), they are matched to the stream or table side by alias. If unqualified, the left operand is assumed to be the stream side and the right is the table side.

## Diff propagation

Joins propagate [Z-set weights](diff-model.md) correctly through weight multiplication. If the stream source produces a retraction (weight=-1) and it matches a right-side record (weight=+1), the output weight is `-1 * +1 = -1` -- a retraction of the joined result.

This extends to CDC on both sides: a retraction on the right side retracts all matching joined results, then the updated right record re-emits corrected results. The math is always `output_weight = left_weight * right_weight`.

## Projection pushdown

The join merge only copies columns that are referenced by the query. For a 30-column merge where only 6 columns are used, this reduces per-record allocation by 5x. The optimizer inspects the SELECT, WHERE, GROUP BY, and ORDER BY clauses to determine which columns are needed.

## Disk-backed arrangements

For large joins or long windows, arrangements can spill to disk. Enable with `--spill-to-disk` or set a memory budget with `--max-memory` (e.g., `--max-memory 512MB`). Stream-stream joins auto-enable spill-to-disk. See [Disk-Backed Arrangements](../architecture/performance.md#disk-backed-arrangements) for overhead benchmarks.

## Subquery as JOIN source

Instead of a file path, the JOIN source can be a parenthesized subquery. The subquery is executed to completion (materialized) before stream processing begins. This is especially powerful when you need to pre-aggregate or filter reference data before joining.

### CDC aggregation example

A common pattern in change data capture pipelines: join a live event stream against pre-aggregated order counts from a Parquet snapshot.

```sql
-- Enrich each customer event with their historical order count
SELECT e.customer_id, e.action, r.order_count
FROM 'kafka://broker/customer_events' e
JOIN (SELECT customer_id, COUNT(*) AS order_count
      FROM '/data/orders.parquet'
      GROUP BY customer_id) r
  ON e.customer_id = r.customer_id
```

Without subqueries, you would need to pre-compute the aggregation in a separate pipeline and write it to a file. With subqueries, the entire workflow is a single query.

### How it works

1. FoldDB parses the inner `SELECT` and executes it as a standalone query.
2. For file sources, DuckDB handles the scan; for GROUP BY, FoldDB's aggregation engine computes the final state.
3. The materialized results are loaded into the right-side join arrangement.
4. Stream processing begins, joining each incoming record against the pre-computed table.

The subquery supports the full FoldDB SQL dialect (WHERE, GROUP BY, HAVING, LIMIT, nested JOINs). For file-based subqueries, Kafka sources are not supported (they would block forever). For Kafka-based subqueries, see Streaming Subqueries below.

## FROM Streaming Subqueries

A FROM subquery can also use a Kafka source. The inner accumulating query runs concurrently and streams its Z-set deltas (retraction+insertion pairs) to the outer query's pipeline. The outer SELECT/WHERE filters operate on each delta as it arrives, enabling real-time filtering of aggregation results.

```sql
-- Filter for order statuses with more than 100 orders, updated live
SELECT * FROM (SELECT status, COUNT(*) AS cnt
                FROM 'kafka://broker/orders.cdc' FORMAT DEBEZIUM
                GROUP BY status) t
WHERE cnt > 100
```

The inner query maintains a live count-per-status aggregation. Each time the count changes, a retraction of the old value and insertion of the new value flow through the outer WHERE filter. Only groups currently exceeding the threshold appear in the output.

## File Left + Streaming Right

When the outer query's FROM source is a file and the JOIN source is a streaming subquery, the file is loaded into the left arrangement as static reference data and the stream runs continuously on the right side. The query keeps running after the file is fully loaded -- right-side deltas are joined against all loaded left records via `ProcessRightDelta`.

```sql
-- Enrich a static customer list with live order counts
SELECT c.name, c.tier, r.order_count
FROM '/data/customers.parquet' c
JOIN (
    SELECT customer_id, COUNT(*) AS order_count
    FROM 'kafka://broker/orders.cdc' FORMAT DEBEZIUM
    GROUP BY customer_id
) r ON c.id = r.customer_id
```

The customer file is loaded once. As orders stream in, the inner subquery updates the count, and the DD join emits corrected results against the full customer table.

## Streaming Subqueries (JOIN)

When the inner query's FROM source is a Kafka topic (`kafka://`), the subquery runs **concurrently** with the outer query instead of being materialized first. The inner query's accumulation results feed into the DD join as live Z-set deltas via `ProcessRightDelta`.

### How it works

```
Inner query goroutine:
  Kafka CDC --> Debezium decode --> Accumulate --> Z-set deltas --> channel

Outer query goroutine:                                              |
  Kafka events --> Decode --> ProcessLeftDelta <-> DD Join <-- ProcessRightDelta (from channel)
                                                    |
                                                  Output
```

1. The inner subquery starts in a goroutine: Kafka source, decode, filter, and accumulate.
2. Each accumulation change emits a retraction of the old value and an insertion of the new value to a shared channel.
3. The outer query reads from that channel via `ProcessRightDelta`, updating the right-side arrangement.
4. Left-side records from the outer query are joined against the live-updating right arrangement.
5. Both goroutines run concurrently, connected by the shared DD join operator (protected by a mutex).

### CDC composition use case

The flagship use case is composing two Kafka streams -- one for raw events, one for CDC-derived aggregates:

```sql
SELECT e.user_id, r.revenue
FROM 'kafka://broker/events' e
JOIN (
    SELECT region, SUM(amount) AS revenue
    FROM 'kafka://broker/orders.cdc' FORMAT DEBEZIUM
    GROUP BY region
) r ON e.region = r.region
```

The inner query maintains a live revenue-per-region aggregation from the CDC stream. The outer query joins each incoming event against the current aggregation state. When an order is updated or deleted in the source database, the CDC stream produces a change, the inner accumulator updates, and the DD join retracts stale results and emits corrected ones.

### Retraction propagation

Retractions flow end-to-end through the pipeline:

1. **Source:** A Debezium update produces a retraction of the old row and insertion of the new row.
2. **Accumulator:** The group's aggregate changes. The accumulator emits a retraction of the old aggregate value (weight=-1) and an insertion of the new value (weight=+1).
3. **DD Join:** `ProcessRightDelta` applies these deltas to the right arrangement. For each retracted right record, the join produces retractions for all matching left records (`output_weight = left_weight * right_weight`). For each inserted right record, the join produces new results.
4. **Sink:** The changelog or TUI renders the corrections.

### Requirements and limitations

- **GROUP BY is mandatory.** The inner subquery must have a GROUP BY clause (or aggregates that imply a single group). Without accumulation, raw Kafka events do not form a useful join table -- each event would insert a row that is never retracted, growing the arrangement unboundedly. If GROUP BY is absent, FoldDB prints a warning and falls back to materialized execution (which blocks forever on Kafka).
- **Not supported in serve mode.** Streaming subqueries are not yet available when running `folddb serve`.

## Limitations

- **Only simple equi-joins.** Composite keys (multiple conditions with AND) are not supported in v0.
- **Stream-to-stream requires WITHIN.** FoldDB errors at parse time if `WITHIN INTERVAL` is missing for a stream-stream join.
