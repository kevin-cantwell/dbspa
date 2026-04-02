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

For large joins or long windows, arrangements can spill to disk. See [Disk-Backed Arrangements](../architecture/performance.md#disk-backed-arrangements) for details on the `--arrangement-mem-limit` flag.

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

The subquery supports the full FoldDB SQL dialect (WHERE, GROUP BY, HAVING, LIMIT, nested JOINs). Kafka sources inside subqueries are not supported.

## Limitations

- **Only simple equi-joins.** Composite keys (multiple conditions with AND) are not supported in v0.
- **Stream-to-stream requires WITHIN.** FoldDB errors at parse time if `WITHIN INTERVAL` is missing for a stream-stream join.
