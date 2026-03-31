# Joins

FoldDB supports **stream-to-table** joins: enriching streaming events with reference data loaded from files.

## How it works

The table side (right side of the JOIN) is loaded entirely into an in-memory hash map, indexed by the join key. For each stream record, the hash map is probed to produce joined output.

```sql
SELECT e.user_id, u.name, u.email, e.action
FROM stdin e
JOIN '/data/users.parquet' u ON e.user_id = u.id
```

1. FoldDB loads `users.parquet` into a hash map keyed by `u.id`.
2. For each record from stdin, it extracts `e.user_id` and looks up matching rows.
3. Matched records are merged (stream columns + table columns) and flow to the next pipeline stage.

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

## Table source formats

The table file can be any format FoldDB supports:

- Parquet (`.parquet`)
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

Joins handle [diffs](diff-model.md) correctly. If the stream source produces retractions (e.g., from Debezium CDC), a retraction flowing through the join triggers a retraction of the joined result. The diff is preserved through the join.

## Limitations

- **Stream-to-stream joins** are not supported. Both sides cannot be unbounded sources.
- **The table side must fit in memory.** The entire file is loaded into a hash map.
- **Only simple equi-joins.** Composite keys (multiple conditions with AND) are not supported in v0.
- **The table is static.** It is loaded once at startup and not refreshed. For mutable lookup data, consider periodically restarting the query.
