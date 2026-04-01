# Streams vs Tables

FoldDB is **schema-on-read**. There is no pre-declared schema — everything is inferred from the data at runtime. The "what type is it?" question is answered when the record arrives, not when the table is created. This applies equally to top-level columns and nested JSON fields.

FoldDB queries operate on two fundamentally different kinds of data: **streams** and **tables**. Understanding the distinction is key to writing correct queries.

## Streams

A stream is an unbounded, append-only sequence of records. Each record arrives once and flows through the pipeline.

Sources that produce streams:

| Source | Notes |
|---|---|
| stdin | Lines arrive until EOF or Ctrl-C |
| Kafka topic | Records arrive continuously |
| Kafka with Debezium | CDC events arrive as inserts, updates, deletes |

Streams are the primary input to FoldDB queries. Every record in a stream carries a [diff](diff-model.md) (`+1` or `-1`).

## Tables

A table is a bounded, finite set of records. Tables are loaded entirely into memory.

Sources that produce tables:

| Source | Notes |
|---|---|
| Parquet file | Loaded via `--input` or `JOIN` |
| NDJSON file | Loaded via `--input` or `JOIN` |
| CSV file | Loaded via `--input` or `JOIN` |

Tables appear in two contexts:

1. **As the primary source** — via `--input file.parquet`. The file is read to completion, the query runs, and FoldDB exits.
2. **As the right side of a JOIN** — via `JOIN '/path/to/file' alias ON ...`. The table is loaded into a hash map and probed for each stream record.

## The distinction matters

### Non-accumulating queries

For a **stream** source, a non-accumulating query (`SELECT ... WHERE ...` without `GROUP BY`) runs indefinitely until the source ends or LIMIT is reached. Records flow through one at a time.

For a **table** source, the same query runs to completion and exits.

### Accumulating queries

For a **stream** source, a `GROUP BY` query produces a continuously updating result. In the terminal, you see a live table. When piped, you see a [changelog](changelog-output.md) of diffs.

For a **table** source, a `GROUP BY` query computes the final result and exits. The changelog contains only the final state.

### Joins

FoldDB supports **stream-to-table** joins only. The stream is the left (driving) side. The table is the right (lookup) side, loaded into memory as a hash map.

```sql
-- Stream (stdin) joined with table (Parquet file)
SELECT e.user_id, u.name, e.action
FROM stdin e
JOIN '/data/users.parquet' u ON e.user_id = u.id
```

Stream-to-stream joins are not supported. See [Joins](joins.md) for details.

## Debezium: a stream that behaves like a table

Debezium CDC streams are special. They carry the full change history of a database table. When consumed from the beginning (`offset=earliest`), the stream reconstructs the table state through a sequence of inserts, updates, and deletes.

FoldDB's [diff model](diff-model.md) handles this natively. An update emits a retraction of the old value and an insertion of the new value. Aggregations over a Debezium stream always reflect the **current** state of the source table, not a cumulative count of all changes.
