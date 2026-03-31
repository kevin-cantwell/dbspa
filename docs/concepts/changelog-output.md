# Changelog Output

When an accumulating query (`GROUP BY`) is piped to another process or written to a file, FoldDB emits **Z-set deltas** as NDJSON — a stream of weighted records that describe how the result set changes over time.

## Format

Each line is a JSON object with a `"_weight"` field — the Z-set multiplicity:

```json
{"_weight":1,"region":"us-east","orders":1}
{"_weight":1,"region":"us-west","orders":1}
{"_weight":-1,"region":"us-east","orders":1}
{"_weight":1,"region":"us-east","orders":2}
```

| `_weight` | Meaning |
|---|---|
| Positive (e.g., `1`) | Insertion — this row is added to the result set |
| Negative (e.g., `-1`) | Retraction — this row is removed from the result set |

This is the DBSP Z-set delta model. The output stream is a sequence of changes to the result collection, not the result collection itself.

## Retraction+insertion pairs

When a group's aggregate result changes (e.g., `COUNT` goes from 1 to 2), the accumulator emits a retraction of the old row followed by an insertion of the new row. These pairs are always adjacent — no other key's deltas interleave between them.

```json
{"_weight":-1,"region":"us-east","orders":1}   <- old row retracted
{"_weight":1,"region":"us-east","orders":2}    <- new row inserted
```

!!! note "Not every retraction has a matching insertion"
    A retraction without a following insertion means the group was deleted — for example, when all rows contributing to a CDC-based count are retracted, or when a group falls below a `HAVING` threshold.

!!! note "First insertion has no preceding retraction"
    When a group key appears for the first time, only an insertion is emitted.

## Weights beyond +1/-1

With [batch compaction](../architecture/pipeline.md), the `_weight` field can be any integer — not just +1 and -1. For example, if 5 insertions and 2 retractions for the same record are compacted in a single batch, the accumulator receives a single entry with `_weight: 3`.

The output weights from the accumulator are always +1 or -1 (since each group key has exactly one current result row). But the internal pipeline may process higher weights.

## How to consume a changelog

A downstream consumer maintaining a map of `GROUP BY keys -> latest positive-weight row` always has the current result set:

```python
state = {}
for line in changelog:
    record = json.loads(line)
    key = record["region"]  # the GROUP BY key(s)
    if record["_weight"] > 0:
        state[key] = record
    elif record["_weight"] < 0:
        if key in state:
            del state[key]
# state is always the current aggregation result
```

## Non-accumulating queries

For non-accumulating queries (no `GROUP BY`), every line is a plain insertion. The `"_weight"` field is omitted for brevity — the output is plain NDJSON:

```json
{"name":"alice","age":30}
{"name":"carol","age":35}
```

## Final snapshot at EOF

For bounded inputs (stdin from a file, `--input`), when the input ends and `ORDER BY` is specified, FoldDB emits a sorted final snapshot after the streaming deltas. The final snapshot contains one positive-weight line per group key, sorted by the ORDER BY clause.

During streaming, deltas are emitted in arrival order (not sorted). The final snapshot gives you a clean, deterministic view of the end state.

## When changelog mode is used

FoldDB auto-detects the output mode:

| Context | Accumulating query | Non-accumulating query |
|---|---|---|
| TTY (terminal) | TUI (live table) | Scrolling NDJSON |
| Piped / non-TTY | Changelog NDJSON with `_weight` | Plain NDJSON |
| `--state file.db` | SQLite UPSERT | SQLite INSERT |

## Relationship to the Z-set model

The changelog is the external representation of FoldDB's internal [Z-set model](diff-model.md). Internally, every record carries a `Weight` field — a Z-set multiplicity. The changelog serializes this directly as the `_weight` JSON field.

The [TUI mode](../reference/cli.md) consumes the same Z-set delta stream internally but renders it as a live-updating table — retractions are invisible because the row updates in place.
