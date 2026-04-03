# Changelog Output

When an accumulating query (`GROUP BY`) is piped to another process or written to a file, DBSPA emits **Z-set deltas** as NDJSON — a stream of weighted records that describe how the result set changes over time.

## Format

Each line is a JSON object with a `"weight"` field (the Z-set multiplicity) and a `"data"` object containing the columns:

```json
{"weight":1,"data":{"region":"us-east","orders":1}}
{"weight":1,"data":{"region":"us-west","orders":1}}
{"weight":-1,"data":{"region":"us-east","orders":1}}
{"weight":1,"data":{"region":"us-east","orders":2}}
```

| `weight` | Meaning |
|---|---|
| Positive (e.g., `1`) | Insertion — this row is added to the result set |
| Negative (e.g., `-1`) | Retraction — this row is removed from the result set |

This is the DBSP Z-set delta model. The output stream is a sequence of changes to the result collection, not the result collection itself.

## Retraction+insertion pairs

When a group's aggregate result changes (e.g., `COUNT` goes from 1 to 2), the accumulator emits a retraction of the old row followed by an insertion of the new row. These pairs are always adjacent — no other key's deltas interleave between them.

```json
{"weight":-1,"data":{"region":"us-east","orders":1}}   <- old row retracted
{"weight":1,"data":{"region":"us-east","orders":2}}    <- new row inserted
```

!!! note "Not every retraction has a matching insertion"
    A retraction without a following insertion means the group was deleted — for example, when all rows contributing to a CDC-based count are retracted, or when a group falls below a `HAVING` threshold.

!!! note "First insertion has no preceding retraction"
    When a group key appears for the first time, only an insertion is emitted.

## Weights beyond +1/-1

With [batch compaction](../architecture/pipeline.md), the `weight` field can be any integer — not just +1 and -1. For example, if 5 insertions and 2 retractions for the same record are compacted in a single batch, the accumulator receives a single entry with `weight: 3`.

The output weights from the accumulator are always +1 or -1 (since each group key has exactly one current result row). But the internal pipeline may process higher weights.

## How to consume a changelog

A downstream consumer maintaining a map of `GROUP BY keys -> latest positive-weight row` always has the current result set:

```python
state = {}
for line in changelog:
    record = json.loads(line)
    key = record["data"]["region"]  # access data inside envelope
    if record["weight"] > 0:
        state[key] = record["data"]
    elif record["weight"] < 0:
        if key in state:
            del state[key]
# state is always the current aggregation result
```

## Consuming a changelog with FORMAT DBSPA

DBSPA's changelog output can be consumed by another DBSPA instance using the `FORMAT DBSPA` envelope. This enables composing pipelines: one instance produces a changelog, another applies further transformations while preserving the Z-set semantics.

```bash
# Instance 1: produce a changelog of order counts by status
dbspa "SELECT status, COUNT(*) FROM 'kafka://broker/orders' GROUP BY status" | \

# Instance 2: consume the changelog, apply further filtering
dbspa "SELECT * FROM stdin FORMAT DBSPA WHERE status = 'pending'"
```

The `FORMAT DBSPA` envelope reads the `weight` field from the Feldera weighted format and unwraps the `data` object, instead of treating every record as an insert. This means retractions flow through correctly -- when Instance 1 retracts an old count and inserts a new one, Instance 2 sees both the retraction and insertion, keeping its own state consistent.

Without `FORMAT DBSPA`, the downstream instance would treat every line (including retractions) as a plain insert with weight=+1, which would produce incorrect results.

See [FORMAT DBSPA](../reference/formats.md#dbspa-changelog) for the full envelope reference.

## Non-accumulating queries

For non-accumulating queries (no `GROUP BY`), every line is a plain insertion. The weight envelope is omitted for brevity — the output is plain NDJSON:

```json
{"name":"alice","age":30}
{"name":"carol","age":35}
```

## Final snapshot at EOF

For bounded inputs (stdin from a file, `--input`), when the input ends and `ORDER BY` is specified, DBSPA emits a sorted final snapshot after the streaming deltas. The final snapshot contains one positive-weight line per group key, sorted by the ORDER BY clause.

During streaming, deltas are emitted in arrival order (not sorted). The final snapshot gives you a clean, deterministic view of the end state.

## When changelog mode is used

DBSPA auto-detects the output mode:

| Context | Accumulating query | Non-accumulating query |
|---|---|---|
| TTY (terminal) | TUI (live table) | Scrolling NDJSON |
| Piped / non-TTY | Changelog NDJSON (Feldera weighted format) | Plain NDJSON |
| `--state file.db` | SQLite UPSERT | SQLite INSERT |

## Relationship to the Z-set model

The changelog is the external representation of DBSPA's internal [Z-set model](diff-model.md). Internally, every record carries a `Weight` field — a Z-set multiplicity. The changelog serializes this using the Feldera weighted format: `{"weight": N, "data": {...}}`. This aligns with [Feldera's](https://github.com/feldera/feldera) "weighted" input format for interoperability.

The [TUI mode](../reference/cli.md) consumes the same Z-set delta stream internally but renders it as a live-updating table — retractions are invisible because the row updates in place.
