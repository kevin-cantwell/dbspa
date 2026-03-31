# Changelog Output

When an accumulating query (`GROUP BY`) is piped to another process or written to a file, FoldDB emits **changelog NDJSON** — a stream of diffs that describe how the result set changes over time.

## Format

Each line is a JSON object with an `"op"` field:

```json
{"op":"+","region":"us-east","orders":1}
{"op":"+","region":"us-west","orders":1}
{"op":"-","region":"us-east","orders":1}
{"op":"+","region":"us-east","orders":2}
```

| `op` | Meaning |
|---|---|
| `"+"` | Insertion — a new or updated result for this group key |
| `"-"` | Retraction — the previous result for this group key is no longer valid |

## Retraction+insertion pairs

When a group's aggregate result changes (e.g., `COUNT` goes from 1 to 2), the accumulator emits a retraction of the old value followed by an insertion of the new value. These pairs are always adjacent — no other key's diffs interleave between them.

```json
{"op":"-","region":"us-east","orders":1}   ← old value retracted
{"op":"+","region":"us-east","orders":2}   ← new value inserted
```

!!! note "Not every retraction has a matching insertion"
    A retraction without a following insertion means the group was deleted — for example, when all rows contributing to a CDC-based count are retracted, or when a group falls below a `HAVING` threshold.

!!! note "First insertion has no preceding retraction"
    When a group key appears for the first time, only an insertion is emitted. There's nothing to retract.

## How to consume a changelog

A downstream consumer maintaining a map of `GROUP BY keys → latest "+" value` always has the current result set:

```python
state = {}
for line in changelog:
    record = json.loads(line)
    key = record["region"]  # the GROUP BY key(s)
    if record["op"] == "+":
        state[key] = record
    elif record["op"] == "-":
        if key in state:
            del state[key]
# state is always the current aggregation result
```

This works regardless of how many updates a key has had — you only keep the latest `"+"`.

## Non-accumulating queries

For non-accumulating queries (no `GROUP BY`), every line is a plain insertion. The `"op"` field is omitted for brevity — the output is plain NDJSON:

```json
{"name":"alice","age":30}
{"name":"carol","age":35}
```

## Final snapshot at EOF

For bounded inputs (stdin from a file, `--input`), when the input ends and `ORDER BY` is specified, FoldDB emits a sorted final snapshot after the streaming changelog. The final snapshot contains one `"+"` line per group key, sorted by the ORDER BY clause.

During streaming, changelog diffs are emitted in arrival order (not sorted). The final snapshot gives you a clean, deterministic view of the end state.

If no ORDER BY is specified, no final snapshot is emitted — only the streaming diffs.

## When changelog mode is used

FoldDB auto-detects the output mode:

| Context | Accumulating query | Non-accumulating query |
|---|---|---|
| TTY (terminal) | TUI (live table) | Scrolling NDJSON |
| Piped / non-TTY | Changelog NDJSON | Plain NDJSON |
| `--state file.db` | SQLite UPSERT | SQLite INSERT |

Override auto-detection with `--mode changelog`, `--mode tui`, or `--mode json`.

## Relationship to the Z-set model

The changelog is the external representation of FoldDB's internal [Z-set model](diff-model.md). Internally, every record carries a `Weight` field — a Z-set multiplicity that can be any integer. The changelog maps positive weights to `"op":"+"` and negative weights to `"op":"-"`.

With [batch compaction](../architecture/pipeline.md), multiple insertions and retractions for the same record can be combined into a single Z-set entry with a summed weight before reaching the accumulator. For example, 5 insertions and 2 retractions become a single entry with weight `+3`. The accumulator processes this as 3 additions, and the changelog output reflects the resulting aggregate change.

The [TUI mode](../reference/cli.md) consumes the same stream internally but renders it as a live-updating table — retractions are invisible because the row updates in place.
