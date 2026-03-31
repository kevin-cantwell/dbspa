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

Retractions and insertions for the same group key are always emitted as adjacent pairs: the `"-"` immediately precedes the `"+"`. No other key's diffs interleave between them.

## How to consume a changelog

A downstream consumer maintaining a map of `GROUP BY keys -> latest "+" value` always has the current result set:

```python
state = {}
for line in changelog:
    record = json.loads(line)
    key = record["region"]  # the GROUP BY key
    if record["op"] == "+":
        state[key] = record
    elif record["op"] == "-":
        del state[key]
# state is always the current aggregation result
```

## Non-accumulating queries

For non-accumulating queries (no `GROUP BY`), every line is a plain insertion. The `"op"` field is omitted for brevity — the output is plain NDJSON:

```json
{"name":"alice","age":30}
{"name":"carol","age":35}
```

## Final snapshot at EOF

For bounded inputs (stdin from a file, `--input`), when the input ends, FoldDB emits a sorted final snapshot after the streaming changelog. The final snapshot contains one `"+"` line per group key, sorted by ORDER BY (or group key order if no ORDER BY is specified).

During streaming, changelog diffs are emitted in arrival order. The final snapshot gives you a clean, deterministic view of the end state.

## When changelog mode is used

FoldDB auto-detects the output mode:

| Context | Accumulating query | Non-accumulating query |
|---|---|---|
| TTY (terminal) | TUI (live table) | Scrolling NDJSON |
| Piped / non-TTY | Changelog NDJSON | Plain NDJSON |
| `--state file.db` | SQLite UPSERT | SQLite INSERT |

Override auto-detection with `--mode changelog`, `--mode tui`, or `--mode json`.

## Relationship to the diff model

The changelog is the external representation of FoldDB's internal [diff model](diff-model.md). Internally, every record carries a `diff` field (`+1` or `-1`). The changelog maps `+1` to `"op":"+"` and `-1` to `"op":"-"`.

The [TUI mode](../reference/cli.md) consumes the same diff stream but renders it as a live-updating table — retractions are invisible because the row updates in place.
