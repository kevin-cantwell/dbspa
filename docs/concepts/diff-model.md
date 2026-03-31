# The Diff Model

This is the most important concept in FoldDB. Everything else follows from it.

## Every record carries a diff

Inside FoldDB, every record has three parts:

| Part | Description |
|---|---|
| **Columns** | The actual data: `{"status": "pending", "amount": 100}` |
| **Timestamp** | When the event happened (event time) or was received (processing time) |
| **Diff** | `+1` (insertion) or `-1` (retraction) |

For most data sources (plain JSON from Kafka, stdin, files), every record is an insertion (`diff = +1`). You never think about diffs — they're always positive.

The diff field matters when you use **Debezium CDC** or when the **accumulator emits updates**.

## Where retractions come from

### Source: Debezium CDC

Debezium watches a database's transaction log and publishes changes to Kafka. Each change has an `op` field:

| Debezium `op` | What happened | FoldDB records emitted |
|---|---|---|
| `c` (create) | Row inserted | 1 record: `(after, diff=+1)` |
| `u` (update) | Row changed | 2 records: `(before, diff=-1)` then `(after, diff=+1)` |
| `d` (delete) | Row deleted | 1 record: `(before, diff=-1)` |
| `r` (read) | Snapshot | 1 record: `(after, diff=+1)` |

An **update** emits two records because the aggregator needs to:

1. **Subtract** the old value's contribution (retraction)
2. **Add** the new value's contribution (insertion)

Without this, an order changing from `pending` to `shipped` would be counted in **both** groups instead of moving from one to the other.

### Source: The accumulator itself

Even without CDC, the accumulator emits retractions. When a `GROUP BY` query processes a new input record, and it changes a group's result, the accumulator emits:

1. A **retraction** of the previous result: `{"op":"-", "status":"pending", "count":99}`
2. An **insertion** of the new result: `{"op":"+", "status":"pending", "count":100}`

This is the **changelog protocol**. It tells downstream consumers exactly what changed.

```
Input records (all diff=+1):          Changelog output:
  {"status":"pending"}          →     {"op":"+","status":"pending","count":1}
  {"status":"shipped"}          →     {"op":"+","status":"shipped","count":1}
  {"status":"pending"}          →     {"op":"-","status":"pending","count":1}
                                      {"op":"+","status":"pending","count":2}
```

## Why this matters

### Correct aggregation over mutable data

Traditional stream processors (Kafka Streams, Flink) handle this with "retract mode" as a special case. In FoldDB, it's the default — **every operator handles diffs natively**.

Consider: "How many orders are in each status right now?"

With insert-only counting, an order that moves from `pending` → `shipped` would be counted in both. With diffs:

```
Event: order 42 created as pending     → pending: +1 = 1
Event: order 42 updated to shipped     → pending: -1 = 0, shipped: +1 = 1
Event: order 42 updated to delivered   → shipped: -1 = 0, delivered: +1 = 1
```

The counts are always correct because updates carry both the removal and addition.

### Efficient sliding windows

A 7-day sliding window doesn't need to re-scan 7 days of data when the window slides. Records entering the window are `+1`, records leaving are `-1`. The accumulator processes both identically.

### Composable operators

Every operator (filter, project, aggregate, join) handles diffs. A retraction flowing through a filter is passed through if the record would have matched. A retraction flowing through a join triggers a retraction of the joined result. The system is compositional.

## Output modes

The diff model surfaces differently depending on the output mode:

| Output mode | How diffs appear |
|---|---|
| **TUI** (terminal) | You see a live-updating table. Retractions are invisible — the row just updates in place. |
| **Changelog NDJSON** (piped) | Every line has `"op":"+"` or `"op":"-"`. Retraction+insertion pairs are always adjacent. |
| **SQLite** (`--state file.db`) | The `result` table is UPSERTed. You always see the current state. |
| **HTTP** (`folddb serve`) | `GET /` returns current state. `GET /stream` returns SSE changelog diffs. |

## Accumulator state cost

The diff model is correct for **all** aggregate functions, but some require more state:

| Aggregate | State per group key | How retractions work |
|---|---|---|
| `COUNT(*)`, `SUM`, `AVG` | O(1) — a few numbers | Subtract the retracted value |
| `MIN`, `MAX` | O(n) — all values in a heap | Remove from heap, recompute |
| `MEDIAN`, `PERCENTILE` | O(n) — all values sorted | Remove from sorted set, recompute |
| `COUNT(DISTINCT)` | O(n) — set of distinct values | Remove from set |

`COUNT(*)` with 10M groups uses ~80MB. `MEDIAN` with 10M groups and 100 values each uses ~8GB. Choose your aggregates accordingly.
