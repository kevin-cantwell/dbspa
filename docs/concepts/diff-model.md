# The Z-Set Model

This is the most important concept in DBSPA. Everything else follows from it.

## What is a Z-set?

A Z-set is a **multiset with integer weights**. It comes from DBSP (Database Stream Processing), the mathematical framework behind [Feldera](https://github.com/feldera/feldera). The key paper is Budiu et al., "DBSP: Automatic Incremental View Maintenance" (VLDB 2023).

In a Z-set, every element has an integer **weight**:

- Weight `+1` means "one copy inserted"
- Weight `-1` means "one copy retracted"
- Weight `+N` means "N identical copies inserted" (multiset semantics)
- Weight `-N` means "N copies retracted"
- Weight `0` means the element has been fully cancelled out

Inside DBSPA, every record flowing through the pipeline is a Z-set entry:

| Part | Description |
|---|---|
| **Columns** | The actual data: `{"status": "pending", "amount": 100}` |
| **Timestamp** | When the event happened (event time) or was received (processing time) |
| **Weight** | An integer multiplicity: `+1`, `-1`, `+5`, `-3`, etc. |

For most data sources (plain JSON from Kafka, stdin, files), every record has `weight = +1`. You never think about weights -- they're always positive.

The weight field matters when you use **Debezium CDC**, when the **accumulator emits updates**, or when **batch compaction** combines multiple records.

## Why Z-sets?

DBSPA uses Z-sets with full integer weights rather than a simple `+1`/`-1` boolean. This unlocks several capabilities:

**Compositional incrementalization.** Every relational operator (filter, project, join, aggregate) has a provably correct incremental version when applied to Z-set deltas. This means you can compose operators freely and the result is always correct -- you don't need to special-case retraction handling per operator.

**Batch compaction.** When multiple records share the same group key, their weights can be summed before reaching the accumulator. If a key is inserted 5 times and retracted 3 times in a single batch, the accumulator sees a single entry with weight `+2` instead of 8 separate operations.

**Formal correctness.** The DBSP framework provides mathematical proofs that incremental evaluation over Z-sets produces the same results as full re-evaluation. This eliminates an entire class of bugs around retractions in complex queries (HAVING with retractions, multi-way joins, nested aggregation).

**Streaming-batch unification.** Z-sets are the foundation that bridges streaming and batch processing. A batch of changes is just a Z-set; a stream is a sequence of Z-sets over time.

## Where retractions come from

### Source: Debezium CDC

Debezium watches a database's transaction log and publishes changes to Kafka. Each change has an `op` field:

| Debezium `op` | What happened | DBSPA records emitted |
|---|---|---|
| `c` (create) | Row inserted | 1 record: `(after, weight=+1)` |
| `u` (update) | Row changed | 2 records: `(before, weight=-1)` then `(after, weight=+1)` |
| `d` (delete) | Row deleted | 1 record: `(before, weight=-1)` |
| `r` (read) | Snapshot | 1 record: `(after, weight=+1)` |

An **update** emits two records because the aggregator needs to:

1. **Subtract** the old value's contribution (retraction)
2. **Add** the new value's contribution (insertion)

Without this, an order changing from `pending` to `shipped` would be counted in **both** groups instead of moving from one to the other.

### Source: The accumulator itself

Even without CDC, the accumulator emits retractions. When a `GROUP BY` query processes a new input record, and it changes a group's result, the accumulator emits:

1. A **retraction** of the previous result: `{"weight":-1, "data":{"status":"pending", "count":99}}`
2. An **insertion** of the new result: `{"weight":1, "data":{"status":"pending", "count":100}}`

This is the **changelog protocol**. It tells downstream consumers exactly what changed.

```
Input records (all weight=+1):         Changelog output:
  {"status":"pending"}          ->     {"weight":1,"data":{"status":"pending","count":1}}
  {"status":"shipped"}          ->     {"weight":1,"data":{"status":"shipped","count":1}}
  {"status":"pending"}          ->     {"weight":-1,"data":{"status":"pending","count":1}}
                                       {"weight":1,"data":{"status":"pending","count":2}}
```

### Source: Batch compaction

When records are batched (see [pipeline](../architecture/pipeline.md)), multiple insertions and retractions of the same key can be compacted into a single Z-set entry with a summed weight. For example, if a batch contains 5 insertions and 2 retractions for the same group key, the compacted entry has `weight = +3`.

## Why this matters

### Correct aggregation over mutable data

Traditional stream processors (Kafka Streams, Flink) handle retractions with "retract mode" as a special case. In DBSPA, it's the default -- **every operator processes weighted records natively**.

Consider: "How many orders are in each status right now?"

With insert-only counting, an order that moves from `pending` to `shipped` would be counted in both. With Z-set weights:

```
Event: order 42 created as pending     -> pending: +1 = 1
Event: order 42 updated to shipped     -> pending: -1 = 0, shipped: +1 = 1
Event: order 42 updated to delivered   -> shipped: -1 = 0, delivered: +1 = 1
```

The counts are always correct because updates carry both the removal and addition.

### Efficient sliding windows

A 7-day sliding window doesn't need to re-scan 7 days of data when the window slides. Each record is assigned to all overlapping windows at ingestion time — for a 7-day window with a 1-day slide, each record is stamped into 7 buckets simultaneously. The accumulator for each bucket holds only summary state (counts, sums, etc.), not the original records. When a window closes, its accumulated result is emitted and the state is discarded.

### Composable operators

Every operator (filter, project, aggregate, join) handles weights. A retraction flowing through a filter is passed through if the record would have matched. A retraction flowing through a join triggers a retraction of the joined result. The system is compositional -- this is the core guarantee of the DBSP framework.

### Multiset semantics

Because weights are full integers (not just +1/-1), DBSPA naturally supports multiset semantics. A weight of `+3` means "three identical copies of this row." This arises naturally from batch compaction and from certain join patterns. Accumulators handle this correctly: `Add` is called with the weight, so `COUNT(*)` increments by the weight value, `SUM` adds `value * weight`, etc.

## Output modes

The Z-set model surfaces differently depending on the output mode:

| Output mode | How weights appear |
|---|---|
| **TUI** (terminal) | You see a live-updating table. Retractions are invisible -- the row just updates in place. |
| **Changelog NDJSON** (piped) | Every line uses the Feldera weighted format: `{"weight": N, "data": {...}}`. Retraction+insertion pairs are always adjacent. |
| **SQLite** (`--state file.db`) | The `result` table is UPSERTed. You always see the current state. |
| **HTTP** (`dbspa serve`) | `GET /` returns current state. `GET /stream` returns SSE changelog entries. |

## Accumulator state cost

The Z-set model is correct for **all** aggregate functions, but some require more state:

| Aggregate | State per group key | How retractions work |
|---|---|---|
| `COUNT(*)`, `SUM`, `AVG` | O(1) -- a few numbers | Subtract the retracted value (scaled by weight) |
| `MIN`, `MAX` | O(n) -- all values in a heap | Remove from heap, recompute |
| `MEDIAN`, `PERCENTILE` | O(n) -- all values sorted | Remove from sorted set, recompute |
| `COUNT(DISTINCT)` | O(n) -- set of distinct values | Remove from set |

`COUNT(*)` with 10M groups uses ~80MB. `MEDIAN` with 10M groups and 100 values each uses ~8GB. Choose your aggregates accordingly.

## Reference

Budiu, M., Chajed, T., McSherry, F., Ryzhyk, L., & Tannen, V. (2023). "DBSP: Automatic Incremental View Maintenance." Proceedings of the VLDB Endowment, 16(7). Z-sets are multisets with integer weights; every relational operator has a provably correct incremental version over Z-set deltas.
