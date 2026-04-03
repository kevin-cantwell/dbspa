# Accumulators

Accumulators are the core of DBSPA's aggregation engine. Every aggregate function (`COUNT`, `SUM`, `AVG`, etc.) is implemented as an `Accumulator` that processes [Z-set](../concepts/diff-model.md) entries -- records with integer weights representing insertions and retractions.

## Interface

```go
type Accumulator interface {
    Add(value Value)         // called for positive weight (insertion)
    Retract(value Value)     // called for negative weight (retraction)
    Result() Value           // current aggregate value
    HasChanged() bool        // did Result() change after the last Add/Retract?
    ResetChanged()           // clear the changed flag
    CanMerge() bool          // supports Merge()?
    Merge(other Accumulator) // combine partial state from another instance
    Marshal() ([]byte, error)
    Unmarshal([]byte) error
}
```

The key insight: **Add and Retract are symmetric.** Every accumulator handles both. This is what makes the [Z-set model](../concepts/diff-model.md) work -- you don't need special retraction handling per operator.

## Z-set weight semantics

Accumulators process Z-set entries where the `Weight` field determines how many times `Add` or `Retract` is called:

- **Weight = +1**: Call `Add` once (standard insertion).
- **Weight = -1**: Call `Retract` once (standard retraction).
- **Weight = +N** (N > 1): Call `Add` N times. This is multiset semantics -- "add this value N times." This arises from batch compaction when multiple identical insertions are summed.
- **Weight = -N** (N > 1): Call `Retract` N times. Multiple retractions of the same value.
- **Weight = 0**: Skip entirely (the insertion and retraction cancelled out during compaction).

## Implementations

| Accumulator | State | Backing structure | Retraction behavior |
|---|---|---|---|
| `CountStarAccumulator` | O(1) — 1 integer | counter | `+1` / `-1`, clamped at 0 |
| `CountAccumulator` | O(1) — 1 integer | counter (skips NULLs) | `+1` / `-1`, clamped at 0 |
| `SumAccumulator` | O(1) — 1 number | running total | add / subtract |
| `AvgAccumulator` | O(1) — 2 numbers | sum + count | derived from sum and count |
| `MinAccumulator` | O(n) — all values | min-heap | remove from heap, O(log n) |
| `MaxAccumulator` | O(n) — all values | max-heap | remove from heap, O(log n) |
| `MedianAccumulator` | O(n) — all values | dual-heap | maintains two heaps split at median |
| `PercentileAccumulator` | O(n) — all values | sorted set | recomputes on change |
| `CountDistinctAccumulator` | O(n) — distinct set | hash set | remove from set |
| `ArrayAggAccumulator` | O(n) — all values | list | remove first matching value |
| `FirstAccumulator` | O(1) — single value | frozen on first `Add` | retractions ignored |
| `LastAccumulator` | O(1) — single value | updates on each `Add` | retractions ignored |

## State cost

State cost determines memory usage and checkpoint size:

- **O(1) aggregates** (`COUNT`, `SUM`, `AVG`): a few numbers per group key. 10M groups uses ~80MB.
- **O(n) aggregates** (`MIN`, `MAX`, `MEDIAN`, `PERCENTILE`, `COUNT(DISTINCT)`, `ARRAY_AGG`): all values retained per group key. 10M groups with 100 values each uses ~8GB.

DBSPA logs a warning when O(n) aggregates exceed a configurable memory threshold.

## NULL handling

All aggregates skip NULL input values (SQL standard):

- `COUNT(*)` counts all rows including NULLs.
- `COUNT(x)` counts only non-NULL values.
- If all inputs are NULL, aggregates return NULL — except `COUNT(*)` and `COUNT(x)` which return 0.
- `FIRST(x)` returns the first non-NULL value. `LAST(x)` returns the most recent non-NULL value.
- `ARRAY_AGG(x)` includes NULLs.

## Emission logic

The accumulator is a hash map: `composite_key -> []Accumulator`. After each `Add` or `Retract`, the pipeline checks `HasChanged()`. If true:

1. Emit a retraction (`op: "-"`) of the previous result for this group key.
2. Emit an insertion (`op: "+"`) of the new result.

This is uniform across all aggregates. The only difference is internal state cost.

## Merge support

Some accumulators support `Merge()` for combining partial state:

| Mergeable | Not mergeable |
|---|---|
| `COUNT`, `SUM`, `MIN`, `MAX`, HLL | `MEDIAN`, `PERCENTILE` |

Merge is used when session windows combine: two session accumulators are merged into one.

## Seeding (SetInitial)

When a query includes `SEED FROM`, each accumulator's initial state is set via `SetInitial(value)` before streaming begins. This bypasses the normal `Add`/`Retract` path -- the seed value is treated as the pre-computed result, not as a raw input.

| Accumulator | SetInitial behavior |
|---|---|
| `CountStarAccumulator` / `CountAccumulator` | Sets counter to seed value |
| `SumAccumulator` | Sets running total to seed value |
| `MinAccumulator` | Sets current minimum to seed value |
| `MaxAccumulator` | Sets current maximum to seed value |
| `AvgAccumulator` | Not supported -- cannot decompose into sum + count |
| `FirstAccumulator` | Not supported -- depends on arrival order |
| `LastAccumulator` | Not supported -- depends on arrival order |

For unsupported accumulators, attempting to seed logs a warning and leaves the accumulator at its zero value. See [SEED FROM](pipeline.md#seed-from) for the full pipeline details.

## Serialization

Every accumulator implements `Marshal()` / `Unmarshal()` for [checkpointing](checkpointing.md). The serialized form is JSON. On restore, accumulator state is deserialized and the pipeline resumes as if it never stopped.
