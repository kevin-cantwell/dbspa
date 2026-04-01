# Performance

All benchmarks run on Apple M4, 100K records, single binary, no external services.

## Format Comparison

### Passthrough (SELECT *)

| Format | Time | Records/sec |
|---|---|---|
| NDJSON (stdin) | 362ms | 276K |
| Avro OCF (stdin) | 414ms | 242K |
| Protobuf (stdin) | 513ms | 195K |
| **Parquet (DuckDB)** | **136ms** | **735K** |

### Filter (WHERE status = 'confirmed')

| Format | Time | Records/sec |
|---|---|---|
| NDJSON | 361ms | 277K |
| Avro OCF | 413ms | 242K |
| Protobuf | 513ms | 195K |
| **Parquet (DuckDB)** | **51ms** | **1,961K** |

Parquet via DuckDB achieves nearly 2M records/sec on filters due to predicate pushdown — DuckDB skips row groups that don't match using column statistics.

### GROUP BY (product, region, COUNT, SUM)

| Format | Time | Records/sec |
|---|---|---|
| NDJSON | 871ms | 115K |
| Avro OCF | 922ms | 108K |
| Parquet (DuckDB scan, FoldDB agg) | 904ms | 111K |

GROUP BY throughput is dominated by accumulation, not decoding — all formats converge around 110K records/sec.

## CDC (Debezium) Performance

### Decode + GROUP BY

| Format | Time | Records/sec |
|---|---|---|
| JSON Debezium | 19ms | 5,263K |
| Avro Debezium | 20ms | 5,000K |

!!! note
    These numbers are high because the WHERE filter rejects ~40% of records early, and the GROUP BY has only 5 groups. The decode cost is amortized across few output records.

### Full Complex Query

The most demanding query FoldDB supports — exercises every major feature:

```sql
SELECT c.tier, c.region,
       COUNT(*), SUM(total), AVG(total), MIN(total), MAX(total),
       SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END),
       SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END)
FROM stdin e
JOIN '/path/to/customers.parquet' c ON e.customer_id = c.id
FORMAT DEBEZIUM
WHERE _op IN ('c', 'u')
GROUP BY c.tier, c.region
```

Features used: Debezium CDC retractions, DD join against DuckDB-loaded Parquet (5K rows), type casts, CASE expressions, 7 aggregates, Z-set weight propagation.

| Format | Time | Records/sec |
|---|---|---|
| JSON Debezium | 988ms | **101K** |
| **Avro Debezium** | **875ms** | **114K** |

### Wire Size

| Format | 100K CDC events |
|---|---|
| JSON Debezium | 33 MB |
| **Avro Debezium** | **14 MB** (2.4x smaller) |

For Kafka deployments, Avro reduces broker storage and network bandwidth by 2.4x.

## FoldDB vs DuckDB

DuckDB is a vectorized columnar engine optimized for batch analytics. FoldDB is a streaming engine with incremental aggregation. They serve different use cases.

### Parquet GROUP BY (100K records)

| Engine | Time | Records/sec |
|---|---|---|
| **DuckDB (native)** | **17ms** | **5,882K** |
| FoldDB (via DuckDB scan) | 936ms | 107K |

DuckDB is ~55x faster on pure batch queries due to SIMD vectorized execution across all cores. FoldDB processes records through a goroutine pipeline optimized for streaming.

!!! tip "When to use which"
    **DuckDB:** One-shot queries on files. Batch analytics. Data exploration.

    **FoldDB:** Streaming queries on Kafka/CDC. Incremental aggregation. Live-updating results. Joins between streams and tables.

    FoldDB uses DuckDB internally for file scanning — you get DuckDB's read performance with FoldDB's streaming semantics.

## Optimization History

The full complex query (CDC + JOIN + 7 aggregates) went through five rounds of optimization:

| Optimization | Throughput | Cumulative |
|---|---|---|
| Initial implementation | 6,700/sec | 1x |
| Fast key extraction + skip left arrangement | 46,500/sec | 6.9x |
| Decode: LazyJsonValue, eliminate double parse | 50,000/sec | 7.5x |
| Typed int64 index + cross-type coercion | 50,000/sec | (fixed Avro joins) |
| Projection pushdown into join merge | **114,000/sec** | **17x** |

### Key optimizations explained

**Fast key extraction.** Join key lookup bypasses the full expression evaluator for simple column references — direct map lookup instead of AST walk.

**Skip left arrangement.** For stream-to-file joins where the right side never changes, the left arrangement is skipped, eliminating `json.Marshal` fingerprinting on every record.

**LazyJsonValue.** Debezium virtual columns (`_before`, `_after`, `_source`) are stored as raw JSON bytes and only parsed if the query accesses them.

**Typed int64 index.** The arrangement uses `map[int64][]Record` for integer join keys, eliminating `strconv.FormatInt` per lookup. Cross-type coercion means `TextValue{"42"}` matches `IntValue{42}`.

**Projection pushdown.** The join merge only copies columns referenced by the query. For 6 used columns from a 30-column merge, this reduces per-record allocation by 5x.

## Profiling

FoldDB includes built-in CPU profiling:

```bash
folddb query --cpuprofile prof.out "SELECT ..."
go tool pprof prof.out
```
