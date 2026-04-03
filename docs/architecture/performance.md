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
| Parquet (DuckDB scan, DBSPA agg) | 904ms | 111K |

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

The most demanding query DBSPA supports — exercises every major feature:

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
| JSON Debezium | 964ms | **104K** |
| **Avro Debezium** | **841ms** | **119K** |

### Wire Size

| Format | 100K CDC events |
|---|---|
| JSON Debezium | 33 MB |
| **Avro Debezium** | **14 MB** (2.4x smaller) |

For Kafka deployments, Avro reduces broker storage and network bandwidth by 2.4x.

## DBSPA vs DuckDB

DuckDB is a vectorized columnar engine optimized for batch analytics. DBSPA is a streaming engine with incremental aggregation. They serve different use cases.

### Parquet GROUP BY (100K records)

| Engine | Time | Records/sec |
|---|---|---|
| **DuckDB (native)** | **17ms** | **5,882K** |
| DBSPA (via DuckDB scan) | 936ms | 107K |

DuckDB is ~55x faster on pure batch queries due to SIMD vectorized execution across all cores. DBSPA processes records through a goroutine pipeline optimized for streaming.

!!! tip "When to use which"
    **DuckDB:** One-shot queries on files. Batch analytics. Data exploration.

    **DBSPA:** Streaming queries on Kafka/CDC. Incremental aggregation. Live-updating results. Joins between streams and tables.

    DBSPA uses DuckDB internally for file scanning — you get DuckDB's read performance with DBSPA's streaming semantics.

## Live Kafka Benchmarks

End-to-end benchmarks with a live Kafka producer and DBSPA consumer. These measure real-world throughput including network I/O, Kafka consumer overhead, deserialization, and aggregation.

### Burst Throughput (200K messages, GROUP BY)

A producer writes 200K messages to a Kafka topic in a burst. DBSPA consumes with a GROUP BY aggregation query.

| Scenario | Throughput | Notes |
|---|---|---|
| JSON, GROUP BY (5 groups) | **68K msgs/sec** | Producer burst, consumer steady-state |

The bottleneck is JSON decoding, not Kafka consumption or aggregation. With Avro encoding, throughput increases proportionally to the decode savings.

### Sustained Throughput

For sustained streaming (producer rate-limited to match consumer), DBSPA processes at the same throughput as the stdin benchmarks above -- the Kafka consumer adds negligible overhead once messages are in the fetch buffer.

## Optimization History

The full complex query (CDC + JOIN + 7 aggregates) went through five rounds of optimization:

| Optimization | Throughput | Cumulative |
|---|---|---|
| Initial implementation | 6,700/sec | 1x |
| Fast key extraction + skip left arrangement | 46,500/sec | 6.9x |
| Decode: LazyJsonValue, eliminate double parse | 50,000/sec | 7.5x |
| Typed int64 index + cross-type coercion | 50,000/sec | (fixed Avro joins) |
| Projection pushdown into join merge | **119,000/sec** | **17.8x** |

### Key optimizations explained

**Fast key extraction.** Join key lookup bypasses the full expression evaluator for simple column references — direct map lookup instead of AST walk.

**Skip left arrangement.** For stream-to-file joins where the right side never changes, the left arrangement is skipped, eliminating `json.Marshal` fingerprinting on every record.

**LazyJsonValue.** Debezium virtual columns (`_before`, `_after`, `_source`) are stored as raw JSON bytes and only parsed if the query accesses them.

**Typed int64 index.** The arrangement uses `map[int64][]Record` for integer join keys, eliminating `strconv.FormatInt` per lookup. Cross-type coercion means `TextValue{"42"}` matches `IntValue{42}`.

**Projection pushdown.** The join merge only copies columns referenced by the query. For 6 used columns from a 30-column merge, this reduces per-record allocation by 5x.

## Disk-Backed Arrangements

For large joins and long windows, arrangements can spill to disk using [Badger](https://github.com/dgraph-io/badger) (a pure Go LSM-tree KV store). Enable with `--spill-to-disk` or set a memory budget with `--max-memory`:

```bash
# Use default memory budget (1M records)
dbspa --spill-to-disk "SELECT ... FROM ... JOIN ..."

# Set explicit memory budget
dbspa --max-memory 512MB "SELECT ... FROM ... JOIN ..."
```

Stream-stream joins (both FROM and JOIN are Kafka topics) auto-enable spill-to-disk to prevent OOM. Override with `--max-memory` to set an explicit budget.

When the in-memory arrangement exceeds the budget, the oldest entries (by timestamp) are spilled to Badger on disk. Lookups merge results from memory and disk transparently.

### Overhead

Benchmarked with a 100K-row reference table, 99% spilled to disk:

| Mode | Time | Records/sec | Overhead |
|---|---|---|---|
| In-memory (default) | 1.05s | 95K | — |
| Disk (limit=10K, 90K spilled) | 1.08s | 93K | 2.5% |
| Disk (limit=1K, 99K spilled) | 1.09s | 92K | 4% |

The overhead is minimal because hot join probes still hit the in-memory portion first, and Badger's prefix scans are fast for the cold path.

## Profiling

DBSPA includes built-in CPU profiling:

```bash
dbspa query --cpuprofile prof.out "SELECT ..."
go tool pprof prof.out
```
