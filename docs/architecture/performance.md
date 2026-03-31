# Performance

## Architecture

FoldDB's performance characteristics follow from its pipeline architecture:

- **JSON decoding is the bottleneck**, not accumulation. The single-goroutine accumulator uses ~20ms/sec of CPU at 275K records/sec for O(1) aggregates.
- **Non-accumulating queries** bypass the accumulator entirely, achieving full parallelism across Kafka partitions.
- **For Kafka sources**, one goroutine per partition provides parallel decode and filter. The fan-in to the accumulator is the serialization point.

## Benchmark suite

Benchmarks are in `bench/bench_test.go`. Run with:

```bash
make bench
```

This builds `folddb` and `folddb-gen` (data generator), then runs end-to-end benchmarks measuring wall-clock throughput including process startup, data generation, decode, filter/aggregate, and output.

### Benchmark categories

**Passthrough** (`SELECT *`) — measures decode + output overhead:

| Benchmark | Records | What it measures |
|---|---|---|
| `BenchmarkPassthrough_100K` | 100K | NDJSON decode + output |
| `BenchmarkPassthrough_1M` | 1M | NDJSON decode + output at scale |

**Filter** (`SELECT * WHERE status = 'confirmed'`) — measures filter evaluation:

| Benchmark | Records |
|---|---|
| `BenchmarkFilter_100K` | 100K |
| `BenchmarkFilter_1M` | 1M |

**GROUP BY** — measures accumulator throughput:

| Benchmark | Records | Query |
|---|---|---|
| `BenchmarkGroupBy_100K` | 100K | Single key, COUNT |
| `BenchmarkGroupBy_1M` | 1M | Single key, COUNT |
| `BenchmarkGroupByMultiKey_100K` | 100K | Two keys, COUNT + SUM |
| `BenchmarkGroupByMultiKey_1M` | 1M | Two keys, COUNT + SUM |
| `BenchmarkGroupByWithFilter_1M` | 1M | Two keys + WHERE filter |

**CDC** — measures Debezium envelope decoding with retractions:

| Benchmark | Records |
|---|---|
| `BenchmarkCDC_100K` | 100K |

### Format comparison

All formats are benchmarked at 100K records across passthrough, filter, and GROUP BY:

| Format | Passthrough | Filter | GROUP BY |
|---|---|---|---|
| NDJSON | `BenchmarkFormat_NDJSON_*` | | |
| Avro OCF | `BenchmarkFormat_Avro_*` | | |
| Protobuf (self-describing) | `BenchmarkFormat_Protobuf_*` | | |
| Protobuf (typed) | `BenchmarkFormat_ProtoTyped_*` | | |
| Parquet | `BenchmarkFormat_Parquet_*` | | |

## Reported performance

From the landing page, approximate throughput on representative hardware:

- **NDJSON passthrough**: ~275K records/sec
- **Parquet filter**: ~850K records/sec

Parquet is significantly faster because it uses columnar storage with predicate pushdown, avoiding full-record deserialization.

## Memory usage

| Aggregate type | State per group key | Example: 10M groups |
|---|---|---|
| `COUNT(*)`, `SUM`, `AVG` | O(1) — a few numbers | ~80MB |
| `MIN`, `MAX` | O(n) — all values in a heap | Depends on values per key |
| `MEDIAN`, `PERCENTILE` | O(n) — all values sorted | ~8GB at 100 values/key |
| `COUNT(DISTINCT)` | O(n) — set of distinct values | Depends on cardinality |

The `--memory-limit` flag (default 1GB) triggers a warning when the accumulator map exceeds the threshold. FoldDB does not spill to disk in v0.

## Batch pipeline performance

The Z-set batch pipeline (`BatchChannel`) collects individual records into slices of up to 1024 before sending them downstream. This reduces per-record channel overhead and improves cache locality.

### Benchmark results

Compared to the previous per-record channel pipeline:

| Query type | Improvement | Why |
|---|---|---|
| Filter/project (non-accumulating) | ~40% faster | Channel sends reduced by ~1000x; filter loop over contiguous slice is cache-friendly |
| GROUP BY aggregation (O(1) aggregates) | ~11% faster | Batch amortizes channel overhead, but accumulator hash map updates remain per-record |

The filter/project improvement is larger because those queries are dominated by channel overhead (the actual per-record work is minimal). Aggregation queries spend more time in accumulator updates, so the channel overhead reduction is a smaller fraction of total cost.

### Remaining bottlenecks

- **JSON parsing** remains the single largest cost (~60% of CPU for NDJSON). Batching does not help here -- each record must still be parsed individually.
- **Accumulator hash map lookups** are per-record even within a batch. Future batch compaction (summing weights for identical group keys before accumulation) will reduce this for high-cardinality workloads.
- **Output serialization** is unchanged -- the sink processes records individually after unbatching.

## Bottleneck analysis

For a typical GROUP BY query over NDJSON:

1. **JSON parsing**: ~60% of CPU. This is the single largest cost.
2. **Expression evaluation** (WHERE, SELECT): ~15%.
3. **Accumulator updates**: ~5% for O(1) aggregates.
4. **Output serialization**: ~10%.
5. **Channel overhead**: ~5% (reduced from ~10% by batching).

For binary formats (Avro, Protobuf, Parquet), step 1 is dramatically cheaper, shifting the bottleneck to I/O or expression evaluation.

## Running benchmarks

```bash
# Full benchmark suite
make bench

# Specific benchmark
go test -tags bench -bench BenchmarkGroupBy_1M -benchtime 1x -timeout 10m ./bench/

# Results are saved to bench/results.txt
```

The benchmarks use `folddb-gen` to produce deterministic test data with `--seed 42` for reproducibility.
