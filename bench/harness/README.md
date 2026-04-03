# DBSPA Benchmark Harness

A self-contained benchmark suite that produces reproducible, machine-readable JSON results for CI regression gates and historical performance graphing.

## Quick Start

```bash
# Run all benchmarks (no Kafka required)
make bench-full

# Run including Kafka integration benchmarks (requires Docker)
make bench-full-kafka

# Compare current performance against baseline
make bench-compare
```

Or run directly:

```bash
./bench/harness/run.sh --output bench/harness/results/my_run.json
```

## What's Measured

### Format Benchmarks (100K records each)

| Benchmark | Description |
|-----------|-------------|
| `format/ndjson/passthrough` | SELECT * from NDJSON stream |
| `format/ndjson/filter` | SELECT * WHERE status = 'confirmed' |
| `format/ndjson/groupby` | GROUP BY product, region with COUNT + SUM |
| `format/avro/passthrough` | SELECT * FORMAT AVRO |
| `format/parquet/passthrough` | SELECT * from Parquet file (via DuckDB) |
| `format/parquet/filter` | SELECT * WHERE ... from Parquet |
| `format/parquet/groupby` | GROUP BY from Parquet |
| `format/csv/passthrough` | SELECT * FORMAT CSV |

### CDC Benchmarks (100K events)

| Benchmark | Description |
|-----------|-------------|
| `cdc/json/groupby` | Debezium JSON envelope GROUP BY |
| `cdc/avro/groupby` | Debezium Avro envelope GROUP BY |

### Join Benchmarks (100K stream x 5K table)

| Benchmark | Description |
|-----------|-------------|
| `join/file/simple` | Stream JOIN file lookup |
| `join/file/groupby` | Stream JOIN file + GROUP BY |
| `join/file/complex` | CDC + JOIN + 7 aggregates + CASE + HAVING + ORDER BY |
| `join/subquery` | JOIN (SELECT ... GROUP BY ...) subquery |

### Pipeline Benchmarks

| Benchmark | Description |
|-----------|-------------|
| `pipeline/batch_compaction` | CompactBatch effectiveness (high key duplication) |
| `pipeline/operator_fusion` | FusedAggregateProcessor (low key duplication) |

### Kafka Benchmarks (--with-kafka only)

| Benchmark | Description |
|-----------|-------------|
| `kafka/produce` | Produce 100K Confluent Avro messages |
| `kafka/consume/passthrough` | Consume 100K from Kafka |
| `kafka/consume/groupby` | Consume + GROUP BY |
| `kafka/live/burst` | Concurrent produce + consume (200K burst) |

## How It Works

1. **Builds** dbspa and dbspa-gen binaries from source
2. **Generates** deterministic fixtures (seed=42) so results are reproducible
3. **Runs** each benchmark 3 times and reports the median
4. **Outputs** JSON with full run details + system metadata
5. **Compares** against baseline.json (if present), flagging regressions >10%

## Output Format

```json
{
  "timestamp": "2026-03-28T15:00:00Z",
  "dbspa_version": "v0.1.0-213-g84294bf",
  "go_version": "go1.25.7",
  "os": "darwin",
  "arch": "arm64",
  "cpu": "Apple M4",
  "benchmarks": [
    {
      "name": "format/ndjson/passthrough",
      "records": 100000,
      "runs": [362, 358, 365],
      "median_ms": 362,
      "records_per_sec": 276243,
      "input_bytes": 17754500
    }
  ]
}
```

## Interpreting Results

- **records_per_sec**: Primary throughput metric. Higher is better.
- **median_ms**: Wall-clock time for the median run. Lower is better.
- **runs**: All 3 raw timings in milliseconds, for variance analysis.
- **input_bytes**: Size of generated fixture data (for bytes/sec calculation).

## Updating the Baseline

After a performance improvement (or when establishing a new baseline on different hardware):

```bash
./bench/harness/run.sh --output bench/harness/baseline.json
```

Then commit baseline.json. The baseline is machine-specific -- numbers from an M4 are not comparable to numbers from an Intel CI runner.

## CI Usage

```yaml
- name: Run benchmarks
  run: |
    make bench-full
    # Exits non-zero if any benchmark regresses >10% vs baseline
```

The harness exits with code 1 if any benchmark is more than 10% slower than the baseline. This makes it suitable as a CI gate.

For CI, you should maintain a baseline.json generated on the same hardware (or instance type) as your CI runners.

## Directory Structure

```
bench/harness/
├── docker-compose.yml   # Kafka + Schema Registry for Kafka benchmarks
├── run.sh               # Main entry point
├── benchmarks.go        # Go benchmark program (standalone main)
├── baseline.json        # Committed reference results
├── results/             # Timestamped JSON results (gitignored)
└── README.md            # This file
```
