# Development

## Prerequisites

- Go 1.25+
- Docker (for integration tests)
- Make

## Project structure

```
dbspa/
├── cmd/
│   ├── dbspa/          # CLI binary
│   ├── dbspa-gen/      # Data generator utility
│   └── demo/            # Interactive web demo
├── internal/
│   ├── sql/
│   │   ├── lexer/       # Tokenizer
│   │   ├── parser/      # Recursive descent parser
│   │   └── ast/         # AST node types
│   ├── engine/
│   │   ├── record.go    # Record type (values, timestamp, diff)
│   │   ├── value.go     # Value types
│   │   ├── pipeline.go  # Pipeline wiring
│   │   ├── filter.go    # WHERE evaluation
│   │   ├── eval.go      # Expression evaluation
│   │   ├── project.go   # SELECT projection
│   │   ├── accumulator.go  # Accumulator interface + implementations
│   │   ├── aggregate.go    # Aggregate operator
│   │   ├── windowed_aggregate.go  # Windowed aggregate operator
│   │   ├── window.go    # Window assignment
│   │   ├── watermark.go # Watermark tracking
│   │   ├── eventtime.go # Event time parsing
│   │   ├── dedup.go     # Dedup LRU cache
│   │   ├── join.go      # Hash join operator
│   │   └── checkpoint.go # Checkpoint manager
│   ├── source/          # Source connectors (Kafka, stdin)
│   ├── format/          # Format decoders (JSON, CSV, Avro, Protobuf, Parquet, Debezium)
│   └── sink/            # Output sinks (TUI, changelog, JSON, SQLite, HTTP)
├── test/integration/    # Integration tests (build tag: integration)
├── testdata/            # Fixture files
├── scripts/             # Helper scripts (verify.sh, seed-kafka.sh)
├── bench/               # Benchmark harness
└── docs/                # Documentation (mkdocs)
```

## Build

```bash
make build    # Builds dbspa and dbspa-gen binaries
```

## Testing

```bash
make test                # Unit tests (go test ./...)
make test-integration    # Integration tests (requires Docker for Kafka)
make test-all            # Unit + integration + verify
make verify              # Compare dbspa results against DuckDB
make bench               # Run benchmarks
make lint                # go vet ./...
```

!!! tip
    Always run `go test ./...` after any code change before committing.

## Integration tests

Integration tests require Docker. They spin up Kafka and Schema Registry via Docker Compose:

```bash
make test-integration
```

This runs `docker compose up -d`, waits for Kafka to be healthy, seeds test data, runs the tests, and tears down the infrastructure.

## Benchmarks

```bash
make bench
```

Builds both binaries, generates test data with `dbspa-gen`, and runs end-to-end throughput benchmarks. Results are saved to `bench/results.txt`.

To run a specific benchmark:

```bash
go test -tags bench -bench BenchmarkGroupBy_1M -benchtime 1x -timeout 10m ./bench/
```

## Verification

The verify script compares DBSPA output against DuckDB for correctness:

```bash
make verify    # Generates 10,000 records and compares results
```

## Key dependencies

| Purpose | Library |
|---|---|
| Kafka client | `github.com/twmb/franz-go` |
| CLI framework | `github.com/alecthomas/kong` |
| SQLite (pure Go) | `modernc.org/sqlite` |
| Avro deserialization | `github.com/linkedin/goavro/v2` |
| Fast JSON | `github.com/goccy/go-json` |

## Commit discipline

Commit every code change immediately. One logical change = one commit. Do not batch unrelated changes.

## Release builds

```bash
make release
```

Produces binaries in `dist/` for Linux (amd64, arm64) and macOS (amd64, arm64).
