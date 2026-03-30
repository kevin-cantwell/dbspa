# FoldDB Development Instructions

## Commit Discipline

**Commit every code change.** Every time you modify code, commit it immediately. This helps with:
- Undoing changes if something breaks
- Viewing changes over time
- Clean git history for review

Do not batch multiple unrelated changes into one commit. One logical change = one commit.

## Testing

- `make test` — run unit tests
- `make test-integration` — run integration tests (requires Docker)
- `make bench` — run benchmarks
- `make verify` — compare folddb results against DuckDB
- Always run `go test ./...` after any code change before committing

## Project Structure

- `cmd/folddb/` — the main CLI binary
- `cmd/folddb-gen/` — data generator utility (separate from folddb)
- `cmd/demo/` — interactive web demo
- `internal/` — core engine, parser, sources, sinks, formats
- `test/integration/` — integration tests (build tag: `integration`)
- `testdata/` — fixture files
- `scripts/` — helper scripts (verify.sh, seed-kafka.sh)
- `bench/` — benchmark harness and results

## Key Dependencies

- `github.com/twmb/franz-go` — Kafka client
- `github.com/alecthomas/kong` — CLI framework
- `modernc.org/sqlite` — pure Go SQLite (no CGo)
