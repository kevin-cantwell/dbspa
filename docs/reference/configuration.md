# Configuration

FoldDB is configured through CLI flags and an optional credentials file. There is no configuration file for general settings.

## CLI flags

All configuration is passed via flags. See the [CLI reference](cli.md) for the complete list.

Key flags:

```bash
folddb [flags] <SQL>
  -i, --input <file>              # Input file (instead of stdin)
  --state <file.db>               # SQLite state file
  --stateful                      # Enable checkpointing
  --state-dir <path>              # Checkpoint directory (default: ~/.folddb/state)
  --checkpoint-interval <dur>     # Checkpoint flush interval (default: 5s)
  --timeout <duration>            # Query timeout
  --limit <n>                     # Max output records
  --dead-letter <file>            # Dead letter file for errors
  --dry-run                       # Print plan, don't execute
  --explain                       # Print plan, then execute
```

## Credentials file

Kafka credentials are stored in `~/.folddb/credentials` (TOML format):

```toml
[kafka.production]
brokers = ["pkc-xxx.confluent.cloud:9092"]
sasl_mechanism = "PLAIN"
sasl_username = "my-api-key"
sasl_password = "my-api-secret"
registry = "https://psrc-xxx.confluent.cloud"

[kafka.staging]
brokers = ["kafka-staging.internal:9092"]
# No auth needed for internal brokers
```

Reference by name in queries:

```sql
FROM 'kafka://production/orders.cdc'
FROM 'kafka://staging/events'
```

### Supported authentication methods

| Method | Configuration |
|---|---|
| PLAINTEXT | No auth params |
| SASL/PLAIN | `sasl_mechanism = "PLAIN"` + username/password |
| SASL/SCRAM-256 | `sasl_mechanism = "SCRAM-SHA-256"` + username/password |
| SASL/SCRAM-512 | `sasl_mechanism = "SCRAM-SHA-512"` + username/password |
| mTLS | `tls_cert`, `tls_key`, `tls_ca` paths |

Authentication can also be passed as URI query parameters:

```sql
FROM 'kafka://broker/topic?sasl_mechanism=PLAIN&sasl_username=X&sasl_password=Y'
FROM 'kafka://broker/topic?tls_cert=/path/cert.pem&tls_key=/path/key.pem&tls_ca=/path/ca.pem'
```

## State directory

Checkpoint state is stored in `~/.folddb/state/<query-hash>/` by default.

Override with `--state-dir`:

```bash
folddb --stateful --state-dir /var/lib/folddb/state "..."
```

Manage checkpoints:

```bash
folddb state list                    # List all checkpointed queries
folddb state inspect <hash>          # Show checkpoint details
folddb state reset <hash>            # Delete checkpoint
```

## SQLite state store

The `--state` flag writes the current aggregation result to a SQLite file:

```bash
folddb --state orders.db "SELECT region, COUNT(*) GROUP BY region"
```

The SQLite file uses WAL mode for concurrent read access. Other processes can read the file while FoldDB is writing:

```bash
sqlite3 orders.db "SELECT * FROM result ORDER BY cnt DESC"
```

**Schema:** The `result` table schema is derived from the query's output columns. GROUP BY columns become the composite primary key (for UPSERT).

**Non-accumulating queries:** Records are INSERTed (append-only) with auto-increment `_rowid` and `_ingested_at` columns.

**File locking:** If another process holds a write lock, FoldDB retries with exponential backoff (100ms to 5s). After 30 seconds total, it exits with an error.

## Memory limit

FoldDB keeps all accumulator state in memory. The default limit is 1GB. When exceeded, a warning is logged but processing continues (no disk spill in v0).

## Output mode auto-detection

| Context | Accumulating | Non-accumulating |
|---|---|---|
| TTY | TUI (live table) | Scrolling NDJSON |
| Piped / non-TTY | Changelog NDJSON | Plain NDJSON |
| `--state file.db` | SQLite UPSERT | SQLite INSERT |

Override with `--mode tui`, `--mode changelog`, or `--mode json`.
