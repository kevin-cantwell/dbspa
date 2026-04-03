# CLI Reference

## Synopsis

```
dbspa [flags] <SQL>
dbspa [flags] -f <file.sql>
dbspa serve [flags] <SQL>
dbspa schema [source [FORMAT ...]]
dbspa state list
dbspa state inspect <hash>
dbspa state reset <hash>
dbspa version
```

## Default command: query

Execute a SQL query against streaming data.

```bash
dbspa "SELECT name, age WHERE age > 25"
dbspa -f query.sql
cat data.json | dbspa "SELECT * WHERE status = 'active'"
```

### Flags

| Flag | Short | Default | Description |
|---|---|---|---|
| `<SQL>` | | | SQL query (positional argument) |
| `-f <file>` | | | Read SQL from a file instead of argument |
| `-i <file>` / `--input <file>` | `-i` | stdin | Read input data from a file instead of stdin |
| `--state <file.db>` | | | Write accumulated state to a SQLite file |
| `--limit <n>` | | 0 (unlimited) | Terminate after N output records |
| `--timeout <duration>` | | 0 (none) | Terminate after duration (e.g., `30s`, `5m`) |
| `--stateful` | | false | Enable persistent checkpoints for fast restart |
| `--state-dir <path>` | | `~/.dbspa/state` | Directory for checkpoint files |
| `--checkpoint-interval <dur>` | | `5s` | How often to flush checkpoints |
| `--dead-letter <file>` | | | Route error records to NDJSON file. See [Error Handling](../concepts/error-handling.md). |
| `--spill-to-disk` | | false | Spill large join arrangements to disk (Badger) to prevent OOM |
| `--max-memory <size>` | | | Memory budget for arrangements before spilling to disk (e.g., `256MB`, `1GB`). Implies `--spill-to-disk`. |
| `--arrangement-mem-limit <n>` | | 0 (unlimited) | *(deprecated, hidden)* Max in-memory records per arrangement before spilling to disk |
| `--cpuprofile <file>` | | | Write CPU profile to file (for `go tool pprof`) |
| `--dry-run` | | false | Parse and print query plan without executing |
| `--explain` | | false | Print query plan, then execute |

### Examples

```bash
# Basic filter
echo '{"name":"alice","age":30}' | dbspa "SELECT name WHERE age > 25"

# Aggregate from file input
dbspa -i orders.ndjson "SELECT status, COUNT(*) GROUP BY status"

# Kafka with state
dbspa --stateful --state orders.db \
  "SELECT region, COUNT(*) FROM 'kafka://broker/orders.cdc' CHANGELOG DEBEZIUM GROUP BY region"

# Dry run
dbspa --dry-run "SELECT status, COUNT(*) GROUP BY status"

# With timeout
dbspa --timeout 30s "SELECT * FROM 'kafka://broker/events'"

# Dead letter output
dbspa --dead-letter errors.ndjson "SELECT * FROM 'kafka://broker/events'"

# Spill to disk for large joins
dbspa --spill-to-disk \
  "SELECT o.*, c.name FROM 'kafka://broker/orders' o JOIN '/data/customers.parquet' c ON o.customer_id = c.id"

# With memory budget
dbspa --max-memory 512MB \
  "SELECT o.*, c.name FROM 'kafka://broker/orders' o JOIN '/data/customers.parquet' c ON o.customer_id = c.id"

# CPU profiling
dbspa --cpuprofile prof.out "SELECT status, COUNT(*) GROUP BY status"
go tool pprof prof.out

# Debezium Avro with schema registry
dbspa "SELECT $op, customer_id, total
        FROM 'kafka://broker/orders.cdc?registry=http://schema-registry:8081'
        FORMAT AVRO CHANGELOG DEBEZIUM"
```

!!! note
    Stream-stream joins (both FROM and JOIN are Kafka topics) auto-enable `--spill-to-disk` to prevent OOM. Override with `--max-memory` to set an explicit budget, or `--spill-to-disk=false` to disable.

## serve

Run a query and serve results via HTTP.

```bash
dbspa serve [flags] <SQL>
```

### Flags

| Flag | Short | Default | Description |
|---|---|---|---|
| `<SQL>` | | | SQL query (positional argument) |
| `--port <n>` | | `8080` | HTTP port |
| `-i <file>` / `--input <file>` | `-i` | stdin | Read input from a file |
| `--state <file.db>` | | | SQLite state file |
| `--timeout <duration>` | | 0 (none) | Query timeout |

### Endpoints

| Endpoint | Description |
|---|---|
| `GET /` | Current result set as JSON array |
| `GET /stream` | SSE changelog stream |
| `GET /health` | Liveness check |
| `GET /schema` | Output schema |

### Example

```bash
dbspa serve --port 8080 \
  "SELECT region, COUNT(*) AS orders
   FROM 'kafka://broker/orders.cdc' CHANGELOG DEBEZIUM
   GROUP BY region"

# Query it
curl http://localhost:8080/
curl http://localhost:8080/stream
curl http://localhost:8080/health
```

## schema

Print the inferred schema for a data source.

```bash
dbspa schema 'kafka://localhost:9092/orders.cdc'
dbspa schema 'kafka://broker/events' FORMAT AVRO
```

Example output:

```
Source:    kafka://localhost:9092/orders.cdc
Format:    NDJSON (detected)
Partitions: 12
Sample:    100 records from partition 0

Inferred schema:
  op          TEXT        "c", "u", "d"
  before      JSON        nullable
  after       JSON        {order_id: INT, status: TEXT, region: TEXT, total: FLOAT, ...}
  source      JSON        {db: TEXT, table: TEXT, ts_ms: BIGINT, lsn: TEXT, ...}

With CHANGELOG DEBEZIUM, virtual columns:
  $op         TEXT
  $before     JSON
  $after      JSON
  $table      TEXT
  $db         TEXT
  $ts         TIMESTAMP
  $source     JSON
```

## state

Manage checkpoint state.

### state list

List all checkpointed queries.

```bash
dbspa state list
```

```
  a1b2c3d4e5f6  last_flush=2026-03-28T14:02:31Z  dir=/Users/you/.dbspa/state/a1b2c3d4e5f6
```

### state inspect

Show checkpoint details for a specific query hash.

```bash
dbspa state inspect a1b2c3d4
```

### state reset

Delete a checkpoint. The next run of the matching query will replay from scratch.

```bash
dbspa state reset a1b2c3d4
```

## version

Print the DBSPA version.

```bash
dbspa version
```

```
dbspa v0.1.0
```

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Error (parse error, connection failure, etc.) |

## Environment

DBSPA reads Kafka credentials from `~/.dbspa/credentials` (TOML format):

```toml
[kafka.my-cluster]
brokers = ["pkc-xxx.confluent.cloud:9092"]
sasl_mechanism = "PLAIN"
sasl_username = "my-key"
sasl_password = "my-secret"
registry = "https://psrc-xxx.confluent.cloud"
```

Reference the cluster by name in queries:

```sql
FROM 'kafka://my-cluster/events'
```
