# Error Handling

FoldDB is designed to keep processing when individual records fail. Errors are logged to stderr, and when `--dead-letter` is configured, the offending records are routed to a file for inspection.

## Error types

### Deserialization errors

A record can't be decoded — malformed JSON, invalid Avro, truncated Protobuf.

```
Warning: JSON decode error: unexpected character 'N' at offset 0
```

The record is skipped. Processing continues.

### Schema drift (incompatible)

A column's type changes mid-stream in a way that breaks the query. For example, a column that was `INT` suddenly arrives as `BOOL`.

```
Error: schema drift: column "amount" changed from INT to BOOL (incompatible) (record skipped)
```

The record is skipped. Processing continues.

### Schema drift (compatible)

A column's type changes in a compatible way. For example, `INT` to `FLOAT`, or `INT` to a text value that parses as a number.

```
Warning: column "amount" type changed from INT to FLOAT (compatible, continuing)
```

The record is processed normally. This is a warning only — no record is skipped.

### Accumulator type errors

A numeric aggregate (`SUM`, `AVG`, `MIN`, `MAX`) receives a non-numeric value that can't be coerced. For example, `SUM` on a `BOOL` or `JSON` value, or `SUM` on a text value that doesn't parse as a number.

```
Warning: SUM received BOOL value, expected numeric. Value skipped.
```

The value is skipped for that aggregate (as if it were NULL). Processing continues.

### Avro schema evolution

When consuming from Kafka with the Confluent Schema Registry, a new schema version may appear. FoldDB compares the new schema against the previous one:

- **Field added**: Warning (compatible).
- **Field removed**: Error if the query references the removed field.
- **Type promotion** (int → long, float → double): Warning (compatible per Avro spec).
- **Breaking type change** (int → string): Error.

## Dead letter queue

The `--dead-letter` flag routes error records to an NDJSON file for inspection:

```bash
folddb --dead-letter errors.ndjson "SELECT SUM(amount) FROM 'kafka://broker/orders' FORMAT AVRO GROUP BY status"
```

Each line in the dead letter file is a JSON object with:

```json
{"error":"schema drift: column \"amount\" changed from INT to BOOL (incompatible)","raw":"{\"amount\":{\"V\":true}}","offset":0,"partition":0}
```

| Field | Description |
|---|---|
| `error` | Human-readable error message |
| `raw` | The raw record data (serialized columns or original bytes) |
| `offset` | Kafka offset (0 for stdin) |
| `partition` | Kafka partition (0 for stdin) |

### What routes to the dead letter file

| Error type | Logged to stderr | Routed to dead letter |
|---|---|---|
| Deserialization errors | Yes | Yes |
| Schema drift (incompatible) | Yes | Yes |
| Schema drift (compatible) | Yes (warning) | No |
| Accumulator type errors | Yes (warning) | Yes |
| Late data (beyond watermark) | Yes | Yes |

### Inspecting dead letters

The dead letter file is plain NDJSON — query it with FoldDB itself:

```bash
# Count errors by type
cat errors.ndjson | folddb "SELECT error, COUNT(*) GROUP BY error"

# Find the most recent errors
tail -10 errors.ndjson | folddb "SELECT error, raw"
```

## Warning rate limiting

Warnings for compatible schema drift and accumulator type errors are rate-limited:

- **Schema drift**: One warning per column (the first time a column's type changes).
- **Accumulator type errors**: One warning per aggregate+type combination (e.g., one warning for `SUM+BOOL`, one for `AVG+JSON`).

This prevents stderr from being overwhelmed when a data quality issue affects many records.

## EXEC error behavior

When a command run via `EXEC()` exits with a non-zero exit code, FoldDB treats it as a **fatal error** and terminates the process. The tail of the command's stderr is included in the error message to aid debugging.

```
Fatal: EXEC command exited with code 1: ERROR: relation "orders" does not exist
```

**Subprocess stderr** is buffered silently during normal execution -- it is only shown on failure. This keeps FoldDB's stderr clean when commands produce informational messages on stderr.

**Exception:** When the query is cancelled via context cancellation (e.g., Ctrl+C), the subprocess exit code is ignored. The subprocess receives SIGTERM and its exit code (typically 143) is not treated as an error.

### Error routing for EXEC

| Error type | Logged to stderr | Routed to dead letter |
|---|---|---|
| EXEC non-zero exit | Yes (fatal -- process terminates) | No (process terminates) |
| EXEC output deserialization error | Yes | Yes |

## Design philosophy

FoldDB follows a **skip and continue** model:

1. **Don't crash.** A single bad record should not terminate a streaming query that has been running for hours.
2. **Don't silently swallow.** Every skipped record is logged to stderr and optionally routed to the dead letter file.
3. **Don't corrupt.** Incompatible values are skipped rather than coerced incorrectly. `SUM(true)` produces a skip, not `SUM(1)`.
4. **Coerce when safe.** `SUM("100.50")` parses the text as a number and adds it. This is schema-on-read — the data is self-describing, and if it can be interpreted correctly, it should be.
