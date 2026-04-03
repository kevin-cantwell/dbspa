# Formats

DBSPA uses a two-tier format model: **encoding** (how bytes are serialized) and **envelope** (how to interpret the record and derive Z-set weights). The format is declared with the `FORMAT` clause or auto-detected from the source.

```sql
FORMAT <encoding> [<envelope>] [(<options>)]
```

See [FORMAT syntax](sql.md#format) for the full clause reference.

---

## Encodings

Encodings define the wire serialization of each record.

### JSON / NDJSON (default)

Newline-delimited JSON. One JSON object per line.

```bash
cat data.ndjson | dbspa "SELECT name, age WHERE age > 25"
```

No `FORMAT` clause needed -- NDJSON is the default for both stdin and Kafka.

Each top-level JSON key becomes a column. Nested objects are accessible via JSON operators (`->`, `->>`).

### CSV

```sql
FORMAT CSV
FORMAT CSV(delimiter=',', header=true, quote='"', null_string='')
```

| Option | Default | Description |
|---|---|---|
| `delimiter` | `,` | Field separator character |
| `header` | `true` | First line contains column names |
| `quote` | `"` | Quote character for fields containing delimiters |
| `null_string` | `''` | String representation of NULL |

```bash
cat data.csv | dbspa "SELECT name, age WHERE age > 25 FORMAT CSV"
cat data.tsv | dbspa "SELECT * FORMAT CSV(delimiter='\t')"
```

When `header=true`, column names come from the header row. When `header=false`, columns are named `col1`, `col2`, etc.

### Avro

Apache Avro Object Container File (OCF) format, or Confluent wire format (with schema registry).

```sql
FORMAT AVRO
FORMAT AVRO(registry='http://registry:8081')
```

**With schema registry:** Kafka messages using the Confluent wire format (magic byte `0x00` + 4-byte schema ID) are auto-detected. The schema is fetched from the registry and cached. See [Confluent Wire Format](#confluent-wire-format) below for details.

**Without registry:** stdin or file input must be Avro OCF (self-contained with embedded schema).

```bash
cat data.avro | dbspa "SELECT * FORMAT AVRO"
```

### Protobuf

Protocol Buffers format.

```sql
-- Self-describing (includes field descriptors)
FORMAT PROTOBUF

-- Typed (requires message name)
FORMAT PROTOBUF(message='Order')
```

**Self-describing Protobuf** includes enough metadata to decode without a `.proto` file. Fields are named by their field numbers (`field1`, `field2`, etc.) unless the message is self-describing.

**Typed Protobuf** uses a pre-registered message name. The schema must be available (via schema registry or compiled into the data generator).

```bash
cat data.pb | dbspa "SELECT * FORMAT PROTOBUF"
cat data.pb | dbspa "SELECT order_id, status FORMAT PROTOBUF(message='Order')"
```

### Parquet

Apache Parquet columnar format. Requires a seekable file (not stdin).

```sql
FORMAT PARQUET
```

Use with `--input` or in a JOIN:

```bash
dbspa -i data.parquet "SELECT * WHERE status = 'active' FORMAT PARQUET"
```

```sql
JOIN '/data/users.parquet' u ON e.user_id = u.id
```

Parquet is significantly faster than NDJSON (~850K records/sec vs ~275K records/sec) because it uses columnar storage and can push down predicates.

---

## Envelopes

Envelopes define how to interpret each record and derive [Z-set weights](../concepts/diff-model.md). When no envelope is specified, every record is treated as a plain insert (weight=+1).

### Debezium CDC

Debezium CDC envelope format. Used with Kafka sources that carry database change events.

```sql
CHANGELOG DEBEZIUM            -- JSON-encoded Debezium CDC
FORMAT AVRO CHANGELOG DEBEZIUM -- Avro-encoded Debezium CDC
```

Debezium records have an `op` field and `before`/`after` payloads. DBSPA unwraps the envelope and derives weights from the `op` field:

| Debezium `op` | DBSPA records emitted |
|---|---|
| `c` (create) | 1 record: `(after, weight=+1)` |
| `u` (update) | 2 records: `(before, weight=-1)` then `(after, weight=+1)` |
| `d` (delete) | 1 record: `(before, weight=-1)` |
| `r` (snapshot read) | 1 record: `(after, weight=+1)` |
| `t` (truncate) | Ignored (logged at warn level) |

When a Debezium envelope is specified, [virtual columns](sql.md#debezium-virtual-columns) (`_op`, `_before`, `_after`, `_table`, `_db`, `_ts`, `_source`) are available.

!!! note
    If `_before` is NULL on an update (common without `REPLICA IDENTITY FULL`), the retraction is skipped. DBSPA logs a warning on the first occurrence. Accumulators may drift over time without full replica identity.

**Avro-encoded Debezium** (`FORMAT AVRO CHANGELOG DEBEZIUM`) combines the CDC semantics with the compact binary encoding of Avro. Debezium Avro messages use the Confluent wire format and require a schema registry:

```sql
FROM 'kafka://broker/orders.cdc?registry=http://schema-registry:8081' FORMAT AVRO CHANGELOG DEBEZIUM
```

Benefits over JSON Debezium:

- **2.4x smaller wire size** -- 14 MB vs 33 MB for 100K CDC events (see [Performance](../architecture/performance.md#wire-size)).
- **Typed before/after records** -- the Avro schema defines the field types, so there is no JSON re-parse step. Fields arrive as native integers, floats, and strings.
- **Faster decoding** -- binary Avro decoding is cheaper than JSON parsing for large payloads.

### DBSPA Changelog

The DBSPA envelope reads the Feldera weighted format — a JSON object with a `weight` field (integer) and a `data` field (object containing the columns). This is the native format of DBSPA's [changelog output](../concepts/changelog-output.md), aligned with [Feldera's](https://github.com/feldera/feldera) "weighted" input format for interoperability.

```sql
CHANGELOG DBSPA              -- Feldera weighted format
```

The primary use case is **composing DBSPA instances**: one instance produces a changelog, another consumes it with further transformations.

```bash
# Instance 1: produce a changelog of order counts by status
dbspa "SELECT status, COUNT(*) FROM 'kafka://broker/orders' GROUP BY status" | \

# Instance 2: consume the changelog and filter for pending orders
dbspa "SELECT * FROM stdin CHANGELOG DBSPA WHERE status = 'pending'"
```

Each record must be a JSON object with a `weight` field (integer) and a `data` field (object). Positive weights are insertions, negative weights are retractions. For example: `{"weight":1,"data":{"status":"pending","cnt":42}}`. Records without a `weight` field default to weight=+1 with the entire object treated as data.

### Confluent Wire Format

Production Kafka deployments using the Confluent Schema Registry encode messages with a 5-byte header:

| Bytes | Content |
|---|---|
| 1 | Magic byte `0x00` |
| 4 | Schema ID (big-endian `uint32`) |
| remaining | Avro (or Protobuf) payload |

When DBSPA detects this header on a Kafka message, it:

1. Extracts the 4-byte schema ID.
2. Fetches the schema from the registry via HTTP (`GET /schemas/ids/{id}`).
3. Caches the schema locally. Schema IDs are immutable in the Confluent registry, so a given ID always maps to the same schema.
4. Decodes the payload using the fetched schema.

The registry URL is configured via the `?registry=` URI parameter or the [credentials file](configuration.md#credentials-file):

```sql
-- Via URI parameter
FROM 'kafka://broker/topic?registry=http://schema-registry:8081' FORMAT AVRO

-- Via credentials file (registry field under [kafka.cluster-name])
FROM 'kafka://production/topic' FORMAT AVRO
```

This wire format is used by both `FORMAT AVRO` and `FORMAT AVRO DEBEZIUM`.

---

## Format auto-detection

| Source | Default format | Override with |
|---|---|---|
| Kafka (no registry) | NDJSON | `FORMAT CSV`, `FORMAT AVRO`, `CHANGELOG DEBEZIUM`, etc. |
| Kafka (with registry) | Auto-detect via magic byte | `FORMAT AVRO`, `FORMAT PROTOBUF` |
| stdin | NDJSON | Any `FORMAT` clause |
| File (JOIN / --input) | Detected from extension | `FORMAT` clause |
