# Checkpointing

Checkpointing allows DBSPA to survive restarts without replaying the entire stream from the beginning.

## Enabling checkpoints

```bash
dbspa --stateful "SELECT region, COUNT(*)
                    FROM 'kafka://broker/orders.cdc' CHANGELOG DEBEZIUM
                    GROUP BY region"
```

## What is saved

A checkpoint contains:

| Field | Description |
|---|---|
| Query fingerprint | SHA-256 hash of the normalized SQL |
| Accumulator state | Serialized group key -> accumulator map |
| Timestamp | When the checkpoint was flushed |

Kafka offsets and the dedup cache are **not** part of the checkpoint. Kafka offset tracking relies on the consumer group commit (if `?group=` is configured) or on replaying from the configured `?offset=` on restart.

## Flush cadence

Checkpoints are flushed on a time-based interval: every `--checkpoint-interval` (default 5 seconds).

The flush is atomic: write to a temp file, `fsync`, then rename. A crash mid-flush never corrupts the checkpoint — the previous checkpoint remains intact.

## Restart behavior

On restart with `--stateful`:

1. Look for an existing checkpoint matching the query fingerprint.
2. If found: restore accumulator state. Kafka resumes from the consumer group's committed offset (or the configured `?offset=` parameter if no group is set).
3. If not found or fingerprint mismatch: start fresh.

Records between the checkpoint timestamp and the current stream position are replayed through the accumulator, which re-derives the correct state since accumulators are idempotent.

!!! warning
    If you change the SQL query, the fingerprint changes and the old checkpoint is ignored. Use `dbspa state reset <hash>` to explicitly delete old checkpoints.

## Delivery semantics

Delivery semantics depend on the sink:

| Sink | Semantics |
|---|---|
| stdout | at-least-once |
| HTTP (`serve`) | at-least-once |
| SQLite (`--stateful`) | **exactly-once** (output state) |

### stdout and HTTP: at-least-once

The failure window:

1. Records are processed and the in-memory accumulator is updated.
2. Output is emitted to the sink.
3. On checkpoint flush: accumulator state is written to disk.

**Crash between step 2 and 3:** Output was emitted but checkpoint was not saved. On restart, the accumulator is restored from the previous checkpoint and records are replayed, producing duplicate output lines. There is no way to un-emit what was already written to stdout or HTTP.

Use `DEDUPLICATE BY` to prevent duplicate *input* records from reaching the accumulator when Kafka redelivers messages:

```sql
SELECT status, COUNT(*) AS orders
FROM 'kafka://broker/orders.cdc' CHANGELOG DEBEZIUM
GROUP BY status
DEDUPLICATE BY $source.gtid WITHIN '10 minutes'
```

### SQLite: exactly-once output state

When `--stateful` is set, DBSPA maintains accumulated results in a SQLite database via UPSERT. After any crash and recovery, the SQLite state converges to the correct value — identical to what it would have been with no crash.

This works because recovery restores the accumulator to its checkpointed state and replays records from the checkpointed offset. The accumulator re-derives the correct final state, and the UPSERT overwrites whatever SQLite had with that correct value. The UPSERT (last-write-wins) is what makes this safe: there is no way to produce a wrong SQLite row that persists, because the next successful recovery will always overwrite it.

This is the same guarantee as Kafka's transactional EOS, achieved via idempotent writes and checkpoint restore rather than atomic transactions.

The caveat: "exactly-once output state" — the converged SQLite rows are always correct. A reader observing SQLite mid-recovery may briefly see a stale value, but the final state is exact.

## Disk full handling

If the disk is full during checkpoint write, the write fails and the previous checkpoint remains intact. DBSPA logs an error and retries on the next interval.

## State directory

Default: `~/.dbspa/state/<query-hash>/`

Override: `--state-dir /path/to/dir`

## Inspection commands

```bash
# List all checkpointed queries
dbspa state list

# Delete a checkpoint (next run replays from scratch)
dbspa state reset a1b2c3d4
```

!!! note
    `dbspa state inspect` is not yet implemented.

## Interaction with SQLite state

When both `--stateful` and `--state file.db` are used, the SQLite UPSERT and the checkpoint flush are **not** in the same transaction. A crash between them can produce a SQLite state slightly ahead of or behind the checkpoint. On restart, replayed records re-UPSERT into SQLite, converging to the correct state (UPSERT is idempotent for accumulating queries).

## Disk-backed arrangements

When `--spill-to-disk` or `--max-memory` is set, join arrangements spill to disk using [Badger](https://github.com/dgraph-io/badger) (a pure Go LSM-tree KV store). The arrangement data is stored separately from checkpoints -- it lives in a temporary directory and is rebuilt on restart from the checkpoint state. Stream-stream joins auto-enable spill-to-disk.

Disk-backed arrangements prevent OOM for large joins or long `WITHIN INTERVAL` windows. See [Performance: Disk-Backed Arrangements](performance.md#disk-backed-arrangements) for overhead benchmarks.

## Interaction with SEED FROM

If a checkpoint exists, it takes precedence over `SEED FROM`. The seed is a cold-start fallback — it is only used when no checkpoint matches the query fingerprint.
