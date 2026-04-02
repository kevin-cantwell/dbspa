# Checkpointing

Checkpointing allows FoldDB to survive restarts without replaying the entire stream from the beginning.

## Enabling checkpoints

```bash
folddb --stateful "SELECT region, COUNT(*)
                    FROM 'kafka://broker/orders.cdc' FORMAT DEBEZIUM
                    GROUP BY region"
```

## What is saved

A checkpoint contains:

| Field | Description |
|---|---|
| Query fingerprint | SHA-256 hash of the normalized SQL |
| Accumulator state | Serialized group key -> accumulator map |
| Kafka offsets | Per-partition last processed offset |
| Dedup cache | If `DEDUPLICATE BY` is active |
| Timestamp | When the checkpoint was flushed |

## Flush cadence

Checkpoints are flushed every `--checkpoint-interval` (default 5 seconds) or every 10,000 records, whichever comes first.

The flush is atomic: write to a temp file, `fsync`, then rename. A crash mid-flush never corrupts the checkpoint — the previous checkpoint remains intact.

## Restart behavior

On restart with `--stateful`:

1. Look for an existing checkpoint matching the query fingerprint.
2. If found: restore accumulator state, resume from saved Kafka offsets.
3. If not found or fingerprint mismatch: start fresh.

The gap between the checkpoint and the current stream position is replayed (typically seconds of data).

!!! warning
    If you change the SQL query, the fingerprint changes and the old checkpoint is ignored. Use `folddb state reset <hash>` to explicitly delete old checkpoints.

## Delivery semantics

FoldDB provides **at-least-once** processing, not exactly-once.

The failure window:

1. Records are processed and the in-memory accumulator is updated.
2. Output is emitted to the sink.
3. On checkpoint flush: accumulator state + offsets are written to disk.
4. After successful flush: offsets are committed to Kafka (if consumer group is set).

**Crash between step 2 and 3:** Output was emitted but checkpoint was not saved. On restart, records are replayed, producing duplicate output. For changelog consumers that maintain a key-value map, the duplicates are idempotent — the map converges to the correct state.

**Crash between step 3 and 4:** Checkpoint is saved but Kafka offsets are not committed. FoldDB resumes from the locally checkpointed offsets, not the Kafka-committed ones.

## Disk full handling

If the disk is full during checkpoint write, the write fails and the previous checkpoint remains intact. FoldDB logs an error and retries on the next interval. If the disk remains full for 3 consecutive intervals, FoldDB exits with:

```
Error: disk full — unable to write checkpoint for 3 consecutive intervals.
```

## State directory

Default: `~/.folddb/state/<query-hash>/`

Override: `--state-dir /path/to/dir`

## Inspection commands

```bash
# List all checkpointed queries
folddb state list

# Inspect a specific checkpoint
folddb state inspect a1b2c3d4

# Delete a checkpoint (next run replays from scratch)
folddb state reset a1b2c3d4
```

Example `inspect` output:

```
Query:       SELECT region, COUNT(*) FROM 'kafka://broker/orders.cdc' ...
Hash:        a1b2c3d4
Offsets:     partition 0 = 48291042, partition 1 = 47103821
Keys:        48
State size:  12 KB
Last flush:  2026-03-28T14:02:31Z
```

## Interaction with SQLite state

When both `--stateful` and `--state file.db` are used, the SQLite UPSERT and the checkpoint flush are **not** in the same transaction. A crash between them can produce a SQLite state slightly ahead of or behind the checkpoint. On restart, replayed records re-UPSERT into SQLite, converging to the correct state (UPSERT is idempotent for accumulating queries).

## Disk-backed arrangements

When `--spill-to-disk` or `--max-memory` is set, join arrangements spill to disk using [Badger](https://github.com/dgraph-io/badger) (a pure Go LSM-tree KV store). The arrangement data is stored separately from checkpoints -- it lives in a temporary directory and is rebuilt on restart from the checkpoint state. Stream-stream joins auto-enable spill-to-disk.

Disk-backed arrangements prevent OOM for large joins or long `WITHIN INTERVAL` windows. See [Performance: Disk-Backed Arrangements](performance.md#disk-backed-arrangements) for overhead benchmarks.

## Interaction with SEED FROM

If a checkpoint exists, it takes precedence over `SEED FROM`. The seed is a cold-start fallback — it is only used when no checkpoint matches the query fingerprint.
