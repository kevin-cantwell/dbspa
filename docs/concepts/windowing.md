# Windowing

Windowed aggregation groups records into time-based buckets. DBSPA supports three window types: tumbling, sliding, and session.

## Tumbling windows

Non-overlapping, fixed-size windows. Each record belongs to exactly one window.

```sql
SELECT window_start, window_end, endpoint, COUNT(*) AS reqs
FROM 'kafka://broker/api_requests'
GROUP BY endpoint
WINDOW TUMBLING '1 minute'
EVENT TIME BY timestamp
```

Windows are aligned to the epoch. A `TUMBLING '1 hour'` window produces `[00:00, 01:00)`, `[01:00, 02:00)`, etc. in UTC. Boundaries are half-open: `[start, end)`.

## Sliding windows

Overlapping, fixed-size windows with a fixed slide interval. A record may belong to multiple windows.

```sql
SELECT window_start, endpoint, COUNT(*) AS reqs, AVG(latency_ms) AS avg_lat
FROM 'kafka://broker/api_requests'
GROUP BY endpoint
WINDOW SLIDING '10 minutes' BY '5 minutes'
EVENT TIME BY timestamp
```

This creates 10-minute windows that advance every 5 minutes. Each record falls into two adjacent windows.

## Session windows

Activity-based windows with variable size. A session ends when no record arrives within the gap duration.

```sql
SELECT window_start, window_end, user_id, COUNT(*) AS events
FROM 'kafka://broker/clicks'
GROUP BY user_id
WINDOW SESSION '5 minutes'
EVENT TIME BY event_time
```

!!! warning
    Session window merging (combining two existing sessions when a record bridges the gap between them) is not yet implemented. Each record currently creates an independent `[ts, ts+gap)` window rather than extending or merging existing sessions. Use tumbling or sliding windows for production aggregation.

!!! note
    The session gap boundary is exclusive: `gap > timeout` starts a new session, `gap <= timeout` extends the current session.

## Window lifecycle

1. **Open** — a window is created when its first record arrives.
2. **Active** — records are accumulated. If `EMIT EARLY` is configured, partial results are emitted periodically.
3. **Close** — the watermark advances past `window_end`. Final results are emitted and state is discarded.
4. **Late arrival** — a record whose event time falls in a closed window is dropped.

## Implicit columns

Windowed queries expose two implicit columns:

| Column | Type | Description |
|---|---|---|
| `window_start` | TEXT (RFC3339) | Start of the window (inclusive) |
| `window_end` | TEXT (RFC3339) | End of the window (exclusive) |

These can be used in SELECT and ORDER BY.

## Event time vs processing time

By default, windows use **processing time** (when DBSPA receives the record). To use a field from the data:

```sql
EVENT TIME BY timestamp
```

The event time column is parsed as ISO 8601, or as a Unix epoch (seconds since epoch, integer or float). Timestamps without a timezone offset are assumed UTC.

## Watermarks

The watermark tracks how far event time has progressed. Records arriving behind the watermark by more than the allowed lateness are dropped.

```sql
WATERMARK '30 seconds'
```

If `EVENT TIME BY` is specified without `WATERMARK`, the default lateness is 5 seconds.

For multi-partition Kafka sources, DBSPA tracks the minimum watermark across all assigned partitions. A stalled partition (no records for 30+ seconds) is excluded from the minimum to prevent watermark stalls.

## Emit control

By default, windowed queries emit results only when the window closes (`EMIT FINAL`). Use `EMIT EARLY` for periodic partial results:

```sql
EMIT EARLY '10 seconds'
```

Early emissions are retraction+insertion pairs for the current partial state of each group in each open window. At window close, a final retraction of the last early emission is followed by the final result.

!!! tip
    `EMIT EARLY` with many group keys produces high output volume. For TUI mode this just means more frequent redraws. For changelog mode, be prepared for retraction noise.

## Non-windowed aggregation

Queries without `WINDOW` aggregate over the entire stream (running/cumulative totals):

```sql
SELECT region, COUNT(*) AS orders
FROM 'kafka://broker/orders.cdc' CHANGELOG DEBEZIUM
GROUP BY region
```

Each input record that changes a group's result triggers an output diff immediately. There are no window boundaries — the aggregation is unbounded.
