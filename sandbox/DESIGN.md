# DBSPA Sandbox

A self-contained Docker environment for exploring DBSPA. Clone the repo, run one command, and have a live Kafka cluster with realistic data sources to query against.

## Goals

- Works out of the box: `cd sandbox && docker compose up`
- No configuration required
- Covers every source format DBSPA supports
- Includes both fast and slow streams, CDC, and file-based reference data
- Output (SQLite, HTTP) is queryable without exec-ing into containers
- Discovery is explicit: running the stack tells you what's available and gives you example queries

## Directory layout

```
sandbox/
  docker-compose.yml      # the whole stack
  producers/              # Go or Python producers, one per topic
  seed/                   # static reference files (parquet, csv, json)
  queries/                # pre-written example dbspa queries
  DESIGN.md               # this file
  README.md               # quick start + discovery guide
```

Isolated from the main source tree. The sandbox imports the `dbspa` binary but does not depend on any internal packages.

## Services

### Infrastructure (from existing docker-compose.yml)
- **Kafka** (KRaft, no ZooKeeper) — `localhost:9092`
- **Schema Registry** — `localhost:8081`

### New: source database for CDC
- **Postgres** — `localhost:5432`, with `REPLICA IDENTITY FULL` on all tables
- **Kafka Connect + Debezium** — reads Postgres WAL, writes CDC events to Kafka topics

### New: data producers (containerized, run forever)
- One container per topic family, rate-configurable via env vars

### New: dbspa instances (pre-configured, optional)
- A few persistent `dbspa serve` instances with interesting pre-written queries
- Results exposed via HTTP on distinct ports
- SQLite output files volume-mounted to `sandbox/output/`

### New: Kafdrop (topic browser)
- `localhost:9000` — browse topics, inspect messages, view schemas
- Gives newcomers a way to see raw data before writing queries

## Topics and data sources

### Slow streams (~1 event/sec)
These simulate production-rate IoT or user activity data. Good for watching the TUI update in real time.

| Topic | Format | Description |
|---|---|---|
| `sensors.temperature` | JSON | 10 virtual sensors reporting °C every 5s |
| `users.activity` | JSON | simulated page views, one every 2s |
| `payments.slow` | Avro (registry) | payment events, ~1/sec |

### Fast streams (~10K events/sec)
High-throughput topics for benchmarking and aggregation demos.

| Topic | Format | Description |
|---|---|---|
| `orders.fast` | JSON | high-volume order stream |
| `logs.app` | JSON | structured application logs (level, service, message, latency_ms) |
| `metrics.counters` | Avro (registry) | per-service request counter increments |

### CDC streams (Debezium, from Postgres)
| Topic | Format | Description |
|---|---|---|
| `cdc.customers` | JSON DEBEZIUM | customer table — inserts, updates, deletes |
| `cdc.orders` | JSON DEBEZIUM | orders table — status transitions (pending→shipped→delivered) |
| `cdc.inventory` | AVRO DEBEZIUM | inventory levels — quantity updates |

### Reference files (static, mounted into sandbox)
| File | Format | Description |
|---|---|---|
| `seed/products.parquet` | Parquet | product catalog (product_id, name, category, price) |
| `seed/regions.csv` | CSV | region lookup table (region_id, name, country) |
| `seed/users.json` | NDJSON | user profiles for joining against activity stream |

## Coherent data model

All topics share a consistent fictional domain so JOINs are interesting:

- **customers** (from CDC): customer_id, name, tier (bronze/silver/gold), region_id
- **orders** (fast stream + CDC): order_id, customer_id, product_id, amount, status
- **products** (parquet): product_id, name, category, price
- **regions** (csv): region_id, name, country
- **payments** (slow Avro): payment_id, order_id, amount, method

This enables queries like:
- Revenue by product category (join orders → products)
- Order status funnel by customer tier (join orders → customers)
- Regional sales heatmap (join orders → customers → regions)
- Inventory level changes over time (CDC aggregation)

## Discovery

On startup, the stack prints a discovery message:

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  DBSPA Sandbox ready
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Kafka:           localhost:9092
  Schema Registry: localhost:8081
  Kafdrop:         http://localhost:9000

  Pre-running queries:
    Orders by status (TUI):  http://localhost:8090/
    Revenue by region:       http://localhost:8091/
    Inventory levels (CDC):  http://localhost:8092/

  Reference files (ready to use in queries):
    /sandbox/seed/products.parquet
    /sandbox/seed/regions.csv
    /sandbox/seed/users.json

  SQLite outputs (updated live):
    /sandbox/output/orders_by_status.db
    /sandbox/output/revenue_by_region.db

  Browse raw data: http://localhost:9000

  Example queries → sandbox/queries/
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

## Pre-written example queries

Stored in `sandbox/queries/` as shell scripts with comments:

```
queries/
  01_count_orders_by_status.sh        # simple aggregation
  02_revenue_by_region.sh             # join stream + CSV file
  03_join_orders_products.sh          # stream + parquet
  04_cdc_inventory_levels.sh          # CDC aggregation
  05_customer_tier_funnel.sh          # CDC join + aggregation
  06_sliding_window_latency.sh        # 5-min sliding window on logs
  07_tumbling_window_payments.sh      # 1-min tumbling window
  08_avro_metrics_rollup.sh           # Avro stream aggregation
  09_chained_instances.sh             # CHANGELOG DBSPA pipe
  10_seed_from_parquet.sh             # SEED FROM for cold start
```

Each script has a comment block explaining what it demonstrates and what to expect.

## Knobs

Environment variables for the producers:
- `FAST_STREAM_RATE` — events/sec for fast topics (default: 10000)
- `SLOW_STREAM_RATE` — events/sec for slow topics (default: 1)
- `CDC_MUTATION_RATE` — Postgres updates/sec (default: 5)

## Open questions

1. **CDC setup complexity.** Debezium + Kafka Connect is the heaviest part of this stack (~3 extra services, slow startup, connector registration). Consider making CDC optional via a compose profile (`--profile cdc`) so the base stack is faster to start.

2. **dbspa binary distribution.** The sandbox needs a dbspa binary. Options: build from source in a multi-stage Dockerfile, or download a pre-built release binary. Multi-stage is more hermetic but slower to build. Pre-built requires a published release.

3. **dbspa as compose services vs user-run.** Pre-running dbspa instances are nice for demos but obscure how to actually use the tool. Consider making `queries/` the primary interface and keeping dbspa out of compose, with a note: "these queries are ready to run."

4. **Volume-mounting seed files.** Seed files need to be accessible both inside the dbspa container and from the host for `--input` queries. Use a named volume or bind mount from `sandbox/seed/`.

5. **Windows compatibility.** Shell scripts in `queries/` won't work on Windows without WSL. Consider providing a Makefile with targets that wrap the queries, or ship them as `.bat` files alongside.

6. **Topic retention.** Kafka retention should be long (or infinite) so newcomers can start from `offset=earliest` and replay the full history. This conflicts with disk usage for long-running sandboxes.
