# Sandbox Implementation Plan

## Architecture decisions

**Binary distribution**: Multi-stage Dockerfile builds `dbspa` from source at `docker compose build` time. A `bin/dbspa` shell wrapper script runs it via `docker run --rm`, so the user needs only Docker — no Go, no local install.

**dbspa NOT in compose**: Users run queries themselves using `./bin/dbspa "..."`. The compose stack is pure infrastructure and data. This keeps the tool visible rather than hidden inside a service.

**CDC as optional profile**: `--profile cdc` adds Postgres + Kafka Connect + Debezium. Base stack starts in ~30s; CDC profile adds ~60s. Base stack is fully useful without it.

**Producers**: Extend the existing `dbspa-gen` binary with new datasets for the sandbox's coherent domain. Run it inside a `producer` container that publishes to Kafka continuously at configured rates.

**Discovery**: A `discover` one-shot service that prints the welcome block to stdout and exits. `docker compose up` scrolls past it on startup; `docker compose run discover` re-prints it on demand.

**Seed files**: Generated once with `dbspa-gen --format parquet` and committed to `sandbox/seed/`. Bind-mounted into the dbspa wrapper container so queries can reference them by path.

**Output**: A `sandbox/output/` directory (gitignored, bind-mounted) receives SQLite files from queries that use `--state`. Users open them with any SQLite browser on the host.

---

## Data model

A fictional e-commerce domain. All topics share foreign keys so JOINs are meaningful.

### Reference tables (static files in `sandbox/seed/`)

| File | Format | Rows | Key fields |
|---|---|---|---|
| `products.parquet` | Parquet | 500 | product_id, name, category, unit_price |
| `regions.csv` | CSV | 20 | region_id, name, country, timezone |
| `customers_snapshot.parquet` | Parquet | 10000 | customer_id, name, tier, region_id — used for SEED FROM demo |

### Live Kafka topics (produced continuously)

| Topic | Format | Rate | Key fields |
|---|---|---|---|
| `orders.placed` | JSON | 200/s | order_id, customer_id, product_id, quantity, amount, region_id, status, ts |
| `orders.status` | JSON | 50/s | order_id, old_status, new_status, ts — lifecycle events only |
| `app.logs` | JSON | 500/s | service, level, endpoint, method, latency_ms, status_code, ts |
| `payments.events` | Avro (registry) | 30/s | payment_id, order_id, amount, method, result, ts |
| `metrics.services` | Avro (registry) | 10/s | service, instance, requests, errors, p99_ms, ts — per-service summaries |

`orders.placed` status field cycles: pending → processing → shipped → delivered (some → cancelled). A new record is emitted at each transition, making it both an event stream and a source of funnel queries.

### CDC topics (--profile cdc only)

| Topic | Format | Mutation rate | Source table |
|---|---|---|---|
| `cdc.customers` | JSON DEBEZIUM | 2/s | customers — tier upgrades, region changes |
| `cdc.inventory` | JSON DEBEZIUM | 5/s | inventory — quantity decrements per order |

A background mutation service runs SQL UPDATE statements on Postgres to drive CDC events.

---

## Directory structure

```
sandbox/
  PLAN.md                       # this file
  DESIGN.md                     # rationale and open questions
  README.md                     # quickstart for users
  docker-compose.yml            # base stack (Kafka, Schema Registry, Kafdrop, producer, discover)
  docker-compose.cdc.yml        # CDC overlay (Postgres, Connect, connector registrar, mutator)
  Makefile                      # up, down, restart, discover, clean
  Dockerfile.dbspa              # multi-stage: builds dbspa from repo root
  Dockerfile.producer           # builds dbspa-gen from repo root
  bin/
    dbspa                       # shell wrapper: docker run --rm dbspa-sandbox ...
  producer/
    entrypoint.sh               # waits for Kafka, registers Avro schemas, starts producers
  connector/
    register.sh                 # POSTs Debezium connector configs to Connect REST API
    customers.json              # connector config for cdc.customers
    inventory.json              # connector config for cdc.inventory
  seed/
    generate.sh                 # script to regenerate seed files (not run at compose up)
    products.parquet            # committed, ~50KB
    regions.csv                 # committed, <1KB
    customers_snapshot.parquet  # committed, ~500KB
  queries/
    01_order_funnel.sh
    02_error_rate_by_service.sh
    03_revenue_by_category.sh
    04_latency_p99_window.sh
    05_orders_enriched_with_region.sh
    06_payment_method_mix.sh
    07_cdc_inventory_levels.sh
    08_cdc_customer_tier_join.sh
    09_sliding_window_order_volume.sh
    10_seed_from_snapshot.sh
    11_deduplicate_payments.sh
    12_chain_two_instances.sh
    13_file_query_products.sh
  output/                       # gitignored, SQLite/CSV outputs land here
```

---

## Implementation phases

### Phase 1 — Scaffold and binary wrapper

**Goal**: `./bin/dbspa --version` works without any local Go install.

1. Write `Dockerfile.dbspa`:
   ```dockerfile
   FROM golang:1.25-alpine AS builder
   WORKDIR /src
   COPY go.mod go.sum ./
   RUN go mod download
   COPY . .
   RUN CGO_ENABLED=0 go build -o /dbspa ./cmd/dbspa

   FROM alpine:3.19
   RUN apk add --no-cache ca-certificates
   COPY --from=builder /dbspa /usr/local/bin/dbspa
   ENTRYPOINT ["dbspa"]
   ```
   Note: `dbspa` uses `modernc.org/sqlite` (pure Go) and `go-duckdb` (requires CGo). Need to check CGo requirements for the multi-stage build. If `go-duckdb` forces CGo, use `golang:1.25` (with gcc) not `alpine` for the builder and ship a dynamically-linked binary inside a `debian:slim` final stage.

2. Write `bin/dbspa`:
   ```bash
   #!/usr/bin/env bash
   SANDBOX_DIR="$(cd "$(dirname "$0")/.." && pwd)"
   exec docker run --rm -it \
     --network sandbox_default \
     -v "$SANDBOX_DIR/seed:/sandbox/seed:ro" \
     -v "$SANDBOX_DIR/output:/sandbox/output" \
     dbspa-sandbox \
     dbspa "$@"
   ```
   Make executable. The `--network sandbox_default` lets dbspa reach `kafka:29092` and `schema-registry:8081` inside compose.

3. Add a `dbspa-sandbox` image build to `sandbox/docker-compose.yml`:
   ```yaml
   services:
     dbspa:
       build:
         context: ..
         dockerfile: sandbox/Dockerfile.dbspa
       image: dbspa-sandbox
       profiles: ["build-only"]   # never starts; just builds the image
   ```
   Running `docker compose build dbspa` pre-builds the image. The wrapper uses it.

4. Test: `docker compose build dbspa && ./bin/dbspa --version`

**Deliverable**: wrapper works, image builds cleanly.

---

### Phase 2 — Base compose stack

**Goal**: `docker compose up` starts Kafka, Schema Registry, Kafdrop, and a silent healthy stack.

1. Write `sandbox/docker-compose.yml` starting from the existing root `docker-compose.yml` (copy Kafka + Schema Registry config, adjust ports if needed).

2. Add Kafdrop:
   ```yaml
   kafdrop:
     image: obsidiandynamics/kafdrop:latest
     depends_on:
       kafka:
         condition: service_healthy
     ports:
       - "9000:9000"
     environment:
       KAFKA_BROKERCONNECT: kafka:29092
       JVM_OPTS: -Xms32M -Xmx64M
   ```

3. Add the `discover` service — a one-shot alpine container that prints the welcome block:
   ```yaml
   discover:
     image: alpine:3.19
     depends_on:
       kafka:
         condition: service_healthy
       schema-registry:
         condition: service_healthy
       kafdrop:
         condition: service_started
     command: /discover.sh
     volumes:
       - ./discover.sh:/discover.sh:ro
     restart: "no"
   ```
   `discover.sh` is a shell heredoc that prints the formatted welcome message and exits 0.

4. Test: `docker compose up -d && docker compose logs discover`

**Deliverable**: stack starts, Kafdrop reachable at localhost:9000, discover message prints.

---

### Phase 3 — Seed files

**Goal**: Parquet and CSV reference files committed to the repo, usable in queries.

1. Extend `dbspa-gen` with new datasets: `products` and `regions`. These are simple static tables — no CDC, no streaming logic needed.

   Add `genProduct(rng, seq)` and `genRegion(rng, seq)` to `cmd/dbspa-gen/datasets.go`.

2. Write `sandbox/seed/generate.sh`:
   ```bash
   #!/usr/bin/env bash
   cd "$(dirname "$0")/.."
   go run ./cmd/dbspa-gen products --count=500 --format=parquet -o sandbox/seed/products.parquet
   go run ./cmd/dbspa-gen regions --count=20 --format=ndjson | ... > sandbox/seed/regions.csv
   go run ./cmd/dbspa-gen customers --count=10000 --format=parquet -o sandbox/seed/customers_snapshot.parquet
   ```
   This script is for re-generation only — not run at compose up. The output files are committed.

3. Run the script, commit the generated files.

4. Test: `./bin/dbspa "SELECT * FROM '/sandbox/seed/products.parquet' LIMIT 5"`

**Deliverable**: seed files accessible from wrapper, queryable.

---

### Phase 4 — Producers

**Goal**: All five live topics produce data continuously. Avro schemas registered in Schema Registry.

1. Write `Dockerfile.producer`:
   ```dockerfile
   FROM golang:1.25 AS builder
   WORKDIR /src
   COPY go.mod go.sum ./
   RUN go mod download
   COPY . .
   RUN go build -o /producer ./cmd/dbspa-gen

   FROM debian:bookworm-slim
   COPY --from=builder /producer /usr/local/bin/dbspa-gen
   COPY sandbox/producer/entrypoint.sh /entrypoint.sh
   RUN chmod +x /entrypoint.sh
   ENTRYPOINT ["/entrypoint.sh"]
   ```

2. Extend `dbspa-gen` with the sandbox datasets. Two new dataset names that produce the coherent domain model fields described above: `sandbox-orders`, `sandbox-logs`, `sandbox-payments`, `sandbox-metrics`.

   Key requirement: `customer_id` and `product_id` in orders must be drawn from the same pool as the reference files so JOINs actually produce results. Use a fixed random seed to keep IDs consistent across the producer and seed files.

3. Write `sandbox/producer/entrypoint.sh`:
   ```bash
   #!/usr/bin/env bash
   set -euo pipefail

   BROKER="${KAFKA_BROKER:-kafka:29092}"
   REGISTRY="${SCHEMA_REGISTRY:-http://schema-registry:8081}"

   # Wait for Kafka
   echo "[producer] Waiting for Kafka..."
   until kafka-topics.sh --bootstrap-server "$BROKER" --list >/dev/null 2>&1; do sleep 2; done

   # Create topics
   for topic in orders.placed orders.status app.logs payments.events metrics.services; do
     kafka-topics.sh --bootstrap-server "$BROKER" \
       --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1
   done

   # Register Avro schemas for payments.events and metrics.services
   curl -s -X POST "$REGISTRY/subjects/payments.events-value/versions" \
     -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     -d @/schemas/payments.json

   curl -s -X POST "$REGISTRY/subjects/metrics.services-value/versions" \
     -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     -d @/schemas/metrics.json

   echo "[producer] Topics created, schemas registered. Starting producers..."

   # Run producers concurrently, rate-controlled
   dbspa-gen sandbox-orders  --count=0 --rate="${ORDERS_RATE:-200}"  --format=ndjson  --kafka="$BROKER" --topic=orders.placed  &
   dbspa-gen sandbox-logs    --count=0 --rate="${LOGS_RATE:-500}"    --format=ndjson  --kafka="$BROKER" --topic=app.logs       &
   dbspa-gen sandbox-payments --count=0 --rate="${PAYMENTS_RATE:-30}" --format=avro   --kafka="$BROKER" --topic=payments.events --registry="$REGISTRY" &
   dbspa-gen sandbox-metrics  --count=0 --rate="${METRICS_RATE:-10}"  --format=avro   --kafka="$BROKER" --topic=metrics.services --registry="$REGISTRY" &
   dbspa-gen sandbox-orders  --count=0 --rate="${STATUS_RATE:-50}"   --format=ndjson  --kafka="$BROKER" --topic=orders.status   &

   wait
   ```

4. Add `producer` service to compose:
   ```yaml
   producer:
     build:
       context: ..
       dockerfile: sandbox/Dockerfile.producer
     depends_on:
       kafka:
         condition: service_healthy
       schema-registry:
         condition: service_healthy
     environment:
       ORDERS_RATE: "200"
       LOGS_RATE: "500"
       PAYMENTS_RATE: "30"
       METRICS_RATE: "10"
     restart: unless-stopped
   ```

5. Test: open Kafdrop at localhost:9000, verify all 5 topics have messages flowing.

**Deliverable**: all topics live, schemas visible in Schema Registry.

---

### Phase 5 — Example queries

**Goal**: 13 runnable query scripts in `sandbox/queries/`, each with comments explaining what it demonstrates.

Each script follows the same structure:
```bash
#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# Query: Order funnel by status
# Demonstrates: GROUP BY aggregation over a live JSON stream
# Expected output: live TUI table updating as orders flow in
# Stop with: Ctrl+C
# ─────────────────────────────────────────────────────────────
cd "$(dirname "$0")/.."

./bin/dbspa "
  SELECT status, COUNT(*) AS orders, SUM(amount) AS revenue
  FROM 'kafka://kafka:29092/orders.placed'
  GROUP BY status
"
```

Full query list with what each one demonstrates:

| Script | Demonstrates |
|---|---|
| `01_order_funnel.sh` | Basic GROUP BY, live JSON stream, TUI |
| `02_error_rate_by_service.sh` | GROUP BY + HAVING, filter on log level |
| `03_revenue_by_category.sh` | Stream-to-Parquet JOIN (orders + products) |
| `04_latency_p99_window.sh` | TUMBLING window, EVENT TIME BY, AVG on logs |
| `05_orders_by_region.sh` | Stream-to-CSV JOIN (orders + regions) |
| `06_payment_method_mix.sh` | Avro stream with schema registry, GROUP BY |
| `07_sliding_order_volume.sh` | SLIDING window, 10-min/1-min advance |
| `08_cdc_inventory.sh` | CDC aggregation (`--profile cdc`) |
| `09_cdc_tier_join.sh` | CDC stream-to-CDC join, customer tier funnel (`--profile cdc`) |
| `10_seed_from_snapshot.sh` | SEED FROM parquet snapshot then continue live |
| `11_dedup_payments.sh` | DEDUPLICATE BY for exactly-once processing |
| `12_chain_instances.sh` | Two dbspa calls piped via `\|`, CHANGELOG DBSPA |
| `13_file_query.sh` | Direct Parquet query (DuckDB path, no Kafka) |
| `14_sqlite_output.sh` | `--state output/orders.db`, shows file updating live |

Query `12_chain_instances.sh` is special — it runs two background processes and pipes them:
```bash
./bin/dbspa "SELECT status, COUNT(*) ... GROUP BY status" | \
./bin/dbspa "SELECT * FROM stdin CHANGELOG DBSPA WHERE status = 'pending'"
```
The wrapper needs `--network sandbox_default` on both invocations.

**Deliverable**: all scripts runnable, each produces meaningful output.

---

### Phase 6 — CDC stack (--profile cdc)

**Goal**: `docker compose --profile cdc up` adds Postgres CDC topics to the mix.

Services to add in `docker-compose.cdc.yml` (used with `-f docker-compose.yml -f docker-compose.cdc.yml`):

```
postgres        — postgres:16-alpine, initialized with schema + REPLICA IDENTITY FULL
connect         — confluentinc/cp-kafka-connect:7.6.0, with Debezium Postgres plugin
registrar       — one-shot alpine that POSTs connector configs via curl on startup
mutator         — runs dbspa-gen sandbox-cdc --rate=5 to drive Postgres UPDATEs
```

**Postgres init** (`sandbox/connector/init.sql`):
```sql
CREATE TABLE customers (
  customer_id TEXT PRIMARY KEY,
  name TEXT, tier TEXT, region_id TEXT, created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE inventory (
  product_id TEXT PRIMARY KEY,
  quantity INT, last_updated TIMESTAMPTZ DEFAULT NOW()
);
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE inventory REPLICA IDENTITY FULL;
-- Seed with data matching the fixed-seed producer pool
COPY customers FROM '/seed/customers_snapshot.csv' CSV HEADER;
COPY inventory FROM '/seed/inventory.csv' CSV HEADER;
```

**Connector configs** (`sandbox/connector/customers.json`, `inventory.json`):
Standard Debezium Postgres connector configs pointing at the local Postgres. Topic prefix: `cdc`.

**Mutator**: a new `dbspa-gen sandbox-cdc-mutations` mode that executes SQL UPDATE statements against Postgres at a configured rate (tier changes, quantity decrements).

**Deliverable**: `cdc.customers` and `cdc.inventory` topics appear in Kafdrop with Debezium envelope. CDC queries 08 and 09 work.

---

### Phase 7 — README and polish

**Goal**: a casual user can clone the repo, read the README, and be running queries in under 5 minutes.

`sandbox/README.md` structure:
1. **Prerequisites**: Docker Desktop (or Docker + Docker Compose v2). That's it.
2. **Quick start** (3 commands):
   ```bash
   cd sandbox
   docker compose build        # ~2 min first time, cached after
   docker compose up -d        # starts in ~30s
   docker compose run discover # prints what's available
   ```
3. **Run your first query**: copy-paste one of the example scripts
4. **Topics at a glance**: table of all topics with format and description
5. **Seed files**: where they are, how to reference them in queries
6. **Rate control**: how to change `ORDERS_RATE` etc. via environment
7. **CDC setup**: `--profile cdc` instructions
8. **Tear down**: `docker compose down -v`

`sandbox/Makefile` wraps the common operations:
```makefile
up:       docker compose up -d && docker compose run discover
down:     docker compose down -v
restart:  docker compose restart producer
discover: docker compose run discover
cdc-up:   docker compose -f docker-compose.yml -f docker-compose.cdc.yml up -d
clean:    docker compose down -v && rm -rf output/*
```

---

## Key implementation notes

### CGo and the dbspa binary

`go-duckdb` requires CGo. The multi-stage Dockerfile must use `golang:1.25` (not Alpine) for the builder and ship the binary in `debian:bookworm-slim`. Build arg `CGO_ENABLED=1`. Alpine can be used if we add `musl-dev` and build with `-musl` tag, but that's fragile. Use Debian.

### Fixed random seed for data coherence

The producer and the seed file generator must use the same `--seed` value when generating customer_id and product_id pools. Otherwise JOIN queries return empty results because the IDs never overlap. Hardcode seed `42` in both places.

### Network name

`docker compose` names the default network `<directory>_default`. If the sandbox lives at `sandbox/`, the network is `sandbox_default`. The `bin/dbspa` wrapper must use this exact name. If users rename the directory, the network name changes and the wrapper breaks. Document this. Alternatively, explicitly name the network in compose:
```yaml
networks:
  default:
    name: dbspa-sandbox
```
This makes the network name stable regardless of directory name. Do this.

### Avro schema registration timing

The producer's `entrypoint.sh` registers schemas before starting producers. The Schema Registry may not be fully ready even after its healthcheck passes. Add a retry loop with `sleep 1` around the curl calls.

### The `--profile build-only` pattern

The `dbspa` service in compose is only there to build the image. Use `profiles: ["build-only"]` so `docker compose up` doesn't try to start it. Users run `docker compose build dbspa` once, then use the wrapper script.

### Windows compatibility

The `bin/dbspa` wrapper is a bash script and won't work in Windows CMD/PowerShell without WSL. Add a note in the README. A `bin/dbspa.bat` wrapper is possible but not worth the complexity for now.

---

## What NOT to include

- **dbspa as a persistent compose service**: obscures the tool, adds complexity
- **A web UI for query authoring**: out of scope, just use the shell
- **Automatic output visualization**: SQLite + any browser is enough
- **Multiple Kafka brokers**: single broker is sufficient for a sandbox, multi-broker setup is integration testing territory
- **Authentication/TLS**: sandbox is local-only, no auth needed
- **Kafka Streams or other competing tools**: this is a DBSPA showcase, not a comparison bench

---

## Open question: dbspa-gen --kafka flag

`dbspa-gen` currently writes to stdout. To produce to Kafka it needs a `--kafka <broker>` and `--topic <name>` flag added. This is the single most important code change required to make the producer work. The entrypoint uses `kafka-console-producer.sh` as a fallback but that loses Avro encoding and schema registration. `dbspa-gen --kafka` is the right solution.
