#!/usr/bin/env bash
set -euo pipefail

BROKER="kafka:29092"
MAX_WAIT=60

echo "[seed] Waiting for Kafka broker at $BROKER..."
waited=0
until kafka-topics.sh --bootstrap-server "$BROKER" --list >/dev/null 2>&1; do
    sleep 2
    waited=$((waited + 2))
    if [ "$waited" -ge "$MAX_WAIT" ]; then
        echo "[seed] ERROR: Kafka not ready after ${MAX_WAIT}s"
        exit 1
    fi
done
echo "[seed] Kafka is ready."

# Create topics with 3 partitions each
for topic in test-orders test-orders-cdc test-metrics; do
    echo "[seed] Creating topic: $topic (3 partitions)"
    kafka-topics.sh --bootstrap-server "$BROKER" \
        --create --if-not-exists \
        --topic "$topic" \
        --partitions 3 \
        --replication-factor 1
done

echo "[seed] Publishing test-orders (1000 records)..."
kafka-console-producer.sh --bootstrap-server "$BROKER" \
    --topic test-orders \
    < /testdata/orders.ndjson

echo "[seed] Publishing test-orders-cdc (200 records)..."
kafka-console-producer.sh --bootstrap-server "$BROKER" \
    --topic test-orders-cdc \
    < /testdata/orders_cdc.ndjson

echo "[seed] Publishing test-metrics (500 records)..."
kafka-console-producer.sh --bootstrap-server "$BROKER" \
    --topic test-metrics \
    < /testdata/api_metrics.ndjson

echo "[seed] All topics seeded successfully."
