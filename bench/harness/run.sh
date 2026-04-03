#!/usr/bin/env bash
# Usage: ./bench/harness/run.sh [--with-kafka] [--output results.json] [--compare baseline.json]
#
# Runs the DBSPA benchmark suite and outputs machine-readable JSON results.
# Use --with-kafka to include Kafka integration benchmarks (requires Docker).
#
# Examples:
#   ./bench/harness/run.sh
#   ./bench/harness/run.sh --output bench/harness/results/run.json
#   ./bench/harness/run.sh --with-kafka --output results.json
#   ./bench/harness/run.sh --compare bench/harness/baseline.json

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Parse arguments
WITH_KAFKA=""
OUTPUT_FILE=""
COMPARE_FILE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --with-kafka)
            WITH_KAFKA="--with-kafka"
            shift
            ;;
        --output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        --compare)
            COMPARE_FILE="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1" >&2
            echo "Usage: $0 [--with-kafka] [--output FILE] [--compare BASELINE]" >&2
            exit 1
            ;;
    esac
done

# Step 1: Build binaries
echo "=== Building dbspa + dbspa-gen ==="
cd "$PROJECT_ROOT"
make build
echo ""

DBSPA_BIN="$PROJECT_ROOT/dbspa"
GEN_BIN="$PROJECT_ROOT/dbspa-gen"

# Step 2: Start Kafka if requested
KAFKA_STARTED=false
if [[ -n "$WITH_KAFKA" ]]; then
    echo "=== Starting Kafka (docker-compose) ==="
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d
    echo "Waiting for Kafka to be healthy..."
    timeout=90
    while [ $timeout -gt 0 ]; do
        if docker compose -f "$SCRIPT_DIR/docker-compose.yml" ps kafka 2>/dev/null | grep -q healthy; then
            break
        fi
        sleep 2
        timeout=$((timeout - 2))
    done
    if [ $timeout -le 0 ]; then
        echo "WARN: Kafka did not become healthy in time" >&2
    fi
    echo "Waiting for Schema Registry..."
    sleep 5
    KAFKA_STARTED=true
    echo ""
fi

# Step 3: Build and run the benchmark harness
echo "=== Building benchmark harness ==="
cd "$SCRIPT_DIR"
go build -o bench-harness ./benchmarks.go
echo ""

echo "=== Running benchmarks ==="

BENCH_ARGS=(
    --dbspa "$DBSPA_BIN"
    --dbspa-gen "$GEN_BIN"
)

if [[ -n "$WITH_KAFKA" ]]; then
    BENCH_ARGS+=("$WITH_KAFKA")
fi

if [[ -n "$OUTPUT_FILE" ]]; then
    # Make output path absolute if relative
    if [[ "$OUTPUT_FILE" != /* ]]; then
        OUTPUT_FILE="$PROJECT_ROOT/$OUTPUT_FILE"
    fi
    BENCH_ARGS+=(--output "$OUTPUT_FILE")
fi

# If --compare provided and no explicit compare file, check for baseline
if [[ -n "$COMPARE_FILE" ]]; then
    if [[ "$COMPARE_FILE" != /* ]]; then
        COMPARE_FILE="$PROJECT_ROOT/$COMPARE_FILE"
    fi
    BENCH_ARGS+=(--compare "$COMPARE_FILE")
elif [[ -f "$SCRIPT_DIR/baseline.json" && -z "$COMPARE_FILE" ]]; then
    # Auto-compare against baseline if it exists
    BENCH_ARGS+=(--compare "$SCRIPT_DIR/baseline.json")
fi

./bench-harness "${BENCH_ARGS[@]}"
EXIT_CODE=$?

# Step 4: Tear down Kafka if we started it
if [[ "$KAFKA_STARTED" == "true" ]]; then
    echo ""
    echo "=== Tearing down Kafka ==="
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" down -v --remove-orphans
fi

# Clean up
rm -f "$SCRIPT_DIR/bench-harness"

exit $EXIT_CODE
