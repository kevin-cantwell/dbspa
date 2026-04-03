#!/usr/bin/env bash
# Usage: ./stress/run.sh [--output FILE] [--scenarios PATTERN] [--duration DUR]
#
# Runs the DBSPA stress test suite.
#
# Examples:
#   ./stress/run.sh
#   ./stress/run.sh --output stress/results/run.json
#   ./stress/run.sh --scenarios "sustained|adversarial" --duration 1m

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Parse arguments
OUTPUT_FILE=""
SCENARIOS=""
DURATION="5m"

while [[ $# -gt 0 ]]; do
    case $1 in
        --output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        --scenarios)
            SCENARIOS="$2"
            shift 2
            ;;
        --duration)
            DURATION="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1" >&2
            echo "Usage: $0 [--output FILE] [--scenarios PATTERN] [--duration DUR]" >&2
            exit 1
            ;;
    esac
done

# Build binaries
echo "=== Building dbspa ==="
cd "$PROJECT_ROOT"
make build
echo ""

DBSPA_BIN="$PROJECT_ROOT/dbspa"

# Build stress harness
echo "=== Building stress harness ==="
cd "$SCRIPT_DIR"
go build -o stress-harness .
echo ""

echo "=== Running stress tests ==="

ARGS=(
    --dbspa "$DBSPA_BIN"
    --duration "$DURATION"
)

if [[ -n "$SCENARIOS" ]]; then
    ARGS+=(--scenarios "$SCENARIOS")
fi

if [[ -n "$OUTPUT_FILE" ]]; then
    if [[ "$OUTPUT_FILE" != /* ]]; then
        OUTPUT_FILE="$PROJECT_ROOT/$OUTPUT_FILE"
    fi
    ARGS+=(--output "$OUTPUT_FILE")
fi

./stress-harness "${ARGS[@]}"
EXIT_CODE=$?

rm -f "$SCRIPT_DIR/stress-harness"
exit $EXIT_CODE
