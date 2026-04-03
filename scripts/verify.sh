#!/usr/bin/env bash
# verify.sh — Generate data, query with dbspa and DuckDB, compare results.
#
# Usage:
#   ./scripts/verify.sh                    # run all verification checks
#   ./scripts/verify.sh --count 100000     # with custom record count
#
# Prerequisites:
#   - duckdb CLI installed (brew install duckdb)
#   - dbspa binary built (make build)

set -euo pipefail

COUNT="${1:-10000}"
SEED=42
TMPDIR=$(mktemp -d)
DBSPA="${DBSPA:-./dbspa}"
DBSPA_GEN="${DBSPA_GEN:-./dbspa-gen}"
DUCKDB="${DUCKDB:-duckdb}"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass=0
fail=0

cleanup() {
  rm -rf "$TMPDIR"
}
trap cleanup EXIT

check() {
  local name="$1"
  local dbspa_result="$2"
  local duckdb_result="$3"

  if [ "$dbspa_result" = "$duckdb_result" ]; then
    echo -e "  ${GREEN}✓${NC} $name"
    pass=$((pass + 1))
  else
    echo -e "  ${RED}✗${NC} $name"
    echo -e "    dbspa: $dbspa_result"
    echo -e "    duckdb: $duckdb_result"
    fail=$((fail + 1))
  fi
}

# Check prerequisites
if ! command -v "$DUCKDB" &>/dev/null; then
  echo -e "${RED}Error: duckdb not found. Install with: brew install duckdb${NC}"
  exit 1
fi

if [ ! -f "$DBSPA" ]; then
  echo "Building dbspa..."
  go build -o "$DBSPA" ./cmd/dbspa
fi

if [ ! -f "$DBSPA_GEN" ]; then
  echo "Building dbspa-gen..."
  go build -o "$DBSPA_GEN" ./cmd/dbspa-gen
fi

echo -e "${YELLOW}=== DBSPA vs DuckDB Verification ===${NC}"
echo "Records: $COUNT  Seed: $SEED"
echo ""

# ─────────────────────────────────────────────────────
# 1. Generate plain orders fixture
# ─────────────────────────────────────────────────────
echo "Generating $COUNT orders..."
"$DBSPA_GEN" orders --count "$COUNT" --seed "$SEED" > "$TMPDIR/orders.ndjson"
echo ""

echo -e "${YELLOW}--- Plain Orders Tests ---${NC}"

# Test 1: COUNT(*)
fdb=$(cat "$TMPDIR/orders.ndjson" | "$DBSPA" "SELECT COUNT(*) AS cnt" 2>/dev/null | grep '"_weight":1' | tail -1 | python3 -c "import sys,json; print(json.loads(sys.stdin.read())['cnt'])")
ddb=$("$DUCKDB" -noheader -csv -c "SELECT COUNT(*) FROM read_ndjson_auto('$TMPDIR/orders.ndjson')")
check "COUNT(*)" "$fdb" "$ddb"

# Test 2: COUNT per status
fdb=$(cat "$TMPDIR/orders.ndjson" | "$DBSPA" "SELECT status, COUNT(*) AS cnt GROUP BY status" 2>/dev/null | grep '"_weight":1' | python3 -c "
import sys, json
state = {}
for line in sys.stdin:
    obj = json.loads(line)
    if obj['_weight'] == 1: state[obj['status']] = obj['cnt']
    elif obj['_weight'] == -1 and obj['status'] in state: del state[obj['status']]
for k in sorted(state): print(f'{k}:{state[k]}')
")

ddb=$("$DUCKDB" -noheader -csv -c "SELECT status, COUNT(*) FROM read_ndjson_auto('$TMPDIR/orders.ndjson') GROUP BY status ORDER BY status" | while IFS=, read -r s c; do echo "$s:$c"; done)
check "COUNT per status" "$fdb" "$ddb"

# Test 3: SUM(total) per region
fdb=$(cat "$TMPDIR/orders.ndjson" | "$DBSPA" "SELECT region, SUM(total) AS revenue GROUP BY region" 2>/dev/null | grep '"_weight":1' | python3 -c "
import sys, json
state = {}
for line in sys.stdin:
    obj = json.loads(line)
    if obj['_weight'] == 1: state[obj['region']] = round(obj['revenue'], 2)
    elif obj['_weight'] == -1 and obj['region'] in state: del state[obj['region']]
for k in sorted(state): print(f'{k}:{state[k]}')
")

ddb=$("$DUCKDB" -noheader -csv -c "SELECT region, ROUND(SUM(total), 2) FROM read_ndjson_auto('$TMPDIR/orders.ndjson') GROUP BY region ORDER BY region" | while IFS=, read -r r v; do echo "$r:$v"; done)
check "SUM(total) per region" "$fdb" "$ddb"

# Test 4: AVG(total) overall
fdb=$(cat "$TMPDIR/orders.ndjson" | "$DBSPA" "SELECT AVG(total) AS avg_total" 2>/dev/null | grep '"_weight":1' | tail -1 | python3 -c "import sys,json; print(round(json.loads(sys.stdin.read())['avg_total'], 4))")
ddb=$("$DUCKDB" -noheader -csv -c "SELECT ROUND(AVG(total), 4) FROM read_ndjson_auto('$TMPDIR/orders.ndjson')")
check "AVG(total)" "$fdb" "$ddb"

# Test 5: MIN/MAX
fdb_min=$(cat "$TMPDIR/orders.ndjson" | "$DBSPA" "SELECT MIN(total) AS min_total" 2>/dev/null | grep '"_weight":1' | tail -1 | python3 -c "import sys,json; print(json.loads(sys.stdin.read())['min_total'])")
fdb_max=$(cat "$TMPDIR/orders.ndjson" | "$DBSPA" "SELECT MAX(total) AS max_total" 2>/dev/null | grep '"_weight":1' | tail -1 | python3 -c "import sys,json; print(json.loads(sys.stdin.read())['max_total'])")
ddb_min=$("$DUCKDB" -noheader -csv -c "SELECT MIN(total) FROM read_ndjson_auto('$TMPDIR/orders.ndjson')")
ddb_max=$("$DUCKDB" -noheader -csv -c "SELECT MAX(total) FROM read_ndjson_auto('$TMPDIR/orders.ndjson')")
check "MIN(total)" "$fdb_min" "$ddb_min"
check "MAX(total)" "$fdb_max" "$ddb_max"

# Test 6: WHERE filter + COUNT
fdb=$(cat "$TMPDIR/orders.ndjson" | "$DBSPA" "SELECT COUNT(*) AS cnt WHERE status = 'pending'" 2>/dev/null | grep '"_weight":1' | tail -1 | python3 -c "import sys,json; print(json.loads(sys.stdin.read())['cnt'])")
ddb=$("$DUCKDB" -noheader -csv -c "SELECT COUNT(*) FROM read_ndjson_auto('$TMPDIR/orders.ndjson') WHERE status = 'pending'")
check "WHERE + COUNT" "$fdb" "$ddb"

# Test 7: GROUP BY + HAVING
fdb=$(cat "$TMPDIR/orders.ndjson" | "$DBSPA" "SELECT region, COUNT(*) AS cnt GROUP BY region HAVING COUNT(*) > $(( COUNT / 10 ))" 2>/dev/null | grep '"_weight":1' | python3 -c "
import sys, json
state = {}
for line in sys.stdin:
    obj = json.loads(line)
    if obj['_weight'] == 1: state[obj['region']] = obj['cnt']
    elif obj['_weight'] == -1 and obj['region'] in state: del state[obj['region']]
for k in sorted(state): print(f'{k}:{state[k]}')
")
ddb=$("$DUCKDB" -noheader -csv -c "SELECT region, COUNT(*) FROM read_ndjson_auto('$TMPDIR/orders.ndjson') GROUP BY region HAVING COUNT(*) > $(( COUNT / 10 )) ORDER BY region" | while IFS=, read -r r c; do echo "$r:$c"; done)
check "GROUP BY + HAVING" "$fdb" "$ddb"

# ─────────────────────────────────────────────────────
# 2. Generate CDC fixture and test retraction math
# ─────────────────────────────────────────────────────
echo ""
echo "Generating CDC events..."
"$DBSPA_GEN" orders-cdc --count "$COUNT" --seed "$SEED" > "$TMPDIR/orders_cdc.ndjson"
echo ""

echo -e "${YELLOW}--- CDC Retraction Tests ---${NC}"

# For CDC, dbspa applies retractions via Debezium format.
# DuckDB needs to replay the CDC logic manually.

# Test 8: Count creates
fdb=$(cat "$TMPDIR/orders_cdc.ndjson" | "$DBSPA" "SELECT COUNT(*) AS creates WHERE _op = 'c' FORMAT DEBEZIUM" 2>/dev/null | grep '"_weight":1' | tail -1 | python3 -c "import sys,json; print(json.loads(sys.stdin.read())['creates'])")
ddb=$("$DUCKDB" -noheader -csv -c "SELECT COUNT(*) FROM read_ndjson_auto('$TMPDIR/orders_cdc.ndjson') WHERE op = 'c'")
check "CDC create count" "$fdb" "$ddb"

# Test 9: Count of net active orders (creates minus deletes)
# dbspa with Debezium handles retractions — COUNT(*) reflects net state.
# DuckDB needs manual CDC replay: creates add, deletes subtract.
fdb=$(cat "$TMPDIR/orders_cdc.ndjson" | "$DBSPA" "SELECT COUNT(*) AS active FORMAT DEBEZIUM" 2>/dev/null | grep '"_weight":1' | tail -1 | python3 -c "import sys,json; print(json.loads(sys.stdin.read())['active'])")
ddb=$("$DUCKDB" -noheader -csv -c "
  SELECT SUM(CASE WHEN op = 'c' THEN 1 WHEN op = 'd' THEN -1 ELSE 0 END)
  FROM read_ndjson_auto('$TMPDIR/orders_cdc.ndjson')
")
check "CDC net active orders (create - delete)" "$fdb" "$ddb"

# ─────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────
echo ""
echo -e "${YELLOW}=== Results ===${NC}"
echo -e "  ${GREEN}Passed: $pass${NC}"
if [ "$fail" -gt 0 ]; then
  echo -e "  ${RED}Failed: $fail${NC}"
  exit 1
else
  echo -e "  ${GREEN}All checks passed!${NC}"
fi
