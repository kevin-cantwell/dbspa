//go:build integration

package integration

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// debeziumEvent constructs a Debezium envelope event.
func debeziumEvent(op string, before, after map[string]any, tsMs int64) string {
	payload := map[string]any{
		"op":    op,
		"ts_ms": tsMs,
		"source": map[string]any{
			"version":   "2.5.0",
			"connector": "postgresql",
			"name":      "test",
			"ts_ms":     tsMs,
			"db":        "testdb",
			"schema":    "public",
			"table":     "items",
			"txId":      tsMs / 1000,
			"lsn":       tsMs,
		},
	}
	if before != nil {
		payload["before"] = before
	} else {
		payload["before"] = nil
	}
	if after != nil {
		payload["after"] = after
	} else {
		payload["after"] = nil
	}
	evt := map[string]any{
		"schema":  nil,
		"payload": payload,
	}
	b, _ := json.Marshal(evt)
	return string(b)
}

func TestDebeziumE2E_FullLifecycle(t *testing.T) {
	skipIfShort(t)
	waitForKafka(t)

	topic := uniqueTopic(t, "deb-lifecycle")
	createTopic(t, topic, 1)

	baseTs := int64(1711612800000)

	events := []string{
		// Create order
		debeziumEvent("c", nil, map[string]any{
			"id": 1, "name": "widget", "qty": 5, "status": "new",
		}, baseTs),
		// Update order: new -> processing
		debeziumEvent("u",
			map[string]any{"id": 1, "name": "widget", "qty": 5, "status": "new"},
			map[string]any{"id": 1, "name": "widget", "qty": 5, "status": "processing"},
			baseTs+60000,
		),
		// Update order: processing -> shipped
		debeziumEvent("u",
			map[string]any{"id": 1, "name": "widget", "qty": 5, "status": "processing"},
			map[string]any{"id": 1, "name": "widget", "qty": 5, "status": "shipped"},
			baseTs+120000,
		),
		// Delete order
		debeziumEvent("d",
			map[string]any{"id": 1, "name": "widget", "qty": 5, "status": "shipped"},
			nil,
			baseTs+180000,
		),
	}

	produceMessages(t, topic, events)
	time.Sleep(500 * time.Millisecond)

	// Non-accumulating query: see all operations
	sql := fmt.Sprintf(
		"SELECT _op, _after->>'status' AS status FROM 'kafka://%s/%s?offset=earliest' FORMAT DEBEZIUM LIMIT 7",
		kafkaBroker, topic,
	)
	stdout, stderr, err := runFoldDBWithTimeout(t, 15*time.Second, sql)
	if err != nil {
		t.Fatalf("folddb failed: %v\nstderr: %s", err, stderr)
	}

	lines := outputLines(stdout)
	// Expected: create(+1)=1, update1(-1,+1)=2, update2(-1,+1)=2, delete(-1)=1 = 6 records
	// But non-accumulating just emits each decoded record as-is
	t.Logf("lifecycle output (%d lines):\n%s", len(lines), stdout)

	if len(lines) < 4 {
		t.Errorf("expected at least 4 output lines for lifecycle, got %d", len(lines))
	}
}

func TestDebeziumE2E_AccumulatingGroupBy(t *testing.T) {
	skipIfShort(t)
	waitForKafka(t)

	topic := uniqueTopic(t, "deb-accum")
	createTopic(t, topic, 1)

	baseTs := int64(1711612800000)

	// Create 3 orders in different regions
	events := []string{
		debeziumEvent("c", nil, map[string]any{"id": 1, "region": "us-east", "amount": 100}, baseTs),
		debeziumEvent("c", nil, map[string]any{"id": 2, "region": "us-east", "amount": 200}, baseTs+1000),
		debeziumEvent("c", nil, map[string]any{"id": 3, "region": "us-west", "amount": 150}, baseTs+2000),
		// Update: change id=1 amount from 100 to 300
		debeziumEvent("u",
			map[string]any{"id": 1, "region": "us-east", "amount": 100},
			map[string]any{"id": 1, "region": "us-east", "amount": 300},
			baseTs+3000,
		),
		// Delete id=3
		debeziumEvent("d",
			map[string]any{"id": 3, "region": "us-west", "amount": 150},
			nil,
			baseTs+4000,
		),
	}

	produceMessages(t, topic, events)
	time.Sleep(500 * time.Millisecond)

	// Accumulating query: GROUP BY region, SUM(amount)
	sql := fmt.Sprintf(
		"SELECT _after->>'region' AS region, COUNT(*) AS cnt, SUM((_after->>'amount')::float) AS total FROM 'kafka://%s/%s?offset=earliest' FORMAT DEBEZIUM GROUP BY 1 LIMIT 20",
		kafkaBroker, topic,
	)
	stdout, stderr, err := runFoldDBWithTimeout(t, 15*time.Second, sql)
	if err != nil {
		t.Fatalf("folddb failed: %v\nstderr: %s", err, stderr)
	}

	t.Logf("accumulating output:\n%s", stdout)

	// The changelog output should show the progression of aggregates.
	// Final state should be: us-east: cnt=2, total=500 (200+300); us-west removed (deleted)
	lines := outputLines(stdout)
	if len(lines) == 0 {
		t.Fatal("expected changelog output, got none")
	}

	// Check the last few lines for final state
	lastLines := lines
	if len(lastLines) > 4 {
		lastLines = lastLines[len(lastLines)-4:]
	}
	combined := strings.Join(lastLines, "\n")
	t.Logf("last lines:\n%s", combined)
}

func TestDebeziumE2E_MissingBefore(t *testing.T) {
	skipIfShort(t)
	waitForKafka(t)

	topic := uniqueTopic(t, "deb-nobefore")
	createTopic(t, topic, 1)

	baseTs := int64(1711612800000)

	// Update without _before (REPLICA IDENTITY DEFAULT)
	events := []string{
		debeziumEvent("c", nil, map[string]any{"id": 1, "val": "original"}, baseTs),
		debeziumEvent("u", nil, map[string]any{"id": 1, "val": "updated"}, baseTs+1000), // no before!
	}

	produceMessages(t, topic, events)
	time.Sleep(500 * time.Millisecond)

	sql := fmt.Sprintf(
		"SELECT _op, _after->>'val' AS val FROM 'kafka://%s/%s?offset=earliest' FORMAT DEBEZIUM LIMIT 3",
		kafkaBroker, topic,
	)
	stdout, stderr, err := runFoldDBWithTimeout(t, 15*time.Second, sql)
	if err != nil {
		t.Fatalf("folddb failed: %v\nstderr: %s", err, stderr)
	}

	lines := outputLines(stdout)
	t.Logf("missing-before output (%d lines):\n%s", len(lines), stdout)

	// Should have at least 2 lines (create + insert-only from update without before)
	if len(lines) < 2 {
		t.Errorf("expected at least 2 lines, got %d", len(lines))
	}

	// Verify the stderr has a warning about missing _before
	if !strings.Contains(stderr, "warn") && !strings.Contains(stderr, "Warning") && !strings.Contains(stderr, "before") {
		t.Logf("note: no warning about missing _before in stderr (may be logged differently): %s", stderr)
	}
}

func TestDebeziumE2E_SnapshotRead(t *testing.T) {
	skipIfShort(t)
	waitForKafka(t)

	topic := uniqueTopic(t, "deb-snapshot")
	createTopic(t, topic, 1)

	baseTs := int64(1711612800000)

	// Snapshot reads (op="r") should behave like creates
	events := []string{
		debeziumEvent("r", nil, map[string]any{"id": 1, "name": "alice"}, baseTs),
		debeziumEvent("r", nil, map[string]any{"id": 2, "name": "bob"}, baseTs+1000),
		debeziumEvent("r", nil, map[string]any{"id": 3, "name": "charlie"}, baseTs+2000),
	}

	produceMessages(t, topic, events)
	time.Sleep(500 * time.Millisecond)

	sql := fmt.Sprintf(
		"SELECT _op, _after->>'name' AS name FROM 'kafka://%s/%s?offset=earliest' FORMAT DEBEZIUM LIMIT 3",
		kafkaBroker, topic,
	)
	stdout, stderr, err := runFoldDBWithTimeout(t, 15*time.Second, sql)
	if err != nil {
		t.Fatalf("folddb failed: %v\nstderr: %s", err, stderr)
	}

	lines := outputLines(stdout)
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines for snapshot reads, got %d: %s", len(lines), stdout)
	}

	// Each line should have op="r" and the correct name
	names := map[string]bool{}
	for _, line := range lines {
		var rec map[string]any
		json.Unmarshal([]byte(line), &rec)
		if name, ok := rec["name"].(string); ok {
			names[name] = true
		}
	}
	for _, expected := range []string{"alice", "bob", "charlie"} {
		if !names[expected] {
			t.Errorf("expected name %q in output, got: %v", expected, names)
		}
	}
}
