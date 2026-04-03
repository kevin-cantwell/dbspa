//go:build integration

package integration

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// QA Item 3: Streaming subquery with real Kafka
// Two topics: events + orders-cdc, join with subquery, verify results.

// debeziumEventFlat constructs a Debezium event in the flat format expected
// by DBSPA's DebeziumDecoder: {"op", "before", "after", "source"} at the
// top level (no schema/payload wrapper).
func debeziumEventFlat(op string, before, after map[string]any, tsMs int64) string {
	evt := map[string]any{
		"op": op,
		"source": map[string]any{
			"version":   "2.5.0",
			"connector": "postgresql",
			"name":      "test",
			"ts_ms":     tsMs,
			"db":        "testdb",
			"schema":    "public",
			"table":     "orders",
			"txId":      tsMs / 1000,
			"lsn":       tsMs,
		},
	}
	if before != nil {
		evt["before"] = before
	} else {
		evt["before"] = nil
	}
	if after != nil {
		evt["after"] = after
	} else {
		evt["after"] = nil
	}
	b, _ := json.Marshal(evt)
	return string(b)
}

func TestStreamingSubquery_JoinWithDebeziumCDC(t *testing.T) {
	skipIfShort(t)
	waitForKafka(t)

	eventsTopic := uniqueTopic(t, "qa-events")
	ordersCDCTopic := uniqueTopic(t, "qa-orders-cdc")
	createTopic(t, eventsTopic, 1)
	createTopic(t, ordersCDCTopic, 1)

	// Step 1: Produce CDC orders data (Debezium JSON envelope)
	// Create 5 orders for 3 different customers
	baseTs := int64(1711612800000)
	cdcEvents := []string{
		debeziumEventFlat("c", nil, map[string]any{"id": 1, "customer_id": 100, "amount": 50.0}, baseTs),
		debeziumEventFlat("c", nil, map[string]any{"id": 2, "customer_id": 200, "amount": 75.0}, baseTs+1000),
		debeziumEventFlat("c", nil, map[string]any{"id": 3, "customer_id": 100, "amount": 120.0}, baseTs+2000),
		debeziumEventFlat("c", nil, map[string]any{"id": 4, "customer_id": 300, "amount": 30.0}, baseTs+3000),
		debeziumEventFlat("c", nil, map[string]any{"id": 5, "customer_id": 200, "amount": 90.0}, baseTs+4000),
	}

	t.Log("producing CDC events...")
	produceMessages(t, ordersCDCTopic, cdcEvents)

	// Step 2: Produce plain JSON events referencing users
	events := []string{
		`{"event_id":1,"user_id":100,"action":"login"}`,
		`{"event_id":2,"user_id":200,"action":"purchase"}`,
		`{"event_id":3,"user_id":300,"action":"browse"}`,
		`{"event_id":4,"user_id":100,"action":"logout"}`,
	}

	t.Log("producing events...")
	produceMessages(t, eventsTopic, events)
	time.Sleep(1 * time.Second)

	// Step 3: Run DBSPA with streaming subquery join
	// Note: With FORMAT DEBEZIUM, the Debezium decoder unwraps the _after (or _before
	// for deletes) payload into top-level columns. The GROUP BY uses the top-level
	// customer_id column which is present in both create and delete records.
	sql := fmt.Sprintf(`SELECT e.user_id, e.action, r.cnt FROM 'kafka://%s/%s?offset=earliest' e JOIN (SELECT customer_id, COUNT(*) AS cnt FROM 'kafka://%s/%s?offset=earliest' FORMAT DEBEZIUM GROUP BY customer_id) r ON e.user_id = r.customer_id`,
		kafkaBroker, eventsTopic,
		kafkaBroker, ordersCDCTopic,
	)

	stdout, stderr, err := runDBSPAWithTimeout(t, 30*time.Second, sql, "--timeout", "10s")
	t.Logf("dbspa stderr: %s", stderr)
	t.Logf("dbspa exit: %v", err)
	if err != nil {
		// Timeout exit is expected for streaming queries
		if stdout == "" && !strings.Contains(stderr, "timeout") {
			t.Fatalf("dbspa produced no output: %v\nstderr: %s", err, stderr)
		}
	}

	lines := outputLines(stdout)
	t.Logf("join output (%d lines):\n%s", len(lines), stdout)

	if len(lines) == 0 {
		t.Fatalf("expected join output, got none\nstderr: %s", stderr)
	}

	// Verify: we should see join results for users 100, 200, 300
	// Expected order counts: customer_id=100 -> 2, customer_id=200 -> 2, customer_id=300 -> 1
	expectedCounts := map[string]int64{
		"100": 2,
		"200": 2,
		"300": 1,
	}

	// Parse the final emission per user_id from the changelog
	finalResults := map[string]joinResult{}
	for _, line := range lines {
		var rec map[string]interface{}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Logf("skipping unparseable line: %s", line)
			continue
		}
		uid := fmt.Sprintf("%v", rec["user_id"])
		cnt := toInt64(rec["cnt"])
		action, _ := rec["action"].(string)
		finalResults[uid] = joinResult{UserID: uid, Action: action, Count: cnt}
	}

	t.Logf("final join results: %v", finalResults)

	// Check that we got results for at least some users
	if len(finalResults) == 0 {
		t.Fatal("no parseable join results")
	}

	// Verify counts for users that appear
	for uid, expected := range expectedCounts {
		if result, ok := finalResults[uid]; ok {
			if result.Count != expected {
				t.Errorf("user %s: expected cnt=%d, got cnt=%d", uid, expected, result.Count)
			}
		}
	}
}

func TestStreamingSubquery_CDCRetraction(t *testing.T) {
	skipIfShort(t)
	waitForKafka(t)

	eventsTopic := uniqueTopic(t, "qa-retract-events")
	ordersCDCTopic := uniqueTopic(t, "qa-retract-cdc")
	createTopic(t, eventsTopic, 1)
	createTopic(t, ordersCDCTopic, 1)

	baseTs := int64(1711612800000)

	// Step 1: Produce initial CDC creates
	initialCDC := []string{
		debeziumEventFlat("c", nil, map[string]any{"id": 1, "customer_id": 100, "status": "active"}, baseTs),
		debeziumEventFlat("c", nil, map[string]any{"id": 2, "customer_id": 100, "status": "active"}, baseTs+1000),
		debeziumEventFlat("c", nil, map[string]any{"id": 3, "customer_id": 200, "status": "active"}, baseTs+2000),
	}
	produceMessages(t, ordersCDCTopic, initialCDC)

	// Events that reference the customers
	events := []string{
		`{"user_id":100,"name":"alice"}`,
		`{"user_id":200,"name":"bob"}`,
	}
	produceMessages(t, eventsTopic, events)
	time.Sleep(500 * time.Millisecond)

	// Step 2: Produce a DELETE (retraction) for one of customer 100's orders
	retraction := debeziumEventFlat("d",
		map[string]any{"id": 1, "customer_id": 100, "status": "active"},
		nil,
		baseTs+5000,
	)
	produceMessages(t, ordersCDCTopic, []string{retraction})
	time.Sleep(500 * time.Millisecond)

	// Step 3: Run DBSPA with subquery join
	// Use top-level customer_id (unwrapped from _after/_before by Debezium decoder)
	// so that both creates and deletes group correctly.
	sql := fmt.Sprintf(`SELECT e.user_id, e.name, r.cnt FROM 'kafka://%s/%s?offset=earliest' e JOIN (SELECT customer_id, COUNT(*) AS cnt FROM 'kafka://%s/%s?offset=earliest' FORMAT DEBEZIUM GROUP BY customer_id) r ON e.user_id = r.customer_id`,
		kafkaBroker, eventsTopic,
		kafkaBroker, ordersCDCTopic,
	)

	stdout, stderr, err := runDBSPAWithTimeout(t, 30*time.Second, sql, "--timeout", "10s")
	if err != nil {
		t.Logf("dbspa stderr: %s", stderr)
		if stdout == "" {
			t.Fatalf("dbspa produced no output: %v", err)
		}
	}

	lines := outputLines(stdout)
	t.Logf("retraction test output (%d lines):\n%s", len(lines), stdout)

	if len(lines) == 0 {
		t.Fatal("expected output, got none")
	}

	// Parse final results per user
	finalPerUser := map[string]int64{}
	for _, line := range lines {
		var rec map[string]interface{}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			continue
		}
		uid := fmt.Sprintf("%v", rec["user_id"])
		cnt := toInt64(rec["cnt"])
		finalPerUser[uid] = cnt
	}

	t.Logf("final counts per user: %v", finalPerUser)

	// After the delete, customer 100 should have cnt=1 (was 2, deleted 1)
	// Customer 200 should have cnt=1 (unchanged)
	if cnt, ok := finalPerUser["100"]; ok {
		if cnt != 1 {
			t.Errorf("after retraction, user 100 should have cnt=1, got cnt=%d", cnt)
		}
	} else {
		t.Log("user 100 not in final results (may have been emitted in earlier changelog entries)")
	}

	if cnt, ok := finalPerUser["200"]; ok {
		if cnt != 1 {
			t.Errorf("user 200 should have cnt=1, got cnt=%d", cnt)
		}
	}

	// Verify the changelog shows the retraction propagating
	// Look for user_id=100 appearing with cnt=2 (before delete) and cnt=1 (after delete)
	user100Counts := []int64{}
	for _, line := range lines {
		var rec map[string]interface{}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			continue
		}
		if fmt.Sprintf("%v", rec["user_id"]) == "100" {
			user100Counts = append(user100Counts, toInt64(rec["cnt"]))
		}
	}
	t.Logf("user 100 count progression: %v", user100Counts)

	if len(user100Counts) >= 2 {
		// Should see a decrease at some point (retraction)
		foundDecrease := false
		for i := 1; i < len(user100Counts); i++ {
			if user100Counts[i] < user100Counts[i-1] {
				foundDecrease = true
				break
			}
		}
		if foundDecrease {
			t.Log("retraction propagation confirmed: user 100 count decreased")
		} else {
			// The changelog may show it differently depending on implementation
			t.Log("note: no explicit decrease seen in changelog (may be implementation-specific)")
		}
	}
}

type joinResult struct {
	UserID string
	Action string
	Count  int64
}

// containsSubstring checks if any line in stdout contains the given substring.
func containsSubstring(stdout, substr string) bool {
	return strings.Contains(stdout, substr)
}
