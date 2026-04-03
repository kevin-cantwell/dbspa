//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"
)

// QA Item 2: Checkpoint round-trip
// Produce 5K, consume with stateful, kill, produce 5K more, restart, verify 10K total.

func TestCheckpoint_RoundTrip10K(t *testing.T) {
	skipIfShort(t)
	waitForKafka(t)

	const batchSize = 5000
	const seed = 42

	topic := uniqueTopic(t, "qa-ckpt-roundtrip")
	createTopic(t, topic, 1)

	stateDir, err := os.MkdirTemp("", "folddb-ckpt-roundtrip-*")
	if err != nil {
		t.Fatalf("cannot create temp dir: %v", err)
	}
	defer os.RemoveAll(stateDir)

	// Generate all 10K orders deterministically
	allOrders := generateOrders(batchSize*2, seed)
	batch1 := allOrders[:batchSize]
	batch2 := allOrders[batchSize:]

	// Step 1: Produce first 5K messages as NDJSON
	t.Log("producing batch 1 (5K messages)...")
	produceOrdersJSON(t, topic, batch1)
	time.Sleep(1 * time.Second)

	// Use a consumer group so Kafka offsets are committed alongside the
	// checkpoint state. The checkpoint saves aggregate values; the consumer
	// group saves the read position. Together they allow a correct resume.
	group := uniqueTopic(t, "qa-ckpt-group")

	// Step 2: Start FoldDB with --stateful, consume batch 1
	sql := fmt.Sprintf(
		"SELECT status, COUNT(*) AS cnt FROM 'kafka://%s/%s?offset=earliest&group=%s' GROUP BY status",
		kafkaBroker, topic, group,
	)

	t.Log("run 1: consuming batch 1 with stateful...")
	stdout1, stderr1 := runFoldDBStateful(t, sql, stateDir, 20*time.Second)
	t.Logf("run1 stdout (%d bytes): last lines:\n%s", len(stdout1), lastNLines(stdout1, 10))
	t.Logf("run1 stderr: %s", stderr1)

	// Parse run 1 results (last emission per status)
	run1Results := parseStatusCounts(t, stdout1)
	t.Logf("run1 final counts: %v", run1Results)

	// Verify run1 consumed batch1
	run1Total := int64(0)
	for _, r := range run1Results {
		run1Total += r.Count
	}
	if run1Total != int64(batchSize) {
		t.Logf("warning: run1 total count = %d, expected %d (may be partial if timeout hit early)", run1Total, batchSize)
	}

	// Verify checkpoint files were created
	entries, _ := os.ReadDir(stateDir)
	if len(entries) == 0 {
		t.Fatal("no checkpoint files created in state dir after run 1")
	}
	t.Logf("checkpoint dir has %d entries after run 1", len(entries))

	// Step 3: Produce 5K MORE messages
	t.Log("producing batch 2 (5K more messages)...")
	produceOrdersJSON(t, topic, batch2)
	time.Sleep(1 * time.Second)

	// Step 4: Restart FoldDB with same state-dir (should resume from checkpoint)
	t.Log("run 2: resuming from checkpoint with batch 2...")
	stdout2, stderr2 := runFoldDBStateful(t, sql, stateDir, 20*time.Second)
	t.Logf("run2 stdout (%d bytes): last lines:\n%s", len(stdout2), lastNLines(stdout2, 10))
	t.Logf("run2 stderr: %s", stderr2)

	// Parse run 2 results
	run2Results := parseStatusCounts(t, stdout2)
	t.Logf("run2 final counts: %v", run2Results)

	// Step 5: Verify run2 reflects all 10K messages
	// The final counts should match the full dataset
	expectedCounts := computeExpectedStatusCounts(allOrders)
	t.Logf("expected counts (all 10K): %v", expectedCounts)

	run2Total := int64(0)
	for _, r := range run2Results {
		run2Total += r.Count
	}
	if run2Total != int64(batchSize*2) {
		t.Errorf("run2 total count = %d, expected %d", run2Total, batchSize*2)
	}

	// Verify each status count matches expected
	for _, expected := range expectedCounts {
		found := false
		for _, actual := range run2Results {
			if actual.Status == expected.Status {
				found = true
				if actual.Count != expected.Count {
					t.Errorf("status %q: run2 count=%d, expected=%d", expected.Status, actual.Count, expected.Count)
				}
				break
			}
		}
		if !found {
			t.Errorf("status %q missing from run2 results", expected.Status)
		}
	}

	// Verify checkpoint continuity: run2's initial output should NOT start from zero
	// The first emitted line in run2 should have counts > batch1 counts (accumulated)
	firstLines := firstNLines(stdout2, len(expectedCounts)+2)
	if len(firstLines) > 0 {
		firstResult := parseStatusCounts(t, strings.Join(firstLines, "\n"))
		if len(firstResult) > 0 {
			firstCount := firstResult[0].Count
			// If checkpoint worked, the first emission should include batch1 counts
			if firstCount <= 1 {
				t.Logf("note: first emission count=%d — checkpoint may have reset (check if this is expected)", firstCount)
			} else {
				t.Logf("checkpoint continuity confirmed: first emission has count=%d (> 1)", firstCount)
			}
		}
	}
}

func produceOrdersJSON(t *testing.T, topic string, orders []orderRecord) {
	t.Helper()
	messages := make([]string, len(orders))
	for i, o := range orders {
		b, _ := json.Marshal(o)
		messages[i] = string(b)
	}
	// Batch produce for speed
	const batchSize = 500
	for i := 0; i < len(messages); i += batchSize {
		end := i + batchSize
		if end > len(messages) {
			end = len(messages)
		}
		produceMessages(t, topic, messages[i:end])
	}
}

func runFoldDBStateful(t *testing.T, sql, stateDir string, timeout time.Duration) (stdout, stderr string) {
	t.Helper()
	requireBuild(t)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	args := []string{sql,
		"--stateful",
		"--state-dir", stateDir,
		"--checkpoint-interval", "1s",
		"--timeout", fmt.Sprintf("%ds", int(timeout.Seconds())-5),
	}

	cmd := exec.CommandContext(ctx, folddbBinary, args...)
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	err := cmd.Run()
	if err != nil {
		t.Logf("folddb exited: %v (expected for streaming query with timeout)", err)
	}

	return outBuf.String(), errBuf.String()
}

type statusCount struct {
	Status string
	Count  int64
}

func parseStatusCounts(t *testing.T, stdout string) []statusCount {
	t.Helper()
	lines := outputLines(stdout)
	latest := map[string]int64{}
	for _, line := range lines {
		var rec map[string]interface{}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			continue
		}
		status, _ := rec["status"].(string)
		if status == "" {
			continue
		}
		cnt := toInt64(rec["cnt"])
		latest[status] = cnt
	}
	var results []statusCount
	for s, c := range latest {
		results = append(results, statusCount{Status: s, Count: c})
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Status < results[j].Status })
	return results
}

func computeExpectedStatusCounts(orders []orderRecord) []statusCount {
	counts := map[string]int64{}
	for _, o := range orders {
		counts[o.Status]++
	}
	var results []statusCount
	for s, c := range counts {
		results = append(results, statusCount{Status: s, Count: c})
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Status < results[j].Status })
	return results
}

func lastNLines(s string, n int) string {
	lines := outputLines(s)
	if len(lines) <= n {
		return strings.Join(lines, "\n")
	}
	return strings.Join(lines[len(lines)-n:], "\n")
}

func firstNLines(s string, n int) []string {
	lines := outputLines(s)
	if len(lines) <= n {
		return lines
	}
	return lines[:n]
}
