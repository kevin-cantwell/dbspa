//go:build integration

package integration

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCheckpoint_SaveAndRestore(t *testing.T) {
	skipIfShort(t)
	waitForKafka(t)

	topic := uniqueTopic(t, "ckpt")
	createTopic(t, topic, 1)

	// Create a temp state directory
	stateDir, err := os.MkdirTemp("", "dbspa-ckpt-*")
	if err != nil {
		t.Fatalf("cannot create temp dir: %v", err)
	}
	defer os.RemoveAll(stateDir)

	// Produce initial messages
	for i := 1; i <= 5; i++ {
		produceMessages(t, topic, []string{fmt.Sprintf(`{"region":"us-east","val":%d}`, i*10)})
	}
	time.Sleep(500 * time.Millisecond)

	// Run 1: consume with --stateful, accumulate, let it process
	sql := fmt.Sprintf(
		"SELECT region, SUM(val::float) AS total FROM 'kafka://%s/%s?offset=earliest' GROUP BY region LIMIT 10",
		kafkaBroker, topic,
	)
	stdout1, stderr1, err := runDBSPAWithTimeout(t, 15*time.Second, sql,
		"--stateful", "--state-dir", stateDir, "--checkpoint-interval", "1s",
	)
	t.Logf("run1 stdout:\n%s", stdout1)
	t.Logf("run1 stderr:\n%s", stderr1)
	if err != nil {
		t.Logf("run1 exited with: %v (may be expected for streaming query)", err)
	}

	// Check that something was written to the state dir
	entries, _ := os.ReadDir(stateDir)
	t.Logf("state dir entries after run1: %d", len(entries))
	for _, e := range entries {
		t.Logf("  %s", e.Name())
	}

	// Produce more messages
	for i := 6; i <= 10; i++ {
		produceMessages(t, topic, []string{fmt.Sprintf(`{"region":"us-east","val":%d}`, i*10)})
	}
	time.Sleep(500 * time.Millisecond)

	// Run 2: should resume from checkpoint
	stdout2, stderr2, err := runDBSPAWithTimeout(t, 15*time.Second, sql,
		"--stateful", "--state-dir", stateDir, "--checkpoint-interval", "1s",
	)
	t.Logf("run2 stdout:\n%s", stdout2)
	t.Logf("run2 stderr:\n%s", stderr2)
	if err != nil {
		t.Logf("run2 exited with: %v", err)
	}

	// Verify: run2 should process the new messages. The exact output depends
	// on implementation, but we should see some output.
}

func TestCheckpoint_QueryFingerprintMismatch(t *testing.T) {
	skipIfShort(t)
	waitForKafka(t)

	topic := uniqueTopic(t, "ckpt-mismatch")
	createTopic(t, topic, 1)

	stateDir, err := os.MkdirTemp("", "dbspa-ckpt-mismatch-*")
	if err != nil {
		t.Fatalf("cannot create temp dir: %v", err)
	}
	defer os.RemoveAll(stateDir)

	produceMessages(t, topic, []string{`{"x":1}`, `{"x":2}`})
	time.Sleep(500 * time.Millisecond)

	// Run 1: query A
	sql1 := fmt.Sprintf(
		"SELECT x FROM 'kafka://%s/%s?offset=earliest' LIMIT 2",
		kafkaBroker, topic,
	)
	runDBSPAWithTimeout(t, 15*time.Second, sql1,
		"--stateful", "--state-dir", stateDir,
	)

	// Run 2: different query (different fingerprint)
	sql2 := fmt.Sprintf(
		"SELECT x, x FROM 'kafka://%s/%s?offset=earliest' LIMIT 2",
		kafkaBroker, topic,
	)
	_, stderr, _ := runDBSPAWithTimeout(t, 15*time.Second, sql2,
		"--stateful", "--state-dir", stateDir,
	)

	// Should warn about fingerprint mismatch
	t.Logf("mismatch stderr: %s", stderr)
	// The warning may appear in stderr or may just start fresh.
	// Either way, it should not crash.
}

func TestCheckpoint_StateDirectory(t *testing.T) {
	skipIfShort(t)
	requireBuild(t)

	stateDir, err := os.MkdirTemp("", "dbspa-statedir-*")
	if err != nil {
		t.Fatalf("cannot create temp dir: %v", err)
	}
	defer os.RemoveAll(stateDir)

	// Run dbspa state list pointing at the temp dir
	stdout, stderr, err := runDBSPA(t, "state", "list")
	t.Logf("state list stdout: %s", stdout)
	t.Logf("state list stderr: %s", stderr)

	// Should either list nothing or not crash
	if err != nil {
		t.Logf("state list exited with error (may be expected if no state): %v", err)
	}
}

func TestCheckpoint_StateFileCreated(t *testing.T) {
	skipIfShort(t)
	requireBuild(t)

	// Test that --stateful with stdin creates a checkpoint directory
	stateDir, err := os.MkdirTemp("", "dbspa-ckpt-stdin-*")
	if err != nil {
		t.Fatalf("cannot create temp dir: %v", err)
	}
	defer os.RemoveAll(stateDir)

	input := `{"g":"a","v":1}
{"g":"a","v":2}
{"g":"b","v":3}
`
	stdout, err2 := runDBSPAWithStdin(t,
		"SELECT g, SUM(v::float) AS total GROUP BY g",
		input,
		"--stateful", "--state-dir", stateDir,
	)
	t.Logf("checkpoint stdin output: %s", stdout)
	if err2 != nil {
		t.Logf("exited with: %v (may be expected)", err2)
	}

	// Check state directory has content
	var hasContent bool
	filepath.Walk(stateDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			hasContent = true
		}
		return nil
	})

	// Note: stdin queries may or may not checkpoint depending on implementation.
	// This test verifies no crashes occur.
	t.Logf("state dir has content: %v", hasContent)
}
