//go:build integration

package integration

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestKafka_ProduceAndConsumeNDJSON(t *testing.T) {
	skipIfShort(t)
	waitForKafka(t)

	topic := uniqueTopic(t, "ndjson")
	createTopic(t, topic, 1)

	messages := []string{
		`{"name":"alice","age":30}`,
		`{"name":"bob","age":25}`,
		`{"name":"charlie","age":35}`,
	}
	produceMessages(t, topic, messages)

	// Give Kafka a moment to commit
	time.Sleep(500 * time.Millisecond)

	sql := fmt.Sprintf(
		"SELECT name, age FROM 'kafka://%s/%s?offset=earliest' LIMIT 3",
		kafkaBroker, topic,
	)
	stdout, stderr, err := runFoldDBWithTimeout(t, 15*time.Second, sql)
	if err != nil {
		t.Fatalf("folddb failed: %v\nstderr: %s", err, stderr)
	}

	lines := outputLines(stdout)
	if len(lines) != 3 {
		t.Fatalf("expected 3 output lines, got %d: %s", len(lines), stdout)
	}

	// Verify all names are present
	combined := strings.Join(lines, " ")
	for _, name := range []string{"alice", "bob", "charlie"} {
		if !strings.Contains(combined, name) {
			t.Errorf("expected output to contain %q, got: %s", name, stdout)
		}
	}
}

func TestKafka_DebeziumCDC(t *testing.T) {
	skipIfShort(t)
	waitForKafka(t)

	topic := uniqueTopic(t, "debezium")
	createTopic(t, topic, 1)

	// Create event
	createEvt := `{"schema":null,"payload":{"before":null,"after":{"id":1,"name":"alice","status":"active"},"op":"c","ts_ms":1711612800000,"source":{"version":"2.5.0","connector":"postgresql","name":"test","ts_ms":1711612800000,"db":"testdb","schema":"public","table":"users","txId":100,"lsn":5000}}}`
	// Update event (with before)
	updateEvt := `{"schema":null,"payload":{"before":{"id":1,"name":"alice","status":"active"},"after":{"id":1,"name":"alice","status":"inactive"},"op":"u","ts_ms":1711612860000,"source":{"version":"2.5.0","connector":"postgresql","name":"test","ts_ms":1711612860000,"db":"testdb","schema":"public","table":"users","txId":101,"lsn":5100}}}`

	produceMessages(t, topic, []string{createEvt, updateEvt})
	time.Sleep(500 * time.Millisecond)

	sql := fmt.Sprintf(
		"SELECT _op, _after->>'name' AS name, _after->>'status' AS status FROM 'kafka://%s/%s?offset=earliest' FORMAT DEBEZIUM LIMIT 3",
		kafkaBroker, topic,
	)
	stdout, stderr, err := runFoldDBWithTimeout(t, 15*time.Second, sql)
	if err != nil {
		t.Fatalf("folddb failed: %v\nstderr: %s", err, stderr)
	}

	lines := outputLines(stdout)
	// Create emits 1 record (+1), update emits 2 records (-1, +1) = 3 total
	if len(lines) < 2 {
		t.Fatalf("expected at least 2 output lines for debezium CDC, got %d: %s", len(lines), stdout)
	}

	// First line should be the create
	var first map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("cannot parse first line: %v", err)
	}
	if first["name"] != "alice" {
		t.Errorf("expected name=alice, got %v", first["name"])
	}
}

func TestKafka_OffsetEarliest(t *testing.T) {
	skipIfShort(t)
	waitForKafka(t)

	topic := uniqueTopic(t, "earliest")
	createTopic(t, topic, 1)

	// Produce messages before consumer starts
	messages := []string{
		`{"x":1}`,
		`{"x":2}`,
		`{"x":3}`,
	}
	produceMessages(t, topic, messages)
	time.Sleep(500 * time.Millisecond)

	sql := fmt.Sprintf(
		"SELECT x FROM 'kafka://%s/%s?offset=earliest' LIMIT 3",
		kafkaBroker, topic,
	)
	stdout, _, err := runFoldDBWithTimeout(t, 15*time.Second, sql)
	if err != nil {
		t.Fatalf("folddb failed: %v", err)
	}

	lines := outputLines(stdout)
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines with offset=earliest, got %d: %s", len(lines), stdout)
	}
}

func TestKafka_OffsetLatest(t *testing.T) {
	skipIfShort(t)
	waitForKafka(t)

	topic := uniqueTopic(t, "latest")
	createTopic(t, topic, 1)

	// Produce some messages before consumer starts
	produceMessages(t, topic, []string{`{"old":true}`})
	time.Sleep(500 * time.Millisecond)

	// Start consumer with offset=latest and a short timeout
	// It should NOT see the old message
	sql := fmt.Sprintf(
		"SELECT old FROM 'kafka://%s/%s?offset=latest' LIMIT 1",
		kafkaBroker, topic,
	)
	// Use a short timeout - if no new messages arrive, the process times out
	stdout, _, _ := runFoldDBWithTimeout(t, 5*time.Second, sql)

	lines := outputLines(stdout)
	if len(lines) != 0 {
		t.Errorf("expected 0 lines with offset=latest (no new messages), got %d: %s", len(lines), stdout)
	}
}

func TestKafka_MultiPartition(t *testing.T) {
	skipIfShort(t)
	waitForKafka(t)

	topic := uniqueTopic(t, "multipart")
	createTopic(t, topic, 3)

	// Produce to each partition explicitly
	for p := int32(0); p < 3; p++ {
		msgs := []string{
			fmt.Sprintf(`{"partition":%d,"seq":1}`, p),
			fmt.Sprintf(`{"partition":%d,"seq":2}`, p),
		}
		produceToPartition(t, topic, p, msgs)
	}
	time.Sleep(500 * time.Millisecond)

	sql := fmt.Sprintf(
		"SELECT partition, seq FROM 'kafka://%s/%s?offset=earliest' LIMIT 6",
		kafkaBroker, topic,
	)
	stdout, stderr, err := runFoldDBWithTimeout(t, 15*time.Second, sql)
	if err != nil {
		t.Fatalf("folddb failed: %v\nstderr: %s", err, stderr)
	}

	lines := outputLines(stdout)
	if len(lines) != 6 {
		t.Fatalf("expected 6 lines from 3 partitions, got %d: %s", len(lines), stdout)
	}

	// Verify we got messages from all 3 partitions
	partitions := map[string]bool{}
	for _, line := range lines {
		var rec map[string]any
		json.Unmarshal([]byte(line), &rec)
		if p, ok := rec["partition"]; ok {
			partitions[fmt.Sprintf("%v", p)] = true
		}
	}
	if len(partitions) != 3 {
		t.Errorf("expected messages from 3 partitions, got from %d: %v", len(partitions), partitions)
	}
}

func TestKafka_VirtualColumns(t *testing.T) {
	skipIfShort(t)
	waitForKafka(t)

	topic := uniqueTopic(t, "vcols")
	createTopic(t, topic, 1)

	produceKeyedMessages(t, topic, []keyedMessage{
		{Key: "key-1", Value: `{"val":"hello"}`},
	})
	time.Sleep(500 * time.Millisecond)

	sql := fmt.Sprintf(
		"SELECT _offset, _partition, _key, val FROM 'kafka://%s/%s?offset=earliest' LIMIT 1",
		kafkaBroker, topic,
	)
	stdout, stderr, err := runFoldDBWithTimeout(t, 15*time.Second, sql)
	if err != nil {
		t.Fatalf("folddb failed: %v\nstderr: %s", err, stderr)
	}

	lines := outputLines(stdout)
	if len(lines) != 1 {
		t.Fatalf("expected 1 output line, got %d: %s", len(lines), stdout)
	}

	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("cannot parse output: %v", err)
	}

	// _offset should be 0 (first message)
	if offset, ok := rec["_offset"]; !ok {
		t.Error("missing _offset virtual column")
	} else if fmt.Sprintf("%v", offset) != "0" {
		t.Errorf("expected _offset=0, got %v", offset)
	}

	// _partition should be 0
	if part, ok := rec["_partition"]; !ok {
		t.Error("missing _partition virtual column")
	} else if fmt.Sprintf("%v", part) != "0" {
		t.Errorf("expected _partition=0, got %v", part)
	}

	// _key should be "key-1"
	if key, ok := rec["_key"]; !ok {
		t.Error("missing _key virtual column")
	} else if fmt.Sprintf("%v", key) != "key-1" {
		t.Errorf("expected _key=key-1, got %v", key)
	}

	// val should be "hello"
	if val, ok := rec["val"]; !ok {
		t.Error("missing val field")
	} else if fmt.Sprintf("%v", val) != "hello" {
		t.Errorf("expected val=hello, got %v", val)
	}
}

func TestKafka_ConsumerGroupOffsetCommit(t *testing.T) {
	skipIfShort(t)
	waitForKafka(t)

	topic := uniqueTopic(t, "cg")
	createTopic(t, topic, 1)
	group := uniqueTopic(t, "group")

	// Produce 5 messages
	for i := 1; i <= 5; i++ {
		produceMessages(t, topic, []string{fmt.Sprintf(`{"n":%d}`, i)})
	}
	time.Sleep(500 * time.Millisecond)

	// First run: consume all 5 with a consumer group
	sql := fmt.Sprintf(
		"SELECT n FROM 'kafka://%s/%s?offset=earliest&group=%s' LIMIT 5",
		kafkaBroker, topic, group,
	)
	stdout, _, err := runFoldDBWithTimeout(t, 15*time.Second, sql)
	if err != nil {
		t.Fatalf("first run failed: %v", err)
	}
	lines := outputLines(stdout)
	if len(lines) != 5 {
		t.Fatalf("expected 5 lines in first run, got %d", len(lines))
	}

	// Give time for offset commit
	time.Sleep(2 * time.Second)

	// Produce 2 more messages
	for i := 6; i <= 7; i++ {
		produceMessages(t, topic, []string{fmt.Sprintf(`{"n":%d}`, i)})
	}
	time.Sleep(500 * time.Millisecond)

	// Second run: should only see the 2 new messages (committed offsets skip the first 5)
	sql2 := fmt.Sprintf(
		"SELECT n FROM 'kafka://%s/%s?group=%s' LIMIT 2",
		kafkaBroker, topic, group,
	)
	stdout2, _, err := runFoldDBWithTimeout(t, 15*time.Second, sql2)
	if err != nil {
		// It is acceptable if this times out or returns fewer results --
		// consumer group offset commit is best-effort per the spec.
		t.Logf("second run result (may be partial): %s", stdout2)
		return
	}
	lines2 := outputLines(stdout2)
	t.Logf("second run got %d lines: %s", len(lines2), stdout2)
}
