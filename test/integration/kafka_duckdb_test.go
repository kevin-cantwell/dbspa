//go:build integration

package integration

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/twmb/franz-go/pkg/kgo"
)

// QA Item 1: Kafka produce (Confluent Avro) -> consume with FoldDB -> verify against DuckDB

const orderAvroSchema = `{
  "type": "record",
  "name": "Order",
  "namespace": "qa.orders",
  "fields": [
    {"name": "order_id", "type": "int"},
    {"name": "customer_id", "type": "int"},
    {"name": "status", "type": "string"},
    {"name": "total", "type": "double"},
    {"name": "region", "type": "string"}
  ]
}`

var orderStatuses = []string{"pending", "shipped", "delivered", "cancelled", "returned"}
var orderRegions = []string{"us-east", "us-west", "eu-west", "ap-south"}

type orderRecord struct {
	OrderID    int     `json:"order_id"`
	CustomerID int     `json:"customer_id"`
	Status     string  `json:"status"`
	Total      float64 `json:"total"`
	Region     string  `json:"region"`
}

// generateOrders creates deterministic order data using the given seed.
func generateOrders(n int, seed int64) []orderRecord {
	rng := rand.New(rand.NewSource(seed))
	orders := make([]orderRecord, n)
	for i := 0; i < n; i++ {
		orders[i] = orderRecord{
			OrderID:    i + 1,
			CustomerID: rng.Intn(500) + 1,
			Status:     orderStatuses[rng.Intn(len(orderStatuses))],
			Total:      float64(rng.Intn(100000)) / 100.0, // 0.00 to 999.99
			Region:     orderRegions[rng.Intn(len(orderRegions))],
		}
	}
	return orders
}

// registerAvroSchema registers an Avro schema with the Confluent Schema Registry
// and returns the assigned schema ID.
func registerAvroSchema(t *testing.T, subject, schema string) int32 {
	t.Helper()
	registryURL := "http://localhost:8081"

	payload := fmt.Sprintf(`{"schema": %s}`, jsonEscape(schema))

	cmd := exec.Command("curl", "-s", "-X", "POST",
		"-H", "Content-Type: application/vnd.schemaregistry.v1+json",
		"-d", payload,
		fmt.Sprintf("%s/subjects/%s/versions", registryURL, subject),
	)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("cannot register schema: %v", err)
	}

	var resp struct {
		ID int32 `json:"id"`
	}
	if err := json.Unmarshal(out, &resp); err != nil {
		t.Fatalf("cannot parse schema registry response: %v\nresponse: %s", err, out)
	}
	if resp.ID == 0 {
		t.Fatalf("schema registry returned id=0, response: %s", out)
	}

	t.Cleanup(func() {
		// Delete subject (soft + hard)
		exec.Command("curl", "-s", "-X", "DELETE",
			fmt.Sprintf("%s/subjects/%s", registryURL, subject)).Run()
		exec.Command("curl", "-s", "-X", "DELETE",
			fmt.Sprintf("%s/subjects/%s?permanent=true", registryURL, subject)).Run()
	})

	return resp.ID
}

func jsonEscape(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// encodeConfluentAvro encodes an order as Confluent wire format:
// [0x00][4-byte schema ID big-endian][Avro binary payload]
func encodeConfluentAvro(codec *goavro.Codec, schemaID int32, record map[string]interface{}) ([]byte, error) {
	avroBytes, err := codec.BinaryFromNative(nil, record)
	if err != nil {
		return nil, fmt.Errorf("avro encode: %w", err)
	}
	buf := make([]byte, 5+len(avroBytes))
	buf[0] = 0x00 // magic byte
	binary.BigEndian.PutUint32(buf[1:5], uint32(schemaID))
	copy(buf[5:], avroBytes)
	return buf, nil
}

func TestKafkaDuckDB_AvroAggregationMatch(t *testing.T) {
	skipIfShort(t)
	waitForKafka(t)

	const numOrders = 10000
	const seed = 42

	topic := uniqueTopic(t, "qa-avro-duckdb")
	createTopic(t, topic, 3)

	// Step 1: Register Avro schema
	subject := topic + "-value"
	schemaID := registerAvroSchema(t, subject, orderAvroSchema)
	t.Logf("registered schema ID: %d", schemaID)

	codec, err := goavro.NewCodec(orderAvroSchema)
	if err != nil {
		t.Fatalf("cannot create avro codec: %v", err)
	}

	// Step 2: Generate deterministic data
	orders := generateOrders(numOrders, seed)

	// Step 3: Produce 10K Confluent Avro messages
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBroker),
		kgo.DefaultProduceTopic(topic),
	)
	if err != nil {
		t.Fatalf("cannot create kafka producer: %v", err)
	}
	defer cl.Close()

	t.Log("producing 10K Avro messages...")
	for i, order := range orders {
		native := map[string]interface{}{
			"order_id":    int32(order.OrderID),
			"customer_id": int32(order.CustomerID),
			"status":      order.Status,
			"total":       order.Total,
			"region":      order.Region,
		}
		encoded, err := encodeConfluentAvro(codec, schemaID, native)
		if err != nil {
			t.Fatalf("encode order %d: %v", i, err)
		}
		r := &kgo.Record{Value: encoded}
		results := cl.ProduceSync(t.Context(), r)
		if results.FirstErr() != nil {
			t.Fatalf("produce error at message %d: %v", i, results.FirstErr())
		}
	}
	t.Log("produced all messages")
	time.Sleep(1 * time.Second)

	// Step 4: Consume with FoldDB
	sql := fmt.Sprintf(
		"SELECT status, COUNT(*) AS cnt, SUM(total) AS total_sum FROM 'kafka://%s/%s?offset=earliest&registry=http://localhost:8081' FORMAT AVRO GROUP BY status",
		kafkaBroker, topic,
	)
	folddbStdout, folddbStderr, err := runFoldDBWithTimeout(t, 60*time.Second, sql, "--timeout", "15s")
	if err != nil {
		t.Logf("folddb stderr: %s", folddbStderr)
		// Timeout exit is expected for streaming queries
		if !strings.Contains(err.Error(), "signal") && !strings.Contains(err.Error(), "exit status") && folddbStdout == "" {
			t.Fatalf("folddb failed with no output: %v", err)
		}
	}
	t.Logf("folddb stdout (%d bytes)", len(folddbStdout))

	// Parse FoldDB results: last emission per status (streaming changelog)
	folddbResults := parseFoldDBGroupByResults(t, folddbStdout)
	if len(folddbResults) == 0 {
		t.Fatalf("folddb produced no output\nstderr: %s", folddbStderr)
	}
	t.Logf("folddb results: %v", folddbResults)

	// Step 5: Compute expected results with DuckDB
	ndjsonFile := writeOrdersNDJSON(t, orders)
	defer os.Remove(ndjsonFile)

	duckdbResults := runDuckDBAggregation(t, ndjsonFile)
	t.Logf("duckdb results: %v", duckdbResults)

	// Step 6: Compare results
	compareAggResults(t, folddbResults, duckdbResults)
}

type aggResult struct {
	Status string
	Count  int64
	Total  float64
}

// parseFoldDBGroupByResults parses the streaming NDJSON output of a GROUP BY query.
// For a changelog stream, the last record per group key represents the final state.
func parseFoldDBGroupByResults(t *testing.T, stdout string) []aggResult {
	t.Helper()
	lines := outputLines(stdout)
	// Keep last emission per status
	latest := map[string]aggResult{}
	for _, line := range lines {
		var rec map[string]interface{}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Logf("skipping unparseable line: %s", line)
			continue
		}
		status, _ := rec["status"].(string)
		if status == "" {
			continue
		}
		cnt := toInt64(rec["cnt"])
		totalSum := toFloat64(rec["total_sum"])
		latest[status] = aggResult{Status: status, Count: cnt, Total: totalSum}
	}
	var results []aggResult
	for _, r := range latest {
		results = append(results, r)
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Status < results[j].Status })
	return results
}

func toInt64(v interface{}) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case json.Number:
		i, _ := n.Int64()
		return i
	case int64:
		return n
	case int:
		return int64(n)
	}
	return 0
}

func toFloat64(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case json.Number:
		f, _ := n.Float64()
		return f
	case int64:
		return float64(n)
	case int:
		return float64(n)
	}
	return 0
}

func writeOrdersNDJSON(t *testing.T, orders []orderRecord) string {
	t.Helper()
	f, err := os.CreateTemp("", "orders-*.ndjson")
	if err != nil {
		t.Fatalf("cannot create temp file: %v", err)
	}
	for _, o := range orders {
		line, _ := json.Marshal(o)
		f.Write(line)
		f.WriteString("\n")
	}
	f.Close()
	return f.Name()
}

func runDuckDBAggregation(t *testing.T, ndjsonFile string) []aggResult {
	t.Helper()
	query := fmt.Sprintf(
		`SELECT status, COUNT(*) AS cnt, SUM(total) AS total_sum FROM read_json_auto('%s') GROUP BY status ORDER BY status;`,
		ndjsonFile,
	)
	cmd := exec.Command("duckdb", "-json", "-c", query)
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			t.Fatalf("duckdb failed: %v\nstderr: %s", err, exitErr.Stderr)
		}
		t.Fatalf("duckdb failed: %v", err)
	}

	var rows []map[string]interface{}
	if err := json.Unmarshal(out, &rows); err != nil {
		t.Fatalf("cannot parse duckdb output: %v\noutput: %s", err, out)
	}

	var results []aggResult
	for _, row := range rows {
		status, _ := row["status"].(string)
		cnt := toInt64(row["cnt"])
		totalSum := toFloat64(row["total_sum"])
		results = append(results, aggResult{Status: status, Count: cnt, Total: totalSum})
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Status < results[j].Status })
	return results
}

func compareAggResults(t *testing.T, folddb, duckdb []aggResult) {
	t.Helper()
	if len(folddb) != len(duckdb) {
		t.Fatalf("result count mismatch: folddb=%d duckdb=%d\nfolddb: %v\nduckdb: %v",
			len(folddb), len(duckdb), folddb, duckdb)
	}
	for i := range folddb {
		f := folddb[i]
		d := duckdb[i]
		if f.Status != d.Status {
			t.Errorf("row %d status mismatch: folddb=%q duckdb=%q", i, f.Status, d.Status)
		}
		if f.Count != d.Count {
			t.Errorf("row %d (%s) count mismatch: folddb=%d duckdb=%d", i, f.Status, f.Count, d.Count)
		}
		// Compare totals with tolerance for floating point
		diff := f.Total - d.Total
		if diff < 0 {
			diff = -diff
		}
		if diff > 0.01 {
			t.Errorf("row %d (%s) total mismatch: folddb=%.2f duckdb=%.2f (diff=%.4f)",
				i, f.Status, f.Total, d.Total, diff)
		}
	}
}
