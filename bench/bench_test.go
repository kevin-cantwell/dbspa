//go:build bench

package bench

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"testing"
)

// These benchmarks measure end-to-end throughput of folddb.
// Run with: go test -tags bench -bench . -benchtime 1x ./bench/
//
// Prerequisites: build folddb and folddb-gen first (make build)

var (
	folddbBin = envOr("FOLDDB", "../folddb")
	genBin    = envOr("FOLDDB_GEN", "../folddb-gen")
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func generateFixture(b *testing.B, dataset string, count int) []byte {
	return generateFixtureFormat(b, dataset, count, "ndjson")
}

func generateFixtureFormat(b *testing.B, dataset string, count int, format string) []byte {
	b.Helper()
	cmd := exec.Command(genBin, dataset, "--count", fmt.Sprintf("%d", count), "--seed", "42", "--format", format)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		b.Fatalf("folddb-gen --format %s failed: %v", format, err)
	}
	return out.Bytes()
}

// generateParquetFile writes a Parquet file to a temp path and returns the path.
func generateParquetFile(b *testing.B, dataset string, count int) string {
	b.Helper()
	f, err := os.CreateTemp("", "bench-*.parquet")
	if err != nil {
		b.Fatalf("cannot create temp file: %v", err)
	}
	path := f.Name()
	f.Close()
	b.Cleanup(func() { os.Remove(path) })

	cmd := exec.Command(genBin, dataset, "--count", fmt.Sprintf("%d", count), "--seed", "42", "--format", "parquet", "--output", path)
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		b.Fatalf("folddb-gen --format parquet failed: %v", err)
	}
	return path
}

func runFoldDBFile(b *testing.B, inputFile string, sql string) []byte {
	b.Helper()
	cmd := exec.Command(folddbBin, "query", "--input", inputFile, sql)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		b.Fatalf("folddb --input failed: %v", err)
	}
	return out.Bytes()
}

func runFoldDB(b *testing.B, input []byte, sql string) []byte {
	b.Helper()
	cmd := exec.Command(folddbBin, "query", sql)
	cmd.Stdin = bytes.NewReader(input)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		b.Fatalf("folddb failed: %v", err)
	}
	return out.Bytes()
}

// countLines counts newline-terminated lines in data.
func countLines(data []byte) int {
	n := 0
	for _, c := range data {
		if c == '\n' {
			n++
		}
	}
	return n
}

// --- Non-accumulating benchmarks (filter/project throughput) ---

func BenchmarkPassthrough_100K(b *testing.B) {
	data := generateFixture(b, "orders", 100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, data, "SELECT *")
	}
	b.ReportMetric(float64(100_000)/b.Elapsed().Seconds(), "records/sec")
}

func BenchmarkPassthrough_1M(b *testing.B) {
	data := generateFixture(b, "orders", 1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, data, "SELECT *")
	}
	b.ReportMetric(float64(1_000_000)/b.Elapsed().Seconds(), "records/sec")
}

func BenchmarkFilter_100K(b *testing.B) {
	data := generateFixture(b, "orders", 100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, data, "SELECT * WHERE status = 'confirmed'")
	}
	b.ReportMetric(float64(100_000)/b.Elapsed().Seconds(), "records/sec")
}

func BenchmarkFilter_1M(b *testing.B) {
	data := generateFixture(b, "orders", 1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, data, "SELECT * WHERE status = 'confirmed'")
	}
	b.ReportMetric(float64(1_000_000)/b.Elapsed().Seconds(), "records/sec")
}

// --- Accumulating benchmarks (GROUP BY throughput) ---

func BenchmarkGroupBy_100K(b *testing.B) {
	data := generateFixture(b, "orders", 100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, data, "SELECT status, COUNT(*) GROUP BY status")
	}
	b.ReportMetric(float64(100_000)/b.Elapsed().Seconds(), "records/sec")
}

func BenchmarkGroupBy_1M(b *testing.B) {
	data := generateFixture(b, "orders", 1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, data, "SELECT status, COUNT(*) GROUP BY status")
	}
	b.ReportMetric(float64(1_000_000)/b.Elapsed().Seconds(), "records/sec")
}

func BenchmarkGroupByMultiKey_100K(b *testing.B) {
	data := generateFixture(b, "orders", 100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, data, "SELECT product, region, COUNT(*), SUM(total) GROUP BY product, region")
	}
	b.ReportMetric(float64(100_000)/b.Elapsed().Seconds(), "records/sec")
}

func BenchmarkGroupByMultiKey_1M(b *testing.B) {
	data := generateFixture(b, "orders", 1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, data, "SELECT product, region, COUNT(*), SUM(total) GROUP BY product, region")
	}
	b.ReportMetric(float64(1_000_000)/b.Elapsed().Seconds(), "records/sec")
}

func BenchmarkGroupByWithFilter_1M(b *testing.B) {
	data := generateFixture(b, "orders", 1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, data, "SELECT product, region, COUNT(*), SUM(total) WHERE status = 'confirmed' GROUP BY product, region")
	}
	b.ReportMetric(float64(1_000_000)/b.Elapsed().Seconds(), "records/sec")
}

// --- CDC benchmarks (Debezium retraction throughput) ---

func BenchmarkCDC_100K(b *testing.B) {
	data := generateFixture(b, "orders-cdc", 100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, data, "SELECT _after->>'status' AS status, COUNT(*) GROUP BY _after->>'status' FORMAT DEBEZIUM")
	}
	b.ReportMetric(float64(100_000)/b.Elapsed().Seconds(), "records/sec")
}

// --- JSON decode microbenchmark ---

func BenchmarkJSONDecode_100K(b *testing.B) {
	data := generateFixture(b, "orders", 100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, data, "SELECT order_id")
	}
	b.ReportMetric(float64(100_000)/b.Elapsed().Seconds(), "records/sec")
}

// =====================================================
// Format comparison benchmarks: NDJSON vs Avro vs Protobuf
// =====================================================

// --- Passthrough (SELECT *) across formats ---

func BenchmarkFormat_NDJSON_Passthrough_100K(b *testing.B) {
	data := generateFixtureFormat(b, "orders", 100_000, "ndjson")
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, data, "SELECT *")
	}
	b.ReportMetric(float64(100_000)/b.Elapsed().Seconds(), "records/sec")
}

func BenchmarkFormat_Avro_Passthrough_100K(b *testing.B) {
	data := generateFixtureFormat(b, "orders", 100_000, "avro")
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, data, "SELECT * FORMAT AVRO")
	}
	b.ReportMetric(float64(100_000)/b.Elapsed().Seconds(), "records/sec")
}

func BenchmarkFormat_Protobuf_Passthrough_100K(b *testing.B) {
	data := generateFixtureFormat(b, "orders", 100_000, "protobuf")
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, data, "SELECT * FORMAT PROTOBUF")
	}
	b.ReportMetric(float64(100_000)/b.Elapsed().Seconds(), "records/sec")
}

// --- Filter across formats ---

func BenchmarkFormat_NDJSON_Filter_100K(b *testing.B) {
	data := generateFixtureFormat(b, "orders", 100_000, "ndjson")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, data, "SELECT * WHERE status = 'confirmed'")
	}
	b.ReportMetric(float64(100_000)/b.Elapsed().Seconds(), "records/sec")
}

func BenchmarkFormat_Avro_Filter_100K(b *testing.B) {
	data := generateFixtureFormat(b, "orders", 100_000, "avro")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, data, "SELECT * WHERE status = 'confirmed' FORMAT AVRO")
	}
	b.ReportMetric(float64(100_000)/b.Elapsed().Seconds(), "records/sec")
}

func BenchmarkFormat_Protobuf_Filter_100K(b *testing.B) {
	data := generateFixtureFormat(b, "orders", 100_000, "protobuf")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, data, "SELECT * WHERE status = 'confirmed' FORMAT PROTOBUF")
	}
	b.ReportMetric(float64(100_000)/b.Elapsed().Seconds(), "records/sec")
}

// --- GROUP BY across formats ---

func BenchmarkFormat_NDJSON_GroupBy_100K(b *testing.B) {
	data := generateFixtureFormat(b, "orders", 100_000, "ndjson")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, data, "SELECT product, region, COUNT(*), SUM(total) GROUP BY product, region")
	}
	b.ReportMetric(float64(100_000)/b.Elapsed().Seconds(), "records/sec")
}

func BenchmarkFormat_Avro_GroupBy_100K(b *testing.B) {
	data := generateFixtureFormat(b, "orders", 100_000, "avro")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, data, "SELECT product, region, COUNT(*), SUM(total) GROUP BY product, region FORMAT AVRO")
	}
	b.ReportMetric(float64(100_000)/b.Elapsed().Seconds(), "records/sec")
}

func BenchmarkFormat_Protobuf_GroupBy_100K(b *testing.B) {
	data := generateFixtureFormat(b, "orders", 100_000, "protobuf")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, data, "SELECT product, region, COUNT(*), SUM(total) GROUP BY product, region FORMAT PROTOBUF")
	}
	b.ReportMetric(float64(100_000)/b.Elapsed().Seconds(), "records/sec")
}

// --- Typed Protobuf benchmarks ---

func BenchmarkFormat_ProtoTyped_Passthrough_100K(b *testing.B) {
	data := generateFixtureFormat(b, "orders", 100_000, "proto-typed")
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, data, "SELECT * FORMAT PROTOBUF(message='Order')")
	}
	b.ReportMetric(float64(100_000)/b.Elapsed().Seconds(), "records/sec")
}

func BenchmarkFormat_ProtoTyped_Filter_100K(b *testing.B) {
	data := generateFixtureFormat(b, "orders", 100_000, "proto-typed")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, data, "SELECT * WHERE status = 'confirmed' FORMAT PROTOBUF(message='Order')")
	}
	b.ReportMetric(float64(100_000)/b.Elapsed().Seconds(), "records/sec")
}

func BenchmarkFormat_ProtoTyped_GroupBy_100K(b *testing.B) {
	data := generateFixtureFormat(b, "orders", 100_000, "proto-typed")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, data, "SELECT product, region, COUNT(*), SUM(total) GROUP BY product, region FORMAT PROTOBUF(message='Order')")
	}
	b.ReportMetric(float64(100_000)/b.Elapsed().Seconds(), "records/sec")
}

// --- Parquet file benchmarks (--input) ---

func BenchmarkFormat_Parquet_Passthrough_100K(b *testing.B) {
	path := generateParquetFile(b, "orders", 100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDBFile(b, path, "SELECT * FORMAT PARQUET")
	}
	b.ReportMetric(float64(100_000)/b.Elapsed().Seconds(), "records/sec")
}

func BenchmarkFormat_Parquet_Filter_100K(b *testing.B) {
	path := generateParquetFile(b, "orders", 100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDBFile(b, path, "SELECT * WHERE status = 'confirmed' FORMAT PARQUET")
	}
	b.ReportMetric(float64(100_000)/b.Elapsed().Seconds(), "records/sec")
}

func BenchmarkFormat_Parquet_GroupBy_100K(b *testing.B) {
	path := generateParquetFile(b, "orders", 100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDBFile(b, path, "SELECT product, region, COUNT(*), SUM(total) GROUP BY product, region FORMAT PARQUET")
	}
	b.ReportMetric(float64(100_000)/b.Elapsed().Seconds(), "records/sec")
}

// =====================================================
// Join benchmarks
// =====================================================

// generateTableFile creates an NDJSON file with N rows of {id, tier, name} data.
// IDs range from 1..size. Returns the file path.
func generateTableFile(b *testing.B, size int) string {
	b.Helper()
	f, err := os.CreateTemp("", "bench-table-*.ndjson")
	if err != nil {
		b.Fatalf("cannot create temp file: %v", err)
	}
	path := f.Name()
	b.Cleanup(func() { os.Remove(path) })

	tiers := []string{"gold", "silver", "bronze", "platinum", "basic"}
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	for i := 1; i <= size; i++ {
		rec := map[string]any{
			"id":   i,
			"tier": tiers[i%len(tiers)],
			"name": fmt.Sprintf("user_%d", i),
		}
		if err := enc.Encode(rec); err != nil {
			f.Close()
			b.Fatalf("write error: %v", err)
		}
	}
	f.Close()
	return path
}

// generateStreamData creates NDJSON stream data with N records,
// each having a customer_id that maps into the table range [1..tableSize].
func generateStreamData(b *testing.B, count, tableSize int) []byte {
	b.Helper()
	rng := rand.New(rand.NewSource(42))
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	actions := []string{"login", "purchase", "click", "view", "logout"}
	for i := 0; i < count; i++ {
		rec := map[string]any{
			"customer_id": rng.Intn(tableSize) + 1,
			"action":      actions[rng.Intn(len(actions))],
			"value":       rng.Intn(1000),
		}
		if err := enc.Encode(rec); err != nil {
			b.Fatalf("encode error: %v", err)
		}
	}
	return buf.Bytes()
}

func BenchmarkJoin_SmallTable_100K(b *testing.B) {
	tableFile := generateTableFile(b, 100)
	streamData := generateStreamData(b, 100_000, 100)
	sql := fmt.Sprintf("SELECT e.action, u.tier FROM stdin e JOIN '%s' u ON e.customer_id = u.id", tableFile)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, streamData, sql)
	}
	b.ReportMetric(float64(100_000)/b.Elapsed().Seconds(), "records/sec")
}

func BenchmarkJoin_LargeTable_100K(b *testing.B) {
	tableFile := generateTableFile(b, 10_000)
	streamData := generateStreamData(b, 100_000, 10_000)
	sql := fmt.Sprintf("SELECT e.action, u.tier FROM stdin e JOIN '%s' u ON e.customer_id = u.id", tableFile)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, streamData, sql)
	}
	b.ReportMetric(float64(100_000)/b.Elapsed().Seconds(), "records/sec")
}

func BenchmarkJoin_WithGroupBy_100K(b *testing.B) {
	tableFile := generateTableFile(b, 100)
	streamData := generateStreamData(b, 100_000, 100)
	sql := fmt.Sprintf("SELECT u.tier, COUNT(*) AS cnt, SUM(e.value) AS total FROM stdin e JOIN '%s' u ON e.customer_id = u.id GROUP BY u.tier", tableFile)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, streamData, sql)
	}
	b.ReportMetric(float64(100_000)/b.Elapsed().Seconds(), "records/sec")
}

// =====================================================
// Batch compaction benchmarks
// =====================================================

// generateBatchData creates NDJSON stream data for batch compaction benchmarks.
// highDuplication: 90% of records share one of 10 group keys.
// lowDuplication: each record gets a distinct group key.
func generateBatchData(b *testing.B, count int, highDuplication bool) []byte {
	b.Helper()
	rng := rand.New(rand.NewSource(42))
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)

	if highDuplication {
		groups := []string{"g0", "g1", "g2", "g3", "g4", "g5", "g6", "g7", "g8", "g9"}
		for i := 0; i < count; i++ {
			g := groups[rng.Intn(len(groups))]
			rec := map[string]any{
				"g": g,
				"v": rng.Intn(1000),
			}
			if err := enc.Encode(rec); err != nil {
				b.Fatalf("encode error: %v", err)
			}
		}
	} else {
		for i := 0; i < count; i++ {
			rec := map[string]any{
				"g": fmt.Sprintf("g_%d", i),
				"v": rng.Intn(1000),
			}
			if err := enc.Encode(rec); err != nil {
				b.Fatalf("encode error: %v", err)
			}
		}
	}
	return buf.Bytes()
}

func BenchmarkBatchCompaction_HighDuplication(b *testing.B) {
	data := generateBatchData(b, 100_000, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, data, "SELECT g, COUNT(*) AS cnt, SUM(v) AS total GROUP BY g")
	}
	b.ReportMetric(float64(100_000)/b.Elapsed().Seconds(), "records/sec")
}

func BenchmarkBatchCompaction_LowDuplication(b *testing.B) {
	data := generateBatchData(b, 100_000, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFoldDB(b, data, "SELECT g, COUNT(*) AS cnt, SUM(v) AS total GROUP BY g")
	}
	b.ReportMetric(float64(100_000)/b.Elapsed().Seconds(), "records/sec")
}
