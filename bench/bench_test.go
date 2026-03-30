//go:build bench

package bench

import (
	"bytes"
	"fmt"
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
