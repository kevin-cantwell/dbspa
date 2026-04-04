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

// These benchmarks measure end-to-end throughput of dbspa.
// Run with: go test -tags bench -bench . -benchtime 1x ./bench/
//
// Prerequisites: build dbspa and dbspa-gen first (make build)
//
// Benchmark hierarchy:
//
//   BenchmarkPipeline  — core query patterns (passthrough, filter, group-by, CDC)
//   BenchmarkFormat    — format comparison (NDJSON vs Avro vs Protobuf vs Parquet)
//   BenchmarkJoin      — stream-to-table join scenarios
//   BenchmarkCompaction — batch compaction: high vs low duplication
//
// NullSink sub-benchmarks eliminate sink serialization overhead, isolating
// the pipeline (decode → filter → aggregate → output channel) throughput.

var (
	dbspaBin = envOr("DBSPA", "../dbspa")
	genBin   = envOr("DBSPA_GEN", "../dbspa-gen")
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
		b.Fatalf("dbspa-gen --format %s failed: %v", format, err)
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
		b.Fatalf("dbspa-gen --format parquet failed: %v", err)
	}
	return path
}

func runDBSPAFile(b *testing.B, inputFile string, sql string) []byte {
	b.Helper()
	cmd := exec.Command(dbspaBin, "query", "--input", inputFile, sql)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		b.Fatalf("dbspa --input failed: %v", err)
	}
	return out.Bytes()
}

func runDBSPA(b *testing.B, input []byte, sql string) []byte {
	b.Helper()
	cmd := exec.Command(dbspaBin, "query", sql)
	cmd.Stdin = bytes.NewReader(input)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		b.Fatalf("dbspa failed: %v", err)
	}
	return out.Bytes()
}

// runDBSPANull runs dbspa with --null-output, discarding all output.
// Use this to measure pipeline throughput without sink serialization overhead.
func runDBSPANull(b *testing.B, input []byte, sql string) {
	b.Helper()
	cmd := exec.Command(dbspaBin, "query", "--null-output", sql)
	cmd.Stdin = bytes.NewReader(input)
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		b.Fatalf("dbspa --null-output failed: %v", err)
	}
}

func runDBSPAFileNull(b *testing.B, inputFile string, sql string) {
	b.Helper()
	cmd := exec.Command(dbspaBin, "query", "--null-output", "--input", inputFile, sql)
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		b.Fatalf("dbspa --null-output --input failed: %v", err)
	}
}

// reportRecordsPerSec sets the records/sec metric for benchstat output.
func reportRecordsPerSec(b *testing.B, count int) {
	b.Helper()
	b.ReportMetric(float64(count)/b.Elapsed().Seconds(), "records/sec")
}

// =====================================================
// BenchmarkPipeline: core query patterns × scale
// =====================================================
//
// Hierarchy: BenchmarkPipeline/<query>/<scale>[/NullSink]
//
// NullSink variants measure pipeline throughput without JSON serialization
// overhead. Compare them against the base variant to understand sink cost.

func BenchmarkPipeline(b *testing.B) {
	b.Run("Passthrough", func(b *testing.B) {
		b.Run("100K", func(b *testing.B) {
			data := generateFixture(b, "orders", 100_000)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runDBSPA(b, data, "SELECT *")
			}
			reportRecordsPerSec(b, 100_000)
		})
		b.Run("1M", func(b *testing.B) {
			data := generateFixture(b, "orders", 1_000_000)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runDBSPA(b, data, "SELECT *")
			}
			reportRecordsPerSec(b, 1_000_000)
		})
		b.Run("1M/NullSink", func(b *testing.B) {
			data := generateFixture(b, "orders", 1_000_000)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runDBSPANull(b, data, "SELECT *")
			}
			reportRecordsPerSec(b, 1_000_000)
		})
	})

	b.Run("Filter", func(b *testing.B) {
		b.Run("100K", func(b *testing.B) {
			data := generateFixture(b, "orders", 100_000)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runDBSPA(b, data, "SELECT * WHERE status = 'confirmed'")
			}
			reportRecordsPerSec(b, 100_000)
		})
		b.Run("1M", func(b *testing.B) {
			data := generateFixture(b, "orders", 1_000_000)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runDBSPA(b, data, "SELECT * WHERE status = 'confirmed'")
			}
			reportRecordsPerSec(b, 1_000_000)
		})
		b.Run("1M/NullSink", func(b *testing.B) {
			data := generateFixture(b, "orders", 1_000_000)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runDBSPANull(b, data, "SELECT * WHERE status = 'confirmed'")
			}
			reportRecordsPerSec(b, 1_000_000)
		})
	})

	b.Run("GroupBy", func(b *testing.B) {
		b.Run("100K", func(b *testing.B) {
			data := generateFixture(b, "orders", 100_000)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runDBSPA(b, data, "SELECT status, COUNT(*) GROUP BY status")
			}
			reportRecordsPerSec(b, 100_000)
		})
		b.Run("1M", func(b *testing.B) {
			data := generateFixture(b, "orders", 1_000_000)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runDBSPA(b, data, "SELECT status, COUNT(*) GROUP BY status")
			}
			reportRecordsPerSec(b, 1_000_000)
		})
		b.Run("1M/NullSink", func(b *testing.B) {
			data := generateFixture(b, "orders", 1_000_000)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runDBSPANull(b, data, "SELECT status, COUNT(*) GROUP BY status")
			}
			reportRecordsPerSec(b, 1_000_000)
		})
	})

	b.Run("GroupByMultiKey", func(b *testing.B) {
		b.Run("100K", func(b *testing.B) {
			data := generateFixture(b, "orders", 100_000)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runDBSPA(b, data, "SELECT product, region, COUNT(*), SUM(total) GROUP BY product, region")
			}
			reportRecordsPerSec(b, 100_000)
		})
		b.Run("1M", func(b *testing.B) {
			data := generateFixture(b, "orders", 1_000_000)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runDBSPA(b, data, "SELECT product, region, COUNT(*), SUM(total) GROUP BY product, region")
			}
			reportRecordsPerSec(b, 1_000_000)
		})
	})

	b.Run("GroupByWithFilter", func(b *testing.B) {
		b.Run("1M", func(b *testing.B) {
			data := generateFixture(b, "orders", 1_000_000)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runDBSPA(b, data, "SELECT product, region, COUNT(*), SUM(total) WHERE status = 'confirmed' GROUP BY product, region")
			}
			reportRecordsPerSec(b, 1_000_000)
		})
		b.Run("1M/NullSink", func(b *testing.B) {
			data := generateFixture(b, "orders", 1_000_000)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runDBSPANull(b, data, "SELECT product, region, COUNT(*), SUM(total) WHERE status = 'confirmed' GROUP BY product, region")
			}
			reportRecordsPerSec(b, 1_000_000)
		})
	})

	b.Run("CDC", func(b *testing.B) {
		b.Run("NDJSON/100K", func(b *testing.B) {
			data := generateFixture(b, "orders-cdc", 100_000)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runDBSPA(b, data, "SELECT _after->>'status' AS status, COUNT(*) GROUP BY _after->>'status' CHANGELOG DEBEZIUM")
			}
			reportRecordsPerSec(b, 100_000)
		})
		b.Run("Avro/100K", func(b *testing.B) {
			data := generateFixtureFormat(b, "orders-cdc", 100_000, "debezium-avro")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runDBSPA(b, data, "SELECT status, COUNT(*) GROUP BY status FORMAT AVRO CHANGELOG DEBEZIUM")
			}
			reportRecordsPerSec(b, 100_000)
		})
	})

	b.Run("SelectiveDecode", func(b *testing.B) {
		// SELECT <single column> uses token-based selective decode (skips unused fields).
		// Measures decode optimization gains on wide records.
		b.Run("100K", func(b *testing.B) {
			data := generateFixture(b, "orders", 100_000)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runDBSPA(b, data, "SELECT order_id")
			}
			reportRecordsPerSec(b, 100_000)
		})
	})
}

// =====================================================
// BenchmarkFormat: wire format comparison
// =====================================================
//
// Hierarchy: BenchmarkFormat/<format>/<query>/100K
//
// All use 100K records. Compare formats within the same query type
// to understand encoding overhead (NDJSON parse vs Avro decode vs Protobuf decode).

func BenchmarkFormat(b *testing.B) {
	formats := []struct {
		name    string
		genFmt  string
		sqlSufx string // appended to each SQL query
		isFile  bool   // use --input (Parquet)
	}{
		{"NDJSON", "ndjson", "", false},
		{"Avro", "avro", " FORMAT AVRO", false},
		{"Protobuf", "protobuf", " FORMAT PROTOBUF", false},
		{"ProtoTyped", "proto-typed", " FORMAT PROTOBUF(message='Order')", false},
		{"Parquet", "parquet", " FORMAT PARQUET", true},
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"Passthrough", "SELECT *"},
		{"Filter", "SELECT * WHERE status = 'confirmed'"},
		{"GroupBy", "SELECT product, region, COUNT(*), SUM(total) GROUP BY product, region"},
	}

	for _, fmt := range formats {
		fmt := fmt
		b.Run(fmt.name, func(b *testing.B) {
			for _, q := range queries {
				q := q
				b.Run(q.name+"/100K", func(b *testing.B) {
					sql := q.sql + fmt.sqlSufx
					if fmt.isFile {
						path := generateParquetFile(b, "orders", 100_000)
						b.SetBytes(0) // file size varies
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							runDBSPAFile(b, path, sql)
						}
					} else {
						data := generateFixtureFormat(b, "orders", 100_000, fmt.genFmt)
						b.SetBytes(int64(len(data)))
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							runDBSPA(b, data, sql)
						}
					}
					reportRecordsPerSec(b, 100_000)
				})
			}
		})
	}
}

// =====================================================
// BenchmarkJoin: stream-to-table join scenarios
// =====================================================
//
// Hierarchy: BenchmarkJoin/<scenario>[/NullSink]
//
// SmallTable: 100-row lookup table (fits in L1/L2 cache)
// LargeTable: 10,000-row lookup table (more realistic reference data)
// WithGroupBy: join + GROUP BY (accumulating path)

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

func BenchmarkJoin(b *testing.B) {
	b.Run("SmallTable/100K", func(b *testing.B) {
		tableFile := generateTableFile(b, 100)
		streamData := generateStreamData(b, 100_000, 100)
		sql := fmt.Sprintf("SELECT e.action, u.tier FROM stdin e JOIN '%s' u ON e.customer_id = u.id", tableFile)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			runDBSPA(b, streamData, sql)
		}
		reportRecordsPerSec(b, 100_000)
	})

	b.Run("LargeTable/100K", func(b *testing.B) {
		tableFile := generateTableFile(b, 10_000)
		streamData := generateStreamData(b, 100_000, 10_000)
		sql := fmt.Sprintf("SELECT e.action, u.tier FROM stdin e JOIN '%s' u ON e.customer_id = u.id", tableFile)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			runDBSPA(b, streamData, sql)
		}
		reportRecordsPerSec(b, 100_000)
	})

	b.Run("WithGroupBy/100K", func(b *testing.B) {
		tableFile := generateTableFile(b, 100)
		streamData := generateStreamData(b, 100_000, 100)
		sql := fmt.Sprintf("SELECT u.tier, COUNT(*) AS cnt, SUM(e.value) AS total FROM stdin e JOIN '%s' u ON e.customer_id = u.id GROUP BY u.tier", tableFile)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			runDBSPA(b, streamData, sql)
		}
		reportRecordsPerSec(b, 100_000)
	})

	b.Run("WithGroupBy/100K/NullSink", func(b *testing.B) {
		tableFile := generateTableFile(b, 100)
		streamData := generateStreamData(b, 100_000, 100)
		sql := fmt.Sprintf("SELECT u.tier, COUNT(*) AS cnt, SUM(e.value) AS total FROM stdin e JOIN '%s' u ON e.customer_id = u.id GROUP BY u.tier", tableFile)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			runDBSPANull(b, streamData, sql)
		}
		reportRecordsPerSec(b, 100_000)
	})
}

// =====================================================
// BenchmarkCompaction: batch compaction efficiency
// =====================================================
//
// Hierarchy: BenchmarkCompaction/<scenario>/100K
//
// HighDuplication: 10 group keys across 100K records — compaction reduces
//   ~10K batches to 10 keys, aggregation becomes nearly free.
// LowDuplication: unique key per record — no compaction benefit, measures
//   raw accumulation overhead with zero redundancy.

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

func BenchmarkCompaction(b *testing.B) {
	b.Run("HighDuplication/100K", func(b *testing.B) {
		data := generateBatchData(b, 100_000, true)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			runDBSPA(b, data, "SELECT g, COUNT(*) AS cnt, SUM(v) AS total GROUP BY g")
		}
		reportRecordsPerSec(b, 100_000)
	})

	b.Run("LowDuplication/100K", func(b *testing.B) {
		data := generateBatchData(b, 100_000, false)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			runDBSPA(b, data, "SELECT g, COUNT(*) AS cnt, SUM(v) AS total GROUP BY g")
		}
		reportRecordsPerSec(b, 100_000)
	})
}
