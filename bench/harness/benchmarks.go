// Command bench-harness runs the DBSPA benchmark suite and outputs JSON results.
//
// This is a standalone program (not go test -bench) for full control over
// fixture generation, measurement, and output format.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

// --- Result types ---

type BenchmarkResult struct {
	Name          string  `json:"name"`
	Records       int     `json:"records"`
	Runs          []int64 `json:"runs"`
	MedianMS      int64   `json:"median_ms"`
	RecordsPerSec int64   `json:"records_per_sec"`
	InputBytes    int64   `json:"input_bytes"`
	MemoryMB      float64 `json:"memory_mb,omitempty"`
}

type SuiteResult struct {
	Timestamp    string            `json:"timestamp"`
	DBSPAVer    string            `json:"dbspa_version"`
	GoVersion    string            `json:"go_version"`
	OS           string            `json:"os"`
	Arch         string            `json:"arch"`
	CPU          string            `json:"cpu"`
	Benchmarks   []BenchmarkResult `json:"benchmarks"`
}

// --- Global config ---

var (
	dbspaBin string
	genBin    string
	runsPerBench = 3
	recordCount  = 100_000
	tableSize    = 5_000
	withKafka    bool
	outputFile   string
	compareFile  string
)

func main() {
	flag.StringVar(&dbspaBin, "dbspa", "", "Path to dbspa binary (auto-detected if empty)")
	flag.StringVar(&genBin, "dbspa-gen", "", "Path to dbspa-gen binary (auto-detected if empty)")
	flag.IntVar(&runsPerBench, "runs", 3, "Number of runs per benchmark")
	flag.IntVar(&recordCount, "records", 100_000, "Number of records per benchmark")
	flag.BoolVar(&withKafka, "with-kafka", false, "Include Kafka benchmarks")
	flag.StringVar(&outputFile, "output", "", "Write JSON results to file (default: stdout)")
	flag.StringVar(&compareFile, "compare", "", "Compare results against baseline JSON file")
	flag.Parse()

	// Find binaries
	if dbspaBin == "" {
		dbspaBin = findBinary("dbspa")
	}
	if genBin == "" {
		genBin = findBinary("dbspa-gen")
	}

	// Verify binaries exist
	for _, bin := range []string{dbspaBin, genBin} {
		if _, err := os.Stat(bin); err != nil {
			fatalf("Binary not found: %s (run 'make build' first)", bin)
		}
	}

	fmt.Fprintf(os.Stderr, "dbspa:     %s\n", dbspaBin)
	fmt.Fprintf(os.Stderr, "dbspa-gen: %s\n", genBin)
	fmt.Fprintf(os.Stderr, "records:    %d\n", recordCount)
	fmt.Fprintf(os.Stderr, "runs:       %d\n", runsPerBench)
	fmt.Fprintf(os.Stderr, "\n")

	suite := &SuiteResult{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		DBSPAVer: getDBSPAVersion(),
		GoVersion: runtime.Version(),
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
		CPU:       getCPUName(),
	}

	// --- Generate fixtures ---
	fmt.Fprintf(os.Stderr, "Generating fixtures (seed=42)...\n")

	ndjsonData := generateFixture("orders", recordCount, "ndjson")
	avroData := generateFixture("orders", recordCount, "avro")
	cdcJSONData := generateFixture("orders-cdc", recordCount, "ndjson")
	cdcAvroData := generateFixture("orders-cdc", recordCount, "debezium-avro")
	parquetFile := generateParquetFixture("orders", recordCount)
	defer os.Remove(parquetFile)

	tableFile := generateTableFileFixture(tableSize)
	defer os.Remove(tableFile)
	streamData := generateStreamFixture(recordCount, tableSize)

	batchHighDup := generateBatchFixture(recordCount, true)
	batchLowDup := generateBatchFixture(recordCount, false)

	csvData := generateCSVFixture(recordCount)

	fmt.Fprintf(os.Stderr, "Fixtures ready.\n\n")

	// --- Format benchmarks ---

	suite.run("format/ndjson/passthrough", recordCount, ndjsonData, func() {
		runDBSPA(ndjsonData, "SELECT *")
	})

	suite.run("format/ndjson/filter", recordCount, ndjsonData, func() {
		runDBSPA(ndjsonData, "SELECT * WHERE status = 'confirmed'")
	})

	suite.run("format/ndjson/groupby", recordCount, ndjsonData, func() {
		runDBSPA(ndjsonData, "SELECT product, region, COUNT(*), SUM(total) GROUP BY product, region")
	})

	suite.run("format/avro/passthrough", recordCount, avroData, func() {
		runDBSPA(avroData, "SELECT * FORMAT AVRO")
	})

	suite.run("format/parquet/passthrough", recordCount, nil, func() {
		runDBSPAFile(parquetFile, "SELECT * FORMAT PARQUET")
	})

	suite.run("format/parquet/filter", recordCount, nil, func() {
		runDBSPAFile(parquetFile, "SELECT * WHERE status = 'confirmed' FORMAT PARQUET")
	})

	suite.run("format/parquet/groupby", recordCount, nil, func() {
		runDBSPAFile(parquetFile, "SELECT product, region, COUNT(*), SUM(total) GROUP BY product, region FORMAT PARQUET")
	})

	suite.run("format/csv/passthrough", recordCount, csvData, func() {
		runDBSPA(csvData, "SELECT * FORMAT CSV")
	})

	// --- CDC benchmarks ---

	suite.run("cdc/json/groupby", recordCount, cdcJSONData, func() {
		runDBSPA(cdcJSONData, "SELECT _after->>'status' AS status, COUNT(*) GROUP BY _after->>'status' FORMAT DEBEZIUM")
	})

	suite.run("cdc/avro/groupby", recordCount, cdcAvroData, func() {
		runDBSPA(cdcAvroData, "SELECT status, COUNT(*) GROUP BY status FORMAT DEBEZIUM_AVRO")
	})

	// --- Join benchmarks ---

	joinSQL := fmt.Sprintf("SELECT e.action, u.tier FROM stdin e JOIN '%s' u ON e.customer_id = u.id", tableFile)
	suite.run("join/file/simple", recordCount, streamData, func() {
		runDBSPA(streamData, joinSQL)
	})

	joinGroupBySQL := fmt.Sprintf("SELECT u.tier, COUNT(*) AS cnt, SUM(e.value) AS total FROM stdin e JOIN '%s' u ON e.customer_id = u.id GROUP BY u.tier", tableFile)
	suite.run("join/file/groupby", recordCount, streamData, func() {
		runDBSPA(streamData, joinGroupBySQL)
	})

	complexJoinSQL := fmt.Sprintf(`SELECT
		u.tier,
		e.action,
		COUNT(*) AS cnt,
		SUM(e.value) AS total_value,
		AVG(e.value) AS avg_value,
		MIN(e.value) AS min_value,
		MAX(e.value) AS max_value,
		SUM(CASE WHEN e.action = 'purchase' THEN e.value ELSE 0 END) AS purchase_value,
		SUM(CASE WHEN e.action = 'login' THEN 1 ELSE 0 END) AS login_count
	FROM stdin e
	JOIN '%s' u ON e.customer_id = u.id
	GROUP BY u.tier, e.action
	HAVING COUNT(*) > 10
	ORDER BY total_value DESC`, tableFile)
	suite.run("join/file/complex", recordCount, streamData, func() {
		runDBSPA(streamData, complexJoinSQL)
	})

	subquerySQL := fmt.Sprintf(`SELECT s.tier, s.cnt, e.action
	FROM stdin e
	JOIN (SELECT u.id, u.tier, COUNT(*) AS cnt FROM '%s' u GROUP BY u.id, u.tier) s
	ON e.customer_id = s.id`, tableFile)
	suite.run("join/subquery", recordCount, streamData, func() {
		runDBSPA(streamData, subquerySQL)
	})

	// --- Pipeline benchmarks ---

	suite.run("pipeline/batch_compaction", recordCount, batchHighDup, func() {
		runDBSPA(batchHighDup, "SELECT g, COUNT(*) AS cnt, SUM(v) AS total GROUP BY g")
	})

	suite.run("pipeline/operator_fusion", recordCount, batchLowDup, func() {
		runDBSPA(batchLowDup, "SELECT g, COUNT(*) AS cnt, SUM(v) AS total GROUP BY g")
	})

	// --- Kafka benchmarks ---

	if withKafka {
		runKafkaBenchmarks(suite)
	}

	// --- Output ---

	resultJSON, err := json.MarshalIndent(suite, "", "  ")
	if err != nil {
		fatalf("JSON marshal error: %v", err)
	}

	if outputFile != "" {
		if err := os.MkdirAll(filepath.Dir(outputFile), 0o755); err != nil {
			fatalf("Cannot create output directory: %v", err)
		}
		if err := os.WriteFile(outputFile, resultJSON, 0o644); err != nil {
			fatalf("Cannot write output: %v", err)
		}
		fmt.Fprintf(os.Stderr, "\nResults written to %s\n", outputFile)
	} else {
		fmt.Println(string(resultJSON))
	}

	// --- Comparison ---

	if compareFile != "" {
		compareBaseline(suite, compareFile)
	}
}

// --- Benchmark runner ---

func (s *SuiteResult) run(name string, records int, inputData []byte, fn func()) {
	fmt.Fprintf(os.Stderr, "  %-40s ", name)

	// Warmup run (not measured)
	fn()

	var durations []int64
	for i := 0; i < runsPerBench; i++ {
		// Force GC before each run for consistent memory
		runtime.GC()

		var memBefore runtime.MemStats
		runtime.ReadMemStats(&memBefore)

		start := time.Now()
		fn()
		elapsed := time.Since(start)

		durations = append(durations, elapsed.Milliseconds())
	}

	medianMS := median(durations)
	var rps int64
	if medianMS > 0 {
		rps = int64(float64(records) / (float64(medianMS) / 1000.0))
	}

	var inputBytes int64
	if inputData != nil {
		inputBytes = int64(len(inputData))
	}

	result := BenchmarkResult{
		Name:          name,
		Records:       records,
		Runs:          durations,
		MedianMS:      medianMS,
		RecordsPerSec: rps,
		InputBytes:    inputBytes,
	}
	s.Benchmarks = append(s.Benchmarks, result)

	fmt.Fprintf(os.Stderr, "%s  %s/sec\n",
		formatDuration(medianMS),
		formatCount(rps))
}

// --- Fixture generation ---

func generateFixture(dataset string, count int, format string) []byte {
	cmd := exec.Command(genBin, dataset, "--count", fmt.Sprintf("%d", count), "--seed", "42", "--format", format)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fatalf("dbspa-gen %s --format %s failed: %v", dataset, format, err)
	}
	return out.Bytes()
}

func generateParquetFixture(dataset string, count int) string {
	f, err := os.CreateTemp("", "bench-*.parquet")
	if err != nil {
		fatalf("cannot create temp file: %v", err)
	}
	path := f.Name()
	f.Close()

	cmd := exec.Command(genBin, dataset, "--count", fmt.Sprintf("%d", count), "--seed", "42", "--format", "parquet", "--output", path)
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fatalf("dbspa-gen parquet failed: %v", err)
	}
	return path
}

func generateTableFileFixture(size int) string {
	f, err := os.CreateTemp("", "bench-table-*.ndjson")
	if err != nil {
		fatalf("cannot create temp file: %v", err)
	}
	defer f.Close()

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
			fatalf("write table error: %v", err)
		}
	}
	return f.Name()
}

func generateStreamFixture(count, tblSize int) []byte {
	rng := rand.New(rand.NewSource(42))
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	actions := []string{"login", "purchase", "click", "view", "logout"}
	for i := 0; i < count; i++ {
		rec := map[string]any{
			"customer_id": rng.Intn(tblSize) + 1,
			"action":      actions[rng.Intn(len(actions))],
			"value":       rng.Intn(1000),
		}
		if err := enc.Encode(rec); err != nil {
			fatalf("stream encode error: %v", err)
		}
	}
	return buf.Bytes()
}

func generateBatchFixture(count int, highDuplication bool) []byte {
	rng := rand.New(rand.NewSource(42))
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)

	if highDuplication {
		groups := []string{"g0", "g1", "g2", "g3", "g4", "g5", "g6", "g7", "g8", "g9"}
		for i := 0; i < count; i++ {
			rec := map[string]any{
				"g": groups[rng.Intn(len(groups))],
				"v": rng.Intn(1000),
			}
			enc.Encode(rec)
		}
	} else {
		for i := 0; i < count; i++ {
			rec := map[string]any{
				"g": fmt.Sprintf("g_%d", i),
				"v": rng.Intn(1000),
			}
			enc.Encode(rec)
		}
	}
	return buf.Bytes()
}

func generateCSVFixture(count int) []byte {
	rng := rand.New(rand.NewSource(42))
	var buf bytes.Buffer

	// CSV header
	buf.WriteString("order_id,customer_id,product,quantity,price,total,status,region\n")

	products := []string{"widget-a", "widget-b", "gadget-x", "gadget-y", "doohickey", "thingamajig", "whatchamacallit"}
	statuses := []string{"pending", "confirmed", "shipped", "delivered", "cancelled"}
	regions := []string{"us-east", "us-west", "eu-west", "eu-central", "ap-south", "ap-east"}

	for i := 0; i < count; i++ {
		qty := rng.Intn(10) + 1
		price := float64(rng.Intn(9900)+100) / 100.0
		total := float64(qty) * price
		fmt.Fprintf(&buf, "%d,%d,%s,%d,%.2f,%.2f,%s,%s\n",
			i+1,
			rng.Intn(5000)+1,
			products[rng.Intn(len(products))],
			qty,
			price,
			total,
			statuses[rng.Intn(len(statuses))],
			regions[rng.Intn(len(regions))],
		)
	}
	return buf.Bytes()
}

// --- DBSPA execution ---

func runDBSPA(input []byte, sql string) []byte {
	cmd := exec.Command(dbspaBin, "query", sql)
	cmd.Stdin = bytes.NewReader(input)
	var out bytes.Buffer
	cmd.Stdout = &out
	var errBuf bytes.Buffer
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		fatalf("dbspa query failed: %v\nSQL: %s\nstderr: %s", err, sql, errBuf.String())
	}
	return out.Bytes()
}

func runDBSPAFile(inputFile string, sql string) []byte {
	cmd := exec.Command(dbspaBin, "query", "--input", inputFile, sql)
	var out bytes.Buffer
	cmd.Stdout = &out
	var errBuf bytes.Buffer
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		fatalf("dbspa --input failed: %v\nSQL: %s\nstderr: %s", err, sql, errBuf.String())
	}
	return out.Bytes()
}

// --- Kafka benchmarks ---

func runKafkaBenchmarks(suite *SuiteResult) {
	fmt.Fprintf(os.Stderr, "\n--- Kafka benchmarks ---\n")

	// Produce 100K Confluent Avro messages
	avroData := generateFixture("orders", recordCount, "avro")

	suite.run("kafka/produce", recordCount, avroData, func() {
		cmd := exec.Command(dbspaBin, "query",
			"SELECT * FORMAT AVRO",
			"--output", "kafka://localhost:9092/bench-orders",
		)
		cmd.Stdin = bytes.NewReader(avroData)
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "WARN: kafka/produce failed: %v\n", err)
		}
	})

	suite.run("kafka/consume/passthrough", recordCount, nil, func() {
		cmd := exec.Command(dbspaBin, "query",
			"SELECT * FROM 'kafka://localhost:9092/bench-orders' FORMAT AVRO",
			"--limit", fmt.Sprintf("%d", recordCount),
		)
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "WARN: kafka/consume/passthrough failed: %v\n", err)
		}
	})

	suite.run("kafka/consume/groupby", recordCount, nil, func() {
		cmd := exec.Command(dbspaBin, "query",
			"SELECT status, COUNT(*), SUM(total) FROM 'kafka://localhost:9092/bench-orders' FORMAT AVRO GROUP BY status",
			"--limit", fmt.Sprintf("%d", recordCount),
		)
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "WARN: kafka/consume/groupby failed: %v\n", err)
		}
	})

	// Burst: produce + consume concurrently
	suite.run("kafka/live/burst", recordCount*2, avroData, func() {
		// Start consumer first
		consumer := exec.Command(dbspaBin, "query",
			"SELECT * FROM 'kafka://localhost:9092/bench-burst' FORMAT AVRO",
			"--limit", fmt.Sprintf("%d", recordCount),
		)
		consumer.Stderr = os.Stderr
		var consumerOut bytes.Buffer
		consumer.Stdout = &consumerOut
		consumer.Start()

		// Produce
		producer := exec.Command(dbspaBin, "query",
			"SELECT * FORMAT AVRO",
			"--output", "kafka://localhost:9092/bench-burst",
		)
		producer.Stdin = bytes.NewReader(avroData)
		producer.Stderr = os.Stderr
		producer.Run()

		// Wait for consumer
		consumer.Wait()
	})
}

// --- Baseline comparison ---

func compareBaseline(current *SuiteResult, baselinePath string) {
	data, err := os.ReadFile(baselinePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\nWARN: Cannot read baseline %s: %v\n", baselinePath, err)
		return
	}

	var baseline SuiteResult
	if err := json.Unmarshal(data, &baseline); err != nil {
		fmt.Fprintf(os.Stderr, "\nWARN: Cannot parse baseline: %v\n", err)
		return
	}

	// Build lookup
	baseMap := make(map[string]BenchmarkResult)
	for _, b := range baseline.Benchmarks {
		baseMap[b.Name] = b
	}

	fmt.Fprintf(os.Stderr, "\n--- Baseline comparison (vs %s) ---\n", baselinePath)
	regressions := 0

	for _, cur := range current.Benchmarks {
		base, ok := baseMap[cur.Name]
		if !ok {
			fmt.Fprintf(os.Stderr, "  ? %-40s %s/sec (no baseline)\n", cur.Name, formatCount(cur.RecordsPerSec))
			continue
		}

		if base.RecordsPerSec == 0 {
			continue
		}

		pctChange := float64(cur.RecordsPerSec-base.RecordsPerSec) / float64(base.RecordsPerSec) * 100.0

		symbol := "+"
		status := "ok"
		marker := ""
		if pctChange < 0 {
			symbol = ""
		}
		if pctChange < -10 {
			status = "REGRESSION"
			marker = " REGRESSION"
			regressions++
		}
		_ = status

		icon := "✓"
		if marker != "" {
			icon = "✗"
		}

		fmt.Fprintf(os.Stderr, "  %s %-40s %s/sec (baseline: %s/sec, %s%.1f%%)%s\n",
			icon, cur.Name,
			formatCount(cur.RecordsPerSec),
			formatCount(base.RecordsPerSec),
			symbol, pctChange, marker)
	}

	if regressions > 0 {
		fmt.Fprintf(os.Stderr, "\n%d REGRESSION(S) DETECTED (>10%% slower than baseline)\n", regressions)
		os.Exit(1)
	} else {
		fmt.Fprintf(os.Stderr, "\nAll benchmarks within tolerance of baseline.\n")
	}
}

// --- Helpers ---

func median(vals []int64) int64 {
	if len(vals) == 0 {
		return 0
	}
	sorted := make([]int64, len(vals))
	copy(sorted, vals)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	mid := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[mid-1] + sorted[mid]) / 2
	}
	return sorted[mid]
}

func formatCount(n int64) string {
	if n >= 1_000_000 {
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	}
	if n >= 1_000 {
		return fmt.Sprintf("%.0fK", math.Round(float64(n)/1_000))
	}
	return fmt.Sprintf("%d", n)
}

func formatDuration(ms int64) string {
	if ms >= 1000 {
		return fmt.Sprintf("%.2fs", float64(ms)/1000.0)
	}
	return fmt.Sprintf("%dms", ms)
}

func findBinary(name string) string {
	// Look in project root (two levels up from bench/harness)
	candidates := []string{
		filepath.Join("..", "..", name),
		filepath.Join(".", name),
		name,
	}
	for _, c := range candidates {
		abs, err := filepath.Abs(c)
		if err != nil {
			continue
		}
		if _, err := os.Stat(abs); err == nil {
			return abs
		}
	}
	return name
}

func getDBSPAVersion() string {
	cmd := exec.Command(dbspaBin, "version")
	out, err := cmd.Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

func getCPUName() string {
	if runtime.GOOS == "darwin" {
		out, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output()
		if err == nil {
			return strings.TrimSpace(string(out))
		}
	}
	if runtime.GOOS == "linux" {
		out, err := exec.Command("bash", "-c", `grep -m1 'model name' /proc/cpuinfo | cut -d: -f2`).Output()
		if err == nil {
			return strings.TrimSpace(string(out))
		}
	}
	return fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FATAL: "+format+"\n", args...)
	os.Exit(1)
}
