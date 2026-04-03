// Command stress runs the DBSPA stress test suite.
//
// This is a standalone binary that exercises dbspa with adversarial data patterns,
// sustained load, and boundary conditions. It monitors RSS and throughput over time
// to detect memory leaks and performance degradation.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"
)

type SuiteResult struct {
	Timestamp string          `json:"timestamp"`
	DBSPAVer string          `json:"dbspa_version"`
	GoVersion string          `json:"go_version"`
	OS        string          `json:"os"`
	Arch      string          `json:"arch"`
	CPU       string          `json:"cpu"`
	Results   []*StressResult `json:"results"`
}

func main() {
	var (
		dbspaBin  string
		genBin     string
		duration   string
		outputFile string
		filter     string
	)

	flag.StringVar(&dbspaBin, "dbspa", "", "Path to dbspa binary")
	flag.StringVar(&genBin, "dbspa-gen", "", "Path to dbspa-gen binary")
	flag.StringVar(&duration, "duration", "5m", "Duration for sustained tests")
	flag.StringVar(&outputFile, "output", "", "Write JSON results to file")
	flag.StringVar(&filter, "scenarios", "", "Regex filter for scenario names")
	flag.Parse()

	if dbspaBin == "" {
		dbspaBin = findBinary("dbspa")
	}
	if genBin == "" {
		genBin = findBinary("dbspa-gen")
	}

	for _, bin := range []string{dbspaBin} {
		if _, err := os.Stat(bin); err != nil {
			fatalf("binary not found: %s (run 'make build' first)", bin)
		}
	}

	dur, err := time.ParseDuration(duration)
	if err != nil {
		fatalf("invalid duration %q: %v", duration, err)
	}

	tmpDir, err := os.MkdirTemp("", "dbspa-stress-*")
	if err != nil {
		fatalf("cannot create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &Config{
		DBSPABin: dbspaBin,
		GenBin:    genBin,
		Duration:  dur,
		TempDir:   tmpDir,
	}

	fmt.Fprintf(os.Stderr, "dbspa:   %s\n", dbspaBin)
	fmt.Fprintf(os.Stderr, "tempdir:  %s\n", tmpDir)
	fmt.Fprintf(os.Stderr, "\n")

	var filterRe *regexp.Regexp
	if filter != "" {
		filterRe, err = regexp.Compile(filter)
		if err != nil {
			fatalf("invalid --scenarios regex: %v", err)
		}
	}

	suite := &SuiteResult{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		DBSPAVer: getDBSPAVersion(dbspaBin),
		GoVersion: runtime.Version(),
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
		CPU:       getCPUName(),
	}

	scenarios := AllScenarios()
	passed, failed, skipped := 0, 0, 0

	fmt.Fprintf(os.Stderr, "=== DBSPA Stress Tests ===\n\n")

	for _, s := range scenarios {
		if filterRe != nil && !filterRe.MatchString(s.Name) {
			skipped++
			continue
		}

		fmt.Fprintf(os.Stderr, "--- %s ", s.Name)

		ctx := context.Background()
		result, err := s.Run(ctx, cfg)
		if err != nil {
			result = &StressResult{Name: s.Name, Error: err.Error()}
		}
		result.Name = s.Name

		// Strip time series from display (keep in JSON output)
		if result.Passed {
			passed++
			fmt.Fprintf(os.Stderr, "PASS (%s, %.0f rec/sec, RSS peak %.0fMB)\n",
				result.Duration, result.ThroughputAvg, result.PeakRSSMB)
		} else {
			failed++
			fmt.Fprintf(os.Stderr, "FAIL (%s)\n", result.Duration)
			if result.Error != "" {
				// Truncate long errors
				errMsg := result.Error
				if len(errMsg) > 200 {
					errMsg = errMsg[:200] + "..."
				}
				fmt.Fprintf(os.Stderr, "     %s\n", errMsg)
			}
		}
		if result.Details != "" {
			fmt.Fprintf(os.Stderr, "     %s\n", result.Details)
		}

		suite.Results = append(suite.Results, result)
	}

	// Summary
	fmt.Fprintf(os.Stderr, "\n=== Summary ===\n")
	fmt.Fprintf(os.Stderr, "  PASS: %d  FAIL: %d  SKIP: %d\n", passed, failed, skipped)

	// Write JSON output
	if outputFile != "" {
		data, _ := json.MarshalIndent(suite, "", "  ")
		os.MkdirAll(filepath.Dir(outputFile), 0o755)
		if err := os.WriteFile(outputFile, data, 0o644); err != nil {
			fatalf("write output: %v", err)
		}
		fmt.Fprintf(os.Stderr, "  Results: %s\n", outputFile)
	}

	if failed > 0 {
		os.Exit(1)
	}
}

func findBinary(name string) string {
	candidates := []string{
		filepath.Join("..", name),
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

func getDBSPAVersion(bin string) string {
	out, err := exec.Command(bin, "version").Output()
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
	return fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FATAL: "+format+"\n", args...)
	os.Exit(1)
}
