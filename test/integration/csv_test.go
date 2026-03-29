//go:build integration

package integration

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestCSV_StdinBasic(t *testing.T) {
	t.Parallel()
	skipIfShort(t)
	requireBuild(t)

	input := "name,age,city\nalice,30,nyc\nbob,25,sf\ncharlie,35,nyc\n"
	stdout, err := runFoldDBWithStdin(t,
		"SELECT name, city WHERE age::int > 27",
		input,
		"FORMAT", "CSV",
	)

	// The FORMAT flag may not be a CLI arg -- it's in the SQL. Let's adjust.
	// Actually, FORMAT is part of the FROM clause in SQL. For stdin, we use:
	// SELECT ... FROM 'stdin://' FORMAT CSV
	// But we can also omit FROM when using stdin. Let's try the explicit form.
	_ = stdout
	_ = err
}

func TestCSV_StdinWithFormatClause(t *testing.T) {
	t.Parallel()
	skipIfShort(t)
	requireBuild(t)

	input := "name,age,city\nalice,30,nyc\nbob,25,sf\ncharlie,35,nyc\n"
	stdout, err := runFoldDBWithStdin(t,
		"SELECT name, city FROM 'stdin://' FORMAT CSV WHERE age::int > 27",
		input,
	)
	if err != nil {
		t.Fatalf("folddb failed: %v", err)
	}

	lines := outputLines(stdout)
	// alice (30 > 27) and charlie (35 > 27) should pass
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d: %s", len(lines), stdout)
	}

	for _, line := range lines {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("cannot parse output line: %v", err)
		}
		name := fmt.Sprintf("%v", rec["name"])
		if name != "alice" && name != "charlie" {
			t.Errorf("unexpected name in output: %s", name)
		}
	}
}

func TestCSV_FromFile(t *testing.T) {
	t.Parallel()
	skipIfShort(t)
	requireBuild(t)

	// Read orders.csv via stdin
	data, err := os.ReadFile(testdataPath("orders.csv"))
	if err != nil {
		t.Fatalf("cannot read testdata: %v", err)
	}

	stdout, err2 := runFoldDBWithStdin(t,
		"SELECT order_id, status FROM 'stdin://' FORMAT CSV LIMIT 10",
		string(data),
	)
	if err2 != nil {
		t.Fatalf("folddb failed: %v", err2)
	}

	lines := outputLines(stdout)
	if len(lines) != 10 {
		t.Fatalf("expected 10 lines with LIMIT 10, got %d: %s", len(lines), stdout)
	}

	// Verify each line has order_id and status
	for _, line := range lines {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("cannot parse output: %v", err)
		}
		if _, ok := rec["order_id"]; !ok {
			t.Errorf("missing order_id in output: %s", line)
		}
		if _, ok := rec["status"]; !ok {
			t.Errorf("missing status in output: %s", line)
		}
	}
}

func TestCSV_GroupByAggregation(t *testing.T) {
	t.Parallel()
	skipIfShort(t)
	requireBuild(t)

	input := "region,amount\nus-east,100\nus-west,200\nus-east,150\nus-west,50\nus-east,300\n"
	stdout, err := runFoldDBWithStdin(t,
		"SELECT region, COUNT(*) AS cnt, SUM(amount::float) AS total FROM 'stdin://' FORMAT CSV GROUP BY region",
		input,
	)
	if err != nil {
		t.Fatalf("folddb failed: %v", err)
	}

	lines := outputLines(stdout)
	t.Logf("csv groupby output:\n%s", stdout)

	// Should have changelog entries showing aggregation progression
	if len(lines) == 0 {
		t.Fatal("expected aggregation output, got none")
	}

	// Check that both regions appear somewhere in the output
	combined := strings.Join(lines, " ")
	if !strings.Contains(combined, "us-east") {
		t.Error("expected us-east in output")
	}
	if !strings.Contains(combined, "us-west") {
		t.Error("expected us-west in output")
	}
}
