package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/kevin-cantwell/folddb/internal/engine"
	"github.com/kevin-cantwell/folddb/internal/format"
	"github.com/kevin-cantwell/folddb/internal/sink"
	"github.com/kevin-cantwell/folddb/internal/sql/parser"
)

// runQuery is a test helper that simulates the main pipeline:
// parse SQL, decode JSON input lines, run pipeline, collect output.
func runQuery(t *testing.T, sql string, inputLines []string) []map[string]any {
	t.Helper()

	p := parser.New(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	pipeline := &engine.Pipeline{
		Columns: stmt.Columns,
		Where:   stmt.Where,
	}

	dec := &format.JSONDecoder{}
	var outBuf bytes.Buffer
	snk := &sink.JSONSink{Writer: &outBuf}

	recordCh := make(chan engine.Record)
	outputCh := make(chan engine.Record)

	// Feed input records
	go func() {
		defer close(recordCh)
		for _, line := range inputLines {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			rec, err := dec.Decode([]byte(line))
			if err != nil {
				t.Errorf("decode error: %v", err)
				continue
			}
			recordCh <- rec
		}
	}()

	// Run pipeline
	go func() {
		pipeline.Process(recordCh, outputCh)
	}()

	// Collect output
	limit := stmt.Limit
	count := 0
	for rec := range outputCh {
		if err := snk.Write(rec); err != nil {
			t.Fatalf("sink error: %v", err)
		}
		count++
		if limit != nil && count >= *limit {
			break
		}
	}

	// Parse output
	var results []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(outBuf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("output parse error: %v\nline: %s", err, line)
		}
		results = append(results, m)
	}
	return results
}

// runAggQuery is a test helper for accumulating (GROUP BY) queries.
// Returns changelog NDJSON output as parsed maps.
func runAggQuery(t *testing.T, sql string, inputLines []string) []map[string]any {
	t.Helper()

	p := parser.New(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if stmt.GroupBy == nil {
		t.Fatal("runAggQuery called on non-accumulating query")
	}

	aggCols, err := engine.ParseAggColumns(stmt.Columns, stmt.GroupBy)
	if err != nil {
		t.Fatalf("aggregate setup error: %v", err)
	}

	columnOrder := make([]string, len(aggCols))
	for i, col := range aggCols {
		columnOrder[i] = col.Alias
	}

	aggOp := engine.NewAggregateOp(aggCols, stmt.GroupBy, stmt.Having)

	dec := &format.JSONDecoder{}
	var outBuf bytes.Buffer
	snk := &sink.ChangelogSink{Writer: &outBuf, ColumnOrder: columnOrder}

	filteredCh := make(chan engine.Record)
	aggOutCh := make(chan engine.Record)

	// Feed input records with WHERE filter
	go func() {
		defer close(filteredCh)
		for _, line := range inputLines {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			rec, err := dec.Decode([]byte(line))
			if err != nil {
				t.Errorf("decode error: %v", err)
				continue
			}
			if stmt.Where != nil {
				pass, err := engine.Filter(stmt.Where, rec)
				if err != nil || !pass {
					continue
				}
			}
			filteredCh <- rec
		}
	}()

	// Run aggregate
	go func() {
		aggOp.Process(filteredCh, aggOutCh)
	}()

	// Collect output
	for rec := range aggOutCh {
		if err := snk.Write(rec); err != nil {
			t.Fatalf("sink error: %v", err)
		}
	}

	// Parse output
	var results []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(outBuf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("output parse error: %v\nline: %s", err, line)
		}
		results = append(results, m)
	}
	return results
}

func TestE2EGroupByBasic(t *testing.T) {
	input := []string{
		`{"status":"pending","amount":100}`,
		`{"status":"complete","amount":200}`,
		`{"status":"pending","amount":50}`,
	}
	results := runAggQuery(t, `SELECT status, COUNT(*) AS cnt, SUM(amount) AS total GROUP BY status`, input)

	// Expected changelog:
	// +pending cnt=1 total=100
	// +complete cnt=1 total=200
	// -pending cnt=1 total=100
	// +pending cnt=2 total=150
	if len(results) != 4 {
		t.Fatalf("expected 4 changelog lines, got %d: %v", len(results), results)
	}
	if results[0]["op"] != "+" || results[0]["status"] != "pending" {
		t.Errorf("unexpected line 0: %v", results[0])
	}
	if results[1]["op"] != "+" || results[1]["status"] != "complete" {
		t.Errorf("unexpected line 1: %v", results[1])
	}
	if results[2]["op"] != "-" || results[2]["status"] != "pending" {
		t.Errorf("unexpected line 2: %v", results[2])
	}
	if results[3]["op"] != "+" || results[3]["status"] != "pending" {
		t.Errorf("unexpected line 3: %v", results[3])
	}
	// Check final pending values
	if results[3]["cnt"] != float64(2) {
		t.Errorf("expected cnt=2, got %v", results[3]["cnt"])
	}
	if results[3]["total"] != float64(150) {
		t.Errorf("expected total=150, got %v", results[3]["total"])
	}
}

func TestE2EGroupByAvgMinMax(t *testing.T) {
	input := []string{
		`{"name":"alice","score":90}`,
		`{"name":"bob","score":85}`,
		`{"name":"alice","score":95}`,
	}
	results := runAggQuery(t,
		`SELECT name, AVG(score) AS avg_score, MIN(score) AS min_score, MAX(score) AS max_score GROUP BY name`,
		input)

	// Last line should be alice with avg=92.5, min=90, max=95
	last := results[len(results)-1]
	if last["name"] != "alice" {
		t.Errorf("expected alice, got %v", last["name"])
	}
	if last["avg_score"] != 92.5 {
		t.Errorf("expected avg 92.5, got %v", last["avg_score"])
	}
	if last["min_score"] != float64(90) {
		t.Errorf("expected min 90, got %v", last["min_score"])
	}
	if last["max_score"] != float64(95) {
		t.Errorf("expected max 95, got %v", last["max_score"])
	}
}

func TestE2EGroupByHaving(t *testing.T) {
	input := []string{
		`{"city":"nyc","sales":100}`,
		`{"city":"sf","sales":50}`,
		`{"city":"nyc","sales":200}`,
		`{"city":"sf","sales":25}`,
	}
	results := runAggQuery(t,
		`SELECT city, SUM(sales) AS total GROUP BY city HAVING SUM(sales) > 100`,
		input)

	// Only NYC should appear (total 300 > 100)
	var inserts []map[string]any
	for _, r := range results {
		if r["op"] == "+" {
			inserts = append(inserts, r)
		}
	}
	if len(inserts) != 1 {
		t.Fatalf("expected 1 insert, got %d: %v", len(inserts), inserts)
	}
	if inserts[0]["city"] != "nyc" {
		t.Errorf("expected nyc, got %v", inserts[0]["city"])
	}
	if inserts[0]["total"] != float64(300) {
		t.Errorf("expected 300, got %v", inserts[0]["total"])
	}
}

func TestE2ESelectWithWhere(t *testing.T) {
	// TC-PARSER-001
	input := []string{
		`{"name":"alice","age":30}`,
		`{"name":"bob","age":20}`,
	}
	results := runQuery(t, `SELECT name WHERE age > 25`, input)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0]["name"] != "alice" {
		t.Errorf("got name=%v, want alice", results[0]["name"])
	}
}

func TestE2EMultipleRecordsSomeFiltered(t *testing.T) {
	input := []string{
		`{"x":1}`,
		`{"x":2}`,
		`{"x":3}`,
		`{"x":4}`,
		`{"x":5}`,
	}
	results := runQuery(t, `SELECT x WHERE x > 3`, input)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

func TestE2ESelectWithAlias(t *testing.T) {
	input := []string{`{"name":"alice"}`}
	results := runQuery(t, `SELECT name AS username`, input)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0]["username"] != "alice" {
		t.Errorf("got %v, want alice", results[0]["username"])
	}
}

func TestE2ESelectStar(t *testing.T) {
	input := []string{`{"a":1,"b":2}`}
	results := runQuery(t, `SELECT *`, input)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	// Both a and b should be present
	if results[0]["a"] == nil || results[0]["b"] == nil {
		t.Errorf("expected both a and b: %v", results[0])
	}
}

func TestE2EArithmeticExpression(t *testing.T) {
	// TC-PARSER-004 / TC-PARSER-005
	input := []string{`{"a":2,"b":3,"c":4}`}
	results := runQuery(t, `SELECT a + b * c AS result`, input)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	// a + b*c = 2 + 12 = 14
	val, ok := results[0]["result"].(float64) // JSON numbers are float64
	if !ok {
		t.Fatalf("expected number, got %T: %v", results[0]["result"], results[0]["result"])
	}
	if val != 14 {
		t.Errorf("got %v, want 14", val)
	}
}

func TestE2EJsonAccess(t *testing.T) {
	// TC-PARSER-007
	input := []string{`{"payload":{"user":{"email":"a@b.com"}}}`}
	results := runQuery(t, `SELECT payload->'user'->>'email' AS email`, input)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0]["email"] != "a@b.com" {
		t.Errorf("got %v, want a@b.com", results[0]["email"])
	}
}

func TestE2ETypeCast(t *testing.T) {
	// TC-PARSER-009
	input := []string{`{"val":"42"}`}
	results := runQuery(t, `SELECT val::INT AS num`, input)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	// After casting to INT, the JSON output should be a number
	val, ok := results[0]["num"].(float64) // JSON marshal yields float64
	if !ok {
		t.Fatalf("expected number, got %T: %v", results[0]["num"], results[0]["num"])
	}
	if val != 42 {
		t.Errorf("got %v, want 42", val)
	}
}

func TestE2ELikeFilter(t *testing.T) {
	input := []string{
		`{"n":"alice"}`,
		`{"n":"bob"}`,
		`{"n":"alicia"}`,
	}
	results := runQuery(t, `SELECT n WHERE n LIKE 'ali%'`, input)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d: %v", len(results), results)
	}
}

func TestE2ECaseWhen(t *testing.T) {
	input := []string{`{"x":5}`}
	results := runQuery(t, `SELECT CASE WHEN x > 3 THEN 'big' ELSE 'small' END AS label`, input)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0]["label"] != "big" {
		t.Errorf("got %v, want big", results[0]["label"])
	}
}

func TestE2ECoalesce(t *testing.T) {
	input := []string{`{"a":null,"b":null,"c":42}`}
	results := runQuery(t, `SELECT COALESCE(a, b, c) AS result`, input)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	val, ok := results[0]["result"].(float64)
	if !ok {
		t.Fatalf("expected number, got %T", results[0]["result"])
	}
	if val != 42 {
		t.Errorf("got %v, want 42", val)
	}
}

func TestE2EIsNull(t *testing.T) {
	input := []string{
		`{"x":1}`,
		`{"x":null}`,
	}
	results := runQuery(t, `SELECT x WHERE x IS NOT NULL`, input)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func TestE2EBetween(t *testing.T) {
	input := []string{
		`{"x":1}`,
		`{"x":5}`,
		`{"x":10}`,
	}
	results := runQuery(t, `SELECT x WHERE x BETWEEN 3 AND 8`, input)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func TestE2EInOperator(t *testing.T) {
	input := []string{
		`{"s":"a"}`,
		`{"s":"b"}`,
		`{"s":"c"}`,
	}
	results := runQuery(t, `SELECT s WHERE s IN ('a', 'c')`, input)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

func TestE2EConcat(t *testing.T) {
	// Note: "last" is a keyword (LAST aggregate), so use "surname" instead
	input := []string{`{"fname":"Alice","surname":"Smith"}`}
	results := runQuery(t, `SELECT fname || ' ' || surname AS full_name`, input)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0]["full_name"] != "Alice Smith" {
		t.Errorf("got %v, want 'Alice Smith'", results[0]["full_name"])
	}
}

func TestE2ELimit(t *testing.T) {
	var input []string
	for i := 0; i < 20; i++ {
		input = append(input, `{"x":1}`)
	}
	results := runQuery(t, `SELECT x LIMIT 5`, input)
	if len(results) != 5 {
		t.Errorf("expected 5 results, got %d", len(results))
	}
}

func TestE2EEmptyInput(t *testing.T) {
	results := runQuery(t, `SELECT x`, nil)
	if len(results) != 0 {
		t.Errorf("expected 0 results for empty input, got %d", len(results))
	}
}

func TestE2ENullInWhereFiltersOut(t *testing.T) {
	// NULL = NULL should not pass WHERE
	input := []string{`{"a":null}`}
	results := runQuery(t, `SELECT a WHERE a = NULL`, input)
	if len(results) != 0 {
		t.Errorf("expected 0 results (NULL = NULL is NULL, not true), got %d", len(results))
	}
}

func TestE2EDistinctKeyword(t *testing.T) {
	// DISTINCT parses but without accumulator it won't dedup yet;
	// verify it at least parses and runs without error
	input := []string{
		`{"x":1}`,
		`{"x":2}`,
	}
	results := runQuery(t, `SELECT DISTINCT x`, input)
	// Without dedup implementation, both should pass through
	if len(results) < 1 {
		t.Error("expected at least 1 result")
	}
}
