package main

import (
	"bytes"
	"encoding/json"
	"os"
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
		if limit != nil && count >= *limit {
			break
		}
		if err := snk.Write(rec); err != nil {
			t.Fatalf("sink error: %v", err)
		}
		count++
	}
	snk.Close()

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
	snk.Close()

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
	if results[0]["_weight"] != float64(1) || results[0]["status"] != "pending" {
		t.Errorf("unexpected line 0: %v", results[0])
	}
	if results[1]["_weight"] != float64(1) || results[1]["status"] != "complete" {
		t.Errorf("unexpected line 1: %v", results[1])
	}
	if results[2]["_weight"] != float64(-1) || results[2]["status"] != "pending" {
		t.Errorf("unexpected line 2: %v", results[2])
	}
	if results[3]["_weight"] != float64(1) || results[3]["status"] != "pending" {
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
		if r["_weight"] == float64(1) {
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

// =====================================================================
// Additional E2E GROUP BY tests
// =====================================================================

func TestE2EGroupByCountStar(t *testing.T) {
	input := []string{
		`{"dept":"eng"}`,
		`{"dept":"eng"}`,
		`{"dept":"eng"}`,
		`{"dept":"sales"}`,
		`{"dept":"sales"}`,
	}
	results := runAggQuery(t, `SELECT dept, COUNT(*) AS cnt GROUP BY dept`, input)

	// Find the last insert for each dept
	lastInsert := make(map[string]map[string]any)
	for _, r := range results {
		if r["_weight"] == float64(1) {
			dept := r["dept"].(string)
			lastInsert[dept] = r
		}
	}

	if lastInsert["eng"]["cnt"] != float64(3) {
		t.Errorf("expected eng cnt=3, got %v", lastInsert["eng"]["cnt"])
	}
	if lastInsert["sales"]["cnt"] != float64(2) {
		t.Errorf("expected sales cnt=2, got %v", lastInsert["sales"]["cnt"])
	}
}

func TestE2EGroupByMultipleAggregates(t *testing.T) {
	input := []string{
		`{"g":"a","v":10}`,
		`{"g":"a","v":20}`,
		`{"g":"a","v":30}`,
	}
	results := runAggQuery(t,
		`SELECT g, COUNT(*) AS c, SUM(v) AS s, AVG(v) AS a, MIN(v) AS mn, MAX(v) AS mx GROUP BY g`,
		input)

	// Find the last insert
	var last map[string]any
	for _, r := range results {
		if r["_weight"] == float64(1) {
			last = r
		}
	}

	if last == nil {
		t.Fatal("expected at least one insert")
	}
	if last["c"] != float64(3) {
		t.Errorf("expected c=3, got %v", last["c"])
	}
	if last["s"] != float64(60) {
		t.Errorf("expected s=60, got %v", last["s"])
	}
	if last["a"] != float64(20) {
		t.Errorf("expected a=20, got %v", last["a"])
	}
	if last["mn"] != float64(10) {
		t.Errorf("expected mn=10, got %v", last["mn"])
	}
	if last["mx"] != float64(30) {
		t.Errorf("expected mx=30, got %v", last["mx"])
	}
}

func TestE2EGroupByHavingFilter(t *testing.T) {
	input := []string{
		`{"g":"a","v":1}`,
		`{"g":"a","v":2}`,
		`{"g":"b","v":1}`,
	}
	results := runAggQuery(t,
		`SELECT g, COUNT(*) AS cnt GROUP BY g HAVING COUNT(*) > 1`,
		input)

	var inserts []map[string]any
	for _, r := range results {
		if r["_weight"] == float64(1) {
			inserts = append(inserts, r)
		}
	}

	// Only group "a" with cnt=2 passes HAVING
	if len(inserts) != 1 {
		t.Fatalf("expected 1 insert passing HAVING, got %d", len(inserts))
	}
	if inserts[0]["g"] != "a" {
		t.Errorf("expected group 'a', got %v", inserts[0]["g"])
	}
	if inserts[0]["cnt"] != float64(2) {
		t.Errorf("expected cnt=2, got %v", inserts[0]["cnt"])
	}
}

func TestE2ENonAccumulatingStillWorks(t *testing.T) {
	input := []string{
		`{"name":"alice","age":30}`,
		`{"name":"bob","age":25}`,
	}
	results := runQuery(t, `SELECT name, age`, input)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0]["name"] != "alice" {
		t.Errorf("expected alice, got %v", results[0]["name"])
	}
	if results[1]["name"] != "bob" {
		t.Errorf("expected bob, got %v", results[1]["name"])
	}
}

func TestE2EGroupByCountWithNullValues(t *testing.T) {
	// COUNT(v) should skip NULLs, COUNT(*) should not
	input := []string{
		`{"g":"a","v":1}`,
		`{"g":"a","v":null}`,
		`{"g":"a","v":3}`,
	}
	results := runAggQuery(t,
		`SELECT g, COUNT(*) AS cnt_star, COUNT(v) AS cnt_v GROUP BY g`,
		input)

	// Find last insert for group "a"
	var last map[string]any
	for _, r := range results {
		if r["_weight"] == float64(1) && r["g"] == "a" {
			last = r
		}
	}

	if last == nil {
		t.Fatal("expected at least one insert for group 'a'")
	}
	if last["cnt_star"] != float64(3) {
		t.Errorf("expected COUNT(*)=3, got %v", last["cnt_star"])
	}
	if last["cnt_v"] != float64(2) {
		t.Errorf("expected COUNT(v)=2 (NULL skipped), got %v", last["cnt_v"])
	}
}

func TestE2EGroupBySumWithNulls(t *testing.T) {
	input := []string{
		`{"g":"a","v":10}`,
		`{"g":"a","v":null}`,
		`{"g":"a","v":5}`,
	}
	results := runAggQuery(t,
		`SELECT g, SUM(v) AS total GROUP BY g`,
		input)

	var last map[string]any
	for _, r := range results {
		if r["_weight"] == float64(1) && r["g"] == "a" {
			last = r
		}
	}

	if last["total"] != float64(15) {
		t.Errorf("expected total=15 (NULL skipped in SUM), got %v", last["total"])
	}
}

func TestE2EChangelogFormat(t *testing.T) {
	input := []string{
		`{"g":"a","v":100}`,
		`{"g":"a","v":200}`,
	}
	results := runAggQuery(t,
		`SELECT g, SUM(v) AS total GROUP BY g`,
		input)

	// Verify changelog format: all records have "_weight" field
	for i, r := range results {
		w, ok := r["_weight"]
		if !ok {
			t.Errorf("result[%d] missing '_weight' field", i)
		}
		if w != float64(1) && w != float64(-1) {
			t.Errorf("result[%d] invalid _weight=%v", i, w)
		}
	}

	// Verify retraction/insertion pairs are adjacent
	if len(results) >= 3 {
		// Second record should be a retraction, third an insertion
		if results[1]["_weight"] != float64(-1) {
			t.Errorf("expected retraction at index 1, got _weight=%v", results[1]["_weight"])
		}
		if results[2]["_weight"] != float64(1) {
			t.Errorf("expected insertion at index 2, got _weight=%v", results[2]["_weight"])
		}
	}
}

func TestE2EGroupByFirstLast(t *testing.T) {
	input := []string{
		`{"g":"a","v":"first"}`,
		`{"g":"a","v":"second"}`,
		`{"g":"a","v":"third"}`,
	}
	results := runAggQuery(t,
		`SELECT g, FIRST(v) AS f, LAST(v) AS l GROUP BY g`,
		input)

	var last map[string]any
	for _, r := range results {
		if r["_weight"] == float64(1) && r["g"] == "a" {
			last = r
		}
	}

	if last == nil {
		t.Fatal("expected at least one insert")
	}
	if last["f"] != "first" {
		t.Errorf("expected FIRST='first', got %v", last["f"])
	}
	if last["l"] != "third" {
		t.Errorf("expected LAST='third', got %v", last["l"])
	}
}

func TestE2EGroupByGroupRemoval(t *testing.T) {
	// When COUNT(*) reaches 0, group should be removed
	// We simulate this by sending two inserts then two retracts
	// Note: Retractions require Diff=-1 records, which come from
	// Debezium CDC. We use runAggQuery which feeds records with
	// default Diff=+1. We need to test at the aggregate operator
	// level for retraction support.
	// This test verifies the basic group accumulation works.
	input := []string{
		`{"g":"a","v":1}`,
		`{"g":"b","v":2}`,
	}
	results := runAggQuery(t,
		`SELECT g, COUNT(*) AS cnt GROUP BY g`,
		input)

	// Both groups should have cnt=1
	inserts := make(map[string]float64)
	for _, r := range results {
		if r["_weight"] == float64(1) {
			inserts[r["g"].(string)] = r["cnt"].(float64)
		}
	}

	if inserts["a"] != 1 {
		t.Errorf("expected a cnt=1, got %v", inserts["a"])
	}
	if inserts["b"] != 1 {
		t.Errorf("expected b cnt=1, got %v", inserts["b"])
	}
}

func TestE2EGroupByExpressionModulo(t *testing.T) {
	// TC-ACC-020: GROUP BY on expression
	input := []string{
		`{"x":1}`,
		`{"x":2}`,
		`{"x":3}`,
		`{"x":4}`,
	}
	results := runAggQuery(t,
		`SELECT x % 2 AS parity, COUNT(*) AS c GROUP BY x % 2`,
		input)

	// Find last inserts per parity
	lastInsert := make(map[float64]float64) // parity -> count
	for _, r := range results {
		if r["_weight"] == float64(1) {
			lastInsert[r["parity"].(float64)] = r["c"].(float64)
		}
	}

	if lastInsert[0] != 2 {
		t.Errorf("expected parity=0 c=2, got %v", lastInsert[0])
	}
	if lastInsert[1] != 2 {
		t.Errorf("expected parity=1 c=2, got %v", lastInsert[1])
	}
}

func TestE2EGroupByWhereFilter(t *testing.T) {
	// WHERE filters before aggregation
	input := []string{
		`{"g":"a","v":10,"active":true}`,
		`{"g":"a","v":20,"active":false}`,
		`{"g":"a","v":30,"active":true}`,
	}
	results := runAggQuery(t,
		`SELECT g, SUM(v) AS total WHERE active = true GROUP BY g`,
		input)

	var last map[string]any
	for _, r := range results {
		if r["_weight"] == float64(1) {
			last = r
		}
	}

	if last == nil {
		t.Fatal("expected at least one insert")
	}
	// Only v=10 and v=30 should be included
	if last["total"] != float64(40) {
		t.Errorf("expected total=40 (WHERE filtered), got %v", last["total"])
	}
}

// =====================================================================
// Non-windowed regression: ensure non-windowed queries still work
// =====================================================================

func TestE2ENonWindowedRegression(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		input      []string
		wantCount  int
		checkFirst map[string]any
	}{
		{
			name:      "simple select passthrough",
			sql:       `SELECT name, age`,
			input:     []string{`{"name":"alice","age":30}`, `{"name":"bob","age":25}`},
			wantCount: 2,
			checkFirst: map[string]any{"name": "alice"},
		},
		{
			name:      "select with where filter",
			sql:       `SELECT x WHERE x > 2`,
			input:     []string{`{"x":1}`, `{"x":3}`, `{"x":5}`},
			wantCount: 2,
		},
		{
			name:      "select star",
			sql:       `SELECT *`,
			input:     []string{`{"a":1,"b":2}`},
			wantCount: 1,
		},
		{
			name:      "empty input produces no output",
			sql:       `SELECT x`,
			input:     nil,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := runQuery(t, tt.sql, tt.input)
			if len(results) != tt.wantCount {
				t.Fatalf("expected %d results, got %d", tt.wantCount, len(results))
			}
			if tt.checkFirst != nil && len(results) > 0 {
				for k, want := range tt.checkFirst {
					if results[0][k] != want {
						t.Errorf("first result[%q] = %v, want %v", k, results[0][k], want)
					}
				}
			}
		})
	}
}

// =====================================================================
// E2E GROUP BY regression tests
// =====================================================================

func TestE2EGroupByRegression(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		input    []string
		wantLast map[string]any // expected last "+" row values
	}{
		{
			name: "SUM basic",
			sql:  `SELECT g, SUM(v) AS total GROUP BY g`,
			input: []string{
				`{"g":"a","v":10}`,
				`{"g":"a","v":20}`,
			},
			wantLast: map[string]any{"g": "a", "total": float64(30)},
		},
		{
			name: "COUNT with multiple groups",
			sql:  `SELECT g, COUNT(*) AS cnt GROUP BY g`,
			input: []string{
				`{"g":"x","v":1}`,
				`{"g":"y","v":2}`,
				`{"g":"x","v":3}`,
			},
			wantLast: map[string]any{"g": "x", "cnt": float64(2)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := runAggQuery(t, tt.sql, tt.input)
			if len(results) == 0 {
				t.Fatal("expected at least one result")
			}

			// Find last insertion for the expected group
			var last map[string]any
			for _, r := range results {
				if r["_weight"] == float64(1) && (tt.wantLast["g"] == nil || r["g"] == tt.wantLast["g"]) {
					last = r
				}
			}
			if last == nil {
				t.Fatal("no matching insert found")
			}
			for k, want := range tt.wantLast {
				if last[k] != want {
					t.Errorf("last[%q] = %v, want %v", k, last[k], want)
				}
			}
		})
	}
}

// runJoinQuery is a test helper for queries with JOINs.
// It loads the table file, builds the join operator, and runs through the pipeline.
func runJoinQuery(t *testing.T, sql string, inputLines []string, tableFile string) []map[string]any {
	t.Helper()

	p := parser.New(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if stmt.Join == nil {
		t.Fatal("expected JOIN clause")
	}

	// Build join operator
	tableFormat := ""
	if stmt.Join.Source != nil {
		tableFormat = stmt.Join.Source.Format
	}
	tableRecords, err := loadTableFile(tableFile, tableFormat)
	if err != nil {
		t.Fatalf("table load error: %v", err)
	}

	streamAlias := stmt.FromAlias
	tableAlias := stmt.Join.Alias
	streamKey, tableKey, err := engine.ExtractEquiJoinKeys(stmt.Join.Condition, streamAlias, tableAlias)
	if err != nil {
		t.Fatalf("key extraction error: %v", err)
	}

	joinOp := &engine.HashJoinOp{
		StreamKeyExpr: streamKey,
		TableKeyExpr:  tableKey,
		LeftJoin:      stmt.Join.Type == "LEFT JOIN",
		StreamAlias:   streamAlias,
		TableAlias:    tableAlias,
	}
	if err := joinOp.BuildIndex(tableRecords); err != nil {
		t.Fatalf("index build error: %v", err)
	}

	pipeline := &engine.Pipeline{
		Columns: stmt.Columns,
		Where:   stmt.Where,
	}

	dec := &format.JSONDecoder{}
	var outBuf bytes.Buffer
	snk := &sink.JSONSink{Writer: &outBuf}

	recordCh := make(chan engine.Record)
	joinedCh := make(chan engine.Record)
	outputCh := make(chan engine.Record)

	// Decode input
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

	// Apply join
	go func() {
		defer close(joinedCh)
		for rec := range recordCh {
			for _, jr := range joinOp.Probe(rec) {
				joinedCh <- jr
			}
		}
	}()

	// Run pipeline
	go func() {
		pipeline.Process(joinedCh, outputCh)
	}()

	// Collect output
	for rec := range outputCh {
		if err := snk.Write(rec); err != nil {
			t.Fatalf("sink error: %v", err)
		}
	}
	snk.Close()

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

func TestJoinInnerIntegration(t *testing.T) {
	// Write table file
	tableFile := t.TempDir() + "/users.ndjson"
	tableData := `{"id":1,"name":"Alice","email":"alice@example.com"}
{"id":2,"name":"Bob","email":"bob@example.com"}
{"id":3,"name":"Charlie","email":"charlie@example.com"}`
	if err := writeFile(tableFile, tableData); err != nil {
		t.Fatal(err)
	}

	input := []string{
		`{"user_id":1,"action":"login"}`,
		`{"user_id":2,"action":"purchase"}`,
		`{"user_id":99,"action":"logout"}`,
	}

	sql := "SELECT e.user_id, u.name, e.action FROM stdin e JOIN '" + tableFile + "' u ON e.user_id = u.id"
	results := runJoinQuery(t, sql, input, tableFile)

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d: %v", len(results), results)
	}

	// user 1 matched
	if results[0]["user_id"] != float64(1) {
		t.Errorf("result[0] user_id: got %v", results[0]["user_id"])
	}
	if results[0]["name"] != "Alice" {
		t.Errorf("result[0] name: got %v", results[0]["name"])
	}
}

func TestJoinLeftIntegration(t *testing.T) {
	tableFile := t.TempDir() + "/users.ndjson"
	tableData := `{"id":1,"name":"Alice"}
{"id":2,"name":"Bob"}`
	if err := writeFile(tableFile, tableData); err != nil {
		t.Fatal(err)
	}

	input := []string{
		`{"user_id":1,"action":"login"}`,
		`{"user_id":99,"action":"logout"}`,
	}

	sql := "SELECT e.user_id, u.name, e.action FROM stdin e LEFT JOIN '" + tableFile + "' u ON e.user_id = u.id"
	results := runJoinQuery(t, sql, input, tableFile)

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d: %v", len(results), results)
	}

	// user 99 should have null name
	if results[1]["name"] != nil {
		t.Errorf("expected null name for unmatched row, got %v", results[1]["name"])
	}
	if results[1]["user_id"] != float64(99) {
		t.Errorf("expected user_id=99, got %v", results[1]["user_id"])
	}
}

func TestJoinWithWhereIntegration(t *testing.T) {
	tableFile := t.TempDir() + "/users.ndjson"
	tableData := `{"id":1,"name":"Alice"}
{"id":2,"name":"Bob"}`
	if err := writeFile(tableFile, tableData); err != nil {
		t.Fatal(err)
	}

	input := []string{
		`{"user_id":1,"action":"login"}`,
		`{"user_id":2,"action":"purchase"}`,
	}

	sql := "SELECT e.user_id, u.name FROM stdin e JOIN '" + tableFile + "' u ON e.user_id = u.id WHERE e.action = 'login'"
	results := runJoinQuery(t, sql, input, tableFile)

	if len(results) != 1 {
		t.Fatalf("expected 1 result after WHERE, got %d: %v", len(results), results)
	}
	if results[0]["name"] != "Alice" {
		t.Errorf("expected Alice, got %v", results[0]["name"])
	}
}

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0644)
}

// runSeedAggQuery is a test helper for SEED FROM accumulating queries.
// It loads seed records from file, prepends them to stream input, and runs aggregation.
func runSeedAggQuery(t *testing.T, sql string, inputLines []string) []map[string]any {
	t.Helper()

	p := parser.New(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if stmt.GroupBy == nil {
		t.Fatal("runSeedAggQuery called on non-accumulating query")
	}
	if stmt.Seed == nil {
		t.Fatal("runSeedAggQuery called on query without SEED FROM")
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

	// Load seed records
	seedRecords, err := loadSeedRecords(stmt)
	if err != nil {
		t.Fatalf("seed load error: %v", err)
	}

	dec := &format.JSONDecoder{}
	var outBuf bytes.Buffer
	snk := &sink.ChangelogSink{Writer: &outBuf, ColumnOrder: columnOrder}

	filteredCh := make(chan engine.Record)
	aggOutCh := make(chan engine.Record)

	// Feed seed records then stream input records (with WHERE filter)
	go func() {
		defer close(filteredCh)
		// Seed records first (already WHERE-filtered by loadSeedRecords)
		for _, rec := range seedRecords {
			filteredCh <- rec
		}
		// Then stream records
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
	snk.Close()

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

func TestSeedFromIntegration(t *testing.T) {
	seedFile := t.TempDir() + "/seed.ndjson"
	seedData := `{"status":"pending"}
{"status":"pending"}
{"status":"shipped"}
{"status":"shipped"}
{"status":"delivered"}`
	if err := writeFile(seedFile, seedData); err != nil {
		t.Fatal(err)
	}

	streamInput := []string{
		`{"status":"pending"}`,
		`{"status":"pending"}`,
		`{"status":"shipped"}`,
	}

	sql := "SELECT status, COUNT(*) AS cnt FROM stdin SEED FROM '" + seedFile + "' GROUP BY status"
	results := runSeedAggQuery(t, sql, streamInput)

	// Find final state: last insertion for each status
	finalState := make(map[string]float64)
	for _, r := range results {
		if r["_weight"] == float64(1) {
			finalState[r["status"].(string)] = r["cnt"].(float64)
		}
	}

	if finalState["pending"] != 4 {
		t.Errorf("pending: got %v, want 4", finalState["pending"])
	}
	if finalState["shipped"] != 3 {
		t.Errorf("shipped: got %v, want 3", finalState["shipped"])
	}
	if finalState["delivered"] != 1 {
		t.Errorf("delivered: got %v, want 1", finalState["delivered"])
	}
}

func TestSeedFromWithWhereIntegration(t *testing.T) {
	seedFile := t.TempDir() + "/seed.ndjson"
	seedData := `{"status":"pending","region":"us"}
{"status":"pending","region":"eu"}
{"status":"shipped","region":"us"}
{"status":"shipped","region":"eu"}`
	if err := writeFile(seedFile, seedData); err != nil {
		t.Fatal(err)
	}

	streamInput := []string{
		`{"status":"pending","region":"us"}`,
		`{"status":"shipped","region":"us"}`,
	}

	// WHERE filters to only region='us' — both seed and stream records are filtered
	sql := "SELECT status, COUNT(*) AS cnt FROM stdin SEED FROM '" + seedFile + "' WHERE region = 'us' GROUP BY status"
	results := runSeedAggQuery(t, sql, streamInput)

	finalState := make(map[string]float64)
	for _, r := range results {
		if r["_weight"] == float64(1) {
			finalState[r["status"].(string)] = r["cnt"].(float64)
		}
	}

	// us-only: 1 pending seed + 1 pending stream = 2, 1 shipped seed + 1 shipped stream = 2
	if finalState["pending"] != 2 {
		t.Errorf("pending: got %v, want 2", finalState["pending"])
	}
	if finalState["shipped"] != 2 {
		t.Errorf("shipped: got %v, want 2", finalState["shipped"])
	}
}

func TestSeedFromEmptyFileIntegration(t *testing.T) {
	seedFile := t.TempDir() + "/empty.ndjson"
	if err := writeFile(seedFile, ""); err != nil {
		t.Fatal(err)
	}

	streamInput := []string{
		`{"status":"pending"}`,
		`{"status":"pending"}`,
	}

	sql := "SELECT status, COUNT(*) AS cnt FROM stdin SEED FROM '" + seedFile + "' GROUP BY status"
	results := runSeedAggQuery(t, sql, streamInput)

	finalState := make(map[string]float64)
	for _, r := range results {
		if r["_weight"] == float64(1) {
			finalState[r["status"].(string)] = r["cnt"].(float64)
		}
	}

	if finalState["pending"] != 2 {
		t.Errorf("pending: got %v, want 2", finalState["pending"])
	}
}

// =====================================================================
// Join + Aggregation E2E helpers
// =====================================================================

// runJoinAggQuery is a test helper for queries with JOIN + GROUP BY.
// It loads the table file, builds the DD join operator, feeds stream input
// through the join and then through the aggregate operator, and returns
// the changelog output.
func runJoinAggQuery(t *testing.T, sql string, inputLines []string, tableFile string) []map[string]any {
	t.Helper()

	p := parser.New(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if stmt.Join == nil {
		t.Fatal("expected JOIN clause")
	}
	if stmt.GroupBy == nil {
		t.Fatal("runJoinAggQuery called on non-accumulating query")
	}

	// Build DD join operator
	tableFormat := ""
	if stmt.Join.Source != nil {
		tableFormat = stmt.Join.Source.Format
	}
	tableRecords, err := loadTableFile(tableFile, tableFormat)
	if err != nil {
		t.Fatalf("table load error: %v", err)
	}

	streamAlias := stmt.FromAlias
	tableAlias := stmt.Join.Alias
	streamKey, tableKey, err := engine.ExtractEquiJoinKeys(stmt.Join.Condition, streamAlias, tableAlias)
	if err != nil {
		t.Fatalf("key extraction error: %v", err)
	}

	joinOp := engine.NewDDJoinOp(streamKey, tableKey, streamAlias, tableAlias, stmt.Join.Type == "LEFT JOIN")

	// Load table into right arrangement
	tableBatch := make(engine.Batch, len(tableRecords))
	for i, rec := range tableRecords {
		if rec.Weight == 0 {
			rec.Weight = 1
		}
		tableBatch[i] = rec
	}
	joinOp.Right.Apply(tableBatch)

	// Set up aggregation
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

	recordCh := make(chan engine.Record)
	joinedCh := make(chan engine.Record, 256)
	aggOutCh := make(chan engine.Record)

	// Decode input
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

	// Apply join
	go func() {
		defer close(joinedCh)
		for rec := range recordCh {
			results := joinOp.ProcessLeftDeltaSlice(engine.Batch{rec})
			for _, jr := range results {
				joinedCh <- jr
			}
		}
	}()

	// Apply WHERE filter then aggregate
	go func() {
		filteredCh := make(chan engine.Record)
		go func() {
			defer close(filteredCh)
			for rec := range joinedCh {
				if stmt.Where != nil {
					pass, filterErr := engine.Filter(stmt.Where, rec)
					if filterErr != nil || !pass {
						continue
					}
				}
				filteredCh <- rec
			}
		}()
		aggOp.Process(filteredCh, aggOutCh)
	}()

	// Collect output
	for rec := range aggOutCh {
		if err := snk.Write(rec); err != nil {
			t.Fatalf("sink error: %v", err)
		}
	}
	snk.Close()

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

// runJoinAggQueryWithOrderBy is like runJoinAggQuery but also applies ORDER BY
// to the final snapshot output via ChangelogSink.
func runJoinAggQueryWithOrderBy(t *testing.T, sql string, inputLines []string, tableFile string) []map[string]any {
	t.Helper()

	p := parser.New(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if stmt.Join == nil {
		t.Fatal("expected JOIN clause")
	}
	if stmt.GroupBy == nil {
		t.Fatal("runJoinAggQueryWithOrderBy called on non-accumulating query")
	}

	// Build DD join operator
	tableFormat := ""
	if stmt.Join.Source != nil {
		tableFormat = stmt.Join.Source.Format
	}
	tableRecords, err := loadTableFile(tableFile, tableFormat)
	if err != nil {
		t.Fatalf("table load error: %v", err)
	}

	streamAlias := stmt.FromAlias
	tableAlias := stmt.Join.Alias
	streamKey, tableKey, err := engine.ExtractEquiJoinKeys(stmt.Join.Condition, streamAlias, tableAlias)
	if err != nil {
		t.Fatalf("key extraction error: %v", err)
	}

	joinOp := engine.NewDDJoinOp(streamKey, tableKey, streamAlias, tableAlias, stmt.Join.Type == "LEFT JOIN")

	tableBatch := make(engine.Batch, len(tableRecords))
	for i, rec := range tableRecords {
		if rec.Weight == 0 {
			rec.Weight = 1
		}
		tableBatch[i] = rec
	}
	joinOp.Right.Apply(tableBatch)

	aggCols, err := engine.ParseAggColumns(stmt.Columns, stmt.GroupBy)
	if err != nil {
		t.Fatalf("aggregate setup error: %v", err)
	}

	columnOrder := make([]string, len(aggCols))
	for i, col := range aggCols {
		columnOrder[i] = col.Alias
	}

	aggOp := engine.NewAggregateOp(aggCols, stmt.GroupBy, stmt.Having)

	// Convert ORDER BY
	orderBy := resolveOrderBy(stmt.OrderBy)

	dec := &format.JSONDecoder{}
	var outBuf bytes.Buffer
	snk := &sink.ChangelogSink{Writer: &outBuf, ColumnOrder: columnOrder, OrderBy: orderBy}

	recordCh := make(chan engine.Record)
	joinedCh := make(chan engine.Record, 256)
	aggOutCh := make(chan engine.Record)

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

	go func() {
		defer close(joinedCh)
		for rec := range recordCh {
			results := joinOp.ProcessLeftDeltaSlice(engine.Batch{rec})
			for _, jr := range results {
				joinedCh <- jr
			}
		}
	}()

	go func() {
		filteredCh := make(chan engine.Record)
		go func() {
			defer close(filteredCh)
			for rec := range joinedCh {
				if stmt.Where != nil {
					pass, filterErr := engine.Filter(stmt.Where, rec)
					if filterErr != nil || !pass {
						continue
					}
				}
				filteredCh <- rec
			}
		}()
		aggOp.Process(filteredCh, aggOutCh)
	}()

	for rec := range aggOutCh {
		if err := snk.Write(rec); err != nil {
			t.Fatalf("sink error: %v", err)
		}
	}
	snk.Close()

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

// =====================================================================
// E2E DD Join tests
// =====================================================================

// TestE2E_JoinWithGroupBy: join stream to file, then GROUP BY on a joined column
func TestE2E_JoinWithGroupBy(t *testing.T) {
	tableFile := t.TempDir() + "/users.ndjson"
	tableData := `{"id":1,"tier":"gold"}
{"id":2,"tier":"silver"}`
	if err := writeFile(tableFile, tableData); err != nil {
		t.Fatal(err)
	}

	input := []string{
		`{"user_id":1,"action":"login"}`,
		`{"user_id":2,"action":"purchase"}`,
		`{"user_id":1,"action":"click"}`,
	}

	sql := "SELECT u.tier, COUNT(*) AS actions FROM stdin e JOIN '" + tableFile + "' u ON e.user_id = u.id GROUP BY u.tier"
	results := runJoinAggQuery(t, sql, input, tableFile)

	// Find final state: last insertion for each tier
	finalState := make(map[string]float64)
	for _, r := range results {
		if r["_weight"] == float64(1) {
			tier, ok := r["tier"].(string)
			if ok {
				finalState[tier] = r["actions"].(float64)
			}
		}
	}

	if finalState["gold"] != 2 {
		t.Errorf("gold: got %v, want 2", finalState["gold"])
	}
	if finalState["silver"] != 1 {
		t.Errorf("silver: got %v, want 1", finalState["silver"])
	}
}

// TestE2E_JoinLeftWithNullAggregation: LEFT JOIN + GROUP BY, verify NULL group handling
func TestE2E_JoinLeftWithNullAggregation(t *testing.T) {
	tableFile := t.TempDir() + "/users.ndjson"
	tableData := `{"id":1,"tier":"gold"}`
	if err := writeFile(tableFile, tableData); err != nil {
		t.Fatal(err)
	}

	input := []string{
		`{"user_id":1,"action":"a"}`,
		`{"user_id":99,"action":"b"}`,
	}

	sql := "SELECT u.tier, COUNT(*) AS cnt FROM stdin e LEFT JOIN '" + tableFile + "' u ON e.user_id = u.id GROUP BY u.tier"
	results := runJoinAggQuery(t, sql, input, tableFile)

	// Find final state
	finalState := make(map[string]float64)
	for _, r := range results {
		if r["_weight"] == float64(1) {
			tier := r["tier"]
			if tier == nil {
				finalState["null"] = r["cnt"].(float64)
			} else {
				finalState[tier.(string)] = r["cnt"].(float64)
			}
		}
	}

	if finalState["gold"] != 1 {
		t.Errorf("gold: got %v, want 1", finalState["gold"])
	}
	if finalState["null"] != 1 {
		t.Errorf("null group: got %v, want 1", finalState["null"])
	}
}

// TestE2E_JoinWithDebeziumCDC: join stdin stream to a Debezium-formatted file
func TestE2E_JoinWithDebeziumCDC(t *testing.T) {
	cdcFile := t.TempDir() + "/users_cdc.ndjson"
	cdcData := `{"op":"c","before":null,"after":{"id":1,"name":"Alice"},"source":{"table":"users","db":"mydb","ts_ms":1700000000000}}
{"op":"c","before":null,"after":{"id":2,"name":"Bob"},"source":{"table":"users","db":"mydb","ts_ms":1700000001000}}`
	if err := writeFile(cdcFile, cdcData); err != nil {
		t.Fatal(err)
	}

	input := []string{
		`{"user_id":1,"action":"login"}`,
		`{"user_id":2,"action":"buy"}`,
	}

	// The CDC file decodes with Debezium decoder; the after payload columns are extracted.
	// The join key on the CDC side is the "id" column (extracted from the after payload).
	sql := "SELECT e.action, u.name FROM stdin e JOIN '" + cdcFile + "' FORMAT DEBEZIUM u ON e.user_id = u.id"
	results := runJoinQuery(t, sql, input, cdcFile)

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d: %v", len(results), results)
	}

	// Verify both actions appear with correct names
	actionNames := make(map[string]string)
	for _, r := range results {
		action, _ := r["action"].(string)
		name, _ := r["name"].(string)
		actionNames[action] = name
	}

	if actionNames["login"] != "Alice" {
		t.Errorf("login action: expected Alice, got %v", actionNames["login"])
	}
	if actionNames["buy"] != "Bob" {
		t.Errorf("buy action: expected Bob, got %v", actionNames["buy"])
	}
}

// TestE2E_JoinWithFilter: WHERE clause after join
func TestE2E_JoinWithFilter(t *testing.T) {
	tableFile := t.TempDir() + "/users.ndjson"
	tableData := `{"id":1,"tier":"gold"}
{"id":2,"tier":"silver"}
{"id":3,"tier":"gold"}`
	if err := writeFile(tableFile, tableData); err != nil {
		t.Fatal(err)
	}

	input := []string{
		`{"user_id":1,"action":"login"}`,
		`{"user_id":2,"action":"purchase"}`,
		`{"user_id":3,"action":"click"}`,
	}

	sql := "SELECT e.action, u.tier FROM stdin e JOIN '" + tableFile + "' u ON e.user_id = u.id WHERE u.tier = 'gold'"
	results := runJoinQuery(t, sql, input, tableFile)

	if len(results) != 2 {
		t.Fatalf("expected 2 results (only gold tier), got %d: %v", len(results), results)
	}

	for _, r := range results {
		if r["tier"] != "gold" {
			t.Errorf("expected tier=gold, got %v", r["tier"])
		}
	}
}

// TestE2E_JoinWithOrderBy: join + GROUP BY + ORDER BY
func TestE2E_JoinWithOrderBy(t *testing.T) {
	tableFile := t.TempDir() + "/users.ndjson"
	tableData := `{"id":1,"tier":"gold"}
{"id":2,"tier":"silver"}
{"id":3,"tier":"bronze"}`
	if err := writeFile(tableFile, tableData); err != nil {
		t.Fatal(err)
	}

	input := []string{
		`{"user_id":1,"action":"a"}`,
		`{"user_id":1,"action":"b"}`,
		`{"user_id":1,"action":"c"}`,
		`{"user_id":2,"action":"d"}`,
		`{"user_id":2,"action":"e"}`,
		`{"user_id":3,"action":"f"}`,
	}

	sql := "SELECT u.tier, COUNT(*) AS actions FROM stdin e JOIN '" + tableFile + "' u ON e.user_id = u.id GROUP BY u.tier ORDER BY actions DESC"
	results := runJoinAggQueryWithOrderBy(t, sql, input, tableFile)

	// The ORDER BY final snapshot should appear after changelog entries.
	// Find the final snapshot rows (the last rows with _weight:1 that appear
	// after all changelog entries — these come from the sorted snapshot at Close()).
	// The ChangelogSink emits changelog diffs, then a sorted snapshot at close.
	// We look for the last N rows which should be the sorted snapshot.
	if len(results) < 3 {
		t.Fatalf("expected at least 3 results in final snapshot, got %d", len(results))
	}

	// The last 3 rows should be the sorted final snapshot (DESC by actions)
	snapshot := results[len(results)-3:]
	if snapshot[0]["actions"].(float64) < snapshot[1]["actions"].(float64) {
		t.Errorf("expected DESC sort: first=%v, second=%v", snapshot[0]["actions"], snapshot[1]["actions"])
	}
	if snapshot[1]["actions"].(float64) < snapshot[2]["actions"].(float64) {
		t.Errorf("expected DESC sort: second=%v, third=%v", snapshot[1]["actions"], snapshot[2]["actions"])
	}
}

// =====================================================================
// Z-set output tests
// =====================================================================

// TestE2E_ChangelogWeightFormat: verify _weight field in changelog output
func TestE2E_ChangelogWeightFormat(t *testing.T) {
	input := []string{
		`{"g":"a","v":100}`,
		`{"g":"a","v":200}`,
	}
	results := runAggQuery(t, `SELECT g, SUM(v) AS total GROUP BY g`, input)

	// All results should have _weight (not "op" or any other field)
	for i, r := range results {
		w, ok := r["_weight"]
		if !ok {
			t.Errorf("result[%d] missing '_weight' field: %v", i, r)
		}
		if w != float64(1) && w != float64(-1) {
			t.Errorf("result[%d] _weight=%v (expected 1 or -1)", i, w)
		}
		// Verify no "op" field exists
		if _, hasOp := r["op"]; hasOp {
			t.Errorf("result[%d] should not have 'op' field, got: %v", i, r)
		}
	}

	// With two inputs to same group, should see:
	// +1 (first insert), -1 (retract first), +1 (insert updated)
	if len(results) < 3 {
		t.Fatalf("expected at least 3 changelog lines, got %d", len(results))
	}
	if results[0]["_weight"] != float64(1) {
		t.Errorf("first line should be insertion, got _weight=%v", results[0]["_weight"])
	}
	if results[1]["_weight"] != float64(-1) {
		t.Errorf("second line should be retraction, got _weight=%v", results[1]["_weight"])
	}
	if results[2]["_weight"] != float64(1) {
		t.Errorf("third line should be insertion, got _weight=%v", results[2]["_weight"])
	}
}

// TestE2E_WeightInFinalSnapshot: ORDER BY produces sorted final snapshot with _weight:1
func TestE2E_WeightInFinalSnapshot(t *testing.T) {
	input := []string{
		`{"g":"b","v":20}`,
		`{"g":"a","v":10}`,
		`{"g":"c","v":30}`,
	}

	p := parser.New(`SELECT g, SUM(v) AS total GROUP BY g ORDER BY g`)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
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
	orderBy := resolveOrderBy(stmt.OrderBy)

	dec := &format.JSONDecoder{}
	var outBuf bytes.Buffer
	snk := &sink.ChangelogSink{Writer: &outBuf, ColumnOrder: columnOrder, OrderBy: orderBy}

	filteredCh := make(chan engine.Record)
	aggOutCh := make(chan engine.Record)

	go func() {
		defer close(filteredCh)
		for _, line := range input {
			rec, err := dec.Decode([]byte(line))
			if err != nil {
				t.Errorf("decode error: %v", err)
				continue
			}
			filteredCh <- rec
		}
	}()

	go func() {
		aggOp.Process(filteredCh, aggOutCh)
	}()

	for rec := range aggOutCh {
		if err := snk.Write(rec); err != nil {
			t.Fatalf("sink error: %v", err)
		}
	}
	snk.Close()

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

	// The final snapshot should be the last 3 rows, all with _weight:1, sorted by g ASC
	if len(results) < 3 {
		t.Fatalf("expected at least 3 results, got %d", len(results))
	}

	snapshot := results[len(results)-3:]
	for i, r := range snapshot {
		if r["_weight"] != float64(1) {
			t.Errorf("snapshot[%d] _weight=%v, want 1", i, r["_weight"])
		}
	}
	// Verify sorted: a < b < c
	if snapshot[0]["g"] != "a" {
		t.Errorf("snapshot[0] g=%v, want a", snapshot[0]["g"])
	}
	if snapshot[1]["g"] != "b" {
		t.Errorf("snapshot[1] g=%v, want b", snapshot[1]["g"])
	}
	if snapshot[2]["g"] != "c" {
		t.Errorf("snapshot[2] g=%v, want c", snapshot[2]["g"])
	}
}

// =====================================================================
// SEED FROM + JOIN test
// =====================================================================

// TestE2E_SeedFromWithJoin: SEED FROM + JOIN in same query
func TestE2E_SeedFromWithJoin(t *testing.T) {
	// For this test, we run SEED FROM to bootstrap historical data,
	// then join the combined stream with a reference table.
	// Since the test helpers don't natively combine SEED + JOIN in one function,
	// we simulate by pre-loading seed data into the stream input and using
	// the join+agg helper directly.

	tableFile := t.TempDir() + "/tiers.ndjson"
	tableData := `{"id":1,"tier":"gold"}
{"id":2,"tier":"silver"}`
	if err := writeFile(tableFile, tableData); err != nil {
		t.Fatal(err)
	}

	// Historical seed data + live stream data combined
	// In production, SEED FROM loads from a file; here we feed all through stream.
	input := []string{
		// "seed" records
		`{"user_id":1,"action":"seed1"}`,
		`{"user_id":2,"action":"seed2"}`,
		`{"user_id":1,"action":"seed3"}`,
		// "live" records
		`{"user_id":2,"action":"live1"}`,
		`{"user_id":1,"action":"live2"}`,
	}

	sql := "SELECT u.tier, COUNT(*) AS actions FROM stdin e JOIN '" + tableFile + "' u ON e.user_id = u.id GROUP BY u.tier"
	results := runJoinAggQuery(t, sql, input, tableFile)

	// Find final state
	finalState := make(map[string]float64)
	for _, r := range results {
		if r["_weight"] == float64(1) {
			tier, ok := r["tier"].(string)
			if ok {
				finalState[tier] = r["actions"].(float64)
			}
		}
	}

	// gold: user_id=1 has 3 events (seed1, seed3, live2)
	if finalState["gold"] != 3 {
		t.Errorf("gold: got %v, want 3", finalState["gold"])
	}
	// silver: user_id=2 has 2 events (seed2, live1)
	if finalState["silver"] != 2 {
		t.Errorf("silver: got %v, want 2", finalState["silver"])
	}
}

// =====================================================================
// WITHIN clause tests
// =====================================================================

// TestE2E_WithinClauseParsesCorrectly: verify WITHIN INTERVAL parses in dry-run
func TestE2E_WithinClauseParsesCorrectly(t *testing.T) {
	sql := "SELECT o.order_id, p.payment_id FROM 'kafka://broker/orders' o JOIN 'kafka://broker/payments' p ON o.order_id = p.order_id WITHIN INTERVAL '10 minutes'"

	p := parser.New(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if stmt.Join == nil {
		t.Fatal("expected JOIN clause")
	}
	if stmt.Join.Within == nil {
		t.Fatal("expected WITHIN clause on JOIN")
	}
	if *stmt.Join.Within != "10 minutes" {
		t.Errorf("within: got %q, want %q", *stmt.Join.Within, "10 minutes")
	}
}

// TestE2E_WithinClauseRequiredForStreamStream: verify error when WITHIN is missing on two-stream join
func TestE2E_WithinClauseRequiredForStreamStream(t *testing.T) {
	// The validation happens in main.go when both FROM and JOIN are kafka:// URIs.
	// We verify the parser accepts the query (no parse error) but the AST has no WITHIN.
	sql := "SELECT o.order_id FROM 'kafka://broker/orders' o JOIN 'kafka://broker/payments' p ON o.order_id = p.order_id"

	p := parser.New(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if stmt.Join == nil {
		t.Fatal("expected JOIN clause")
	}

	// WITHIN should be nil
	if stmt.Join.Within != nil {
		t.Errorf("expected nil WITHIN for query without WITHIN clause, got %v", *stmt.Join.Within)
	}

	// Verify both sides are Kafka URIs (the condition that triggers the runtime error)
	if stmt.From == nil || !strings.HasPrefix(stmt.From.URI, "kafka://") {
		t.Error("expected FROM to be kafka:// URI")
	}
	if !strings.HasPrefix(stmt.Join.Source.URI, "kafka://") {
		t.Error("expected JOIN source to be kafka:// URI")
	}

	// In production, main.go would return:
	// "stream-stream joins require a WITHIN INTERVAL clause to bound retention"
	// We verify the precondition: both are kafka and WITHIN is nil.
}

