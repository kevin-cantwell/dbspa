package engine

import (
	"testing"
	"time"

	"github.com/kevin-cantwell/folddb/internal/sql/ast"
)

// helper to make a record with diff
func mkRec(diff int8, cols map[string]Value) Record {
	return Record{
		Columns:   cols,
		Timestamp: time.Now(),
		Diff:      diff,
	}
}

// helper to collect records from a channel
func collectRecords(ch <-chan Record) []Record {
	var recs []Record
	for r := range ch {
		recs = append(recs, r)
	}
	return recs
}

// buildSimpleAggOp creates an AggregateOp for:
//
//	SELECT <groupCol>, <aggFunc>(<aggArg>) AS <aggAlias> GROUP BY <groupCol>
func buildSimpleAggOp(groupCol, aggFunc, aggArg, aggAlias string, having ast.Expr) *AggregateOp {
	isStar := aggArg == "*"
	var aggArgExpr ast.Expr
	if !isStar {
		aggArgExpr = &ast.ColumnRef{Name: aggArg}
	}

	columns := []AggColumn{
		{Alias: groupCol, Expr: &ast.ColumnRef{Name: groupCol}},
		{Alias: aggAlias, Expr: nil, IsAggregate: true, AggFunc: aggFunc, AggArg: aggArgExpr, IsStar: isStar},
	}
	groupBy := []ast.Expr{&ast.ColumnRef{Name: groupCol}}
	return NewAggregateOp(columns, groupBy, having)
}

// processAndCollect feeds records through an AggregateOp and collects output.
func processAndCollect(op *AggregateOp, records []Record) []Record {
	in := make(chan Record)
	out := make(chan Record, 100)

	go func() {
		defer close(in)
		for _, r := range records {
			in <- r
		}
	}()

	go func() {
		op.Process(in, out)
	}()

	return collectRecords(out)
}

func TestAggregateOp_BasicGroupBy(t *testing.T) {
	op := buildSimpleAggOp("g", "COUNT", "*", "cnt", nil)

	records := []Record{
		mkRec(1, map[string]Value{"g": TextValue{V: "a"}}),
		mkRec(1, map[string]Value{"g": TextValue{V: "b"}}),
		mkRec(1, map[string]Value{"g": TextValue{V: "a"}}),
	}

	results := processAndCollect(op, records)

	// Expected output:
	// +{g=a, cnt=1}
	// +{g=b, cnt=1}
	// -{g=a, cnt=1}
	// +{g=a, cnt=2}
	if len(results) != 4 {
		t.Fatalf("expected 4 records, got %d", len(results))
	}

	// First: insert a cnt=1
	if results[0].Diff != 1 {
		t.Errorf("result[0] expected diff=+1, got %d", results[0].Diff)
	}
	if v := results[0].Columns["cnt"].(IntValue).V; v != 1 {
		t.Errorf("result[0] cnt expected 1, got %d", v)
	}

	// Last: insert a cnt=2
	if results[3].Diff != 1 {
		t.Errorf("result[3] expected diff=+1, got %d", results[3].Diff)
	}
	if v := results[3].Columns["cnt"].(IntValue).V; v != 2 {
		t.Errorf("result[3] cnt expected 2, got %d", v)
	}
}

func TestAggregateOp_ChangelogRetractionInsertionAdjacent(t *testing.T) {
	op := buildSimpleAggOp("g", "COUNT", "*", "cnt", nil)

	records := []Record{
		mkRec(1, map[string]Value{"g": TextValue{V: "a"}}),
		mkRec(1, map[string]Value{"g": TextValue{V: "a"}}),
	}

	results := processAndCollect(op, records)

	// After second insert to "a":
	// result[0]: +{a, 1}
	// result[1]: -{a, 1}   (retraction)
	// result[2]: +{a, 2}   (insertion)
	if len(results) != 3 {
		t.Fatalf("expected 3 records, got %d", len(results))
	}

	// Retraction and insertion should be adjacent
	if results[1].Diff != -1 {
		t.Errorf("result[1] expected retraction (diff=-1), got %d", results[1].Diff)
	}
	if results[2].Diff != 1 {
		t.Errorf("result[2] expected insertion (diff=+1), got %d", results[2].Diff)
	}

	// Both should be for the same group key
	if results[1].Columns["g"].(TextValue).V != "a" {
		t.Errorf("retraction should be for group 'a'")
	}
	if results[2].Columns["g"].(TextValue).V != "a" {
		t.Errorf("insertion should be for group 'a'")
	}
}

func TestAggregateOp_HavingFilter(t *testing.T) {
	// HAVING COUNT(*) > 1
	having := &ast.BinaryExpr{
		Left: &ast.FunctionCall{
			Name: "COUNT",
			Args: []ast.Expr{&ast.StarExpr{}},
		},
		Op:    ">",
		Right: &ast.NumberLiteral{Value: "1"},
	}

	op := buildSimpleAggOp("g", "COUNT", "*", "cnt", having)

	records := []Record{
		mkRec(1, map[string]Value{"g": TextValue{V: "a"}}),
		mkRec(1, map[string]Value{"g": TextValue{V: "a"}}),
	}

	results := processAndCollect(op, records)

	// First insert: cnt=1, HAVING fails -> no output
	// Second insert: cnt=2, HAVING passes -> output
	var inserts []Record
	for _, r := range results {
		if r.Diff == 1 {
			inserts = append(inserts, r)
		}
	}

	if len(inserts) != 1 {
		t.Fatalf("expected 1 insert passing HAVING, got %d", len(inserts))
	}
	if v := inserts[0].Columns["cnt"].(IntValue).V; v != 2 {
		t.Errorf("expected cnt=2, got %d", v)
	}
}

func TestAggregateOp_HavingGroupPassesThenFails(t *testing.T) {
	// HAVING COUNT(*) > 1
	having := &ast.BinaryExpr{
		Left: &ast.FunctionCall{
			Name: "COUNT",
			Args: []ast.Expr{&ast.StarExpr{}},
		},
		Op:    ">",
		Right: &ast.NumberLiteral{Value: "1"},
	}

	op := buildSimpleAggOp("g", "COUNT", "*", "cnt", having)

	records := []Record{
		mkRec(1, map[string]Value{"g": TextValue{V: "a"}}),
		mkRec(1, map[string]Value{"g": TextValue{V: "a"}}),
		// Now retract one so count drops to 1, HAVING should fail
		mkRec(-1, map[string]Value{"g": TextValue{V: "a"}}),
	}

	results := processAndCollect(op, records)

	// After 2 inserts: cnt=2, HAVING passes -> emit +
	// After retraction: cnt=1, HAVING fails -> emit - (retract previous), no new +
	var lastRetract Record
	hasRetract := false
	for _, r := range results {
		if r.Diff == -1 {
			lastRetract = r
			hasRetract = true
		}
	}

	if !hasRetract {
		t.Fatal("expected a retraction when HAVING stops passing")
	}
	if v := lastRetract.Columns["cnt"].(IntValue).V; v != 2 {
		t.Errorf("retraction should retract the old cnt=2, got %d", v)
	}
}

func TestAggregateOp_MultipleAggregates(t *testing.T) {
	columns := []AggColumn{
		{Alias: "g", Expr: &ast.ColumnRef{Name: "g"}},
		{Alias: "cnt", IsAggregate: true, AggFunc: "COUNT", IsStar: true},
		{Alias: "total", IsAggregate: true, AggFunc: "SUM", AggArg: &ast.ColumnRef{Name: "v"}},
	}
	groupBy := []ast.Expr{&ast.ColumnRef{Name: "g"}}
	op := NewAggregateOp(columns, groupBy, nil)

	records := []Record{
		mkRec(1, map[string]Value{"g": TextValue{V: "a"}, "v": IntValue{V: 10}}),
		mkRec(1, map[string]Value{"g": TextValue{V: "a"}, "v": IntValue{V: 20}}),
		mkRec(1, map[string]Value{"g": TextValue{V: "a"}, "v": IntValue{V: 30}}),
	}

	results := processAndCollect(op, records)

	// Last insertion should have cnt=3, total=60
	last := results[len(results)-1]
	if last.Diff != 1 {
		t.Fatal("last result should be an insertion")
	}
	if v := last.Columns["cnt"].(IntValue).V; v != 3 {
		t.Errorf("expected cnt=3, got %d", v)
	}
	if v := last.Columns["total"].(IntValue).V; v != 60 {
		t.Errorf("expected total=60, got %d", v)
	}
}

func TestAggregateOp_GroupRemovalCountZero(t *testing.T) {
	op := buildSimpleAggOp("g", "COUNT", "*", "cnt", nil)

	records := []Record{
		mkRec(1, map[string]Value{"g": TextValue{V: "a"}}),
		mkRec(-1, map[string]Value{"g": TextValue{V: "a"}}),
	}

	results := processAndCollect(op, records)

	// First: +{a, 1}
	// Second: -{a, 1} (retraction, group removed)
	if len(results) != 2 {
		t.Fatalf("expected 2 records, got %d", len(results))
	}
	if results[0].Diff != 1 {
		t.Errorf("result[0] expected +1")
	}
	if results[1].Diff != -1 {
		t.Errorf("result[1] expected -1")
	}
}

func TestAggregateOp_GroupReinsertAfterRemoval(t *testing.T) {
	// TC-ACC-026 at the operator level
	op := buildSimpleAggOp("g", "COUNT", "*", "cnt", nil)

	records := []Record{
		mkRec(1, map[string]Value{"g": TextValue{V: "a"}}),
		mkRec(-1, map[string]Value{"g": TextValue{V: "a"}}),
		mkRec(1, map[string]Value{"g": TextValue{V: "a"}}),
	}

	results := processAndCollect(op, records)

	// +{a,1}, -{a,1}, +{a,1} (fresh emission)
	if len(results) != 3 {
		t.Fatalf("expected 3 records, got %d", len(results))
	}
	if results[2].Diff != 1 {
		t.Errorf("result[2] expected +1 (fresh insert)")
	}
	if v := results[2].Columns["cnt"].(IntValue).V; v != 1 {
		t.Errorf("expected cnt=1, got %d", v)
	}
}

func TestAggregateOp_NullGroupByKey(t *testing.T) {
	// TC-ACC-022: NULL as GROUP BY key
	columns := []AggColumn{
		{Alias: "g", Expr: &ast.ColumnRef{Name: "g"}},
		{Alias: "total", IsAggregate: true, AggFunc: "SUM", AggArg: &ast.ColumnRef{Name: "v"}},
	}
	groupBy := []ast.Expr{&ast.ColumnRef{Name: "g"}}
	op := NewAggregateOp(columns, groupBy, nil)

	records := []Record{
		mkRec(1, map[string]Value{"g": NullValue{}, "v": IntValue{V: 1}}),
		mkRec(1, map[string]Value{"g": NullValue{}, "v": IntValue{V: 2}}),
		mkRec(1, map[string]Value{"g": TextValue{V: "a"}, "v": IntValue{V: 3}}),
	}

	results := processAndCollect(op, records)

	// All NULL keys should be grouped together
	// Last result for null group should have total=3
	var nullGroupResults []Record
	for _, r := range results {
		gv := r.Columns["g"]
		if gv.IsNull() && r.Diff == 1 {
			nullGroupResults = append(nullGroupResults, r)
		}
	}

	if len(nullGroupResults) == 0 {
		t.Fatal("expected at least one null-group result")
	}
	last := nullGroupResults[len(nullGroupResults)-1]
	if v := last.Columns["total"].(IntValue).V; v != 3 {
		t.Errorf("expected null-group total=3, got %d", v)
	}
}

func TestAggregateOp_SumRetraction(t *testing.T) {
	// TC-ACC-003 at operator level
	columns := []AggColumn{
		{Alias: "g", Expr: &ast.ColumnRef{Name: "g"}},
		{Alias: "total", IsAggregate: true, AggFunc: "SUM", AggArg: &ast.ColumnRef{Name: "v"}},
		{Alias: "cnt", IsAggregate: true, AggFunc: "COUNT", IsStar: true},
	}
	groupBy := []ast.Expr{&ast.ColumnRef{Name: "g"}}
	op := NewAggregateOp(columns, groupBy, nil)

	records := []Record{
		mkRec(1, map[string]Value{"g": TextValue{V: "x"}, "v": IntValue{V: 10}}),
		mkRec(1, map[string]Value{"g": TextValue{V: "x"}, "v": IntValue{V: 20}}),
		mkRec(-1, map[string]Value{"g": TextValue{V: "x"}, "v": IntValue{V: 10}}),
	}

	results := processAndCollect(op, records)

	// Final insertion should have total=20
	last := results[len(results)-1]
	if last.Diff != 1 {
		t.Fatal("last result should be insertion")
	}
	if v := last.Columns["total"].(IntValue).V; v != 20 {
		t.Errorf("expected total=20, got %d", v)
	}
}

func TestAggregateOp_SumNullSkipped(t *testing.T) {
	// TC-ACC-004 at operator level
	columns := []AggColumn{
		{Alias: "g", Expr: &ast.ColumnRef{Name: "g"}},
		{Alias: "total", IsAggregate: true, AggFunc: "SUM", AggArg: &ast.ColumnRef{Name: "v"}},
	}
	groupBy := []ast.Expr{&ast.ColumnRef{Name: "g"}}
	op := NewAggregateOp(columns, groupBy, nil)

	records := []Record{
		mkRec(1, map[string]Value{"g": TextValue{V: "a"}, "v": IntValue{V: 10}}),
		mkRec(1, map[string]Value{"g": TextValue{V: "a"}, "v": NullValue{}}),
		mkRec(1, map[string]Value{"g": TextValue{V: "a"}, "v": IntValue{V: 5}}),
	}

	results := processAndCollect(op, records)

	// Find last insertion for group "a"
	var lastInsert Record
	for _, r := range results {
		if r.Diff == 1 {
			if gv, ok := r.Columns["g"].(TextValue); ok && gv.V == "a" {
				lastInsert = r
			}
		}
	}

	if v := lastInsert.Columns["total"].(IntValue).V; v != 15 {
		t.Errorf("expected total=15, got %d", v)
	}
}

func TestAggregateOp_MultipleGroups(t *testing.T) {
	op := buildSimpleAggOp("g", "COUNT", "*", "cnt", nil)

	records := []Record{
		mkRec(1, map[string]Value{"g": TextValue{V: "a"}}),
		mkRec(1, map[string]Value{"g": TextValue{V: "b"}}),
		mkRec(1, map[string]Value{"g": TextValue{V: "c"}}),
	}

	results := processAndCollect(op, records)

	// Each group gets exactly one insertion
	inserts := make(map[string]int64)
	for _, r := range results {
		if r.Diff == 1 {
			g := r.Columns["g"].(TextValue).V
			inserts[g] = r.Columns["cnt"].(IntValue).V
		}
	}

	for _, g := range []string{"a", "b", "c"} {
		if inserts[g] != 1 {
			t.Errorf("expected group %q cnt=1, got %d", g, inserts[g])
		}
	}
}

func TestAggregateOp_AvgRetraction(t *testing.T) {
	// TC-ACC-005 at operator level
	columns := []AggColumn{
		{Alias: "g", Expr: &ast.ColumnRef{Name: "g"}},
		{Alias: "avg_v", IsAggregate: true, AggFunc: "AVG", AggArg: &ast.ColumnRef{Name: "v"}},
		{Alias: "cnt", IsAggregate: true, AggFunc: "COUNT", IsStar: true},
	}
	groupBy := []ast.Expr{&ast.ColumnRef{Name: "g"}}
	op := NewAggregateOp(columns, groupBy, nil)

	records := []Record{
		mkRec(1, map[string]Value{"g": TextValue{V: "x"}, "v": IntValue{V: 10}}),
		mkRec(1, map[string]Value{"g": TextValue{V: "x"}, "v": IntValue{V: 20}}),
		mkRec(-1, map[string]Value{"g": TextValue{V: "x"}, "v": IntValue{V: 10}}),
	}

	results := processAndCollect(op, records)

	// Final: AVG should be 20.0
	last := results[len(results)-1]
	if last.Diff != 1 {
		t.Fatal("last should be insertion")
	}
	if v := last.Columns["avg_v"].(FloatValue).V; v != 20.0 {
		t.Errorf("expected avg=20.0, got %f", v)
	}
}

func TestAggregateOp_MinMaxRetraction(t *testing.T) {
	// TC-ACC-007 and TC-ACC-008 at operator level
	columns := []AggColumn{
		{Alias: "g", Expr: &ast.ColumnRef{Name: "g"}},
		{Alias: "min_v", IsAggregate: true, AggFunc: "MIN", AggArg: &ast.ColumnRef{Name: "v"}},
		{Alias: "max_v", IsAggregate: true, AggFunc: "MAX", AggArg: &ast.ColumnRef{Name: "v"}},
		{Alias: "cnt", IsAggregate: true, AggFunc: "COUNT", IsStar: true},
	}
	groupBy := []ast.Expr{&ast.ColumnRef{Name: "g"}}
	op := NewAggregateOp(columns, groupBy, nil)

	records := []Record{
		mkRec(1, map[string]Value{"g": TextValue{V: "x"}, "v": IntValue{V: 5}}),
		mkRec(1, map[string]Value{"g": TextValue{V: "x"}, "v": IntValue{V: 10}}),
		mkRec(1, map[string]Value{"g": TextValue{V: "x"}, "v": IntValue{V: 3}}),
		mkRec(-1, map[string]Value{"g": TextValue{V: "x"}, "v": IntValue{V: 3}}),  // retract min
		mkRec(-1, map[string]Value{"g": TextValue{V: "x"}, "v": IntValue{V: 10}}), // retract max
	}

	results := processAndCollect(op, records)

	last := results[len(results)-1]
	if last.Diff != 1 {
		t.Fatal("last should be insertion")
	}
	if v := last.Columns["min_v"].(IntValue).V; v != 5 {
		t.Errorf("expected min=5 after retract, got %d", v)
	}
	if v := last.Columns["max_v"].(IntValue).V; v != 5 {
		t.Errorf("expected max=5 after retract, got %d", v)
	}
}

func TestAggregateOp_FirstLastOperator(t *testing.T) {
	columns := []AggColumn{
		{Alias: "g", Expr: &ast.ColumnRef{Name: "g"}},
		{Alias: "first_v", IsAggregate: true, AggFunc: "FIRST", AggArg: &ast.ColumnRef{Name: "v"}},
		{Alias: "last_v", IsAggregate: true, AggFunc: "LAST", AggArg: &ast.ColumnRef{Name: "v"}},
		{Alias: "cnt", IsAggregate: true, AggFunc: "COUNT", IsStar: true},
	}
	groupBy := []ast.Expr{&ast.ColumnRef{Name: "g"}}
	op := NewAggregateOp(columns, groupBy, nil)

	records := []Record{
		mkRec(1, map[string]Value{"g": TextValue{V: "x"}, "v": IntValue{V: 10}}),
		mkRec(1, map[string]Value{"g": TextValue{V: "x"}, "v": IntValue{V: 20}}),
		mkRec(1, map[string]Value{"g": TextValue{V: "x"}, "v": IntValue{V: 30}}),
	}

	results := processAndCollect(op, records)

	last := results[len(results)-1]
	if v := last.Columns["first_v"].(IntValue).V; v != 10 {
		t.Errorf("expected first=10, got %d", v)
	}
	if v := last.Columns["last_v"].(IntValue).V; v != 30 {
		t.Errorf("expected last=30, got %d", v)
	}
}

func TestNewAccumulator(t *testing.T) {
	tests := []struct {
		name   string
		isStar bool
		want   string
	}{
		{"COUNT", true, "*CountStarAccumulator"},
		{"COUNT", false, "*CountAccumulator"},
		{"SUM", false, "*SumAccumulator"},
		{"AVG", false, "*AvgAccumulator"},
		{"MIN", false, "*MinAccumulator"},
		{"MAX", false, "*MaxAccumulator"},
		{"FIRST", false, "*FirstAccumulator"},
		{"LAST", false, "*LastAccumulator"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			acc := newAccumulator(tt.name, tt.isStar)
			got := ""
			switch acc.(type) {
			case *CountStarAccumulator:
				got = "*CountStarAccumulator"
			case *CountAccumulator:
				got = "*CountAccumulator"
			case *SumAccumulator:
				got = "*SumAccumulator"
			case *AvgAccumulator:
				got = "*AvgAccumulator"
			case *MinAccumulator:
				got = "*MinAccumulator"
			case *MaxAccumulator:
				got = "*MaxAccumulator"
			case *FirstAccumulator:
				got = "*FirstAccumulator"
			case *LastAccumulator:
				got = "*LastAccumulator"
			}
			if got != tt.want {
				t.Errorf("newAccumulator(%q, %v) = %s, want %s", tt.name, tt.isStar, got, tt.want)
			}
		})
	}
}

func TestCompositeKey(t *testing.T) {
	tests := []struct {
		name string
		vals []Value
		same []Value // should produce same key
		diff []Value // should produce different key
	}{
		{
			"single_string",
			[]Value{TextValue{V: "a"}},
			[]Value{TextValue{V: "a"}},
			[]Value{TextValue{V: "b"}},
		},
		{
			"with_null",
			[]Value{NullValue{}},
			[]Value{NullValue{}},
			[]Value{TextValue{V: ""}},
		},
		{
			"multi",
			[]Value{TextValue{V: "a"}, IntValue{V: 1}},
			[]Value{TextValue{V: "a"}, IntValue{V: 1}},
			[]Value{TextValue{V: "a"}, IntValue{V: 2}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k1 := compositeKey(tt.vals)
			k2 := compositeKey(tt.same)
			k3 := compositeKey(tt.diff)
			if k1 != k2 {
				t.Errorf("expected same key, got %q vs %q", k1, k2)
			}
			if k1 == k3 {
				t.Errorf("expected different keys, both got %q", k1)
			}
		})
	}
}

func TestIsAggregateFunc(t *testing.T) {
	aggs := []string{"COUNT", "SUM", "AVG", "MIN", "MAX", "FIRST", "LAST", "ARRAY_AGG", "MEDIAN", "APPROX_COUNT_DISTINCT"}
	for _, name := range aggs {
		if !isAggregateFunc(name) {
			t.Errorf("expected %q to be aggregate", name)
		}
	}
	nonAggs := []string{"UPPER", "LOWER", "COALESCE", "TRIM"}
	for _, name := range nonAggs {
		if isAggregateFunc(name) {
			t.Errorf("expected %q to NOT be aggregate", name)
		}
	}
}
