package engine

import (
	"testing"
	"time"

	"github.com/kevin-cantwell/dbspa/internal/sql/ast"
)

// helper to make a record with diff
func mkRec(diff int, cols map[string]Value) Record {
	return Record{
		Columns:   cols,
		Timestamp: time.Now(),
		Weight:      diff,
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
	if results[0].Weight != 1 {
		t.Errorf("result[0] expected diff=+1, got %d", results[0].Weight)
	}
	if v := results[0].Columns["cnt"].(IntValue).V; v != 1 {
		t.Errorf("result[0] cnt expected 1, got %d", v)
	}

	// Last: insert a cnt=2
	if results[3].Weight != 1 {
		t.Errorf("result[3] expected diff=+1, got %d", results[3].Weight)
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
	if results[1].Weight != -1 {
		t.Errorf("result[1] expected retraction (diff=-1), got %d", results[1].Weight)
	}
	if results[2].Weight != 1 {
		t.Errorf("result[2] expected insertion (diff=+1), got %d", results[2].Weight)
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
		if r.Weight == 1 {
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
		if r.Weight == -1 {
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
	if last.Weight != 1 {
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
	if results[0].Weight != 1 {
		t.Errorf("result[0] expected +1")
	}
	if results[1].Weight != -1 {
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
	if results[2].Weight != 1 {
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
		if gv.IsNull() && r.Weight == 1 {
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
	if last.Weight != 1 {
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
		if r.Weight == 1 {
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
		if r.Weight == 1 {
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
	if last.Weight != 1 {
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
	if last.Weight != 1 {
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
		if !IsAggregateFunc(name) {
			t.Errorf("expected %q to be aggregate", name)
		}
	}
	nonAggs := []string{"UPPER", "LOWER", "COALESCE", "TRIM"}
	for _, name := range nonAggs {
		if IsAggregateFunc(name) {
			t.Errorf("expected %q to NOT be aggregate", name)
		}
	}
}

// --- ImportInitialState tests ---

// buildSeedTestAggOp creates an AggregateOp with multiple aggregate columns:
//
//	SELECT groupCol, <agg1Func>(<agg1Arg>) AS <agg1Alias>, <agg2Func>(<agg2Arg>) AS <agg2Alias> GROUP BY groupCol
func buildSeedTestAggOp(groupCol string, aggs []struct {
	Func, Arg, Alias string
}) *AggregateOp {
	columns := []AggColumn{
		{Alias: groupCol, Expr: &ast.ColumnRef{Name: groupCol}, GroupByIdx: 0},
	}
	for _, a := range aggs {
		isStar := a.Arg == "*"
		var argExpr ast.Expr
		if !isStar {
			argExpr = &ast.ColumnRef{Name: a.Arg}
		}
		columns = append(columns, AggColumn{
			Alias:       a.Alias,
			IsAggregate: true,
			AggFunc:     a.Func,
			AggArg:      argExpr,
			IsStar:      isStar,
			GroupByIdx:  -1,
		})
	}
	groupBy := []ast.Expr{&ast.ColumnRef{Name: groupCol}}
	return NewAggregateOp(columns, groupBy, nil)
}

func TestImportInitialState_CountAndSum(t *testing.T) {
	// SELECT region, COUNT(*) AS cnt, SUM(amount) AS total GROUP BY region
	op := buildSeedTestAggOp("region", []struct{ Func, Arg, Alias string }{
		{"COUNT", "*", "cnt"},
		{"SUM", "amount", "total"},
	})

	// Seed: region=us-east has cnt=100, total=1000000
	seedRecords := []Record{
		mkRec(1, map[string]Value{
			"region": TextValue{V: "us-east"},
			"cnt":    IntValue{V: 100},
			"total":  IntValue{V: 1000000},
		}),
	}

	seedOut := make(chan Record, 10)
	op.ImportInitialState(seedRecords, seedOut)
	close(seedOut)

	var seeded []Record
	for r := range seedOut {
		seeded = append(seeded, r)
	}

	if len(seeded) != 1 {
		t.Fatalf("expected 1 seeded record, got %d", len(seeded))
	}
	if seeded[0].Weight != 1 {
		t.Errorf("seeded record weight: got %d, want 1", seeded[0].Weight)
	}
	if v, ok := seeded[0].Columns["cnt"].(IntValue); !ok || v.V != 100 {
		t.Errorf("seeded cnt: got %v, want 100", seeded[0].Columns["cnt"])
	}
	if v, ok := seeded[0].Columns["total"].(IntValue); !ok || v.V != 1000000 {
		t.Errorf("seeded total: got %v, want 1000000", seeded[0].Columns["total"])
	}

	// Now stream a record: region=us-east, amount=50
	streamRecords := []Record{
		mkRec(1, map[string]Value{"region": TextValue{V: "us-east"}, "amount": IntValue{V: 50}}),
	}
	results := processAndCollect(op, streamRecords)

	// Should retract old state and emit new: cnt=101, total=1000050
	var lastInsert Record
	for _, r := range results {
		if r.Weight == 1 {
			lastInsert = r
		}
	}
	if v, ok := lastInsert.Columns["cnt"].(IntValue); !ok || v.V != 101 {
		t.Errorf("after stream cnt: got %v, want 101", lastInsert.Columns["cnt"])
	}
	if v, ok := lastInsert.Columns["total"].(IntValue); !ok || v.V != 1000050 {
		t.Errorf("after stream total: got %v, want 1000050", lastInsert.Columns["total"])
	}
}

func TestImportInitialState_MinMax(t *testing.T) {
	// SELECT region, MIN(amount) AS lo, MAX(amount) AS hi GROUP BY region
	op := buildSeedTestAggOp("region", []struct{ Func, Arg, Alias string }{
		{"MIN", "amount", "lo"},
		{"MAX", "amount", "hi"},
	})

	seedRecords := []Record{
		mkRec(1, map[string]Value{
			"region": TextValue{V: "west"},
			"lo":     IntValue{V: 5},
			"hi":     IntValue{V: 999},
		}),
	}

	seedOut := make(chan Record, 10)
	op.ImportInitialState(seedRecords, seedOut)
	close(seedOut)

	var seeded []Record
	for r := range seedOut {
		seeded = append(seeded, r)
	}

	if len(seeded) != 1 {
		t.Fatalf("expected 1 seeded record, got %d", len(seeded))
	}
	if v, ok := seeded[0].Columns["lo"].(IntValue); !ok || v.V != 5 {
		t.Errorf("seeded lo: got %v, want 5", seeded[0].Columns["lo"])
	}
	if v, ok := seeded[0].Columns["hi"].(IntValue); !ok || v.V != 999 {
		t.Errorf("seeded hi: got %v, want 999", seeded[0].Columns["hi"])
	}

	// Stream: amount=3 (new min), amount=500 (no change to min or max)
	streamRecords := []Record{
		mkRec(1, map[string]Value{"region": TextValue{V: "west"}, "amount": IntValue{V: 3}}),
		mkRec(1, map[string]Value{"region": TextValue{V: "west"}, "amount": IntValue{V: 500}}),
	}
	results := processAndCollect(op, streamRecords)

	// After amount=3: lo should be 3, hi stays 999
	// After amount=500: lo stays 3, hi stays 999 (500 < 999)
	var lastInsert Record
	for _, r := range results {
		if r.Weight == 1 {
			lastInsert = r
		}
	}
	if v, ok := lastInsert.Columns["lo"].(IntValue); !ok || v.V != 3 {
		t.Errorf("after stream lo: got %v, want 3", lastInsert.Columns["lo"])
	}
	if v, ok := lastInsert.Columns["hi"].(IntValue); !ok || v.V != 999 {
		t.Errorf("after stream hi: got %v, want 999", lastInsert.Columns["hi"])
	}
}

func TestImportInitialState_MissingSeedColumn(t *testing.T) {
	// SELECT region, COUNT(*) AS cnt, SUM(amount) AS total GROUP BY region
	op := buildSeedTestAggOp("region", []struct{ Func, Arg, Alias string }{
		{"COUNT", "*", "cnt"},
		{"SUM", "amount", "total"},
	})

	// Seed only has "cnt", missing "total"
	seedRecords := []Record{
		mkRec(1, map[string]Value{
			"region": TextValue{V: "us-east"},
			"cnt":    IntValue{V: 50},
		}),
	}

	seedOut := make(chan Record, 10)
	op.ImportInitialState(seedRecords, seedOut)
	close(seedOut)

	var seeded []Record
	for r := range seedOut {
		seeded = append(seeded, r)
	}

	if len(seeded) != 1 {
		t.Fatalf("expected 1 seeded record, got %d", len(seeded))
	}
	// cnt should be seeded at 50
	if v, ok := seeded[0].Columns["cnt"].(IntValue); !ok || v.V != 50 {
		t.Errorf("seeded cnt: got %v, want 50", seeded[0].Columns["cnt"])
	}
	// total should be zero/null (missing from seed)
	totalVal := seeded[0].Columns["total"]
	if totalVal == nil || totalVal.IsNull() {
		// Expected: SUM starts at NULL (no value)
	} else if v, ok := totalVal.(IntValue); ok && v.V != 0 {
		t.Errorf("missing seed total: got %v, want NULL or 0", totalVal)
	}

	// Stream a record — total should start accumulating from zero
	streamRecords := []Record{
		mkRec(1, map[string]Value{"region": TextValue{V: "us-east"}, "amount": IntValue{V: 100}}),
	}
	results := processAndCollect(op, streamRecords)

	var lastInsert Record
	for _, r := range results {
		if r.Weight == 1 {
			lastInsert = r
		}
	}
	if v, ok := lastInsert.Columns["cnt"].(IntValue); !ok || v.V != 51 {
		t.Errorf("after stream cnt: got %v, want 51", lastInsert.Columns["cnt"])
	}
	if v, ok := lastInsert.Columns["total"].(IntValue); !ok || v.V != 100 {
		t.Errorf("after stream total: got %v, want 100", lastInsert.Columns["total"])
	}
}

func TestImportInitialState_MultipleGroups(t *testing.T) {
	// SELECT region, SUM(amount) AS total GROUP BY region
	op := buildSimpleAggOp("region", "SUM", "amount", "total", nil)

	seedRecords := []Record{
		mkRec(1, map[string]Value{"region": TextValue{V: "us-east"}, "total": IntValue{V: 1000}}),
		mkRec(1, map[string]Value{"region": TextValue{V: "us-west"}, "total": IntValue{V: 2000}}),
		mkRec(1, map[string]Value{"region": TextValue{V: "eu-west"}, "total": IntValue{V: 3000}}),
	}

	seedOut := make(chan Record, 10)
	op.ImportInitialState(seedRecords, seedOut)
	close(seedOut)

	var seeded []Record
	for r := range seedOut {
		seeded = append(seeded, r)
	}

	if len(seeded) != 3 {
		t.Fatalf("expected 3 seeded records, got %d", len(seeded))
	}

	// Verify each group was seeded
	seededByRegion := make(map[string]int64)
	for _, r := range seeded {
		region := r.Columns["region"].(TextValue).V
		total := r.Columns["total"].(IntValue).V
		seededByRegion[region] = total
	}
	if seededByRegion["us-east"] != 1000 {
		t.Errorf("us-east: got %d, want 1000", seededByRegion["us-east"])
	}
	if seededByRegion["us-west"] != 2000 {
		t.Errorf("us-west: got %d, want 2000", seededByRegion["us-west"])
	}
	if seededByRegion["eu-west"] != 3000 {
		t.Errorf("eu-west: got %d, want 3000", seededByRegion["eu-west"])
	}

	// Stream a record to us-east only
	streamRecords := []Record{
		mkRec(1, map[string]Value{"region": TextValue{V: "us-east"}, "amount": IntValue{V: 50}}),
	}
	results := processAndCollect(op, streamRecords)

	// us-east should update to 1050, others unchanged
	var lastInsert Record
	for _, r := range results {
		if r.Weight == 1 {
			lastInsert = r
		}
	}
	if v := lastInsert.Columns["total"].(IntValue).V; v != 1050 {
		t.Errorf("us-east after stream: got %d, want 1050", v)
	}
}
