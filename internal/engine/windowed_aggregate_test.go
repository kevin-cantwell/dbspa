package engine

import (
	"testing"
	"time"

	"github.com/kevin-cantwell/folddb/internal/sql/ast"
)

func TestWindowedAggregateOpTumbling(t *testing.T) {
	aggCols := []AggColumn{
		{Alias: "window_start", Expr: &ast.ColumnRef{Name: "window_start"}},
		{Alias: "endpoint", Expr: &ast.ColumnRef{Name: "endpoint"}},
		{Alias: "reqs", IsAggregate: true, AggFunc: "COUNT", IsStar: true},
		{Alias: "avg_lat", IsAggregate: true, AggFunc: "AVG", AggArg: &ast.ColumnRef{Name: "latency"}},
	}
	groupBy := []ast.Expr{&ast.ColumnRef{Name: "endpoint"}}

	windowSpec := WindowSpec{Type: "TUMBLING", Size: time.Minute, Advance: time.Minute}
	watermark := NewWatermarkTracker(0) // no delay for test

	eventTimeExpr := &ast.ColumnRef{Name: "ts"}

	op := NewWindowedAggregateOp(aggCols, groupBy, nil, windowSpec, eventTimeExpr, watermark, "FINAL", 0)

	in := make(chan Record, 3)
	out := make(chan Record, 10)

	// Send 3 records: 2 in first minute, 1 in second minute
	base := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)

	in <- Record{
		Columns: map[string]Value{
			"ts":       TextValue{V: base.Add(5 * time.Second).Format(time.RFC3339)},
			"endpoint": TextValue{V: "/api"},
			"latency":  IntValue{V: 50},
		},
		Timestamp: time.Now(),
		Diff:      1,
	}
	in <- Record{
		Columns: map[string]Value{
			"ts":       TextValue{V: base.Add(35 * time.Second).Format(time.RFC3339)},
			"endpoint": TextValue{V: "/api"},
			"latency":  IntValue{V: 100},
		},
		Timestamp: time.Now(),
		Diff:      1,
	}
	in <- Record{
		Columns: map[string]Value{
			"ts":       TextValue{V: base.Add(65 * time.Second).Format(time.RFC3339)},
			"endpoint": TextValue{V: "/api"},
			"latency":  IntValue{V: 75},
		},
		Timestamp: time.Now(),
		Diff:      1,
	}
	close(in)

	go op.Process(in, out)

	var results []Record
	for rec := range out {
		results = append(results, rec)
	}

	// Should get 2 results: one for each minute window
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// First window: 2 records, avg latency 75
	r1 := results[0]
	if r1.Columns["window_start"].String() != "2026-03-28T10:00:00Z" {
		t.Errorf("window_start = %v, want 2026-03-28T10:00:00Z", r1.Columns["window_start"])
	}
	if v, ok := r1.Columns["reqs"].(IntValue); !ok || v.V != 2 {
		t.Errorf("reqs = %v, want 2", r1.Columns["reqs"])
	}
	if v, ok := r1.Columns["avg_lat"].(FloatValue); !ok || v.V != 75.0 {
		t.Errorf("avg_lat = %v, want 75", r1.Columns["avg_lat"])
	}

	// Second window: 1 record, avg latency 75
	r2 := results[1]
	if r2.Columns["window_start"].String() != "2026-03-28T10:01:00Z" {
		t.Errorf("window_start = %v, want 2026-03-28T10:01:00Z", r2.Columns["window_start"])
	}
	if v, ok := r2.Columns["reqs"].(IntValue); !ok || v.V != 1 {
		t.Errorf("reqs = %v, want 1", r2.Columns["reqs"])
	}
}
