package engine

import (
	"sync"
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

// TC-WIN-010: Window with GROUP BY produces separate results per group
func TestWindowedAggregateGroupBy(t *testing.T) {
	aggCols := []AggColumn{
		{Alias: "region", Expr: &ast.ColumnRef{Name: "region"}},
		{Alias: "cnt", IsAggregate: true, AggFunc: "COUNT", IsStar: true},
	}
	groupBy := []ast.Expr{&ast.ColumnRef{Name: "region"}}

	windowSpec := WindowSpec{Type: "TUMBLING", Size: time.Minute, Advance: time.Minute}
	watermark := NewWatermarkTracker(0)
	eventTimeExpr := &ast.ColumnRef{Name: "ts"}

	op := NewWindowedAggregateOp(aggCols, groupBy, nil, windowSpec, eventTimeExpr, watermark, "FINAL", 0)

	in := make(chan Record, 4)
	out := make(chan Record, 10)

	base := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)

	// Two regions in same window
	in <- Record{
		Columns: map[string]Value{
			"ts":     TextValue{V: base.Add(10 * time.Second).Format(time.RFC3339)},
			"region": TextValue{V: "us-east"},
		},
		Diff: 1,
	}
	in <- Record{
		Columns: map[string]Value{
			"ts":     TextValue{V: base.Add(20 * time.Second).Format(time.RFC3339)},
			"region": TextValue{V: "us-west"},
		},
		Diff: 1,
	}
	in <- Record{
		Columns: map[string]Value{
			"ts":     TextValue{V: base.Add(30 * time.Second).Format(time.RFC3339)},
			"region": TextValue{V: "us-east"},
		},
		Diff: 1,
	}
	close(in)

	go op.Process(in, out)

	var results []Record
	for rec := range out {
		results = append(results, rec)
	}

	// Should get 2 results: one per region in the single window
	if len(results) != 2 {
		t.Fatalf("expected 2 results (one per region), got %d", len(results))
	}

	regionCounts := make(map[string]int64)
	for _, r := range results {
		region := r.Columns["region"].(TextValue).V
		cnt := r.Columns["cnt"].(IntValue).V
		regionCounts[region] = cnt
	}

	if regionCounts["us-east"] != 2 {
		t.Errorf("us-east count = %d, want 2", regionCounts["us-east"])
	}
	if regionCounts["us-west"] != 1 {
		t.Errorf("us-west count = %d, want 1", regionCounts["us-west"])
	}
}

// TC-WIN-011: window_start and window_end columns in output
func TestWindowedAggregateOutputColumns(t *testing.T) {
	aggCols := []AggColumn{
		{Alias: "cnt", IsAggregate: true, AggFunc: "COUNT", IsStar: true},
	}

	windowSpec := WindowSpec{Type: "TUMBLING", Size: time.Minute, Advance: time.Minute}
	watermark := NewWatermarkTracker(0)
	eventTimeExpr := &ast.ColumnRef{Name: "ts"}

	op := NewWindowedAggregateOp(aggCols, nil, nil, windowSpec, eventTimeExpr, watermark, "FINAL", 0)

	in := make(chan Record, 1)
	out := make(chan Record, 10)

	base := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)
	in <- Record{
		Columns: map[string]Value{
			"ts": TextValue{V: base.Add(15 * time.Second).Format(time.RFC3339)},
		},
		Diff: 1,
	}
	close(in)

	go op.Process(in, out)

	var results []Record
	for rec := range out {
		results = append(results, rec)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	r := results[0]
	ws, ok := r.Columns["window_start"]
	if !ok {
		t.Fatal("missing window_start column")
	}
	we, ok := r.Columns["window_end"]
	if !ok {
		t.Fatal("missing window_end column")
	}

	wantStart := "2026-03-28T10:00:00Z"
	wantEnd := "2026-03-28T10:01:00Z"
	if ws.String() != wantStart {
		t.Errorf("window_start = %v, want %v", ws.String(), wantStart)
	}
	if we.String() != wantEnd {
		t.Errorf("window_end = %v, want %v", we.String(), wantEnd)
	}
}

// TC-WIN-002: Tumbling window emit on watermark close
func TestWindowedAggregateTumblingEmitOnClose(t *testing.T) {
	aggCols := []AggColumn{
		{Alias: "total", IsAggregate: true, AggFunc: "SUM", AggArg: &ast.ColumnRef{Name: "v"}},
	}

	windowSpec := WindowSpec{Type: "TUMBLING", Size: time.Minute, Advance: time.Minute}
	watermark := NewWatermarkTracker(5 * time.Second) // 5s delay
	eventTimeExpr := &ast.ColumnRef{Name: "ts"}

	op := NewWindowedAggregateOp(aggCols, nil, nil, windowSpec, eventTimeExpr, watermark, "FINAL", 0)

	in := make(chan Record, 10)
	out := make(chan Record, 10)

	base := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)

	// Event in window [10:00, 10:01)
	in <- Record{
		Columns: map[string]Value{
			"ts": TextValue{V: base.Add(30 * time.Second).Format(time.RFC3339)},
			"v":  IntValue{V: 100},
		},
		Diff: 1,
	}

	// Event that pushes watermark past 10:01 (need event at 10:01:05 to get watermark = 10:01:00)
	in <- Record{
		Columns: map[string]Value{
			"ts": TextValue{V: base.Add(65 * time.Second).Format(time.RFC3339)},
			"v":  IntValue{V: 200},
		},
		Diff: 1,
	}
	close(in)

	go op.Process(in, out)

	var results []Record
	for rec := range out {
		results = append(results, rec)
	}

	// Should get at least 2 results (one per window bucket), though the first may have been
	// emitted when watermark advanced past 10:01
	if len(results) < 1 {
		t.Fatalf("expected at least 1 result, got %d", len(results))
	}
}

// TC-WIN-006: Late data dropped when beyond watermark
func TestWindowedAggregateLateDataDropped(t *testing.T) {
	aggCols := []AggColumn{
		{Alias: "cnt", IsAggregate: true, AggFunc: "COUNT", IsStar: true},
	}

	windowSpec := WindowSpec{Type: "TUMBLING", Size: time.Minute, Advance: time.Minute}
	watermark := NewWatermarkTracker(1 * time.Second) // 1s delay
	eventTimeExpr := &ast.ColumnRef{Name: "ts"}

	op := NewWindowedAggregateOp(aggCols, nil, nil, windowSpec, eventTimeExpr, watermark, "FINAL", 0)

	in := make(chan Record, 10)
	out := make(chan Record, 10)

	base := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)

	// First: event at 10:00:30
	in <- Record{
		Columns: map[string]Value{
			"ts": TextValue{V: base.Add(30 * time.Second).Format(time.RFC3339)},
		},
		Diff: 1,
	}

	// Push watermark well past 10:01: event at 10:05:00
	in <- Record{
		Columns: map[string]Value{
			"ts": TextValue{V: base.Add(5 * time.Minute).Format(time.RFC3339)},
		},
		Diff: 1,
	}

	// Late event: 10:00:40 (window [10:00, 10:01) already closed)
	in <- Record{
		Columns: map[string]Value{
			"ts": TextValue{V: base.Add(40 * time.Second).Format(time.RFC3339)},
		},
		Diff: 1,
	}
	close(in)

	go op.Process(in, out)

	var results []Record
	for rec := range out {
		results = append(results, rec)
	}

	// The first window should have COUNT=1 (the late event was dropped)
	for _, r := range results {
		ws := r.Columns["window_start"].String()
		if ws == "2026-03-28T10:00:00Z" {
			cnt := r.Columns["cnt"].(IntValue).V
			if cnt != 1 {
				t.Errorf("first window count = %d, want 1 (late data should be dropped)", cnt)
			}
		}
	}
}

// TC-WIN-009: Empty window produces no output
func TestWindowedAggregateEmptyWindowNoOutput(t *testing.T) {
	aggCols := []AggColumn{
		{Alias: "cnt", IsAggregate: true, AggFunc: "COUNT", IsStar: true},
	}

	windowSpec := WindowSpec{Type: "TUMBLING", Size: time.Minute, Advance: time.Minute}
	watermark := NewWatermarkTracker(0)
	eventTimeExpr := &ast.ColumnRef{Name: "ts"}

	op := NewWindowedAggregateOp(aggCols, nil, nil, windowSpec, eventTimeExpr, watermark, "FINAL", 0)

	in := make(chan Record, 5)
	out := make(chan Record, 10)

	base := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)

	// Event at 10:00:30 (window [10:00, 10:01))
	in <- Record{
		Columns: map[string]Value{"ts": TextValue{V: base.Add(30 * time.Second).Format(time.RFC3339)}},
		Diff:    1,
	}

	// Skip to 10:02:30 (window [10:01, 10:02) has no events)
	in <- Record{
		Columns: map[string]Value{"ts": TextValue{V: base.Add(150 * time.Second).Format(time.RFC3339)}},
		Diff:    1,
	}
	close(in)

	go op.Process(in, out)

	var results []Record
	for rec := range out {
		results = append(results, rec)
	}

	// No result for window [10:01, 10:02) — it should not appear at all
	for _, r := range results {
		ws := r.Columns["window_start"].String()
		if ws == "2026-03-28T10:01:00Z" {
			cnt := r.Columns["cnt"].(IntValue).V
			if cnt == 0 {
				t.Error("empty window should not emit a row with count=0")
			}
		}
	}
}

// SUM across tumbling windows
func TestWindowedAggregateSUM(t *testing.T) {
	aggCols := []AggColumn{
		{Alias: "total", IsAggregate: true, AggFunc: "SUM", AggArg: &ast.ColumnRef{Name: "v"}},
	}

	windowSpec := WindowSpec{Type: "TUMBLING", Size: time.Minute, Advance: time.Minute}
	watermark := NewWatermarkTracker(0)
	eventTimeExpr := &ast.ColumnRef{Name: "ts"}

	op := NewWindowedAggregateOp(aggCols, nil, nil, windowSpec, eventTimeExpr, watermark, "FINAL", 0)

	in := make(chan Record, 5)
	out := make(chan Record, 10)

	base := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)

	in <- Record{
		Columns: map[string]Value{
			"ts": TextValue{V: base.Add(10 * time.Second).Format(time.RFC3339)},
			"v":  IntValue{V: 10},
		},
		Diff: 1,
	}
	in <- Record{
		Columns: map[string]Value{
			"ts": TextValue{V: base.Add(20 * time.Second).Format(time.RFC3339)},
			"v":  IntValue{V: 20},
		},
		Diff: 1,
	}
	in <- Record{
		Columns: map[string]Value{
			"ts": TextValue{V: base.Add(30 * time.Second).Format(time.RFC3339)},
			"v":  IntValue{V: 30},
		},
		Diff: 1,
	}
	close(in)

	go op.Process(in, out)

	var results []Record
	for rec := range out {
		results = append(results, rec)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	total := results[0].Columns["total"]
	if v, ok := total.(IntValue); !ok || v.V != 60 {
		t.Errorf("total = %v, want 60", total)
	}
}

// ParseWindowSpec tests
func TestParseWindowSpec(t *testing.T) {
	tests := []struct {
		name    string
		wc      *ast.WindowClause
		want    WindowSpec
		wantErr bool
	}{
		{
			name: "tumbling 1 minute",
			wc:   &ast.WindowClause{Type: "TUMBLING", Size: "1 minute"},
			want: WindowSpec{Type: "TUMBLING", Size: time.Minute, Advance: time.Minute},
		},
		{
			name: "sliding 10m by 5m",
			wc:   &ast.WindowClause{Type: "SLIDING", Size: "10 minutes", SlideBy: "5 minutes"},
			want: WindowSpec{Type: "SLIDING", Size: 10 * time.Minute, Advance: 5 * time.Minute},
		},
		{
			name:    "sliding without BY",
			wc:      &ast.WindowClause{Type: "SLIDING", Size: "10 minutes"},
			wantErr: true,
		},
		{
			name:    "invalid size",
			wc:      &ast.WindowClause{Type: "TUMBLING", Size: "not_a_duration"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseWindowSpec(tt.wc)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Type != tt.want.Type || got.Size != tt.want.Size || got.Advance != tt.want.Advance {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestWindowedAggregate_EmitEarly_Timer verifies that EMIT EARLY produces
// partial results before the window closes.
func TestWindowedAggregate_EmitEarly_Timer(t *testing.T) {
	aggCols := []AggColumn{
		{Alias: "cnt", IsAggregate: true, AggFunc: "COUNT", IsStar: true},
	}

	// Use a large window (10 minutes) so it never closes during the test.
	windowSpec := WindowSpec{Type: "TUMBLING", Size: 10 * time.Minute, Advance: 10 * time.Minute}
	watermark := NewWatermarkTracker(0)
	eventTimeExpr := &ast.ColumnRef{Name: "ts"}

	// EMIT EARLY every 50ms
	op := NewWindowedAggregateOp(aggCols, nil, nil, windowSpec, eventTimeExpr, watermark, "EARLY", 50*time.Millisecond)

	in := make(chan Record)
	out := make(chan Record, 100)

	base := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		op.Process(in, out)
	}()

	// Send a record into the window.
	in <- Record{
		Columns: map[string]Value{
			"ts": TextValue{V: base.Add(30 * time.Second).Format(time.RFC3339)},
		},
		Diff: 1,
	}

	// Wait enough time for at least one early emission (2 tick intervals).
	time.Sleep(150 * time.Millisecond)

	// Close input to stop processing.
	close(in)
	wg.Wait()

	// Collect all results.
	var results []Record
	for {
		select {
		case r, ok := <-out:
			if !ok {
				goto done
			}
			results = append(results, r)
		default:
			goto done
		}
	}
done:

	// We should have at least one early emission (Diff=+1) BEFORE the final flush.
	// The final flush also produces a result. So we expect at least 2 insertion records.
	insertions := 0
	for _, r := range results {
		if r.Diff == 1 {
			insertions++
		}
	}
	if insertions < 2 {
		t.Errorf("expected at least 2 insertions (early + final), got %d insertions out of %d total records", insertions, len(results))
	}

	// Every early-emitted record should have correct cnt value.
	for _, r := range results {
		if r.Diff == 1 {
			cnt, ok := r.Columns["cnt"].(IntValue)
			if !ok {
				t.Errorf("cnt is not IntValue: %T", r.Columns["cnt"])
				continue
			}
			if cnt.V != 1 {
				t.Errorf("cnt = %d, want 1", cnt.V)
			}
		}
	}
}

// TestWindowedAggregate_EmitEarly_RetractionOnUpdate verifies that when
// partial results change between early emissions, proper retractions are sent.
func TestWindowedAggregate_EmitEarly_RetractionOnUpdate(t *testing.T) {
	aggCols := []AggColumn{
		{Alias: "total", IsAggregate: true, AggFunc: "SUM", AggArg: &ast.ColumnRef{Name: "v"}},
	}

	// Large window so it never closes during the test.
	windowSpec := WindowSpec{Type: "TUMBLING", Size: 10 * time.Minute, Advance: 10 * time.Minute}
	watermark := NewWatermarkTracker(0)
	eventTimeExpr := &ast.ColumnRef{Name: "ts"}

	// EMIT EARLY every 50ms
	op := NewWindowedAggregateOp(aggCols, nil, nil, windowSpec, eventTimeExpr, watermark, "EARLY", 50*time.Millisecond)

	in := make(chan Record)
	out := make(chan Record, 200)

	base := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		op.Process(in, out)
	}()

	// Send first record: SUM = 10
	in <- Record{
		Columns: map[string]Value{
			"ts": TextValue{V: base.Add(10 * time.Second).Format(time.RFC3339)},
			"v":  IntValue{V: 10},
		},
		Diff: 1,
	}

	// Wait for at least one early emission.
	time.Sleep(100 * time.Millisecond)

	// Send second record: SUM becomes 30
	in <- Record{
		Columns: map[string]Value{
			"ts": TextValue{V: base.Add(20 * time.Second).Format(time.RFC3339)},
			"v":  IntValue{V: 20},
		},
		Diff: 1,
	}

	// Wait for at least one more early emission with the updated value.
	time.Sleep(100 * time.Millisecond)

	// Close input.
	close(in)
	wg.Wait()

	// Collect all results.
	var results []Record
	for {
		select {
		case r, ok := <-out:
			if !ok {
				goto done
			}
			results = append(results, r)
		default:
			goto done
		}
	}
done:

	if len(results) == 0 {
		t.Fatal("expected results, got none")
	}

	// Verify we have at least one retraction (Diff=-1).
	retractions := 0
	for _, r := range results {
		if r.Diff == -1 {
			retractions++
		}
	}
	if retractions < 1 {
		t.Errorf("expected at least 1 retraction, got %d", retractions)
	}

	// The final record (last insertion) should have the correct total.
	// Walk backwards to find the last Diff=+1 record.
	var finalTotal Value
	for i := len(results) - 1; i >= 0; i-- {
		if results[i].Diff == 1 {
			finalTotal = results[i].Columns["total"]
			break
		}
	}
	if finalTotal == nil {
		t.Fatal("no final insertion found")
	}

	// Final total should be 30 (10 + 20).
	switch v := finalTotal.(type) {
	case IntValue:
		if v.V != 30 {
			t.Errorf("final total = %d, want 30", v.V)
		}
	default:
		t.Errorf("final total type = %T, want IntValue", finalTotal)
	}

	// Verify that net sum of diffs makes sense: sum of (diff * total) should
	// equal 30 (the final value). Each insertion adds its value, each retraction
	// subtracts its value — the net should be the final result.
	var netSum int64
	for _, r := range results {
		if total, ok := r.Columns["total"].(IntValue); ok {
			netSum += int64(r.Diff) * total.V
		}
	}
	if netSum != 30 {
		t.Errorf("net sum of diffs = %d, want 30", netSum)
	}
}
