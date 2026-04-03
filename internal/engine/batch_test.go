package engine

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kevin-cantwell/dbspa/internal/sql/ast"
)

func TestBatchChannel_FullBatch(t *testing.T) {
	in := make(chan Record, 10)
	batchSize := 3

	// Send exactly one full batch + close
	for i := 0; i < 3; i++ {
		in <- NewRecord(map[string]Value{"i": IntValue{V: int64(i)}})
	}
	close(in)

	out := BatchChannel(in, batchSize)

	batch, ok := <-out
	if !ok {
		t.Fatal("expected a batch, channel was closed")
	}
	if len(batch) != 3 {
		t.Fatalf("expected batch of 3, got %d", len(batch))
	}

	// Channel should be closed after all records consumed
	_, ok = <-out
	if ok {
		t.Fatal("expected channel to be closed")
	}
}

func TestBatchChannel_PartialFlush(t *testing.T) {
	// Send fewer records than batch size — they should flush via timer
	in := make(chan Record, 10)
	batchSize := 100

	in <- NewRecord(map[string]Value{"x": IntValue{V: 1}})
	in <- NewRecord(map[string]Value{"x": IntValue{V: 2}})
	close(in)

	out := BatchChannel(in, batchSize)

	batch, ok := <-out
	if !ok {
		t.Fatal("expected a batch")
	}
	if len(batch) != 2 {
		t.Fatalf("expected batch of 2, got %d", len(batch))
	}

	_, ok = <-out
	if ok {
		t.Fatal("expected channel to be closed")
	}
}

func TestBatchChannel_TimerFlush(t *testing.T) {
	// Send one record, leave channel open, verify timer flushes it
	in := make(chan Record, 10)
	batchSize := 100

	in <- NewRecord(map[string]Value{"x": IntValue{V: 1}})

	out := BatchChannel(in, batchSize)

	select {
	case batch := <-out:
		if len(batch) != 1 {
			t.Fatalf("expected batch of 1, got %d", len(batch))
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timer did not flush partial batch within 100ms")
	}

	close(in)
}

func TestBatchChannel_MultipleBatches(t *testing.T) {
	in := make(chan Record, 20)
	batchSize := 3

	// Send 7 records → expect batches of 3, 3, 1
	for i := 0; i < 7; i++ {
		in <- NewRecord(map[string]Value{"i": IntValue{V: int64(i)}})
	}
	close(in)

	out := BatchChannel(in, batchSize)

	var batches []Batch
	for b := range out {
		batches = append(batches, b)
	}

	total := 0
	for _, b := range batches {
		total += len(b)
	}
	if total != 7 {
		t.Fatalf("expected 7 total records, got %d", total)
	}

	// First two batches should be full
	if len(batches) < 2 {
		t.Fatalf("expected at least 2 batches, got %d", len(batches))
	}
	if len(batches[0]) != 3 {
		t.Fatalf("expected first batch of 3, got %d", len(batches[0]))
	}
	if len(batches[1]) != 3 {
		t.Fatalf("expected second batch of 3, got %d", len(batches[1]))
	}
}

func TestBatchChannel_Empty(t *testing.T) {
	in := make(chan Record)
	close(in)

	out := BatchChannel(in, 10)

	_, ok := <-out
	if ok {
		t.Fatal("expected channel to be closed immediately for empty input")
	}
}

func TestUnbatchChannel(t *testing.T) {
	in := make(chan Batch, 3)

	b1 := Batch{
		NewRecord(map[string]Value{"x": IntValue{V: 1}}),
		NewRecord(map[string]Value{"x": IntValue{V: 2}}),
	}
	b2 := Batch{
		NewRecord(map[string]Value{"x": IntValue{V: 3}}),
	}
	in <- b1
	in <- b2
	close(in)

	out := UnbatchChannel(in)

	var records []Record
	for r := range out {
		records = append(records, r)
	}

	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}
	for i, rec := range records {
		v := rec.Get("x").(IntValue).V
		if v != int64(i+1) {
			t.Fatalf("record %d: expected x=%d, got x=%d", i, i+1, v)
		}
	}
}

func TestBatchUnbatchRoundtrip(t *testing.T) {
	// Records → BatchChannel → UnbatchChannel should preserve all records in order
	in := make(chan Record, 100)
	n := 50
	for i := 0; i < n; i++ {
		in <- NewRecord(map[string]Value{"i": IntValue{V: int64(i)}})
	}
	close(in)

	batched := BatchChannel(in, 8)
	unbatched := UnbatchChannel(batched)

	var got []int64
	for rec := range unbatched {
		got = append(got, rec.Get("i").(IntValue).V)
	}

	if len(got) != n {
		t.Fatalf("expected %d records, got %d", n, len(got))
	}
	for i, v := range got {
		if v != int64(i) {
			t.Fatalf("record %d: expected %d, got %d", i, i, v)
		}
	}
}

func TestAggregateOp_ProcessBatch(t *testing.T) {
	// Verify ProcessBatches produces same results as Process
	cols := []AggColumn{
		{Alias: "total", IsAggregate: true, AggFunc: "COUNT", IsStar: true, GroupByIdx: -1},
	}

	records := []Record{
		NewRecord(map[string]Value{"a": IntValue{V: 1}}),
		NewRecord(map[string]Value{"a": IntValue{V: 2}}),
		NewRecord(map[string]Value{"a": IntValue{V: 3}}),
	}

	// Using Process (record-at-a-time)
	op1 := NewAggregateOp(cols, nil, nil)
	in1 := make(chan Record, len(records))
	for _, r := range records {
		in1 <- r
	}
	close(in1)
	out1 := make(chan Record, 100)
	op1.Process(in1, out1)

	var results1 []Record
	for r := range out1 {
		results1 = append(results1, r)
	}

	// Using ProcessBatches
	op2 := NewAggregateOp(cols, nil, nil)
	batchIn := make(chan Batch, 1)
	batchIn <- Batch(records)
	close(batchIn)
	out2 := make(chan Record, 100)
	op2.ProcessBatches(batchIn, out2)

	var results2 []Record
	for r := range out2 {
		results2 = append(results2, r)
	}

	// Both should produce the same number of output records
	if len(results1) != len(results2) {
		t.Fatalf("Process produced %d records, ProcessBatches produced %d", len(results1), len(results2))
	}

	// Final record should show count=3 in both
	last1 := results1[len(results1)-1]
	last2 := results2[len(results2)-1]

	v1 := last1.Get("total").(IntValue).V
	v2 := last2.Get("total").(IntValue).V
	if v1 != v2 {
		t.Fatalf("Process final count=%d, ProcessBatches final count=%d", v1, v2)
	}
	if v1 != 3 {
		t.Fatalf("expected final count=3, got %d", v1)
	}
}

// Benchmarks comparing record-at-a-time vs batched throughput

func BenchmarkPipeline_RecordAtATime(b *testing.B) {
	records := makeBenchRecords(b.N)

	in := make(chan Record, 256)
	out := make(chan Record, 256)

	go func() {
		defer close(in)
		for _, r := range records {
			in <- r
		}
	}()

	p := &Pipeline{}
	go func() {
		p.Process(in, out)
	}()

	for range out {
	}
}

func BenchmarkPipeline_Batched(b *testing.B) {
	records := makeBenchRecords(b.N)

	in := make(chan Record, 256)
	out := make(chan Record, 256)

	go func() {
		defer close(in)
		for _, r := range records {
			in <- r
		}
	}()

	batched := BatchChannel(in, DefaultBatchSize)

	go func() {
		p := &Pipeline{}
		p.ProcessBatches(batched, out)
	}()

	for range out {
	}
}

func BenchmarkAggregate_RecordAtATime(b *testing.B) {
	records := makeBenchRecords(b.N)
	cols := []AggColumn{
		{Alias: "total", IsAggregate: true, AggFunc: "COUNT", IsStar: true, GroupByIdx: -1},
	}

	in := make(chan Record, 256)
	out := make(chan Record, 256)

	go func() {
		defer close(in)
		for _, r := range records {
			in <- r
		}
	}()

	op := NewAggregateOp(cols, nil, nil)
	go func() {
		op.Process(in, out)
	}()

	for range out {
	}
}

func BenchmarkAggregate_Batched(b *testing.B) {
	records := makeBenchRecords(b.N)
	cols := []AggColumn{
		{Alias: "total", IsAggregate: true, AggFunc: "COUNT", IsStar: true, GroupByIdx: -1},
	}

	in := make(chan Record, 256)
	out := make(chan Record, 256)

	go func() {
		defer close(in)
		for _, r := range records {
			in <- r
		}
	}()

	batched := BatchChannel(in, DefaultBatchSize)

	op := NewAggregateOp(cols, nil, nil)
	go func() {
		op.ProcessBatches(batched, out)
	}()

	for range out {
	}
}

// --- CompactBatch tests ---

func TestCompactBatch_OppositeWeightsCancel(t *testing.T) {
	// Two records with same columns but opposite weights should cancel out
	batch := make(Batch, CompactBatchThreshold+2)
	// Fill with unique filler records to exceed threshold
	for i := 0; i < CompactBatchThreshold; i++ {
		batch[i] = Record{
			Columns: map[string]Value{"x": IntValue{V: int64(i + 100)}},
			Weight:  1,
		}
	}
	// Add a pair that should cancel
	batch[CompactBatchThreshold] = Record{
		Columns: map[string]Value{"x": IntValue{V: 42}},
		Weight:  1,
	}
	batch[CompactBatchThreshold+1] = Record{
		Columns: map[string]Value{"x": IntValue{V: 42}},
		Weight:  -1,
	}

	result := CompactBatch(batch)

	// The cancelling pair should be gone
	for _, rec := range result {
		if v, ok := rec.Get("x").(IntValue); ok && v.V == 42 {
			t.Fatalf("expected x=42 to be cancelled, but found it with weight=%d", rec.Weight)
		}
	}
	if len(result) != CompactBatchThreshold {
		t.Fatalf("expected %d records after compaction, got %d", CompactBatchThreshold, len(result))
	}
}

func TestCompactBatch_SameWeightAccumulates(t *testing.T) {
	// Two inserts of the same record should merge into weight=+2
	batch := make(Batch, CompactBatchThreshold+2)
	for i := 0; i < CompactBatchThreshold; i++ {
		batch[i] = Record{
			Columns: map[string]Value{"x": IntValue{V: int64(i + 100)}},
			Weight:  1,
		}
	}
	batch[CompactBatchThreshold] = Record{
		Columns: map[string]Value{"x": IntValue{V: 99}},
		Weight:  1,
	}
	batch[CompactBatchThreshold+1] = Record{
		Columns: map[string]Value{"x": IntValue{V: 99}},
		Weight:  1,
	}

	result := CompactBatch(batch)

	found := false
	for _, rec := range result {
		if v, ok := rec.Get("x").(IntValue); ok && v.V == 99 {
			if rec.Weight != 2 {
				t.Fatalf("expected weight=2 for x=99, got weight=%d", rec.Weight)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("expected to find x=99 in compacted batch")
	}
}

func TestCompactBatch_MixedBatchReducesSize(t *testing.T) {
	// Build a batch with many duplicates — compaction should reduce it
	n := 100
	batch := make(Batch, n)
	for i := 0; i < n; i++ {
		// Only 5 distinct values
		batch[i] = Record{
			Columns: map[string]Value{"x": IntValue{V: int64(i % 5)}},
			Weight:  1,
		}
	}

	result := CompactBatch(batch)

	if len(result) != 5 {
		t.Fatalf("expected 5 unique records after compaction, got %d", len(result))
	}

	// Each should have weight=20 (100 records / 5 distinct values)
	for _, rec := range result {
		if rec.Weight != 20 {
			t.Fatalf("expected weight=20, got weight=%d for %v", rec.Weight, rec.Get("x"))
		}
	}
}

func TestCompactBatch_BelowThresholdReturnsUnchanged(t *testing.T) {
	// Small batch should be returned as-is (no compaction overhead)
	batch := Batch{
		{Columns: map[string]Value{"x": IntValue{V: 1}}, Weight: 1},
		{Columns: map[string]Value{"x": IntValue{V: 1}}, Weight: -1},
	}

	result := CompactBatch(batch)

	// Should NOT be compacted because batch is below threshold
	if len(result) != 2 {
		t.Fatalf("expected 2 records (no compaction below threshold), got %d", len(result))
	}
}

func TestCompactBatch_CorrectnessSameAsUncompacted(t *testing.T) {
	// Build a batch with insertions and retractions for a COUNT(*) aggregation
	// and verify compacted processing produces identical final state.
	n := 200
	batch := make(Batch, n)
	for i := 0; i < n; i++ {
		batch[i] = Record{
			Columns: map[string]Value{"region": TextValue{V: "us-east"}, "amount": IntValue{V: int64(i % 10)}},
			Weight:  1,
		}
	}
	// Add some retractions
	for i := 0; i < 50; i++ {
		batch = append(batch, Record{
			Columns: map[string]Value{"region": TextValue{V: "us-east"}, "amount": IntValue{V: int64(i % 10)}},
			Weight:  -1,
		})
	}

	cols := []AggColumn{
		{Alias: "region", Expr: &ast.ColumnRef{Name: "region"}, GroupByIdx: 0},
		{Alias: "total", IsAggregate: true, AggFunc: "COUNT", IsStar: true, GroupByIdx: -1},
	}
	groupBy := []ast.Expr{&ast.ColumnRef{Name: "region"}}

	// Non-compacted: process the full batch record-at-a-time
	op1 := NewAggregateOp(cols, groupBy, nil)
	in1 := make(chan Record, len(batch))
	for _, r := range batch {
		in1 <- r
	}
	close(in1)
	out1 := make(chan Record, 1000)
	op1.Process(in1, out1)

	var results1 []Record
	for r := range out1 {
		results1 = append(results1, r)
	}

	// Compacted: process via ProcessBatch (which calls CompactBatch)
	op2 := NewAggregateOp(cols, groupBy, nil)
	out2 := make(chan Record, 1000)
	op2.ProcessBatch(batch, out2)
	close(out2)

	var results2 []Record
	for r := range out2 {
		results2 = append(results2, r)
	}

	// Both should arrive at the same final count.
	// Get final insertion record from each.
	var final1, final2 Record
	for _, r := range results1 {
		if r.Weight > 0 {
			final1 = r
		}
	}
	for _, r := range results2 {
		if r.Weight > 0 {
			final2 = r
		}
	}

	v1 := final1.Get("total").(IntValue).V
	v2 := final2.Get("total").(IntValue).V
	if v1 != v2 {
		t.Fatalf("correctness violation: non-compacted count=%d, compacted count=%d", v1, v2)
	}
	// 200 inserts - 50 retracts = 150
	if v1 != 150 {
		t.Fatalf("expected final count=150, got %d", v1)
	}
}

// --- GroupByKey tests ---

func TestGroupByKey_PartitionsByKey(t *testing.T) {
	batch := Batch{
		{Columns: map[string]Value{"region": TextValue{V: "us-east"}, "x": IntValue{V: 1}}, Weight: 1},
		{Columns: map[string]Value{"region": TextValue{V: "us-west"}, "x": IntValue{V: 2}}, Weight: 1},
		{Columns: map[string]Value{"region": TextValue{V: "us-east"}, "x": IntValue{V: 3}}, Weight: 1},
		{Columns: map[string]Value{"region": TextValue{V: "eu"}, "x": IntValue{V: 4}}, Weight: 1},
		{Columns: map[string]Value{"region": TextValue{V: "us-west"}, "x": IntValue{V: 5}}, Weight: 1},
	}

	keyExprs := []ast.Expr{&ast.ColumnRef{Name: "region"}}
	groups := GroupByKey(batch, keyExprs)

	if len(groups) != 3 {
		t.Fatalf("expected 3 groups, got %d", len(groups))
	}

	// Count records per group
	for key, sub := range groups {
		switch {
		case len(sub) == 2:
			// us-east or us-west
		case len(sub) == 1:
			// eu
		default:
			t.Fatalf("unexpected group %s with %d records", key, len(sub))
		}
	}
}

// --- Compaction benchmarks ---

func BenchmarkCompactBatch_HighDuplication(b *testing.B) {
	// Batch with lots of duplicates — compaction should help
	batch := make(Batch, 1024)
	for i := range batch {
		batch[i] = Record{
			Columns: map[string]Value{"x": IntValue{V: int64(i % 10)}},
			Weight:  1,
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CompactBatch(batch)
	}
}

func BenchmarkCompactBatch_NoDuplication(b *testing.B) {
	// Batch with all unique records — compaction is pure overhead
	batch := make(Batch, 1024)
	for i := range batch {
		batch[i] = Record{
			Columns: map[string]Value{"x": IntValue{V: int64(i)}},
			Weight:  1,
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CompactBatch(batch)
	}
}

func BenchmarkAggregate_BatchedWithCompaction(b *testing.B) {
	// Same as BenchmarkAggregate_Batched but with duplicates to exercise compaction
	records := make([]Record, b.N)
	for i := range records {
		records[i] = Record{
			Columns: map[string]Value{"x": IntValue{V: int64(i % 100)}},
			Weight:  1,
		}
	}
	cols := []AggColumn{
		{Alias: "total", IsAggregate: true, AggFunc: "COUNT", IsStar: true, GroupByIdx: -1},
	}

	in := make(chan Record, 256)
	out := make(chan Record, 256)

	go func() {
		defer close(in)
		for _, r := range records {
			in <- r
		}
	}()

	batched := BatchChannel(in, DefaultBatchSize)

	op := NewAggregateOp(cols, nil, nil)
	go func() {
		op.ProcessBatches(batched, out)
	}()

	for range out {
	}
}

func makeBenchRecords(n int) []Record {
	records := make([]Record, n)
	for i := range records {
		records[i] = Record{
			Columns: map[string]Value{"x": IntValue{V: int64(i)}},
			Weight:  1,
		}
	}
	return records
}

// --- FilterProjectBatch tests ---

func TestFilterProjectBatch_WithFilter(t *testing.T) {
	// Only records where x > 5 should pass
	p := &Pipeline{
		Columns: []ast.Column{{Expr: &ast.ColumnRef{Name: "x"}, Alias: "x"}},
		Where: &ast.BinaryExpr{
			Left:  &ast.ColumnRef{Name: "x"},
			Op:    ">",
			Right: &ast.NumberLiteral{Value: "5"},
		},
	}

	batch := make(Batch, 10)
	for i := range batch {
		batch[i] = NewRecord(map[string]Value{"x": IntValue{V: int64(i)}})
	}

	result := p.FilterProjectBatch(batch)
	// x=6,7,8,9 should pass
	if len(result) != 4 {
		t.Fatalf("expected 4 records, got %d", len(result))
	}
	for _, rec := range result {
		v := rec.Get("x").(IntValue).V
		if v <= 5 {
			t.Fatalf("record with x=%d should have been filtered", v)
		}
	}
}

func TestFilterProjectBatch_NoFilter(t *testing.T) {
	// No WHERE clause — all records should pass with projection applied
	p := &Pipeline{
		Columns: []ast.Column{{Expr: &ast.ColumnRef{Name: "x"}, Alias: "val"}},
	}

	batch := Batch{
		NewRecord(map[string]Value{"x": IntValue{V: 1}, "y": IntValue{V: 2}}),
		NewRecord(map[string]Value{"x": IntValue{V: 3}, "y": IntValue{V: 4}}),
	}

	result := p.FilterProjectBatch(batch)
	if len(result) != 2 {
		t.Fatalf("expected 2 records, got %d", len(result))
	}
	// Should have "val" column, not "x" or "y"
	for _, rec := range result {
		if _, ok := rec.Columns["val"]; !ok {
			t.Fatal("expected projected column 'val'")
		}
		if _, ok := rec.Columns["y"]; ok {
			t.Fatal("column 'y' should have been projected away")
		}
	}
}

func TestFilterProjectBatch_MatchesProcessBatches(t *testing.T) {
	// FilterProjectBatch should produce identical results to ProcessBatches
	p := &Pipeline{
		Columns: []ast.Column{{Expr: &ast.ColumnRef{Name: "x"}, Alias: "x"}},
		Where: &ast.BinaryExpr{
			Left:  &ast.ColumnRef{Name: "x"},
			Op:    ">",
			Right: &ast.NumberLiteral{Value: "3"},
		},
	}

	records := make(Batch, 10)
	for i := range records {
		records[i] = NewRecord(map[string]Value{"x": IntValue{V: int64(i)}, "y": IntValue{V: 99}})
	}

	// FilterProjectBatch
	fpResult := p.FilterProjectBatch(records)

	// ProcessBatches
	batchIn := make(chan Batch, 1)
	batchIn <- records
	close(batchIn)
	out := make(chan Record, 100)
	p2 := &Pipeline{
		Columns: p.Columns,
		Where:   p.Where,
	}
	p2.ProcessBatches(batchIn, out)

	var pbResult []Record
	for r := range out {
		pbResult = append(pbResult, r)
	}

	if len(fpResult) != len(pbResult) {
		t.Fatalf("FilterProjectBatch produced %d records, ProcessBatches produced %d", len(fpResult), len(pbResult))
	}
	for i := range fpResult {
		v1 := fpResult[i].Get("x").(IntValue).V
		v2 := pbResult[i].Get("x").(IntValue).V
		if v1 != v2 {
			t.Fatalf("record %d: FilterProjectBatch x=%d, ProcessBatches x=%d", i, v1, v2)
		}
	}
}

// --- FusedAggregateProcessor tests ---

func TestFusedAggregateProcessor_IdenticalToNonFused(t *testing.T) {
	// Build an accumulating query: SELECT COUNT(*) AS total WHERE x > 3
	cols := []AggColumn{
		{Alias: "total", IsAggregate: true, AggFunc: "COUNT", IsStar: true, GroupByIdx: -1},
	}
	where := &ast.BinaryExpr{
		Left:  &ast.ColumnRef{Name: "x"},
		Op:    ">",
		Right: &ast.NumberLiteral{Value: "3"},
	}

	records := make([]Record, 20)
	for i := range records {
		records[i] = NewRecord(map[string]Value{"x": IntValue{V: int64(i)}})
	}

	// Non-fused path: filter manually, then aggregate
	op1 := NewAggregateOp(cols, nil, nil)
	in1 := make(chan Record, len(records))
	for _, r := range records {
		pass, _ := Filter(where, r)
		if pass {
			in1 <- r
		}
	}
	close(in1)
	out1 := make(chan Record, 100)
	op1.Process(in1, out1)

	var results1 []Record
	for r := range out1 {
		results1 = append(results1, r)
	}

	// Fused path
	op2 := NewAggregateOp(cols, nil, nil)
	fused := &FusedAggregateProcessor{
		Where:       where,
		AggregateOp: op2,
	}
	batchIn := make(chan Batch, 1)
	batchIn <- Batch(records)
	close(batchIn)
	out2 := make(chan Record, 100)
	fused.ProcessBatches(batchIn, out2)

	var results2 []Record
	for r := range out2 {
		results2 = append(results2, r)
	}

	// Both should produce the same final count
	if len(results1) == 0 || len(results2) == 0 {
		t.Fatal("expected results from both paths")
	}

	// Get last positive-weight record from each
	var last1, last2 Record
	for _, r := range results1 {
		if r.Weight > 0 {
			last1 = r
		}
	}
	for _, r := range results2 {
		if r.Weight > 0 {
			last2 = r
		}
	}

	v1 := last1.Get("total").(IntValue).V
	v2 := last2.Get("total").(IntValue).V
	if v1 != v2 {
		t.Fatalf("non-fused count=%d, fused count=%d", v1, v2)
	}
	// x > 3 means x=4..19 → 16 records
	if v1 != 16 {
		t.Fatalf("expected count=16, got %d", v1)
	}
}

func TestFusedAggregateProcessor_FilterRejectsMost(t *testing.T) {
	// WHERE x = 0 — only 1 out of 100 records matches
	cols := []AggColumn{
		{Alias: "total", IsAggregate: true, AggFunc: "COUNT", IsStar: true, GroupByIdx: -1},
	}
	where := &ast.BinaryExpr{
		Left:  &ast.ColumnRef{Name: "x"},
		Op:    "=",
		Right: &ast.NumberLiteral{Value: "0"},
	}

	records := make([]Record, 100)
	for i := range records {
		records[i] = NewRecord(map[string]Value{"x": IntValue{V: int64(i)}})
	}

	op := NewAggregateOp(cols, nil, nil)
	fused := &FusedAggregateProcessor{
		Where:       where,
		AggregateOp: op,
	}
	batchIn := make(chan Batch, 1)
	batchIn <- Batch(records)
	close(batchIn)
	out := make(chan Record, 100)
	fused.ProcessBatches(batchIn, out)

	var last Record
	for r := range out {
		if r.Weight > 0 {
			last = r
		}
	}
	v := last.Get("total").(IntValue).V
	if v != 1 {
		t.Fatalf("expected count=1 (only x=0 passes), got %d", v)
	}
}

func TestFusedAggregateProcessor_NoFilter(t *testing.T) {
	// No WHERE clause — all records should be aggregated
	cols := []AggColumn{
		{Alias: "region", Expr: &ast.ColumnRef{Name: "region"}, GroupByIdx: 0},
		{Alias: "total", IsAggregate: true, AggFunc: "COUNT", IsStar: true, GroupByIdx: -1},
	}
	groupBy := []ast.Expr{&ast.ColumnRef{Name: "region"}}

	records := []Record{
		NewRecord(map[string]Value{"region": TextValue{V: "us"}}),
		NewRecord(map[string]Value{"region": TextValue{V: "eu"}}),
		NewRecord(map[string]Value{"region": TextValue{V: "us"}}),
		NewRecord(map[string]Value{"region": TextValue{V: "us"}}),
	}

	op := NewAggregateOp(cols, groupBy, nil)
	fused := &FusedAggregateProcessor{
		Where:       nil,
		AggregateOp: op,
	}
	batchIn := make(chan Batch, 1)
	batchIn <- Batch(records)
	close(batchIn)
	out := make(chan Record, 100)
	fused.ProcessBatches(batchIn, out)

	// Collect final positive-weight records per group
	groups := make(map[string]int64)
	for r := range out {
		if r.Weight > 0 {
			region := r.Get("region").(TextValue).V
			groups[region] = r.Get("total").(IntValue).V
		}
	}

	if groups["us"] != 3 {
		t.Fatalf("expected us=3, got us=%d", groups["us"])
	}
	if groups["eu"] != 1 {
		t.Fatalf("expected eu=1, got eu=%d", groups["eu"])
	}
}

func TestFusedAggregateProcessor_InputCount(t *testing.T) {
	// Verify InputCount tracks all records (not just filtered ones)
	cols := []AggColumn{
		{Alias: "total", IsAggregate: true, AggFunc: "COUNT", IsStar: true, GroupByIdx: -1},
	}
	where := &ast.BinaryExpr{
		Left:  &ast.ColumnRef{Name: "x"},
		Op:    ">",
		Right: &ast.NumberLiteral{Value: "50"},
	}

	records := make([]Record, 100)
	for i := range records {
		records[i] = NewRecord(map[string]Value{"x": IntValue{V: int64(i)}})
	}

	var count atomic.Int64
	op := NewAggregateOp(cols, nil, nil)
	fused := &FusedAggregateProcessor{
		Where:       where,
		AggregateOp: op,
		InputCount:  &count,
	}
	batchIn := make(chan Batch, 1)
	batchIn <- Batch(records)
	close(batchIn)
	out := make(chan Record, 100)
	fused.ProcessBatches(batchIn, out)
	for range out {
	}

	// InputCount should be 100 (all records), not 49 (filtered)
	if count.Load() != 100 {
		t.Fatalf("expected InputCount=100, got %d", count.Load())
	}
}

// --- Fused vs non-fused benchmarks ---

func BenchmarkAccumulating_NonFused(b *testing.B) {
	// Simulates the old path: filter → chan Record → BatchChannel → Aggregate
	records := makeBenchRecords(b.N)
	cols := []AggColumn{
		{Alias: "total", IsAggregate: true, AggFunc: "COUNT", IsStar: true, GroupByIdx: -1},
	}

	// Filtered channel
	filteredCh := make(chan Record, 256)
	go func() {
		defer close(filteredCh)
		for _, r := range records {
			filteredCh <- r
		}
	}()

	// Batch the filtered records
	batchedCh := BatchChannel(filteredCh, DefaultBatchSize)

	// Aggregate
	op := NewAggregateOp(cols, nil, nil)
	out := make(chan Record, 256)
	go func() {
		op.ProcessBatches(batchedCh, out)
	}()

	for range out {
	}
}

func BenchmarkAccumulating_Fused(b *testing.B) {
	// Simulates the new path: BatchChannel → FusedAggregateProcessor
	records := makeBenchRecords(b.N)
	cols := []AggColumn{
		{Alias: "total", IsAggregate: true, AggFunc: "COUNT", IsStar: true, GroupByIdx: -1},
	}

	// Raw channel (unfiltered)
	rawCh := make(chan Record, 256)
	go func() {
		defer close(rawCh)
		for _, r := range records {
			rawCh <- r
		}
	}()

	// Batch raw records
	batchedCh := BatchChannel(rawCh, DefaultBatchSize)

	// Fused filter + aggregate
	op := NewAggregateOp(cols, nil, nil)
	fused := &FusedAggregateProcessor{
		Where:       nil,
		AggregateOp: op,
	}
	out := make(chan Record, 256)
	go func() {
		fused.ProcessBatches(batchedCh, out)
	}()

	for range out {
	}
}

func BenchmarkAccumulating_Fused_WithFilter(b *testing.B) {
	// Fused path with a WHERE filter that passes ~50% of records
	records := makeBenchRecords(b.N)
	cols := []AggColumn{
		{Alias: "total", IsAggregate: true, AggFunc: "COUNT", IsStar: true, GroupByIdx: -1},
	}
	where := &ast.BinaryExpr{
		Left:  &ast.ColumnRef{Name: "x"},
		Op:    ">",
		Right: &ast.NumberLiteral{Value: fmt.Sprintf("%d", b.N/2)},
	}

	rawCh := make(chan Record, 256)
	go func() {
		defer close(rawCh)
		for _, r := range records {
			rawCh <- r
		}
	}()

	batchedCh := BatchChannel(rawCh, DefaultBatchSize)

	op := NewAggregateOp(cols, nil, nil)
	fused := &FusedAggregateProcessor{
		Where:       where,
		AggregateOp: op,
	}
	out := make(chan Record, 256)
	go func() {
		fused.ProcessBatches(batchedCh, out)
	}()

	for range out {
	}
}

func BenchmarkAccumulating_NonFused_WithFilter(b *testing.B) {
	// Non-fused path with the same ~50% filter for comparison
	records := makeBenchRecords(b.N)
	cols := []AggColumn{
		{Alias: "total", IsAggregate: true, AggFunc: "COUNT", IsStar: true, GroupByIdx: -1},
	}
	where := &ast.BinaryExpr{
		Left:  &ast.ColumnRef{Name: "x"},
		Op:    ">",
		Right: &ast.NumberLiteral{Value: fmt.Sprintf("%d", b.N/2)},
	}

	filteredCh := make(chan Record, 256)
	go func() {
		defer close(filteredCh)
		for _, r := range records {
			pass, _ := Filter(where, r)
			if pass {
				filteredCh <- r
			}
		}
	}()

	batchedCh := BatchChannel(filteredCh, DefaultBatchSize)

	op := NewAggregateOp(cols, nil, nil)
	out := make(chan Record, 256)
	go func() {
		op.ProcessBatches(batchedCh, out)
	}()

	for range out {
	}
}
