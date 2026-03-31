package engine

import (
	"testing"
	"time"
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
