package sink

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/kevin-cantwell/folddb/internal/engine"
)

func parseNDJSON(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var results []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("JSON parse error: %v\nline: %s", err, line)
		}
		results = append(results, m)
	}
	return results
}

func TestChangelogSink_InsertionHasPlusOp(t *testing.T) {
	var buf bytes.Buffer
	s := &ChangelogSink{Writer: &buf}

	rec := engine.Record{
		Columns: map[string]engine.Value{
			"name": engine.TextValue{V: "alice"},
			"cnt":  engine.IntValue{V: 5},
		},
		Weight: 1,
	}

	if err := s.Write(rec); err != nil {
		t.Fatalf("Write error: %v", err)
	}
	s.Close()
	results := parseNDJSON(t, &buf)
	if len(results) != 1 {
		t.Fatalf("expected 1 line, got %d: %q", len(results), buf.String())
	}
	if results[0]["op"] != "+" {
		t.Errorf("expected op='+', got %v", results[0]["op"])
	}
}

func TestChangelogSink_RetractionHasMinusOp(t *testing.T) {
	var buf bytes.Buffer
	s := &ChangelogSink{Writer: &buf}

	rec := engine.Record{
		Columns: map[string]engine.Value{
			"name": engine.TextValue{V: "alice"},
			"cnt":  engine.IntValue{V: 5},
		},
		Weight: -1,
	}

	if err := s.Write(rec); err != nil {
		t.Fatalf("Write error: %v", err)
	}

	s.Close()
	results := parseNDJSON(t, &buf)
	if results[0]["op"] != "-" {
		t.Errorf("expected op='-', got %v", results[0]["op"])
	}
}

func TestChangelogSink_ColumnOrderPreserved(t *testing.T) {
	var buf bytes.Buffer
	s := &ChangelogSink{
		Writer:      &buf,
		ColumnOrder: []string{"city", "total", "avg"},
	}

	rec := engine.Record{
		Columns: map[string]engine.Value{
			"avg":   engine.FloatValue{V: 3.14},
			"city":  engine.TextValue{V: "nyc"},
			"total": engine.IntValue{V: 100},
		},
		Weight: 1,
	}

	if err := s.Write(rec); err != nil {
		t.Fatalf("Write error: %v", err)
	}
	s.Close()

	line := strings.TrimSpace(buf.String())
	// "op" should come first, then columns in specified order
	if !strings.HasPrefix(line, `{"op":"+"`) {
		t.Errorf("expected line to start with op, got: %s", line)
	}

	// Verify the column order by checking byte positions
	cityIdx := strings.Index(line, `"city"`)
	totalIdx := strings.Index(line, `"total"`)
	avgIdx := strings.Index(line, `"avg"`)
	if cityIdx >= totalIdx || totalIdx >= avgIdx {
		t.Errorf("column order not preserved: city@%d, total@%d, avg@%d in: %s",
			cityIdx, totalIdx, avgIdx, line)
	}
}

func TestChangelogSink_NoColumnOrderSortsKeys(t *testing.T) {
	var buf bytes.Buffer
	s := &ChangelogSink{Writer: &buf} // no ColumnOrder

	rec := engine.Record{
		Columns: map[string]engine.Value{
			"z": engine.IntValue{V: 1},
			"a": engine.IntValue{V: 2},
			"m": engine.IntValue{V: 3},
		},
		Weight: 1,
	}

	if err := s.Write(rec); err != nil {
		t.Fatalf("Write error: %v", err)
	}
	s.Close()

	line := strings.TrimSpace(buf.String())
	// Keys should be sorted: op, a, m, z
	aIdx := strings.Index(line, `"a"`)
	mIdx := strings.Index(line, `"m"`)
	zIdx := strings.Index(line, `"z"`)
	if aIdx >= mIdx || mIdx >= zIdx {
		t.Errorf("keys not sorted: a@%d, m@%d, z@%d in: %s", aIdx, mIdx, zIdx, line)
	}
}

func TestChangelogSink_NullValues(t *testing.T) {
	var buf bytes.Buffer
	s := &ChangelogSink{Writer: &buf, ColumnOrder: []string{"x"}}

	rec := engine.Record{
		Columns: map[string]engine.Value{
			"x": engine.NullValue{},
		},
		Weight: 1,
	}

	if err := s.Write(rec); err != nil {
		t.Fatalf("Write error: %v", err)
	}

	s.Close()
	results := parseNDJSON(t, &buf)
	if results[0]["x"] != nil {
		t.Errorf("expected null, got %v", results[0]["x"])
	}
}

func TestChangelogSink_Close(t *testing.T) {
	s := &ChangelogSink{Writer: &bytes.Buffer{}}
	if err := s.Close(); err != nil {
		t.Errorf("Close should not error: %v", err)
	}
}

func TestChangelogSink_MultipleWrites(t *testing.T) {
	var buf bytes.Buffer
	s := &ChangelogSink{Writer: &buf, ColumnOrder: []string{"g", "cnt"}}

	// Simulate retraction + insertion pair
	retract := engine.Record{
		Columns: map[string]engine.Value{
			"g":   engine.TextValue{V: "a"},
			"cnt": engine.IntValue{V: 1},
		},
		Weight: -1,
	}
	insert := engine.Record{
		Columns: map[string]engine.Value{
			"g":   engine.TextValue{V: "a"},
			"cnt": engine.IntValue{V: 2},
		},
		Weight: 1,
	}

	if err := s.Write(retract); err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if err := s.Write(insert); err != nil {
		t.Fatalf("Write error: %v", err)
	}

	s.Close()
	results := parseNDJSON(t, &buf)
	if len(results) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(results))
	}
	if results[0]["op"] != "-" {
		t.Errorf("first line should be retraction")
	}
	if results[1]["op"] != "+" {
		t.Errorf("second line should be insertion")
	}
}

func TestJSONSink_NoOpField(t *testing.T) {
	// Non-accumulating queries use JSONSink which omits "op"
	var buf bytes.Buffer
	s := &JSONSink{Writer: &buf}

	rec := engine.Record{
		Columns: map[string]engine.Value{
			"name": engine.TextValue{V: "alice"},
		},
		Weight: 1,
	}

	if err := s.Write(rec); err != nil {
		t.Fatalf("Write error: %v", err)
	}

	s.Close()
	results := parseNDJSON(t, &buf)
	if _, ok := results[0]["op"]; ok {
		t.Error("JSONSink should not include 'op' field")
	}
}

func TestChangelogSink_BoolValue(t *testing.T) {
	var buf bytes.Buffer
	s := &ChangelogSink{Writer: &buf, ColumnOrder: []string{"active"}}

	rec := engine.Record{
		Columns: map[string]engine.Value{
			"active": engine.BoolValue{V: true},
		},
		Weight: 1,
	}

	if err := s.Write(rec); err != nil {
		t.Fatalf("Write error: %v", err)
	}

	s.Close()
	results := parseNDJSON(t, &buf)
	if results[0]["active"] != true {
		t.Errorf("expected true, got %v", results[0]["active"])
	}
}

func TestChangelogSink_FloatValue(t *testing.T) {
	var buf bytes.Buffer
	s := &ChangelogSink{Writer: &buf, ColumnOrder: []string{"val"}}

	rec := engine.Record{
		Columns: map[string]engine.Value{
			"val": engine.FloatValue{V: 3.14},
		},
		Weight: 1,
	}

	if err := s.Write(rec); err != nil {
		t.Fatalf("Write error: %v", err)
	}

	s.Close()
	results := parseNDJSON(t, &buf)
	if results[0]["val"] != 3.14 {
		t.Errorf("expected 3.14, got %v", results[0]["val"])
	}
}

func TestChangelogSink_OrderBy_SortedFinalSnapshot(t *testing.T) {
	var buf bytes.Buffer
	s := &ChangelogSink{
		Writer:      &buf,
		ColumnOrder: []string{"g", "cnt"},
		OrderBy:     []OrderBySpec{{Column: "g", Desc: false}},
	}

	// Write records in unsorted order
	for _, rec := range []engine.Record{
		{Columns: map[string]engine.Value{"g": engine.TextValue{V: "c"}, "cnt": engine.IntValue{V: 3}}, Weight: 1},
		{Columns: map[string]engine.Value{"g": engine.TextValue{V: "a"}, "cnt": engine.IntValue{V: 1}}, Weight: 1},
		{Columns: map[string]engine.Value{"g": engine.TextValue{V: "b"}, "cnt": engine.IntValue{V: 2}}, Weight: 1},
	} {
		if err := s.Write(rec); err != nil {
			t.Fatalf("Write error: %v", err)
		}
	}

	s.Close()
	results := parseNDJSON(t, &buf)

	// First 3 are streaming diffs (unsorted), next 3 are sorted final snapshot
	if len(results) != 6 {
		t.Fatalf("expected 6 lines (3 diffs + 3 snapshot), got %d: %q", len(results), buf.String())
	}

	// Verify sorted final snapshot (last 3 lines)
	snapshot := results[3:]
	if snapshot[0]["g"] != "a" || snapshot[1]["g"] != "b" || snapshot[2]["g"] != "c" {
		t.Errorf("final snapshot not sorted by g ASC: %v", snapshot)
	}
}

func TestChangelogSink_OrderBy_DescSort(t *testing.T) {
	var buf bytes.Buffer
	s := &ChangelogSink{
		Writer:      &buf,
		ColumnOrder: []string{"g", "cnt"},
		OrderBy:     []OrderBySpec{{Column: "cnt", Desc: true}},
	}

	for _, rec := range []engine.Record{
		{Columns: map[string]engine.Value{"g": engine.TextValue{V: "a"}, "cnt": engine.IntValue{V: 1}}, Weight: 1},
		{Columns: map[string]engine.Value{"g": engine.TextValue{V: "b"}, "cnt": engine.IntValue{V: 3}}, Weight: 1},
		{Columns: map[string]engine.Value{"g": engine.TextValue{V: "c"}, "cnt": engine.IntValue{V: 2}}, Weight: 1},
	} {
		if err := s.Write(rec); err != nil {
			t.Fatalf("Write error: %v", err)
		}
	}

	s.Close()
	results := parseNDJSON(t, &buf)

	// Last 3 are sorted snapshot
	snapshot := results[3:]
	// cnt DESC: 3, 2, 1
	if snapshot[0]["cnt"] != float64(3) || snapshot[1]["cnt"] != float64(2) || snapshot[2]["cnt"] != float64(1) {
		t.Errorf("final snapshot not sorted by cnt DESC: %v", snapshot)
	}
}

func TestChangelogSink_OrderBy_RetractedRowsExcluded(t *testing.T) {
	var buf bytes.Buffer
	s := &ChangelogSink{
		Writer:      &buf,
		ColumnOrder: []string{"g", "cnt"},
		OrderBy:     []OrderBySpec{{Column: "g"}},
	}

	// Insert then retract "b"
	s.Write(engine.Record{Columns: map[string]engine.Value{"g": engine.TextValue{V: "a"}, "cnt": engine.IntValue{V: 1}}, Weight: 1})
	s.Write(engine.Record{Columns: map[string]engine.Value{"g": engine.TextValue{V: "b"}, "cnt": engine.IntValue{V: 2}}, Weight: 1})
	s.Write(engine.Record{Columns: map[string]engine.Value{"g": engine.TextValue{V: "b"}, "cnt": engine.IntValue{V: 2}}, Weight: -1})
	s.Write(engine.Record{Columns: map[string]engine.Value{"g": engine.TextValue{V: "c"}, "cnt": engine.IntValue{V: 3}}, Weight: 1})

	s.Close()
	results := parseNDJSON(t, &buf)

	// 4 streaming diffs + 2 final snapshot (b retracted)
	if len(results) != 6 {
		t.Fatalf("expected 6 lines, got %d: %q", len(results), buf.String())
	}
	snapshot := results[4:]
	if len(snapshot) != 2 {
		t.Fatalf("expected 2 snapshot rows, got %d", len(snapshot))
	}
	if snapshot[0]["g"] != "a" || snapshot[1]["g"] != "c" {
		t.Errorf("snapshot should have a, c (not b): %v", snapshot)
	}
}

func TestChangelogSink_NoOrderBy_NoSnapshot(t *testing.T) {
	var buf bytes.Buffer
	s := &ChangelogSink{
		Writer:      &buf,
		ColumnOrder: []string{"g"},
	}

	s.Write(engine.Record{Columns: map[string]engine.Value{"g": engine.TextValue{V: "a"}}, Weight: 1})
	s.Write(engine.Record{Columns: map[string]engine.Value{"g": engine.TextValue{V: "b"}}, Weight: 1})
	s.Close()

	results := parseNDJSON(t, &buf)
	// No OrderBy means no final snapshot, just 2 diffs
	if len(results) != 2 {
		t.Fatalf("expected 2 lines (no snapshot), got %d", len(results))
	}
}
