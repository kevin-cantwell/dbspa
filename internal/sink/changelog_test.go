package sink

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/kevin-cantwell/dbspa/internal/engine"
)

// weightedRecord is the Feldera weighted format: {"weight": N, "data": {...}}
type weightedRecord struct {
	Weight float64        `json:"weight"`
	Data   map[string]any `json:"data"`
}

func parseWeightedNDJSON(t *testing.T, buf *bytes.Buffer) []weightedRecord {
	t.Helper()
	var results []weightedRecord
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var r weightedRecord
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("JSON parse error: %v\nline: %s", err, line)
		}
		results = append(results, r)
	}
	return results
}

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

func TestChangelogSink_InsertionHasPositiveWeight(t *testing.T) {
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
	results := parseWeightedNDJSON(t, &buf)
	if len(results) != 1 {
		t.Fatalf("expected 1 line, got %d: %q", len(results), buf.String())
	}
	if results[0].Weight != 1 {
		t.Errorf("expected weight=1, got %v", results[0].Weight)
	}
	if results[0].Data["name"] != "alice" {
		t.Errorf("expected name=alice, got %v", results[0].Data["name"])
	}
}

func TestChangelogSink_RetractionHasNegativeWeight(t *testing.T) {
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
	results := parseWeightedNDJSON(t, &buf)
	if results[0].Weight != -1 {
		t.Errorf("expected weight=-1, got %v", results[0].Weight)
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
	// Should start with weight and data envelope
	if !strings.HasPrefix(line, `{"weight":1,"data":{`) {
		t.Errorf("expected line to start with weight+data envelope, got: %s", line)
	}

	// Verify the column order within the data object
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
	// Keys in data should be sorted: a, m, z
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
	results := parseWeightedNDJSON(t, &buf)
	if results[0].Data["x"] != nil {
		t.Errorf("expected null, got %v", results[0].Data["x"])
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
	results := parseWeightedNDJSON(t, &buf)
	if len(results) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(results))
	}
	if results[0].Weight != -1 {
		t.Errorf("first line should be retraction")
	}
	if results[1].Weight != 1 {
		t.Errorf("second line should be insertion")
	}
}

func TestJSONSink_NoWeightField(t *testing.T) {
	// Non-accumulating queries use JSONSink which omits weight envelope
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
	if _, ok := results[0]["weight"]; ok {
		t.Error("JSONSink should not include 'weight' field")
	}
	if _, ok := results[0]["data"]; ok {
		t.Error("JSONSink should not include 'data' envelope")
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
	results := parseWeightedNDJSON(t, &buf)
	if results[0].Data["active"] != true {
		t.Errorf("expected true, got %v", results[0].Data["active"])
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
	results := parseWeightedNDJSON(t, &buf)
	if results[0].Data["val"] != 3.14 {
		t.Errorf("expected 3.14, got %v", results[0].Data["val"])
	}
}

func TestChangelogSink_OrderBy_SortedFinalSnapshot(t *testing.T) {
	var buf bytes.Buffer
	s := &ChangelogSink{
		Writer:      &buf,
		ColumnOrder: []string{"g", "cnt"},
		OrderBy:     []OrderBySpec{{Column: "g", Desc: false}},
	}

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
	results := parseWeightedNDJSON(t, &buf)

	// First 3 are streaming diffs (unsorted), next 3 are sorted final snapshot
	if len(results) != 6 {
		t.Fatalf("expected 6 lines (3 diffs + 3 snapshot), got %d: %q", len(results), buf.String())
	}

	// Verify sorted final snapshot (last 3 lines)
	snapshot := results[3:]
	if snapshot[0].Data["g"] != "a" || snapshot[1].Data["g"] != "b" || snapshot[2].Data["g"] != "c" {
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
	results := parseWeightedNDJSON(t, &buf)
	snapshot := results[3:]
	if snapshot[0].Data["cnt"] != float64(3) || snapshot[1].Data["cnt"] != float64(2) || snapshot[2].Data["cnt"] != float64(1) {
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

	s.Write(engine.Record{Columns: map[string]engine.Value{"g": engine.TextValue{V: "a"}, "cnt": engine.IntValue{V: 1}}, Weight: 1})
	s.Write(engine.Record{Columns: map[string]engine.Value{"g": engine.TextValue{V: "b"}, "cnt": engine.IntValue{V: 2}}, Weight: 1})
	s.Write(engine.Record{Columns: map[string]engine.Value{"g": engine.TextValue{V: "b"}, "cnt": engine.IntValue{V: 2}}, Weight: -1})
	s.Write(engine.Record{Columns: map[string]engine.Value{"g": engine.TextValue{V: "c"}, "cnt": engine.IntValue{V: 3}}, Weight: 1})

	s.Close()
	results := parseWeightedNDJSON(t, &buf)

	// 4 streaming diffs + 2 final snapshot (b retracted)
	if len(results) != 6 {
		t.Fatalf("expected 6 lines, got %d: %q", len(results), buf.String())
	}
	snapshot := results[4:]
	if len(snapshot) != 2 {
		t.Fatalf("expected 2 snapshot rows, got %d", len(snapshot))
	}
	if snapshot[0].Data["g"] != "a" || snapshot[1].Data["g"] != "c" {
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

	results := parseWeightedNDJSON(t, &buf)
	if len(results) != 2 {
		t.Fatalf("expected 2 lines (no snapshot), got %d", len(results))
	}
}
