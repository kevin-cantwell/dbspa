package sink

import (
	"bufio"
	"encoding/json"
	"io"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/kevin-cantwell/folddb/internal/engine"
)

// ChangelogSink writes accumulating query results as changelog NDJSON.
// Each line uses Feldera's weighted format: {"weight": N, "data": {...}}.
type ChangelogSink struct {
	Writer      io.Writer
	ColumnOrder []string
	OrderBy     []OrderBySpec // optional ORDER BY for sorted final snapshot at Close()
	bw          *bufio.Writer
	rows        map[string]map[string]engine.Value // accumulated state for final snapshot
}

func (s *ChangelogSink) writer() *bufio.Writer {
	if s.bw == nil {
		s.bw = bufio.NewWriterSize(s.Writer, 256*1024)
	}
	return s.bw
}

// Write serializes the record as a changelog JSON line with a Z-set weight.
// When OrderBy is set, it also accumulates the current state for a sorted final snapshot.
func (s *ChangelogSink) Write(rec engine.Record) error {
	// Always emit the streaming diff
	if err := s.writeRecord(rec); err != nil {
		return err
	}

	// Accumulate state for sorted final snapshot
	if len(s.OrderBy) > 0 {
		if s.rows == nil {
			s.rows = make(map[string]map[string]engine.Value)
		}
		key := s.rowKey(rec)
		if rec.Weight < 0 {
			delete(s.rows, key)
		} else {
			row := make(map[string]engine.Value, len(rec.Columns))
			for k, v := range rec.Columns {
				row[k] = v
			}
			s.rows[key] = row
		}
	}

	return nil
}

func (s *ChangelogSink) writeRecord(rec engine.Record) error {
	w := s.writer()

	w.WriteString(`{"weight":`)
	w.WriteString(strconv.Itoa(rec.Weight))
	w.WriteString(`,"data":{`)

	first := true
	if len(s.ColumnOrder) > 0 {
		for _, col := range s.ColumnOrder {
			if v, ok := rec.Columns[col]; ok {
				if !first {
					w.WriteByte(',')
				}
				first = false
				w.WriteString(`"`)
				w.WriteString(col)
				w.WriteString(`":`)
				writeValue(w, v)
			}
		}
	} else {
		keys := make([]string, 0, len(rec.Columns))
		for k := range rec.Columns {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if !first {
				w.WriteByte(',')
			}
			first = false
			w.WriteString(`"`)
			w.WriteString(k)
			w.WriteString(`":`)
			writeValue(w, rec.Columns[k])
		}
	}

	w.WriteString("}}\n")
	return nil
}

// Close flushes the buffered writer. If OrderBy is set and there are accumulated rows,
// it emits a final sorted snapshot after the normal diffs.
func (s *ChangelogSink) Close() error {
	if len(s.OrderBy) > 0 && len(s.rows) > 0 {
		// Collect and sort keys
		keys := make([]string, 0, len(s.rows))
		for k := range s.rows {
			keys = append(keys, k)
		}
		slices.SortFunc(keys, func(a, b string) int {
			rowA := s.rows[a]
			rowB := s.rows[b]
			for _, spec := range s.OrderBy {
				va := rowA[spec.Column]
				vb := rowB[spec.Column]
				c := CompareValues(va, vb)
				if spec.Desc {
					c = -c
				}
				if c != 0 {
					return c
				}
			}
			return 0
		})

		// Emit sorted final snapshot as insert records
		for _, key := range keys {
			row := s.rows[key]
			rec := engine.Record{
				Columns: row,
				Weight:    1,
			}
			if err := s.writeRecord(rec); err != nil {
				return err
			}
		}
	}
	if s.bw != nil {
		return s.bw.Flush()
	}
	return nil
}

func (s *ChangelogSink) rowKey(rec engine.Record) string {
	var parts []string
	for _, col := range s.ColumnOrder {
		if v, ok := rec.Columns[col]; ok {
			parts = append(parts, v.String())
		} else {
			parts = append(parts, "NULL")
		}
	}
	return strings.Join(parts, "\x00")
}

func writeValue(w *bufio.Writer, v engine.Value) {
	if v == nil || v.IsNull() {
		w.WriteString("null")
		return
	}
	switch val := v.(type) {
	case engine.IntValue:
		w.WriteString(strconv.FormatInt(val.V, 10))
	case engine.FloatValue:
		if math.IsInf(val.V, 0) || math.IsNaN(val.V) {
			w.WriteString("null")
		} else {
			w.WriteString(strconv.FormatFloat(val.V, 'f', -1, 64))
		}
	case engine.BoolValue:
		if val.V {
			w.WriteString("true")
		} else {
			w.WriteString("false")
		}
	case engine.TextValue:
		b, _ := json.Marshal(val.V)
		w.Write(b)
	default:
		b, _ := json.Marshal(v.ToJSON())
		w.Write(b)
	}
}
