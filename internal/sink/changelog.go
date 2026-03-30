package sink

import (
	"bufio"
	"encoding/json"
	"io"
	"sort"
	"strconv"

	"github.com/kevin-cantwell/folddb/internal/engine"
)

// ChangelogSink writes accumulating query results as changelog NDJSON.
// Each line includes an "op" field: "+" for insert, "-" for retract.
type ChangelogSink struct {
	Writer      io.Writer
	ColumnOrder []string
	bw          *bufio.Writer
}

func (s *ChangelogSink) writer() *bufio.Writer {
	if s.bw == nil {
		s.bw = bufio.NewWriterSize(s.Writer, 256*1024)
	}
	return s.bw
}

// Write serializes the record as a changelog JSON line with an "op" field.
func (s *ChangelogSink) Write(rec engine.Record) error {
	w := s.writer()
	op := "+"
	if rec.Diff < 0 {
		op = "-"
	}

	w.WriteString(`{"op":"`)
	w.WriteString(op)
	w.WriteByte('"')

	if len(s.ColumnOrder) > 0 {
		for _, col := range s.ColumnOrder {
			if v, ok := rec.Columns[col]; ok {
				w.WriteString(`,"`)
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
			w.WriteString(`,"`)
			w.WriteString(k)
			w.WriteString(`":`)
			writeValue(w, rec.Columns[k])
		}
	}

	w.WriteString("}\n")
	return nil
}

// Close flushes the buffered writer.
func (s *ChangelogSink) Close() error {
	if s.bw != nil {
		return s.bw.Flush()
	}
	return nil
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
		w.WriteString(strconv.FormatFloat(val.V, 'f', -1, 64))
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
