package sink

import (
	"bufio"
	"encoding/json"
	"io"
	"sort"
	"strconv"

	"github.com/kevin-cantwell/folddb/internal/engine"
)

// JSONSink writes records as NDJSON to an io.Writer.
type JSONSink struct {
	Writer io.Writer
	bw     *bufio.Writer
}

func (s *JSONSink) writer() *bufio.Writer {
	if s.bw == nil {
		s.bw = bufio.NewWriterSize(s.Writer, 256*1024)
	}
	return s.bw
}

// Write serializes the record as a JSON object and writes it as one line.
func (s *JSONSink) Write(rec engine.Record) error {
	w := s.writer()

	// Collect and sort keys for deterministic output
	keys := make([]string, 0, len(rec.Columns))
	for k := range rec.Columns {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Write JSON manually to avoid per-record allocations
	w.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			w.WriteByte(',')
		}
		// Write key
		w.WriteByte('"')
		w.WriteString(k) // keys are safe identifiers, no escaping needed
		w.WriteString(`":`)

		// Write value
		v := rec.Columns[k]
		writeJSONValue(w, v)
	}
	w.WriteByte('}')
	w.WriteByte('\n')
	return nil
}

// Close flushes the buffered writer.
func (s *JSONSink) Close() error {
	if s.bw != nil {
		return s.bw.Flush()
	}
	return nil
}

func writeJSONValue(w *bufio.Writer, v engine.Value) {
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
		b, _ := json.Marshal(val.V) // handles escaping
		w.Write(b)
	default:
		b, _ := json.Marshal(v.ToJSON())
		w.Write(b)
	}
}
