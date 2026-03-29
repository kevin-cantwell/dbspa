package sink

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"github.com/kevin-cantwell/folddb/internal/engine"
)

// JSONSink writes records as NDJSON to an io.Writer.
type JSONSink struct {
	Writer io.Writer
}

// Write serializes the record as a JSON object and writes it as one line.
func (s *JSONSink) Write(rec engine.Record) error {
	// Build ordered output map
	m := make(map[string]any, len(rec.Columns))
	for k, v := range rec.Columns {
		m[k] = v.ToJSON()
	}

	data, err := marshalOrdered(rec, m)
	if err != nil {
		return fmt.Errorf("JSON encode error: %w", err)
	}

	_, err = fmt.Fprintf(s.Writer, "%s\n", data)
	return err
}

// Close is a no-op for the JSON sink.
func (s *JSONSink) Close() error {
	return nil
}

// marshalOrdered produces JSON with keys in sorted order for deterministic output.
func marshalOrdered(_ engine.Record, m map[string]any) ([]byte, error) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	ordered := make([]orderedKV, len(keys))
	for i, k := range keys {
		ordered[i] = orderedKV{Key: k, Value: m[k]}
	}

	return json.Marshal(orderedMap(ordered))
}

type orderedKV struct {
	Key   string
	Value any
}

type orderedMap []orderedKV

func (o orderedMap) MarshalJSON() ([]byte, error) {
	buf := []byte{'{'}
	for i, kv := range o {
		if i > 0 {
			buf = append(buf, ',')
		}
		key, _ := json.Marshal(kv.Key)
		val, err := json.Marshal(kv.Value)
		if err != nil {
			return nil, err
		}
		buf = append(buf, key...)
		buf = append(buf, ':')
		buf = append(buf, val...)
	}
	buf = append(buf, '}')
	return buf, nil
}
