package format

import (
	"fmt"
	"io"
	"time"

	"github.com/kevin-cantwell/dbspa/internal/engine"
	"github.com/linkedin/goavro/v2"
)

// AvroOCFDecoder decodes Avro Object Container File streams into Records.
// It implements StreamDecoder since Avro OCF has its own block-based framing.
type AvroOCFDecoder struct{}

// Decode is not supported for Avro OCF — use DecodeStream instead.
func (d *AvroOCFDecoder) Decode(data []byte) (engine.Record, error) {
	return engine.Record{}, fmt.Errorf("avro OCF decoder requires stream mode; use DecodeStream")
}

// DecodeStream reads an Avro OCF stream and sends decoded records to ch.
func (d *AvroOCFDecoder) DecodeStream(r io.Reader, ch chan<- engine.Record) error {
	defer close(ch)

	ocfr, err := goavro.NewOCFReader(r)
	if err != nil {
		return fmt.Errorf("avro OCF reader error: %w", err)
	}

	for ocfr.Scan() {
		datum, err := ocfr.Read()
		if err != nil {
			return fmt.Errorf("avro read error: %w", err)
		}

		rec, err := avroToRecord(datum)
		if err != nil {
			return fmt.Errorf("avro convert error: %w", err)
		}

		ch <- rec
	}

	return ocfr.Err()
}

// avroToRecord converts a native Avro datum (typically map[string]any) to an engine.Record.
func avroToRecord(datum any) (engine.Record, error) {
	m, ok := datum.(map[string]any)
	if !ok {
		return engine.Record{}, fmt.Errorf("expected map[string]any, got %T", datum)
	}

	cols := make(map[string]engine.Value, len(m))
	for k, v := range m {
		cols[k] = avroInferValue(v)
	}

	return engine.Record{
		Columns:   cols,
		Timestamp: time.Now(),
		Weight:      1,
	}, nil
}

// avroInferValue converts an Avro native Go value to an engine.Value.
// goavro returns: int32, int64, float32, float64, string, bool, nil,
// map[string]any (for records/maps), []any (for arrays),
// and map[string]any with union keys like {"string": "value"}.
func avroInferValue(v any) engine.Value {
	if v == nil {
		return engine.NullValue{}
	}
	switch val := v.(type) {
	case int32:
		return engine.IntValue{V: int64(val)}
	case int64:
		return engine.IntValue{V: val}
	case float32:
		return engine.FloatValue{V: float64(val)}
	case float64:
		return engine.FloatValue{V: val}
	case string:
		return engine.TextValue{V: val}
	case bool:
		return engine.BoolValue{V: val}
	case map[string]any:
		// Avro unions are represented as {"type": value} maps.
		// If the map has exactly one key, unwrap the union.
		if len(val) == 1 {
			for _, inner := range val {
				return avroInferValue(inner)
			}
		}
		return engine.JsonValue{V: val}
	case []any:
		return engine.JsonValue{V: val}
	default:
		return engine.TextValue{V: fmt.Sprintf("%v", val)}
	}
}
