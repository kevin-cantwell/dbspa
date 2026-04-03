package format

import (
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/kevin-cantwell/dbspa/internal/engine"
	"github.com/linkedin/goavro/v2"
)

// DebeziumAvroDecoder decodes Avro OCF streams containing Debezium envelope records.
// Unlike the JSON DebeziumDecoder, before/after are already decoded as typed Avro
// records (map[string]any) — no second JSON parse is needed.
//
// It implements both Decoder (stub) and StreamDecoder.
type DebeziumAvroDecoder struct {
	warnedNullBefore sync.Once
}

// Decode is not supported for Debezium Avro — use DecodeStream instead.
func (d *DebeziumAvroDecoder) Decode(data []byte) (engine.Record, error) {
	return engine.Record{}, fmt.Errorf("debezium avro decoder requires stream mode; use DecodeStream")
}

// DecodeStream reads an Avro OCF stream with Debezium envelope schema and sends
// decoded records to ch. Each Debezium event may produce 0-2 engine records
// depending on the operation type.
func (d *DebeziumAvroDecoder) DecodeStream(r io.Reader, ch chan<- engine.Record) error {
	defer close(ch)

	ocfr, err := goavro.NewOCFReader(r)
	if err != nil {
		return fmt.Errorf("debezium avro OCF reader error: %w", err)
	}

	for ocfr.Scan() {
		datum, err := ocfr.Read()
		if err != nil {
			return fmt.Errorf("debezium avro read error: %w", err)
		}

		recs, err := d.decodeEnvelope(datum)
		if err != nil {
			return fmt.Errorf("debezium avro decode error: %w", err)
		}

		for _, rec := range recs {
			ch <- rec
		}
	}

	return ocfr.Err()
}

// decodeEnvelope processes a single Avro-decoded Debezium envelope and returns
// 0-2 engine records.
func (d *DebeziumAvroDecoder) decodeEnvelope(datum any) ([]engine.Record, error) {
	env, ok := datum.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected map[string]any, got %T", datum)
	}

	op, _ := env["op"].(string)
	if op == "" {
		return nil, fmt.Errorf("missing or empty 'op' field")
	}

	before := unwrapAvroUnion(env["before"])
	after := unwrapAvroUnion(env["after"])

	sourceDB, _ := env["source_db"].(string)
	sourceTable, _ := env["source_table"].(string)
	sourceTsMs, _ := env["source_ts_ms"].(int64)
	// goavro may return int32 for long fields in some cases
	if sourceTsMs == 0 {
		if v, ok := env["source_ts_ms"].(int32); ok {
			sourceTsMs = int64(v)
		}
	}

	now := time.Now()

	switch op {
	case "c", "r":
		// Create or snapshot read: emit (after columns, weight=+1)
		afterMap, ok := after.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("debezium %s: after is not a record", op)
		}
		rec := d.buildRecord(afterMap, op, before, after, sourceDB, sourceTable, sourceTsMs, now, 1)
		return []engine.Record{rec}, nil

	case "u":
		// Update: emit (before, weight=-1) then (after, weight=+1)
		afterMap, ok := after.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("debezium update: after is not a record (malformed)")
		}

		var recs []engine.Record

		if beforeMap, ok := before.(map[string]any); ok {
			rec := d.buildRecord(beforeMap, op, before, after, sourceDB, sourceTable, sourceTsMs, now, -1)
			recs = append(recs, rec)
		} else if before != nil {
			return nil, fmt.Errorf("debezium update: before is not a record")
		} else {
			// Null before — REPLICA IDENTITY DEFAULT
			d.warnedNullBefore.Do(func() {
				log.Println("Warning: Debezium Avro update with null before (REPLICA IDENTITY not FULL). Retraction skipped; accumulators may drift.")
			})
		}

		rec := d.buildRecord(afterMap, op, before, after, sourceDB, sourceTable, sourceTsMs, now, 1)
		recs = append(recs, rec)
		return recs, nil

	case "d":
		// Delete: emit (before, weight=-1)
		beforeMap, ok := before.(map[string]any)
		if !ok {
			d.warnedNullBefore.Do(func() {
				log.Println("Warning: Debezium Avro delete with null before. Record is un-retractable.")
			})
			return nil, fmt.Errorf("debezium delete: before is null (un-retractable)")
		}
		rec := d.buildRecord(beforeMap, op, before, after, sourceDB, sourceTable, sourceTsMs, now, -1)
		return []engine.Record{rec}, nil

	case "t":
		log.Println("Warning: Debezium Avro truncate event received, skipping")
		return nil, nil

	default:
		return nil, fmt.Errorf("debezium avro: unknown op %q", op)
	}
}

// buildRecord converts an Avro record map to an engine.Record with virtual columns.
func (d *DebeziumAvroDecoder) buildRecord(
	payload map[string]any,
	op string,
	before, after any,
	sourceDB, sourceTable string,
	sourceTsMs int64,
	ts time.Time,
	weight int,
) engine.Record {
	cols := make(map[string]engine.Value, len(payload)+numVirtuals)

	for k, v := range payload {
		cols[k] = avroInferValue(v)
	}

	// Virtual columns
	cols["_op"] = engine.TextValue{V: op}

	if before == nil {
		cols["_before"] = engine.NullValue{}
	} else {
		cols["_before"] = engine.JsonValue{V: before}
	}

	if after == nil {
		cols["_after"] = engine.NullValue{}
	} else {
		cols["_after"] = engine.JsonValue{V: after}
	}

	cols["_table"] = engine.TextValue{V: sourceTable}
	cols["_db"] = engine.TextValue{V: sourceDB}
	if sourceTsMs > 0 {
		cols["_ts"] = engine.TimestampValue{V: time.UnixMilli(sourceTsMs).UTC()}
	} else {
		cols["_ts"] = engine.NullValue{}
	}
	// No _source in Avro envelope (fields are top-level), set to null
	cols["_source"] = engine.NullValue{}

	return engine.Record{
		Columns:   cols,
		Timestamp: ts,
		Weight:    weight,
	}
}

// unwrapAvroUnion extracts the inner value from an Avro union.
// goavro represents unions as map[string]any{"TypeName": value} for non-null,
// and nil for null.
func unwrapAvroUnion(v any) any {
	if v == nil {
		return nil
	}
	if m, ok := v.(map[string]any); ok && len(m) == 1 {
		for _, inner := range m {
			return inner
		}
	}
	return v
}
