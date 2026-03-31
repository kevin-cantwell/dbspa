package format

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"sync"
	"time"

	"github.com/kevin-cantwell/folddb/internal/engine"
)

// DebeziumDecoder decodes Debezium JSON envelope records.
// An update emits TWO records (retraction + insertion), so the
// multi-record interface DecodeMulti must be used.
type DebeziumDecoder struct {
	warnedNullBefore sync.Once
}

// debeziumEnvelope is the wire format of a Debezium change event.
type debeziumEnvelope struct {
	Op     string          `json:"op"`
	Before json.RawMessage `json:"before"`
	After  json.RawMessage `json:"after"`
	Source json.RawMessage `json:"source"`
}

// debeziumSource is a subset of the source block we extract virtual columns from.
type debeziumSource struct {
	Table string `json:"table"`
	DB    string `json:"db"`
	TsMs  int64  `json:"ts_ms"`
}

// Decode returns the first record from a Debezium envelope. For updates
// this only returns the _after insertion. Use DecodeMulti for full semantics.
func (d *DebeziumDecoder) Decode(data []byte) (engine.Record, error) {
	recs, err := d.DecodeMulti(data)
	if err != nil {
		return engine.Record{}, err
	}
	if len(recs) == 0 {
		return engine.Record{}, fmt.Errorf("debezium: no records emitted (op may be truncate)")
	}
	return recs[0], nil
}

// DecodeMulti parses a Debezium JSON envelope and returns 0-2 records
// depending on the operation type.
func (d *DebeziumDecoder) DecodeMulti(data []byte) ([]engine.Record, error) {
	var env debeziumEnvelope
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&env); err != nil {
		return nil, fmt.Errorf("debezium decode error: %w", err)
	}

	// Parse source block for virtual columns
	var src debeziumSource
	if env.Source != nil {
		_ = json.Unmarshal(env.Source, &src)
	}

	// Build shared virtual columns
	virtuals := map[string]engine.Value{
		"_op": engine.TextValue{V: env.Op},
	}

	// _before virtual column
	if isJSONNull(env.Before) {
		virtuals["_before"] = engine.NullValue{}
	} else {
		var beforeAny any
		_ = json.Unmarshal(env.Before, &beforeAny)
		virtuals["_before"] = engine.JsonValue{V: beforeAny}
	}

	// _after virtual column
	if isJSONNull(env.After) {
		virtuals["_after"] = engine.NullValue{}
	} else {
		var afterAny any
		_ = json.Unmarshal(env.After, &afterAny)
		virtuals["_after"] = engine.JsonValue{V: afterAny}
	}

	// Source-derived virtual columns
	virtuals["_table"] = engine.TextValue{V: src.Table}
	virtuals["_db"] = engine.TextValue{V: src.DB}
	if src.TsMs > 0 {
		virtuals["_ts"] = engine.TimestampValue{V: time.UnixMilli(src.TsMs).UTC()}
	} else {
		virtuals["_ts"] = engine.NullValue{}
	}
	if env.Source != nil {
		var sourceAny any
		_ = json.Unmarshal(env.Source, &sourceAny)
		virtuals["_source"] = engine.JsonValue{V: sourceAny}
	} else {
		virtuals["_source"] = engine.NullValue{}
	}

	now := time.Now()

	switch env.Op {
	case "c", "r":
		// Create or snapshot read: emit (_after columns, diff=+1)
		rec, err := d.buildRecord(env.After, virtuals, now, 1)
		if err != nil {
			return nil, fmt.Errorf("debezium %s: %w", env.Op, err)
		}
		return []engine.Record{rec}, nil

	case "u":
		// Update: emit (_before, diff=-1) then (_after, diff=+1)
		var recs []engine.Record

		if isJSONNull(env.After) {
			return nil, fmt.Errorf("debezium update: _after is null (malformed)")
		}

		if !isJSONNull(env.Before) {
			beforeRec, err := d.buildRecord(env.Before, virtuals, now, -1)
			if err != nil {
				return nil, fmt.Errorf("debezium update _before: %w", err)
			}
			recs = append(recs, beforeRec)
		} else {
			// REPLICA IDENTITY DEFAULT: skip retraction, warn once
			d.warnedNullBefore.Do(func() {
				log.Println("Warning: Debezium update with null _before (REPLICA IDENTITY not FULL). Retraction skipped; accumulators may drift.")
			})
		}

		afterRec, err := d.buildRecord(env.After, virtuals, now, 1)
		if err != nil {
			return nil, fmt.Errorf("debezium update _after: %w", err)
		}
		recs = append(recs, afterRec)
		return recs, nil

	case "d":
		// Delete: emit (_before, diff=-1)
		if isJSONNull(env.Before) {
			// No _before on delete — un-retractable per spec
			d.warnedNullBefore.Do(func() {
				log.Println("Warning: Debezium delete with null _before. Record is un-retractable.")
			})
			return nil, fmt.Errorf("debezium delete: _before is null (un-retractable, should route to dead-letter)")
		}
		rec, err := d.buildRecord(env.Before, virtuals, now, -1)
		if err != nil {
			return nil, fmt.Errorf("debezium delete: %w", err)
		}
		return []engine.Record{rec}, nil

	case "t":
		// Truncate: log warning, skip
		log.Println("Warning: Debezium truncate event received, skipping")
		return nil, nil

	default:
		return nil, fmt.Errorf("debezium: unknown op %q", env.Op)
	}
}

// buildRecord parses a JSON payload and merges in virtual columns.
func (d *DebeziumDecoder) buildRecord(payload json.RawMessage, virtuals map[string]engine.Value, ts time.Time, diff int) (engine.Record, error) {
	var raw map[string]any
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	if err := dec.Decode(&raw); err != nil {
		return engine.Record{}, fmt.Errorf("payload decode: %w", err)
	}

	cols := make(map[string]engine.Value, len(raw)+len(virtuals))
	for k, v := range raw {
		cols[k] = inferValue(v)
	}
	// Merge virtual columns (overwrite payload columns if name collision)
	maps.Copy(cols, virtuals)

	return engine.Record{
		Columns:   cols,
		Timestamp: ts,
		Weight:      diff,
	}, nil
}

// isJSONNull returns true if the raw JSON is null or empty.
func isJSONNull(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return true
	}
	trimmed := bytes.TrimSpace(raw)
	return string(trimmed) == "null"
}
