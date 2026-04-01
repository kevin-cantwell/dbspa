package format

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
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
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("debezium decode error: %w", err)
	}

	// Parse source block for virtual columns
	var src debeziumSource
	if env.Source != nil {
		_ = json.Unmarshal(env.Source, &src)
	}

	now := time.Now()

	switch env.Op {
	case "c", "r":
		// Create or snapshot read: emit (_after columns, diff=+1)
		rec, err := d.buildRecord(env.After, &env, &src, now, 1)
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
			beforeRec, err := d.buildRecord(env.Before, &env, &src, now, -1)
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

		afterRec, err := d.buildRecord(env.After, &env, &src, now, 1)
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
		rec, err := d.buildRecord(env.Before, &env, &src, now, -1)
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

// numVirtuals is the number of virtual columns added to each record.
const numVirtuals = 7 // _op, _before, _after, _table, _db, _ts, _source

// buildRecord parses a JSON payload and adds virtual columns directly.
// This avoids creating a separate virtuals map and copying it.
func (d *DebeziumDecoder) buildRecord(payload json.RawMessage, env *debeziumEnvelope, src *debeziumSource, ts time.Time, diff int) (engine.Record, error) {
	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err != nil {
		return engine.Record{}, fmt.Errorf("payload decode: %w", err)
	}

	cols := make(map[string]engine.Value, len(raw)+numVirtuals)
	for k, v := range raw {
		cols[k] = inferValue(v)
	}

	// Add virtual columns directly (overwrite payload columns if name collision)
	cols["_op"] = engine.TextValue{V: env.Op}

	if isJSONNull(env.Before) {
		cols["_before"] = engine.NullValue{}
	} else {
		cols["_before"] = &engine.LazyJsonValue{Raw: env.Before}
	}

	if isJSONNull(env.After) {
		cols["_after"] = engine.NullValue{}
	} else {
		cols["_after"] = &engine.LazyJsonValue{Raw: env.After}
	}

	cols["_table"] = engine.TextValue{V: src.Table}
	cols["_db"] = engine.TextValue{V: src.DB}
	if src.TsMs > 0 {
		cols["_ts"] = engine.TimestampValue{V: time.UnixMilli(src.TsMs).UTC()}
	} else {
		cols["_ts"] = engine.NullValue{}
	}
	if env.Source != nil {
		cols["_source"] = &engine.LazyJsonValue{Raw: env.Source}
	} else {
		cols["_source"] = engine.NullValue{}
	}

	return engine.Record{
		Columns:   cols,
		Timestamp: ts,
		Weight:    diff,
	}, nil
}

// isJSONNull returns true if the raw JSON is null or empty.
func isJSONNull(raw json.RawMessage) bool {
	n := len(raw)
	if n == 0 {
		return true
	}
	// Fast path: check for literal "null" without trimming
	if n == 4 && raw[0] == 'n' {
		return raw[1] == 'u' && raw[2] == 'l' && raw[3] == 'l'
	}
	// Slow path: handle whitespace-padded null
	if n > 4 {
		trimmed := bytes.TrimSpace(raw)
		return len(trimmed) == 4 && trimmed[0] == 'n' && trimmed[1] == 'u' && trimmed[2] == 'l' && trimmed[3] == 'l'
	}
	return false
}
