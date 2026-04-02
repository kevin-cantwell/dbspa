package format

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/kevin-cantwell/folddb/internal/engine"
	"github.com/kevin-cantwell/folddb/internal/registry"
)

// ConfluentAvroDecoder decodes Kafka messages encoded with the Confluent
// Schema Registry wire format:
//
//	[0x00] [4-byte schema ID (big-endian)] [Avro binary payload]
//
// Each message is self-describing via the schema ID, which is resolved
// against the Schema Registry. Schemas are cached — IDs are immutable.
type ConfluentAvroDecoder struct {
	Registry *registry.Client
}

// NewConfluentAvroDecoder creates a decoder that uses the given registry client
// to resolve schema IDs in the Confluent wire format.
func NewConfluentAvroDecoder(reg *registry.Client) *ConfluentAvroDecoder {
	return &ConfluentAvroDecoder{Registry: reg}
}

// Decode handles a single Kafka message with the Confluent wire format.
func (d *ConfluentAvroDecoder) Decode(data []byte) (engine.Record, error) {
	recs, err := d.DecodeMulti(data)
	if err != nil {
		return engine.Record{}, err
	}
	if len(recs) == 0 {
		return engine.Record{}, fmt.Errorf("confluent avro: decoded zero records")
	}
	return recs[0], nil
}

// DecodeMulti decodes a Confluent wire format message. If the decoded Avro
// record is a Debezium envelope (has op, before, after fields), it applies
// Debezium CDC semantics and may return 0-2 records. Otherwise it returns
// a single record.
func (d *ConfluentAvroDecoder) DecodeMulti(data []byte) ([]engine.Record, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("confluent avro: message too short (%d bytes, need at least 5)", len(data))
	}

	// Verify magic byte
	if data[0] != 0x00 {
		return nil, fmt.Errorf("confluent avro: invalid magic byte 0x%02x (expected 0x00)", data[0])
	}

	// Extract schema ID (4-byte big-endian uint32)
	schemaID := int32(binary.BigEndian.Uint32(data[1:5]))

	// Get codec from registry (cached)
	codec, err := d.Registry.GetCodec(schemaID)
	if err != nil {
		return nil, fmt.Errorf("confluent avro: schema ID %d: %w", schemaID, err)
	}

	// Decode Avro binary payload
	native, _, err := codec.NativeFromBinary(data[5:])
	if err != nil {
		return nil, fmt.Errorf("confluent avro: decode error (schema ID %d): %w", schemaID, err)
	}

	// Convert to engine.Record
	rec, err := avroToRecord(native)
	if err != nil {
		return nil, fmt.Errorf("confluent avro: convert error: %w", err)
	}

	return []engine.Record{rec}, nil
}

// parseConfluentHeader extracts the schema ID from a Confluent wire format
// message, returning the schema ID and the payload bytes after the header.
func parseConfluentHeader(data []byte) (schemaID int32, payload []byte, err error) {
	if len(data) < 5 {
		return 0, nil, fmt.Errorf("message too short (%d bytes, need at least 5)", len(data))
	}
	if data[0] != 0x00 {
		return 0, nil, fmt.Errorf("invalid magic byte 0x%02x (expected 0x00)", data[0])
	}
	schemaID = int32(binary.BigEndian.Uint32(data[1:5]))
	return schemaID, data[5:], nil
}

// IsConfluentWireFormat checks if data starts with the Confluent wire format
// magic byte and has enough bytes for the header.
func IsConfluentWireFormat(data []byte) bool {
	return len(data) >= 5 && data[0] == 0x00
}

// ConfluentDebeziumAvroDecoder decodes Kafka messages in the Confluent wire
// format where the Avro schema is a Debezium envelope (with op, before, after
// fields). It strips the wire header, decodes with the registry schema, and
// applies Debezium CDC semantics (op → weight mapping).
type ConfluentDebeziumAvroDecoder struct {
	Registry *registry.Client
	inner    DebeziumAvroDecoder // reuse envelope decoding logic
}

// NewConfluentDebeziumAvroDecoder creates a Debezium-aware Confluent Avro decoder.
func NewConfluentDebeziumAvroDecoder(reg *registry.Client) *ConfluentDebeziumAvroDecoder {
	return &ConfluentDebeziumAvroDecoder{Registry: reg}
}

// Decode handles a single Kafka message.
func (d *ConfluentDebeziumAvroDecoder) Decode(data []byte) (engine.Record, error) {
	recs, err := d.DecodeMulti(data)
	if err != nil {
		return engine.Record{}, err
	}
	if len(recs) == 0 {
		return engine.Record{}, fmt.Errorf("confluent debezium avro: decoded zero records")
	}
	return recs[0], nil
}

// DecodeMulti decodes a Confluent wire format message containing a Debezium
// envelope. Returns 0-2 records depending on the operation type.
func (d *ConfluentDebeziumAvroDecoder) DecodeMulti(data []byte) ([]engine.Record, error) {
	schemaID, payload, err := parseConfluentHeader(data)
	if err != nil {
		return nil, fmt.Errorf("confluent debezium avro: %w", err)
	}

	codec, err := d.Registry.GetCodec(schemaID)
	if err != nil {
		return nil, fmt.Errorf("confluent debezium avro: schema ID %d: %w", schemaID, err)
	}

	native, _, err := codec.NativeFromBinary(payload)
	if err != nil {
		return nil, fmt.Errorf("confluent debezium avro: decode error (schema ID %d): %w", schemaID, err)
	}

	// Delegate to the Debezium envelope decoder
	recs, err := d.inner.decodeEnvelope(native)
	if err != nil {
		return nil, fmt.Errorf("confluent debezium avro: %w", err)
	}

	// Set timestamps to now for records without explicit timestamps
	now := time.Now()
	for i := range recs {
		if recs[i].Timestamp.IsZero() {
			recs[i].Timestamp = now
		}
	}

	return recs, nil
}
