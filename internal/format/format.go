// Package format provides decoders that convert raw bytes into engine Records.
package format

import (
	"fmt"
	"strings"

	"github.com/kevin-cantwell/folddb/internal/engine"
)

// Decoder converts raw bytes into an engine Record.
type Decoder interface {
	// Decode parses raw bytes into a Record.
	Decode(data []byte) (engine.Record, error)
}

// MultiDecoder can emit multiple records from a single input (e.g., Debezium
// updates emit a retraction and an insertion).
type MultiDecoder interface {
	Decoder
	// DecodeMulti parses raw bytes into zero or more Records.
	DecodeMulti(data []byte) ([]engine.Record, error)
}

// NewDecoder returns the appropriate decoder for the given format string.
// An empty string defaults to JSON/NDJSON.
func NewDecoder(formatStr string) (Decoder, error) {
	switch strings.ToUpper(strings.TrimSpace(formatStr)) {
	case "", "JSON", "NDJSON":
		return &JSONDecoder{}, nil
	case "DEBEZIUM":
		return &DebeziumDecoder{}, nil
	default:
		return nil, fmt.Errorf("unsupported format: %q", formatStr)
	}
}

// DecodeAll decodes data using the given decoder. If the decoder is a
// MultiDecoder, it uses DecodeMulti; otherwise it wraps the single record.
func DecodeAll(dec Decoder, data []byte) ([]engine.Record, error) {
	if md, ok := dec.(MultiDecoder); ok {
		return md.DecodeMulti(data)
	}
	rec, err := dec.Decode(data)
	if err != nil {
		return nil, err
	}
	return []engine.Record{rec}, nil
}
