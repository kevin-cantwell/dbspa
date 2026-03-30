// Package format provides decoders that convert raw bytes into engine Records.
package format

import (
	"fmt"
	"io"
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

// StreamDecoder reads from a binary stream and sends records to a channel.
// Unlike Decoder (which processes individual lines), StreamDecoder handles
// its own framing (e.g., Avro OCF blocks, length-delimited Protobuf).
type StreamDecoder interface {
	// DecodeStream reads from r and sends decoded records to ch.
	// It closes ch when the reader is exhausted or an error occurs.
	DecodeStream(r io.Reader, ch chan<- engine.Record) error
}

// NewDecoder returns the appropriate decoder for the given format string.
// An empty string defaults to JSON/NDJSON.
func NewDecoder(formatStr string) (Decoder, error) {
	return NewDecoderWithOptions(formatStr, nil)
}

// NewDecoderWithOptions returns the appropriate decoder for the given format
// string and optional key-value options (e.g., delimiter, header, quote for CSV).
func NewDecoderWithOptions(formatStr string, opts map[string]string) (Decoder, error) {
	switch strings.ToUpper(strings.TrimSpace(formatStr)) {
	case "", "JSON", "NDJSON":
		return &JSONDecoder{}, nil
	case "DEBEZIUM":
		return &DebeziumDecoder{}, nil
	case "CSV":
		return newCSVDecoder(opts), nil
	case "AVRO":
		return &AvroOCFDecoder{}, nil
	case "PROTOBUF":
		// Check for typed protobuf via message= option
		if opts != nil {
			if msgType, ok := opts["message"]; ok && msgType != "" {
				return &TypedProtobufDecoder{MessageType: msgType}, nil
			}
		}
		return &ProtobufDecoder{}, nil
	case "PARQUET":
		return &ParquetDecoder{}, nil
	default:
		return nil, fmt.Errorf("unsupported format: %q", formatStr)
	}
}

// newCSVDecoder creates a CSVDecoder from format options.
func newCSVDecoder(opts map[string]string) *CSVDecoder {
	d := &CSVDecoder{
		Delimiter: ',',
		Header:    true,
	}
	if opts == nil {
		return d
	}
	if v, ok := opts["delimiter"]; ok && len(v) > 0 {
		switch v {
		case "\\t", "tab":
			d.Delimiter = '\t'
		default:
			d.Delimiter = rune(v[0])
		}
	}
	if v, ok := opts["header"]; ok {
		d.Header = strings.ToLower(v) == "true"
	}
	if v, ok := opts["quote"]; ok && len(v) > 0 {
		d.Quote = rune(v[0])
	}
	return d
}

// DecodeAll decodes data using the given decoder. If the decoder is a
// MultiDecoder, it uses DecodeMulti; otherwise it wraps the single record.
// Returns (nil, ErrHeaderRow) for CSV header rows.
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
