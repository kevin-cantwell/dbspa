package format

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/kevin-cantwell/folddb/internal/engine"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// ProtobufDecoder decodes length-delimited google.protobuf.Struct messages.
// Each message is framed with a 4-byte big-endian length prefix.
// It implements StreamDecoder.
type ProtobufDecoder struct{}

// Decode is not supported for Protobuf streams — use DecodeStream instead.
func (d *ProtobufDecoder) Decode(data []byte) (engine.Record, error) {
	return engine.Record{}, fmt.Errorf("protobuf decoder requires stream mode; use DecodeStream")
}

// DecodeStream reads length-delimited protobuf Struct messages from r
// and sends decoded records to ch.
func (d *ProtobufDecoder) DecodeStream(r io.Reader, ch chan<- engine.Record) error {
	defer close(ch)

	var lenBuf [4]byte
	for {
		// Read 4-byte length prefix
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil // end of stream
			}
			return fmt.Errorf("protobuf read length: %w", err)
		}

		msgLen := binary.BigEndian.Uint32(lenBuf[:])
		if msgLen == 0 {
			continue
		}

		// Read message bytes
		msgBuf := make([]byte, msgLen)
		if _, err := io.ReadFull(r, msgBuf); err != nil {
			return fmt.Errorf("protobuf read message (%d bytes): %w", msgLen, err)
		}

		// Unmarshal as google.protobuf.Struct
		s := &structpb.Struct{}
		if err := proto.Unmarshal(msgBuf, s); err != nil {
			return fmt.Errorf("protobuf unmarshal: %w", err)
		}

		rec := structToRecord(s)
		ch <- rec
	}
}

// structToRecord converts a protobuf Struct to an engine.Record.
func structToRecord(s *structpb.Struct) engine.Record {
	cols := make(map[string]engine.Value, len(s.Fields))
	for k, v := range s.Fields {
		cols[k] = pbValueToEngine(v)
	}
	return engine.Record{
		Columns:   cols,
		Timestamp: time.Now(),
		Weight:      1,
	}
}

// pbValueToEngine converts a protobuf Value to an engine.Value.
func pbValueToEngine(v *structpb.Value) engine.Value {
	switch v.Kind.(type) {
	case *structpb.Value_NullValue:
		return engine.NullValue{}
	case *structpb.Value_NumberValue:
		f := v.GetNumberValue()
		// Recover integer type for whole numbers
		if f == float64(int64(f)) {
			return engine.IntValue{V: int64(f)}
		}
		return engine.FloatValue{V: f}
	case *structpb.Value_StringValue:
		return engine.TextValue{V: v.GetStringValue()}
	case *structpb.Value_BoolValue:
		return engine.BoolValue{V: v.GetBoolValue()}
	case *structpb.Value_StructValue:
		return engine.JsonValue{V: v.GetStructValue().AsMap()}
	case *structpb.Value_ListValue:
		return engine.JsonValue{V: v.GetListValue().AsSlice()}
	default:
		return engine.NullValue{}
	}
}
