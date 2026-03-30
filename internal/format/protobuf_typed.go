package format

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/kevin-cantwell/folddb/internal/engine"

	pb "github.com/kevin-cantwell/folddb/proto/pb"
	"google.golang.org/protobuf/proto"
)

// TypedProtobufDecoder reads length-delimited typed protobuf messages.
// It uses a known message type specified by name to decode fields with
// their proper types (int64, double, string) instead of the generic
// structpb.Struct wrapper.
type TypedProtobufDecoder struct {
	MessageType string // "Order", "Metric", "Click"
}

// Decode is a stub for the Decoder interface.
func (d *TypedProtobufDecoder) Decode(data []byte) (engine.Record, error) {
	return engine.Record{}, fmt.Errorf("typed protobuf requires stream decoding (use --input or pipe)")
}

// DecodeStream reads length-delimited typed protobuf messages from the reader.
func (d *TypedProtobufDecoder) DecodeStream(r io.Reader, ch chan<- engine.Record) error {
	defer close(ch)

	var lenBuf [4]byte
	for {
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return fmt.Errorf("protobuf read length error: %w", err)
		}

		msgLen := binary.BigEndian.Uint32(lenBuf[:])
		msgBuf := make([]byte, msgLen)
		if _, err := io.ReadFull(r, msgBuf); err != nil {
			return fmt.Errorf("protobuf read message error: %w", err)
		}

		rec, err := d.decodeMessage(msgBuf)
		if err != nil {
			return err
		}
		ch <- rec
	}
}

func (d *TypedProtobufDecoder) decodeMessage(data []byte) (engine.Record, error) {
	cols := make(map[string]engine.Value)

	switch d.MessageType {
	case "Order":
		var msg pb.Order
		if err := proto.Unmarshal(data, &msg); err != nil {
			return engine.Record{}, fmt.Errorf("protobuf unmarshal error: %w", err)
		}
		cols["order_id"] = engine.IntValue{V: msg.OrderId}
		cols["customer_id"] = engine.IntValue{V: msg.CustomerId}
		cols["product"] = engine.TextValue{V: msg.Product}
		cols["quantity"] = engine.IntValue{V: msg.Quantity}
		cols["price"] = floatOrInt(msg.Price)
		cols["total"] = floatOrInt(msg.Total)
		cols["status"] = engine.TextValue{V: msg.Status}
		cols["region"] = engine.TextValue{V: msg.Region}
		cols["created_at"] = engine.TextValue{V: msg.CreatedAt}

	case "Metric":
		var msg pb.Metric
		if err := proto.Unmarshal(data, &msg); err != nil {
			return engine.Record{}, fmt.Errorf("protobuf unmarshal error: %w", err)
		}
		cols["endpoint"] = engine.TextValue{V: msg.Endpoint}
		cols["method"] = engine.TextValue{V: msg.Method}
		cols["status_code"] = engine.IntValue{V: msg.StatusCode}
		cols["latency_ms"] = engine.IntValue{V: msg.LatencyMs}
		cols["region"] = engine.TextValue{V: msg.Region}
		cols["timestamp"] = engine.TextValue{V: msg.Timestamp}

	case "Click":
		var msg pb.Click
		if err := proto.Unmarshal(data, &msg); err != nil {
			return engine.Record{}, fmt.Errorf("protobuf unmarshal error: %w", err)
		}
		cols["user_id"] = engine.TextValue{V: msg.UserId}
		cols["page"] = engine.TextValue{V: msg.Page}
		cols["action"] = engine.TextValue{V: msg.Action}
		cols["session_id"] = engine.TextValue{V: msg.SessionId}
		cols["duration_ms"] = engine.IntValue{V: msg.DurationMs}
		cols["timestamp"] = engine.TextValue{V: msg.Timestamp}

	default:
		return engine.Record{}, fmt.Errorf("unknown protobuf message type: %s", d.MessageType)
	}

	return engine.Record{
		Columns:   cols,
		Timestamp: time.Now(),
		Diff:      1,
	}, nil
}

func floatOrInt(f float64) engine.Value {
	if f == math.Trunc(f) && f >= math.MinInt64 && f <= math.MaxInt64 {
		return engine.IntValue{V: int64(f)}
	}
	return engine.FloatValue{V: f}
}
