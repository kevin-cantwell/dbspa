package main

import (
	"encoding/binary"
	"math/rand"
	"os"

	pb "github.com/kevin-cantwell/dbspa/proto/pb"
	"google.golang.org/protobuf/proto"
)

// typedProtobufEncoder encodes records using typed protobuf messages
// (not structpb.Struct). This produces smaller, faster wire format.
type typedProtobufEncoder struct {
	dataset string
}

func newTypedProtobufEncoder(dataset string) (*typedProtobufEncoder, error) {
	return &typedProtobufEncoder{dataset: dataset}, nil
}

func (e *typedProtobufEncoder) Encode(rec map[string]any) error {
	var data []byte
	var err error

	switch e.dataset {
	case "orders":
		msg := &pb.Order{
			OrderId:    int64(toInt(rec["order_id"])),
			CustomerId: int64(toInt(rec["customer_id"])),
			Product:    rec["product"].(string),
			Quantity:   int64(toInt(rec["quantity"])),
			Price:      toFloat64(rec["price"]),
			Total:      toFloat64(rec["total"]),
			Status:     rec["status"].(string),
			Region:     rec["region"].(string),
			CreatedAt:  rec["created_at"].(string),
		}
		data, err = proto.Marshal(msg)
	case "metrics":
		msg := &pb.Metric{
			Endpoint:   rec["endpoint"].(string),
			Method:     rec["method"].(string),
			StatusCode: int64(toInt(rec["status_code"])),
			LatencyMs:  int64(toInt(rec["latency_ms"])),
			Region:     rec["region"].(string),
			Timestamp:  rec["timestamp"].(string),
		}
		data, err = proto.Marshal(msg)
	case "clickstream":
		msg := &pb.Click{
			UserId:     rec["user_id"].(string),
			Page:       rec["page"].(string),
			Action:     rec["action"].(string),
			SessionId:  rec["session_id"].(string),
			DurationMs: int64(toInt(rec["duration_ms"])),
			Timestamp:  rec["timestamp"].(string),
		}
		data, err = proto.Marshal(msg)
	default:
		// Fall back to structpb for unsupported datasets
		enc, _ := newProtobufEncoder(e.dataset)
		return enc.Encode(rec)
	}

	if err != nil {
		return err
	}

	// Length-delimited framing (4-byte big-endian)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
	if _, err := os.Stdout.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err = os.Stdout.Write(data)
	return err
}

func (e *typedProtobufEncoder) Close() error { return nil }

// Suppress unused warning
var _ = func(rng *rand.Rand) {}
