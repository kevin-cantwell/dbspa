package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/linkedin/goavro/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/structpb"

	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoregistry"
	descriptorpb "google.golang.org/protobuf/types/descriptorpb"
)

// GenerateCmd creates synthetic data fixtures at controlled throughput.
type GenerateCmd struct {
	Dataset string `arg:"" help:"Dataset: orders, orders-cdc, metrics, clickstream." enum:"orders,orders-cdc,metrics,clickstream"`
	Count   int    `help:"Number of records to generate (0=infinite)." default:"10000"`
	Rate    int    `help:"Records per second (0=unlimited/burst)." default:"0"`
	Seed    int64  `help:"Random seed for reproducible output." default:"0"`
	Format  string `help:"Output format: ndjson, avro, protobuf." default:"ndjson" enum:"ndjson,avro,protobuf"`
}

func (c *GenerateCmd) Run() error {
	rng := rand.New(rand.NewSource(c.Seed))
	if c.Seed == 0 {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	var gen func(rng *rand.Rand, seq int) map[string]any

	switch c.Dataset {
	case "orders":
		gen = genOrder
	case "orders-cdc":
		g := newCDCGenerator(rng)
		gen = func(rng *rand.Rand, seq int) map[string]any { return g.next(rng, seq) }
	case "metrics":
		gen = genMetric
	case "clickstream":
		gen = genClick
	default:
		return fmt.Errorf("unknown dataset: %s", c.Dataset)
	}

	// Build encoder based on format
	var encoder recordEncoder
	switch c.Format {
	case "ndjson":
		encoder = newJSONEncoder()
	case "avro":
		schema, err := avroSchemaFor(c.Dataset)
		if err != nil {
			return fmt.Errorf("avro schema error: %w", err)
		}
		enc, err := newAvroEncoder(schema, c.Dataset)
		if err != nil {
			return err
		}
		encoder = enc
	case "protobuf":
		enc, err := newProtobufEncoder(c.Dataset)
		if err != nil {
			return err
		}
		encoder = enc
	}

	// Rate limiter
	var ticker *time.Ticker
	if c.Rate > 0 {
		interval := time.Second / time.Duration(c.Rate)
		if interval < time.Microsecond {
			interval = time.Microsecond
		}
		ticker = time.NewTicker(interval)
		defer ticker.Stop()
	}

	count := 0
	for {
		if c.Count > 0 && count >= c.Count {
			return encoder.Close()
		}

		if ticker != nil {
			<-ticker.C
		}

		rec := gen(rng, count)
		if err := encoder.Encode(rec); err != nil {
			return nil // broken pipe is normal
		}
		count++
	}
}

// recordEncoder writes records in a specific format.
type recordEncoder interface {
	Encode(rec map[string]any) error
	Close() error
}

// --- JSON encoder ---

type jsonEncoder struct {
	enc *json.Encoder
}

func newJSONEncoder() *jsonEncoder {
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	return &jsonEncoder{enc: enc}
}

func (e *jsonEncoder) Encode(rec map[string]any) error {
	return e.enc.Encode(rec)
}

func (e *jsonEncoder) Close() error { return nil }

// --- Avro encoder (OCF - Object Container File to stdout) ---

type avroEncoder struct {
	writer  *goavro.OCFWriter
	dataset string
}

func newAvroEncoder(schema string, dataset string) (*avroEncoder, error) {
	writer, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      os.Stdout,
		Schema: schema,
	})
	if err != nil {
		return nil, fmt.Errorf("avro writer error: %w", err)
	}
	return &avroEncoder{writer: writer, dataset: dataset}, nil
}

func (e *avroEncoder) Encode(rec map[string]any) error {
	avroRec := toAvroMap(rec, e.dataset)
	return e.writer.Append([]any{avroRec})
}

func (e *avroEncoder) Close() error { return nil }

func avroSchemaFor(dataset string) (string, error) {
	switch dataset {
	case "orders":
		return `{
			"type": "record",
			"name": "Order",
			"fields": [
				{"name": "order_id", "type": "int"},
				{"name": "customer_id", "type": "int"},
				{"name": "product", "type": "string"},
				{"name": "quantity", "type": "int"},
				{"name": "price", "type": "double"},
				{"name": "total", "type": "double"},
				{"name": "status", "type": "string"},
				{"name": "region", "type": "string"},
				{"name": "created_at", "type": "string"}
			]
		}`, nil
	case "metrics":
		return `{
			"type": "record",
			"name": "Metric",
			"fields": [
				{"name": "endpoint", "type": "string"},
				{"name": "method", "type": "string"},
				{"name": "status_code", "type": "int"},
				{"name": "latency_ms", "type": "int"},
				{"name": "region", "type": "string"},
				{"name": "timestamp", "type": "string"}
			]
		}`, nil
	case "clickstream":
		return `{
			"type": "record",
			"name": "Click",
			"fields": [
				{"name": "user_id", "type": "string"},
				{"name": "page", "type": "string"},
				{"name": "action", "type": "string"},
				{"name": "session_id", "type": "string"},
				{"name": "duration_ms", "type": "int"},
				{"name": "timestamp", "type": "string"}
			]
		}`, nil
	case "orders-cdc":
		// CDC has nested structures — use string for before/after/source
		return `{
			"type": "record",
			"name": "CDCEvent",
			"fields": [
				{"name": "op", "type": "string"},
				{"name": "before", "type": ["null", "string"], "default": null},
				{"name": "after", "type": ["null", "string"], "default": null},
				{"name": "source", "type": ["null", "string"], "default": null}
			]
		}`, nil
	default:
		return "", fmt.Errorf("no avro schema for dataset: %s", dataset)
	}
}

// --- Protobuf encoder (length-delimited messages to stdout) ---

type protobufEncoder struct {
	msgDesc *descriptorpb.DescriptorProto
	dataset string
}

func newProtobufEncoder(dataset string) (*protobufEncoder, error) {
	return &protobufEncoder{dataset: dataset}, nil
}

func (e *protobufEncoder) Encode(rec map[string]any) error {
	// Use structpb.Struct for dynamic protobuf encoding — this wraps
	// the record as a google.protobuf.Struct (self-describing)
	s, err := structpb.NewStruct(rec)
	if err != nil {
		return fmt.Errorf("protobuf struct error: %w", err)
	}
	data, err := proto.Marshal(s)
	if err != nil {
		return fmt.Errorf("protobuf marshal error: %w", err)
	}
	// Write length-delimited: 4-byte big-endian length + message
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
	if _, err := os.Stdout.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err = os.Stdout.Write(data)
	return err
}

func (e *protobufEncoder) Close() error { return nil }

// Suppress unused import warnings for protodesc and protoregistry
// (they'll be needed when we add typed protobuf schemas)
var (
	_ = protodesc.NewFile
	_ = protoregistry.GlobalFiles
	_ = (*dynamicpb.Message)(nil)
)
