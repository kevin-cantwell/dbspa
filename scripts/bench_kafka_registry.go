// +build ignore

// bench_kafka_registry.go produces Confluent-wire-format Avro messages to Kafka
// for benchmarking DBSPA's schema registry integration.
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	count := 100000
	if len(os.Args) > 1 {
		if n, err := strconv.Atoi(os.Args[1]); err == nil {
			count = n
		}
	}

	broker := "localhost:9092"
	topic := "bench-orders"
	schemaID := int32(1)

	schema := `{"type":"record","name":"Order","namespace":"dbspa.bench","fields":[{"name":"order_id","type":"int"},{"name":"customer_id","type":"int"},{"name":"product","type":"string"},{"name":"quantity","type":"int"},{"name":"price","type":"double"},{"name":"total","type":"double"},{"name":"status","type":"string"},{"name":"region","type":"string"},{"name":"created_at","type":"string"}]}`

	codec, err := goavro.NewCodec(schema)
	if err != nil {
		fmt.Fprintf(os.Stderr, "codec error: %v\n", err)
		os.Exit(1)
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		fmt.Fprintf(os.Stderr, "kafka client error: %v\n", err)
		os.Exit(1)
	}
	defer cl.Close()

	products := []string{"widget-a", "widget-b", "gadget-x", "gadget-y", "doohickey", "thingamajig"}
	statuses := []string{"pending", "confirmed", "shipped", "delivered", "cancelled"}
	regions := []string{"us-east", "us-west", "eu-west", "eu-central", "ap-south", "ap-east"}

	rng := rand.New(rand.NewSource(42))
	ctx := context.Background()

	start := time.Now()
	records := make([]*kgo.Record, 0, 1000)

	for i := 0; i < count; i++ {
		qty := rng.Intn(10) + 1
		price := float64(rng.Intn(9900)+100) / 100.0

		native := map[string]any{
			"order_id":    int32(i + 1),
			"customer_id": int32(rng.Intn(5000) + 1),
			"product":     products[rng.Intn(len(products))],
			"quantity":    int32(qty),
			"price":       price,
			"total":       float64(qty) * price,
			"status":      statuses[rng.Intn(len(statuses))],
			"region":      regions[rng.Intn(len(regions))],
			"created_at":  time.Now().UTC().Format(time.RFC3339),
		}

		payload, err := codec.BinaryFromNative(nil, native)
		if err != nil {
			fmt.Fprintf(os.Stderr, "encode error: %v\n", err)
			os.Exit(1)
		}

		// Confluent wire format: [0x00][4-byte schema ID][payload]
		msg := make([]byte, 5+len(payload))
		msg[0] = 0x00
		binary.BigEndian.PutUint32(msg[1:5], uint32(schemaID))
		copy(msg[5:], payload)

		records = append(records, &kgo.Record{
			Topic: topic,
			Value: msg,
		})

		// Flush in batches of 1000
		if len(records) >= 1000 {
			results := cl.ProduceSync(ctx, records...)
			for _, r := range results {
				if r.Err != nil {
					fmt.Fprintf(os.Stderr, "produce error: %v\n", r.Err)
					os.Exit(1)
				}
			}
			records = records[:0]
		}
	}

	// Flush remaining
	if len(records) > 0 {
		results := cl.ProduceSync(ctx, records...)
		for _, r := range results {
			if r.Err != nil {
				fmt.Fprintf(os.Stderr, "produce error: %v\n", r.Err)
				os.Exit(1)
			}
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("Produced %d messages in %s (%.0f msgs/sec)\n", count, elapsed, float64(count)/elapsed.Seconds())
}
