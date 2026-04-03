//go:build ignore

// bench_kafka_live.go benchmarks DBSPA's live Kafka consumption by
// producing and consuming concurrently. Measures end-to-end latency
// and throughput for the real-world streaming use case.
//
// Usage:
//   # Start Kafka + Schema Registry first:
//   docker compose up -d
//
//   # Run benchmark:
//   go run scripts/bench_kafka_live.go [count] [rate]
//
//   # count: number of messages (default 100000)
//   # rate:  messages/sec (default 0 = unlimited)
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	count := 100000
	rate := 0 // 0 = unlimited
	if len(os.Args) > 1 {
		if n, err := strconv.Atoi(os.Args[1]); err == nil {
			count = n
		}
	}
	if len(os.Args) > 2 {
		if n, err := strconv.Atoi(os.Args[2]); err == nil {
			rate = n
		}
	}

	broker := "localhost:9092"
	registryURL := "http://localhost:8081"
	topic := fmt.Sprintf("bench-live-%d", time.Now().UnixMilli())

	fmt.Printf("=== Live Kafka Benchmark ===\n")
	fmt.Printf("Topic:    %s\n", topic)
	fmt.Printf("Messages: %d\n", count)
	if rate > 0 {
		fmt.Printf("Rate:     %d msgs/sec\n", rate)
	} else {
		fmt.Printf("Rate:     unlimited (burst)\n")
	}
	fmt.Println()

	// Create topic
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		fatal("kafka client: %v", err)
	}
	admin := kadm.NewClient(cl)
	_, err = admin.CreateTopic(context.Background(), 3, 1, nil, topic)
	if err != nil {
		fatal("create topic: %v", err)
	}
	cl.Close()
	fmt.Printf("Created topic %s (3 partitions)\n", topic)

	// Register schema
	schema := `{"type":"record","name":"Order","namespace":"dbspa.bench","fields":[{"name":"order_id","type":"int"},{"name":"customer_id","type":"int"},{"name":"product","type":"string"},{"name":"quantity","type":"int"},{"name":"price","type":"double"},{"name":"total","type":"double"},{"name":"status","type":"string"},{"name":"region","type":"string"},{"name":"created_at","type":"string"}]}`

	schemaID := registerSchema(registryURL, topic+"-value", schema)
	fmt.Printf("Registered schema ID: %d\n\n", schemaID)

	codec, err := goavro.NewCodec(schema)
	if err != nil {
		fatal("codec: %v", err)
	}

	// Build dbspa binary
	dbspaPath := "/tmp/dbspa"
	if _, err := os.Stat(dbspaPath); os.IsNotExist(err) {
		fmt.Println("Building dbspa...")
		cmd := exec.Command("go", "build", "-o", dbspaPath, "./cmd/dbspa")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			fatal("build: %v", err)
		}
	}

	// Start DBSPA consumer FIRST (so it's ready when messages arrive)
	sql := fmt.Sprintf("SELECT status, region, COUNT(*) AS cnt, SUM(total) AS revenue FROM 'kafka://%s/%s?offset=earliest&registry=%s' FORMAT AVRO GROUP BY status, region", broker, topic, registryURL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var consumerLines atomic.Int64
	var consumerDone sync.WaitGroup
	consumerDone.Add(1)

	consumerCmd := exec.CommandContext(ctx, dbspaPath, "query", sql)
	consumerOut, err := consumerCmd.StdoutPipe()
	if err != nil {
		fatal("stdout pipe: %v", err)
	}
	consumerCmd.Stderr = os.Stderr

	if err := consumerCmd.Start(); err != nil {
		fatal("start dbspa: %v", err)
	}

	// Count output lines in background
	go func() {
		defer consumerDone.Done()
		buf := make([]byte, 64*1024)
		for {
			n, err := consumerOut.Read(buf)
			if n > 0 {
				// Count newlines
				for i := 0; i < n; i++ {
					if buf[i] == '\n' {
						consumerLines.Add(1)
					}
				}
			}
			if err != nil {
				return
			}
		}
	}()

	// Give consumer a moment to connect
	time.Sleep(2 * time.Second)
	fmt.Println("DBSPA consumer started, producing messages...")

	// Start producer
	producerCl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		fatal("producer client: %v", err)
	}
	defer producerCl.Close()

	products := []string{"widget-a", "widget-b", "gadget-x", "gadget-y", "doohickey", "thingamajig"}
	statuses := []string{"pending", "confirmed", "shipped", "delivered", "cancelled"}
	regions := []string{"us-east", "us-west", "eu-west", "eu-central", "ap-south", "ap-east"}
	rng := rand.New(rand.NewSource(42))

	var ticker *time.Ticker
	if rate > 0 {
		interval := time.Second / time.Duration(rate)
		if interval < time.Microsecond {
			interval = time.Microsecond
		}
		ticker = time.NewTicker(interval)
		defer ticker.Stop()
	}

	produceStart := time.Now()
	batch := make([]*kgo.Record, 0, 1000)

	for i := 0; i < count; i++ {
		if ticker != nil {
			<-ticker.C
		}

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

		payload, _ := codec.BinaryFromNative(nil, native)
		msg := make([]byte, 5+len(payload))
		msg[0] = 0x00
		binary.BigEndian.PutUint32(msg[1:5], uint32(schemaID))
		copy(msg[5:], payload)

		batch = append(batch, &kgo.Record{Topic: topic, Value: msg})

		if len(batch) >= 1000 {
			results := producerCl.ProduceSync(context.Background(), batch...)
			for _, r := range results {
				if r.Err != nil {
					fatal("produce: %v", r.Err)
				}
			}
			batch = batch[:0]
		}
	}

	// Flush remaining
	if len(batch) > 0 {
		producerCl.ProduceSync(context.Background(), batch...)
	}
	produceElapsed := time.Since(produceStart)

	fmt.Printf("\nProduction complete: %d msgs in %s (%.0f msgs/sec)\n",
		count, produceElapsed, float64(count)/produceElapsed.Seconds())

	// Wait for consumer to catch up
	fmt.Println("Waiting for consumer to catch up...")
	deadline := time.Now().Add(30 * time.Second)
	lastLines := consumerLines.Load()
	stableCount := 0
	for time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		current := consumerLines.Load()
		if current == lastLines && current > 0 {
			stableCount++
			if stableCount >= 4 { // 2 seconds of no new output
				break
			}
		} else {
			stableCount = 0
		}
		lastLines = current
	}

	totalElapsed := time.Since(produceStart)
	outputLines := consumerLines.Load()

	// Kill consumer
	cancel()
	consumerCmd.Wait()

	fmt.Printf("\n=== Results ===\n")
	fmt.Printf("Messages produced:    %d\n", count)
	fmt.Printf("Changelog lines out:  %d\n", outputLines)
	fmt.Printf("Production time:      %s (%.0f msgs/sec)\n", produceElapsed, float64(count)/produceElapsed.Seconds())
	fmt.Printf("Total end-to-end:     %s\n", totalElapsed)
	fmt.Printf("Effective throughput: %.0f msgs/sec\n", float64(count)/totalElapsed.Seconds())

	// Cleanup topic
	admin2 := kadm.NewClient(producerCl)
	admin2.DeleteTopics(context.Background(), topic)
	fmt.Printf("\nCleaned up topic %s\n", topic)
}

func registerSchema(registryURL, subject, schema string) int32 {
	import_json := fmt.Sprintf(`{"schema": %s}`, strconv.Quote(schema))
	cmd := exec.Command("curl", "-s", "-X", "POST",
		registryURL+"/subjects/"+subject+"/versions",
		"-H", "Content-Type: application/vnd.schemaregistry.v1+json",
		"-d", import_json)
	out, err := cmd.Output()
	if err != nil {
		fatal("register schema: %v", err)
	}
	// Parse {"id":N}
	s := string(out)
	if idx := strings.Index(s, `"id":`); idx >= 0 {
		rest := s[idx+5:]
		end := strings.IndexAny(rest, ",}")
		if end > 0 {
			id, _ := strconv.Atoi(strings.TrimSpace(rest[:end]))
			return int32(id)
		}
	}
	fatal("cannot parse schema ID from: %s", s)
	return 0
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "Error: "+format+"\n", args...)
	os.Exit(1)
}
