package source

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaRecord wraps raw message bytes with Kafka-specific metadata
// needed for virtual column injection.
type KafkaRecord struct {
	Value     []byte
	Key       []byte
	Offset    int64
	Partition int32
	Timestamp time.Time
}

// KafkaClient abstracts the Kafka consumer so we can mock it in tests.
type KafkaClient interface {
	// PollFetches returns the next batch of fetches. Blocks until records
	// are available or the context is cancelled.
	PollFetches(ctx context.Context) kgo.Fetches
	// Close shuts down the client.
	Close()
}

// Kafka reads records from a Kafka topic and sends them downstream.
type Kafka struct {
	Config *KafkaConfig
	Client KafkaClient // set externally for testing; if nil, a real client is created
	ctx    context.Context
	cancel context.CancelFunc
}

// NewKafka creates a Kafka source from the given config. The context controls
// the consumer lifecycle — cancelling it triggers graceful shutdown.
func NewKafka(ctx context.Context, cfg *KafkaConfig) *Kafka {
	ctx, cancel := context.WithCancel(ctx)
	return &Kafka{
		Config: cfg,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Read starts consuming and returns a channel of KafkaRecords.
// The channel is closed when the context is cancelled.
func (k *Kafka) Read() <-chan KafkaRecord {
	ch := make(chan KafkaRecord, 256)
	go func() {
		defer close(ch)

		client := k.Client
		if client == nil {
			var err error
			client, err = k.createClient()
			if err != nil {
				fmt.Printf("Error: cannot connect to %s — %v\n", k.Config.Broker, err)
				return
			}
		}
		defer client.Close()

		for {
			fetches := client.PollFetches(k.ctx)
			if k.ctx.Err() != nil {
				return
			}
			fetches.EachRecord(func(r *kgo.Record) {
				rec := KafkaRecord{
					Value:     r.Value,
					Key:       r.Key,
					Offset:    r.Offset,
					Partition: r.Partition,
					Timestamp: r.Timestamp,
				}
				select {
				case ch <- rec:
				case <-k.ctx.Done():
					return
				}
			})
		}
	}()
	return ch
}

// ReadRaw satisfies the Source interface by returning raw bytes.
// Kafka virtual columns are NOT injected here — use Read() for
// full KafkaRecord metadata.
func (k *Kafka) ReadRaw() <-chan []byte {
	rawCh := make(chan []byte, 256)
	kafkaCh := k.Read()
	go func() {
		defer close(rawCh)
		for rec := range kafkaCh {
			if rec.Value != nil {
				rawCh <- rec.Value
			}
		}
	}()
	return rawCh
}

// Stop cancels the consumer context.
func (k *Kafka) Stop() {
	k.cancel()
}

func (k *Kafka) createClient() (KafkaClient, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(k.Config.Broker),
		kgo.ConsumeTopics(k.Config.Topic),
	}

	// Offset
	switch k.Config.Offset {
	case "earliest":
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	case "latest", "":
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	default:
		// Could be an integer offset or a timestamp — for now just use latest
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	}

	// Consumer group
	if k.Config.Group != "" {
		opts = append(opts, kgo.ConsumerGroup(k.Config.Group))
	}

	// Specific partitions
	if len(k.Config.Partitions) > 0 {
		partMap := make(map[string]map[int32]kgo.Offset)
		offsets := make(map[int32]kgo.Offset, len(k.Config.Partitions))
		for _, p := range k.Config.Partitions {
			offsets[p] = kgo.NewOffset().AtEnd()
		}
		partMap[k.Config.Topic] = offsets
		opts = append(opts, kgo.ConsumePartitions(partMap))
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka client error: %w", err)
	}
	return cl, nil
}

// KafkaMultiTopicRecord extends KafkaRecord with the topic name,
// used for stream-stream joins where a single consumer reads from
// multiple topics and dispatches by topic.
type KafkaMultiTopicRecord struct {
	KafkaRecord
	Topic string
}

// NewKafkaMultiTopic creates a Kafka source that consumes from multiple topics
// using a single consumer connection. This is more efficient than creating
// separate consumers per topic (one TCP connection, one consumer group).
func NewKafkaMultiTopic(ctx context.Context, broker string, topics []string, offset string, group string) (*KafkaMultiTopic, error) {
	ctx, cancel := context.WithCancel(ctx)
	return &KafkaMultiTopic{
		broker: broker,
		topics: topics,
		offset: offset,
		group:  group,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// KafkaMultiTopic reads from multiple topics on a single consumer.
type KafkaMultiTopic struct {
	broker string
	topics []string
	offset string
	group  string
	ctx    context.Context
	cancel context.CancelFunc
}

// Read starts consuming from all topics and returns a channel of records
// tagged with their source topic.
func (k *KafkaMultiTopic) Read() <-chan KafkaMultiTopicRecord {
	ch := make(chan KafkaMultiTopicRecord, 256)
	go func() {
		defer close(ch)

		opts := []kgo.Opt{
			kgo.SeedBrokers(k.broker),
			kgo.ConsumeTopics(k.topics...),
		}

		switch k.offset {
		case "earliest":
			opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
		default:
			opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
		}

		if k.group != "" {
			opts = append(opts, kgo.ConsumerGroup(k.group))
		}

		cl, err := kgo.NewClient(opts...)
		if err != nil {
			fmt.Printf("Error: cannot connect to %s — %v\n", k.broker, err)
			return
		}
		defer cl.Close()

		for {
			fetches := cl.PollFetches(k.ctx)
			if k.ctx.Err() != nil {
				return
			}
			fetches.EachRecord(func(r *kgo.Record) {
				rec := KafkaMultiTopicRecord{
					KafkaRecord: KafkaRecord{
						Value:     r.Value,
						Key:       r.Key,
						Offset:    r.Offset,
						Partition: r.Partition,
						Timestamp: r.Timestamp,
					},
					Topic: r.Topic,
				}
				select {
				case ch <- rec:
				case <-k.ctx.Done():
					return
				}
			})
		}
	}()
	return ch
}

// Stop cancels the consumer context.
func (k *KafkaMultiTopic) Stop() {
	k.cancel()
}

// EncodeKafkaKey encodes a Kafka message key as a UTF-8 string.
// Non-UTF-8 keys are base64-encoded with a "b64:" prefix per spec 12.5.
func EncodeKafkaKey(key []byte) string {
	if key == nil {
		return ""
	}
	if utf8.Valid(key) {
		return string(key)
	}
	return "b64:" + base64.StdEncoding.EncodeToString(key)
}
