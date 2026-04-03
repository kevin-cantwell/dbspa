//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	dbspaBinary string
	buildOnce    sync.Once
	buildErr     error
)

const (
	kafkaBroker    = "localhost:9092"
	defaultTimeout = 30 * time.Second
)

// TestMain builds the dbspa binary once before all integration tests.
func TestMain(m *testing.M) {
	buildOnce.Do(func() {
		// Build the binary into a temp location
		tmpDir, err := os.MkdirTemp("", "dbspa-integration-*")
		if err != nil {
			buildErr = fmt.Errorf("cannot create temp dir: %w", err)
			return
		}
		dbspaBinary = filepath.Join(tmpDir, "dbspa")
		cmd := exec.Command("go", "build", "-o", dbspaBinary, "./cmd/dbspa")
		cmd.Dir = repoRoot()
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			buildErr = fmt.Errorf("cannot build dbspa: %w", err)
			return
		}
	})

	code := m.Run()

	// Cleanup
	if dbspaBinary != "" {
		os.RemoveAll(filepath.Dir(dbspaBinary))
	}
	os.Exit(code)
}

// repoRoot returns the repository root directory.
func repoRoot() string {
	// Walk up from the test directory to find go.mod
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	// Fallback: assume test/integration is two levels deep
	wd, _ := os.Getwd()
	return filepath.Join(wd, "..", "..")
}

// skipIfShort skips tests when -short flag is set (for CI).
func skipIfShort(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping slow integration test in short mode")
	}
}

// requireBuild fails the test if the dbspa binary could not be built.
func requireBuild(t *testing.T) {
	t.Helper()
	if buildErr != nil {
		t.Fatalf("dbspa build failed: %v", buildErr)
	}
}

// waitForKafka polls until the Kafka broker is reachable or the timeout expires.
func waitForKafka(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", kafkaBroker, 2*time.Second)
		if err == nil {
			conn.Close()
			// Also verify we can list topics
			cl, err := kgo.NewClient(kgo.SeedBrokers(kafkaBroker))
			if err == nil {
				adm := kadm.NewClient(cl)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err := adm.ListTopics(ctx)
				cancel()
				cl.Close()
				if err == nil {
					return
				}
			}
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatal("Kafka broker not reachable at", kafkaBroker)
}

// uniqueTopic generates a unique topic name for test isolation.
func uniqueTopic(t *testing.T, prefix string) string {
	t.Helper()
	// Use test name + timestamp for uniqueness
	name := strings.ReplaceAll(t.Name(), "/", "-")
	name = strings.ReplaceAll(name, " ", "-")
	return fmt.Sprintf("%s-%s-%d", prefix, name, time.Now().UnixNano()%1000000)
}

// createTopic creates a Kafka topic with the given number of partitions.
func createTopic(t *testing.T, topic string, partitions int32) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(kafkaBroker))
	if err != nil {
		t.Fatalf("cannot create kafka client: %v", err)
	}
	defer cl.Close()

	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := adm.CreateTopics(ctx, partitions, 1, nil, topic)
	if err != nil {
		t.Fatalf("cannot create topic %s: %v", topic, err)
	}
	for _, r := range resp {
		if r.Err != nil {
			t.Fatalf("topic creation error for %s: %v", r.Topic, r.Err)
		}
	}

	// Clean up topic after test
	t.Cleanup(func() {
		cl2, err := kgo.NewClient(kgo.SeedBrokers(kafkaBroker))
		if err != nil {
			return
		}
		defer cl2.Close()
		adm2 := kadm.NewClient(cl2)
		ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel2()
		adm2.DeleteTopics(ctx2, topic)
	})
}

// produceMessages publishes messages to a Kafka topic.
func produceMessages(t *testing.T, topic string, messages []string) {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBroker),
		kgo.DefaultProduceTopic(topic),
	)
	if err != nil {
		t.Fatalf("cannot create kafka producer: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	for _, msg := range messages {
		r := &kgo.Record{Value: []byte(msg)}
		results := cl.ProduceSync(ctx, r)
		if results.FirstErr() != nil {
			t.Fatalf("cannot produce message: %v", results.FirstErr())
		}
	}
}

// produceKeyedMessages publishes keyed messages to a Kafka topic.
func produceKeyedMessages(t *testing.T, topic string, messages []keyedMessage) {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBroker),
		kgo.DefaultProduceTopic(topic),
	)
	if err != nil {
		t.Fatalf("cannot create kafka producer: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	for _, msg := range messages {
		r := &kgo.Record{
			Key:   []byte(msg.Key),
			Value: []byte(msg.Value),
		}
		results := cl.ProduceSync(ctx, r)
		if results.FirstErr() != nil {
			t.Fatalf("cannot produce keyed message: %v", results.FirstErr())
		}
	}
}

type keyedMessage struct {
	Key   string
	Value string
}

// produceToPartition publishes a message to a specific partition.
func produceToPartition(t *testing.T, topic string, partition int32, messages []string) {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBroker),
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatalf("cannot create kafka producer: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	for _, msg := range messages {
		r := &kgo.Record{
			Value:     []byte(msg),
			Partition: partition,
		}
		results := cl.ProduceSync(ctx, r)
		if results.FirstErr() != nil {
			t.Fatalf("cannot produce to partition %d: %v", partition, results.FirstErr())
		}
	}
}

// runDBSPA executes the dbspa binary with the given SQL and options.
// Returns captured stdout, stderr, and any error.
func runDBSPA(t *testing.T, sql string, opts ...string) (stdout, stderr string, err error) {
	t.Helper()
	requireBuild(t)

	args := append([]string{sql}, opts...)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, dbspaBinary, args...)
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	err = cmd.Run()
	return outBuf.String(), errBuf.String(), err
}

// runDBSPAWithTimeout is like runDBSPA but with a custom timeout.
func runDBSPAWithTimeout(t *testing.T, timeout time.Duration, sql string, opts ...string) (stdout, stderr string, err error) {
	t.Helper()
	requireBuild(t)

	args := append([]string{sql}, opts...)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, dbspaBinary, args...)
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	err = cmd.Run()
	return outBuf.String(), errBuf.String(), err
}

// runDBSPAWithStdin executes the dbspa binary with the given SQL and stdin input.
func runDBSPAWithStdin(t *testing.T, sql string, input string, opts ...string) (stdout string, err error) {
	t.Helper()
	requireBuild(t)

	args := append([]string{sql}, opts...)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, dbspaBinary, args...)
	cmd.Stdin = strings.NewReader(input)
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	err = cmd.Run()
	if err != nil {
		return outBuf.String(), fmt.Errorf("%w: stderr=%s", err, errBuf.String())
	}
	return outBuf.String(), nil
}

// runDBSPAStreaming starts dbspa and returns the process, allowing callers to
// read output incrementally or kill the process.
func runDBSPAStreaming(t *testing.T, sql string, opts ...string) *exec.Cmd {
	t.Helper()
	requireBuild(t)

	args := append([]string{sql}, opts...)
	cmd := exec.Command(dbspaBinary, args...)
	return cmd
}

// outputLines splits stdout output into non-empty lines.
func outputLines(output string) []string {
	var lines []string
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// testdataPath returns the absolute path to a file in testdata/.
func testdataPath(name string) string {
	return filepath.Join(repoRoot(), "testdata", name)
}
