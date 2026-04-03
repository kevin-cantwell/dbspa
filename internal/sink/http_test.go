package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/kevin-cantwell/dbspa/internal/engine"
)

func findFreePort() int {
	// Use port 0 to let the OS pick a free port, but since HTTPSink
	// takes a port number, we'll use high ephemeral ports.
	// For tests, just use a range unlikely to conflict.
	return 0
}

func startTestSink(t *testing.T, columns []string, port int) *HTTPSink {
	t.Helper()
	s := &HTTPSink{
		ColumnOrder: columns,
		Port:        port,
	}
	s.Start()
	// Give the server a moment to bind
	time.Sleep(50 * time.Millisecond)
	return s
}

func TestHTTPSink_Health(t *testing.T) {
	s := startTestSink(t, []string{"region", "count"}, 19081)
	defer s.Close()

	resp, err := http.Get("http://localhost:19081/health")
	if err != nil {
		t.Fatalf("GET /health failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("expected status ok, got %v", body["status"])
	}
}

func TestHTTPSink_ResultSet(t *testing.T) {
	s := startTestSink(t, []string{"region", "count"}, 19082)
	defer s.Close()

	// Write some records
	s.Write(engine.Record{
		Columns: map[string]engine.Value{
			"region": engine.TextValue{V: "us-east"},
			"count":  engine.IntValue{V: 100},
		},
		Weight: 1,
	})
	s.Write(engine.Record{
		Columns: map[string]engine.Value{
			"region": engine.TextValue{V: "us-west"},
			"count":  engine.IntValue{V: 200},
		},
		Weight: 1,
	})

	resp, err := http.Get("http://localhost:19082/")
	if err != nil {
		t.Fatalf("GET / failed: %v", err)
	}
	defer resp.Body.Close()

	var rows []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	// Check first row
	if rows[0]["region"] != "us-east" {
		t.Errorf("expected region us-east, got %v", rows[0]["region"])
	}
	// JSON numbers decode as float64
	if rows[0]["count"] != float64(100) {
		t.Errorf("expected count 100, got %v", rows[0]["count"])
	}
}

func TestHTTPSink_Retraction(t *testing.T) {
	s := startTestSink(t, []string{"region", "count"}, 19083)
	defer s.Close()

	// Insert
	s.Write(engine.Record{
		Columns: map[string]engine.Value{
			"region": engine.TextValue{V: "us-east"},
			"count":  engine.IntValue{V: 100},
		},
		Weight: 1,
	})

	// Retract
	s.Write(engine.Record{
		Columns: map[string]engine.Value{
			"region": engine.TextValue{V: "us-east"},
			"count":  engine.IntValue{V: 100},
		},
		Weight: -1,
	})

	// Re-insert with updated value
	s.Write(engine.Record{
		Columns: map[string]engine.Value{
			"region": engine.TextValue{V: "us-east"},
			"count":  engine.IntValue{V: 101},
		},
		Weight: 1,
	})

	resp, err := http.Get("http://localhost:19083/")
	if err != nil {
		t.Fatalf("GET / failed: %v", err)
	}
	defer resp.Body.Close()

	var rows []map[string]any
	json.NewDecoder(resp.Body).Decode(&rows)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after retract+reinsert, got %d", len(rows))
	}
	if rows[0]["count"] != float64(101) {
		t.Errorf("expected count 101, got %v", rows[0]["count"])
	}
}

func TestHTTPSink_Schema(t *testing.T) {
	s := startTestSink(t, []string{"region", "count"}, 19084)
	defer s.Close()

	// Write a record so types can be inferred
	s.Write(engine.Record{
		Columns: map[string]engine.Value{
			"region": engine.TextValue{V: "us-east"},
			"count":  engine.IntValue{V: 42},
		},
		Weight: 1,
	})

	resp, err := http.Get("http://localhost:19084/schema")
	if err != nil {
		t.Fatalf("GET /schema failed: %v", err)
	}
	defer resp.Body.Close()

	var body map[string]any
	json.NewDecoder(resp.Body).Decode(&body)

	cols, ok := body["columns"].([]any)
	if !ok {
		t.Fatalf("expected columns array, got %T", body["columns"])
	}
	if len(cols) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(cols))
	}
}

func TestHTTPSink_SSE(t *testing.T) {
	s := startTestSink(t, []string{"region", "count"}, 19095)
	defer s.Close()

	// Connect SSE client with a context we can cancel
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", "http://localhost:19095/stream", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /stream failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("expected text/event-stream, got %s", resp.Header.Get("Content-Type"))
	}

	// Write a record in a goroutine (after SSE is connected)
	go func() {
		time.Sleep(100 * time.Millisecond)
		s.Write(engine.Record{
			Columns: map[string]engine.Value{
				"region": engine.TextValue{V: "eu-west"},
				"count":  engine.IntValue{V: 500},
			},
			Weight: 1,
		})
	}()

	// Read the SSE event
	scanner := bufio.NewScanner(resp.Body)
	var eventData string
	done := make(chan struct{})
	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				eventData = strings.TrimPrefix(line, "data: ")
				close(done)
				return
			}
		}
	}()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timeout waiting for SSE event")
	}

	var event struct {
		Weight float64        `json:"weight"`
		Data   map[string]any `json:"data"`
	}
	if err := json.Unmarshal([]byte(eventData), &event); err != nil {
		t.Fatalf("failed to parse SSE event: %v", err)
	}
	if event.Weight != 1 {
		t.Errorf("expected weight 1, got %v", event.Weight)
	}
	if event.Data["region"] != "eu-west" {
		t.Errorf("expected region eu-west, got %v", event.Data["region"])
	}
}

func TestHTTPSink_OrderBy(t *testing.T) {
	s := &HTTPSink{
		ColumnOrder: []string{"name", "score"},
		OrderBy:     []OrderBySpec{{Column: "score", Desc: true}},
		Port:        19086,
	}
	s.Start()
	defer s.Close()
	time.Sleep(50 * time.Millisecond)

	for i, name := range []string{"alice", "bob", "charlie"} {
		s.Write(engine.Record{
			Columns: map[string]engine.Value{
				"name":  engine.TextValue{V: name},
				"score": engine.IntValue{V: int64((i + 1) * 10)},
			},
			Weight: 1,
		})
	}

	resp, err := http.Get("http://localhost:19086/")
	if err != nil {
		t.Fatalf("GET / failed: %v", err)
	}
	defer resp.Body.Close()

	var rows []map[string]any
	json.NewDecoder(resp.Body).Decode(&rows)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// Sorted DESC by score: charlie(30), bob(20), alice(10)
	names := []string{
		fmt.Sprintf("%v", rows[0]["name"]),
		fmt.Sprintf("%v", rows[1]["name"]),
		fmt.Sprintf("%v", rows[2]["name"]),
	}
	expected := []string{"charlie", "bob", "alice"}
	for i, n := range names {
		if n != expected[i] {
			t.Errorf("row %d: expected %s, got %s", i, expected[i], n)
		}
	}
}
