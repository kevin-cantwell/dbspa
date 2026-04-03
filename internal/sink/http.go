package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/kevin-cantwell/folddb/internal/engine"
)

// HTTPSink maintains an in-memory result set and serves it via HTTP.
// It implements the Sink interface.
type HTTPSink struct {
	ColumnOrder []string
	OrderBy     []OrderBySpec
	Port        int

	mu       sync.RWMutex
	rows     map[string]map[string]engine.Value
	rowOrder []string
	stats    struct {
		recordCount int64
		startTime   time.Time
		groupCount  int
	}

	// SSE subscribers
	subs   map[chan string]struct{}
	subsMu sync.Mutex

	server *http.Server
}

// Start begins the HTTP server in a background goroutine.
func (s *HTTPSink) Start() {
	s.rows = make(map[string]map[string]engine.Value)
	s.subs = make(map[chan string]struct{})
	s.stats.startTime = time.Now()

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleResults)
	mux.HandleFunc("/stream", s.handleStream)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/schema", s.handleSchema)

	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.Port),
		Handler: mux,
	}

	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("HTTP server error: %v\n", err)
		}
	}()
}

// Write processes a changelog record, updating the in-memory result set.
// It also notifies all SSE subscribers of the change.
func (s *HTTPSink) Write(rec engine.Record) error {
	s.mu.Lock()

	s.stats.recordCount++
	key := s.rowKey(rec)

	if rec.Weight < 0 {
		delete(s.rows, key)
		for i, k := range s.rowOrder {
			if k == key {
				s.rowOrder = append(s.rowOrder[:i], s.rowOrder[i+1:]...)
				break
			}
		}
	} else {
		if _, exists := s.rows[key]; !exists {
			s.rowOrder = append(s.rowOrder, key)
		}
		s.rows[key] = make(map[string]engine.Value, len(rec.Columns))
		for k, v := range rec.Columns {
			s.rows[key][k] = v
		}
	}
	s.stats.groupCount = len(s.rows)

	s.mu.Unlock()

	// Build SSE event from the record
	event := s.buildSSEEvent(rec)

	s.subsMu.Lock()
	for ch := range s.subs {
		select {
		case ch <- event:
		default:
			// Drop if subscriber is slow
		}
	}
	s.subsMu.Unlock()

	return nil
}

// Close shuts down the HTTP server gracefully.
func (s *HTTPSink) Close() error {
	// Close all SSE subscriber channels
	s.subsMu.Lock()
	for ch := range s.subs {
		close(ch)
		delete(s.subs, ch)
	}
	s.subsMu.Unlock()

	if s.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.server.Shutdown(ctx)
	}
	return nil
}

func (s *HTTPSink) handleResults(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	s.mu.RLock()
	rows := s.snapshotRows()
	s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(rows)
}

func (s *HTTPSink) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// Flush headers immediately so the client sees them
	flusher.Flush()

	ch := make(chan string, 64)
	s.subsMu.Lock()
	s.subs[ch] = struct{}{}
	s.subsMu.Unlock()

	defer func() {
		s.subsMu.Lock()
		delete(s.subs, ch)
		s.subsMu.Unlock()
	}()

	ctx := r.Context()
	for {
		select {
		case event, ok := <-ch:
			if !ok {
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", event)
			flusher.Flush()
		case <-ctx.Done():
			return
		}
	}
}

func (s *HTTPSink) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	uptime := time.Since(s.stats.startTime).Truncate(time.Second).String()
	records := s.stats.recordCount
	groups := s.stats.groupCount
	s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":  "ok",
		"uptime":  uptime,
		"records": records,
		"groups":  groups,
	})
}

func (s *HTTPSink) handleSchema(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	cols := make([]map[string]string, len(s.ColumnOrder))
	for i, name := range s.ColumnOrder {
		typ := "TEXT" // default
		// Try to infer type from first row
		for _, row := range s.rows {
			if v, ok := row[name]; ok && !v.IsNull() {
				typ = v.Type()
			}
			break
		}
		cols[i] = map[string]string{"name": name, "type": typ}
	}
	s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"columns": cols,
	})
}

// snapshotRows returns the current result set as a JSON-serializable slice.
// Must be called with s.mu held (at least RLock).
func (s *HTTPSink) snapshotRows() []map[string]any {
	order := make([]string, len(s.rowOrder))
	copy(order, s.rowOrder)

	// Sort if ORDER BY is set
	if len(s.OrderBy) > 0 {
		slices.SortFunc(order, func(a, b string) int {
			rowA := s.rows[a]
			rowB := s.rows[b]
			for _, spec := range s.OrderBy {
				va := rowA[spec.Column]
				vb := rowB[spec.Column]
				c := CompareValues(va, vb)
				if spec.Desc {
					c = -c
				}
				if c != 0 {
					return c
				}
			}
			return 0
		})
	}

	result := make([]map[string]any, 0, len(order))
	for _, key := range order {
		row, ok := s.rows[key]
		if !ok {
			continue
		}
		m := make(map[string]any, len(s.ColumnOrder))
		for _, col := range s.ColumnOrder {
			if v, ok := row[col]; ok {
				m[col] = v.ToJSON()
			} else {
				m[col] = nil
			}
		}
		result = append(result, m)
	}
	return result
}

func (s *HTTPSink) buildSSEEvent(rec engine.Record) string {
	data := make(map[string]any, len(s.ColumnOrder))
	for _, col := range s.ColumnOrder {
		if v, ok := rec.Columns[col]; ok {
			data[col] = v.ToJSON()
		} else {
			data[col] = nil
		}
	}
	envelope := map[string]any{
		"weight": rec.Weight,
		"data":   data,
	}
	b, _ := json.Marshal(envelope)
	return string(b)
}

func (s *HTTPSink) rowKey(rec engine.Record) string {
	var parts []string
	for _, col := range s.ColumnOrder {
		if v, ok := rec.Columns[col]; ok {
			parts = append(parts, v.String())
		} else {
			parts = append(parts, "NULL")
		}
	}
	return strings.Join(parts, "\x00")
}
