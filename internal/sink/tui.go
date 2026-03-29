package sink

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kevin-cantwell/folddb/internal/engine"
)

// TUISink displays accumulating query results as a table that redraws in place.
// Redraws are throttled to a fixed frame rate to avoid terminal bottlenecks.
type TUISink struct {
	Writer      io.Writer
	ColumnOrder []string
	FPS         int // target frames per second (default 15)

	// InputCount can be set to an external counter that the pipeline increments
	// to track total records read from the source (before filtering).
	InputCount *atomic.Int64

	mu          sync.Mutex
	rows        map[string]map[string]engine.Value
	rowOrder    []string
	linesDrawn  int
	recordCount int // records that reached the accumulator (after filtering)
	dirty       bool
	done        chan struct{}
	wg          sync.WaitGroup
}

// Start begins the background redraw loop. Must be called before Write.
func (s *TUISink) Start() {
	if s.FPS <= 0 {
		s.FPS = 15
	}
	s.done = make(chan struct{})
	s.rows = make(map[string]map[string]engine.Value)
	s.wg.Add(1)
	go s.renderLoop()
}

func (s *TUISink) renderLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(time.Second / time.Duration(s.FPS))
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.mu.Lock()
			if s.dirty {
				s.redraw()
				s.dirty = false
			}
			s.mu.Unlock()
		case <-s.done:
			// Final redraw
			s.mu.Lock()
			s.redraw()
			s.dirty = false
			s.mu.Unlock()
			return
		}
	}
}

// Write processes a changelog record, updating the in-memory table.
// Does NOT redraw — the background loop handles rendering.
func (s *TUISink) Write(rec engine.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.recordCount++
	key := s.rowKey(rec)

	if rec.Diff < 0 {
		delete(s.rows, key)
		for i, k := range s.rowOrder {
			if k == key {
				s.rowOrder = append(s.rowOrder[:i], s.rowOrder[i+1:]...)
				break
			}
		}
		s.dirty = true
	} else {
		if _, exists := s.rows[key]; !exists {
			s.rowOrder = append(s.rowOrder, key)
		}
		s.rows[key] = make(map[string]engine.Value, len(rec.Columns))
		for k, v := range rec.Columns {
			s.rows[key][k] = v
		}
		s.dirty = true
	}

	return nil
}

// Close stops the render loop and writes a final newline.
func (s *TUISink) Close() error {
	if s.done != nil {
		close(s.done)
		s.wg.Wait()
	}
	fmt.Fprintln(s.Writer)
	return nil
}

func (s *TUISink) rowKey(rec engine.Record) string {
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

func (s *TUISink) redraw() {
	if len(s.ColumnOrder) == 0 || len(s.rows) == 0 {
		return
	}

	// Calculate column widths
	widths := make([]int, len(s.ColumnOrder))
	for i, col := range s.ColumnOrder {
		widths[i] = len(col)
	}
	for _, key := range s.rowOrder {
		row, ok := s.rows[key]
		if !ok {
			continue
		}
		for i, col := range s.ColumnOrder {
			val := "NULL"
			if v, ok := row[col]; ok && !v.IsNull() {
				val = v.String()
			}
			if len(val) > widths[i] {
				widths[i] = len(val)
			}
		}
	}

	// Build the entire frame as a single string to minimize write syscalls
	var buf strings.Builder

	// Move cursor to start of previous draw and clear everything below
	if s.linesDrawn > 0 {
		fmt.Fprintf(&buf, "\033[%dA", s.linesDrawn) // move up
	}
	buf.WriteString("\r\033[J") // carriage return + clear from cursor to end of screen

	var lines int

	// Header
	for i, col := range s.ColumnOrder {
		if i > 0 {
			buf.WriteString(" | ")
		}
		buf.WriteString(padRight(strings.ToUpper(col), widths[i]))
	}
	buf.WriteByte('\n')
	lines++

	// Separator
	for i, w := range widths {
		if i > 0 {
			buf.WriteString("-+-")
		}
		buf.WriteString(strings.Repeat("-", w))
	}
	buf.WriteByte('\n')
	lines++

	// Rows
	for _, key := range s.rowOrder {
		row, ok := s.rows[key]
		if !ok {
			continue
		}
		for i, col := range s.ColumnOrder {
			if i > 0 {
				buf.WriteString(" | ")
			}
			val := "NULL"
			if v, ok := row[col]; ok && !v.IsNull() {
				val = v.String()
			}
			buf.WriteString(padRight(val, widths[i]))
		}
		buf.WriteByte('\n')
		lines++
	}

	// Footer
	var inputCount int64
	if s.InputCount != nil {
		inputCount = s.InputCount.Load()
	}
	if inputCount > 0 {
		fmt.Fprintf(&buf, "(%d groups | %d matched | %d input)\n", len(s.rows), s.recordCount, inputCount)
	} else {
		fmt.Fprintf(&buf, "(%d groups | %d accumulated)\n", len(s.rows), s.recordCount)
	}
	lines++

	// Single write to terminal
	io.WriteString(s.Writer, buf.String())
	s.linesDrawn = lines
}

func padRight(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}
