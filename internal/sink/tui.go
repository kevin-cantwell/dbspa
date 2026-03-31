package sink

import (
	"fmt"
	"io"
	"slices"
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
	OrderBy     []OrderBySpec // optional ORDER BY specification for sorted output
	FPS         int           // target frames per second (default 15)

	// InputCount can be set to an external counter that the pipeline increments
	// to track total records read from the source (before filtering).
	InputCount *atomic.Int64

	mu            sync.Mutex
	rows          map[string]map[string]engine.Value
	rowOrder      []string
	pendingDelete map[string]bool // keys with retraction but no follow-up insertion yet
	linesDrawn    int
	recordCount   int
	dirty         bool
	done          chan struct{}
	wg            sync.WaitGroup
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
			frame := s.buildFrame()
			if frame != "" {
				io.WriteString(s.Writer, frame)
			}
		case <-s.done:
			frame := s.buildFrame()
			if frame != "" {
				io.WriteString(s.Writer, frame)
			}
			return
		}
	}
}

// buildFrame takes a snapshot of state under the lock, builds the frame string,
// and returns it. The actual terminal write happens OUTSIDE the lock.
func (s *TUISink) buildFrame() string {
	s.mu.Lock()

	needsRedraw := s.dirty
	if !needsRedraw && s.InputCount != nil && s.linesDrawn > 0 {
		needsRedraw = true
	}
	if !needsRedraw || (len(s.ColumnOrder) == 0 || len(s.rows) == 0) {
		// Nothing to draw, but still check if we need a footer-only update
		if !needsRedraw || len(s.rows) == 0 {
			s.mu.Unlock()
			return ""
		}
	}

	// Apply pending deletions — these are retractions that had no follow-up insertion
	for key := range s.pendingDelete {
		delete(s.rows, key)
		for i, k := range s.rowOrder {
			if k == key {
				s.rowOrder = append(s.rowOrder[:i], s.rowOrder[i+1:]...)
				break
			}
		}
	}
	s.pendingDelete = nil

	// Snapshot state
	colOrder := s.ColumnOrder
	rowOrder := make([]string, len(s.rowOrder))
	copy(rowOrder, s.rowOrder)
	rows := make(map[string]map[string]engine.Value, len(s.rows))
	for k, v := range s.rows {
		row := make(map[string]engine.Value, len(v))
		for ck, cv := range v {
			row[ck] = cv
		}
		rows[k] = row
	}
	nGroups := len(s.rows)
	recCount := s.recordCount
	linesDrawn := s.linesDrawn
	var inputCount int64
	if s.InputCount != nil {
		inputCount = s.InputCount.Load()
	}
	orderBy := s.OrderBy
	s.dirty = false

	s.mu.Unlock()

	// Sort rows by ORDER BY specification
	if len(orderBy) > 0 {
		slices.SortFunc(rowOrder, func(a, b string) int {
			rowA := rows[a]
			rowB := rows[b]
			for _, spec := range orderBy {
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

	// Build frame string outside the lock
	widths := make([]int, len(colOrder))
	for i, col := range colOrder {
		widths[i] = len(col)
	}
	for _, key := range rowOrder {
		row, ok := rows[key]
		if !ok {
			continue
		}
		for i, col := range colOrder {
			val := "NULL"
			if v, ok := row[col]; ok && !v.IsNull() {
				val = v.String()
			}
			if len(val) > widths[i] {
				widths[i] = len(val)
			}
		}
	}

	var buf strings.Builder
	buf.Grow(4096)

	// Move cursor to start of previous draw and clear
	if linesDrawn > 0 {
		fmt.Fprintf(&buf, "\033[%dA", linesDrawn)
	}
	buf.WriteString("\r\033[J")

	var lines int

	// Header
	for i, col := range colOrder {
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
	for _, key := range rowOrder {
		row, ok := rows[key]
		if !ok {
			continue
		}
		for i, col := range colOrder {
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
	if inputCount > 0 {
		fmt.Fprintf(&buf, "(%d groups | %d matched | %d input)\n", nGroups, recCount, inputCount)
	} else {
		fmt.Fprintf(&buf, "(%d groups | %d accumulated)\n", nGroups, recCount)
	}
	lines++

	// Update linesDrawn under lock
	s.mu.Lock()
	s.linesDrawn = lines
	s.mu.Unlock()

	return buf.String()
}

// Write processes a changelog record, updating the in-memory table.
func (s *TUISink) Write(rec engine.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.recordCount++
	key := s.rowKey(rec)

	if rec.Weight < 0 {
		// Mark as pending deletion — don't remove yet.
		// If the next Write for this key is an insertion (retraction+insertion pair),
		// the insertion will overwrite the values and clear the pending flag.
		// If no insertion follows, buildFrame() will clean up.
		if s.pendingDelete == nil {
			s.pendingDelete = make(map[string]bool)
		}
		s.pendingDelete[key] = true
	} else {
		// Insertion: either a new key or an update (clears pending deletion)
		delete(s.pendingDelete, key)
		if _, exists := s.rows[key]; !exists {
			s.rowOrder = append(s.rowOrder, key)
		}
		s.rows[key] = make(map[string]engine.Value, len(rec.Columns))
		for k, v := range rec.Columns {
			s.rows[key][k] = v
		}
	}
	s.dirty = true

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

func padRight(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}
