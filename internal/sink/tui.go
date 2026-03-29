package sink

import (
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/kevin-cantwell/folddb/internal/engine"
)

// TUISink displays accumulating query results as a table that redraws in place.
// Uses ANSI escape codes for cursor movement and clearing.
type TUISink struct {
	Writer      io.Writer
	ColumnOrder []string

	mu          sync.Mutex
	rows        map[string]map[string]engine.Value // composite key -> column values
	rowOrder    []string                            // insertion-order keys
	linesDrawn  int                                 // how many lines we drew last time
	recordCount int
}

// Write processes a changelog record, updating the in-memory table.
func (s *TUISink) Write(rec engine.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.rows == nil {
		s.rows = make(map[string]map[string]engine.Value)
	}

	s.recordCount++

	// Build a composite key from the non-aggregate column values
	key := s.rowKey(rec)

	if rec.Diff < 0 {
		// Retraction — remove the row
		delete(s.rows, key)
		for i, k := range s.rowOrder {
			if k == key {
				s.rowOrder = append(s.rowOrder[:i], s.rowOrder[i+1:]...)
				break
			}
		}
	} else {
		// Insertion — add or update the row
		if _, exists := s.rows[key]; !exists {
			s.rowOrder = append(s.rowOrder, key)
		}
		s.rows[key] = make(map[string]engine.Value, len(rec.Columns))
		for k, v := range rec.Columns {
			s.rows[key][k] = v
		}
	}

	// Redraw the table after each insertion (not on retraction alone,
	// since retractions are always immediately followed by insertions)
	if rec.Diff >= 0 {
		s.redraw()
	}

	return nil
}

// Close writes a final newline.
func (s *TUISink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Move cursor below the table
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

	// Move cursor up to clear previous draw
	if s.linesDrawn > 0 {
		fmt.Fprintf(s.Writer, "\033[%dA", s.linesDrawn)
		fmt.Fprintf(s.Writer, "\033[J") // clear from cursor to end of screen
	}

	var lines int

	// Header
	var header strings.Builder
	for i, col := range s.ColumnOrder {
		if i > 0 {
			header.WriteString(" | ")
		}
		header.WriteString(padRight(strings.ToUpper(col), widths[i]))
	}
	fmt.Fprintln(s.Writer, header.String())
	lines++

	// Separator
	var sep strings.Builder
	for i, w := range widths {
		if i > 0 {
			sep.WriteString("-+-")
		}
		sep.WriteString(strings.Repeat("-", w))
	}
	fmt.Fprintln(s.Writer, sep.String())
	lines++

	// Rows
	for _, key := range s.rowOrder {
		row, ok := s.rows[key]
		if !ok {
			continue
		}
		var line strings.Builder
		for i, col := range s.ColumnOrder {
			if i > 0 {
				line.WriteString(" | ")
			}
			val := "NULL"
			if v, ok := row[col]; ok && !v.IsNull() {
				val = v.String()
			}
			line.WriteString(padRight(val, widths[i]))
		}
		fmt.Fprintln(s.Writer, line.String())
		lines++
	}

	// Footer
	fmt.Fprintf(s.Writer, "(%d groups, %d records processed)\n", len(s.rows), s.recordCount)
	lines++

	s.linesDrawn = lines
}

func padRight(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}
