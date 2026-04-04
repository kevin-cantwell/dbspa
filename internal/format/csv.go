package format

import (
	"encoding/csv"
	"fmt"
	"strings"
	"time"

	"github.com/kevin-cantwell/dbspa/internal/engine"
)

// CSVDecoder decodes CSV-formatted lines into Records.
// It maintains state for header detection: the first call to Decode (when
// header=true) captures column names from the header row and returns no record.
type CSVDecoder struct {
	Delimiter rune
	Quote     rune
	Header    bool

	// columns holds the column names derived from the header row,
	// or generated names ("1", "2", ...) when Header is false.
	columns []string
	// headerConsumed tracks whether the header row has been consumed.
	headerConsumed bool
}

// Decode parses a single CSV line into a Record. If Header is true and this is
// the first line, it captures column names and returns an empty record with a
// sentinel error (ErrHeaderRow) so the caller can skip it.
func (d *CSVDecoder) Decode(data []byte) (engine.Record, error) {
	line := string(data)
	if len(strings.TrimSpace(line)) == 0 {
		return engine.Record{}, fmt.Errorf("empty CSV line")
	}

	r := csv.NewReader(strings.NewReader(line))
	r.Comma = d.Delimiter
	if d.Quote != 0 {
		// Go's csv package doesn't support configurable quote char directly.
		// The default is '"' which covers the common case.
		// For non-standard quotes, we'd need custom parsing; for v0, we support
		// the default double-quote only.
	}
	r.FieldsPerRecord = -1 // allow variable field count
	r.LazyQuotes = true

	fields, err := r.Read()
	if err != nil {
		return engine.Record{}, fmt.Errorf("CSV parse error: %w", err)
	}

	// Handle header row
	if d.Header && !d.headerConsumed {
		d.columns = make([]string, len(fields))
		for i, f := range fields {
			d.columns[i] = strings.TrimSpace(f)
		}
		d.headerConsumed = true
		return engine.Record{}, ErrHeaderRow
	}

	// Generate column names if needed (1-based integer strings: "1", "2", ...)
	if d.columns == nil {
		d.columns = make([]string, len(fields))
		for i := range fields {
			d.columns[i] = fmt.Sprintf("%d", i+1)
		}
	}

	cols := make(map[string]engine.Value, len(fields))
	for i, f := range fields {
		var name string
		if i < len(d.columns) {
			name = d.columns[i]
		} else {
			name = fmt.Sprintf("%d", i+1)
		}
		cols[name] = engine.TextValue{V: f}
	}

	return engine.Record{
		Columns:   cols,
		Timestamp: time.Now(),
		Weight:      1,
	}, nil
}

// ErrHeaderRow is a sentinel error indicating the decoded line was a header row.
var ErrHeaderRow = fmt.Errorf("header row")
