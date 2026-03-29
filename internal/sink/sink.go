// Package sink provides output writers for FoldDB query results.
package sink

import "github.com/kevin-cantwell/folddb/internal/engine"

// Sink writes records to an output destination.
type Sink interface {
	// Write writes a single record to the output.
	Write(rec engine.Record) error
	// Close flushes any buffered output and releases resources.
	Close() error
}
