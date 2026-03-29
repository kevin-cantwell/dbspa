// Package format provides decoders that convert raw bytes into engine Records.
package format

import "github.com/kevin-cantwell/folddb/internal/engine"

// Decoder converts raw bytes into an engine Record.
type Decoder interface {
	// Decode parses raw bytes into a Record.
	Decode(data []byte) (engine.Record, error)
}
