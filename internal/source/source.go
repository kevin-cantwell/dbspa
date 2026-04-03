// Package source provides stream source implementations for DBSPA.
package source

// Source reads raw records from an external system and sends them as byte
// slices over a channel. The channel is closed when the source is exhausted.
type Source interface {
	// Read starts reading and returns a channel of raw record bytes.
	// The channel is closed when the source is exhausted (e.g., stdin EOF).
	Read() <-chan []byte
}
