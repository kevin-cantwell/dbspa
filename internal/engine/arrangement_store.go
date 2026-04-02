package engine

import "time"

// ArrangementStore is the interface for indexed Z-set arrangements used by
// the DD join operator. Both the in-memory Arrangement and the hybrid
// DiskArrangement implement this interface, making the join operator
// transparent to the storage backend.
type ArrangementStore interface {
	// Apply merges a batch of Z-set deltas into the arrangement.
	// Returns the delta records that were successfully applied.
	Apply(delta Batch) Batch

	// LookupValue returns all entries matching the given key value (with locking).
	LookupValue(key Value) []Record

	// LookupValueUnsafe returns entries matching the key value without locking.
	// Only safe when the caller guarantees no concurrent writes.
	LookupValueUnsafe(key Value) []Record

	// Lookup returns all entries matching the given key value (alias for LookupValue).
	Lookup(key Value) []Record

	// EvictBefore removes all entries with Timestamp before the given cutoff.
	// Returns evicted entries with negated weights for retraction propagation.
	EvictBefore(cutoff time.Time) Batch

	// ColumnNames returns all column names seen in this arrangement.
	ColumnNames() []string

	// Close releases any resources held by the arrangement.
	Close() error
}
