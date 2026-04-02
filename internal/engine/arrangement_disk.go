package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/kevin-cantwell/folddb/internal/sql/ast"
)

// DiskArrangement is a hybrid arrangement that keeps hot data in memory
// and spills cold data to Badger when the in-memory buffer exceeds a
// configurable threshold. It implements ArrangementStore and is transparent
// to the DD join operator.
//
// The Badger DB is created lazily — only when the first spill happens.
// When memLimit is 0, the arrangement is purely in-memory and Badger
// is never opened.
type DiskArrangement struct {
	mu       sync.RWMutex
	mem      *Arrangement // in-memory buffer (existing type)
	db       *badger.DB   // disk-backed store (nil until first spill)
	memLimit int          // max records in memory before spill (0 = unlimited)
	diskDir  string       // directory for Badger data
	keyExpr  ast.Expr     // key expression (same as mem's)
}

// NewDiskArrangement creates a new hybrid arrangement.
// If memLimit is 0, the arrangement is purely in-memory (Badger is never used).
// diskDir is the directory where Badger will store its data (created on first spill).
func NewDiskArrangement(keyExpr ast.Expr, memLimit int, diskDir string) *DiskArrangement {
	return &DiskArrangement{
		mem:      NewArrangement(keyExpr),
		memLimit: memLimit,
		diskDir:  diskDir,
		keyExpr:  keyExpr,
	}
}

// Apply merges a batch of Z-set deltas into the arrangement.
// Deltas are always applied to the in-memory buffer first. If the memory
// exceeds the threshold, the oldest entries are spilled to disk.
func (d *DiskArrangement) Apply(delta Batch) Batch {
	d.mu.Lock()
	defer d.mu.Unlock()

	applied := d.mem.Apply(delta)

	if d.memLimit > 0 {
		d.maybeSpill()
	}

	return applied
}

// LookupValue returns all entries matching the given key value, merging
// results from both the in-memory buffer and disk.
func (d *DiskArrangement) LookupValue(key Value) []Record {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if key.IsNull() {
		return nil
	}

	return d.lookupMerged(key)
}

// LookupValueUnsafe returns entries matching the key value without locking.
// Only safe when the caller guarantees no concurrent writes.
func (d *DiskArrangement) LookupValueUnsafe(key Value) []Record {
	if key.IsNull() {
		return nil
	}
	return d.lookupMerged(key)
}

// Lookup returns all entries matching the given key value.
func (d *DiskArrangement) Lookup(key Value) []Record {
	return d.LookupValue(key)
}

// lookupMerged returns entries from both memory and disk for the given key.
func (d *DiskArrangement) lookupMerged(key Value) []Record {
	memResults := d.mem.lookupValue(key)

	if d.db == nil {
		return memResults
	}

	diskResults := d.scanDisk(d.keyString(key))
	if len(diskResults) == 0 {
		return memResults
	}
	if len(memResults) == 0 {
		return diskResults
	}
	return append(memResults, diskResults...)
}

// EvictBefore removes all entries with Timestamp before the given cutoff
// from both memory and disk. Returns evicted entries with negated weights.
func (d *DiskArrangement) EvictBefore(cutoff time.Time) Batch {
	d.mu.Lock()
	defer d.mu.Unlock()

	evicted := d.mem.EvictBefore(cutoff)

	if d.db != nil {
		diskEvicted := d.evictDiskBefore(cutoff)
		evicted = append(evicted, diskEvicted...)
	}

	return evicted
}

// ColumnNames returns all column names seen in this arrangement.
func (d *DiskArrangement) ColumnNames() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.mem.ColumnNames()
}

// Close closes the Badger DB and removes the data directory.
func (d *DiskArrangement) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.db != nil {
		if err := d.db.Close(); err != nil {
			return err
		}
		d.db = nil
		// Clean up the data directory (ephemeral by default)
		_ = os.RemoveAll(d.diskDir)
	}
	return nil
}

// memRecordCount returns the approximate number of records in memory.
// Must be called with at least a read lock held.
func (d *DiskArrangement) memRecordCount() int {
	count := 0
	d.mem.mu.RLock()
	defer d.mem.mu.RUnlock()
	if d.mem.useIntKey {
		for _, entries := range d.mem.intIndex {
			count += len(entries)
		}
	} else if d.mem.strIndex != nil {
		for _, entries := range d.mem.strIndex {
			count += len(entries)
		}
	}
	return count
}

// maybeSpill checks if the in-memory buffer exceeds the threshold and
// spills the oldest entries to disk. Must be called with the write lock held.
func (d *DiskArrangement) maybeSpill() {
	count := d.memRecordCount()
	if count <= d.memLimit {
		return
	}

	// Collect all records with their keys and timestamps for sorting
	type keyedRecord struct {
		keyStr    string
		record    Record
		timestamp time.Time
	}

	var all []keyedRecord

	d.mem.mu.RLock()
	if d.mem.useIntKey {
		for k, entries := range d.mem.intIndex {
			ks := fmt.Sprintf("%d", k)
			for _, rec := range entries {
				all = append(all, keyedRecord{keyStr: ks, record: rec, timestamp: rec.Timestamp})
			}
		}
	} else if d.mem.strIndex != nil {
		for k, entries := range d.mem.strIndex {
			for _, rec := range entries {
				all = append(all, keyedRecord{keyStr: k, record: rec, timestamp: rec.Timestamp})
			}
		}
	}
	d.mem.mu.RUnlock()

	// Sort by timestamp — oldest first
	sort.Slice(all, func(i, j int) bool {
		return all[i].timestamp.Before(all[j].timestamp)
	})

	// Spill enough to get below threshold
	spillCount := count - d.memLimit
	if spillCount <= 0 {
		return
	}
	if spillCount > len(all) {
		spillCount = len(all)
	}

	toSpill := all[:spillCount]

	// Ensure Badger is open
	if d.db == nil {
		db, err := d.openBadger()
		if err != nil {
			// If we can't open Badger, just keep everything in memory
			return
		}
		d.db = db
	}

	// Write spilled records to Badger
	err := d.db.Update(func(txn *badger.Txn) error {
		for _, kr := range toSpill {
			fp := recordColumnsFingerprint(kr.record)
			bkey := badgerKey(kr.keyStr, fp)
			val, err := marshalRecord(kr.record)
			if err != nil {
				continue
			}
			if err := txn.Set(bkey, val); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return // keep in memory if disk write fails
	}

	// Remove spilled records from memory
	d.mem.mu.Lock()
	for _, kr := range toSpill {
		fp := recordColumnsFingerprint(kr.record)
		if d.mem.useIntKey {
			// Parse int key back
			var intKey int64
			fmt.Sscanf(kr.keyStr, "%d", &intKey)
			entries := d.mem.intIndex[intKey]
			for i, existing := range entries {
				if recordColumnsFingerprint(existing) == fp {
					d.mem.intIndex[intKey] = append(entries[:i], entries[i+1:]...)
					if len(d.mem.intIndex[intKey]) == 0 {
						delete(d.mem.intIndex, intKey)
					}
					break
				}
			}
		} else if d.mem.strIndex != nil {
			entries := d.mem.strIndex[kr.keyStr]
			for i, existing := range entries {
				if recordColumnsFingerprint(existing) == fp {
					d.mem.strIndex[kr.keyStr] = append(entries[:i], entries[i+1:]...)
					if len(d.mem.strIndex[kr.keyStr]) == 0 {
						delete(d.mem.strIndex, kr.keyStr)
					}
					break
				}
			}
		}
	}
	d.mem.mu.Unlock()
}

// openBadger opens or creates the Badger database at diskDir.
func (d *DiskArrangement) openBadger() (*badger.DB, error) {
	if err := os.MkdirAll(d.diskDir, 0o755); err != nil {
		return nil, fmt.Errorf("create badger dir: %w", err)
	}
	opts := badger.DefaultOptions(d.diskDir)
	opts.Logger = nil // suppress Badger's internal logging
	return badger.Open(opts)
}

// keyString returns the string representation of a Value for use as a Badger key prefix.
func (d *DiskArrangement) keyString(v Value) string {
	return v.String()
}

// badgerKey constructs the Badger key: "arr:" + keyString + ":" + fingerprint
func badgerKey(keyStr, fingerprint string) []byte {
	return []byte("arr:" + keyStr + ":" + fingerprint)
}

// badgerKeyPrefix returns the prefix for scanning all records with a given key.
func badgerKeyPrefix(keyStr string) []byte {
	return []byte("arr:" + keyStr + ":")
}

// scanDisk returns all records from Badger matching the given key string.
func (d *DiskArrangement) scanDisk(keyStr string) []Record {
	if d.db == nil {
		return nil
	}

	prefix := badgerKeyPrefix(keyStr)
	var results []Record

	err := d.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				rec, err := unmarshalRecord(val)
				if err != nil {
					return err
				}
				results = append(results, rec)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil
	}

	return results
}

// evictDiskBefore removes all entries from Badger with Timestamp before cutoff.
// Returns evicted entries with negated weights.
func (d *DiskArrangement) evictDiskBefore(cutoff time.Time) Batch {
	var evicted Batch
	var keysToDelete [][]byte

	// First pass: find entries to evict
	err := d.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte("arr:")
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek([]byte("arr:")); it.ValidForPrefix([]byte("arr:")); it.Next() {
			item := it.Item()
			key := make([]byte, len(item.Key()))
			copy(key, item.Key())

			err := item.Value(func(val []byte) error {
				rec, err := unmarshalRecord(val)
				if err != nil {
					return nil // skip corrupt entries
				}
				if rec.Timestamp.Before(cutoff) {
					keysToDelete = append(keysToDelete, key)
					retraction := rec
					retraction.Weight = -rec.Weight
					evicted = append(evicted, retraction)
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil
	}

	// Second pass: delete evicted entries
	if len(keysToDelete) > 0 {
		_ = d.db.Update(func(txn *badger.Txn) error {
			for _, key := range keysToDelete {
				if err := txn.Delete(key); err != nil {
					return err
				}
			}
			return nil
		})
	}

	return evicted
}

// Record serialization for Badger storage.
// Uses JSON with a type-tagged value encoding.

type serializedRecord struct {
	Columns   map[string]serializedValue `json:"c"`
	Timestamp time.Time                  `json:"t"`
	Weight    int                        `json:"w"`
}

type serializedValue struct {
	Type string `json:"T"`
	Val  string `json:"V"`
}

func marshalRecord(r Record) ([]byte, error) {
	sr := serializedRecord{
		Columns:   make(map[string]serializedValue, len(r.Columns)),
		Timestamp: r.Timestamp,
		Weight:    r.Weight,
	}
	for k, v := range r.Columns {
		sr.Columns[k] = serializeValue(v)
	}
	return json.Marshal(sr)
}

func unmarshalRecord(data []byte) (Record, error) {
	var sr serializedRecord
	if err := json.Unmarshal(data, &sr); err != nil {
		return Record{}, err
	}
	r := Record{
		Columns:   make(map[string]Value, len(sr.Columns)),
		Timestamp: sr.Timestamp,
		Weight:    sr.Weight,
	}
	for k, sv := range sr.Columns {
		v, err := deserializeValue(sv)
		if err != nil {
			return Record{}, fmt.Errorf("column %q: %w", k, err)
		}
		r.Columns[k] = v
	}
	return r, nil
}

func serializeValue(v Value) serializedValue {
	if v == nil || v.IsNull() {
		return serializedValue{Type: "NULL"}
	}
	return serializedValue{
		Type: v.Type(),
		Val:  v.String(),
	}
}

func deserializeValue(sv serializedValue) (Value, error) {
	switch sv.Type {
	case "NULL":
		return NullValue{}, nil
	case "BOOL":
		if sv.Val == "true" {
			return BoolValue{V: true}, nil
		}
		return BoolValue{V: false}, nil
	case "INT":
		n, err := parseInt64(sv.Val)
		if err != nil {
			return nil, err
		}
		return IntValue{V: n}, nil
	case "FLOAT":
		f, err := parseFloat64(sv.Val)
		if err != nil {
			return nil, err
		}
		return FloatValue{V: f}, nil
	case "TEXT":
		return TextValue{V: sv.Val}, nil
	case "TIMESTAMP":
		t, err := time.Parse(time.RFC3339Nano, sv.Val)
		if err != nil {
			return nil, err
		}
		return TimestampValue{V: t}, nil
	case "JSON":
		var j any
		if err := json.Unmarshal([]byte(sv.Val), &j); err != nil {
			return nil, err
		}
		return JsonValue{V: j}, nil
	default:
		return nil, fmt.Errorf("unknown value type: %s", sv.Type)
	}
}

func parseInt64(s string) (int64, error) {
	var n int64
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}

func parseFloat64(s string) (float64, error) {
	var f float64
	_, err := fmt.Sscanf(s, "%g", &f)
	return f, err
}
