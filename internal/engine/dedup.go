package engine

import (
	"container/list"
	"sync"
	"time"

	"github.com/kevin-cantwell/folddb/internal/sql/ast"
)

// DedupFilter implements bounded LRU deduplication.
// Records with a previously-seen key within the WITHIN duration are dropped.
// Retractions (diff=-1) always pass through.
type DedupFilter struct {
	mu       sync.Mutex
	keyExpr  ast.Expr
	within   time.Duration // 0 means no time limit
	capacity int

	// LRU cache: map key string -> list element
	cache map[string]*list.Element
	order *list.List // front = most recent, back = oldest
}

type dedupEntry struct {
	key     string
	addedAt time.Time
}

// NewDedupFilter creates a new dedup filter.
func NewDedupFilter(keyExpr ast.Expr, within time.Duration, capacity int) *DedupFilter {
	if capacity <= 0 {
		capacity = 100000 // spec default
	}
	return &DedupFilter{
		keyExpr:  keyExpr,
		within:   within,
		capacity: capacity,
		cache:    make(map[string]*list.Element),
		order:    list.New(),
	}
}

// ShouldDrop returns true if the record should be dropped (duplicate).
// Retractions always pass through (returns false).
func (df *DedupFilter) ShouldDrop(rec Record) bool {
	// Retractions bypass dedup entirely (spec 3.8)
	if rec.Weight < 0 {
		return false
	}

	// Evaluate dedup key
	val, err := Eval(df.keyExpr, rec)
	if err != nil {
		// Can't evaluate key — pass through
		return false
	}

	// NULL keys are treated as equal (spec 3.8)
	var keyStr string
	if val.IsNull() {
		keyStr = "__NULL__"
	} else {
		keyStr = val.String()
	}

	df.mu.Lock()
	defer df.mu.Unlock()

	now := time.Now()

	// Check if key exists and is within the WITHIN window
	if elem, ok := df.cache[keyStr]; ok {
		entry := elem.Value.(*dedupEntry)
		if df.within == 0 || now.Sub(entry.addedAt) <= df.within {
			// Duplicate — move to front (LRU touch) and drop
			df.order.MoveToFront(elem)
			entry.addedAt = now
			return true
		}
		// Expired — remove and re-add below
		df.order.Remove(elem)
		delete(df.cache, keyStr)
	}

	// Not a duplicate — add to cache
	entry := &dedupEntry{key: keyStr, addedAt: now}
	elem := df.order.PushFront(entry)
	df.cache[keyStr] = elem

	// Evict if over capacity
	for df.order.Len() > df.capacity {
		oldest := df.order.Back()
		if oldest == nil {
			break
		}
		oldEntry := oldest.Value.(*dedupEntry)
		df.order.Remove(oldest)
		delete(df.cache, oldEntry.key)
	}

	return false
}
