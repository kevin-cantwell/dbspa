package engine

import (
	"sync"

	"github.com/kevin-cantwell/folddb/internal/sql/ast"
)

// Arrangement is an indexed Z-set that supports efficient lookup by key.
// It maintains weighted entries grouped by a join key expression. This is
// the core data structure for differential dataflow joins — both sides of
// the join maintain an arrangement, and deltas are joined against the
// opposite side's arrangement.
type Arrangement struct {
	mu      sync.RWMutex
	// index maps join key (as string) → list of weighted records.
	index   map[string][]Record
	// keyExpr is the expression used to extract the join key from a record.
	keyExpr ast.Expr
	// columns tracks all column names seen, used for LEFT JOIN NULL generation.
	columns map[string]struct{}
}

// NewArrangement creates a new empty arrangement with the given key expression.
func NewArrangement(keyExpr ast.Expr) *Arrangement {
	return &Arrangement{
		index:   make(map[string][]Record),
		keyExpr: keyExpr,
		columns: make(map[string]struct{}),
	}
}

// Apply merges a batch of Z-set deltas into the arrangement.
// For each delta record, the weight is added to any existing entry with
// the same key and column fingerprint. If the net weight reaches zero,
// the entry is removed. This is Z-set algebra.
//
// Returns the delta records that were successfully applied (with their
// original weights, for join propagation).
func (a *Arrangement) Apply(delta Batch) Batch {
	a.mu.Lock()
	defer a.mu.Unlock()

	applied := make(Batch, 0, len(delta))
	for _, rec := range delta {
		key, err := Eval(a.keyExpr, rec)
		if err != nil || key.IsNull() {
			continue // NULLs don't join
		}
		keyStr := key.String()

		// Track columns
		for k := range rec.Columns {
			a.columns[k] = struct{}{}
		}

		fp := recordColumnsFingerprint(rec)
		entries := a.index[keyStr]
		merged := false
		for i, existing := range entries {
			if recordColumnsFingerprint(existing) == fp {
				newWeight := existing.Weight + rec.Weight
				if newWeight == 0 {
					// Remove entry — swap with last and truncate
					a.index[keyStr] = append(entries[:i], entries[i+1:]...)
				} else {
					entries[i].Weight = newWeight
				}
				merged = true
				break
			}
		}
		if !merged {
			a.index[keyStr] = append(a.index[keyStr], rec)
		}
		applied = append(applied, rec)
	}
	return applied
}

// Lookup returns all entries matching the given key value.
func (a *Arrangement) Lookup(keyValue Value) []Record {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if keyValue.IsNull() {
		return nil
	}
	return a.index[keyValue.String()]
}

// LookupString returns all entries matching the given key string.
func (a *Arrangement) LookupString(key string) []Record {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.index[key]
}

// ColumnNames returns all column names seen in this arrangement.
func (a *Arrangement) ColumnNames() []string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	names := make([]string, 0, len(a.columns))
	for k := range a.columns {
		names = append(names, k)
	}
	return names
}

// recordColumnsFingerprint returns a fingerprint based only on column values
// (excluding weight), suitable for identifying "the same record" in Z-set terms.
func recordColumnsFingerprint(r Record) string {
	return RecordFingerprint(Record{Columns: r.Columns, Weight: 1})
}
