package engine

import (
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/kevin-cantwell/folddb/internal/sql/ast"
)

// Arrangement is an indexed Z-set that supports efficient lookup by key.
// It maintains weighted entries grouped by a join key expression. This is
// the core data structure for differential dataflow joins — both sides of
// the join maintain an arrangement, and deltas are joined against the
// opposite side's arrangement.
//
// For performance, it uses a dual-index strategy: if all keys seen so far
// are integers, it uses a map[int64][]Record (avoiding strconv.FormatInt
// and cheaper hash). If a non-integer key is seen, it migrates once to
// map[string][]Record.
type Arrangement struct {
	mu      sync.RWMutex
	// intIndex maps join key (as int64) → list of weighted records.
	// Used when all keys so far have been integers (useIntKey == true).
	intIndex map[int64][]Record
	// strIndex maps join key (as string) → list of weighted records.
	// Used as fallback when non-integer keys are present.
	strIndex map[string][]Record
	// keyExpr is the expression used to extract the join key from a record.
	keyExpr ast.Expr
	// useIntKey is true if all keys seen so far have been integers.
	useIntKey bool
	// decided is true once we've committed to an index type.
	decided bool
	// columns tracks all column names seen, used for LEFT JOIN NULL generation.
	columns map[string]struct{}
}

// NewArrangement creates a new empty arrangement with the given key expression.
func NewArrangement(keyExpr ast.Expr) *Arrangement {
	return &Arrangement{
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
		key, err := EvalKeyExpr(a.keyExpr, rec)
		if err != nil || key.IsNull() {
			continue // NULLs don't join
		}

		// Track columns
		for k := range rec.Columns {
			a.columns[k] = struct{}{}
		}

		fp := recordColumnsFingerprint(rec)

		// Decide which index to use based on the key type.
		if !a.decided {
			if _, ok := key.(IntValue); ok {
				a.useIntKey = true
				a.intIndex = make(map[int64][]Record)
			} else {
				a.useIntKey = false
				a.strIndex = make(map[string][]Record)
			}
			a.decided = true
		}

		if a.useIntKey {
			intKey, ok := valueToInt64(key)
			if !ok {
				// Non-integer key arrived: migrate intIndex → strIndex
				a.migrateToStrIndex()
				a.applyStr(key.String(), rec, fp)
			} else {
				a.applyInt(intKey, rec, fp)
			}
		} else {
			a.applyStr(key.String(), rec, fp)
		}
		applied = append(applied, rec)
	}
	return applied
}

// applyInt merges a record into the intIndex.
func (a *Arrangement) applyInt(key int64, rec Record, fp string) {
	entries := a.intIndex[key]
	for i, existing := range entries {
		if recordColumnsFingerprint(existing) == fp {
			newWeight := existing.Weight + rec.Weight
			if newWeight == 0 {
				a.intIndex[key] = append(entries[:i], entries[i+1:]...)
			} else {
				entries[i].Weight = newWeight
			}
			return
		}
	}
	a.intIndex[key] = append(a.intIndex[key], rec)
}

// applyStr merges a record into the strIndex.
func (a *Arrangement) applyStr(key string, rec Record, fp string) {
	entries := a.strIndex[key]
	for i, existing := range entries {
		if recordColumnsFingerprint(existing) == fp {
			newWeight := existing.Weight + rec.Weight
			if newWeight == 0 {
				a.strIndex[key] = append(entries[:i], entries[i+1:]...)
			} else {
				entries[i].Weight = newWeight
			}
			return
		}
	}
	a.strIndex[key] = append(a.strIndex[key], rec)
}

// migrateToStrIndex converts intIndex entries to strIndex. Called once when
// a non-integer key is first seen after integer keys.
func (a *Arrangement) migrateToStrIndex() {
	a.strIndex = make(map[string][]Record, len(a.intIndex))
	for k, entries := range a.intIndex {
		a.strIndex[strconv.FormatInt(k, 10)] = entries
	}
	a.intIndex = nil
	a.useIntKey = false
}

// valueToInt64 extracts an int64 from a Value, with coercion for common types.
// TextValue is tried as strconv.ParseInt, FloatValue is accepted if it's a whole number.
func valueToInt64(v Value) (int64, bool) {
	switch val := v.(type) {
	case IntValue:
		return val.V, true
	case TextValue:
		n, err := strconv.ParseInt(val.V, 10, 64)
		if err != nil {
			return 0, false
		}
		return n, true
	case FloatValue:
		if val.V == math.Trunc(val.V) && !math.IsInf(val.V, 0) && !math.IsNaN(val.V) {
			return int64(val.V), true
		}
		return 0, false
	default:
		return 0, false
	}
}

// Lookup returns all entries matching the given key value.
func (a *Arrangement) Lookup(keyValue Value) []Record {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if keyValue.IsNull() {
		return nil
	}
	return a.lookupValue(keyValue)
}

// LookupValue returns all entries matching the given key value, using the
// int fast path when available with cross-type coercion.
func (a *Arrangement) LookupValue(keyValue Value) []Record {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if keyValue.IsNull() {
		return nil
	}
	return a.lookupValue(keyValue)
}

// LookupValueUnsafe returns entries matching the key value without locking.
// Only safe when the caller guarantees no concurrent writes.
func (a *Arrangement) LookupValueUnsafe(keyValue Value) []Record {
	if keyValue.IsNull() {
		return nil
	}
	return a.lookupValue(keyValue)
}

// lookupValue is the inner lookup that dispatches to int or string index.
func (a *Arrangement) lookupValue(keyValue Value) []Record {
	if a.useIntKey {
		if intKey, ok := valueToInt64(keyValue); ok {
			return a.intIndex[intKey]
		}
		// Key couldn't be coerced to int64 — no match in int index
		return nil
	}
	if a.strIndex == nil {
		return nil
	}
	return a.strIndex[keyValue.String()]
}

// LookupString returns all entries matching the given key string.
func (a *Arrangement) LookupString(key string) []Record {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.useIntKey {
		// Try parsing the string as int for cross-type coercion
		if n, err := strconv.ParseInt(key, 10, 64); err == nil {
			return a.intIndex[n]
		}
		return nil
	}
	if a.strIndex == nil {
		return nil
	}
	return a.strIndex[key]
}

// LookupStringUnsafe returns entries matching the key without locking.
// Only safe when the caller guarantees no concurrent writes.
func (a *Arrangement) LookupStringUnsafe(key string) []Record {
	if a.useIntKey {
		if n, err := strconv.ParseInt(key, 10, 64); err == nil {
			return a.intIndex[n]
		}
		return nil
	}
	if a.strIndex == nil {
		return nil
	}
	return a.strIndex[key]
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

// EvictBefore removes all entries with Timestamp before the given cutoff time.
// Returns the evicted entries as a batch with negated weights, so the join
// operator can emit retractions for expired join results.
func (a *Arrangement) EvictBefore(cutoff time.Time) Batch {
	a.mu.Lock()
	defer a.mu.Unlock()

	var evicted Batch
	if a.useIntKey {
		for key, entries := range a.intIndex {
			var kept []Record
			for _, rec := range entries {
				if rec.Timestamp.Before(cutoff) {
					retraction := rec
					retraction.Weight = -rec.Weight
					evicted = append(evicted, retraction)
				} else {
					kept = append(kept, rec)
				}
			}
			if len(kept) == 0 {
				delete(a.intIndex, key)
			} else {
				a.intIndex[key] = kept
			}
		}
	} else if a.strIndex != nil {
		for key, entries := range a.strIndex {
			var kept []Record
			for _, rec := range entries {
				if rec.Timestamp.Before(cutoff) {
					retraction := rec
					retraction.Weight = -rec.Weight
					evicted = append(evicted, retraction)
				} else {
					kept = append(kept, rec)
				}
			}
			if len(kept) == 0 {
				delete(a.strIndex, key)
			} else {
				a.strIndex[key] = kept
			}
		}
	}
	return evicted
}

// recordColumnsFingerprint returns a fingerprint based only on column values
// (excluding weight), suitable for identifying "the same record" in Z-set terms.
func recordColumnsFingerprint(r Record) string {
	return RecordFingerprint(Record{Columns: r.Columns, Weight: 1})
}
