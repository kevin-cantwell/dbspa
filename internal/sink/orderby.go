package sink

import (
	"cmp"

	"github.com/kevin-cantwell/folddb/internal/engine"
)

// OrderBySpec describes a single ORDER BY column for sorting output rows.
type OrderBySpec struct {
	Column string // column name to sort by
	Desc   bool   // true for DESC
}

// CompareValues compares two engine.Values for ordering.
// NULL sorts last (after all non-NULL values).
// Numeric types (INT, FLOAT) are compared numerically, with INT promoted to FLOAT for mixed comparisons.
// TEXT is compared lexicographically.
// All other types fall back to string representation comparison.
func CompareValues(a, b engine.Value) int {
	aNil := a == nil || a.IsNull()
	bNil := b == nil || b.IsNull()
	if aNil && bNil {
		return 0
	}
	if aNil {
		return 1 // NULL sorts last
	}
	if bNil {
		return -1 // NULL sorts last
	}

	// Try numeric comparison
	af, aNum := toFloat64(a)
	bf, bNum := toFloat64(b)
	if aNum && bNum {
		return cmp.Compare(af, bf)
	}

	// TEXT comparison
	if at, ok := a.(engine.TextValue); ok {
		if bt, ok := b.(engine.TextValue); ok {
			return cmp.Compare(at.V, bt.V)
		}
	}

	// Fallback: string representation
	return cmp.Compare(a.String(), b.String())
}

func toFloat64(v engine.Value) (float64, bool) {
	switch val := v.(type) {
	case engine.IntValue:
		return float64(val.V), true
	case engine.FloatValue:
		return val.V, true
	default:
		return 0, false
	}
}
