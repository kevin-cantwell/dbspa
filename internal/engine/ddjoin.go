package engine

import (
	"sync"
	"time"

	"github.com/kevin-cantwell/folddb/internal/sql/ast"
)

// DDJoinOp implements the DBSP join formula:
//
//	delta(A JOIN B) = (delta_A JOIN B) UNION (A JOIN delta_B) UNION (delta_A JOIN delta_B)
//
// Both sides maintain an Arrangement (indexed Z-set). When a delta arrives on
// either side, it is applied to that side's arrangement and then joined against
// the other side's arrangement. Output weights are the product of left and right
// weights, which ensures retractions propagate correctly through the join.
type DDJoinOp struct {
	mu           sync.Mutex   // protects concurrent ProcessLeftDelta/ProcessRightDelta calls
	Left         *Arrangement // left (stream) side arrangement
	Right        *Arrangement // right (table/CDC) side arrangement
	LeftKeyExpr  ast.Expr     // expression for left join key
	RightKeyExpr ast.Expr     // expression for right join key
	LeftAlias    string       // alias prefix for left columns (e.g., "e")
	RightAlias   string       // alias prefix for right columns (e.g., "u")
	IsLeftJoin   bool         // emit NULL-filled rows for unmatched left records
	RightIsStatic bool        // when true, skip left arrangement (right side never changes)
}

// NewDDJoinOp creates a new differential dataflow join operator.
func NewDDJoinOp(leftKeyExpr, rightKeyExpr ast.Expr, leftAlias, rightAlias string, isLeftJoin bool) *DDJoinOp {
	return &DDJoinOp{
		Left:         NewArrangement(leftKeyExpr),
		Right:        NewArrangement(rightKeyExpr),
		LeftKeyExpr:  leftKeyExpr,
		RightKeyExpr: rightKeyExpr,
		LeftAlias:    leftAlias,
		RightAlias:   rightAlias,
		IsLeftJoin:   isLeftJoin,
	}
}

// ProcessLeftDelta handles new records from the left (stream) side.
// Each delta record is applied to the left arrangement, then joined against
// the right arrangement. Joined records are sent to the output channel.
func (j *DDJoinOp) ProcessLeftDelta(delta Batch, out chan<- Record) {
	j.mu.Lock()
	defer j.mu.Unlock()

	// When right side is static, skip left arrangement entirely — it's only
	// needed for ProcessRightDelta lookups which won't happen.
	var applied Batch
	if j.RightIsStatic {
		applied = delta
	} else {
		applied = j.Left.Apply(delta)
	}

	for _, leftRec := range applied {
		key, err := EvalKeyExpr(j.LeftKeyExpr, leftRec)
		if err != nil || key.IsNull() {
			if j.IsLeftJoin {
				out <- j.mergeWithNulls(leftRec)
			}
			continue
		}

		rightMatches := j.Right.Lookup(key)
		if len(rightMatches) == 0 {
			if j.IsLeftJoin {
				out <- j.mergeWithNulls(leftRec)
			}
			continue
		}

		for _, rightRec := range rightMatches {
			merged := j.merge(leftRec, rightRec)
			merged.Weight = leftRec.Weight * rightRec.Weight
			out <- merged
		}
	}
}

// ProcessRightDelta handles new records from the right (table/CDC) side.
// Each delta record is applied to the right arrangement, then joined against
// the left arrangement. This is what enables live table updates to propagate
// through the join — the killer feature of differential dataflow.
//
// For LEFT JOIN: when a right record appears for a key that previously had
// no matches, existing left records for that key need their NULL rows retracted
// and matched rows emitted. When a right record is removed and leaves no
// matches, NULL rows must be re-emitted.
func (j *DDJoinOp) ProcessRightDelta(delta Batch, out chan<- Record) {
	j.mu.Lock()
	defer j.mu.Unlock()

	for _, rightRec := range delta {
		key, err := EvalKeyExpr(j.RightKeyExpr, rightRec)
		if err != nil || key.IsNull() {
			continue
		}

		// Check right arrangement state BEFORE applying this delta
		rightMatchesBefore := j.Right.Lookup(key)
		hadMatchesBefore := len(rightMatchesBefore) > 0

		// Apply this single record to the right arrangement
		j.Right.Apply(Batch{rightRec})

		// Look up all left records that match this key
		leftMatches := j.Left.Lookup(key)

		for _, leftRec := range leftMatches {
			// Emit joined record with multiplied weight
			merged := j.merge(leftRec, rightRec)
			merged.Weight = leftRec.Weight * rightRec.Weight
			out <- merged

			// LEFT JOIN: handle NULL row transitions
			if j.IsLeftJoin {
				rightMatchesAfter := j.Right.Lookup(key)
				hasMatchesAfter := len(rightMatchesAfter) > 0

				if !hadMatchesBefore && hasMatchesAfter {
					// Right side went from empty to non-empty: retract the NULL row
					nullRow := j.mergeWithNulls(leftRec)
					nullRow.Weight = -leftRec.Weight
					out <- nullRow
				} else if hadMatchesBefore && !hasMatchesAfter {
					// Right side went from non-empty to empty: re-emit NULL row
					nullRow := j.mergeWithNulls(leftRec)
					nullRow.Weight = leftRec.Weight
					out <- nullRow
				}
			}
		}
	}
}

// EvictAndRetract evicts expired entries from both arrangements and processes
// the resulting retractions through the join to produce output retractions.
// This is the safe way to run eviction — it holds the DDJoinOp mutex to prevent
// concurrent modification during Process* calls.
func (j *DDJoinOp) EvictAndRetract(cutoff time.Time, out chan<- Record) {
	j.mu.Lock()
	defer j.mu.Unlock()

	// Evict from left arrangement and process retractions against right
	leftEvicted := j.Left.EvictBefore(cutoff)
	for _, leftRec := range leftEvicted {
		key, err := EvalKeyExpr(j.LeftKeyExpr, leftRec)
		if err != nil || key.IsNull() {
			continue
		}
		rightMatches := j.Right.Lookup(key)
		if len(rightMatches) == 0 {
			continue
		}
		for _, rightRec := range rightMatches {
			merged := j.merge(leftRec, rightRec)
			merged.Weight = leftRec.Weight * rightRec.Weight
			out <- merged
		}
	}

	// Evict from right arrangement and process retractions against left
	rightEvicted := j.Right.EvictBefore(cutoff)
	for _, rightRec := range rightEvicted {
		key, err := EvalKeyExpr(j.RightKeyExpr, rightRec)
		if err != nil || key.IsNull() {
			continue
		}
		leftMatches := j.Left.Lookup(key)
		if len(leftMatches) == 0 {
			continue
		}
		for _, leftRec := range leftMatches {
			merged := j.merge(leftRec, rightRec)
			merged.Weight = leftRec.Weight * rightRec.Weight
			out <- merged
		}
	}
}

// ProcessLeftDeltaSlice is a convenience method that returns results as a slice
// instead of writing to a channel. Useful for testing and synchronous pipelines.
func (j *DDJoinOp) ProcessLeftDeltaSlice(delta Batch) []Record {
	ch := make(chan Record, len(delta)*4)
	done := make(chan struct{})
	var results []Record
	go func() {
		for rec := range ch {
			results = append(results, rec)
		}
		close(done)
	}()
	j.ProcessLeftDelta(delta, ch)
	close(ch)
	<-done
	return results
}

// ProcessRightDeltaSlice is a convenience method that returns results as a slice.
func (j *DDJoinOp) ProcessRightDeltaSlice(delta Batch) []Record {
	ch := make(chan Record, len(delta)*4)
	done := make(chan struct{})
	var results []Record
	go func() {
		for rec := range ch {
			results = append(results, rec)
		}
		close(done)
	}()
	j.ProcessRightDelta(delta, ch)
	close(ch)
	<-done
	return results
}

// merge combines left and right records into a single joined record.
// Columns are prefixed with aliases if provided. Right columns overwrite
// left columns in case of name conflicts (same as HashJoinOp behavior).
func (j *DDJoinOp) merge(leftRec, rightRec Record) Record {
	merged := Record{
		Columns:   make(map[string]Value, len(leftRec.Columns)+len(rightRec.Columns)),
		Timestamp: leftRec.Timestamp,
		Weight:    leftRec.Weight * rightRec.Weight,
	}

	// Add left columns (unqualified and qualified)
	for k, v := range leftRec.Columns {
		merged.Columns[k] = v
		if j.LeftAlias != "" {
			merged.Columns[j.LeftAlias+"."+k] = v
		}
	}

	// Add right columns (unqualified and qualified)
	// Right overwrites left on unqualified name conflict
	for k, v := range rightRec.Columns {
		merged.Columns[k] = v
		if j.RightAlias != "" {
			merged.Columns[j.RightAlias+"."+k] = v
		}
	}

	return merged
}

// mergeWithNulls creates a joined record for LEFT JOIN with no right match.
func (j *DDJoinOp) mergeWithNulls(leftRec Record) Record {
	rightCols := j.Right.ColumnNames()
	merged := Record{
		Columns:   make(map[string]Value, len(leftRec.Columns)+len(rightCols)),
		Timestamp: leftRec.Timestamp,
		Weight:    leftRec.Weight,
	}

	// Add left columns
	for k, v := range leftRec.Columns {
		merged.Columns[k] = v
		if j.LeftAlias != "" {
			merged.Columns[j.LeftAlias+"."+k] = v
		}
	}

	// Add NULL values for right columns
	for _, k := range rightCols {
		merged.Columns[k] = NullValue{}
		if j.RightAlias != "" {
			merged.Columns[j.RightAlias+"."+k] = NullValue{}
		}
	}

	return merged
}
