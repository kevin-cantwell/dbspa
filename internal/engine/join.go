package engine

import (
	"fmt"

	"github.com/kevin-cantwell/dbspa/internal/sql/ast"
)

// HashJoinOp implements a stream-to-file hash join.
// The table side is loaded into an in-memory hash map indexed by join key.
// For each stream record, the hash map is probed to produce joined records.
type HashJoinOp struct {
	// Index maps join key (as string) to matching table records.
	Index map[string][]Record

	// StreamKeyExpr extracts the join key from a stream record.
	StreamKeyExpr ast.Expr

	// TableKeyExpr extracts the join key from a table record.
	TableKeyExpr ast.Expr

	// LeftJoin emits unmatched stream records with NULLs for table columns.
	LeftJoin bool

	// StreamAlias is the alias for the stream side (e.g., "e").
	StreamAlias string

	// TableAlias is the alias for the table side (e.g., "u").
	TableAlias string

	// tableColumns tracks the column names from the table side, used for
	// generating NULL values in LEFT JOIN when there's no match.
	tableColumns []string
}

// ExtractEquiJoinKeys parses an ON condition of the form `a.x = b.y` and
// returns the left and right key expressions. Returns an error if the
// condition is not a simple equi-join.
func ExtractEquiJoinKeys(cond ast.Expr, streamAlias, tableAlias string) (streamKey, tableKey ast.Expr, err error) {
	bin, ok := cond.(*ast.BinaryExpr)
	if !ok || bin.Op != "=" {
		return nil, nil, fmt.Errorf("JOIN ON condition must be an equi-join (a.x = b.y), got %T with op %q", cond, opOf(cond))
	}

	leftQual, leftOk := qualifierOf(bin.Left)
	rightQual, rightOk := qualifierOf(bin.Right)

	// If both sides are qualified, match by alias
	if leftOk && rightOk {
		if leftQual == streamAlias && rightQual == tableAlias {
			return bin.Left, bin.Right, nil
		}
		if leftQual == tableAlias && rightQual == streamAlias {
			return bin.Right, bin.Left, nil
		}
		return nil, nil, fmt.Errorf("JOIN ON qualifiers %q and %q don't match aliases %q and %q", leftQual, rightQual, streamAlias, tableAlias)
	}

	// If neither is qualified, assume left = stream, right = table
	return bin.Left, bin.Right, nil
}

func opOf(e ast.Expr) string {
	if b, ok := e.(*ast.BinaryExpr); ok {
		return b.Op
	}
	return ""
}

func qualifierOf(e ast.Expr) (string, bool) {
	if q, ok := e.(*ast.QualifiedRef); ok {
		return q.Qualifier, true
	}
	return "", false
}

// BuildIndex builds the hash index from table records.
func (h *HashJoinOp) BuildIndex(records []Record) error {
	h.Index = make(map[string][]Record, len(records))

	// Track table columns for LEFT JOIN NULL generation
	if len(records) > 0 {
		for k := range records[0].Columns {
			h.tableColumns = append(h.tableColumns, k)
		}
	}

	for _, rec := range records {
		key, err := Eval(h.TableKeyExpr, rec)
		if err != nil {
			continue // skip records where key extraction fails
		}
		if key.IsNull() {
			continue // NULLs don't join
		}
		keyStr := key.String()
		h.Index[keyStr] = append(h.Index[keyStr], rec)
	}

	return nil
}

// Probe looks up the stream record's join key in the hash index and returns
// zero or more joined records. Each joined record merges the stream and table
// columns, prefixed with their aliases.
func (h *HashJoinOp) Probe(streamRec Record) []Record {
	key, err := Eval(h.StreamKeyExpr, streamRec)
	if err != nil || key.IsNull() {
		if h.LeftJoin {
			return []Record{h.mergeWithNulls(streamRec)}
		}
		return nil
	}

	matches, found := h.Index[key.String()]
	if !found || len(matches) == 0 {
		if h.LeftJoin {
			return []Record{h.mergeWithNulls(streamRec)}
		}
		return nil
	}

	results := make([]Record, 0, len(matches))
	for _, tableRec := range matches {
		results = append(results, h.merge(streamRec, tableRec))
	}
	return results
}

// merge combines stream and table records into a single record.
// Columns are prefixed with aliases if provided.
func (h *HashJoinOp) merge(streamRec, tableRec Record) Record {
	merged := Record{
		Columns:   make(map[string]Value, len(streamRec.Columns)+len(tableRec.Columns)),
		Timestamp: streamRec.Timestamp,
		Weight:      streamRec.Weight,
	}

	// Add stream columns (unqualified and qualified)
	for k, v := range streamRec.Columns {
		merged.Columns[k] = v
		if h.StreamAlias != "" {
			merged.Columns[h.StreamAlias+"."+k] = v
		}
	}

	// Add table columns (unqualified and qualified)
	for k, v := range tableRec.Columns {
		// Unqualified: table columns overwrite stream columns if names collide
		// (since the user must use qualified refs to disambiguate)
		merged.Columns[k] = v
		if h.TableAlias != "" {
			merged.Columns[h.TableAlias+"."+k] = v
		}
	}

	return merged
}

// mergeWithNulls creates a joined record for LEFT JOIN with no table match.
func (h *HashJoinOp) mergeWithNulls(streamRec Record) Record {
	merged := Record{
		Columns:   make(map[string]Value, len(streamRec.Columns)+len(h.tableColumns)),
		Timestamp: streamRec.Timestamp,
		Weight:      streamRec.Weight,
	}

	// Add stream columns
	for k, v := range streamRec.Columns {
		merged.Columns[k] = v
		if h.StreamAlias != "" {
			merged.Columns[h.StreamAlias+"."+k] = v
		}
	}

	// Add NULL values for table columns
	for _, k := range h.tableColumns {
		merged.Columns[k] = NullValue{}
		if h.TableAlias != "" {
			merged.Columns[h.TableAlias+"."+k] = NullValue{}
		}
	}

	return merged
}
