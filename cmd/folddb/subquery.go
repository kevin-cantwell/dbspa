package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"

	"github.com/kevin-cantwell/folddb/internal/engine"
	"github.com/kevin-cantwell/folddb/internal/format"
	"github.com/kevin-cantwell/folddb/internal/source"
	"github.com/kevin-cantwell/folddb/internal/sql/ast"
)

// executeSubquery runs an inner SELECT statement to completion and returns
// the materialized results as a slice of records. The inner query is fully
// evaluated before returning — no streaming. For accumulating queries (GROUP BY),
// only the final state (positive-weight records) is returned.
func executeSubquery(ctx context.Context, stmt *ast.SelectStatement) ([]engine.Record, error) {
	// If SELECT has aggregates but no GROUP BY, create an implicit single group
	if stmt.GroupBy == nil && hasAggregateInSelect(stmt.Columns) {
		stmt.GroupBy = []ast.Expr{}
	}

	isAccumulating := stmt.GroupBy != nil

	// Determine source
	fromURI := ""
	if stmt.From != nil {
		fromURI = stmt.From.URI
	}

	// Step 1: Get source records
	var sourceRecords <-chan engine.Record

	switch {
	case stmt.FromSubquery != nil:
		// Nested subquery: execute recursively
		inner, err := executeSubquery(ctx, stmt.FromSubquery.Query)
		if err != nil {
			return nil, fmt.Errorf("nested subquery: %w", err)
		}
		ch := make(chan engine.Record, len(inner))
		for _, rec := range inner {
			ch <- rec
		}
		close(ch)
		sourceRecords = ch

	case fromURI != "" && isFileSource(fromURI):
		// File source: route to DuckDB/FoldDB decoders
		ch := make(chan engine.Record, 256)
		go func() {
			defer close(ch)
			if err := duckDBScanToChannel(ctx, fromURI, ch); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: subquery DuckDB scan error: %v, falling back to FoldDB\n", err)
				// Fallback: load via table loader
				formatHint := ""
				if stmt.From != nil {
					formatHint = stmt.From.Format
				}
				recs, loadErr := loadTableFile(fromURI, formatHint)
				if loadErr != nil {
					fmt.Fprintf(os.Stderr, "Warning: subquery file load error: %v\n", loadErr)
					return
				}
				for _, rec := range recs {
					select {
					case ch <- rec:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
		sourceRecords = ch

	case fromURI != "" && strings.HasPrefix(fromURI, "kafka://"):
		return nil, fmt.Errorf("Kafka sources in subqueries are not yet supported")

	default:
		// stdin source (no FROM or FROM stdin)
		var formatStr string
		var formatOpts map[string]string
		if stmt.From != nil {
			formatStr = stmt.From.Format
			formatOpts = stmt.From.FormatOptions
		}
		dec, err := format.NewDecoderWithOptions(formatStr, formatOpts)
		if err != nil {
			return nil, fmt.Errorf("subquery decoder error: %w", err)
		}

		inputSrc := &source.Stdin{Reader: os.Stdin}
		ch := make(chan engine.Record, 256)

		// Stream decoders handle their own framing
		if sd, ok := dec.(format.StreamDecoder); ok {
			go func() {
				if err := sd.DecodeStream(inputSrc.ReadRaw(), ch); err != nil {
					fmt.Fprintf(os.Stderr, "Warning: subquery stream decode error: %v\n", err)
				}
			}()
		} else {
			rawCh := inputSrc.Read()
			go func() {
				defer close(ch)
				for raw := range rawCh {
					recs, err := format.DecodeAll(dec, raw)
					if err != nil {
						if err == format.ErrHeaderRow {
							continue
						}
						fmt.Fprintf(os.Stderr, "Warning: subquery decode error: %v\n", err)
						continue
					}
					for _, rec := range recs {
						select {
						case ch <- rec:
						case <-ctx.Done():
							return
						}
					}
				}
			}()
		}
		sourceRecords = ch
	}

	// Step 2: Handle JOIN within the subquery (if present)
	if stmt.Join != nil {
		joinOp, err := buildSubqueryJoinOp(ctx, stmt)
		if err != nil {
			return nil, fmt.Errorf("subquery join error: %w", err)
		}
		sourceRecords = applyJoin(ctx, joinOp, sourceRecords)
	}

	// Step 3: Process through pipeline
	if isAccumulating {
		return executeAccumulatingSubquery(ctx, stmt, sourceRecords)
	}
	return executeNonAccumulatingSubquery(ctx, stmt, sourceRecords)
}

// executeAccumulatingSubquery runs an accumulating (GROUP BY) subquery and
// returns the final aggregated state as positive-weight records.
func executeAccumulatingSubquery(ctx context.Context, stmt *ast.SelectStatement, records <-chan engine.Record) ([]engine.Record, error) {
	aggCols, err := engine.ParseAggColumns(stmt.Columns, stmt.GroupBy)
	if err != nil {
		return nil, fmt.Errorf("subquery aggregate setup error: %w", err)
	}

	aggOp := engine.NewAggregateOp(aggCols, stmt.GroupBy, stmt.Having)

	// Process all input records through the fused filter+aggregate processor
	var inputCount atomic.Int64
	fused := &engine.FusedAggregateProcessor{
		Where:       stmt.Where,
		AggregateOp: aggOp,
		InputCount:  &inputCount,
	}

	batchedCh := engine.BatchChannel(records, engine.DefaultBatchSize)
	outCh := make(chan engine.Record, 256)
	go func() {
		fused.ProcessBatches(batchedCh, outCh)
	}()

	// Drain output channel (these are changelog diffs — we want final state)
	for range outCh {
		// discard intermediate changelog records
	}

	// Return the final materialized state (only positive-weight records)
	finalState := aggOp.CurrentState()
	var results []engine.Record
	for _, rec := range finalState {
		if rec.Weight > 0 {
			results = append(results, rec)
		}
	}
	return results, nil
}

// executeNonAccumulatingSubquery runs a non-accumulating subquery (no GROUP BY)
// and collects all output records.
func executeNonAccumulatingSubquery(ctx context.Context, stmt *ast.SelectStatement, records <-chan engine.Record) ([]engine.Record, error) {
	pipeline := &engine.Pipeline{
		Columns: stmt.Columns,
		Where:   stmt.Where,
	}

	batchedCh := engine.BatchChannel(records, engine.DefaultBatchSize)
	outCh := make(chan engine.Record, 256)
	go func() {
		pipeline.ProcessBatches(batchedCh, outCh)
	}()

	var results []engine.Record
	limit := stmt.Limit
	distinct := stmt.Distinct
	seen := make(map[string]struct{})

	for rec := range outCh {
		if limit != nil && len(results) >= *limit {
			break
		}
		if distinct {
			key := engine.RecordFingerprint(rec)
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
		}
		results = append(results, rec)
	}
	return results, nil
}

// buildSubqueryJoinOp builds a DD join operator for a JOIN within a subquery.
// This handles the case where a subquery itself contains a JOIN clause.
func buildSubqueryJoinOp(ctx context.Context, stmt *ast.SelectStatement) (*engine.DDJoinOp, error) {
	join := stmt.Join

	var tableRecords []engine.Record
	var err error

	if join.Subquery != nil {
		// JOIN source is itself a subquery — execute recursively
		tableRecords, err = executeSubquery(ctx, join.Subquery.Query)
		if err != nil {
			return nil, fmt.Errorf("join subquery: %w", err)
		}
	} else {
		// JOIN source is a file
		tableFormat := ""
		if join.Source != nil {
			tableFormat = join.Source.Format
		}
		tableRecords, err = loadTableFile(join.Source.URI, tableFormat)
		if err != nil {
			return nil, fmt.Errorf("join table load error: %w", err)
		}
	}

	streamAlias := stmt.FromAlias
	tableAlias := join.Alias

	streamKey, tableKey, err := engine.ExtractEquiJoinKeys(join.Condition, streamAlias, tableAlias)
	if err != nil {
		return nil, fmt.Errorf("join key extraction error: %w", err)
	}

	op := engine.NewDDJoinOp(streamKey, tableKey, streamAlias, tableAlias, join.Type == "LEFT JOIN")
	op.RightIsStatic = true
	op.ProjectedColumns = engine.ExtractReferencedColumns(stmt)

	tableBatch := make(engine.Batch, len(tableRecords))
	for i, rec := range tableRecords {
		if rec.Weight == 0 {
			rec.Weight = 1
		}
		tableBatch[i] = rec
	}
	op.Right.Apply(tableBatch)

	return op, nil
}
