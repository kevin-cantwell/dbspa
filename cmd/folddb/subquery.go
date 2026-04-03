package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
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
	case stmt.FromExec != nil:
		// EXEC source: run command, materialize records
		recs, err := materializeExecSource(stmt.FromExec)
		if err != nil {
			return nil, fmt.Errorf("subquery EXEC error: %w", err)
		}
		ch := make(chan engine.Record, len(recs))
		for _, rec := range recs {
			ch <- rec
		}
		close(ch)
		sourceRecords = ch

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
					formatHint = ast.CombinedFormat(stmt.From.Format, stmt.Changelog)
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
		// Kafka sources in materialized subqueries block forever (no EOF).
		// For FROM subqueries, delegate to the streaming path via executeStreamingSubquery
		// (caller should use isStreamingSubquery to detect this case and call
		// executeStreamingSubquery directly). If we get here, it means the caller
		// expected materialized results from a Kafka source, which cannot work.
		return nil, fmt.Errorf("kafka source in subquery requires streaming execution (use executeStreamingSubquery)")

	default:
		// stdin source (no FROM or FROM stdin)
		var formatStr string
		var formatOpts map[string]string
		if stmt.From != nil {
			formatStr = ast.CombinedFormat(stmt.From.Format, stmt.Changelog)
			formatOpts = stmt.From.FormatOpts
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

	if join.Exec != nil {
		// JOIN source is EXEC — run command, materialize records
		tableRecords, err = materializeExecSource(join.Exec)
		if err != nil {
			return nil, fmt.Errorf("join EXEC error: %w", err)
		}
	} else if join.Subquery != nil {
		// JOIN source is itself a subquery — execute recursively
		tableRecords, err = executeSubquery(ctx, join.Subquery.Query)
		if err != nil {
			return nil, fmt.Errorf("join subquery: %w", err)
		}
	} else {
		// JOIN source is a file
		tableFormat := ""
		if join.Source != nil {
			tableFormat = ast.CombinedFormat(join.Source.Format, join.Source.Changelog)
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

// isStreamingSubquery returns true if the subquery's FROM source is a Kafka stream.
// Streaming subqueries run concurrently with the outer query, feeding Z-set deltas
// into the DD join via ProcessRightDelta. The subquery must have GROUP BY — without
// accumulation, raw events don't form a useful join table.
func isStreamingSubquery(sq *ast.SubquerySource) bool {
	if sq == nil || sq.Query == nil || sq.Query.From == nil {
		return false
	}
	return strings.HasPrefix(sq.Query.From.URI, "kafka://")
}

// executeStreamingSubquery runs an accumulating subquery concurrently,
// sending Z-set deltas (retraction+insertion pairs) to the returned channel.
// The channel is closed when the context is cancelled or the Kafka source ends.
//
// The inner query runs: Kafka → decode → filter → accumulate → outCh
// Each accumulator change emits a retraction of the old value and insertion of
// the new value, which the outer query's DD join consumes via ProcessRightDelta.
func executeStreamingSubquery(ctx context.Context, stmt *ast.SelectStatement) (<-chan engine.Record, error) {
	if stmt.From == nil || !strings.HasPrefix(stmt.From.URI, "kafka://") {
		return nil, fmt.Errorf("streaming subquery requires a kafka:// source")
	}

	// If SELECT has aggregates but no GROUP BY, create an implicit single group
	if stmt.GroupBy == nil && hasAggregateInSelect(stmt.Columns) {
		stmt.GroupBy = []ast.Expr{}
	}

	if stmt.GroupBy == nil {
		return nil, fmt.Errorf("streaming subquery must have GROUP BY (raw events without accumulation don't form a useful join table)")
	}

	aggCols, err := engine.ParseAggColumns(stmt.Columns, stmt.GroupBy)
	if err != nil {
		return nil, fmt.Errorf("streaming subquery aggregate setup: %w", err)
	}

	// Parse Kafka URI
	cfg, err := source.ParseKafkaURI(stmt.From.URI)
	if err != nil {
		return nil, fmt.Errorf("streaming subquery kafka URI: %w", err)
	}

	// Build decoder
	formatStr := ast.CombinedFormat(stmt.From.Format, stmt.Changelog)
	formatOpts := stmt.From.FormatOpts
	var dec format.Decoder
	if cfg.Registry != "" {
		dec, err = format.NewDecoderForKafka(formatStr, formatOpts, cfg.Registry)
	} else {
		dec, err = format.NewDecoderWithOptions(formatStr, formatOpts)
	}
	if err != nil {
		return nil, fmt.Errorf("streaming subquery decoder: %w", err)
	}

	outCh := make(chan engine.Record, 256)

	go func() {
		defer close(outCh)

		kafkaSrc := source.NewKafka(ctx, cfg)
		kafkaCh := kafkaSrc.Read()

		// Decode Kafka records into engine records
		recordCh := make(chan engine.Record, 256)
		go func() {
			defer close(recordCh)
			for kr := range kafkaCh {
				if kr.Value == nil {
					continue // tombstone
				}
				recs, err := decodeWithCSVHeader(dec, kr.Value)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Warning: streaming subquery decode error: %v\n", err)
					continue
				}
				recs = format.InjectKafkaVirtuals(recs, kr)
				for _, rec := range recs {
					select {
					case recordCh <- rec:
					case <-ctx.Done():
						return
					}
				}
			}
		}()

		// Run accumulating pipeline: filter + aggregate
		aggOp := engine.NewAggregateOp(aggCols, stmt.GroupBy, stmt.Having)
		var inputCount atomic.Int64
		fused := &engine.FusedAggregateProcessor{
			Where:       stmt.Where,
			AggregateOp: aggOp,
			InputCount:  &inputCount,
		}

		batchedCh := engine.BatchChannel(recordCh, engine.DefaultBatchSize)

		// Forward aggregation deltas to outCh (the channel the outer join reads from)
		fused.ProcessBatches(batchedCh, outCh)
	}()

	return outCh, nil
}

// buildStreamingSubqueryJoinOp constructs a DDJoinOp for a streaming subquery JOIN.
// Unlike buildDDJoinOp which materializes the right side, this starts the right
// arrangement empty and returns a channel of Z-set deltas from the inner query.
// The caller is responsible for feeding these deltas to ProcessRightDelta.
func buildStreamingSubqueryJoinOp(ctx context.Context, stmt *ast.SelectStatement, arrangementMemLimit int) (*engine.DDJoinOp, <-chan engine.Record, error) {
	join := stmt.Join

	// Start the streaming inner query
	innerCh, err := executeStreamingSubquery(ctx, join.Subquery.Query)
	if err != nil {
		return nil, nil, fmt.Errorf("streaming subquery: %w", err)
	}

	streamAlias := stmt.FromAlias
	tableAlias := join.Alias

	streamKey, tableKey, err := engine.ExtractEquiJoinKeys(join.Condition, streamAlias, tableAlias)
	if err != nil {
		return nil, nil, fmt.Errorf("join key extraction error: %w", err)
	}

	op := engine.NewDDJoinOp(streamKey, tableKey, streamAlias, tableAlias, join.Type == "LEFT JOIN")

	// Use disk-backed arrangements if memory limit is set
	if arrangementMemLimit > 0 {
		op.Left = engine.NewDiskArrangement(streamKey, arrangementMemLimit, "")
		op.Right = engine.NewDiskArrangement(tableKey, arrangementMemLimit, "")
	}

	// Streaming subquery: right side is NOT static — it receives live deltas.
	op.RightIsStatic = false

	// Projection pushdown
	op.ProjectedColumns = engine.ExtractReferencedColumns(stmt)

	return op, innerCh, nil
}

// applyStreamingSubqueryJoin wraps a left record channel with a DD join that also
// receives right-side deltas from a streaming subquery. Returns the joined output channel.
// The right delta goroutine runs concurrently with the left delta processing.
//
// The left side (e.g., a file) is loaded fully into the left arrangement as static
// reference data. The right side (streaming subquery from Kafka) runs continuously,
// and each right-side delta is joined against ALL left records via ProcessRightDelta.
// The query runs until the parent context is cancelled (Ctrl+C / --timeout) or the
// right-side channel closes — NOT when the left side finishes loading.
func applyStreamingSubqueryJoin(ctx context.Context, joinOp *engine.DDJoinOp, leftIn <-chan engine.Record, rightIn <-chan engine.Record) <-chan engine.Record {
	out := make(chan engine.Record, 256)

	var wg sync.WaitGroup
	wg.Add(2)

	// Feed right-side deltas from the streaming subquery.
	// This goroutine runs until the right channel closes or the parent context
	// is cancelled. It does NOT stop when the left side finishes.
	go func() {
		defer wg.Done()
		for {
			select {
			case rec, ok := <-rightIn:
				if !ok {
					return
				}
				joinOp.ProcessRightDelta(engine.Batch{rec}, out)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Feed left-side deltas from the outer query's source.
	// When the left side is exhausted (EOF), the goroutine finishes but the
	// right side keeps running — the left records remain in the arrangement
	// for ProcessRightDelta lookups.
	go func() {
		defer wg.Done()
		joinOp.ProcessLeftDeltaStream(ctx, leftIn, out)
	}()

	// Close output when both sides are done
	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
