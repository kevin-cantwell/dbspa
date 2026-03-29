// Command folddb executes SQL queries against streaming data sources.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kevin-cantwell/folddb/internal/engine"
	"github.com/kevin-cantwell/folddb/internal/format"
	"github.com/kevin-cantwell/folddb/internal/sink"
	"github.com/kevin-cantwell/folddb/internal/source"
	"github.com/kevin-cantwell/folddb/internal/sql/ast"
	"github.com/kevin-cantwell/folddb/internal/sql/parser"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// cliFlags holds parsed CLI flags.
type cliFlags struct {
	stateful           bool
	stateDB            string // --state <file.db>
	stateDir           string // --state-dir
	checkpointInterval time.Duration
}

func parseFlags() (cliFlags, []string) {
	var flags cliFlags
	flags.checkpointInterval = 5 * time.Second

	args := os.Args[1:]
	var remaining []string

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--stateful":
			flags.stateful = true
		case "--state":
			if i+1 < len(args) {
				i++
				flags.stateDB = args[i]
			}
		case "--state-dir":
			if i+1 < len(args) {
				i++
				flags.stateDir = args[i]
			}
		case "--checkpoint-interval":
			if i+1 < len(args) {
				i++
				d, err := engine.ParseDuration(args[i])
				if err == nil {
					flags.checkpointInterval = d
				}
			}
		default:
			remaining = append(remaining, args[i])
		}
	}
	return flags, remaining
}

func run() error {
	flags, remaining := parseFlags()

	// Check for subcommands
	if len(remaining) > 0 && remaining[0] == "schema" {
		return runSchema(remaining[1:])
	}
	if len(remaining) > 0 && remaining[0] == "state" {
		return runStateCmd(remaining[1:])
	}

	sql, err := getSQLFromArgs(remaining)
	if err != nil {
		return err
	}

	// Parse
	p := parser.New(sql)
	stmt, err := p.Parse()
	if err != nil {
		return fmt.Errorf("parse error: %w", err)
	}

	// Set up context with SIGINT handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	// Determine format decoder
	var formatStr string
	if stmt.From != nil {
		formatStr = stmt.From.Format
	}
	dec, err := format.NewDecoder(formatStr)
	if err != nil {
		return err
	}

	isAccumulating := stmt.GroupBy != nil
	isWindowed := stmt.Window != nil

	// Determine source and build record channel
	if stmt.From != nil && strings.HasPrefix(stmt.From.URI, "kafka://") {
		return runKafka(ctx, stmt, dec, isAccumulating, isWindowed, flags)
	}

	// Default: stdin source
	stdinSrc := &source.Stdin{Reader: os.Stdin}
	if isWindowed {
		return runWindowed(ctx, stmt, stdinSrc, dec, flags)
	}
	if isAccumulating {
		return runAccumulating(ctx, stmt, stdinSrc, dec)
	}
	return runNonAccumulating(ctx, stmt, stdinSrc, dec)
}

func runStateCmd(args []string) error {
	if len(args) == 0 {
		fmt.Println("Usage: folddb state list|inspect <hash>|reset <hash>")
		return nil
	}

	switch args[0] {
	case "list":
		infos, err := engine.ListCheckpoints()
		if err != nil {
			return err
		}
		if len(infos) == 0 {
			fmt.Println("No checkpointed queries found.")
			return nil
		}
		for _, info := range infos {
			fmt.Printf("  %s  last_flush=%s  dir=%s\n", info.Hash, info.Timestamp.Format(time.RFC3339), info.Dir)
		}
		return nil

	case "inspect":
		if len(args) < 2 {
			return fmt.Errorf("usage: folddb state inspect <query-hash>")
		}
		// TODO: implement detailed inspection
		fmt.Printf("Inspect checkpoint %s (not yet implemented)\n", args[1])
		return nil

	case "reset":
		if len(args) < 2 {
			return fmt.Errorf("usage: folddb state reset <query-hash>")
		}
		return engine.ResetCheckpoint(args[1])

	default:
		return fmt.Errorf("unknown state subcommand: %s", args[0])
	}
}

func runKafka(ctx context.Context, stmt *ast.SelectStatement, dec format.Decoder, isAccumulating, isWindowed bool, flags cliFlags) error {
	cfg, err := source.ParseKafkaURI(stmt.From.URI)
	if err != nil {
		return err
	}

	kafkaSrc := source.NewKafka(ctx, cfg)
	kafkaCh := kafkaSrc.Read()

	// Build decoded record channel with Kafka virtual columns
	recordCh := make(chan engine.Record, 256)
	go func() {
		defer close(recordCh)
		for kr := range kafkaCh {
			if kr.Value == nil {
				continue // tombstone
			}
			recs, err := format.DecodeAll(dec, kr.Value)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: %v\n", err)
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

	if isWindowed {
		return runWindowedFromRecords(ctx, stmt, recordCh, flags)
	}
	if isAccumulating {
		return runAccumulatingFromRecords(ctx, stmt, recordCh)
	}
	return runNonAccumulatingFromRecords(ctx, stmt, recordCh)
}

func runNonAccumulating(ctx context.Context, stmt *ast.SelectStatement, src *source.Stdin, dec format.Decoder) error {
	rawCh := src.Read()
	recordCh := make(chan engine.Record)
	go func() {
		defer close(recordCh)
		for raw := range rawCh {
			recs, err := format.DecodeAll(dec, raw)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: %v\n", err)
				continue
			}
			for _, rec := range recs {
				select {
				case recordCh <- rec:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return runNonAccumulatingFromRecords(ctx, stmt, recordCh)
}

func runNonAccumulatingFromRecords(ctx context.Context, stmt *ast.SelectStatement, recordCh <-chan engine.Record) error {
	// Wire dedup if present
	if stmt.Deduplicate != nil {
		recordCh = applyDedup(ctx, stmt.Deduplicate, recordCh)
	}

	pipeline := &engine.Pipeline{
		Columns: stmt.Columns,
		Where:   stmt.Where,
	}

	snk := &sink.JSONSink{Writer: os.Stdout}
	outputCh := make(chan engine.Record)

	go func() {
		pipeline.Process(recordCh, outputCh)
	}()

	limit := stmt.Limit
	count := 0
	for {
		select {
		case rec, ok := <-outputCh:
			if !ok {
				return snk.Close()
			}
			if err := snk.Write(rec); err != nil {
				return fmt.Errorf("output error: %w", err)
			}
			count++
			if limit != nil && count >= *limit {
				return snk.Close()
			}
		case <-ctx.Done():
			return snk.Close()
		}
	}
}

func runAccumulating(ctx context.Context, stmt *ast.SelectStatement, src *source.Stdin, dec format.Decoder) error {
	rawCh := src.Read()
	filteredCh := make(chan engine.Record)

	go func() {
		defer close(filteredCh)
		for raw := range rawCh {
			recs, err := format.DecodeAll(dec, raw)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: %v\n", err)
				continue
			}
			for _, rec := range recs {
				if stmt.Where != nil {
					pass, err := engine.Filter(stmt.Where, rec)
					if err != nil || !pass {
						continue
					}
				}
				select {
				case filteredCh <- rec:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return runAccumulatingFromFiltered(ctx, stmt, filteredCh)
}

func runAccumulatingFromRecords(ctx context.Context, stmt *ast.SelectStatement, recordCh <-chan engine.Record) error {
	filteredCh := make(chan engine.Record)
	go func() {
		defer close(filteredCh)
		for rec := range recordCh {
			if stmt.Where != nil {
				pass, err := engine.Filter(stmt.Where, rec)
				if err != nil || !pass {
					continue
				}
			}
			select {
			case filteredCh <- rec:
			case <-ctx.Done():
				return
			}
		}
	}()
	return runAccumulatingFromFiltered(ctx, stmt, filteredCh)
}

func runAccumulatingFromFiltered(ctx context.Context, stmt *ast.SelectStatement, filteredCh <-chan engine.Record) error {
	aggCols, err := engine.ParseAggColumns(stmt.Columns, stmt.GroupBy)
	if err != nil {
		return fmt.Errorf("aggregate setup error: %w", err)
	}

	columnOrder := make([]string, len(aggCols))
	for i, col := range aggCols {
		columnOrder[i] = col.Alias
	}

	aggOp := engine.NewAggregateOp(aggCols, stmt.GroupBy, stmt.Having)

	var snk sink.Sink
	if isTTY() {
		snk = &sink.TUISink{
			Writer:      os.Stdout,
			ColumnOrder: columnOrder,
		}
	} else {
		snk = &sink.ChangelogSink{
			Writer:      os.Stdout,
			ColumnOrder: columnOrder,
		}
	}

	aggOutCh := make(chan engine.Record)
	go func() {
		aggOp.Process(filteredCh, aggOutCh)
	}()

	limit := stmt.Limit
	count := 0
	for {
		select {
		case rec, ok := <-aggOutCh:
			if !ok {
				return snk.Close()
			}
			if err := snk.Write(rec); err != nil {
				return fmt.Errorf("output error: %w", err)
			}
			count++
			if limit != nil && count >= *limit {
				return snk.Close()
			}
		case <-ctx.Done():
			return snk.Close()
		}
	}
}

// runWindowed handles windowed aggregation from stdin source.
func runWindowed(ctx context.Context, stmt *ast.SelectStatement, src *source.Stdin, dec format.Decoder, flags cliFlags) error {
	rawCh := src.Read()
	recordCh := make(chan engine.Record)
	go func() {
		defer close(recordCh)
		for raw := range rawCh {
			recs, err := format.DecodeAll(dec, raw)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: %v\n", err)
				continue
			}
			for _, rec := range recs {
				select {
				case recordCh <- rec:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return runWindowedFromRecords(ctx, stmt, recordCh, flags)
}

// runWindowedFromRecords runs a windowed aggregation query from a record channel.
func runWindowedFromRecords(ctx context.Context, stmt *ast.SelectStatement, recordCh <-chan engine.Record, flags cliFlags) error {
	// Apply WHERE filter
	filteredCh := make(chan engine.Record)
	go func() {
		defer close(filteredCh)
		for rec := range recordCh {
			if stmt.Where != nil {
				pass, err := engine.Filter(stmt.Where, rec)
				if err != nil || !pass {
					continue
				}
			}
			select {
			case filteredCh <- rec:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Apply dedup if present
	var dedupedCh <-chan engine.Record = filteredCh
	if stmt.Deduplicate != nil {
		dedupedCh = applyDedup(ctx, stmt.Deduplicate, filteredCh)
	}

	// Parse window spec
	windowSpec, err := engine.ParseWindowSpec(stmt.Window)
	if err != nil {
		return fmt.Errorf("window setup error: %w", err)
	}

	// Parse watermark delay
	var watermarkDelay time.Duration
	if stmt.Watermark != nil {
		watermarkDelay, err = engine.ParseDuration(stmt.Watermark.Duration)
		if err != nil {
			return fmt.Errorf("watermark duration error: %w", err)
		}
	} else if stmt.EventTime != nil {
		watermarkDelay = 5 * time.Second // default per spec
	}

	watermark := engine.NewWatermarkTracker(watermarkDelay)

	// Parse emit mode
	emitMode := "FINAL"
	var emitInterval time.Duration
	if stmt.Emit != nil {
		emitMode = stmt.Emit.Type
		if stmt.Emit.Interval != "" {
			emitInterval, err = engine.ParseDuration(stmt.Emit.Interval)
			if err != nil {
				return fmt.Errorf("emit interval error: %w", err)
			}
		}
	}

	// Parse aggregate columns
	aggCols, err := engine.ParseAggColumns(stmt.Columns, stmt.GroupBy)
	if err != nil {
		return fmt.Errorf("aggregate setup error: %w", err)
	}

	// Build column order for output, including window_start/window_end
	var columnOrder []string
	for _, col := range aggCols {
		columnOrder = append(columnOrder, col.Alias)
	}
	// Ensure window_start and window_end are in the order if referenced in SELECT
	hasWindowStart := false
	hasWindowEnd := false
	for _, col := range columnOrder {
		if col == "window_start" {
			hasWindowStart = true
		}
		if col == "window_end" {
			hasWindowEnd = true
		}
	}
	_ = hasWindowStart
	_ = hasWindowEnd

	// Get event time expression
	var eventTimeExpr ast.Expr
	if stmt.EventTime != nil {
		eventTimeExpr = stmt.EventTime.Expr
	}

	windowedOp := engine.NewWindowedAggregateOp(
		aggCols, stmt.GroupBy, stmt.Having,
		windowSpec, eventTimeExpr, watermark,
		emitMode, emitInterval,
	)

	// Determine output sink
	var snk sink.Sink
	if flags.stateDB != "" {
		// Determine primary keys from GROUP BY column aliases
		var pkCols []string
		for _, col := range aggCols {
			if !col.IsAggregate {
				pkCols = append(pkCols, col.Alias)
			}
		}
		// Add window_start to primary key
		pkCols = append([]string{"window_start", "window_end"}, pkCols...)

		sqliteSink, err := sink.NewSQLiteSink(flags.stateDB, columnOrder, pkCols, true)
		if err != nil {
			return fmt.Errorf("SQLite state output error: %w", err)
		}
		snk = sqliteSink
	} else if isTTY() {
		snk = &sink.TUISink{
			Writer:      os.Stdout,
			ColumnOrder: columnOrder,
		}
	} else {
		snk = &sink.ChangelogSink{
			Writer:      os.Stdout,
			ColumnOrder: columnOrder,
		}
	}

	aggOutCh := make(chan engine.Record)
	go func() {
		windowedOp.Process(dedupedCh, aggOutCh)
	}()

	limit := stmt.Limit
	count := 0
	for {
		select {
		case rec, ok := <-aggOutCh:
			if !ok {
				return snk.Close()
			}
			if err := snk.Write(rec); err != nil {
				return fmt.Errorf("output error: %w", err)
			}
			count++
			if limit != nil && count >= *limit {
				return snk.Close()
			}
		case <-ctx.Done():
			return snk.Close()
		}
	}
}

// applyDedup creates a dedup filter and returns a filtered channel.
func applyDedup(ctx context.Context, dedupClause *ast.DeduplicateClause, in <-chan engine.Record) <-chan engine.Record {
	var within time.Duration
	if dedupClause.Within != "" {
		d, err := engine.ParseDuration(dedupClause.Within)
		if err == nil {
			within = d
		}
	}
	capacity := 100000
	if dedupClause.Capacity != nil {
		capacity = *dedupClause.Capacity
	}

	df := engine.NewDedupFilter(dedupClause.Key, within, capacity)

	out := make(chan engine.Record)
	go func() {
		defer close(out)
		for rec := range in {
			if df.ShouldDrop(rec) {
				continue
			}
			select {
			case out <- rec:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func isTTY() bool {
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

func getSQL() (string, error) {
	_, remaining := parseFlags()
	return getSQLFromArgs(remaining)
}

func getSQLFromArgs(args []string) (string, error) {
	// Check for -f flag
	for i, arg := range args {
		if arg == "-f" && i+1 < len(args) {
			data, err := os.ReadFile(args[i+1])
			if err != nil {
				return "", fmt.Errorf("cannot read SQL file: %w", err)
			}
			return string(data), nil
		}
	}

	// First positional argument is the SQL
	for _, arg := range args {
		if len(arg) > 0 && arg[0] != '-' {
			return arg, nil
		}
	}

	return "", fmt.Errorf("usage: folddb <SQL> or folddb -f <file.sql>")
}
