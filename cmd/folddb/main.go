// Command folddb executes SQL queries against streaming data sources.
package main

import (
	"context"
	"encoding/json"
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

// Version is set at build time via -ldflags.
var Version = "dev"

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
	deadLetter         string        // --dead-letter <file>
	dryRun             bool          // --dry-run
	explain            bool          // --explain
	timeout            time.Duration // --timeout
	limit              int           // --limit (CLI-level, 0 = unlimited)
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
		case "--dead-letter":
			if i+1 < len(args) {
				i++
				flags.deadLetter = args[i]
			}
		case "--dry-run":
			flags.dryRun = true
		case "--explain":
			flags.explain = true
		case "--timeout":
			if i+1 < len(args) {
				i++
				d, err := time.ParseDuration(args[i])
				if err == nil {
					flags.timeout = d
				}
			}
		case "--limit":
			if i+1 < len(args) {
				i++
				var n int
				if _, err := fmt.Sscanf(args[i], "%d", &n); err == nil {
					flags.limit = n
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
	if len(remaining) > 0 && remaining[0] == "version" {
		fmt.Printf("folddb %s\n", Version)
		return nil
	}
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

	// Handle --dry-run
	if flags.dryRun {
		printQueryPlan(stmt)
		return nil
	}

	// Handle --explain (print plan then execute)
	if flags.explain {
		printQueryPlan(stmt)
		fmt.Fprintln(os.Stderr, "---")
	}

	// Set up context with SIGINT handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Apply --timeout
	if flags.timeout > 0 {
		var timeoutCancel context.CancelFunc
		ctx, timeoutCancel = context.WithTimeout(ctx, flags.timeout)
		defer timeoutCancel()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	// Determine format decoder
	var formatStr string
	var formatOpts map[string]string
	if stmt.From != nil {
		formatStr = stmt.From.Format
		formatOpts = stmt.From.FormatOptions
	}
	dec, err := format.NewDecoderWithOptions(formatStr, formatOpts)
	if err != nil {
		return err
	}

	// Open dead letter file if specified
	var dlWriter *deadLetterWriter
	if flags.deadLetter != "" {
		dlWriter, err = newDeadLetterWriter(flags.deadLetter)
		if err != nil {
			return fmt.Errorf("cannot open dead letter file: %w", err)
		}
		defer dlWriter.Close()
	}

	// Merge CLI --limit with SQL LIMIT (CLI takes precedence if set)
	if flags.limit > 0 {
		stmt.Limit = &flags.limit
	}

	// If SELECT has aggregates but no GROUP BY, create an implicit single group
	if stmt.GroupBy == nil && hasAggregateInSelect(stmt.Columns) {
		stmt.GroupBy = []ast.Expr{}
	}

	isAccumulating := stmt.GroupBy != nil
	isWindowed := stmt.Window != nil

	// Determine source and build record channel
	fromURI := ""
	if stmt.From != nil {
		fromURI = stmt.From.URI
	}
	if fromURI != "" && strings.HasPrefix(fromURI, "kafka://") {
		return runKafka(ctx, stmt, dec, isAccumulating, isWindowed, flags, dlWriter)
	}

	// Default: stdin source
	stdinSrc := &source.Stdin{Reader: os.Stdin}
	if isWindowed {
		return runWindowed(ctx, stmt, stdinSrc, dec, flags, dlWriter)
	}
	if isAccumulating {
		return runAccumulating(ctx, stmt, stdinSrc, dec, dlWriter)
	}
	return runNonAccumulating(ctx, stmt, stdinSrc, dec, dlWriter)
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

func runKafka(ctx context.Context, stmt *ast.SelectStatement, dec format.Decoder, isAccumulating, isWindowed bool, flags cliFlags, dl *deadLetterWriter) error {
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
			recs, err := decodeWithCSVHeader(dec, kr.Value)
			if err != nil {
				handleDeserError(dl, err, kr.Value, kr.Offset, int64(kr.Partition))
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

func runNonAccumulating(ctx context.Context, stmt *ast.SelectStatement, src *source.Stdin, dec format.Decoder, dl *deadLetterWriter) error {
	rawCh := src.Read()
	recordCh := make(chan engine.Record)
	go func() {
		defer close(recordCh)
		var offset int64
		for raw := range rawCh {
			recs, err := decodeWithCSVHeader(dec, raw)
			if err != nil {
				handleDeserError(dl, err, raw, offset, 0)
				offset++
				continue
			}
			for _, rec := range recs {
				select {
				case recordCh <- rec:
				case <-ctx.Done():
					return
				}
			}
			offset++
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
			if limit != nil && count >= *limit {
				return snk.Close()
			}
			if err := snk.Write(rec); err != nil {
				return fmt.Errorf("output error: %w", err)
			}
			count++
		case <-ctx.Done():
			return snk.Close()
		}
	}
}

func runAccumulating(ctx context.Context, stmt *ast.SelectStatement, src *source.Stdin, dec format.Decoder, dl *deadLetterWriter) error {
	rawCh := src.Read()
	filteredCh := make(chan engine.Record)

	go func() {
		defer close(filteredCh)
		var offset int64
		for raw := range rawCh {
			recs, err := decodeWithCSVHeader(dec, raw)
			if err != nil {
				handleDeserError(dl, err, raw, offset, 0)
				offset++
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
			offset++
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
			if limit != nil && count >= *limit {
				return snk.Close()
			}
			if err := snk.Write(rec); err != nil {
				return fmt.Errorf("output error: %w", err)
			}
			count++
		case <-ctx.Done():
			return snk.Close()
		}
	}
}

// runWindowed handles windowed aggregation from stdin source.
func runWindowed(ctx context.Context, stmt *ast.SelectStatement, src *source.Stdin, dec format.Decoder, flags cliFlags, dl *deadLetterWriter) error {
	rawCh := src.Read()
	recordCh := make(chan engine.Record)
	go func() {
		defer close(recordCh)
		var offset int64
		for raw := range rawCh {
			recs, err := decodeWithCSVHeader(dec, raw)
			if err != nil {
				handleDeserError(dl, err, raw, offset, 0)
				offset++
				continue
			}
			for _, rec := range recs {
				select {
				case recordCh <- rec:
				case <-ctx.Done():
					return
				}
			}
			offset++
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
			if limit != nil && count >= *limit {
				return snk.Close()
			}
			if err := snk.Write(rec); err != nil {
				return fmt.Errorf("output error: %w", err)
			}
			count++
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

// printQueryPlan outputs a human-readable query plan to stderr.
func printQueryPlan(stmt *ast.SelectStatement) {
	fmt.Fprintln(os.Stderr, "Query Plan:")

	// Source
	if stmt.From != nil && stmt.From.URI != "" {
		if strings.HasPrefix(stmt.From.URI, "kafka://") {
			fmt.Fprintf(os.Stderr, "  Source: Kafka (%s)\n", stmt.From.URI)
		} else {
			fmt.Fprintf(os.Stderr, "  Source: File/URI (%s)\n", stmt.From.URI)
		}
	} else {
		fmt.Fprintln(os.Stderr, "  Source: stdin")
	}

	// Format
	if stmt.From != nil && stmt.From.Format != "" {
		fmt.Fprintf(os.Stderr, "  Format: %s\n", stmt.From.Format)
		if stmt.From.FormatOptions != nil {
			for k, v := range stmt.From.FormatOptions {
				fmt.Fprintf(os.Stderr, "    %s = %s\n", k, v)
			}
		}
	} else {
		fmt.Fprintln(os.Stderr, "  Format: JSON (default)")
	}

	// Columns
	fmt.Fprintf(os.Stderr, "  Columns: %d\n", len(stmt.Columns))
	for _, col := range stmt.Columns {
		if col.Alias != "" {
			fmt.Fprintf(os.Stderr, "    - %s (alias: %s)\n", exprString(col.Expr), col.Alias)
		} else {
			fmt.Fprintf(os.Stderr, "    - %s\n", exprString(col.Expr))
		}
	}

	// WHERE
	if stmt.Where != nil {
		fmt.Fprintf(os.Stderr, "  Filter: %s\n", exprString(stmt.Where))
	}

	// GROUP BY
	if stmt.GroupBy != nil {
		var keys []string
		for _, e := range stmt.GroupBy {
			keys = append(keys, exprString(e))
		}
		fmt.Fprintf(os.Stderr, "  Group By: %s\n", strings.Join(keys, ", "))
	}

	// HAVING
	if stmt.Having != nil {
		fmt.Fprintf(os.Stderr, "  Having: %s\n", exprString(stmt.Having))
	}

	// Window
	if stmt.Window != nil {
		fmt.Fprintf(os.Stderr, "  Window: %s %s\n", stmt.Window.Type, stmt.Window.Size)
		if stmt.Window.SlideBy != "" {
			fmt.Fprintf(os.Stderr, "    Slide By: %s\n", stmt.Window.SlideBy)
		}
	}

	// Event Time
	if stmt.EventTime != nil {
		fmt.Fprintf(os.Stderr, "  Event Time: %s\n", exprString(stmt.EventTime.Expr))
	}

	// Watermark
	if stmt.Watermark != nil {
		fmt.Fprintf(os.Stderr, "  Watermark: %s\n", stmt.Watermark.Duration)
	}

	// Emit
	if stmt.Emit != nil {
		fmt.Fprintf(os.Stderr, "  Emit: %s\n", stmt.Emit.Type)
		if stmt.Emit.Interval != "" {
			fmt.Fprintf(os.Stderr, "    Interval: %s\n", stmt.Emit.Interval)
		}
	}

	// Limit
	if stmt.Limit != nil {
		fmt.Fprintf(os.Stderr, "  Limit: %d\n", *stmt.Limit)
	}

	// Query type
	if stmt.Window != nil {
		fmt.Fprintln(os.Stderr, "  Type: Windowed aggregation")
	} else if stmt.GroupBy != nil {
		fmt.Fprintln(os.Stderr, "  Type: Accumulating aggregation")
	} else {
		fmt.Fprintln(os.Stderr, "  Type: Non-accumulating (filter/project)")
	}
}

// exprString returns a simple string representation of an AST expression.
func exprString(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.StarExpr:
		return "*"
	case *ast.ColumnRef:
		return v.Name
	case *ast.NumberLiteral:
		return v.Value
	case *ast.StringLiteral:
		return "'" + v.Value + "'"
	case *ast.BoolLiteral:
		if v.Value {
			return "TRUE"
		}
		return "FALSE"
	case *ast.NullLiteral:
		return "NULL"
	case *ast.BinaryExpr:
		return exprString(v.Left) + " " + v.Op + " " + exprString(v.Right)
	case *ast.UnaryExpr:
		return v.Op + " " + exprString(v.Expr)
	case *ast.FunctionCall:
		var args []string
		for _, a := range v.Args {
			args = append(args, exprString(a))
		}
		return v.Name + "(" + strings.Join(args, ", ") + ")"
	case *ast.CastExpr:
		return exprString(v.Expr) + "::" + v.TypeName
	case *ast.JsonAccessExpr:
		op := "->"
		if v.AsText {
			op = "->>"
		}
		return exprString(v.Left) + op + exprString(v.Key)
	default:
		return fmt.Sprintf("%T", e)
	}
}

// hasAggregateInSelect returns true if any column in the SELECT list is an aggregate function.
func hasAggregateInSelect(columns []ast.Column) bool {
	for _, col := range columns {
		if fc, ok := col.Expr.(*ast.FunctionCall); ok {
			if engine.IsAggregateFunc(fc.Name) {
				return true
			}
		}
	}
	return false
}

// decodeWithCSVHeader handles CSV header row skipping transparently.
func decodeWithCSVHeader(dec format.Decoder, data []byte) ([]engine.Record, error) {
	recs, err := format.DecodeAll(dec, data)
	if err == format.ErrHeaderRow {
		return nil, nil // skip header, not an error
	}
	return recs, err
}

// deadLetterWriter writes deserialization errors to a file as NDJSON.
type deadLetterWriter struct {
	f *os.File
	e *json.Encoder
}

func newDeadLetterWriter(path string) (*deadLetterWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &deadLetterWriter{f: f, e: json.NewEncoder(f)}, nil
}

func (w *deadLetterWriter) Write(errMsg string, raw []byte, offset, partition int64) {
	w.e.Encode(map[string]any{
		"error":     errMsg,
		"raw":       string(raw),
		"offset":    offset,
		"partition": partition,
	})
}

func (w *deadLetterWriter) Close() error {
	return w.f.Close()
}

// handleDeserError routes deserialization errors to dead letter file or stderr.
func handleDeserError(dl *deadLetterWriter, err error, raw []byte, offset, partition int64) {
	if err == nil {
		return
	}
	if dl != nil {
		dl.Write(err.Error(), raw, offset, partition)
	} else {
		fmt.Fprintf(os.Stderr, "Warning: %v\n", err)
	}
}
