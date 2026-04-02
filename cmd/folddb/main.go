// Command folddb executes SQL queries against streaming data sources.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/alecthomas/kong"
	"github.com/kevin-cantwell/folddb/internal/engine"
	"github.com/kevin-cantwell/folddb/internal/format"
	"github.com/kevin-cantwell/folddb/internal/sink"
	"github.com/kevin-cantwell/folddb/internal/source"
	"github.com/kevin-cantwell/folddb/internal/sql/ast"
	"github.com/kevin-cantwell/folddb/internal/sql/parser"
)

// Version is set at build time via -ldflags.
var Version = "dev"

// CLI defines the Kong command structure for folddb.
type CLI struct {
	// Default command: execute SQL
	Query QueryCmd `cmd:"" default:"withargs" help:"Execute a SQL query."`

	// Subcommands
	Serve   ServeCmd   `cmd:"" help:"Run a query and serve results via HTTP."`
	Schema  SchemaCmd  `cmd:"" help:"Print schema for a source."`
	State_  StateCmd   `cmd:"" name:"state" help:"Manage checkpoint state."`
	Version VersionCmd `cmd:"" help:"Print version."`
}

// QueryCmd is the default command that executes SQL queries.
type QueryCmd struct {
	// Positional SQL argument
	SQL  string `arg:"" optional:"" help:"SQL query to execute."`
	File string `short:"f" type:"existingfile" help:"Read SQL from file."`

	// Input
	Input string `short:"i" help:"Read input data from file (instead of stdin)." type:"existingfile"`

	// Output
	State   string        `help:"Write state to SQLite file." placeholder:"FILE"`
	Limit   int           `help:"Max output records (0=unlimited)." default:"0"`
	Timeout time.Duration `help:"Terminate after duration." default:"0s"`

	// Streaming
	Stateful           bool          `help:"Enable persistent checkpoints."`
	StateDir           string        `help:"Checkpoint directory." default:"~/.folddb/state"`
	CheckpointInterval time.Duration `help:"Checkpoint flush interval." default:"5s"`
	ArrangementMemLimit int          `help:"Max records in join arrangement memory before spilling to disk (0=unlimited)." default:"0" name:"arrangement-mem-limit"`

	// Debug
	DeadLetter string `help:"Route errors to NDJSON file." placeholder:"FILE" name:"dead-letter"`
	DryRun     bool   `help:"Print query plan without executing." name:"dry-run"`
	Explain    bool   `help:"Print query plan then execute."`

	// Profiling
	CPUProfile string `help:"Write CPU profile to file." placeholder:"FILE" name:"cpuprofile"`
}

// ServeCmd runs a query and serves results via HTTP.
type ServeCmd struct {
	SQL     string        `arg:"" help:"SQL query to execute."`
	Port    int           `help:"HTTP port." default:"8080"`
	Input   string        `short:"i" help:"Read input from file." type:"existingfile"`
	State   string        `help:"SQLite state file." placeholder:"FILE"`
	Timeout time.Duration `help:"Query timeout." default:"0s"`
}

func (c *ServeCmd) Run() error {
	return runServe(c)
}

// SchemaCmd is the "schema" subcommand.
type SchemaCmd struct {
	Args []string `arg:"" optional:"" help:"Optional source URI and FORMAT arguments."`
}

func (c *SchemaCmd) Run() error {
	return runSchema(c.Args)
}

// StateCmd is the "state" subcommand with nested subcommands.
type StateCmd struct {
	List    StateListCmd    `cmd:"" help:"List checkpointed queries."`
	Inspect StateInspectCmd `cmd:"" help:"Inspect a checkpoint."`
	Reset   StateResetCmd   `cmd:"" help:"Reset a checkpoint."`
}

// StateListCmd lists checkpointed queries.
type StateListCmd struct{}

func (c *StateListCmd) Run() error {
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
}

// StateInspectCmd inspects a specific checkpoint.
type StateInspectCmd struct {
	Hash string `arg:"" help:"Query hash to inspect."`
}

func (c *StateInspectCmd) Run() error {
	// TODO: implement detailed inspection
	fmt.Printf("Inspect checkpoint %s (not yet implemented)\n", c.Hash)
	return nil
}

// StateResetCmd resets a specific checkpoint.
type StateResetCmd struct {
	Hash string `arg:"" help:"Query hash to reset."`
}

func (c *StateResetCmd) Run() error {
	return engine.ResetCheckpoint(c.Hash)
}

// VersionCmd prints the version.
type VersionCmd struct{}

func (c *VersionCmd) Run() error {
	fmt.Printf("folddb %s\n", Version)
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// cliFlags holds parsed CLI flags, used by execution functions.
type cliFlags struct {
	stateDB            string
	stateful           bool
	stateDir           string
	checkpointInterval time.Duration
	sql                string // original SQL for fingerprinting
}

// activeDLWriter is the package-level dead letter writer, set in run().
// Used by withSchemaTracking to route schema drift errors to the DLQ.
var activeDLWriter *deadLetterWriter

func run() error {
	var cli CLI
	ctx := kong.Parse(&cli,
		kong.Name("folddb"),
		kong.Description("Execute SQL queries against streaming data sources."),
		kong.UsageOnError(),
		kong.ConfigureHelp(kong.HelpOptions{Compact: true}),
		kong.Vars{"version": Version},
	)

	// Dispatch subcommands via Kong's Run interface
	switch ctx.Command() {
	case "serve <sql>":
		return cli.Serve.Run()
	case "schema", "schema <args>":
		return cli.Schema.Run()
	case "version":
		return cli.Version.Run()
	case "state list":
		return cli.State_.List.Run()
	case "state inspect <hash>":
		return cli.State_.Inspect.Run()
	case "state reset <hash>":
		return cli.State_.Reset.Run()
	}

	// Default command: execute SQL query
	q := &cli.Query

	// CPU profiling support
	if q.CPUProfile != "" {
		f, err := os.Create(q.CPUProfile)
		if err != nil {
			return fmt.Errorf("cannot create CPU profile: %w", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			f.Close()
			return fmt.Errorf("cannot start CPU profile: %w", err)
		}
		defer func() {
			pprof.StopCPUProfile()
			f.Close()
		}()
	}

	sql, err := getSQL(q)
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
	if q.DryRun {
		printQueryPlan(stmt)
		return nil
	}

	// Handle --explain (print plan then execute)
	if q.Explain {
		printQueryPlan(stmt)
		fmt.Fprintln(os.Stderr, "---")
	}

	// Set up context with SIGINT handling
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Apply --timeout
	if q.Timeout > 0 {
		var timeoutCancel context.CancelFunc
		runCtx, timeoutCancel = context.WithTimeout(runCtx, q.Timeout)
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
	if q.DeadLetter != "" {
		dlWriter, err = newDeadLetterWriter(q.DeadLetter)
		if err != nil {
			return fmt.Errorf("cannot open dead letter file: %w", err)
		}
		activeDLWriter = dlWriter // make available to withSchemaTracking

		// Route accumulator type errors to dead letter
		engine.AggregateErrorHandler = func(aggName string, val engine.Value) {
			raw := fmt.Sprintf(`{"aggregate":"%s","value_type":"%s","value":"%v"}`, aggName, val.Type(), val.String())
			dlWriter.Write(
				fmt.Sprintf("%s received incompatible %s value", aggName, val.Type()),
				[]byte(raw), 0, 0,
			)
		}

		defer dlWriter.Close()
	}

	// Merge CLI --limit with SQL LIMIT (CLI takes precedence if set)
	if q.Limit > 0 {
		stmt.Limit = &q.Limit
	}

	// If SELECT has aggregates but no GROUP BY, create an implicit single group
	if stmt.GroupBy == nil && hasAggregateInSelect(stmt.Columns) {
		stmt.GroupBy = []ast.Expr{}
	}

	isAccumulating := stmt.GroupBy != nil
	isWindowed := stmt.Window != nil

	// Detect stream-stream join: both FROM and JOIN are Kafka URIs
	// Subquery sources are never stream-stream (they're materialized).
	isStreamStreamJoin := false
	if stmt.Join != nil && stmt.Join.Subquery == nil && stmt.From != nil && stmt.FromSubquery == nil {
		leftIsKafka := strings.HasPrefix(stmt.From.URI, "kafka://")
		rightIsKafka := strings.HasPrefix(stmt.Join.Source.URI, "kafka://")
		isStreamStreamJoin = leftIsKafka && rightIsKafka
	}

	// Validate: stream-stream joins require WITHIN interval
	if isStreamStreamJoin && stmt.Join.Within == nil {
		return fmt.Errorf("stream-stream joins require a WITHIN INTERVAL clause to bound retention")
	}

	// Build DD join operator if JOIN is present
	var joinOp *engine.DDJoinOp
	if stmt.Join != nil && !isStreamStreamJoin {
		var err error
		joinOp, err = buildDDJoinOp(stmt, q.ArrangementMemLimit)
		if err != nil {
			return err
		}
	}

	// Map CLI fields to cliFlags for execution functions
	flags := cliFlags{
		stateDB:            q.State,
		stateful:           q.Stateful,
		stateDir:           q.StateDir,
		checkpointInterval: q.CheckpointInterval,
		sql:                sql,
	}

	// FROM subquery: execute inner query, feed results into outer pipeline
	if stmt.FromSubquery != nil {
		subRecords, subErr := executeSubquery(runCtx, stmt.FromSubquery.Query)
		if subErr != nil {
			return fmt.Errorf("FROM subquery error: %w", subErr)
		}
		recordCh := make(chan engine.Record, len(subRecords))
		for _, rec := range subRecords {
			recordCh <- rec
		}
		close(recordCh)

		// Apply JOIN if present
		var finalCh <-chan engine.Record = recordCh
		if joinOp != nil {
			finalCh = applyJoin(runCtx, joinOp, recordCh)
		}

		if isWindowed {
			return runWindowedFromRecords(runCtx, stmt, finalCh, flags)
		}
		if isAccumulating {
			return runAccumulatingFromRecords(runCtx, stmt, finalCh, flags)
		}
		return runNonAccumulatingFromRecords(runCtx, stmt, finalCh)
	}

	// Determine source and build record channel
	fromURI := ""
	if stmt.From != nil {
		fromURI = stmt.From.URI
	}

	// Stream-stream Kafka join: both sides are Kafka topics
	if isStreamStreamJoin {
		return runStreamStreamKafka(runCtx, stmt, dec, isAccumulating, isWindowed, flags, dlWriter)
	}

	if fromURI != "" && strings.HasPrefix(fromURI, "kafka://") {
		return runKafka(runCtx, stmt, dec, isAccumulating, isWindowed, flags, dlWriter, joinOp)
	}

	// DuckDB file/database source: route file paths and database URIs to DuckDB.
	if fromURI != "" && isFileSource(fromURI) {
		defer closeDuckDB()

		// For non-accumulating queries without a JOIN, DuckDB can handle the
		// entire query (predicate pushdown, column pruning, ORDER BY, LIMIT).
		if !isAccumulating && !isWindowed && joinOp == nil {
			return runDuckDBNonAccumulating(runCtx, stmt)
		}

		// For accumulating/windowed queries or queries with joins, DuckDB
		// provides the source data and FoldDB handles the rest.
		recordCh := make(chan engine.Record, 256)
		go func() {
			defer close(recordCh)
			if err := duckDBScanToChannel(runCtx, fromURI, recordCh); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: DuckDB scan error: %v\n", err)
			}
		}()

		// Apply JOIN if present
		var finalCh <-chan engine.Record = recordCh
		if joinOp != nil {
			finalCh = applyJoin(runCtx, joinOp, recordCh)
		}

		if isWindowed {
			return runWindowedFromRecords(runCtx, stmt, finalCh, flags)
		}
		if isAccumulating {
			return runAccumulatingFromRecords(runCtx, stmt, finalCh, flags)
		}
		return runNonAccumulatingFromRecords(runCtx, stmt, finalCh)
	}

	// Reject unknown URI schemes
	if fromURI != "" && !strings.HasPrefix(fromURI, "stdin://") &&
		strings.Contains(fromURI, "://") {
		scheme := fromURI
		if idx := strings.Index(fromURI, "://"); idx >= 0 {
			scheme = fromURI[:idx]
		}
		return fmt.Errorf("source type %q is not supported. Supported: kafka://, file paths (.parquet, .csv, .json), pg://, mysql://", scheme)
	}

	// Reject bare file paths that weren't handled by DuckDB (unrecognized extension)
	if fromURI != "" && !strings.HasPrefix(fromURI, "stdin://") &&
		(strings.HasPrefix(fromURI, "/") || strings.HasPrefix(fromURI, "./") || strings.HasPrefix(fromURI, "../")) {
		return fmt.Errorf("cannot determine format for %q. Use a recognized extension (.parquet, .csv, .json, .ndjson, .avro) or specify FORMAT explicitly", fromURI)
	}

	// Default: stdin source (or --input file)
	var reader io.Reader = os.Stdin
	if q.Input != "" {
		f, err := os.Open(q.Input)
		if err != nil {
			return fmt.Errorf("cannot open input file: %w", err)
		}
		defer f.Close()
		reader = f
	}
	inputSrc := &source.Stdin{Reader: reader}
	if isWindowed {
		return runWindowed(runCtx, stmt, inputSrc, dec, flags, dlWriter, joinOp)
	}
	if isAccumulating {
		return runAccumulating(runCtx, stmt, inputSrc, dec, flags, dlWriter, joinOp)
	}
	return runNonAccumulating(runCtx, stmt, inputSrc, dec, dlWriter, joinOp)
}

func getSQL(q *QueryCmd) (string, error) {
	if q.File != "" {
		data, err := os.ReadFile(q.File)
		if err != nil {
			return "", fmt.Errorf("cannot read SQL file: %w", err)
		}
		return string(data), nil
	}
	if q.SQL != "" {
		return q.SQL, nil
	}
	return "", fmt.Errorf("usage: folddb <SQL> or folddb -f <file.sql>")
}

func runKafka(ctx context.Context, stmt *ast.SelectStatement, dec format.Decoder, isAccumulating, isWindowed bool, flags cliFlags, dl *deadLetterWriter, joinOp *engine.DDJoinOp) error {
	cfg, err := source.ParseKafkaURI(stmt.From.URI)
	if err != nil {
		return err
	}

	// If a Schema Registry URL is configured, use a registry-aware decoder
	if cfg.Registry != "" {
		formatStr := ""
		if stmt.From != nil {
			formatStr = stmt.From.Format
		}
		regDec, err := format.NewDecoderForKafka(formatStr, nil, cfg.Registry)
		if err != nil {
			return fmt.Errorf("registry decoder error: %w", err)
		}
		dec = regDec
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

	// Apply JOIN if present
	var finalCh <-chan engine.Record = recordCh
	if joinOp != nil {
		finalCh = applyJoin(ctx, joinOp, recordCh)
	}

	if isWindowed {
		return runWindowedFromRecords(ctx, stmt, finalCh, flags)
	}
	if isAccumulating {
		return runAccumulatingFromRecords(ctx, stmt, finalCh, flags)
	}
	return runNonAccumulatingFromRecords(ctx, stmt, finalCh)
}

func runStreamStreamKafka(ctx context.Context, stmt *ast.SelectStatement, dec format.Decoder, isAccumulating, isWindowed bool, flags cliFlags, dl *deadLetterWriter) error {
	join := stmt.Join

	// Parse WITHIN interval
	withinDuration, err := engine.ParseDuration(*join.Within)
	if err != nil {
		return fmt.Errorf("invalid WITHIN interval: %w", err)
	}

	// Parse both Kafka URIs
	leftCfg, err := source.ParseKafkaURI(stmt.From.URI)
	if err != nil {
		return fmt.Errorf("left source: %w", err)
	}
	rightCfg, err := source.ParseKafkaURI(join.Source.URI)
	if err != nil {
		return fmt.Errorf("right source: %w", err)
	}

	// Extract join keys
	streamAlias := stmt.FromAlias
	tableAlias := join.Alias
	streamKey, tableKey, err := engine.ExtractEquiJoinKeys(join.Condition, streamAlias, tableAlias)
	if err != nil {
		return fmt.Errorf("join key extraction error: %w", err)
	}

	// Create DD join operator with empty arrangements (both sides are streams)
	joinOp := engine.NewDDJoinOp(streamKey, tableKey, streamAlias, tableAlias, join.Type == "LEFT JOIN")

	// Right side decoder (may have different format)
	rightFormatStr := ""
	if join.Source != nil {
		rightFormatStr = join.Source.Format
	}
	rightDec, err := format.NewDecoder(rightFormatStr)
	if err != nil {
		return fmt.Errorf("right source decoder: %w", err)
	}

	// Single Kafka consumer reading from both topics
	leftTopic := leftCfg.Topic
	rightTopic := rightCfg.Topic
	offset := leftCfg.Offset // use left side's offset config
	group := leftCfg.Group
	if rightCfg.Group != "" && group == "" {
		group = rightCfg.Group
	}

	multiConsumer, err := source.NewKafkaMultiTopic(ctx, leftCfg.Broker, []string{leftTopic, rightTopic}, offset, group)
	if err != nil {
		return fmt.Errorf("kafka consumer error: %w", err)
	}
	multiCh := multiConsumer.Read()

	// Output channel for joined records
	outCh := make(chan engine.Record, 256)

	// Single dispatch goroutine: route by topic to left/right delta processing
	go func() {
		for kr := range multiCh {
			if kr.Value == nil {
				continue
			}
			// Pick decoder based on topic
			var topicDec format.Decoder
			if kr.Topic == rightTopic {
				topicDec = rightDec
			} else {
				topicDec = dec
			}
			recs, err := decodeWithCSVHeader(topicDec, kr.Value)
			if err != nil {
				handleDeserError(dl, err, kr.Value, kr.Offset, int64(kr.Partition))
				continue
			}
			recs = format.InjectKafkaVirtuals(recs, kr.KafkaRecord)
			// Dispatch by topic
			if kr.Topic == rightTopic {
				for _, rec := range recs {
					joinOp.ProcessRightDelta(engine.Batch{rec}, outCh)
				}
			} else {
				for _, rec := range recs {
					joinOp.ProcessLeftDelta(engine.Batch{rec}, outCh)
				}
			}
		}
	}()

	// Eviction goroutine: periodically evict expired entries
	evictInterval := withinDuration / 10
	if evictInterval < time.Second {
		evictInterval = time.Second
	}
	go func() {
		ticker := time.NewTicker(evictInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				cutoff := time.Now().Add(-withinDuration)
				joinOp.EvictAndRetract(cutoff, outCh)
			case <-ctx.Done():
				return
			}
		}
	}()

	if isWindowed {
		return runWindowedFromRecords(ctx, stmt, outCh, flags)
	}
	if isAccumulating {
		return runAccumulatingFromRecords(ctx, stmt, outCh, flags)
	}
	return runNonAccumulatingFromRecords(ctx, stmt, outCh)
}

func runNonAccumulating(ctx context.Context, stmt *ast.SelectStatement, src *source.Stdin, dec format.Decoder, dl *deadLetterWriter, joinOp *engine.DDJoinOp) error {
	recordCh := make(chan engine.Record)

	// Stream decoders handle their own framing — bypass line-based reading.
	if sd, ok := dec.(format.StreamDecoder); ok {
		go func() {
			if err := sd.DecodeStream(src.ReadRaw(), recordCh); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: stream decode error: %v\n", err)
			}
		}()
		// Apply JOIN if present
		var joinedCh <-chan engine.Record = recordCh
		if joinOp != nil {
			joinedCh = applyJoin(ctx, joinOp, recordCh)
		}
		return runNonAccumulatingFromRecords(ctx, stmt, joinedCh)
	}

	rawCh := src.Read()
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

	// Apply JOIN if present
	var finalCh <-chan engine.Record = recordCh
	if joinOp != nil {
		finalCh = applyJoin(ctx, joinOp, recordCh)
	}

	return runNonAccumulatingFromRecords(ctx, stmt, finalCh)
}

func runNonAccumulatingFromRecords(ctx context.Context, stmt *ast.SelectStatement, recordCh <-chan engine.Record) error {
	// Schema drift detection
	recordCh = withSchemaTracking(ctx, recordCh)

	// Wire dedup if present
	if stmt.Deduplicate != nil {
		recordCh = applyDedup(ctx, stmt.Deduplicate, recordCh)
	}

	pipeline := &engine.Pipeline{
		Columns: stmt.Columns,
		Where:   stmt.Where,
	}

	snk := &sink.JSONSink{Writer: os.Stdout}

	// Batch records before filter+projection to amortize channel overhead.
	batchedCh := engine.BatchChannel(recordCh, engine.DefaultBatchSize)

	outputCh := make(chan engine.Record)

	go func() {
		pipeline.ProcessBatches(batchedCh, outputCh)
	}()

	limit := stmt.Limit
	distinct := stmt.Distinct
	seen := make(map[string]struct{})
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
			if distinct {
				key := engine.RecordFingerprint(rec)
				if _, dup := seen[key]; dup {
					continue
				}
				seen[key] = struct{}{}
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

func runAccumulating(ctx context.Context, stmt *ast.SelectStatement, src *source.Stdin, dec format.Decoder, flags cliFlags, dl *deadLetterWriter, joinOp *engine.DDJoinOp) error {
	// Stream decoders handle their own framing — bypass line-based reading.
	if sd, ok := dec.(format.StreamDecoder); ok {
		recordCh := make(chan engine.Record)
		go func() {
			if err := sd.DecodeStream(src.ReadRaw(), recordCh); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: stream decode error: %v\n", err)
			}
		}()
		var joined <-chan engine.Record = recordCh
		if joinOp != nil {
			joined = applyJoin(ctx, joinOp, recordCh)
		}
		return runAccumulatingFromRecords(ctx, stmt, joined, flags)
	}

	// Decode raw lines into records
	rawCh := src.Read()
	decodedCh := make(chan engine.Record)

	go func() {
		defer close(decodedCh)
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
				case decodedCh <- rec:
				case <-ctx.Done():
					return
				}
			}
			offset++
		}
	}()

	// Apply JOIN if present (before WHERE filtering)
	var joinedCh <-chan engine.Record = decodedCh
	if joinOp != nil {
		joinedCh = applyJoin(ctx, joinOp, decodedCh)
	}

	// Batch unfiltered records. WHERE filtering is fused with aggregation
	// inside runAccumulatingFromBatches via FusedAggregateProcessor.
	return runAccumulatingFromRecords(ctx, stmt, joinedCh, flags)
}

func runAccumulatingFromRecords(ctx context.Context, stmt *ast.SelectStatement, recordCh <-chan engine.Record, flags cliFlags) error {
	// Schema drift detection
	recordCh = withSchemaTracking(ctx, recordCh)

	// Batch unfiltered records. The FusedAggregateProcessor applies WHERE
	// filtering and aggregation in a single pass per batch, eliminating the
	// intermediate filtered channel and second batch/unbatch cycle.
	var inputCount atomic.Int64
	batchedCh := engine.BatchChannel(recordCh, engine.DefaultBatchSize)
	return runAccumulatingFromBatches(ctx, stmt, batchedCh, &inputCount, flags)
}

// runAccumulatingFromBatches is the fused accumulating path. It takes pre-batched
// unfiltered records and applies WHERE filter + aggregation in a single pass per
// batch via FusedAggregateProcessor. This eliminates the intermediate filtered
// channel and second batch/unbatch cycle that runAccumulatingFromFiltered requires.
func runAccumulatingFromBatches(ctx context.Context, stmt *ast.SelectStatement, batchedCh <-chan engine.Batch, inputCount *atomic.Int64, flags cliFlags) error {
	aggCols, err := engine.ParseAggColumns(stmt.Columns, stmt.GroupBy)
	if err != nil {
		return fmt.Errorf("aggregate setup error: %w", err)
	}

	columnOrder := make([]string, len(aggCols))
	for i, col := range aggCols {
		columnOrder[i] = col.Alias
	}

	aggOp := engine.NewAggregateOp(aggCols, stmt.GroupBy, stmt.Having)

	// Convert AST ORDER BY to sink OrderBySpec
	orderBy := resolveOrderBy(stmt.OrderBy)

	var snk sink.Sink
	if flags.stateDB != "" {
		// SQLite state output
		var pkCols []string
		for _, col := range aggCols {
			if !col.IsAggregate {
				pkCols = append(pkCols, col.Alias)
			}
		}
		sqliteSink, err := sink.NewSQLiteSink(flags.stateDB, columnOrder, pkCols, true)
		if err != nil {
			return fmt.Errorf("SQLite state output error: %w", err)
		}
		snk = sqliteSink
	} else if isTTY() {
		tui := &sink.TUISink{
			Writer:      os.Stdout,
			ColumnOrder: columnOrder,
			OrderBy:     orderBy,
		}
		tui.InputCount = inputCount
		tui.Start()
		snk = tui
	} else {
		snk = &sink.ChangelogSink{
			Writer:      os.Stdout,
			ColumnOrder: columnOrder,
			OrderBy:     orderBy,
		}
	}

	// Set up checkpointing if --stateful
	restoredFromCheckpoint := false
	var cpMgr *engine.CheckpointManager
	if flags.stateful {
		cpMgr, err = engine.NewCheckpointManager(flags.sql, flags.stateDir, flags.checkpointInterval)
		if err != nil {
			return fmt.Errorf("checkpoint setup error: %w", err)
		}

		// Restore from existing checkpoint
		cp, loadErr := cpMgr.Load()
		if loadErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: cannot load checkpoint: %v (starting fresh)\n", loadErr)
		} else if cp != nil && cp.State != nil {
			if restoreErr := aggOp.UnmarshalState(cp.State); restoreErr != nil {
				fmt.Fprintf(os.Stderr, "Warning: cannot restore state: %v (starting fresh)\n", restoreErr)
			} else {
				restoredFromCheckpoint = true
				// Re-emit current state to sink so TUI/changelog shows existing data
				for _, rec := range aggOp.CurrentState() {
					if writeErr := snk.Write(rec); writeErr != nil {
						return fmt.Errorf("output error during state restore: %w", writeErr)
					}
				}
			}
		}
	}

	// SEED FROM: load seed file and feed directly to aggregator (skip if checkpoint restored).
	// Seed records are already WHERE-filtered by loadSeedRecords, so we bypass
	// the fused processor's filter and send them straight to the aggregate.
	if stmt.Seed != nil && !restoredFromCheckpoint {
		seedRecords, seedErr := loadSeedRecords(stmt)
		if seedErr != nil {
			return fmt.Errorf("seed load error: %w", seedErr)
		}
		if len(seedRecords) > 0 {
			seedOutCh := make(chan engine.Record, 256)
			go func() {
				aggOp.ProcessBatch(engine.Batch(seedRecords), seedOutCh)
				close(seedOutCh)
			}()
			for rec := range seedOutCh {
				if err := snk.Write(rec); err != nil {
					return fmt.Errorf("output error during seed: %w", err)
				}
			}
		}
	}

	// saveCheckpoint is a helper for periodic and final saves.
	saveCheckpoint := func() {
		if cpMgr == nil {
			return
		}
		stateBytes, marshalErr := aggOp.MarshalState()
		if marshalErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: checkpoint marshal error: %v\n", marshalErr)
			return
		}
		cpData := &engine.CheckpointData{
			State: stateBytes,
		}
		if saveErr := cpMgr.Save(cpData); saveErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: checkpoint save error: %v\n", saveErr)
		}
	}

	// Start periodic checkpoint ticker
	var ticker *time.Ticker
	var tickerDone chan struct{}
	if cpMgr != nil {
		interval := flags.checkpointInterval
		if interval == 0 {
			interval = 5 * time.Second
		}
		ticker = time.NewTicker(interval)
		tickerDone = make(chan struct{})
		go func() {
			defer close(tickerDone)
			for {
				select {
				case <-ticker.C:
					saveCheckpoint()
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// Fused filter+aggregate: WHERE filtering and aggregation happen in a
	// single pass per batch, eliminating the intermediate record channel.
	fused := &engine.FusedAggregateProcessor{
		Where:       stmt.Where,
		AggregateOp: aggOp,
		InputCount:  inputCount,
	}

	aggOutCh := make(chan engine.Record)
	go func() {
		fused.ProcessBatches(batchedCh, aggOutCh)
	}()

	limit := stmt.Limit
	count := 0
	var exitErr error
loop:
	for {
		select {
		case rec, ok := <-aggOutCh:
			if !ok {
				exitErr = snk.Close()
				break loop
			}
			if limit != nil && count >= *limit {
				exitErr = snk.Close()
				break loop
			}
			if err := snk.Write(rec); err != nil {
				exitErr = fmt.Errorf("output error: %w", err)
				break loop
			}
			count++
		case <-ctx.Done():
			exitErr = snk.Close()
			break loop
		}
	}

	// Final checkpoint save at shutdown
	if ticker != nil {
		ticker.Stop()
	}
	_ = tickerDone
	saveCheckpoint()

	return exitErr
}



// runWindowed handles windowed aggregation from stdin source.
func runWindowed(ctx context.Context, stmt *ast.SelectStatement, src *source.Stdin, dec format.Decoder, flags cliFlags, dl *deadLetterWriter, joinOp *engine.DDJoinOp) error {
	recordCh := make(chan engine.Record)

	// Stream decoders handle their own framing — bypass line-based reading.
	if sd, ok := dec.(format.StreamDecoder); ok {
		go func() {
			if err := sd.DecodeStream(src.ReadRaw(), recordCh); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: stream decode error: %v\n", err)
			}
		}()
		var joined <-chan engine.Record = recordCh
		if joinOp != nil {
			joined = applyJoin(ctx, joinOp, recordCh)
		}
		return runWindowedFromRecords(ctx, stmt, joined, flags)
	}

	rawCh := src.Read()
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

	var joined <-chan engine.Record = recordCh
	if joinOp != nil {
		joined = applyJoin(ctx, joinOp, recordCh)
	}
	return runWindowedFromRecords(ctx, stmt, joined, flags)
}

// runWindowedFromRecords runs a windowed aggregation query from a record channel.
func runWindowedFromRecords(ctx context.Context, stmt *ast.SelectStatement, recordCh <-chan engine.Record, flags cliFlags) error {
	// Schema drift detection
	recordCh = withSchemaTracking(ctx, recordCh)

	// SEED FROM: load seed file and prepend to stream (before WHERE filter)
	if stmt.Seed != nil {
		seedRecords, seedErr := loadSeedFile(stmt.Seed)
		if seedErr != nil {
			return fmt.Errorf("seed load error: %w", seedErr)
		}
		if len(seedRecords) > 0 {
			mergedCh := make(chan engine.Record, 256)
			go func() {
				defer close(mergedCh)
				for _, rec := range seedRecords {
					select {
					case mergedCh <- rec:
					case <-ctx.Done():
						return
					}
				}
				for rec := range recordCh {
					select {
					case mergedCh <- rec:
					case <-ctx.Done():
						return
					}
				}
			}()
			recordCh = mergedCh
		}
	}

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
		tui := &sink.TUISink{
			Writer:      os.Stdout,
			ColumnOrder: columnOrder,
			OrderBy:     resolveOrderBy(stmt.OrderBy),
		}
		tui.Start()
		snk = tui
	} else {
		snk = &sink.ChangelogSink{
			Writer:      os.Stdout,
			ColumnOrder: columnOrder,
			OrderBy:     resolveOrderBy(stmt.OrderBy),
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

// printQueryPlan outputs a human-readable query plan to stderr.
func printQueryPlan(stmt *ast.SelectStatement) {
	fmt.Fprintln(os.Stderr, "Query Plan:")

	// Source
	if stmt.FromSubquery != nil {
		fmt.Fprintf(os.Stderr, "  Source: Subquery (alias: %s)\n", stmt.FromSubquery.Alias)
	} else if stmt.From != nil && stmt.From.URI != "" {
		if strings.HasPrefix(stmt.From.URI, "kafka://") {
			fmt.Fprintf(os.Stderr, "  Source: Kafka (%s)\n", stmt.From.URI)
		} else {
			fmt.Fprintf(os.Stderr, "  Source: File/URI (%s)\n", stmt.From.URI)
		}
	} else {
		fmt.Fprintln(os.Stderr, "  Source: stdin")
	}

	// Join
	if stmt.Join != nil {
		if stmt.Join.Subquery != nil {
			fmt.Fprintf(os.Stderr, "  %s: Subquery (alias: %s)", stmt.Join.Type, stmt.Join.Subquery.Alias)
		} else {
			fmt.Fprintf(os.Stderr, "  %s: %s", stmt.Join.Type, stmt.Join.Source.URI)
		}
		if stmt.Join.Alias != "" {
			fmt.Fprintf(os.Stderr, " (alias: %s)", stmt.Join.Alias)
		}
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "  ON: %s\n", exprString(stmt.Join.Condition))
	}

	// Seed
	if stmt.Seed != nil {
		fmt.Fprintf(os.Stderr, "  Seed: %s", stmt.Seed.Source.URI)
		if stmt.Seed.Source.Format != "" {
			fmt.Fprintf(os.Stderr, " (format: %s)", stmt.Seed.Source.Format)
		}
		fmt.Fprintf(os.Stderr, "\n")
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
	case *ast.QualifiedRef:
		return v.Qualifier + "." + v.Name
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

// resolveOrderBy converts AST ORDER BY items to sink OrderBySpec.
// It resolves column references to their output names.
func resolveOrderBy(items []ast.OrderByItem) []sink.OrderBySpec {
	if len(items) == 0 {
		return nil
	}
	specs := make([]sink.OrderBySpec, 0, len(items))
	for _, item := range items {
		col := ""
		switch e := item.Expr.(type) {
		case *ast.ColumnRef:
			col = e.Name
		default:
			// For non-column-ref expressions, use the string representation.
			// This handles aliases that match aggregate output column names.
			col = exprString(item.Expr)
		}
		specs = append(specs, sink.OrderBySpec{
			Column: col,
			Desc:   item.Desc,
		})
	}
	return specs
}

// runServe implements the "serve" subcommand: runs a query pipeline and serves
// the live result set via HTTP.
func runServe(c *ServeCmd) error {
	// Parse the SQL query
	p := parser.New(c.SQL)
	stmt, err := p.Parse()
	if err != nil {
		return fmt.Errorf("parse error: %w", err)
	}

	// Set up context with SIGINT handling
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if c.Timeout > 0 {
		var timeoutCancel context.CancelFunc
		runCtx, timeoutCancel = context.WithTimeout(runCtx, c.Timeout)
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

	// If SELECT has aggregates but no GROUP BY, create an implicit single group
	if stmt.GroupBy == nil && hasAggregateInSelect(stmt.Columns) {
		stmt.GroupBy = []ast.Expr{}
	}

	isAccumulating := stmt.GroupBy != nil

	// Validate stream-stream join in serve mode
	if stmt.Join != nil && stmt.Join.Subquery == nil && stmt.From != nil && stmt.FromSubquery == nil {
		leftIsKafka := strings.HasPrefix(stmt.From.URI, "kafka://")
		rightIsKafka := strings.HasPrefix(stmt.Join.Source.URI, "kafka://")
		if leftIsKafka && rightIsKafka {
			if stmt.Join.Within == nil {
				return fmt.Errorf("stream-stream joins require a WITHIN INTERVAL clause to bound retention")
			}
			return fmt.Errorf("stream-stream joins are not yet supported in serve mode")
		}
	}

	// Build DD join operator if JOIN is present
	var joinOp *engine.DDJoinOp
	if stmt.Join != nil {
		joinOp, err = buildDDJoinOp(stmt, 0) // serve mode: in-memory only
		if err != nil {
			return err
		}
	}

	// Parse aggregate columns to get column order
	var columnOrder []string
	if isAccumulating {
		aggCols, err := engine.ParseAggColumns(stmt.Columns, stmt.GroupBy)
		if err != nil {
			return fmt.Errorf("aggregate setup error: %w", err)
		}
		columnOrder = make([]string, len(aggCols))
		for i, col := range aggCols {
			columnOrder[i] = col.Alias
		}
	} else {
		// For non-accumulating queries, derive columns from SELECT
		for _, col := range stmt.Columns {
			if col.Alias != "" {
				columnOrder = append(columnOrder, col.Alias)
			} else {
				columnOrder = append(columnOrder, exprString(col.Expr))
			}
		}
	}

	orderBy := resolveOrderBy(stmt.OrderBy)

	httpSink := &sink.HTTPSink{
		ColumnOrder: columnOrder,
		OrderBy:     orderBy,
		Port:        c.Port,
	}
	httpSink.Start()
	defer httpSink.Close()

	fmt.Fprintf(os.Stderr, "Serving on http://localhost:%d\n", c.Port)

	// Determine source
	fromURI := ""
	if stmt.From != nil {
		fromURI = stmt.From.URI
	}

	if fromURI != "" && strings.HasPrefix(fromURI, "kafka://") {
		return runServeKafka(runCtx, stmt, dec, isAccumulating, httpSink, c, joinOp)
	}

	// DuckDB file/database source in serve mode
	if fromURI != "" && isFileSource(fromURI) {
		defer closeDuckDB()
		recordCh := make(chan engine.Record, 256)
		go func() {
			defer close(recordCh)
			if err := duckDBScanToChannel(runCtx, fromURI, recordCh); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: DuckDB scan error: %v\n", err)
			}
		}()
		var finalCh <-chan engine.Record = recordCh
		if joinOp != nil {
			finalCh = applyJoin(runCtx, joinOp, recordCh)
		}
		if isAccumulating {
			return runServeAccumulatingFromRecords(runCtx, stmt, finalCh, httpSink)
		}
		return runServeNonAccumulatingFromRecords(runCtx, stmt, finalCh, httpSink)
	}

	// Default: stdin source (or --input file)
	var reader io.Reader = os.Stdin
	if c.Input != "" {
		f, err := os.Open(c.Input)
		if err != nil {
			return fmt.Errorf("cannot open input file: %w", err)
		}
		defer f.Close()
		reader = f
	}
	inputSrc := &source.Stdin{Reader: reader}

	if isAccumulating {
		return runServeAccumulating(runCtx, stmt, inputSrc, dec, httpSink, joinOp)
	}
	return runServeNonAccumulating(runCtx, stmt, inputSrc, dec, httpSink, joinOp)
}

func runServeKafka(ctx context.Context, stmt *ast.SelectStatement, dec format.Decoder, isAccumulating bool, snk *sink.HTTPSink, c *ServeCmd, joinOp *engine.DDJoinOp) error {
	cfg, err := source.ParseKafkaURI(stmt.From.URI)
	if err != nil {
		return err
	}

	// If a Schema Registry URL is configured, use a registry-aware decoder
	if cfg.Registry != "" {
		formatStr := ""
		if stmt.From != nil {
			formatStr = stmt.From.Format
		}
		regDec, err := format.NewDecoderForKafka(formatStr, nil, cfg.Registry)
		if err != nil {
			return fmt.Errorf("registry decoder error: %w", err)
		}
		dec = regDec
	}

	kafkaSrc := source.NewKafka(ctx, cfg)
	kafkaCh := kafkaSrc.Read()

	recordCh := make(chan engine.Record, 256)
	go func() {
		defer close(recordCh)
		for kr := range kafkaCh {
			if kr.Value == nil {
				continue
			}
			recs, err := decodeWithCSVHeader(dec, kr.Value)
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

	var finalCh <-chan engine.Record = recordCh
	if joinOp != nil {
		finalCh = applyJoin(ctx, joinOp, recordCh)
	}

	if isAccumulating {
		return runServeAccumulatingFromRecords(ctx, stmt, finalCh, snk)
	}
	return runServeNonAccumulatingFromRecords(ctx, stmt, finalCh, snk)
}

func runServeAccumulating(ctx context.Context, stmt *ast.SelectStatement, src *source.Stdin, dec format.Decoder, snk *sink.HTTPSink, joinOp *engine.DDJoinOp) error {
	recordCh := make(chan engine.Record)

	if sd, ok := dec.(format.StreamDecoder); ok {
		go func() {
			if err := sd.DecodeStream(src.ReadRaw(), recordCh); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: stream decode error: %v\n", err)
			}
		}()
		var joined <-chan engine.Record = recordCh
		if joinOp != nil {
			joined = applyJoin(ctx, joinOp, recordCh)
		}
		return runServeAccumulatingFromRecords(ctx, stmt, joined, snk)
	}

	rawCh := src.Read()
	go func() {
		defer close(recordCh)
		for raw := range rawCh {
			recs, err := decodeWithCSVHeader(dec, raw)
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

	var finalCh <-chan engine.Record = recordCh
	if joinOp != nil {
		finalCh = applyJoin(ctx, joinOp, recordCh)
	}
	return runServeAccumulatingFromRecords(ctx, stmt, finalCh, snk)
}

func runServeAccumulatingFromRecords(ctx context.Context, stmt *ast.SelectStatement, recordCh <-chan engine.Record, snk *sink.HTTPSink) error {
	// Schema drift detection
	recordCh = withSchemaTracking(ctx, recordCh)

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

	aggCols, err := engine.ParseAggColumns(stmt.Columns, stmt.GroupBy)
	if err != nil {
		return fmt.Errorf("aggregate setup error: %w", err)
	}
	aggOp := engine.NewAggregateOp(aggCols, stmt.GroupBy, stmt.Having)

	// Batch records before aggregation to amortize channel overhead.
	batchedCh := engine.BatchChannel(filteredCh, engine.DefaultBatchSize)

	aggOutCh := make(chan engine.Record)
	go func() {
		aggOp.ProcessBatches(batchedCh, aggOutCh)
	}()

	for {
		select {
		case rec, ok := <-aggOutCh:
			if !ok {
				return nil
			}
			if err := snk.Write(rec); err != nil {
				return fmt.Errorf("output error: %w", err)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func runServeNonAccumulating(ctx context.Context, stmt *ast.SelectStatement, src *source.Stdin, dec format.Decoder, snk *sink.HTTPSink, joinOp *engine.DDJoinOp) error {
	recordCh := make(chan engine.Record)

	if sd, ok := dec.(format.StreamDecoder); ok {
		go func() {
			if err := sd.DecodeStream(src.ReadRaw(), recordCh); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: stream decode error: %v\n", err)
			}
		}()
		var joined <-chan engine.Record = recordCh
		if joinOp != nil {
			joined = applyJoin(ctx, joinOp, recordCh)
		}
		return runServeNonAccumulatingFromRecords(ctx, stmt, joined, snk)
	}

	rawCh := src.Read()
	go func() {
		defer close(recordCh)
		for raw := range rawCh {
			recs, err := decodeWithCSVHeader(dec, raw)
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

	var finalCh <-chan engine.Record = recordCh
	if joinOp != nil {
		finalCh = applyJoin(ctx, joinOp, recordCh)
	}
	return runServeNonAccumulatingFromRecords(ctx, stmt, finalCh, snk)
}

func runServeNonAccumulatingFromRecords(ctx context.Context, stmt *ast.SelectStatement, recordCh <-chan engine.Record, snk *sink.HTTPSink) error {
	// Schema drift detection
	recordCh = withSchemaTracking(ctx, recordCh)

	pipeline := &engine.Pipeline{
		Columns: stmt.Columns,
		Where:   stmt.Where,
	}

	// Batch records before filter+projection to amortize channel overhead.
	batchedCh := engine.BatchChannel(recordCh, engine.DefaultBatchSize)

	outputCh := make(chan engine.Record)
	go func() {
		pipeline.ProcessBatches(batchedCh, outputCh)
	}()

	for {
		select {
		case rec, ok := <-outputCh:
			if !ok {
				return nil
			}
			if err := snk.Write(rec); err != nil {
				return fmt.Errorf("output error: %w", err)
			}
		case <-ctx.Done():
			return nil
		}
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

// withSchemaTracking wraps a record channel with schema drift detection.
// Records with compatible type drift pass through with a warning to stderr.
// Records with incompatible type drift are skipped with an error to stderr.
func withSchemaTracking(ctx context.Context, in <-chan engine.Record) <-chan engine.Record {
	tracker := engine.NewSchemaTracker()
	out := make(chan engine.Record, cap(in))
	go func() {
		defer close(out)
		for rec := range in {
			if err := tracker.Track(rec); err != nil {
				if activeDLWriter != nil {
					raw, _ := json.Marshal(rec.Columns)
					activeDLWriter.Write(err.Error(), raw, 0, 0)
				}
				fmt.Fprintf(os.Stderr, "Error: %v (record skipped)\n", err)
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

// loadSeedFile loads all records from a SEED FROM file (no WHERE filtering).
func loadSeedFile(seed *ast.SeedClause) ([]engine.Record, error) {
	seedFormat := ""
	if seed.Source != nil {
		seedFormat = seed.Source.Format
	}
	return loadTableFile(seed.Source.URI, seedFormat)
}

// loadSeedRecords loads and WHERE-filters records from a SEED FROM file.
func loadSeedRecords(stmt *ast.SelectStatement) ([]engine.Record, error) {
	records, err := loadSeedFile(stmt.Seed)
	if err != nil {
		return nil, err
	}

	// Apply WHERE filter to seed records
	if stmt.Where == nil {
		return records, nil
	}
	var filtered []engine.Record
	for _, rec := range records {
		pass, err := engine.Filter(stmt.Where, rec)
		if err != nil || !pass {
			continue
		}
		filtered = append(filtered, rec)
	}
	return filtered, nil
}

// buildDDJoinOp constructs a DDJoinOp from the parsed JOIN clause.
// The right (table) side is loaded into the right arrangement as initial state.
// The left (stream) side arrangement starts empty and receives deltas during streaming.
func buildDDJoinOp(stmt *ast.SelectStatement, arrangementMemLimit int) (*engine.DDJoinOp, error) {
	join := stmt.Join

	var tableRecords []engine.Record
	var err error

	if join.Subquery != nil {
		// JOIN source is a subquery — execute it to get materialized records
		ctx := context.Background()
		tableRecords, err = executeSubquery(ctx, join.Subquery.Query)
		if err != nil {
			return nil, fmt.Errorf("join subquery execution error: %w", err)
		}
	} else {
		// Load table file
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

	// Extract equi-join keys from the ON condition
	streamKey, tableKey, err := engine.ExtractEquiJoinKeys(join.Condition, streamAlias, tableAlias)
	if err != nil {
		return nil, fmt.Errorf("join key extraction error: %w", err)
	}

	op := engine.NewDDJoinOp(streamKey, tableKey, streamAlias, tableAlias, join.Type == "LEFT JOIN")

	// If arrangement memory limit is set, use disk-backed arrangements
	if arrangementMemLimit > 0 {
		op.Left = engine.NewDiskArrangement(streamKey, arrangementMemLimit, "")
		op.Right = engine.NewDiskArrangement(tableKey, arrangementMemLimit, "")
	}

	// Stream-to-file joins have a static right side — no CDC updates will arrive.
	// This lets the join skip maintaining the left arrangement entirely.
	op.RightIsStatic = true

	// Projection pushdown: only copy columns the query actually references.
	// Returns nil for SELECT * (meaning copy everything).
	op.ProjectedColumns = engine.ExtractReferencedColumns(stmt)

	// Load table records into right arrangement as initial state
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

// applyJoin wraps a record channel with DD join, processing each stream record
// as a left delta against the right arrangement.
func applyJoin(ctx context.Context, joinOp *engine.DDJoinOp, in <-chan engine.Record) <-chan engine.Record {
	out := make(chan engine.Record, 256)
	go func() {
		defer close(out)
		joinOp.ProcessLeftDeltaStream(ctx, in, out)
	}()
	return out
}
