// Command folddb executes SQL queries against streaming data sources.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

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

func run() error {
	// Check for subcommands
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "schema" {
		return runSchema(args[1:])
	}

	sql, err := getSQL()
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

	// Determine source and build record channel
	if stmt.From != nil && strings.HasPrefix(stmt.From.URI, "kafka://") {
		return runKafka(ctx, stmt, dec, isAccumulating)
	}

	// Default: stdin source
	if isAccumulating {
		return runAccumulating(ctx, stmt, &source.Stdin{Reader: os.Stdin}, dec)
	}
	return runNonAccumulating(ctx, stmt, &source.Stdin{Reader: os.Stdin}, dec)
}

func runKafka(ctx context.Context, stmt *ast.SelectStatement, dec format.Decoder, isAccumulating bool) error {
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

func isTTY() bool {
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

func getSQL() (string, error) {
	// Check for -f flag
	args := os.Args[1:]
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
