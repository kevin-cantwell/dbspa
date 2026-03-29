// Command folddb executes SQL queries against streaming data sources.
package main

import (
	"fmt"
	"os"

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

	// Build source
	src := &source.Stdin{Reader: os.Stdin}

	// Build decoder
	dec := &format.JSONDecoder{}

	isAccumulating := stmt.GroupBy != nil

	if isAccumulating {
		return runAccumulating(stmt, src, dec)
	}
	return runNonAccumulating(stmt, src, dec)
}

func runNonAccumulating(stmt *ast.SelectStatement, src *source.Stdin, dec *format.JSONDecoder) error {
	pipeline := &engine.Pipeline{
		Columns: stmt.Columns,
		Where:   stmt.Where,
	}

	snk := &sink.JSONSink{Writer: os.Stdout}

	rawCh := src.Read()
	recordCh := make(chan engine.Record)
	outputCh := make(chan engine.Record)

	// Decode goroutine
	go func() {
		defer close(recordCh)
		for raw := range rawCh {
			rec, err := dec.Decode(raw)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: %v\n", err)
				continue
			}
			recordCh <- rec
		}
	}()

	// Pipeline goroutine
	go func() {
		pipeline.Process(recordCh, outputCh)
	}()

	// Sink (main goroutine)
	limit := stmt.Limit
	count := 0
	for rec := range outputCh {
		if err := snk.Write(rec); err != nil {
			return fmt.Errorf("output error: %w", err)
		}
		count++
		if limit != nil && count >= *limit {
			break
		}
	}

	return snk.Close()
}

func runAccumulating(stmt *ast.SelectStatement, src *source.Stdin, dec *format.JSONDecoder) error {
	// Parse aggregate columns
	aggCols, err := engine.ParseAggColumns(stmt.Columns, stmt.GroupBy)
	if err != nil {
		return fmt.Errorf("aggregate setup error: %w", err)
	}

	// Determine column order for output
	columnOrder := make([]string, len(aggCols))
	for i, col := range aggCols {
		columnOrder[i] = col.Alias
	}

	// Create aggregate operator
	aggOp := engine.NewAggregateOp(aggCols, stmt.GroupBy, stmt.Having)

	// Select sink based on whether stdout is a TTY
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

	rawCh := src.Read()
	filteredCh := make(chan engine.Record)
	aggOutCh := make(chan engine.Record)

	// Decode + WHERE filter goroutine
	go func() {
		defer close(filteredCh)
		for raw := range rawCh {
			rec, err := dec.Decode(raw)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: %v\n", err)
				continue
			}
			// Apply WHERE filter before aggregation
			if stmt.Where != nil {
				pass, err := engine.Filter(stmt.Where, rec)
				if err != nil || !pass {
					continue
				}
			}
			filteredCh <- rec
		}
	}()

	// Aggregation goroutine
	go func() {
		aggOp.Process(filteredCh, aggOutCh)
	}()

	// Sink (main goroutine)
	limit := stmt.Limit
	count := 0
	for rec := range aggOutCh {
		if err := snk.Write(rec); err != nil {
			return fmt.Errorf("output error: %w", err)
		}
		count++
		if limit != nil && count >= *limit {
			break
		}
	}

	return snk.Close()
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
