// Command folddb executes SQL queries against streaming data sources.
package main

import (
	"fmt"
	"os"

	"github.com/kevin-cantwell/folddb/internal/engine"
	"github.com/kevin-cantwell/folddb/internal/format"
	"github.com/kevin-cantwell/folddb/internal/sink"
	"github.com/kevin-cantwell/folddb/internal/source"
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

	// For v0a: only non-accumulating queries
	if stmt.GroupBy != nil {
		return fmt.Errorf("GROUP BY is not yet supported (coming in Phase 0b)")
	}

	// Build source
	src := &source.Stdin{Reader: os.Stdin}

	// Build decoder
	dec := &format.JSONDecoder{}

	// Build pipeline
	pipeline := &engine.Pipeline{
		Columns: stmt.Columns,
		Where:   stmt.Where,
	}

	// Build sink
	snk := &sink.JSONSink{Writer: os.Stdout}

	// Wire it up: source -> decode -> pipeline -> sink
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
		if arg[0] != '-' {
			return arg, nil
		}
	}

	return "", fmt.Errorf("usage: folddb <SQL> or folddb -f <file.sql>")
}
