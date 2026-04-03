package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/kevin-cantwell/dbspa/internal/engine"
	"github.com/kevin-cantwell/dbspa/internal/format"
	"github.com/kevin-cantwell/dbspa/internal/source"
)

// loadTableFile loads all records from a file into a slice.
// For formats supported by DuckDB (Parquet, CSV, JSON), it routes through DuckDB
// for better performance (predicate pushdown, column pruning). Falls back to
// DBSPA's own decoders for Avro, Protobuf, Debezium, or if DuckDB fails.
func loadTableFile(path string, formatHint string) ([]engine.Record, error) {
	// If the format is explicitly set to a streaming/envelope format that
	// DuckDB can't interpret (Debezium, Avro, Protobuf), always use DBSPA decoders.
	if requiresDBSPADecoder(formatHint) {
		return loadViaDBSPA(path, formatHint)
	}

	// Try DuckDB first for supported formats
	if source.IsDuckDBSupported(path) {
		records, err := loadViaDuckDB(path)
		if err != nil {
			// Fall back to DBSPA decoders with a warning
			fmt.Fprintf(os.Stderr, "Warning: DuckDB load failed (%v), falling back to DBSPA decoder\n", err)
		} else {
			return records, nil
		}
	}

	// Fall back to DBSPA's own decoders
	return loadViaDBSPA(path, formatHint)
}

// requiresDBSPADecoder returns true for format hints that indicate an envelope
// or streaming format that DuckDB cannot interpret (e.g., Debezium CDC, Avro, Protobuf).
func requiresDBSPADecoder(formatHint string) bool {
	switch strings.ToUpper(formatHint) {
	case "DEBEZIUM", "AVRO", "PROTOBUF", "PROTO":
		return true
	}
	return false
}

// loadViaDuckDB loads a file using DuckDB's native scanners.
func loadViaDuckDB(path string) ([]engine.Record, error) {
	duckExpr, err := source.TranslateToDuckDB(path)
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf("SELECT * FROM %s", duckExpr)

	duckSrc, err := getDuckDB()
	if err != nil {
		return nil, err
	}
	return duckSrc.Query(query)
}

// loadViaDBSPA loads a file using DBSPA's built-in decoders.
func loadViaDBSPA(path string, formatHint string) ([]engine.Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cannot open table file: %w", err)
	}
	defer f.Close()

	// Detect format from extension if not specified
	if formatHint == "" {
		formatHint = detectTableFormat(path)
	}

	dec, err := format.NewDecoder(formatHint)
	if err != nil {
		return nil, fmt.Errorf("cannot create decoder for table file: %w", err)
	}

	// Stream decoders (Parquet, Avro) need special handling
	if sd, ok := dec.(format.StreamDecoder); ok {
		return loadViaStream(sd, f)
	}

	// Line-based decoders (JSON, CSV)
	return loadLineByLine(dec, f)
}

func detectTableFormat(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".parquet":
		return "PARQUET"
	case ".csv":
		return "CSV"
	case ".avro":
		return "AVRO"
	case ".json", ".ndjson", ".jsonl":
		return "JSON"
	default:
		return "JSON"
	}
}

func loadViaStream(sd format.StreamDecoder, r io.Reader) ([]engine.Record, error) {
	ch := make(chan engine.Record, 256)
	var streamErr error
	go func() {
		streamErr = sd.DecodeStream(r, ch)
	}()

	var records []engine.Record
	for rec := range ch {
		records = append(records, rec)
	}
	if streamErr != nil {
		return nil, streamErr
	}
	return records, nil
}

func loadLineByLine(dec format.Decoder, r io.Reader) ([]engine.Record, error) {
	scanner := bufio.NewScanner(r)
	// Allow up to 10MB lines
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)

	var records []engine.Record
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		recs, err := format.DecodeAll(dec, line)
		if err != nil {
			if err == format.ErrHeaderRow {
				continue
			}
			return nil, fmt.Errorf("table file decode error: %w", err)
		}
		records = append(records, recs...)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("table file read error: %w", err)
	}
	return records, nil
}
