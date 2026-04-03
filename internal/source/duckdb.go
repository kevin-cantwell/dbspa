// Package source provides stream source implementations for DBSPA.
package source

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/kevin-cantwell/dbspa/internal/engine"

	_ "github.com/marcboeker/go-duckdb" // DuckDB database/sql driver
)

// DuckDBSource executes SQL queries against DuckDB and returns results as
// engine Records. Used for file queries, database queries, and join table loading.
type DuckDBSource struct {
	db *sql.DB
}

// NewDuckDBSource creates a DuckDB source with an in-memory database.
func NewDuckDBSource() (*DuckDBSource, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("duckdb: cannot open in-memory database: %w", err)
	}
	// Verify the connection works.
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("duckdb: ping failed: %w", err)
	}
	return &DuckDBSource{db: db}, nil
}

// Query executes a SQL query and returns results as Records.
func (s *DuckDBSource) Query(query string) ([]engine.Record, error) {
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("duckdb query error: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("duckdb columns error: %w", err)
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("duckdb column types error: %w", err)
	}

	var records []engine.Record
	for rows.Next() {
		rec, err := scanRow(rows, columns, colTypes)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("duckdb rows iteration error: %w", err)
	}
	return records, nil
}

// QueryToChannel executes a SQL query and streams results to a channel.
// The channel is closed when all results have been sent or the context is cancelled.
func (s *DuckDBSource) QueryToChannel(ctx context.Context, query string, ch chan<- engine.Record) error {
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("duckdb query error: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("duckdb columns error: %w", err)
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("duckdb column types error: %w", err)
	}

	for rows.Next() {
		rec, err := scanRow(rows, columns, colTypes)
		if err != nil {
			return err
		}
		select {
		case ch <- rec:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return rows.Err()
}

// Close closes the DuckDB connection.
func (s *DuckDBSource) Close() error {
	return s.db.Close()
}

// scanRow scans a single row from sql.Rows into an engine.Record.
func scanRow(rows *sql.Rows, columns []string, colTypes []*sql.ColumnType) (engine.Record, error) {
	values := make([]any, len(columns))
	ptrs := make([]any, len(columns))
	for i := range values {
		ptrs[i] = &values[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return engine.Record{}, fmt.Errorf("duckdb scan error: %w", err)
	}

	rec := engine.Record{
		Columns:   make(map[string]engine.Value, len(columns)),
		Timestamp: time.Now(),
		Weight:    1,
	}
	for i, col := range columns {
		rec.Columns[col] = convertDuckDBValue(values[i], colTypes[i])
	}
	return rec, nil
}

// convertDuckDBValue converts a value from database/sql scanning into an engine.Value.
func convertDuckDBValue(v any, ct *sql.ColumnType) engine.Value {
	if v == nil {
		return engine.NullValue{}
	}

	switch val := v.(type) {
	case bool:
		return engine.BoolValue{V: val}
	case int8:
		return engine.IntValue{V: int64(val)}
	case int16:
		return engine.IntValue{V: int64(val)}
	case int32:
		return engine.IntValue{V: int64(val)}
	case int64:
		return engine.IntValue{V: val}
	case int:
		return engine.IntValue{V: int64(val)}
	case uint8:
		return engine.IntValue{V: int64(val)}
	case uint16:
		return engine.IntValue{V: int64(val)}
	case uint32:
		return engine.IntValue{V: int64(val)}
	case uint64:
		return engine.IntValue{V: int64(val)}
	case float32:
		return engine.FloatValue{V: float64(val)}
	case float64:
		return engine.FloatValue{V: val}
	case string:
		return engine.TextValue{V: val}
	case []byte:
		return engine.TextValue{V: string(val)}
	case time.Time:
		return engine.TimestampValue{V: val.UTC()}
	default:
		// For complex types (arrays, structs, maps), serialize to string.
		return engine.TextValue{V: fmt.Sprintf("%v", val)}
	}
}

// IsDuckDBSupported returns true if the given path or URI can be handled by DuckDB.
func IsDuckDBSupported(path string) bool {
	// File extensions DuckDB can read natively
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".parquet", ".csv", ".json", ".ndjson", ".jsonl":
		return true
	}
	// URI schemes DuckDB can handle
	if strings.HasPrefix(path, "pg://") || strings.HasPrefix(path, "postgres://") ||
		strings.HasPrefix(path, "mysql://") ||
		strings.HasPrefix(path, "s3://") ||
		strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		return true
	}
	return false
}

// TranslateToDuckDB converts a DBSPA source URI or file path into a DuckDB
// SQL table expression (e.g., "read_parquet('/path/to/file.parquet')").
func TranslateToDuckDB(uri string) (string, error) {
	// S3 paths — DuckDB reads them natively via httpfs extension
	if strings.HasPrefix(uri, "s3://") {
		ext := strings.ToLower(filepath.Ext(uri))
		switch ext {
		case ".csv":
			return fmt.Sprintf("read_csv_auto('%s')", escapeSingleQuote(uri)), nil
		case ".json", ".ndjson", ".jsonl":
			return fmt.Sprintf("read_json_auto('%s')", escapeSingleQuote(uri)), nil
		default:
			// Default to Parquet for S3
			return fmt.Sprintf("read_parquet('%s')", escapeSingleQuote(uri)), nil
		}
	}

	// HTTP(S) paths
	if strings.HasPrefix(uri, "http://") || strings.HasPrefix(uri, "https://") {
		ext := strings.ToLower(filepath.Ext(uri))
		switch ext {
		case ".csv":
			return fmt.Sprintf("read_csv_auto('%s')", escapeSingleQuote(uri)), nil
		case ".json", ".ndjson", ".jsonl":
			return fmt.Sprintf("read_json_auto('%s')", escapeSingleQuote(uri)), nil
		default:
			return fmt.Sprintf("read_parquet('%s')", escapeSingleQuote(uri)), nil
		}
	}

	// Postgres URI: pg://user:pass@host/db/table → postgres_scan(...)
	if strings.HasPrefix(uri, "pg://") || strings.HasPrefix(uri, "postgres://") {
		return translatePostgresURI(uri)
	}

	// MySQL URI: mysql://user:pass@host/db/table
	if strings.HasPrefix(uri, "mysql://") {
		return translateMySQLURI(uri)
	}

	// Local file path — detect by extension
	ext := strings.ToLower(filepath.Ext(uri))
	switch ext {
	case ".parquet":
		return fmt.Sprintf("read_parquet('%s')", escapeSingleQuote(uri)), nil
	case ".csv":
		return fmt.Sprintf("read_csv_auto('%s')", escapeSingleQuote(uri)), nil
	case ".json", ".ndjson", ".jsonl":
		return fmt.Sprintf("read_json_auto('%s')", escapeSingleQuote(uri)), nil
	default:
		return "", fmt.Errorf("duckdb: unsupported file extension %q for %s", ext, uri)
	}
}

// translatePostgresURI converts pg://user:pass@host:port/db/table to a DuckDB postgres_scan call.
func translatePostgresURI(uri string) (string, error) {
	// Normalize pg:// to postgres:// for url.Parse
	normalized := uri
	if strings.HasPrefix(uri, "pg://") {
		normalized = "postgres://" + strings.TrimPrefix(uri, "pg://")
	}
	u, err := url.Parse(normalized)
	if err != nil {
		return "", fmt.Errorf("duckdb: invalid postgres URI: %w", err)
	}

	// Path should be /dbname/tablename
	parts := strings.SplitN(strings.TrimPrefix(u.Path, "/"), "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", fmt.Errorf("duckdb: postgres URI must have format pg://user:pass@host/db/table, got %s", uri)
	}
	dbName := parts[0]
	tableName := parts[1]

	// Build a libpq-style connection string
	host := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "5432"
	}
	user := u.User.Username()
	pass, _ := u.User.Password()

	connStr := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s",
		host, port, dbName, user, pass)

	return fmt.Sprintf("postgres_scan('%s', 'public', '%s')",
		escapeSingleQuote(connStr), escapeSingleQuote(tableName)), nil
}

// translateMySQLURI converts mysql://user:pass@host:port/db/table to a DuckDB mysql_scan call.
func translateMySQLURI(uri string) (string, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", fmt.Errorf("duckdb: invalid mysql URI: %w", err)
	}

	parts := strings.SplitN(strings.TrimPrefix(u.Path, "/"), "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", fmt.Errorf("duckdb: mysql URI must have format mysql://user:pass@host/db/table, got %s", uri)
	}
	dbName := parts[0]
	tableName := parts[1]

	host := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "3306"
	}
	user := u.User.Username()
	pass, _ := u.User.Password()

	connStr := fmt.Sprintf("host=%s port=%s database=%s user=%s password=%s",
		host, port, dbName, user, pass)

	return fmt.Sprintf("mysql_scan('%s', '%s')",
		escapeSingleQuote(connStr), escapeSingleQuote(tableName)), nil
}

// escapeSingleQuote escapes single quotes for DuckDB SQL string literals.
func escapeSingleQuote(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
