package sink

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/kevin-cantwell/folddb/internal/engine"

	_ "modernc.org/sqlite"
)

// SQLiteSink writes records to a SQLite database table.
// Writes are batched into transactions for performance (~100x faster than
// individual autocommit statements).
type SQLiteSink struct {
	db          *sql.DB
	tableName   string
	columnOrder []string
	primaryKeys []string // GROUP BY column names (for UPSERT)
	isAccum     bool     // true for accumulating queries (UPSERT), false for append
	initialized bool

	// Batch transaction state
	tx        *sql.Tx
	batchSize int // flush after this many writes (default 1000)
	pending   int // writes since last flush
}

// NewSQLiteSink creates a new SQLite sink.
func NewSQLiteSink(dbPath string, columnOrder []string, primaryKeys []string, isAccum bool) (*SQLiteSink, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("cannot open SQLite database: %w", err)
	}

	// Enable WAL mode for concurrent reads
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("cannot set WAL mode: %w", err)
	}

	return &SQLiteSink{
		db:          db,
		tableName:   "result",
		columnOrder: columnOrder,
		primaryKeys: primaryKeys,
		isAccum:     isAccum,
		batchSize:   1000,
	}, nil
}

// Write writes a record to the SQLite table. Writes are batched into
// transactions — the transaction is committed every batchSize writes
// or when Close() is called.
func (s *SQLiteSink) Write(rec engine.Record) error {
	if !s.initialized {
		if err := s.createTable(rec); err != nil {
			return err
		}
		s.initialized = true
	}

	// For accumulating queries with retractions, only process insertions (op=+)
	if s.isAccum && rec.Weight < 0 {
		return nil
	}

	// Begin transaction if needed
	if s.tx == nil {
		tx, err := s.db.Begin()
		if err != nil {
			return fmt.Errorf("begin transaction: %w", err)
		}
		s.tx = tx
	}

	var err error
	if s.isAccum {
		err = s.upsertTx(rec)
	} else {
		err = s.insertTx(rec)
	}
	if err != nil {
		return err
	}

	s.pending++
	if s.pending >= s.batchSize {
		return s.flushTx()
	}
	return nil
}

// flushTx commits the current transaction and resets state.
func (s *SQLiteSink) flushTx() error {
	if s.tx == nil {
		return nil
	}
	err := s.tx.Commit()
	s.tx = nil
	s.pending = 0
	return err
}

func (s *SQLiteSink) createTable(rec engine.Record) error {
	var cols []string
	var pkCols []string

	// Use columnOrder for deterministic column ordering
	columns := s.columnOrder
	if len(columns) == 0 {
		for k := range rec.Columns {
			columns = append(columns, k)
		}
	}

	isPK := make(map[string]bool)
	for _, pk := range s.primaryKeys {
		isPK[pk] = true
	}

	for _, col := range columns {
		sqlType := "TEXT" // default
		if v, ok := rec.Columns[col]; ok {
			sqlType = goTypeToSQLite(v)
		}
		cols = append(cols, fmt.Sprintf("%q %s", col, sqlType))
		if isPK[col] {
			pkCols = append(pkCols, fmt.Sprintf("%q", col))
		}
	}

	if !s.isAccum {
		cols = append([]string{"_rowid INTEGER PRIMARY KEY AUTOINCREMENT", "_ingested_at TEXT DEFAULT (datetime('now'))"}, cols...)
	}

	ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %q (%s", s.tableName, strings.Join(cols, ", "))
	if s.isAccum && len(pkCols) > 0 {
		ddl += fmt.Sprintf(", PRIMARY KEY (%s)", strings.Join(pkCols, ", "))
	}
	ddl += ")"

	_, err := s.db.Exec(ddl)
	return err
}

func (s *SQLiteSink) upsertTx(rec engine.Record) error {
	columns := s.columnOrder
	if len(columns) == 0 {
		return fmt.Errorf("column order required for upsert")
	}

	placeholders := make([]string, len(columns))
	quotedCols := make([]string, len(columns))
	values := make([]any, len(columns))
	var updateParts []string

	isPK := make(map[string]bool)
	for _, pk := range s.primaryKeys {
		isPK[pk] = true
	}

	for i, col := range columns {
		placeholders[i] = "?"
		quotedCols[i] = fmt.Sprintf("%q", col)
		if v, ok := rec.Columns[col]; ok {
			values[i] = valueToSQLite(v)
		} else {
			values[i] = nil
		}
		if !isPK[col] {
			updateParts = append(updateParts, fmt.Sprintf("%q = excluded.%q", col, col))
		}
	}

	query := fmt.Sprintf(
		"INSERT INTO %q (%s) VALUES (%s) ON CONFLICT DO UPDATE SET %s",
		s.tableName,
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(updateParts, ", "),
	)

	_, err := s.tx.Exec(query, values...)
	return err
}

func (s *SQLiteSink) insertTx(rec engine.Record) error {
	columns := s.columnOrder
	if len(columns) == 0 {
		for k := range rec.Columns {
			columns = append(columns, k)
		}
	}

	placeholders := make([]string, len(columns))
	quotedCols := make([]string, len(columns))
	values := make([]any, len(columns))

	for i, col := range columns {
		placeholders[i] = "?"
		quotedCols[i] = fmt.Sprintf("%q", col)
		if v, ok := rec.Columns[col]; ok {
			values[i] = valueToSQLite(v)
		} else {
			values[i] = nil
		}
	}

	query := fmt.Sprintf(
		"INSERT INTO %q (%s) VALUES (%s)",
		s.tableName,
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "),
	)

	_, err := s.tx.Exec(query, values...)
	return err
}

// Close flushes the pending transaction and closes the database connection.
func (s *SQLiteSink) Close() error {
	if err := s.flushTx(); err != nil {
		if s.db != nil {
			s.db.Close()
		}
		return err
	}
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func goTypeToSQLite(v engine.Value) string {
	switch v.(type) {
	case engine.IntValue:
		return "INTEGER"
	case engine.FloatValue:
		return "REAL"
	case engine.BoolValue:
		return "INTEGER"
	default:
		return "TEXT"
	}
}

func valueToSQLite(v engine.Value) any {
	if v.IsNull() {
		return nil
	}
	switch val := v.(type) {
	case engine.IntValue:
		return val.V
	case engine.FloatValue:
		return val.V
	case engine.BoolValue:
		if val.V {
			return 1
		}
		return 0
	default:
		return v.String()
	}
}
