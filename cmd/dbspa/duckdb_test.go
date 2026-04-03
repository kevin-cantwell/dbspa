package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kevin-cantwell/dbspa/internal/engine"
	"github.com/kevin-cantwell/dbspa/internal/source"
	"github.com/kevin-cantwell/dbspa/internal/sql/parser"
)

func TestLoadTableFile_CSVViaDuckDB(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "users.csv")
	if err := os.WriteFile(csvPath, []byte("id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35\n"), 0644); err != nil {
		t.Fatal(err)
	}

	records, err := loadTableFile(csvPath, "")
	if err != nil {
		t.Fatalf("loadTableFile: %v", err)
	}

	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}

	// Verify data integrity
	names := map[string]bool{}
	for _, rec := range records {
		name := rec.Get("name")
		if name.Type() != "TEXT" {
			t.Errorf("name type: expected TEXT, got %s", name.Type())
		}
		names[name.String()] = true
	}
	for _, expected := range []string{"Alice", "Bob", "Charlie"} {
		if !names[expected] {
			t.Errorf("missing name: %s", expected)
		}
	}
}

func TestLoadTableFile_JSONViaDuckDB(t *testing.T) {
	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "data.json")
	if err := os.WriteFile(jsonPath, []byte(`[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]`), 0644); err != nil {
		t.Fatal(err)
	}

	records, err := loadTableFile(jsonPath, "")
	if err != nil {
		t.Fatalf("loadTableFile: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
}

func TestLoadTableFile_NDJSONViaDuckDB(t *testing.T) {
	dir := t.TempDir()
	ndjsonPath := filepath.Join(dir, "data.ndjson")
	if err := os.WriteFile(ndjsonPath, []byte("{\"id\":1,\"name\":\"Alice\"}\n{\"id\":2,\"name\":\"Bob\"}\n"), 0644); err != nil {
		t.Fatal(err)
	}

	records, err := loadTableFile(ndjsonPath, "")
	if err != nil {
		t.Fatalf("loadTableFile: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
}

func TestLoadTableFile_DebeziumFallsBackToDBSPA(t *testing.T) {
	dir := t.TempDir()
	ndjsonPath := filepath.Join(dir, "cdc.ndjson")
	data := `{"op":"c","before":null,"after":{"id":1,"name":"Alice"},"source":{"table":"users","db":"mydb","ts_ms":1700000000000}}
{"op":"c","before":null,"after":{"id":2,"name":"Bob"},"source":{"table":"users","db":"mydb","ts_ms":1700000001000}}`
	if err := os.WriteFile(ndjsonPath, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}

	// With DEBEZIUM format hint, should use DBSPA decoder, not DuckDB
	records, err := loadTableFile(ndjsonPath, "DEBEZIUM")
	if err != nil {
		t.Fatalf("loadTableFile with DEBEZIUM: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	// Debezium decoder extracts the "after" payload, so columns should be "id" and "name"
	rec := records[0]
	if rec.Get("name").IsNull() {
		t.Error("expected non-null 'name' column from Debezium after payload")
	}
}

func TestLoadViaDuckDB_Parquet(t *testing.T) {
	// Create a Parquet file via DuckDB itself, then read it back
	duckSrc, err := source.NewDuckDBSource()
	if err != nil {
		t.Fatal(err)
	}
	defer duckSrc.Close()

	dir := t.TempDir()
	parquetPath := filepath.Join(dir, "test.parquet")

	// Create Parquet file using DuckDB
	_, err = duckSrc.Query("COPY (SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2, 'Bob') TO '" + parquetPath + "' (FORMAT PARQUET)")
	if err != nil {
		t.Fatalf("create parquet: %v", err)
	}

	// Now load it via the table loader
	records, err := loadTableFile(parquetPath, "")
	if err != nil {
		t.Fatalf("loadTableFile parquet: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
}

func TestDuckDBJoinTableLoading(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "users.csv")
	if err := os.WriteFile(csvPath, []byte("id,name\n1,Alice\n2,Bob\n3,Charlie\n"), 0644); err != nil {
		t.Fatal(err)
	}

	records, err := loadTableFile(csvPath, "")
	if err != nil {
		t.Fatalf("loadTableFile: %v", err)
	}

	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}

	for _, rec := range records {
		if rec.Get("id").IsNull() {
			t.Error("missing id column")
		}
		if rec.Get("name").IsNull() {
			t.Error("missing name column")
		}
		if rec.Weight != 1 {
			t.Errorf("expected weight=1, got %d", rec.Weight)
		}
	}
}

func TestTranslateDBSPAQueryToDuckDB(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		contains string
	}{
		{
			name:     "simple select star from parquet",
			sql:      "SELECT * FROM '/data/orders.parquet'",
			contains: "read_parquet('/data/orders.parquet')",
		},
		{
			name:     "select with where from csv",
			sql:      "SELECT * FROM '/data/orders.csv' WHERE age > 30",
			contains: "WHERE",
		},
		{
			name:     "select with limit",
			sql:      "SELECT name, age FROM '/data/users.csv' LIMIT 10",
			contains: "LIMIT 10",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := parser.New(tt.sql)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}

			query, err := translateDBSPAQueryToDuckDB(stmt)
			if err != nil {
				t.Fatalf("translate error: %v", err)
			}

			if !strings.Contains(query, tt.contains) {
				t.Errorf("DuckDB query %q does not contain %q", query, tt.contains)
			}
		})
	}
}

func TestDuckDBPredicatePushdown(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "data.csv")
	if err := os.WriteFile(csvPath, []byte("id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35\n"), 0644); err != nil {
		t.Fatal(err)
	}

	duckSrc, err := source.NewDuckDBSource()
	if err != nil {
		t.Fatal(err)
	}
	defer duckSrc.Close()

	records, err := duckSrc.Query("SELECT * FROM read_csv_auto('" + csvPath + "') WHERE age > 28")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records (age > 28), got %d", len(records))
	}

	for _, rec := range records {
		age := rec.Get("age")
		if age.Type() != "INT" {
			t.Fatalf("expected INT type for age, got %s", age.Type())
		}
		ival := age.(engine.IntValue).V
		if ival <= 28 {
			t.Errorf("predicate pushdown failed: got age=%d, expected > 28", ival)
		}
	}
}

func TestIsFileSource(t *testing.T) {
	tests := []struct {
		uri      string
		expected bool
	}{
		{"/data/file.parquet", true},
		{"/data/file.csv", true},
		{"/data/file.json", true},
		{"pg://user:pass@host/db/table", true},
		{"kafka://broker/topic", false},
		{"stdin://", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.uri, func(t *testing.T) {
			got := isFileSource(tt.uri)
			if got != tt.expected {
				t.Errorf("isFileSource(%q) = %v, want %v", tt.uri, got, tt.expected)
			}
		})
	}
}

func TestRequiresDBSPADecoder(t *testing.T) {
	tests := []struct {
		format   string
		expected bool
	}{
		{"DEBEZIUM", true},
		{"AVRO", true},
		{"PROTOBUF", true},
		{"PROTO", true},
		{"JSON", false},
		{"CSV", false},
		{"PARQUET", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.format, func(t *testing.T) {
			got := requiresDBSPADecoder(tt.format)
			if got != tt.expected {
				t.Errorf("requiresDBSPADecoder(%q) = %v, want %v", tt.format, got, tt.expected)
			}
		})
	}
}
