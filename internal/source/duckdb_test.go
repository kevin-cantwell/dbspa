package source

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kevin-cantwell/folddb/internal/engine"
)

func TestNewDuckDBSource(t *testing.T) {
	src, err := NewDuckDBSource()
	if err != nil {
		t.Fatalf("NewDuckDBSource: %v", err)
	}
	defer src.Close()
}

func TestDuckDBSource_QueryCSV(t *testing.T) {
	// Write a temp CSV file
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "test.csv")
	err := os.WriteFile(csvPath, []byte("id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35\n"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	src, err := NewDuckDBSource()
	if err != nil {
		t.Fatalf("NewDuckDBSource: %v", err)
	}
	defer src.Close()

	records, err := src.Query("SELECT * FROM read_csv_auto('" + csvPath + "')")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}

	// Check first record
	name := records[0].Get("name")
	if name.String() != "Alice" {
		t.Errorf("expected name=Alice, got %s", name.String())
	}

	// Check weights are set
	for _, rec := range records {
		if rec.Weight != 1 {
			t.Errorf("expected Weight=1, got %d", rec.Weight)
		}
	}
}

func TestDuckDBSource_QueryJSON(t *testing.T) {
	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "test.json")
	err := os.WriteFile(jsonPath, []byte(`[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]`), 0644)
	if err != nil {
		t.Fatal(err)
	}

	src, err := NewDuckDBSource()
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	records, err := src.Query("SELECT * FROM read_json_auto('" + jsonPath + "')")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
}

func TestDuckDBSource_QueryWithWhere(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "test.csv")
	err := os.WriteFile(csvPath, []byte("id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35\n"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	src, err := NewDuckDBSource()
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	records, err := src.Query("SELECT * FROM read_csv_auto('" + csvPath + "') WHERE age > 28")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records (age > 28), got %d", len(records))
	}
}

func TestDuckDBSource_QueryToChannel(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "test.csv")
	err := os.WriteFile(csvPath, []byte("id,name,age\n1,Alice,30\n2,Bob,25\n"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	src, err := NewDuckDBSource()
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	ch := make(chan engine.Record, 10)
	ctx := context.Background()

	err = src.QueryToChannel(ctx, "SELECT * FROM read_csv_auto('"+csvPath+"')", ch)
	if err != nil {
		t.Fatalf("QueryToChannel: %v", err)
	}
	close(ch)

	var records []engine.Record
	for rec := range ch {
		records = append(records, rec)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
}

func TestDuckDBSource_TypeConversion(t *testing.T) {
	src, err := NewDuckDBSource()
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	records, err := src.Query(`
		SELECT
			42::INTEGER as int_val,
			3.14::DOUBLE as float_val,
			'hello'::VARCHAR as text_val,
			true::BOOLEAN as bool_val,
			NULL as null_val
	`)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	rec := records[0]

	if rec.Get("int_val").Type() != "INT" {
		t.Errorf("int_val type: expected INT, got %s", rec.Get("int_val").Type())
	}
	if rec.Get("float_val").Type() != "FLOAT" {
		t.Errorf("float_val type: expected FLOAT, got %s", rec.Get("float_val").Type())
	}
	if rec.Get("text_val").Type() != "TEXT" {
		t.Errorf("text_val type: expected TEXT, got %s", rec.Get("text_val").Type())
	}
	if rec.Get("bool_val").Type() != "BOOL" {
		t.Errorf("bool_val type: expected BOOL, got %s", rec.Get("bool_val").Type())
	}
	if rec.Get("null_val").Type() != "NULL" {
		t.Errorf("null_val type: expected NULL, got %s", rec.Get("null_val").Type())
	}
}

func TestTranslateToDuckDB(t *testing.T) {
	tests := []struct {
		input    string
		expected string
		wantErr  bool
	}{
		{"/data/orders.parquet", "read_parquet('/data/orders.parquet')", false},
		{"/data/orders.csv", "read_csv_auto('/data/orders.csv')", false},
		{"/data/orders.json", "read_json_auto('/data/orders.json')", false},
		{"/data/orders.ndjson", "read_json_auto('/data/orders.ndjson')", false},
		{"/data/orders.jsonl", "read_json_auto('/data/orders.jsonl')", false},
		{"s3://bucket/data.parquet", "read_parquet('s3://bucket/data.parquet')", false},
		{"s3://bucket/data.csv", "read_csv_auto('s3://bucket/data.csv')", false},
		{"https://example.com/data.csv", "read_csv_auto('https://example.com/data.csv')", false},
		{"/data/orders.avro", "", true}, // Avro not supported by DuckDB
		// Postgres URI
		{"pg://user:pass@localhost/mydb/orders", "postgres_scan('host=localhost port=5432 dbname=mydb user=user password=pass', 'public', 'orders')", false},
		// MySQL URI
		{"mysql://user:pass@localhost/mydb/orders", "mysql_scan('host=localhost port=3306 database=mydb user=user password=pass', 'orders')", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := TranslateToDuckDB(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error for %s, got nil", tt.input)
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error for %s: %v", tt.input, err)
				return
			}
			if result != tt.expected {
				t.Errorf("TranslateToDuckDB(%s):\n  got  %s\n  want %s", tt.input, result, tt.expected)
			}
		})
	}
}

func TestIsDuckDBSupported(t *testing.T) {
	tests := []struct {
		path     string
		expected bool
	}{
		{"/data/orders.parquet", true},
		{"/data/orders.csv", true},
		{"/data/orders.json", true},
		{"/data/orders.ndjson", true},
		{"/data/orders.avro", false},
		{"/data/orders.proto", false},
		{"pg://user:pass@host/db/table", true},
		{"mysql://user:pass@host/db/table", true},
		{"s3://bucket/data.parquet", true},
		{"kafka://broker/topic", false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := IsDuckDBSupported(tt.path)
			if result != tt.expected {
				t.Errorf("IsDuckDBSupported(%s) = %v, want %v", tt.path, result, tt.expected)
			}
		})
	}
}
