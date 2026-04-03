package format

import (
	"bytes"
	"testing"
	"time"

	"github.com/kevin-cantwell/dbspa/internal/engine"
	"github.com/linkedin/goavro/v2"
)

const testDebeziumAvroSchema = `{
	"type": "record",
	"name": "Envelope",
	"namespace": "dbspa.test",
	"fields": [
		{"name": "op", "type": "string"},
		{"name": "before", "type": ["null", {
			"type": "record",
			"name": "Order",
			"fields": [
				{"name": "order_id", "type": "int"},
				{"name": "customer_id", "type": "int"},
				{"name": "product", "type": "string"},
				{"name": "quantity", "type": "int"},
				{"name": "price", "type": "double"},
				{"name": "total", "type": "double"},
				{"name": "status", "type": "string"},
				{"name": "region", "type": "string"},
				{"name": "updated_at", "type": "string"}
			]
		}], "default": null},
		{"name": "after", "type": ["null", "Order"], "default": null},
		{"name": "source_db", "type": "string", "default": ""},
		{"name": "source_table", "type": "string", "default": ""},
		{"name": "source_ts_ms", "type": "long", "default": 0}
	]
}`

func writeDebeziumAvroOCF(t *testing.T, records []map[string]any) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	writer, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      &buf,
		Schema: testDebeziumAvroSchema,
	})
	if err != nil {
		t.Fatalf("OCF writer error: %v", err)
	}
	for _, rec := range records {
		if err := writer.Append([]any{rec}); err != nil {
			t.Fatalf("OCF append error: %v", err)
		}
	}
	return &buf
}

func makeOrderRecord(id int32, customer int32, product string, qty int32, price, total float64, status, region, updatedAt string) map[string]any {
	return map[string]any{
		"order_id":    id,
		"customer_id": customer,
		"product":     product,
		"quantity":    qty,
		"price":       price,
		"total":       total,
		"status":      status,
		"region":      region,
		"updated_at":  updatedAt,
	}
}

func TestDebeziumAvroCreate(t *testing.T) {
	order := makeOrderRecord(1, 100, "widget", 3, 9.99, 29.97, "pending", "us-east", "2024-01-01T00:00:00Z")
	buf := writeDebeziumAvroOCF(t, []map[string]any{
		{
			"op":             "c",
			"before":         nil,
			"after":          goavro.Union("dbspa.test.Order", order),
			"source_db":      "ecommerce",
			"source_table":   "orders",
			"source_ts_ms":   int64(1700000000000),
		},
	})

	dec := &DebeziumAvroDecoder{}
	ch := make(chan engine.Record, 10)
	if err := dec.DecodeStream(buf, ch); err != nil {
		t.Fatalf("DecodeStream error: %v", err)
	}

	var recs []engine.Record
	for rec := range ch {
		recs = append(recs, rec)
	}

	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}

	rec := recs[0]
	if rec.Weight != 1 {
		t.Errorf("expected weight=+1, got %d", rec.Weight)
	}

	// Check payload columns (Avro int -> int32 -> IntValue with int64)
	if v, ok := rec.Columns["order_id"].(engine.IntValue); !ok || v.V != 1 {
		t.Errorf("order_id: got %v, want 1", rec.Columns["order_id"])
	}
	if v, ok := rec.Columns["product"].(engine.TextValue); !ok || v.V != "widget" {
		t.Errorf("product: got %v, want widget", rec.Columns["product"])
	}
	if v, ok := rec.Columns["price"].(engine.FloatValue); !ok || v.V != 9.99 {
		t.Errorf("price: got %v, want 9.99", rec.Columns["price"])
	}

	// Check virtual columns
	if rec.Get("_op").String() != "c" {
		t.Errorf("_op: got %q, want %q", rec.Get("_op").String(), "c")
	}
	if rec.Get("_table").String() != "orders" {
		t.Errorf("_table: got %q, want %q", rec.Get("_table").String(), "orders")
	}
	if rec.Get("_db").String() != "ecommerce" {
		t.Errorf("_db: got %q, want %q", rec.Get("_db").String(), "ecommerce")
	}
	if rec.Get("_before").IsNull() != true {
		t.Errorf("_before should be null for create")
	}
	if rec.Get("_ts").Type() != "TIMESTAMP" {
		t.Errorf("_ts should be TIMESTAMP, got %s", rec.Get("_ts").Type())
	}
}

func TestDebeziumAvroUpdate(t *testing.T) {
	before := makeOrderRecord(1, 100, "widget", 3, 9.99, 29.97, "pending", "us-east", "2024-01-01T00:00:00Z")
	after := makeOrderRecord(1, 100, "widget", 3, 9.99, 29.97, "shipped", "us-east", "2024-01-02T00:00:00Z")

	buf := writeDebeziumAvroOCF(t, []map[string]any{
		{
			"op":             "u",
			"before":         goavro.Union("dbspa.test.Order", before),
			"after":          goavro.Union("dbspa.test.Order", after),
			"source_db":      "ecommerce",
			"source_table":   "orders",
			"source_ts_ms":   int64(1700000001000),
		},
	})

	dec := &DebeziumAvroDecoder{}
	ch := make(chan engine.Record, 10)
	if err := dec.DecodeStream(buf, ch); err != nil {
		t.Fatalf("DecodeStream error: %v", err)
	}

	var recs []engine.Record
	for rec := range ch {
		recs = append(recs, rec)
	}

	if len(recs) != 2 {
		t.Fatalf("expected 2 records (retraction + insertion), got %d", len(recs))
	}

	// First: retraction
	if recs[0].Weight != -1 {
		t.Errorf("expected first record weight=-1, got %d", recs[0].Weight)
	}
	if recs[0].Get("status").String() != "pending" {
		t.Errorf("expected retraction status=pending, got %s", recs[0].Get("status").String())
	}

	// Second: insertion
	if recs[1].Weight != 1 {
		t.Errorf("expected second record weight=+1, got %d", recs[1].Weight)
	}
	if recs[1].Get("status").String() != "shipped" {
		t.Errorf("expected insertion status=shipped, got %s", recs[1].Get("status").String())
	}
}

func TestDebeziumAvroUpdateNullBefore(t *testing.T) {
	after := makeOrderRecord(1, 100, "widget", 3, 9.99, 29.97, "shipped", "us-east", "2024-01-02T00:00:00Z")

	buf := writeDebeziumAvroOCF(t, []map[string]any{
		{
			"op":             "u",
			"before":         nil,
			"after":          goavro.Union("dbspa.test.Order", after),
			"source_db":      "ecommerce",
			"source_table":   "orders",
			"source_ts_ms":   int64(1700000001000),
		},
	})

	dec := &DebeziumAvroDecoder{}
	ch := make(chan engine.Record, 10)
	if err := dec.DecodeStream(buf, ch); err != nil {
		t.Fatalf("DecodeStream error: %v", err)
	}

	var recs []engine.Record
	for rec := range ch {
		recs = append(recs, rec)
	}

	if len(recs) != 1 {
		t.Fatalf("expected 1 record (insertion only, retraction skipped), got %d", len(recs))
	}
	if recs[0].Weight != 1 {
		t.Errorf("expected weight=+1, got %d", recs[0].Weight)
	}
}

func TestDebeziumAvroDelete(t *testing.T) {
	before := makeOrderRecord(1, 100, "widget", 3, 9.99, 29.97, "pending", "us-east", "2024-01-01T00:00:00Z")

	buf := writeDebeziumAvroOCF(t, []map[string]any{
		{
			"op":             "d",
			"before":         goavro.Union("dbspa.test.Order", before),
			"after":          nil,
			"source_db":      "ecommerce",
			"source_table":   "orders",
			"source_ts_ms":   int64(1700000002000),
		},
	})

	dec := &DebeziumAvroDecoder{}
	ch := make(chan engine.Record, 10)
	if err := dec.DecodeStream(buf, ch); err != nil {
		t.Fatalf("DecodeStream error: %v", err)
	}

	var recs []engine.Record
	for rec := range ch {
		recs = append(recs, rec)
	}

	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}
	if recs[0].Weight != -1 {
		t.Errorf("expected weight=-1, got %d", recs[0].Weight)
	}
	if recs[0].Get("product").String() != "widget" {
		t.Errorf("expected product=widget, got %s", recs[0].Get("product").String())
	}
}

func TestDebeziumAvroDeleteNullBefore(t *testing.T) {
	buf := writeDebeziumAvroOCF(t, []map[string]any{
		{
			"op":             "d",
			"before":         nil,
			"after":          nil,
			"source_db":      "ecommerce",
			"source_table":   "orders",
			"source_ts_ms":   int64(0),
		},
	})

	dec := &DebeziumAvroDecoder{}
	ch := make(chan engine.Record, 10)
	err := dec.DecodeStream(buf, ch)
	if err == nil {
		t.Fatal("expected error for delete with null before")
	}
}

func TestDebeziumAvroTruncate(t *testing.T) {
	buf := writeDebeziumAvroOCF(t, []map[string]any{
		{
			"op":             "t",
			"before":         nil,
			"after":          nil,
			"source_db":      "ecommerce",
			"source_table":   "orders",
			"source_ts_ms":   int64(0),
		},
	})

	dec := &DebeziumAvroDecoder{}
	ch := make(chan engine.Record, 10)
	if err := dec.DecodeStream(buf, ch); err != nil {
		t.Fatalf("DecodeStream error: %v", err)
	}

	var count int
	for range ch {
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 records for truncate, got %d", count)
	}
}

func TestDebeziumAvroVirtualColumns(t *testing.T) {
	order := makeOrderRecord(42, 200, "gadget", 1, 19.99, 19.99, "confirmed", "eu-west", "2024-06-15T12:00:00Z")
	buf := writeDebeziumAvroOCF(t, []map[string]any{
		{
			"op":             "c",
			"before":         nil,
			"after":          goavro.Union("dbspa.test.Order", order),
			"source_db":      "mydb",
			"source_table":   "items",
			"source_ts_ms":   int64(1700000000000),
		},
	})

	dec := &DebeziumAvroDecoder{}
	ch := make(chan engine.Record, 10)
	if err := dec.DecodeStream(buf, ch); err != nil {
		t.Fatalf("DecodeStream error: %v", err)
	}

	var recs []engine.Record
	for rec := range ch {
		recs = append(recs, rec)
	}

	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}
	rec := recs[0]

	checks := map[string]string{
		"_op":    "c",
		"_table": "items",
		"_db":    "mydb",
	}
	for col, want := range checks {
		got := rec.Get(col).String()
		if got != want {
			t.Errorf("%s: got %q, want %q", col, got, want)
		}
	}

	// _before should be null for create
	if !rec.Get("_before").IsNull() {
		t.Errorf("_before should be null for create, got %v", rec.Get("_before"))
	}
	// _after should be JSON
	if rec.Get("_after").Type() != "JSON" {
		t.Errorf("_after should be JSON, got %s", rec.Get("_after").Type())
	}
	// _ts should be a timestamp
	if rec.Get("_ts").Type() != "TIMESTAMP" {
		t.Errorf("_ts should be TIMESTAMP, got %s", rec.Get("_ts").Type())
	}
	// Verify _ts value
	ts, ok := rec.Columns["_ts"].(engine.TimestampValue)
	if !ok {
		t.Fatalf("_ts is not TimestampValue")
	}
	expected := time.UnixMilli(1700000000000).UTC()
	if !ts.V.Equal(expected) {
		t.Errorf("_ts: got %v, want %v", ts.V, expected)
	}
}

func TestDebeziumAvroMixedOps(t *testing.T) {
	// Test a stream with multiple operations
	order1 := makeOrderRecord(1, 100, "widget", 2, 5.00, 10.00, "pending", "us-east", "2024-01-01T00:00:00Z")
	order1Updated := makeOrderRecord(1, 100, "widget", 2, 5.00, 10.00, "shipped", "us-east", "2024-01-02T00:00:00Z")
	order2 := makeOrderRecord(2, 200, "gadget", 1, 20.00, 20.00, "pending", "eu-west", "2024-01-01T00:00:00Z")

	buf := writeDebeziumAvroOCF(t, []map[string]any{
		// Create order 1
		{
			"op":             "c",
			"before":         nil,
			"after":          goavro.Union("dbspa.test.Order", order1),
			"source_db":      "ecommerce",
			"source_table":   "orders",
			"source_ts_ms":   int64(1000),
		},
		// Create order 2
		{
			"op":             "c",
			"before":         nil,
			"after":          goavro.Union("dbspa.test.Order", order2),
			"source_db":      "ecommerce",
			"source_table":   "orders",
			"source_ts_ms":   int64(2000),
		},
		// Update order 1
		{
			"op":             "u",
			"before":         goavro.Union("dbspa.test.Order", order1),
			"after":          goavro.Union("dbspa.test.Order", order1Updated),
			"source_db":      "ecommerce",
			"source_table":   "orders",
			"source_ts_ms":   int64(3000),
		},
		// Delete order 2
		{
			"op":             "d",
			"before":         goavro.Union("dbspa.test.Order", order2),
			"after":          nil,
			"source_db":      "ecommerce",
			"source_table":   "orders",
			"source_ts_ms":   int64(4000),
		},
	})

	dec := &DebeziumAvroDecoder{}
	ch := make(chan engine.Record, 20)
	if err := dec.DecodeStream(buf, ch); err != nil {
		t.Fatalf("DecodeStream error: %v", err)
	}

	var recs []engine.Record
	for rec := range ch {
		recs = append(recs, rec)
	}

	// Expected: create(+1), create(+1), update(-1,+1), delete(-1) = 5 records
	if len(recs) != 5 {
		t.Fatalf("expected 5 records, got %d", len(recs))
	}

	// Verify weights
	expectedWeights := []int{1, 1, -1, 1, -1}
	for i, want := range expectedWeights {
		if recs[i].Weight != want {
			t.Errorf("record %d: weight=%d, want %d", i, recs[i].Weight, want)
		}
	}
}
