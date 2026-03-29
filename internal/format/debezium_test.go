package format

import (
	"testing"
)

func TestDebeziumCreate(t *testing.T) {
	d := &DebeziumDecoder{}
	data := []byte(`{
		"op": "c",
		"before": null,
		"after": {"id": 1, "name": "alice", "status": "active"},
		"source": {"db": "mydb", "table": "users", "ts_ms": 1700000000000}
	}`)

	recs, err := d.DecodeMulti(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}
	rec := recs[0]
	if rec.Diff != 1 {
		t.Errorf("expected diff=+1, got %d", rec.Diff)
	}
	// Check payload columns
	if rec.Get("name").String() != "alice" {
		t.Errorf("expected name=alice, got %s", rec.Get("name").String())
	}
	// Check virtual columns
	if rec.Get("_op").String() != "c" {
		t.Errorf("expected _op=c, got %s", rec.Get("_op").String())
	}
	if rec.Get("_table").String() != "users" {
		t.Errorf("expected _table=users, got %s", rec.Get("_table").String())
	}
	if rec.Get("_db").String() != "mydb" {
		t.Errorf("expected _db=mydb, got %s", rec.Get("_db").String())
	}
}

func TestDebeziumSnapshotRead(t *testing.T) {
	d := &DebeziumDecoder{}
	data := []byte(`{
		"op": "r",
		"before": null,
		"after": {"id": 1, "name": "bob"},
		"source": {"db": "testdb", "table": "orders", "ts_ms": 0}
	}`)

	recs, err := d.DecodeMulti(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}
	if recs[0].Diff != 1 {
		t.Errorf("expected diff=+1, got %d", recs[0].Diff)
	}
}

func TestDebeziumUpdateFull(t *testing.T) {
	d := &DebeziumDecoder{}
	data := []byte(`{
		"op": "u",
		"before": {"id": 1, "name": "alice", "status": "pending"},
		"after": {"id": 1, "name": "alice", "status": "shipped"},
		"source": {"db": "mydb", "table": "orders", "ts_ms": 1700000001000}
	}`)

	recs, err := d.DecodeMulti(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("expected 2 records (retraction + insertion), got %d", len(recs))
	}

	// First: retraction of old state
	if recs[0].Diff != -1 {
		t.Errorf("expected first record diff=-1, got %d", recs[0].Diff)
	}
	if recs[0].Get("status").String() != "pending" {
		t.Errorf("expected retraction status=pending, got %s", recs[0].Get("status").String())
	}

	// Second: insertion of new state
	if recs[1].Diff != 1 {
		t.Errorf("expected second record diff=+1, got %d", recs[1].Diff)
	}
	if recs[1].Get("status").String() != "shipped" {
		t.Errorf("expected insertion status=shipped, got %s", recs[1].Get("status").String())
	}
}

func TestDebeziumUpdateNullBefore(t *testing.T) {
	// REPLICA IDENTITY DEFAULT — before is null, should skip retraction
	d := &DebeziumDecoder{}
	data := []byte(`{
		"op": "u",
		"before": null,
		"after": {"id": 1, "name": "alice", "status": "shipped"},
		"source": {"db": "mydb", "table": "orders", "ts_ms": 1700000001000}
	}`)

	recs, err := d.DecodeMulti(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 record (insertion only, retraction skipped), got %d", len(recs))
	}
	if recs[0].Diff != 1 {
		t.Errorf("expected diff=+1, got %d", recs[0].Diff)
	}
}

func TestDebeziumUpdateNullAfter(t *testing.T) {
	// Malformed: after is null on an update
	d := &DebeziumDecoder{}
	data := []byte(`{
		"op": "u",
		"before": {"id": 1},
		"after": null,
		"source": {"db": "mydb", "table": "orders", "ts_ms": 0}
	}`)

	_, err := d.DecodeMulti(data)
	if err == nil {
		t.Fatal("expected error for update with null _after")
	}
}

func TestDebeziumDelete(t *testing.T) {
	d := &DebeziumDecoder{}
	data := []byte(`{
		"op": "d",
		"before": {"id": 1, "name": "alice", "status": "active"},
		"after": null,
		"source": {"db": "mydb", "table": "users", "ts_ms": 1700000002000}
	}`)

	recs, err := d.DecodeMulti(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}
	if recs[0].Diff != -1 {
		t.Errorf("expected diff=-1, got %d", recs[0].Diff)
	}
	if recs[0].Get("name").String() != "alice" {
		t.Errorf("expected name=alice, got %s", recs[0].Get("name").String())
	}
}

func TestDebeziumDeleteNullBefore(t *testing.T) {
	d := &DebeziumDecoder{}
	data := []byte(`{
		"op": "d",
		"before": null,
		"after": null,
		"source": {"db": "mydb", "table": "users", "ts_ms": 0}
	}`)

	_, err := d.DecodeMulti(data)
	if err == nil {
		t.Fatal("expected error for delete with null _before (un-retractable)")
	}
}

func TestDebeziumTruncate(t *testing.T) {
	d := &DebeziumDecoder{}
	data := []byte(`{
		"op": "t",
		"before": null,
		"after": null,
		"source": {"db": "mydb", "table": "users", "ts_ms": 0}
	}`)

	recs, err := d.DecodeMulti(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("expected 0 records for truncate, got %d", len(recs))
	}
}

func TestDebeziumVirtualColumns(t *testing.T) {
	d := &DebeziumDecoder{}
	data := []byte(`{
		"op": "c",
		"before": null,
		"after": {"id": 42},
		"source": {"db": "mydb", "table": "items", "ts_ms": 1700000000000}
	}`)

	recs, err := d.DecodeMulti(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
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

	// _before should be null
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
	// _source should be JSON
	if rec.Get("_source").Type() != "JSON" {
		t.Errorf("_source should be JSON, got %s", rec.Get("_source").Type())
	}
}
