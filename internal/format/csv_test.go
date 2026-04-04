package format

import (
	"testing"
)

func TestCSVDecodeWithHeader(t *testing.T) {
	d := &CSVDecoder{Delimiter: ',', Header: true}

	// First line is the header
	_, err := d.Decode([]byte("name,age,city"))
	if err != ErrHeaderRow {
		t.Fatalf("expected ErrHeaderRow, got %v", err)
	}

	// Second line is data
	rec, err := d.Decode([]byte("alice,30,nyc"))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if rec.Columns["name"].String() != "alice" {
		t.Errorf("name: got %q, want %q", rec.Columns["name"].String(), "alice")
	}
	if rec.Columns["age"].String() != "30" {
		t.Errorf("age: got %q, want %q", rec.Columns["age"].String(), "30")
	}
	if rec.Columns["city"].String() != "nyc" {
		t.Errorf("city: got %q, want %q", rec.Columns["city"].String(), "nyc")
	}
}

func TestCSVDecodeWithoutHeader(t *testing.T) {
	d := &CSVDecoder{Delimiter: ',', Header: false}

	rec, err := d.Decode([]byte("alice,30,nyc"))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if rec.Columns["1"].String() != "alice" {
		t.Errorf("1: got %q, want %q", rec.Columns["1"].String(), "alice")
	}
	if rec.Columns["2"].String() != "30" {
		t.Errorf("2: got %q, want %q", rec.Columns["2"].String(), "30")
	}
	if rec.Columns["3"].String() != "nyc" {
		t.Errorf("3: got %q, want %q", rec.Columns["3"].String(), "nyc")
	}
}

func TestCSVDecodeTabDelimiter(t *testing.T) {
	d := &CSVDecoder{Delimiter: '\t', Header: true}

	_, err := d.Decode([]byte("name\tage"))
	if err != ErrHeaderRow {
		t.Fatalf("expected ErrHeaderRow, got %v", err)
	}

	rec, err := d.Decode([]byte("bob\t25"))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if rec.Columns["name"].String() != "bob" {
		t.Errorf("name: got %q, want %q", rec.Columns["name"].String(), "bob")
	}
}

func TestCSVDecodeQuotedFields(t *testing.T) {
	d := &CSVDecoder{Delimiter: ',', Header: false}

	rec, err := d.Decode([]byte(`"hello, world","foo"`))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if rec.Columns["1"].String() != "hello, world" {
		t.Errorf("1: got %q, want %q", rec.Columns["1"].String(), "hello, world")
	}
}

func TestCSVDecodeMultipleDataRows(t *testing.T) {
	d := &CSVDecoder{Delimiter: ',', Header: true}

	_, _ = d.Decode([]byte("x,y"))

	rec1, err := d.Decode([]byte("1,2"))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	rec2, err := d.Decode([]byte("3,4"))
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	if rec1.Columns["x"].String() != "1" {
		t.Errorf("rec1.x: got %q, want %q", rec1.Columns["x"].String(), "1")
	}
	if rec2.Columns["y"].String() != "4" {
		t.Errorf("rec2.y: got %q, want %q", rec2.Columns["y"].String(), "4")
	}
}

func TestCSVDecodeDiffAlwaysPositive(t *testing.T) {
	d := &CSVDecoder{Delimiter: ',', Header: false}
	rec, err := d.Decode([]byte("a,b"))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if rec.Weight != 1 {
		t.Errorf("expected diff=1, got %d", rec.Weight)
	}
}

func TestCSVDecodeEmpty(t *testing.T) {
	d := &CSVDecoder{Delimiter: ',', Header: false}
	_, err := d.Decode([]byte(""))
	if err == nil {
		t.Error("expected error for empty line")
	}
}
