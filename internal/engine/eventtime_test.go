package engine

import (
	"testing"
	"time"

	"github.com/kevin-cantwell/folddb/internal/sql/ast"
)

func TestExtractEventTime(t *testing.T) {
	expr := &ast.ColumnRef{Name: "ts"}

	// ISO 8601 with timezone
	rec := Record{
		Columns: map[string]Value{
			"ts": TextValue{V: "2026-03-28T10:00:05Z"},
		},
	}
	got := ExtractEventTime(expr, rec)
	want := time.Date(2026, 3, 28, 10, 0, 5, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("ExtractEventTime = %v, want %v", got, want)
	}
}

func TestExtractEventTimeISO8601NoTimezone(t *testing.T) {
	expr := &ast.ColumnRef{Name: "ts"}

	rec := Record{
		Columns: map[string]Value{
			"ts": TextValue{V: "2026-03-28T14:30:00"},
		},
	}
	got := ExtractEventTime(expr, rec)
	want := time.Date(2026, 3, 28, 14, 30, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("ExtractEventTime = %v, want %v", got, want)
	}
}

func TestExtractEventTimeUnixEpoch(t *testing.T) {
	expr := &ast.ColumnRef{Name: "ts"}

	rec := Record{
		Columns: map[string]Value{
			"ts": IntValue{V: 1743170400},
		},
	}
	got := ExtractEventTime(expr, rec)
	want := time.Unix(1743170400, 0).UTC()
	if !got.Equal(want) {
		t.Errorf("ExtractEventTime = %v, want %v", got, want)
	}
}

func TestExtractEventTimeNull(t *testing.T) {
	expr := &ast.ColumnRef{Name: "ts"}

	rec := Record{
		Columns: map[string]Value{
			"ts": NullValue{},
		},
	}
	// NULL should fall back to processing time (approximately now)
	got := ExtractEventTime(expr, rec)
	if time.Since(got) > 2*time.Second {
		t.Errorf("NULL event time should use processing time, got %v", got)
	}
}

func TestExtractEventTimeNilExpr(t *testing.T) {
	rec := Record{
		Columns: map[string]Value{"ts": TextValue{V: "2026-03-28T10:00:05Z"}},
	}
	// nil expression means processing time
	got := ExtractEventTime(nil, rec)
	if time.Since(got) > 2*time.Second {
		t.Errorf("nil expression should use processing time, got %v", got)
	}
}
