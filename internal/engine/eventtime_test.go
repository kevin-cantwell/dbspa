package engine

import (
	"testing"
	"time"

	"github.com/kevin-cantwell/dbspa/internal/sql/ast"
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

// ISO 8601 with timezone offsets
func TestExtractEventTimeWithTimezoneOffset(t *testing.T) {
	tests := []struct {
		name string
		ts   string
		want time.Time
	}{
		{
			name: "positive offset +05:30",
			ts:   "2026-03-28T15:30:00+05:30",
			want: time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC),
		},
		{
			name: "negative offset -07:00",
			ts:   "2026-03-28T03:00:00-07:00",
			want: time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC),
		},
		{
			name: "Z suffix",
			ts:   "2026-03-28T10:00:00Z",
			want: time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC),
		},
	}

	expr := &ast.ColumnRef{Name: "ts"}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := Record{Columns: map[string]Value{"ts": TextValue{V: tt.ts}}}
			got := ExtractEventTime(expr, rec)
			if !got.Equal(tt.want) {
				t.Errorf("ExtractEventTime(%q) = %v, want %v", tt.ts, got, tt.want)
			}
		})
	}
}

// Unix epoch: integer and float
func TestExtractEventTimeUnixEpochFormats(t *testing.T) {
	expr := &ast.ColumnRef{Name: "ts"}
	baseUnix := int64(1743170400) // some fixed epoch

	tests := []struct {
		name string
		val  Value
		want time.Time
	}{
		{
			name: "integer seconds",
			val:  IntValue{V: baseUnix},
			want: time.Unix(baseUnix, 0).UTC(),
		},
		{
			name: "float seconds with fractional",
			val:  FloatValue{V: float64(baseUnix) + 0.5},
			want: time.Unix(baseUnix, 500_000_000).UTC(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := Record{Columns: map[string]Value{"ts": tt.val}}
			got := ExtractEventTime(expr, rec)
			if !got.Equal(tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// parseTimestampString: string that looks like milliseconds epoch
func TestParseTimestampStringMilliseconds(t *testing.T) {
	// 1743170400000 ms = 1743170400 seconds
	ts, err := parseTimestampString("1743170400000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := time.Unix(1743170400, 0).UTC()
	if !ts.Equal(want) {
		t.Errorf("millisecond epoch parse = %v, want %v", ts, want)
	}
}

// parseTimestampString: nanoseconds epoch
func TestParseTimestampStringNanoseconds(t *testing.T) {
	ns := int64(1743170400_000_000_000)
	ts, err := parseTimestampString("1743170400000000000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := time.Unix(0, ns).UTC()
	if !ts.Equal(want) {
		t.Errorf("nanosecond epoch parse = %v, want %v", ts, want)
	}
}

// Various date/time formats
func TestParseTimestampStringFormats(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  time.Time
	}{
		{
			name:  "date only",
			input: "2026-03-28",
			want:  time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "datetime with space separator",
			input: "2026-03-28 14:30:00",
			want:  time.Date(2026, 3, 28, 14, 30, 0, 0, time.UTC),
		},
		{
			name:  "datetime T separator no TZ",
			input: "2026-03-28T14:30:00",
			want:  time.Date(2026, 3, 28, 14, 30, 0, 0, time.UTC),
		},
		{
			name:  "RFC3339Nano",
			input: "2026-03-28T14:30:00.123456789Z",
			want:  time.Date(2026, 3, 28, 14, 30, 0, 123456789, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTimestampString(tt.input)
			if err != nil {
				t.Fatalf("error: %v", err)
			}
			if !got.Equal(tt.want) {
				t.Errorf("parseTimestampString(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

// Unparseable string fallback to processing time
func TestExtractEventTimeUnparseable(t *testing.T) {
	expr := &ast.ColumnRef{Name: "ts"}
	rec := Record{Columns: map[string]Value{"ts": TextValue{V: "not-a-timestamp"}}}
	got := ExtractEventTime(expr, rec)
	if time.Since(got) > 2*time.Second {
		t.Errorf("unparseable should fall back to processing time, got %v", got)
	}
}

// Missing column fallback
func TestExtractEventTimeMissingColumn(t *testing.T) {
	expr := &ast.ColumnRef{Name: "ts"}
	rec := Record{Columns: map[string]Value{"other": TextValue{V: "hello"}}}
	got := ExtractEventTime(expr, rec)
	// Missing column -> eval returns null or error -> processing time
	if time.Since(got) > 2*time.Second {
		t.Errorf("missing column should fall back to processing time, got %v", got)
	}
}

// TimestampValue direct
func TestExtractEventTimeTimestampValue(t *testing.T) {
	expr := &ast.ColumnRef{Name: "ts"}
	want := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)
	rec := Record{Columns: map[string]Value{"ts": TimestampValue{V: want}}}
	got := ExtractEventTime(expr, rec)
	if !got.Equal(want) {
		t.Errorf("TimestampValue extraction = %v, want %v", got, want)
	}
}

// parseTimestampString error on garbage
func TestParseTimestampStringError(t *testing.T) {
	_, err := parseTimestampString("totally not a date")
	if err == nil {
		t.Error("expected error for unparseable string")
	}
}
