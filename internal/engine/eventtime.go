package engine

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/kevin-cantwell/dbspa/internal/sql/ast"
)

// ExtractEventTime extracts the event time from a record using the given expression.
// If the expression is nil, returns processing time (time.Now()).
// If the column doesn't exist or value is NULL, returns processing time with debug log.
// If the value can't be parsed as a timestamp, returns processing time with warning.
func ExtractEventTime(expr ast.Expr, rec Record) time.Time {
	if expr == nil {
		return time.Now()
	}

	val, err := Eval(expr, rec)
	if err != nil {
		log.Printf("warning: event time extraction error: %v, using processing time", err)
		return time.Now()
	}

	if val.IsNull() {
		log.Printf("debug: event time column is NULL, using processing time")
		return time.Now()
	}

	ts, err := parseEventTime(val)
	if err != nil {
		log.Printf("warning: cannot parse event time %q: %v, using processing time", val.String(), err)
		return time.Now()
	}

	return ts
}

// parseEventTime parses a Value as a timestamp.
// Supports: TimestampValue, TextValue (ISO 8601), IntValue/FloatValue (unix epoch).
func parseEventTime(v Value) (time.Time, error) {
	switch val := v.(type) {
	case TimestampValue:
		return val.V.UTC(), nil

	case TextValue:
		return parseTimestampString(val.V)

	case IntValue:
		return time.Unix(val.V, 0).UTC(), nil

	case FloatValue:
		sec := int64(val.V)
		nsec := int64((val.V - float64(sec)) * 1e9)
		return time.Unix(sec, nsec).UTC(), nil

	default:
		// Try converting to string and parsing
		return parseTimestampString(v.String())
	}
}

// parseTimestampString parses a string as a timestamp using multiple formats.
func parseTimestampString(s string) (time.Time, error) {
	s = strings.TrimSpace(s)

	// Try as numeric (unix epoch)
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		// Heuristic: if > 1e12, it's probably milliseconds
		if f > 1e15 {
			// Nanoseconds
			return time.Unix(0, int64(f)).UTC(), nil
		}
		if f > 1e12 {
			// Milliseconds
			return time.Unix(0, int64(f*1e6)).UTC(), nil
		}
		// Seconds
		sec := int64(f)
		nsec := int64((f - float64(sec)) * 1e9)
		return time.Unix(sec, nsec).UTC(), nil
	}

	// ISO 8601 formats
	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02",
	}

	for _, fmt := range formats {
		if t, err := time.Parse(fmt, s); err == nil {
			return t.UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf("cannot parse %q as timestamp", s)
}
