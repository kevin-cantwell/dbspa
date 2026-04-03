// Package format provides decoders that convert raw bytes into engine Records.
package format

import (
	"bytes"
	"fmt"
)

// DetectEncoding peeks at the first bytes and returns the encoding name.
// Returns an error if the encoding cannot be determined — never guesses.
func DetectEncoding(data []byte) (string, error) {
	if len(data) == 0 {
		return "", fmt.Errorf("empty input, cannot detect format")
	}
	b := bytes.TrimSpace(data)
	if len(b) == 0 {
		return "", fmt.Errorf("empty input after trimming")
	}
	switch {
	case b[0] == '{' || b[0] == '[':
		return "JSON", nil
	case bytes.HasPrefix(b, []byte("Obj\x01")):
		return "AVRO", nil
	case bytes.HasPrefix(b, []byte("PAR1")):
		return "PARQUET", nil
	case b[0] == 0x00 && len(b) >= 5:
		return "CONFLUENT", nil // Confluent wire format
	default:
		if looksLikeCSV(b) {
			return "CSV", nil
		}
		n := len(b)
		if n > 10 {
			n = 10
		}
		return "", fmt.Errorf("cannot detect encoding from first bytes: %x", b[:n])
	}
}

// looksLikeCSV returns true if the data looks like it could be CSV:
// printable ASCII with commas and/or tabs, with at least one line that
// contains a separator.
func looksLikeCSV(data []byte) bool {
	// Check just the first line
	line := data
	if idx := bytes.IndexByte(data, '\n'); idx >= 0 {
		line = data[:idx]
	}
	if len(line) == 0 {
		return false
	}
	hasSep := false
	for _, b := range line {
		if b == ',' || b == '\t' {
			hasSep = true
		}
		// Non-printable (except tab, CR) suggests binary
		if b < 0x20 && b != '\t' && b != '\r' {
			return false
		}
	}
	return hasSep
}

// DetectChangelog inspects a decoded record's fields to determine the
// changelog envelope type. Returns the changelog family name or an error
// if CHANGELOG was specified but the envelope cannot be determined.
func DetectChangelog(fields map[string]any) (string, error) {
	// Check for FoldDB changelog: has _weight field
	if _, ok := fields["_weight"]; ok {
		return "FOLDDB", nil
	}
	// Check for Debezium: has "op" AND ("before" OR "after")
	if _, hasOp := fields["op"]; hasOp {
		_, hasBefore := fields["before"]
		_, hasAfter := fields["after"]
		if hasBefore || hasAfter {
			return "DEBEZIUM", nil
		}
	}
	return "", fmt.Errorf("CHANGELOG specified but cannot detect envelope type from record fields: %v", fieldNames(fields))
}

// fieldNames returns the keys of a map for error reporting.
func fieldNames(m map[string]any) []string {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	return names
}
