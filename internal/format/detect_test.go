package format

import (
	"testing"
)

func TestDetectEncoding(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    string
		wantErr bool
	}{
		{
			name: "JSON object",
			data: []byte(`{"name":"Alice"}`),
			want: "JSON",
		},
		{
			name: "JSON array",
			data: []byte(`[{"name":"Alice"}]`),
			want: "JSON",
		},
		{
			name: "JSON with leading whitespace",
			data: []byte(`  {"name":"Alice"}`),
			want: "JSON",
		},
		{
			name: "Avro OCF magic bytes",
			data: []byte("Obj\x01\x00\x00"),
			want: "AVRO",
		},
		{
			name: "Parquet magic bytes",
			data: []byte("PAR1\x00\x00\x00\x00"),
			want: "PARQUET",
		},
		{
			name: "Confluent wire format",
			data: []byte("\x00\x00\x00\x00\x01some-data"),
			want: "CONFLUENT",
		},
		{
			name: "CSV with commas",
			data: []byte("name,age,city\nAlice,30,NYC\n"),
			want: "CSV",
		},
		{
			name: "CSV with tabs",
			data: []byte("name\tage\tcity\nAlice\t30\tNYC\n"),
			want: "CSV",
		},
		{
			name: "empty input",
			data: []byte{},
			wantErr: true,
		},
		{
			name: "whitespace only",
			data: []byte("   \n  "),
			wantErr: true,
		},
		{
			name:    "unknown binary",
			data:    []byte{0x01, 0x02, 0x03, 0x04},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DetectEncoding(tt.data)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDetectChangelog(t *testing.T) {
	tests := []struct {
		name    string
		fields  map[string]any
		want    string
		wantErr bool
	}{
		{
			name:   "DBSPA changelog (has weight + data)",
			fields: map[string]any{"weight": float64(1), "data": map[string]any{"name": "Alice"}},
			want:   "DBSPA",
		},
		{
			name:   "Debezium (has op + after)",
			fields: map[string]any{"op": "c", "after": map[string]any{"id": 1}},
			want:   "DEBEZIUM",
		},
		{
			name:   "Debezium (has op + before)",
			fields: map[string]any{"op": "d", "before": map[string]any{"id": 1}},
			want:   "DEBEZIUM",
		},
		{
			name:   "Debezium (has op + both)",
			fields: map[string]any{"op": "u", "before": map[string]any{"id": 1}, "after": map[string]any{"id": 2}},
			want:   "DEBEZIUM",
		},
		{
			name:    "plain record (no changelog signals)",
			fields:  map[string]any{"name": "Alice", "age": 30},
			wantErr: true,
		},
		{
			name:    "has op but no before/after",
			fields:  map[string]any{"op": "insert", "data": "something"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DetectChangelog(tt.fields)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}
