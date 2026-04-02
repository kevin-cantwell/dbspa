package format

import (
	"strings"
	"testing"
)

func TestSchemaEvolution_FieldAdded(t *testing.T) {
	tracker := newSchemaEvolutionTracker()

	schema1 := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}`
	schema2 := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"},{"name":"email","type":"string"}]}`

	// First schema — no evolution
	if err := tracker.checkEvolution(1, schema1); err != nil {
		t.Fatalf("unexpected error on first schema: %v", err)
	}

	// Second schema adds a field — should warn but not error
	if err := tracker.checkEvolution(2, schema2); err != nil {
		t.Fatalf("field addition should be compatible, got error: %v", err)
	}
}

func TestSchemaEvolution_FieldRemoved(t *testing.T) {
	tracker := newSchemaEvolutionTracker()

	schema1 := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"},{"name":"email","type":"string"}]}`
	schema2 := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}`

	if err := tracker.checkEvolution(1, schema1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Removing a field is incompatible
	err := tracker.checkEvolution(2, schema2)
	if err == nil {
		t.Fatal("field removal should be incompatible")
	}
	if !strings.Contains(err.Error(), "removed") {
		t.Errorf("error should mention 'removed', got: %v", err)
	}
}

func TestSchemaEvolution_CompatibleTypeChange(t *testing.T) {
	tracker := newSchemaEvolutionTracker()

	schema1 := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"},{"name":"amount","type":"int"}]}`
	schema2 := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"},{"name":"amount","type":"long"}]}`

	if err := tracker.checkEvolution(1, schema1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// int → long is compatible
	if err := tracker.checkEvolution(2, schema2); err != nil {
		t.Fatalf("int→long should be compatible, got error: %v", err)
	}
}

func TestSchemaEvolution_IncompatibleTypeChange(t *testing.T) {
	tracker := newSchemaEvolutionTracker()

	schema1 := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"},{"name":"active","type":"int"}]}`
	schema2 := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"},{"name":"active","type":"string"}]}`

	if err := tracker.checkEvolution(1, schema1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// int → string is incompatible
	err := tracker.checkEvolution(2, schema2)
	if err == nil {
		t.Fatal("int→string should be incompatible")
	}
	if !strings.Contains(err.Error(), "incompatible") {
		t.Errorf("error should mention 'incompatible', got: %v", err)
	}
}

func TestSchemaEvolution_SameSchema(t *testing.T) {
	tracker := newSchemaEvolutionTracker()

	schema := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}`

	if err := tracker.checkEvolution(1, schema); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Same schema ID again — no evolution
	if err := tracker.checkEvolution(1, schema); err != nil {
		t.Fatalf("same schema should not error: %v", err)
	}
}

func TestSchemaEvolution_NullableField(t *testing.T) {
	tracker := newSchemaEvolutionTracker()

	schema1 := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}`
	// name changed from string to ["null","string"] — simplified type is still "string"
	schema2 := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"},{"name":"name","type":["null","string"]}]}`

	if err := tracker.checkEvolution(1, schema1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Making a field nullable should be compatible (type stays "string")
	if err := tracker.checkEvolution(2, schema2); err != nil {
		t.Fatalf("making field nullable should be compatible, got error: %v", err)
	}
}

func TestSchemaEvolution_IntToFloat(t *testing.T) {
	tracker := newSchemaEvolutionTracker()

	schema1 := `{"type":"record","name":"Test","fields":[{"name":"val","type":"int"}]}`
	schema2 := `{"type":"record","name":"Test","fields":[{"name":"val","type":"float"}]}`

	if err := tracker.checkEvolution(1, schema1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// int → float is compatible per Avro spec
	if err := tracker.checkEvolution(2, schema2); err != nil {
		t.Fatalf("int→float should be compatible, got error: %v", err)
	}
}

func TestSchemaEvolution_FloatToDouble(t *testing.T) {
	tracker := newSchemaEvolutionTracker()

	schema1 := `{"type":"record","name":"Test","fields":[{"name":"val","type":"float"}]}`
	schema2 := `{"type":"record","name":"Test","fields":[{"name":"val","type":"double"}]}`

	if err := tracker.checkEvolution(1, schema1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := tracker.checkEvolution(2, schema2); err != nil {
		t.Fatalf("float→double should be compatible, got error: %v", err)
	}
}

func TestParseAvroFields(t *testing.T) {
	schema := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"},{"name":"data","type":["null","string"]}]}`

	fields, err := parseAvroFields(schema)
	if err != nil {
		t.Fatalf("parseAvroFields: %v", err)
	}

	if len(fields) != 3 {
		t.Fatalf("expected 3 fields, got %d", len(fields))
	}

	if fields[0].Name != "id" || fields[0].Type != "int" {
		t.Errorf("field 0: got %+v", fields[0])
	}
	if fields[1].Name != "name" || fields[1].Type != "string" {
		t.Errorf("field 1: got %+v", fields[1])
	}
	if fields[2].Name != "data" || fields[2].Type != "string" {
		t.Errorf("field 2: got %+v, expected type 'string' (nullable stripped)", fields[2])
	}
}

func TestSimplifyAvroType(t *testing.T) {
	tests := []struct {
		raw  string
		want string
	}{
		{`"int"`, "int"},
		{`"string"`, "string"},
		{`["null","int"]`, "int"},
		{`["string","null"]`, "string"},
		{`{"type":"record","name":"Foo","fields":[]}`, "record"},
		{`{"type":"array","items":"string"}`, "array"},
	}

	for _, tt := range tests {
		got := simplifyAvroType([]byte(tt.raw))
		if got != tt.want {
			t.Errorf("simplifyAvroType(%s) = %q, want %q", tt.raw, got, tt.want)
		}
	}
}
