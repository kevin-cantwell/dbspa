package engine

import (
	"strings"
	"testing"
)

func TestSchemaTracker_FirstRecord(t *testing.T) {
	st := NewSchemaTracker()
	rec := NewRecord(map[string]Value{
		"id":   IntValue{V: 1},
		"name": TextValue{V: "Alice"},
	})
	if err := st.Track(rec); err != nil {
		t.Fatalf("unexpected error on first record: %v", err)
	}
}

func TestSchemaTracker_SameTypes(t *testing.T) {
	st := NewSchemaTracker()
	for i := 0; i < 5; i++ {
		rec := NewRecord(map[string]Value{
			"id":    IntValue{V: int64(i)},
			"price": FloatValue{V: 9.99},
		})
		if err := st.Track(rec); err != nil {
			t.Fatalf("unexpected error on record %d: %v", i, err)
		}
	}
}

func TestSchemaTracker_CompatibleDrift_IntToFloat(t *testing.T) {
	st := NewSchemaTracker()

	rec1 := NewRecord(map[string]Value{"amount": IntValue{V: 100}})
	if err := st.Track(rec1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// INT → FLOAT is compatible
	rec2 := NewRecord(map[string]Value{"amount": FloatValue{V: 100.5}})
	if err := st.Track(rec2); err != nil {
		t.Fatalf("INT→FLOAT should be compatible, got error: %v", err)
	}
}

func TestSchemaTracker_CompatibleDrift_FloatToInt(t *testing.T) {
	st := NewSchemaTracker()

	rec1 := NewRecord(map[string]Value{"amount": FloatValue{V: 10.5}})
	if err := st.Track(rec1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// FLOAT → INT is compatible (precision loss)
	rec2 := NewRecord(map[string]Value{"amount": IntValue{V: 10}})
	if err := st.Track(rec2); err != nil {
		t.Fatalf("FLOAT→INT should be compatible, got error: %v", err)
	}
}

func TestSchemaTracker_CompatibleDrift_IntToNumericText(t *testing.T) {
	st := NewSchemaTracker()

	rec1 := NewRecord(map[string]Value{"amount": IntValue{V: 100}})
	if err := st.Track(rec1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// INT → TEXT("42") is compatible
	rec2 := NewRecord(map[string]Value{"amount": TextValue{V: "42"}})
	if err := st.Track(rec2); err != nil {
		t.Fatalf("INT→TEXT(numeric) should be compatible, got error: %v", err)
	}
}

func TestSchemaTracker_IncompatibleDrift_IntToNonNumericText(t *testing.T) {
	st := NewSchemaTracker()

	rec1 := NewRecord(map[string]Value{"amount": IntValue{V: 100}})
	if err := st.Track(rec1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// INT → TEXT("hello") is incompatible
	rec2 := NewRecord(map[string]Value{"amount": TextValue{V: "hello"}})
	if err := st.Track(rec2); err == nil {
		t.Fatal("INT→TEXT(non-numeric) should be incompatible")
	}
}

func TestSchemaTracker_IncompatibleDrift_IntToBool(t *testing.T) {
	st := NewSchemaTracker()

	rec1 := NewRecord(map[string]Value{"active": IntValue{V: 1}})
	if err := st.Track(rec1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// INT → BOOL is incompatible
	rec2 := NewRecord(map[string]Value{"active": BoolValue{V: true}})
	err := st.Track(rec2)
	if err == nil {
		t.Fatal("INT→BOOL should be incompatible")
	}
	if !strings.Contains(err.Error(), "incompatible") {
		t.Errorf("error should mention 'incompatible', got: %v", err)
	}
}

func TestSchemaTracker_IncompatibleDrift_IntToJSON(t *testing.T) {
	st := NewSchemaTracker()

	rec1 := NewRecord(map[string]Value{"data": IntValue{V: 1}})
	if err := st.Track(rec1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// INT → JSON is incompatible
	rec2 := NewRecord(map[string]Value{"data": JsonValue{V: map[string]any{"key": "val"}}})
	err := st.Track(rec2)
	if err == nil {
		t.Fatal("INT→JSON should be incompatible")
	}
}

func TestSchemaTracker_IncompatibleDrift_BoolToText(t *testing.T) {
	st := NewSchemaTracker()

	rec1 := NewRecord(map[string]Value{"flag": BoolValue{V: true}})
	if err := st.Track(rec1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// BOOL → TEXT is incompatible
	rec2 := NewRecord(map[string]Value{"flag": TextValue{V: "yes"}})
	err := st.Track(rec2)
	if err == nil {
		t.Fatal("BOOL→TEXT should be incompatible")
	}
}

func TestSchemaTracker_NullSkipped(t *testing.T) {
	st := NewSchemaTracker()

	rec1 := NewRecord(map[string]Value{"id": IntValue{V: 1}})
	if err := st.Track(rec1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// NULL value should be skipped, no error
	rec2 := NewRecord(map[string]Value{"id": NullValue{}})
	if err := st.Track(rec2); err != nil {
		t.Fatalf("NULL should not cause drift error: %v", err)
	}
}

func TestSchemaTracker_TextToInt_Compatible(t *testing.T) {
	st := NewSchemaTracker()

	rec1 := NewRecord(map[string]Value{"id": TextValue{V: "hello"}})
	if err := st.Track(rec1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// TEXT → INT is compatible (numeric types from text)
	rec2 := NewRecord(map[string]Value{"id": IntValue{V: 42}})
	if err := st.Track(rec2); err != nil {
		t.Fatalf("TEXT→INT should be compatible, got error: %v", err)
	}
}

func TestSchemaTracker_WarningRateLimited(t *testing.T) {
	st := NewSchemaTracker()

	rec1 := NewRecord(map[string]Value{"amount": IntValue{V: 100}})
	st.Track(rec1)

	// Multiple compatible drifts should only warn once (we just verify no error)
	for i := 0; i < 100; i++ {
		rec := NewRecord(map[string]Value{"amount": FloatValue{V: float64(i)}})
		if err := st.Track(rec); err != nil {
			t.Fatalf("compatible drift should not error on iteration %d: %v", i, err)
		}
	}
}
