package engine

import (
	"testing"
)

func TestCountStarAccumulator(t *testing.T) {
	a := &CountStarAccumulator{}

	// Add three values (including NULL — COUNT(*) counts all)
	a.Add(IntValue{V: 1})
	if !a.HasChanged() {
		t.Error("expected changed after Add")
	}
	if r := a.Result().(IntValue).V; r != 1 {
		t.Errorf("expected 1, got %d", r)
	}

	a.ResetChanged()
	a.Add(NullValue{})
	if r := a.Result().(IntValue).V; r != 2 {
		t.Errorf("expected 2, got %d", r)
	}

	a.ResetChanged()
	a.Add(IntValue{V: 3})
	if r := a.Result().(IntValue).V; r != 3 {
		t.Errorf("expected 3, got %d", r)
	}

	// Retract one
	a.ResetChanged()
	a.Retract(IntValue{V: 1})
	if r := a.Result().(IntValue).V; r != 2 {
		t.Errorf("expected 2, got %d", r)
	}

	// Retract below zero — clamp at 0
	a.Retract(IntValue{V: 1})
	a.Retract(IntValue{V: 1})
	a.Retract(IntValue{V: 1})
	if r := a.Result().(IntValue).V; r != 0 {
		t.Errorf("expected 0 (clamped), got %d", r)
	}
}

func TestCountAccumulator(t *testing.T) {
	a := &CountAccumulator{}

	// NULL is skipped
	a.Add(NullValue{})
	if r := a.Result().(IntValue).V; r != 0 {
		t.Errorf("expected 0 after adding NULL, got %d", r)
	}

	a.Add(IntValue{V: 1})
	a.Add(IntValue{V: 2})
	if r := a.Result().(IntValue).V; r != 2 {
		t.Errorf("expected 2, got %d", r)
	}

	// Retract NULL is ignored
	a.ResetChanged()
	a.Retract(NullValue{})
	if a.HasChanged() {
		t.Error("retract NULL should not change count")
	}
	if r := a.Result().(IntValue).V; r != 2 {
		t.Errorf("expected 2 after null retract, got %d", r)
	}

	a.Retract(IntValue{V: 1})
	if r := a.Result().(IntValue).V; r != 1 {
		t.Errorf("expected 1, got %d", r)
	}
}

func TestSumAccumulator(t *testing.T) {
	a := &SumAccumulator{}

	// Empty sum returns NULL
	if !a.Result().IsNull() {
		t.Error("expected NULL for empty SUM")
	}

	// Add values
	a.Add(IntValue{V: 100})
	a.Add(IntValue{V: 200})
	a.Add(IntValue{V: 50})

	r := a.Result()
	if v, ok := r.(IntValue); !ok || v.V != 350 {
		t.Errorf("expected 350, got %v", r)
	}

	// NULL is skipped
	a.ResetChanged()
	a.Add(NullValue{})
	if a.HasChanged() {
		t.Error("adding NULL should not change SUM")
	}

	// Retract
	a.Retract(IntValue{V: 200})
	r = a.Result()
	if v, ok := r.(IntValue); !ok || v.V != 150 {
		t.Errorf("expected 150 after retract, got %v", r)
	}

	// SUM can go negative (spec 12.2)
	a.Retract(IntValue{V: 300})
	r = a.Result()
	if v, ok := r.(IntValue); !ok || v.V != -150 {
		t.Errorf("expected -150, got %v", r)
	}
}

func TestAvgAccumulator(t *testing.T) {
	a := &AvgAccumulator{}

	// Empty returns NULL
	if !a.Result().IsNull() {
		t.Error("expected NULL for empty AVG")
	}

	a.Add(IntValue{V: 90})
	a.Add(IntValue{V: 95})
	r := a.Result().(FloatValue).V
	if r != 92.5 {
		t.Errorf("expected 92.5, got %f", r)
	}

	// Retract
	a.Retract(IntValue{V: 90})
	r = a.Result().(FloatValue).V
	if r != 95 {
		t.Errorf("expected 95 after retract, got %f", r)
	}

	// Retract to empty
	a.Retract(IntValue{V: 95})
	if !a.Result().IsNull() {
		t.Error("expected NULL after all retracted")
	}
}

func TestMinAccumulator(t *testing.T) {
	a := &MinAccumulator{}

	if !a.Result().IsNull() {
		t.Error("expected NULL for empty MIN")
	}

	a.Add(IntValue{V: 50})
	a.Add(IntValue{V: 10})
	a.Add(IntValue{V: 30})
	if v := a.Result().(IntValue).V; v != 10 {
		t.Errorf("expected 10, got %d", v)
	}

	// Retract the minimum
	a.Retract(IntValue{V: 10})
	if v := a.Result().(IntValue).V; v != 30 {
		t.Errorf("expected 30 after retract min, got %d", v)
	}

	// NULL is skipped
	a.ResetChanged()
	a.Add(NullValue{})
	if a.HasChanged() {
		t.Error("NULL add should not change MIN")
	}
}

func TestMaxAccumulator(t *testing.T) {
	a := &MaxAccumulator{}

	if !a.Result().IsNull() {
		t.Error("expected NULL for empty MAX")
	}

	a.Add(IntValue{V: 50})
	a.Add(IntValue{V: 90})
	a.Add(IntValue{V: 30})
	if v := a.Result().(IntValue).V; v != 90 {
		t.Errorf("expected 90, got %d", v)
	}

	// Retract the maximum
	a.Retract(IntValue{V: 90})
	if v := a.Result().(IntValue).V; v != 50 {
		t.Errorf("expected 50 after retract max, got %d", v)
	}
}

func TestFirstAccumulator(t *testing.T) {
	a := &FirstAccumulator{}

	// NULL is skipped
	a.Add(NullValue{})
	if !a.Result().IsNull() {
		t.Error("expected NULL — FIRST skips NULL")
	}

	a.Add(TextValue{V: "alice"})
	if v := a.Result().(TextValue).V; v != "alice" {
		t.Errorf("expected alice, got %s", v)
	}

	// Subsequent adds do not change FIRST
	a.ResetChanged()
	a.Add(TextValue{V: "bob"})
	if a.HasChanged() {
		t.Error("FIRST should not change after first value is set")
	}
	if v := a.Result().(TextValue).V; v != "alice" {
		t.Errorf("expected alice still, got %s", v)
	}

	// Retraction is ignored
	a.Retract(TextValue{V: "alice"})
	if v := a.Result().(TextValue).V; v != "alice" {
		t.Errorf("FIRST retraction should be ignored, got %s", v)
	}
}

func TestLastAccumulator(t *testing.T) {
	a := &LastAccumulator{}

	a.Add(TextValue{V: "first"})
	a.Add(TextValue{V: "second"})
	a.Add(TextValue{V: "third"})

	if v := a.Result().(TextValue).V; v != "third" {
		t.Errorf("expected third, got %s", v)
	}

	// Retraction is ignored
	a.Retract(TextValue{V: "third"})
	if v := a.Result().(TextValue).V; v != "third" {
		t.Errorf("LAST retraction should be ignored, got %s", v)
	}

	// NULL add is skipped
	a.ResetChanged()
	a.Add(NullValue{})
	if a.HasChanged() {
		t.Error("LAST should skip NULL add")
	}
}

func TestSumRetractToEmpty(t *testing.T) {
	a := &SumAccumulator{}
	a.Add(IntValue{V: 100})
	a.Retract(IntValue{V: 100})
	// count drops to 0, should return NULL
	if !a.Result().IsNull() {
		t.Errorf("expected NULL when all values retracted, got %v", a.Result())
	}
}
