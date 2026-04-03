package sink

import (
	"testing"

	"github.com/kevin-cantwell/dbspa/internal/engine"
)

func TestCompareValues_Nulls(t *testing.T) {
	// NULL vs NULL
	if c := CompareValues(engine.NullValue{}, engine.NullValue{}); c != 0 {
		t.Errorf("NULL vs NULL: expected 0, got %d", c)
	}
	// NULL sorts last
	if c := CompareValues(engine.NullValue{}, engine.IntValue{V: 1}); c != 1 {
		t.Errorf("NULL vs 1: expected 1, got %d", c)
	}
	if c := CompareValues(engine.IntValue{V: 1}, engine.NullValue{}); c != -1 {
		t.Errorf("1 vs NULL: expected -1, got %d", c)
	}
	// nil value treated as NULL
	if c := CompareValues(nil, engine.IntValue{V: 1}); c != 1 {
		t.Errorf("nil vs 1: expected 1, got %d", c)
	}
}

func TestCompareValues_Ints(t *testing.T) {
	if c := CompareValues(engine.IntValue{V: 1}, engine.IntValue{V: 2}); c >= 0 {
		t.Errorf("1 vs 2: expected negative, got %d", c)
	}
	if c := CompareValues(engine.IntValue{V: 5}, engine.IntValue{V: 5}); c != 0 {
		t.Errorf("5 vs 5: expected 0, got %d", c)
	}
	if c := CompareValues(engine.IntValue{V: 10}, engine.IntValue{V: 3}); c <= 0 {
		t.Errorf("10 vs 3: expected positive, got %d", c)
	}
}

func TestCompareValues_Floats(t *testing.T) {
	if c := CompareValues(engine.FloatValue{V: 1.5}, engine.FloatValue{V: 2.5}); c >= 0 {
		t.Errorf("1.5 vs 2.5: expected negative, got %d", c)
	}
}

func TestCompareValues_MixedNumeric(t *testing.T) {
	// INT promoted to FLOAT for comparison
	if c := CompareValues(engine.IntValue{V: 2}, engine.FloatValue{V: 2.5}); c >= 0 {
		t.Errorf("INT(2) vs FLOAT(2.5): expected negative, got %d", c)
	}
	if c := CompareValues(engine.FloatValue{V: 3.0}, engine.IntValue{V: 3}); c != 0 {
		t.Errorf("FLOAT(3.0) vs INT(3): expected 0, got %d", c)
	}
}

func TestCompareValues_Text(t *testing.T) {
	if c := CompareValues(engine.TextValue{V: "apple"}, engine.TextValue{V: "banana"}); c >= 0 {
		t.Errorf("apple vs banana: expected negative, got %d", c)
	}
	if c := CompareValues(engine.TextValue{V: "z"}, engine.TextValue{V: "a"}); c <= 0 {
		t.Errorf("z vs a: expected positive, got %d", c)
	}
}

func TestCompareValues_FallbackToString(t *testing.T) {
	// Bool values fall back to string comparison
	c := CompareValues(engine.BoolValue{V: false}, engine.BoolValue{V: true})
	if c >= 0 {
		t.Errorf("false vs true: expected negative (lexicographic), got %d", c)
	}
}
