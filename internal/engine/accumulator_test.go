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
	// spec 12.2: SUM returns running total (0) after full retraction, not NULL
	assertIntResult(t, a, 0)
}

// =====================================================================
// TC-ACC-001 through TC-ACC-031: Comprehensive accumulator tests
// from TESTPLAN.md
// =====================================================================

// TC-ACC-001: COUNT(*) with inserts only
func TestTC_ACC_001_CountStarInsertsOnly(t *testing.T) {
	a := &CountStarAccumulator{}
	a.Add(IntValue{V: 1})
	assertIntResult(t, a, 1)
	a.Add(IntValue{V: 2})
	assertIntResult(t, a, 2)
	a.Add(IntValue{V: 3})
	assertIntResult(t, a, 3)
}

// TC-ACC-002: COUNT(*) with insert then retract
func TestTC_ACC_002_CountStarInsertRetract(t *testing.T) {
	a := &CountStarAccumulator{}
	a.Add(IntValue{V: 1})
	assertIntResult(t, a, 1)
	a.Retract(IntValue{V: 1})
	assertIntResult(t, a, 0)
}

// TC-ACC-003: SUM with inserts and retractions
func TestTC_ACC_003_SumInsertRetract(t *testing.T) {
	a := &SumAccumulator{}
	a.Add(IntValue{V: 10})
	assertIntResult(t, a, 10)
	a.Add(IntValue{V: 20})
	assertIntResult(t, a, 30)
	a.Retract(IntValue{V: 10})
	assertIntResult(t, a, 20)
}

// TC-ACC-004: SUM with NULL values (NULLs skipped)
func TestTC_ACC_004_SumWithNulls(t *testing.T) {
	a := &SumAccumulator{}
	a.Add(IntValue{V: 10})
	a.Add(NullValue{})
	a.Add(IntValue{V: 5})
	assertIntResult(t, a, 15)
}

// TC-ACC-005: AVG correctness with retractions
func TestTC_ACC_005_AvgWithRetractions(t *testing.T) {
	a := &AvgAccumulator{}
	a.Add(IntValue{V: 10})
	assertFloatResult(t, a, 10.0)
	a.Add(IntValue{V: 20})
	assertFloatResult(t, a, 15.0)
	a.Retract(IntValue{V: 10})
	assertFloatResult(t, a, 20.0)
}

// TC-ACC-006: AVG with all NULLs returns NULL
func TestTC_ACC_006_AvgAllNulls(t *testing.T) {
	a := &AvgAccumulator{}
	a.Add(NullValue{})
	a.Add(NullValue{})
	if !a.Result().IsNull() {
		t.Errorf("expected NULL for AVG of all NULLs, got %v", a.Result())
	}
}

// TC-ACC-007: MIN with retraction of the minimum value
func TestTC_ACC_007_MinRetractMinimum(t *testing.T) {
	a := &MinAccumulator{}
	a.Add(IntValue{V: 5})
	assertIntResult(t, a, 5)
	a.Add(IntValue{V: 10})
	assertIntResult(t, a, 5)
	a.Add(IntValue{V: 3})
	assertIntResult(t, a, 3)
	a.Retract(IntValue{V: 3})
	assertIntResult(t, a, 5)
}

// TC-ACC-008: MAX with retraction of the maximum value
func TestTC_ACC_008_MaxRetractMaximum(t *testing.T) {
	a := &MaxAccumulator{}
	a.Add(IntValue{V: 100})
	assertIntResult(t, a, 100)
	a.Add(IntValue{V: 50})
	assertIntResult(t, a, 100)
	a.Retract(IntValue{V: 100})
	assertIntResult(t, a, 50)
}

// TC-ACC-009: MIN/MAX on empty group after all retractions
func TestTC_ACC_009_MinMaxEmptyAfterRetract(t *testing.T) {
	minAcc := &MinAccumulator{}
	minAcc.Add(IntValue{V: 10})
	minAcc.Retract(IntValue{V: 10})
	if !minAcc.Result().IsNull() {
		t.Errorf("expected NULL for MIN after all retracted, got %v", minAcc.Result())
	}

	maxAcc := &MaxAccumulator{}
	maxAcc.Add(IntValue{V: 10})
	maxAcc.Retract(IntValue{V: 10})
	if !maxAcc.Result().IsNull() {
		t.Errorf("expected NULL for MAX after all retracted, got %v", maxAcc.Result())
	}
}

// TC-ACC-012: FIRST(x) behavior on retraction (retraction ignored)
func TestTC_ACC_012_FirstRetraction(t *testing.T) {
	a := &FirstAccumulator{}
	a.Add(IntValue{V: 1})
	a.Add(IntValue{V: 2})
	assertIntResult(t, a, 1)
	a.Retract(IntValue{V: 1})
	assertIntResult(t, a, 1) // retraction ignored
}

// TC-ACC-013: LAST(x) updates on each insert
func TestTC_ACC_013_LastUpdatesOnInsert(t *testing.T) {
	a := &LastAccumulator{}
	a.Add(IntValue{V: 1})
	assertIntResult(t, a, 1)
	a.Add(IntValue{V: 2})
	assertIntResult(t, a, 2)
	a.Add(IntValue{V: 3})
	assertIntResult(t, a, 3)
}

// TC-ACC-017: COUNT(*) with HAVING filtering everything
func TestTC_ACC_017_CountStarHavingFiltersAll(t *testing.T) {
	// This is tested at the AggregateOp level (see aggregate_test.go).
	// Unit-level: just verify COUNT(*) with single add.
	a := &CountStarAccumulator{}
	a.Add(IntValue{V: 1})
	assertIntResult(t, a, 1)
	// The HAVING c > 10 logic is tested in aggregate_test.go
}

// TC-ACC-018: Multiple aggregates on same group
func TestTC_ACC_018_MultipleAggregates(t *testing.T) {
	countStar := &CountStarAccumulator{}
	sum := &SumAccumulator{}
	avg := &AvgAccumulator{}
	min := &MinAccumulator{}
	max := &MaxAccumulator{}

	values := []int64{10, 20, 30}
	for _, v := range values {
		val := IntValue{V: v}
		countStar.Add(val)
		sum.Add(val)
		avg.Add(val)
		min.Add(val)
		max.Add(val)
	}

	assertIntResult(t, countStar, 3)
	assertIntResult(t, sum, 60)
	assertFloatResult(t, avg, 20.0)
	assertIntResult(t, min, 10)
	assertIntResult(t, max, 30)
}

// TC-ACC-019: Retraction on accumulator with no prior state
func TestTC_ACC_019_RetractOnEmpty(t *testing.T) {
	tests := []struct {
		name string
		acc  Accumulator
	}{
		{"CountStar", &CountStarAccumulator{}},
		{"Count", &CountAccumulator{}},
		{"Sum", &SumAccumulator{}},
		{"Avg", &AvgAccumulator{}},
		{"Min", &MinAccumulator{}},
		{"Max", &MaxAccumulator{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.acc.Retract(IntValue{V: 42})
			// Should not panic. For COUNT, clamped at 0.
			_ = tt.acc.Result()
		})
	}
}

// TC-ACC-021: Single-element group retraction leaves group empty
func TestTC_ACC_021_SingleElementRetract(t *testing.T) {
	a := &CountStarAccumulator{}
	a.Add(IntValue{V: 5})
	assertIntResult(t, a, 1)
	a.Retract(IntValue{V: 5})
	assertIntResult(t, a, 0) // group should be removed at AggregateOp level
}

// TC-ACC-023: SUM going negative from retractions
// spec 12.2: SUM is allowed to go negative (mathematically correct).
func TestTC_ACC_023_SumGoesNegative(t *testing.T) {
	a := &SumAccumulator{}
	a.Add(IntValue{V: 5})
	a.Retract(IntValue{V: 10})
	// spec 12.2: SUM returns the running total (-5), not NULL
	assertIntResult(t, a, -5)
	// Verify HasChanged is true (retraction did change state)
	if !a.HasChanged() {
		t.Error("expected HasChanged after retraction")
	}
}

// TC-ACC-024: Retraction of NULL value from SUM (ignored)
func TestTC_ACC_024_SumRetractNull(t *testing.T) {
	a := &SumAccumulator{}
	a.Add(IntValue{V: 10})
	a.ResetChanged()
	a.Retract(NullValue{})
	if a.HasChanged() {
		t.Error("retracting NULL from SUM should not change result")
	}
	assertIntResult(t, a, 10)
}

// TC-ACC-025: Double retraction — COUNT clamped at 0
func TestTC_ACC_025_DoubleRetraction(t *testing.T) {
	cntStar := &CountStarAccumulator{}
	cntStar.Add(IntValue{V: 1})
	cntStar.Retract(IntValue{V: 1})
	cntStar.Retract(IntValue{V: 1})
	assertIntResult(t, cntStar, 0) // clamped at 0

	cnt := &CountAccumulator{}
	cnt.Add(IntValue{V: 1})
	cnt.Retract(IntValue{V: 1})
	cnt.Retract(IntValue{V: 1})
	assertIntResult(t, cnt, 0) // clamped at 0

	// SUM: spec 12.2 — SUM returns running total after retractions, not NULL
	sum := &SumAccumulator{}
	sum.Add(IntValue{V: 1})
	sum.Retract(IntValue{V: 1})
	sum.Retract(IntValue{V: 1})
	assertIntResult(t, sum, -1)
}

// TC-ACC-026: Insert after all values retracted (group re-created)
func TestTC_ACC_026_InsertAfterAllRetracted(t *testing.T) {
	a := &CountStarAccumulator{}
	a.Add(IntValue{V: 1})
	assertIntResult(t, a, 1)
	a.Retract(IntValue{V: 1})
	assertIntResult(t, a, 0)
	a.Add(IntValue{V: 5})
	assertIntResult(t, a, 1)
	if !a.HasChanged() {
		t.Error("expected HasChanged after re-insert")
	}
}

// TC-ACC-029: FIRST(x) with NULL inputs (returns first non-NULL)
func TestTC_ACC_029_FirstWithNulls(t *testing.T) {
	a := &FirstAccumulator{}
	a.Add(NullValue{})
	a.Add(NullValue{})
	if !a.Result().IsNull() {
		t.Error("FIRST of all NULLs should be NULL")
	}
	a.Add(IntValue{V: 42})
	assertIntResult(t, a, 42)
	a.Add(IntValue{V: 99})
	assertIntResult(t, a, 42) // still first non-NULL
}

// TC-ACC-030: LAST(x) with NULL inputs (returns most recent non-NULL)
func TestTC_ACC_030_LastWithNulls(t *testing.T) {
	a := &LastAccumulator{}
	a.Add(IntValue{V: 1})
	a.Add(NullValue{})
	assertIntResult(t, a, 1) // NULL skipped
	a.Add(IntValue{V: 3})
	assertIntResult(t, a, 3)
	a.Add(NullValue{})
	assertIntResult(t, a, 3) // NULL skipped again
}

// TC-ACC-031: Retraction with mismatched value
func TestTC_ACC_031_MismatchedRetraction(t *testing.T) {
	// SUM: spec 12.2 — returns running total even with mismatched retraction
	sum := &SumAccumulator{}
	sum.Add(IntValue{V: 10})
	sum.Retract(IntValue{V: 99})
	assertIntResult(t, sum, -89)

	// COUNT: goes 1 -> 0 (decrements by 1 regardless of value)
	cnt := &CountAccumulator{}
	cnt.Add(IntValue{V: 10})
	cnt.Retract(IntValue{V: 99})
	assertIntResult(t, cnt, 0)

	// MIN: may return incorrect results
	minAcc := &MinAccumulator{}
	minAcc.Add(IntValue{V: 10})
	minAcc.Retract(IntValue{V: 99}) // 99 not in values, not removed
	assertIntResult(t, minAcc, 10)  // value 10 still present
}

// =====================================================================
// Additional edge-case tests beyond the test plan
// =====================================================================

func TestCountAccumulatorNullRetraction(t *testing.T) {
	a := &CountAccumulator{}
	a.Add(IntValue{V: 1})
	a.Add(IntValue{V: 2})
	a.ResetChanged()
	a.Retract(NullValue{})
	if a.HasChanged() {
		t.Error("retracting NULL from COUNT should be no-op")
	}
	assertIntResult(t, a, 2)
}

func TestCountStarCountsNulls(t *testing.T) {
	a := &CountStarAccumulator{}
	a.Add(NullValue{})
	assertIntResult(t, a, 1) // COUNT(*) includes NULLs
	a.Add(NullValue{})
	assertIntResult(t, a, 2)
}

func TestMinAccumulatorNullRetraction(t *testing.T) {
	a := &MinAccumulator{}
	a.Add(IntValue{V: 5})
	a.ResetChanged()
	a.Retract(NullValue{})
	if a.HasChanged() {
		t.Error("retracting NULL from MIN should be no-op")
	}
	assertIntResult(t, a, 5)
}

func TestMaxAccumulatorNullRetraction(t *testing.T) {
	a := &MaxAccumulator{}
	a.Add(IntValue{V: 5})
	a.ResetChanged()
	a.Retract(NullValue{})
	if a.HasChanged() {
		t.Error("retracting NULL from MAX should be no-op")
	}
	assertIntResult(t, a, 5)
}

func TestAvgRetractNull(t *testing.T) {
	a := &AvgAccumulator{}
	a.Add(IntValue{V: 10})
	a.Add(IntValue{V: 20})
	a.ResetChanged()
	a.Retract(NullValue{})
	if a.HasChanged() {
		t.Error("retracting NULL from AVG should be no-op")
	}
	assertFloatResult(t, a, 15.0)
}

func TestSumWithFloats(t *testing.T) {
	a := &SumAccumulator{}
	a.Add(FloatValue{V: 1.5})
	a.Add(FloatValue{V: 2.5})
	r := a.Result()
	fv, ok := r.(FloatValue)
	if !ok {
		// Could be IntValue if sum is exact integer
		if iv, ok2 := r.(IntValue); ok2 {
			if iv.V != 4 {
				t.Errorf("expected 4, got %d", iv.V)
			}
			return
		}
		t.Fatalf("expected numeric, got %T: %v", r, r)
	}
	if fv.V != 4.0 {
		t.Errorf("expected 4.0, got %f", fv.V)
	}
}

func TestMinWithFloats(t *testing.T) {
	a := &MinAccumulator{}
	a.Add(FloatValue{V: 3.14})
	a.Add(FloatValue{V: 2.71})
	a.Add(FloatValue{V: 1.41})
	r := a.Result()
	fv, ok := r.(FloatValue)
	if !ok {
		t.Fatalf("expected FloatValue, got %T: %v", r, r)
	}
	if fv.V != 1.41 {
		t.Errorf("expected 1.41, got %f", fv.V)
	}
}

func TestMaxWithFloats(t *testing.T) {
	a := &MaxAccumulator{}
	a.Add(FloatValue{V: 3.14})
	a.Add(FloatValue{V: 2.71})
	a.Add(FloatValue{V: 9.99})
	r := a.Result()
	fv, ok := r.(FloatValue)
	if !ok {
		t.Fatalf("expected FloatValue, got %T: %v", r, r)
	}
	if fv.V != 9.99 {
		t.Errorf("expected 9.99, got %f", fv.V)
	}
}

func TestHasChangedTracking(t *testing.T) {
	tests := []struct {
		name    string
		acc     Accumulator
		addVal  Value
		wantChanged bool
	}{
		{"CountStar_add", &CountStarAccumulator{}, IntValue{V: 1}, true},
		{"Count_addNull", &CountAccumulator{}, NullValue{}, false},
		{"Sum_addNull", &SumAccumulator{}, NullValue{}, false},
		{"Avg_addNull", &AvgAccumulator{}, NullValue{}, false},
		{"Min_addNull", &MinAccumulator{}, NullValue{}, false},
		{"Max_addNull", &MaxAccumulator{}, NullValue{}, false},
		{"First_addNull", &FirstAccumulator{}, NullValue{}, false},
		{"Last_addNull", &LastAccumulator{}, NullValue{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.acc.ResetChanged()
			tt.acc.Add(tt.addVal)
			if tt.acc.HasChanged() != tt.wantChanged {
				t.Errorf("HasChanged()=%v, want %v", tt.acc.HasChanged(), tt.wantChanged)
			}
		})
	}
}

func TestResetChangedClearsFlag(t *testing.T) {
	accs := []Accumulator{
		&CountStarAccumulator{},
		&CountAccumulator{},
		&SumAccumulator{},
		&AvgAccumulator{},
		&MinAccumulator{},
		&MaxAccumulator{},
		&FirstAccumulator{},
		&LastAccumulator{},
	}
	for _, a := range accs {
		a.Add(IntValue{V: 42})
		if !a.HasChanged() {
			continue // some (like Count with NULL) may not change
		}
		a.ResetChanged()
		if a.HasChanged() {
			t.Errorf("%T: ResetChanged did not clear flag", a)
		}
	}
}

func TestMarshalUnmarshal(t *testing.T) {
	tests := []struct {
		name string
		acc  Accumulator
		add  []Value
	}{
		{"CountStar", &CountStarAccumulator{}, []Value{IntValue{V: 1}, IntValue{V: 2}}},
		{"Count", &CountAccumulator{}, []Value{IntValue{V: 1}, IntValue{V: 2}}},
		{"Sum", &SumAccumulator{}, []Value{IntValue{V: 10}, IntValue{V: 20}}},
		{"Avg", &AvgAccumulator{}, []Value{IntValue{V: 10}, IntValue{V: 20}}},
		{"Min", &MinAccumulator{}, []Value{IntValue{V: 10}, IntValue{V: 5}}},
		{"Max", &MaxAccumulator{}, []Value{IntValue{V: 10}, IntValue{V: 50}}},
		{"First", &FirstAccumulator{}, []Value{TextValue{V: "hello"}}},
		{"Last", &LastAccumulator{}, []Value{TextValue{V: "hello"}, TextValue{V: "world"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, v := range tt.add {
				tt.acc.Add(v)
			}
			data, err := tt.acc.Marshal()
			if err != nil {
				t.Fatalf("Marshal error: %v", err)
			}

			// Create a fresh accumulator of the same type and unmarshal
			var fresh Accumulator
			switch tt.acc.(type) {
			case *CountStarAccumulator:
				fresh = &CountStarAccumulator{}
			case *CountAccumulator:
				fresh = &CountAccumulator{}
			case *SumAccumulator:
				fresh = &SumAccumulator{}
			case *AvgAccumulator:
				fresh = &AvgAccumulator{}
			case *MinAccumulator:
				fresh = &MinAccumulator{}
			case *MaxAccumulator:
				fresh = &MaxAccumulator{}
			case *FirstAccumulator:
				fresh = &FirstAccumulator{}
			case *LastAccumulator:
				fresh = &LastAccumulator{}
			}

			if err := fresh.Unmarshal(data); err != nil {
				t.Fatalf("Unmarshal error: %v", err)
			}

			origStr := tt.acc.Result().String()
			freshStr := fresh.Result().String()
			if origStr != freshStr {
				t.Errorf("Result mismatch after marshal/unmarshal: %q vs %q", origStr, freshStr)
			}
		})
	}
}

func TestMerge(t *testing.T) {
	t.Run("CountStar", func(t *testing.T) {
		a := &CountStarAccumulator{}
		a.Add(IntValue{V: 1})
		a.Add(IntValue{V: 2})
		b := &CountStarAccumulator{}
		b.Add(IntValue{V: 3})
		a.Merge(b)
		assertIntResult(t, a, 3)
	})

	t.Run("Count", func(t *testing.T) {
		a := &CountAccumulator{}
		a.Add(IntValue{V: 1})
		b := &CountAccumulator{}
		b.Add(IntValue{V: 2})
		b.Add(IntValue{V: 3})
		a.Merge(b)
		assertIntResult(t, a, 3)
	})

	t.Run("Sum", func(t *testing.T) {
		a := &SumAccumulator{}
		a.Add(IntValue{V: 10})
		b := &SumAccumulator{}
		b.Add(IntValue{V: 20})
		a.Merge(b)
		assertIntResult(t, a, 30)
	})

	t.Run("Min", func(t *testing.T) {
		a := &MinAccumulator{}
		a.Add(IntValue{V: 10})
		b := &MinAccumulator{}
		b.Add(IntValue{V: 5})
		a.Merge(b)
		assertIntResult(t, a, 5)
	})

	t.Run("Max", func(t *testing.T) {
		a := &MaxAccumulator{}
		a.Add(IntValue{V: 10})
		b := &MaxAccumulator{}
		b.Add(IntValue{V: 50})
		a.Merge(b)
		assertIntResult(t, a, 50)
	})

	t.Run("FirstCannotMerge", func(t *testing.T) {
		a := &FirstAccumulator{}
		if a.CanMerge() {
			t.Error("FIRST should not support merge")
		}
	})

	t.Run("LastCannotMerge", func(t *testing.T) {
		a := &LastAccumulator{}
		if a.CanMerge() {
			t.Error("LAST should not support merge")
		}
	})
}

func TestLastRetractDoesNotChange(t *testing.T) {
	a := &LastAccumulator{}
	a.Add(TextValue{V: "first"})
	a.Add(TextValue{V: "second"})
	a.ResetChanged()
	a.Retract(TextValue{V: "second"})
	// Retraction is ignored for LAST, so HasChanged should be false
	if a.HasChanged() {
		t.Error("LAST retraction should not set changed flag")
	}
}

func TestFirstRetractDoesNotChange(t *testing.T) {
	a := &FirstAccumulator{}
	a.Add(TextValue{V: "first"})
	a.ResetChanged()
	a.Retract(TextValue{V: "first"})
	if a.HasChanged() {
		t.Error("FIRST retraction should not set changed flag")
	}
}

func TestSumRetractBelowZeroCount(t *testing.T) {
	// spec 12.2: SUM returns running total, not NULL, after retractions
	a := &SumAccumulator{}
	a.Add(IntValue{V: 5})
	a.Retract(IntValue{V: 5})
	assertIntResult(t, a, 0)
	a.Retract(IntValue{V: 5})
	// count is now -1, sum is -5; SUM still returns running total
	assertIntResult(t, a, -5)
}

func TestAvgSingleElement(t *testing.T) {
	a := &AvgAccumulator{}
	a.Add(IntValue{V: 42})
	assertFloatResult(t, a, 42.0)
}

func TestMinSingleElement(t *testing.T) {
	a := &MinAccumulator{}
	a.Add(IntValue{V: 42})
	assertIntResult(t, a, 42)
}

func TestMaxSingleElement(t *testing.T) {
	a := &MaxAccumulator{}
	a.Add(IntValue{V: 42})
	assertIntResult(t, a, 42)
}

func TestValueToFloat(t *testing.T) {
	tests := []struct {
		name   string
		val    Value
		want   float64
		wantOK bool
	}{
		{"int", IntValue{V: 42}, 42.0, true},
		{"float", FloatValue{V: 3.14}, 3.14, true},
		{"text_number", TextValue{V: "123.45"}, 123.45, true},
		{"text_invalid", TextValue{V: "abc"}, 0, false},
		{"null", NullValue{}, 0, false},
		{"bool", BoolValue{V: true}, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := valueToFloat(tt.val)
			if ok != tt.wantOK {
				t.Errorf("ok=%v, want %v", ok, tt.wantOK)
			}
			if ok && got != tt.want {
				t.Errorf("got %f, want %f", got, tt.want)
			}
		})
	}
}

func TestValuesEqual(t *testing.T) {
	tests := []struct {
		a, b Value
		want bool
	}{
		{nil, nil, true},
		{nil, NullValue{}, false},
		{NullValue{}, NullValue{}, true},
		{NullValue{}, IntValue{V: 0}, false},
		{IntValue{V: 1}, IntValue{V: 1}, true},
		{IntValue{V: 1}, IntValue{V: 2}, false},
		{TextValue{V: "a"}, TextValue{V: "a"}, true},
		{TextValue{V: "a"}, TextValue{V: "b"}, false},
	}
	for i, tt := range tests {
		got := valuesEqual(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("case %d: valuesEqual(%v, %v)=%v, want %v", i, tt.a, tt.b, got, tt.want)
		}
	}
}

func TestAnyToValue(t *testing.T) {
	tests := []struct {
		in   any
		want string
	}{
		{nil, "NULL"},
		{float64(42), "42"},
		{float64(3.14), "3.14"},
		{"hello", "hello"},
		{true, "true"},
	}
	for _, tt := range tests {
		v := anyToValue(tt.in)
		if v.String() != tt.want {
			t.Errorf("anyToValue(%v).String()=%q, want %q", tt.in, v.String(), tt.want)
		}
	}
}

// --- helpers ---

func assertIntResult(t *testing.T, a Accumulator, want int64) {
	t.Helper()
	r := a.Result()
	iv, ok := r.(IntValue)
	if !ok {
		t.Fatalf("expected IntValue, got %T: %v", r, r)
	}
	if iv.V != want {
		t.Errorf("expected %d, got %d", want, iv.V)
	}
}

func assertFloatResult(t *testing.T, a Accumulator, want float64) {
	t.Helper()
	r := a.Result()
	fv, ok := r.(FloatValue)
	if !ok {
		t.Fatalf("expected FloatValue, got %T: %v", r, r)
	}
	if fv.V != want {
		t.Errorf("expected %f, got %f", want, fv.V)
	}
}

// --- Accumulator type coercion tests ---

func TestSumAccumulator_NumericText(t *testing.T) {
	resetAggregateWarnings()
	a := &SumAccumulator{}
	a.Add(IntValue{V: 50})
	a.Add(TextValue{V: "100.50"})
	// TextValue("100.50") parses as float, so SUM should be 150.5
	r := a.Result()
	fv, ok := r.(FloatValue)
	if !ok {
		t.Fatalf("expected FloatValue, got %T: %v", r, r)
	}
	if fv.V != 150.5 {
		t.Errorf("expected 150.5, got %f", fv.V)
	}
}

func TestSumAccumulator_NonNumericText(t *testing.T) {
	resetAggregateWarnings()
	a := &SumAccumulator{}
	a.Add(IntValue{V: 50})
	// "hello" can't parse as number — should be skipped with warning
	a.Add(TextValue{V: "hello"})
	r := a.Result()
	iv, ok := r.(IntValue)
	if !ok {
		t.Fatalf("expected IntValue, got %T: %v", r, r)
	}
	if iv.V != 50 {
		t.Errorf("expected 50, got %d", iv.V)
	}
}

func TestSumAccumulator_BoolValue(t *testing.T) {
	resetAggregateWarnings()
	a := &SumAccumulator{}
	a.Add(IntValue{V: 10})
	// Bool should be skipped with warning
	a.Add(BoolValue{V: true})
	r := a.Result()
	iv, ok := r.(IntValue)
	if !ok {
		t.Fatalf("expected IntValue, got %T: %v", r, r)
	}
	if iv.V != 10 {
		t.Errorf("expected 10, got %d", iv.V)
	}
}

func TestSumAccumulator_JsonValue(t *testing.T) {
	resetAggregateWarnings()
	a := &SumAccumulator{}
	a.Add(IntValue{V: 10})
	// JSON should be skipped with warning
	a.Add(JsonValue{V: map[string]any{"key": "val"}})
	r := a.Result()
	iv, ok := r.(IntValue)
	if !ok {
		t.Fatalf("expected IntValue, got %T: %v", r, r)
	}
	if iv.V != 10 {
		t.Errorf("expected 10, got %d", iv.V)
	}
}

func TestAvgAccumulator_BoolSkipped(t *testing.T) {
	resetAggregateWarnings()
	a := &AvgAccumulator{}
	a.Add(IntValue{V: 10})
	a.Add(IntValue{V: 20})
	a.Add(BoolValue{V: false}) // should be skipped
	r := a.Result().(FloatValue).V
	if r != 15.0 {
		t.Errorf("expected 15.0 (bool skipped), got %f", r)
	}
}

func TestMinAccumulator_BoolSkipped(t *testing.T) {
	resetAggregateWarnings()
	a := &MinAccumulator{}
	a.Add(IntValue{V: 5})
	a.Add(BoolValue{V: true}) // should be skipped
	a.Add(IntValue{V: 10})
	r := a.Result()
	iv, ok := r.(IntValue)
	if !ok {
		t.Fatalf("expected IntValue, got %T: %v", r, r)
	}
	if iv.V != 5 {
		t.Errorf("expected 5, got %d", iv.V)
	}
}

func TestMaxAccumulator_BoolSkipped(t *testing.T) {
	resetAggregateWarnings()
	a := &MaxAccumulator{}
	a.Add(IntValue{V: 5})
	a.Add(BoolValue{V: true}) // should be skipped
	a.Add(IntValue{V: 10})
	r := a.Result()
	iv, ok := r.(IntValue)
	if !ok {
		t.Fatalf("expected IntValue, got %T: %v", r, r)
	}
	if iv.V != 10 {
		t.Errorf("expected 10, got %d", iv.V)
	}
}
