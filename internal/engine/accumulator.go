package engine

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
)

// Accumulator is the interface for retraction-aware aggregate state.
type Accumulator interface {
	// Add incorporates a value (diff=+1).
	Add(value Value)
	// Retract removes a value (diff=-1).
	Retract(value Value)
	// Result returns the current aggregate value.
	Result() Value
	// HasChanged reports whether Result() changed after the last Add/Retract.
	HasChanged() bool
	// Reset clears the changed flag without modifying state.
	ResetChanged()
	// CanMerge reports whether this accumulator supports Merge.
	CanMerge() bool
	// Merge combines another accumulator's state into this one.
	Merge(other Accumulator)
	// Marshal serializes the accumulator state.
	Marshal() ([]byte, error)
	// Unmarshal restores accumulator state.
	Unmarshal([]byte) error
	// SetInitial sets the accumulator to a pre-computed initial value.
	// Used by SEED FROM to inject pre-accumulated state (e.g., from BigQuery).
	// This replaces whatever state the accumulator had.
	// Supported for COUNT, COUNT(*), SUM, MIN, MAX.
	// Not supported for AVG (needs both SUM and COUNT), MEDIAN, FIRST, LAST —
	// those accumulators log a warning and ignore the call.
	SetInitial(value Value)
}

// --- CountStarAccumulator ---

// CountStarAccumulator counts all rows, including NULLs. O(1) state.
type CountStarAccumulator struct {
	count   int64
	prev    int64
	changed bool
}

func (a *CountStarAccumulator) Add(_ Value) {
	a.prev = a.count
	a.count++
	a.changed = a.count != a.prev
}

func (a *CountStarAccumulator) Retract(_ Value) {
	a.prev = a.count
	a.count--
	if a.count < 0 {
		a.count = 0 // spec 12.2: COUNT clamped at 0
	}
	a.changed = a.count != a.prev
}

func (a *CountStarAccumulator) Result() Value {
	return IntValue{V: a.count}
}

func (a *CountStarAccumulator) HasChanged() bool { return a.changed }
func (a *CountStarAccumulator) ResetChanged()    { a.changed = false }
func (a *CountStarAccumulator) CanMerge() bool    { return true }
func (a *CountStarAccumulator) Merge(other Accumulator) {
	if o, ok := other.(*CountStarAccumulator); ok {
		a.prev = a.count
		a.count += o.count
		a.changed = a.count != a.prev
	}
}

func (a *CountStarAccumulator) SetInitial(value Value) {
	f, ok := valueToFloat(value)
	if !ok {
		log.Printf("Warning: COUNT(*) SetInitial received non-numeric value %v, ignoring", value)
		return
	}
	a.count = int64(f)
	a.changed = true
}

func (a *CountStarAccumulator) Marshal() ([]byte, error) {
	return json.Marshal(a.count)
}

func (a *CountStarAccumulator) Unmarshal(data []byte) error {
	return json.Unmarshal(data, &a.count)
}

// --- CountAccumulator ---

// CountAccumulator counts non-NULL values. O(1) state.
type CountAccumulator struct {
	count   int64
	prev    int64
	changed bool
}

func (a *CountAccumulator) Add(value Value) {
	if value.IsNull() {
		return
	}
	a.prev = a.count
	a.count++
	a.changed = a.count != a.prev
}

func (a *CountAccumulator) Retract(value Value) {
	if value.IsNull() {
		return // spec 12.2: retraction of NULL is ignored
	}
	a.prev = a.count
	a.count--
	if a.count < 0 {
		a.count = 0
	}
	a.changed = a.count != a.prev
}

func (a *CountAccumulator) Result() Value {
	return IntValue{V: a.count}
}

func (a *CountAccumulator) HasChanged() bool { return a.changed }
func (a *CountAccumulator) ResetChanged()    { a.changed = false }
func (a *CountAccumulator) CanMerge() bool    { return true }
func (a *CountAccumulator) Merge(other Accumulator) {
	if o, ok := other.(*CountAccumulator); ok {
		a.prev = a.count
		a.count += o.count
		a.changed = a.count != a.prev
	}
}

func (a *CountAccumulator) SetInitial(value Value) {
	f, ok := valueToFloat(value)
	if !ok {
		log.Printf("Warning: COUNT SetInitial received non-numeric value %v, ignoring", value)
		return
	}
	a.count = int64(f)
	a.changed = true
}

func (a *CountAccumulator) Marshal() ([]byte, error) {
	return json.Marshal(a.count)
}

func (a *CountAccumulator) Unmarshal(data []byte) error {
	return json.Unmarshal(data, &a.count)
}

// --- SumAccumulator ---

// SumAccumulator maintains a running sum. O(1) state. Skips NULLs.
// Returns NULL if no non-NULL inputs. SUM can go negative (spec 12.2).
type SumAccumulator struct {
	sum      float64
	count    int64 // count of non-NULL values added
	hasValue bool  // true once any non-NULL value has been added; never reset
	prevSum  float64
	prevCnt  int64
	changed  bool
}

func (a *SumAccumulator) Add(value Value) {
	if value.IsNull() {
		return
	}
	f, ok := valueToFloat(value)
	if !ok {
		// Non-numeric value: warn and skip.
		warnIncompatibleAggregate("SUM", value)
		a.hasValue = true
		a.changed = true
		return
	}
	a.prevSum = a.sum
	a.prevCnt = a.count
	a.sum += f
	a.count++
	a.hasValue = true
	a.changed = true
}

func (a *SumAccumulator) Retract(value Value) {
	if value.IsNull() {
		return // spec 12.2: retraction of NULL is ignored
	}
	f, ok := valueToFloat(value)
	if !ok {
		return
	}
	a.prevSum = a.sum
	a.prevCnt = a.count
	a.sum -= f
	a.count--
	a.changed = true
}

func (a *SumAccumulator) Result() Value {
	if !a.hasValue {
		return NullValue{}
	}
	// Return int if the sum is an exact integer
	if a.sum == float64(int64(a.sum)) {
		return IntValue{V: int64(a.sum)}
	}
	return FloatValue{V: a.sum}
}

func (a *SumAccumulator) HasChanged() bool { return a.changed }
func (a *SumAccumulator) ResetChanged()    { a.changed = false }
func (a *SumAccumulator) CanMerge() bool    { return true }
func (a *SumAccumulator) Merge(other Accumulator) {
	if o, ok := other.(*SumAccumulator); ok {
		a.sum += o.sum
		a.count += o.count
		a.changed = true
	}
}

func (a *SumAccumulator) SetInitial(value Value) {
	f, ok := valueToFloat(value)
	if !ok {
		log.Printf("Warning: SUM SetInitial received non-numeric value %v, ignoring", value)
		return
	}
	a.sum = f
	a.count = 1 // mark as having at least one value
	a.hasValue = true
	a.changed = true
}

func (a *SumAccumulator) Marshal() ([]byte, error) {
	hasVal := float64(0)
	if a.hasValue {
		hasVal = 1
	}
	return json.Marshal([]float64{a.sum, float64(a.count), hasVal})
}

func (a *SumAccumulator) Unmarshal(data []byte) error {
	var v []float64
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	if len(v) >= 2 {
		a.sum = v[0]
		a.count = int64(v[1])
	}
	if len(v) >= 3 {
		a.hasValue = v[2] != 0
	}
	return nil
}

// --- AvgAccumulator ---

// AvgAccumulator maintains sum + count for running average. O(1) state.
type AvgAccumulator struct {
	sum     float64
	count   int64
	prevRes Value
	changed bool
}

func (a *AvgAccumulator) Add(value Value) {
	if value.IsNull() {
		return
	}
	f, ok := valueToFloat(value)
	if !ok {
		warnIncompatibleAggregate("AVG", value)
		return
	}
	a.prevRes = a.Result()
	a.sum += f
	a.count++
	a.changed = !valuesEqual(a.prevRes, a.Result())
}

func (a *AvgAccumulator) Retract(value Value) {
	if value.IsNull() {
		return
	}
	f, ok := valueToFloat(value)
	if !ok {
		return
	}
	a.prevRes = a.Result()
	a.sum -= f
	a.count--
	a.changed = !valuesEqual(a.prevRes, a.Result())
}

func (a *AvgAccumulator) Result() Value {
	if a.count <= 0 {
		return NullValue{}
	}
	return FloatValue{V: a.sum / float64(a.count)}
}

func (a *AvgAccumulator) HasChanged() bool { return a.changed }
func (a *AvgAccumulator) ResetChanged()    { a.changed = false }
func (a *AvgAccumulator) CanMerge() bool    { return true }
func (a *AvgAccumulator) Merge(other Accumulator) {
	if o, ok := other.(*AvgAccumulator); ok {
		a.sum += o.sum
		a.count += o.count
		a.changed = true
	}
}

func (a *AvgAccumulator) SetInitial(value Value) {
	log.Printf("Warning: AVG cannot be seeded from pre-accumulated state (need both SUM and COUNT). Use SUM + COUNT separately in the seed query.")
}

func (a *AvgAccumulator) Marshal() ([]byte, error) {
	return json.Marshal([]float64{a.sum, float64(a.count)})
}

func (a *AvgAccumulator) Unmarshal(data []byte) error {
	var v []float64
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	if len(v) >= 2 {
		a.sum = v[0]
		a.count = int64(v[1])
	}
	return nil
}

// --- MinAccumulator ---

// MinAccumulator maintains all values in a min-heap. O(n) state.
type MinAccumulator struct {
	values  []float64
	prevRes Value
	changed bool
}

func (a *MinAccumulator) Add(value Value) {
	if value.IsNull() {
		return
	}
	f, ok := valueToFloat(value)
	if !ok {
		warnIncompatibleAggregate("MIN", value)
		return
	}
	a.prevRes = a.Result()
	a.values = append(a.values, f)
	sort.Float64s(a.values)
	a.changed = !valuesEqual(a.prevRes, a.Result())
}

func (a *MinAccumulator) Retract(value Value) {
	if value.IsNull() {
		return
	}
	f, ok := valueToFloat(value)
	if !ok {
		return
	}
	a.prevRes = a.Result()
	for i, v := range a.values {
		if v == f {
			a.values = append(a.values[:i], a.values[i+1:]...)
			break
		}
	}
	a.changed = !valuesEqual(a.prevRes, a.Result())
}

func (a *MinAccumulator) Result() Value {
	if len(a.values) == 0 {
		return NullValue{}
	}
	v := a.values[0]
	if v == float64(int64(v)) {
		return IntValue{V: int64(v)}
	}
	return FloatValue{V: v}
}

func (a *MinAccumulator) HasChanged() bool { return a.changed }
func (a *MinAccumulator) ResetChanged()    { a.changed = false }
func (a *MinAccumulator) CanMerge() bool    { return true }
func (a *MinAccumulator) Merge(other Accumulator) {
	if o, ok := other.(*MinAccumulator); ok {
		a.values = append(a.values, o.values...)
		sort.Float64s(a.values)
		a.changed = true
	}
}

func (a *MinAccumulator) SetInitial(value Value) {
	f, ok := valueToFloat(value)
	if !ok {
		log.Printf("Warning: MIN SetInitial received non-numeric value %v, ignoring", value)
		return
	}
	a.values = []float64{f}
	a.changed = true
}

func (a *MinAccumulator) Marshal() ([]byte, error) {
	return json.Marshal(a.values)
}

func (a *MinAccumulator) Unmarshal(data []byte) error {
	return json.Unmarshal(data, &a.values)
}

// --- MaxAccumulator ---

// MaxAccumulator maintains all values in sorted order (descending for max). O(n) state.
type MaxAccumulator struct {
	values  []float64
	prevRes Value
	changed bool
}

func (a *MaxAccumulator) Add(value Value) {
	if value.IsNull() {
		return
	}
	f, ok := valueToFloat(value)
	if !ok {
		warnIncompatibleAggregate("MAX", value)
		return
	}
	a.prevRes = a.Result()
	a.values = append(a.values, f)
	sort.Float64s(a.values)
	a.changed = !valuesEqual(a.prevRes, a.Result())
}

func (a *MaxAccumulator) Retract(value Value) {
	if value.IsNull() {
		return
	}
	f, ok := valueToFloat(value)
	if !ok {
		return
	}
	a.prevRes = a.Result()
	for i, v := range a.values {
		if v == f {
			a.values = append(a.values[:i], a.values[i+1:]...)
			break
		}
	}
	a.changed = !valuesEqual(a.prevRes, a.Result())
}

func (a *MaxAccumulator) Result() Value {
	if len(a.values) == 0 {
		return NullValue{}
	}
	v := a.values[len(a.values)-1]
	if v == float64(int64(v)) {
		return IntValue{V: int64(v)}
	}
	return FloatValue{V: v}
}

func (a *MaxAccumulator) HasChanged() bool { return a.changed }
func (a *MaxAccumulator) ResetChanged()    { a.changed = false }
func (a *MaxAccumulator) CanMerge() bool    { return true }
func (a *MaxAccumulator) Merge(other Accumulator) {
	if o, ok := other.(*MaxAccumulator); ok {
		a.values = append(a.values, o.values...)
		sort.Float64s(a.values)
		a.changed = true
	}
}

func (a *MaxAccumulator) SetInitial(value Value) {
	f, ok := valueToFloat(value)
	if !ok {
		log.Printf("Warning: MAX SetInitial received non-numeric value %v, ignoring", value)
		return
	}
	a.values = []float64{f}
	a.changed = true
}

func (a *MaxAccumulator) Marshal() ([]byte, error) {
	return json.Marshal(a.values)
}

func (a *MaxAccumulator) Unmarshal(data []byte) error {
	return json.Unmarshal(data, &a.values)
}

// --- FirstAccumulator ---

// FirstAccumulator records the first non-NULL value. Not retractable.
type FirstAccumulator struct {
	value   Value
	set     bool
	changed bool
}

func (a *FirstAccumulator) Add(value Value) {
	if value.IsNull() {
		return
	}
	if !a.set {
		a.value = value
		a.set = true
		a.changed = true
	}
}

func (a *FirstAccumulator) Retract(_ Value) {
	// spec 12.2: retraction ignored for FIRST
	log.Printf("debug: FIRST retraction ignored")
}

func (a *FirstAccumulator) Result() Value {
	if !a.set {
		return NullValue{}
	}
	return a.value
}

func (a *FirstAccumulator) HasChanged() bool { return a.changed }
func (a *FirstAccumulator) ResetChanged()    { a.changed = false }
func (a *FirstAccumulator) CanMerge() bool    { return false }
func (a *FirstAccumulator) Merge(_ Accumulator) {}

func (a *FirstAccumulator) SetInitial(value Value) {
	log.Printf("Warning: FIRST cannot be seeded from pre-accumulated state.")
}

func (a *FirstAccumulator) Marshal() ([]byte, error) {
	if !a.set {
		return json.Marshal(nil)
	}
	return json.Marshal(a.value.ToJSON())
}

func (a *FirstAccumulator) Unmarshal(data []byte) error {
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	if v == nil {
		a.set = false
		return nil
	}
	a.value = anyToValue(v)
	a.set = true
	return nil
}

// --- LastAccumulator ---

// LastAccumulator records the most recent non-NULL value. Updates on +1, ignores -1.
type LastAccumulator struct {
	value   Value
	set     bool
	changed bool
}

func (a *LastAccumulator) Add(value Value) {
	if value.IsNull() {
		return
	}
	a.value = value
	a.set = true
	a.changed = true
}

func (a *LastAccumulator) Retract(_ Value) {
	// spec: LAST retraction ignored
	log.Printf("debug: LAST retraction ignored")
}

func (a *LastAccumulator) Result() Value {
	if !a.set {
		return NullValue{}
	}
	return a.value
}

func (a *LastAccumulator) HasChanged() bool { return a.changed }
func (a *LastAccumulator) ResetChanged()    { a.changed = false }
func (a *LastAccumulator) CanMerge() bool    { return false }
func (a *LastAccumulator) Merge(_ Accumulator) {}

func (a *LastAccumulator) SetInitial(value Value) {
	log.Printf("Warning: LAST cannot be seeded from pre-accumulated state.")
}

func (a *LastAccumulator) Marshal() ([]byte, error) {
	if !a.set {
		return json.Marshal(nil)
	}
	return json.Marshal(a.value.ToJSON())
}

func (a *LastAccumulator) Unmarshal(data []byte) error {
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	if v == nil {
		a.set = false
		return nil
	}
	a.value = anyToValue(v)
	a.set = true
	return nil
}

// --- Helpers ---

// aggregateWarnings tracks which aggregate+type combinations have already been
// warned about, to avoid spamming stderr with repetitive messages.
var (
	aggregateWarnings   = make(map[string]bool)
	aggregateWarningsMu sync.Mutex
)

// AggregateErrorHandler is called when a numeric aggregate receives an
// incompatible value. If nil, errors are logged to stderr only.
// Set this from the CLI layer to route to the dead letter file.
var AggregateErrorHandler func(aggName string, val Value)

// warnIncompatibleAggregate logs a warning and optionally routes to the
// error handler when a numeric aggregate receives a non-numeric value.
// Rate-limited: one warning per aggregate+type pair.
func warnIncompatibleAggregate(aggName string, val Value) {
	key := aggName + ":" + val.Type()
	aggregateWarningsMu.Lock()
	defer aggregateWarningsMu.Unlock()
	if aggregateWarnings[key] {
		// Still call the error handler (for DLQ) even if we've already warned
		if AggregateErrorHandler != nil {
			AggregateErrorHandler(aggName, val)
		}
		return
	}
	aggregateWarnings[key] = true
	log.Printf("Warning: %s received %s value, expected numeric. Value skipped.", aggName, val.Type())
	if AggregateErrorHandler != nil {
		AggregateErrorHandler(aggName, val)
	}
}

// resetAggregateWarnings clears the warning dedup state. Used in tests.
func resetAggregateWarnings() {
	aggregateWarningsMu.Lock()
	defer aggregateWarningsMu.Unlock()
	aggregateWarnings = make(map[string]bool)
}

// valueToFloat extracts a float64 from a numeric Value.
func valueToFloat(v Value) (float64, bool) {
	switch val := v.(type) {
	case IntValue:
		return float64(val.V), true
	case FloatValue:
		return val.V, true
	case TextValue:
		// Try parsing text as number for SUM/AVG etc.
		var f float64
		if _, err := fmt.Sscanf(val.V, "%f", &f); err == nil {
			return f, true
		}
		return 0, false
	default:
		return 0, false
	}
}

// valuesEqual compares two Values for equality.
func valuesEqual(a, b Value) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.IsNull() && b.IsNull() {
		return true
	}
	if a.IsNull() || b.IsNull() {
		return false
	}
	return a.String() == b.String()
}

// anyToValue converts a raw Go value to an engine Value.
func anyToValue(v any) Value {
	if v == nil {
		return NullValue{}
	}
	switch val := v.(type) {
	case float64:
		if val == float64(int64(val)) {
			return IntValue{V: int64(val)}
		}
		return FloatValue{V: val}
	case string:
		return TextValue{V: val}
	case bool:
		return BoolValue{V: val}
	default:
		return TextValue{V: fmt.Sprintf("%v", val)}
	}
}

// Ensure heap.Interface is not needed here — we use sort-based approach for simplicity.
var _ heap.Interface = (*float64Heap)(nil) // compile-time check, but we don't use it

type float64Heap []float64

func (h float64Heap) Len() int            { return len(h) }
func (h float64Heap) Less(i, j int) bool   { return h[i] < h[j] }
func (h float64Heap) Swap(i, j int)        { h[i], h[j] = h[j], h[i] }
func (h *float64Heap) Push(x any)         { *h = append(*h, x.(float64)) }
func (h *float64Heap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
