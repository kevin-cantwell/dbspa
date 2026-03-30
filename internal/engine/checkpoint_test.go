package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kevin-cantwell/folddb/internal/sql/ast"
)

// TC-CKPT-001: Basic save and restore cycle
func TestCheckpointSaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	cm, err := NewCheckpointManager("SELECT COUNT(*) GROUP BY region", dir, time.Second)
	if err != nil {
		t.Fatalf("NewCheckpointManager: %v", err)
	}

	state := json.RawMessage(`{"offsets":{"0":100,"1":200},"groups":{"us-east":42}}`)
	data := &CheckpointData{State: state}

	if err := cm.Save(data); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := cm.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded == nil {
		t.Fatal("Load returned nil")
	}

	// Verify state round-trips
	var loadedState map[string]any
	if err := json.Unmarshal(loaded.State, &loadedState); err != nil {
		t.Fatalf("unmarshal state: %v", err)
	}

	offsets, ok := loadedState["offsets"].(map[string]any)
	if !ok {
		t.Fatalf("expected offsets map, got %T", loadedState["offsets"])
	}
	if offsets["0"] != float64(100) {
		t.Errorf("offset[0] = %v, want 100", offsets["0"])
	}
}

// TC-CKPT-002: Query fingerprint mismatch detection
func TestCheckpointFingerprintMismatch(t *testing.T) {
	dir := t.TempDir()
	cm1, err := NewCheckpointManager("SELECT COUNT(*) GROUP BY a", dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	data := &CheckpointData{State: json.RawMessage(`{}`)}
	if err := cm1.Save(data); err != nil {
		t.Fatal(err)
	}

	// Different query => different fingerprint
	cm2, err := NewCheckpointManager("SELECT SUM(v) GROUP BY b", dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	_, err = cm2.Load()
	if err == nil {
		t.Fatal("expected fingerprint mismatch error, got nil")
	}
}

// TC-CKPT-003: QueryFingerprint determinism
func TestQueryFingerprint(t *testing.T) {
	sql := "SELECT COUNT(*) GROUP BY region"
	h1 := QueryFingerprint(sql)
	h2 := QueryFingerprint(sql)
	if h1 != h2 {
		t.Errorf("same query produces different fingerprints: %s vs %s", h1, h2)
	}
	if len(h1) != 16 {
		t.Errorf("fingerprint length = %d, want 16", len(h1))
	}

	// Different queries should produce different fingerprints
	h3 := QueryFingerprint("SELECT SUM(v) GROUP BY region")
	if h3 == h1 {
		t.Error("different queries should produce different fingerprints")
	}
}

// TC-CKPT-004: Corrupted checkpoint file
func TestCheckpointCorrupted(t *testing.T) {
	dir := t.TempDir()
	cm, err := NewCheckpointManager("SELECT *", dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Write garbage to checkpoint file
	cpPath := filepath.Join(dir, "checkpoint.json")
	if err := os.WriteFile(cpPath, []byte("{truncated"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err = cm.Load()
	if err == nil {
		t.Fatal("expected error on corrupted checkpoint, got nil")
	}
}

// TC-CKPT-005: Atomic write - temp file does not persist on success
func TestCheckpointAtomicWrite(t *testing.T) {
	dir := t.TempDir()
	cm, err := NewCheckpointManager("SELECT *", dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	data := &CheckpointData{State: json.RawMessage(`{"key":"value"}`)}
	if err := cm.Save(data); err != nil {
		t.Fatal(err)
	}

	// Temp file should not exist after successful save
	tmpPath := filepath.Join(dir, "checkpoint.json.tmp")
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error("temp file should not exist after successful save")
	}

	// Checkpoint file should exist
	cpPath := filepath.Join(dir, "checkpoint.json")
	if _, err := os.Stat(cpPath); err != nil {
		t.Errorf("checkpoint file should exist: %v", err)
	}
}

// TC-CKPT-005 continued: Crash simulation - if temp file exists without rename, original is used
func TestCheckpointCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	cm, err := NewCheckpointManager("SELECT *", dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Save a valid checkpoint
	data := &CheckpointData{State: json.RawMessage(`{"version":1}`)}
	if err := cm.Save(data); err != nil {
		t.Fatal(err)
	}

	// Simulate crash: write a temp file (as if rename never happened)
	tmpPath := filepath.Join(dir, "checkpoint.json.tmp")
	if err := os.WriteFile(tmpPath, []byte("incomplete"), 0644); err != nil {
		t.Fatal(err)
	}

	// Load should succeed with the original valid checkpoint
	loaded, err := cm.Load()
	if err != nil {
		t.Fatalf("Load should use valid checkpoint, not temp file: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected loaded checkpoint")
	}

	var state map[string]any
	json.Unmarshal(loaded.State, &state)
	if state["version"] != float64(1) {
		t.Errorf("state version = %v, want 1", state["version"])
	}
}

// TC-CKPT-008: ShouldFlush interval
func TestCheckpointShouldFlush(t *testing.T) {
	dir := t.TempDir()
	cm, err := NewCheckpointManager("SELECT *", dir, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	// Before any save, should flush immediately
	if !cm.ShouldFlush() {
		t.Error("should flush before any save")
	}

	data := &CheckpointData{State: json.RawMessage(`{}`)}
	cm.Save(data)

	// Right after save, should NOT flush
	if cm.ShouldFlush() {
		t.Error("should not flush immediately after save")
	}

	// Wait for interval
	time.Sleep(15 * time.Millisecond)
	if !cm.ShouldFlush() {
		t.Error("should flush after interval has elapsed")
	}
}

// TC-CKPT-009: Reset deletes checkpoint
func TestCheckpointReset(t *testing.T) {
	dir := t.TempDir()
	cm, err := NewCheckpointManager("SELECT *", dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	data := &CheckpointData{State: json.RawMessage(`{"key":"value"}`)}
	cm.Save(data)

	if err := cm.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	loaded, err := cm.Load()
	if err != nil {
		t.Fatalf("Load after reset: %v", err)
	}
	if loaded != nil {
		t.Error("expected nil after reset, checkpoint should be gone")
	}
}

// TC-CKPT-009: Reset on non-existent checkpoint is not an error
func TestCheckpointResetNoFile(t *testing.T) {
	dir := t.TempDir()
	cm, err := NewCheckpointManager("SELECT *", dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	if err := cm.Reset(); err != nil {
		t.Errorf("Reset on empty dir should not error: %v", err)
	}
}

// Load when no checkpoint exists returns nil, nil
func TestCheckpointLoadEmpty(t *testing.T) {
	dir := t.TempDir()
	cm, err := NewCheckpointManager("SELECT *", dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	loaded, err := cm.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if loaded != nil {
		t.Error("expected nil for non-existent checkpoint")
	}
}

// StateDir returns correct path
func TestCheckpointStateDir(t *testing.T) {
	dir := t.TempDir()
	cm, err := NewCheckpointManager("SELECT *", dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if cm.StateDir() != dir {
		t.Errorf("StateDir = %q, want %q", cm.StateDir(), dir)
	}
}

// Save populates QueryHash and Timestamp
func TestCheckpointSaveSetsMetadata(t *testing.T) {
	dir := t.TempDir()
	cm, err := NewCheckpointManager("SELECT *", dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	before := time.Now()
	data := &CheckpointData{State: json.RawMessage(`{}`)}
	cm.Save(data)

	loaded, err := cm.Load()
	if err != nil {
		t.Fatal(err)
	}

	if loaded.QueryHash == "" {
		t.Error("QueryHash should be set")
	}
	if loaded.Timestamp.Before(before) {
		t.Error("Timestamp should be set to approximately now")
	}
}

// Multiple saves: last one wins
func TestCheckpointMultipleSaves(t *testing.T) {
	dir := t.TempDir()
	cm, err := NewCheckpointManager("SELECT *", dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		data := &CheckpointData{
			State: json.RawMessage(`{"version":` + string(rune('0'+i)) + `}`),
		}
		cm.Save(data)
	}

	loaded, err := cm.Load()
	if err != nil {
		t.Fatal(err)
	}

	var state map[string]any
	json.Unmarshal(loaded.State, &state)
	if state["version"] != float64(4) {
		t.Errorf("last save should win, got version %v", state["version"])
	}
}

// --- Stateful AggregateOp integration tests ---

// buildMultiAggOp creates an AggregateOp for:
//
//	SELECT g, COUNT(*) AS cnt, SUM(val) AS total GROUP BY g
func buildMultiAggOp() *AggregateOp {
	columns := []AggColumn{
		{Alias: "g", Expr: &ast.ColumnRef{Name: "g"}, GroupByIdx: 0},
		{Alias: "cnt", IsAggregate: true, AggFunc: "COUNT", IsStar: true, GroupByIdx: -1},
		{Alias: "total", IsAggregate: true, AggFunc: "SUM", AggArg: &ast.ColumnRef{Name: "val"}, GroupByIdx: -1},
	}
	groupBy := []ast.Expr{&ast.ColumnRef{Name: "g"}}
	return NewAggregateOp(columns, groupBy, nil)
}

// TestAggregateOp_MarshalUnmarshalRoundTrip tests that accumulator state
// survives a marshal/unmarshal cycle and continues accumulating correctly.
func TestAggregateOp_MarshalUnmarshalRoundTrip(t *testing.T) {
	op := buildMultiAggOp()

	// Phase 1: feed initial records
	records := []Record{
		mkRec(1, map[string]Value{"g": TextValue{V: "a"}, "val": IntValue{V: 10}}),
		mkRec(1, map[string]Value{"g": TextValue{V: "b"}, "val": IntValue{V: 20}}),
		mkRec(1, map[string]Value{"g": TextValue{V: "a"}, "val": IntValue{V: 30}}),
	}
	processAndCollect(op, records)

	// Marshal state
	data, err := op.MarshalState()
	if err != nil {
		t.Fatalf("MarshalState: %v", err)
	}

	// Unmarshal into a fresh op
	op2 := buildMultiAggOp()
	if err := op2.UnmarshalState(data); err != nil {
		t.Fatalf("UnmarshalState: %v", err)
	}

	// Verify restored state via CurrentState
	state := op2.CurrentState()
	if len(state) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(state))
	}

	type gv struct{ cnt, total int64 }
	groups := make(map[string]gv)
	for _, rec := range state {
		g := rec.Columns["g"].String()
		cnt := rec.Columns["cnt"].(IntValue).V
		total := rec.Columns["total"].(IntValue).V
		groups[g] = gv{cnt, total}
	}

	if g := groups["a"]; g.cnt != 2 || g.total != 40 {
		t.Errorf("group 'a': cnt=%d (want 2), total=%d (want 40)", g.cnt, g.total)
	}
	if g := groups["b"]; g.cnt != 1 || g.total != 20 {
		t.Errorf("group 'b': cnt=%d (want 1), total=%d (want 20)", g.cnt, g.total)
	}

	// Phase 2: feed more records after restore — accumulation should continue
	more := []Record{
		mkRec(1, map[string]Value{"g": TextValue{V: "a"}, "val": IntValue{V: 5}}),
		mkRec(1, map[string]Value{"g": TextValue{V: "c"}, "val": IntValue{V: 100}}),
	}
	results := processAndCollect(op2, more)
	if len(results) == 0 {
		t.Fatal("expected output records after resumed processing")
	}

	finalState := op2.CurrentState()
	finalGroups := make(map[string]gv)
	for _, rec := range finalState {
		g := rec.Columns["g"].String()
		cnt := rec.Columns["cnt"].(IntValue).V
		total := rec.Columns["total"].(IntValue).V
		finalGroups[g] = gv{cnt, total}
	}

	if g := finalGroups["a"]; g.cnt != 3 || g.total != 45 {
		t.Errorf("group 'a' after resume: cnt=%d (want 3), total=%d (want 45)", g.cnt, g.total)
	}
	if g := finalGroups["b"]; g.cnt != 1 || g.total != 20 {
		t.Errorf("group 'b' after resume: cnt=%d (want 1), total=%d (want 20)", g.cnt, g.total)
	}
	if g := finalGroups["c"]; g.cnt != 1 || g.total != 100 {
		t.Errorf("group 'c' after resume: cnt=%d (want 1), total=%d (want 100)", g.cnt, g.total)
	}
}

// TestAggregateOp_UnmarshalState_ColumnMismatch verifies that restoring
// into an op with a different schema produces an error.
func TestAggregateOp_UnmarshalState_ColumnMismatch(t *testing.T) {
	op := buildMultiAggOp()
	records := []Record{
		mkRec(1, map[string]Value{"g": TextValue{V: "a"}, "val": IntValue{V: 10}}),
	}
	processAndCollect(op, records)

	data, err := op.MarshalState()
	if err != nil {
		t.Fatalf("MarshalState: %v", err)
	}

	// Different schema: COUNT only, no SUM
	diffOp := buildSimpleAggOp("g", "COUNT", "*", "cnt", nil)
	if err := diffOp.UnmarshalState(data); err == nil {
		t.Fatal("expected error on column mismatch, got nil")
	}
}

// TestStatefulEndToEnd_CheckpointSaveRestore simulates the full stateful
// pipeline: run query, checkpoint, restart with same query, verify continuation.
func TestStatefulEndToEnd_CheckpointSaveRestore(t *testing.T) {
	tmpDir := t.TempDir()
	stateDir := filepath.Join(tmpDir, "state")

	sql := "SELECT g, COUNT(*) AS cnt FROM stream GROUP BY g"

	// --- Run 1: Process some records and checkpoint ---
	mgr, err := NewCheckpointManager(sql, stateDir, time.Second)
	if err != nil {
		t.Fatalf("NewCheckpointManager: %v", err)
	}

	op := buildSimpleAggOp("g", "COUNT", "*", "cnt", nil)
	records := []Record{
		mkRec(1, map[string]Value{"g": TextValue{V: "x"}}),
		mkRec(1, map[string]Value{"g": TextValue{V: "y"}}),
		mkRec(1, map[string]Value{"g": TextValue{V: "x"}}),
	}
	processAndCollect(op, records)

	// Save checkpoint
	stateBytes, err := op.MarshalState()
	if err != nil {
		t.Fatalf("MarshalState: %v", err)
	}
	if err := mgr.Save(&CheckpointData{State: stateBytes}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Verify checkpoint file exists
	cpPath := filepath.Join(stateDir, "checkpoint.json")
	if _, err := os.Stat(cpPath); err != nil {
		t.Fatalf("checkpoint file not found: %v", err)
	}

	// --- Run 2: Restart, restore, process more records ---
	mgr2, err := NewCheckpointManager(sql, stateDir, time.Second)
	if err != nil {
		t.Fatalf("NewCheckpointManager: %v", err)
	}

	loaded, err := mgr2.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected checkpoint data, got nil")
	}

	op2 := buildSimpleAggOp("g", "COUNT", "*", "cnt", nil)
	if err := op2.UnmarshalState(loaded.State); err != nil {
		t.Fatalf("UnmarshalState: %v", err)
	}

	// Verify restored state
	restored := op2.CurrentState()
	restoredMap := make(map[string]int64)
	for _, rec := range restored {
		g := rec.Columns["g"].String()
		cnt := rec.Columns["cnt"].(IntValue).V
		restoredMap[g] = cnt
	}
	if restoredMap["x"] != 2 {
		t.Errorf("restored group 'x': cnt=%d, want 2", restoredMap["x"])
	}
	if restoredMap["y"] != 1 {
		t.Errorf("restored group 'y': cnt=%d, want 1", restoredMap["y"])
	}

	// Process more records on the restored op
	moreRecords := []Record{
		mkRec(1, map[string]Value{"g": TextValue{V: "x"}}),
		mkRec(1, map[string]Value{"g": TextValue{V: "z"}}),
	}
	processAndCollect(op2, moreRecords)

	// Verify final state reflects continuation
	finalState := op2.CurrentState()
	finalMap := make(map[string]int64)
	for _, rec := range finalState {
		g := rec.Columns["g"].String()
		cnt := rec.Columns["cnt"].(IntValue).V
		finalMap[g] = cnt
	}

	if finalMap["x"] != 3 {
		t.Errorf("final group 'x': cnt=%d, want 3", finalMap["x"])
	}
	if finalMap["y"] != 1 {
		t.Errorf("final group 'y': cnt=%d, want 1", finalMap["y"])
	}
	if finalMap["z"] != 1 {
		t.Errorf("final group 'z': cnt=%d, want 1", finalMap["z"])
	}
}
