package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
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
