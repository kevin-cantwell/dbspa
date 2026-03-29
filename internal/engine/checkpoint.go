package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// CheckpointManager handles persisting and restoring stateful checkpoint data.
type CheckpointManager struct {
	queryHash string
	stateDir  string
	interval  time.Duration
	lastFlush time.Time
}

// CheckpointData is the serialized checkpoint state.
type CheckpointData struct {
	QueryHash string          `json:"query_hash"`
	Timestamp time.Time       `json:"timestamp"`
	State     json.RawMessage `json:"state,omitempty"`
}

// NewCheckpointManager creates a checkpoint manager for the given query.
func NewCheckpointManager(sql string, stateDir string, interval time.Duration) (*CheckpointManager, error) {
	hash := QueryFingerprint(sql)

	if stateDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("cannot determine home directory: %w", err)
		}
		stateDir = filepath.Join(home, ".folddb", "state", hash)
	}

	if err := os.MkdirAll(stateDir, 0755); err != nil {
		return nil, fmt.Errorf("cannot create state directory: %w", err)
	}

	if interval == 0 {
		interval = 5 * time.Second
	}

	return &CheckpointManager{
		queryHash: hash,
		stateDir:  stateDir,
		interval:  interval,
	}, nil
}

// QueryFingerprint produces a stable hash of the normalized SQL query.
func QueryFingerprint(sql string) string {
	h := sha256.Sum256([]byte(sql))
	return hex.EncodeToString(h[:8]) // 16-char hex
}

// Save writes the checkpoint atomically (write temp, fsync, rename).
func (cm *CheckpointManager) Save(data *CheckpointData) error {
	data.QueryHash = cm.queryHash
	data.Timestamp = time.Now()

	b, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("checkpoint marshal error: %w", err)
	}

	checkpointPath := filepath.Join(cm.stateDir, "checkpoint.json")
	tmpPath := checkpointPath + ".tmp"

	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("checkpoint write error: %w", err)
	}

	if _, err := f.Write(b); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("checkpoint write error: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("checkpoint fsync error: %w", err)
	}
	f.Close()

	if err := os.Rename(tmpPath, checkpointPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("checkpoint rename error: %w", err)
	}

	cm.lastFlush = time.Now()
	return nil
}

// Load restores the checkpoint if it exists and the query fingerprint matches.
func (cm *CheckpointManager) Load() (*CheckpointData, error) {
	checkpointPath := filepath.Join(cm.stateDir, "checkpoint.json")

	b, err := os.ReadFile(checkpointPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No checkpoint
		}
		return nil, fmt.Errorf("checkpoint read error: %w", err)
	}

	var data CheckpointData
	if err := json.Unmarshal(b, &data); err != nil {
		return nil, fmt.Errorf("checkpoint unmarshal error: %w", err)
	}

	if data.QueryHash != cm.queryHash {
		return nil, fmt.Errorf("checkpoint query fingerprint mismatch: expected %s, got %s", cm.queryHash, data.QueryHash)
	}

	return &data, nil
}

// ShouldFlush returns true if enough time has passed since the last flush.
func (cm *CheckpointManager) ShouldFlush() bool {
	return time.Since(cm.lastFlush) >= cm.interval
}

// Reset deletes the checkpoint for this query.
func (cm *CheckpointManager) Reset() error {
	checkpointPath := filepath.Join(cm.stateDir, "checkpoint.json")
	err := os.Remove(checkpointPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// StateDir returns the state directory path.
func (cm *CheckpointManager) StateDir() string {
	return cm.stateDir
}

// ListCheckpoints lists all checkpointed queries in the default state directory.
func ListCheckpoints() ([]CheckpointInfo, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}

	stateRoot := filepath.Join(home, ".folddb", "state")
	entries, err := os.ReadDir(stateRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var results []CheckpointInfo
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		cpPath := filepath.Join(stateRoot, entry.Name(), "checkpoint.json")
		b, err := os.ReadFile(cpPath)
		if err != nil {
			continue
		}
		var data CheckpointData
		if err := json.Unmarshal(b, &data); err != nil {
			continue
		}
		results = append(results, CheckpointInfo{
			Hash:      data.QueryHash,
			Timestamp: data.Timestamp,
			Dir:       filepath.Join(stateRoot, entry.Name()),
		})
	}

	return results, nil
}

// ResetCheckpoint deletes the checkpoint for the given query hash.
func ResetCheckpoint(hash string) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	dir := filepath.Join(home, ".folddb", "state", hash)
	return os.RemoveAll(dir)
}

// CheckpointInfo describes a stored checkpoint.
type CheckpointInfo struct {
	Hash      string
	Timestamp time.Time
	Dir       string
}
