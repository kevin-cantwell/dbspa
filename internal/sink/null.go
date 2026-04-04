package sink

import "github.com/kevin-cantwell/dbspa/internal/engine"

// NullSink discards all records. Used for benchmarking pipeline throughput
// without sink serialization or I/O overhead.
type NullSink struct{}

func (s *NullSink) Write(_ engine.Record) error { return nil }
func (s *NullSink) Close() error                { return nil }
