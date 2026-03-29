package format

import (
	"time"

	"github.com/kevin-cantwell/folddb/internal/engine"
	"github.com/kevin-cantwell/folddb/internal/source"
)

// InjectKafkaVirtuals adds Kafka transport-layer virtual columns to all
// records decoded from a KafkaRecord. This is called after format decoding.
func InjectKafkaVirtuals(recs []engine.Record, kr source.KafkaRecord) []engine.Record {
	keyStr := source.EncodeKafkaKey(kr.Key)
	for i := range recs {
		if recs[i].Columns == nil {
			recs[i].Columns = make(map[string]engine.Value)
		}
		recs[i].Columns["_offset"] = engine.IntValue{V: kr.Offset}
		recs[i].Columns["_partition"] = engine.IntValue{V: int64(kr.Partition)}
		if !kr.Timestamp.IsZero() {
			recs[i].Columns["_timestamp"] = engine.TimestampValue{V: kr.Timestamp.UTC()}
		} else {
			recs[i].Columns["_timestamp"] = engine.TimestampValue{V: time.Now().UTC()}
		}
		if keyStr != "" {
			recs[i].Columns["_key"] = engine.TextValue{V: keyStr}
		} else {
			recs[i].Columns["_key"] = engine.NullValue{}
		}
	}
	return recs
}
