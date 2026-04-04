package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"strings"
)

// HighCardinalityStream generates NDJSON with many unique group keys.
func HighCardinalityStream(count int, uniqueKeys int) io.Reader {
	r, w := io.Pipe()
	go func() {
		defer w.Close()
		rng := rand.New(rand.NewSource(42))
		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		for i := 0; i < count; i++ {
			enc.Encode(map[string]any{
				"group_key": fmt.Sprintf("key_%d", rng.Intn(uniqueKeys)),
				"value":     rng.Intn(1000),
			})
		}
	}()
	return r
}

// HotKeySkewStream generates NDJSON where skewPct of traffic goes to hotKeys keys.
func HotKeySkewStream(count int, hotKeys int, coldKeys int, skewPct float64) io.Reader {
	r, w := io.Pipe()
	go func() {
		defer w.Close()
		rng := rand.New(rand.NewSource(42))
		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		for i := 0; i < count; i++ {
			var key string
			if rng.Float64() < skewPct {
				key = fmt.Sprintf("hot_%d", rng.Intn(hotKeys))
			} else {
				key = fmt.Sprintf("cold_%d", rng.Intn(coldKeys))
			}
			enc.Encode(map[string]any{
				"group_key": key,
				"value":     rng.Intn(1000),
			})
		}
	}()
	return r
}

// SchemaDriftStream generates NDJSON that changes a field's type after driftAfter records.
func SchemaDriftStream(count int, driftAfter int) io.Reader {
	r, w := io.Pipe()
	go func() {
		defer w.Close()
		rng := rand.New(rand.NewSource(42))
		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		for i := 0; i < count; i++ {
			rec := map[string]any{
				"id":     i,
				"region": fmt.Sprintf("region_%d", rng.Intn(10)),
			}
			if i < driftAfter {
				rec["amount"] = rng.Intn(1000)
			} else {
				// Type change: int -> string (incompatible for SUM)
				rec["amount"] = "not_a_number"
			}
			enc.Encode(rec)
		}
	}()
	return r
}

// HighRetractionCDCStream generates Debezium CDC JSON with a high update ratio.
func HighRetractionCDCStream(count int, updatePct float64) io.Reader {
	r, w := io.Pipe()
	go func() {
		defer w.Close()
		rng := rand.New(rand.NewSource(42))
		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)

		type row struct {
			ID     int
			Status string
			Amount int
		}
		active := make(map[int]*row)
		activeIDs := make([]int, 0, 1024) // O(1) random pick
		nextID := 1
		statuses := []string{"pending", "confirmed", "shipped", "delivered"}

		for i := 0; i < count; i++ {
			roll := rng.Float64()

			// Force creates if not enough active rows
			if len(active) < 10 || roll >= updatePct {
				o := &row{ID: nextID, Status: "pending", Amount: rng.Intn(1000)}
				nextID++
				active[o.ID] = o
				activeIDs = append(activeIDs, o.ID)
				enc.Encode(map[string]any{
					"op":    "c",
					"after": map[string]any{"id": o.ID, "status": o.Status, "amount": o.Amount},
				})
				continue
			}

			// Pick a random active row (O(1) via slice)
			var o *row
			for o == nil {
				idx := rng.Intn(len(activeIDs))
				id := activeIDs[idx]
				o = active[id]
				if o == nil {
					// Stale entry — swap-remove
					activeIDs[idx] = activeIDs[len(activeIDs)-1]
					activeIDs = activeIDs[:len(activeIDs)-1]
				}
			}
			before := map[string]any{"id": o.ID, "status": o.Status, "amount": o.Amount}

			// Advance status
			for si, s := range statuses {
				if s == o.Status && si < len(statuses)-1 {
					o.Status = statuses[si+1]
					break
				}
			}
			o.Amount = rng.Intn(1000)

			enc.Encode(map[string]any{
				"op":     "u",
				"before": before,
				"after":  map[string]any{"id": o.ID, "status": o.Status, "amount": o.Amount},
			})
		}
	}()
	return r
}

// WideRecordStream generates NDJSON with many columns and nested objects.
func WideRecordStream(count int, columns int, nestDepth int) io.Reader {
	r, w := io.Pipe()
	go func() {
		defer w.Close()
		rng := rand.New(rand.NewSource(42))
		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		for i := 0; i < count; i++ {
			rec := make(map[string]any, columns)
			rec["id"] = i
			rec["group_key"] = fmt.Sprintf("g_%d", rng.Intn(100))
			for c := 0; c < columns-2; c++ {
				if c < columns/3 {
					rec[fmt.Sprintf("int_%d", c)] = rng.Intn(10000)
				} else if c < 2*columns/3 {
					rec[fmt.Sprintf("str_%d", c)] = fmt.Sprintf("val_%d_%d", i, c)
				} else {
					// Nested object
					rec[fmt.Sprintf("nested_%d", c)] = buildNested(rng, nestDepth)
				}
			}
			enc.Encode(rec)
		}
	}()
	return r
}

func buildNested(rng *rand.Rand, depth int) any {
	if depth <= 0 {
		return rng.Intn(1000)
	}
	return map[string]any{
		"value": rng.Intn(1000),
		"child": buildNested(rng, depth-1),
	}
}

// PlainStream generates simple NDJSON with a configurable number of unique group keys.
func PlainStream(count int, groups int) io.Reader {
	r, w := io.Pipe()
	go func() {
		defer w.Close()
		rng := rand.New(rand.NewSource(42))
		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		regions := []string{"us-east", "us-west", "eu-west", "eu-central", "ap-south", "ap-east"}
		for i := 0; i < count; i++ {
			enc.Encode(map[string]any{
				"group_key": fmt.Sprintf("g_%d", rng.Intn(groups)),
				"region":    regions[rng.Intn(len(regions))],
				"value":     rng.Intn(10000),
				"price":     float64(rng.Intn(9900)+100) / 100.0,
			})
		}
	}()
	return r
}

// JoinStream generates NDJSON stream events with a user_id that randomly
// references entries in a reference table of tableSize users. matchPct controls
// the fraction of events whose user_id falls within [0, tableSize) — the
// remaining events use an out-of-range id and will not match the join.
func JoinStream(count int, tableSize int, matchPct float64) io.Reader {
	r, w := io.Pipe()
	go func() {
		defer w.Close()
		rng := rand.New(rand.NewSource(42))
		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		for i := 0; i < count; i++ {
			var userID int
			if rng.Float64() < matchPct {
				userID = rng.Intn(tableSize)
			} else {
				userID = tableSize + rng.Intn(tableSize) // out-of-range, no match
			}
			enc.Encode(map[string]any{
				"event_id": i,
				"user_id":  fmt.Sprintf("user_%d", userID),
				"amount":   rng.Intn(1000),
			})
		}
	}()
	return r
}

// ReaderToBytes drains an io.Reader into a byte slice.
func ReaderToBytes(r io.Reader) []byte {
	b, err := io.ReadAll(r)
	if err != nil {
		panic(fmt.Sprintf("ReaderToBytes: %v", err))
	}
	return b
}

// CountLines counts newlines in output bytes.
func CountLines(data []byte) int {
	return strings.Count(string(data), "\n")
}
