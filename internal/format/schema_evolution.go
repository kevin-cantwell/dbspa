package format

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
)

// avroFieldInfo represents a single field from a parsed Avro schema.
type avroFieldInfo struct {
	Name string
	Type string // simplified type: "null", "boolean", "int", "long", "float", "double", "string", "bytes", "record", "array", "map", "enum", "fixed", "union"
}

// schemaLineage tracks the evolution of a single Avro record type (by name).
type schemaLineage struct {
	lastSchemaID int32
	lastFieldMap map[string]string // field name → type
}

// schemaEvolutionTracker compares successive Avro schemas fetched from the
// registry and detects breaking changes. Schemas are grouped by record name,
// so different record types (e.g., "Order" and "User") are tracked independently.
type schemaEvolutionTracker struct {
	mu       sync.Mutex
	lineages map[string]*schemaLineage // record name → lineage
	warned   map[string]bool           // "schemaName:schemaID" → already warned
}

func newSchemaEvolutionTracker() *schemaEvolutionTracker {
	return &schemaEvolutionTracker{
		lineages: make(map[string]*schemaLineage),
		warned:   make(map[string]bool),
	}
}

// checkEvolution compares a new schema against the previously seen schema
// with the same record name. Returns an error for incompatible changes.
// Compatible changes are logged as warnings (rate-limited per schema ID).
func (t *schemaEvolutionTracker) checkEvolution(schemaID int32, rawSchema string) error {
	name, fields, err := parseAvroRecord(rawSchema)
	if err != nil {
		// Can't parse schema — not a record type, skip evolution check
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	lineage, exists := t.lineages[name]
	if !exists {
		// First schema for this record name — just record it
		t.lineages[name] = &schemaLineage{
			lastSchemaID: schemaID,
			lastFieldMap: fieldsToMap(fields),
		}
		return nil
	}

	if schemaID == lineage.lastSchemaID {
		return nil // same schema, no evolution
	}

	// Already warned about this schema transition
	warnKey := fmt.Sprintf("%s:%d", name, schemaID)
	if t.warned[warnKey] {
		lineage.lastSchemaID = schemaID
		lineage.lastFieldMap = fieldsToMap(fields)
		return nil
	}

	newFieldMap := fieldsToMap(fields)

	// Check for removed fields (present in old, absent in new)
	for fieldName, oldType := range lineage.lastFieldMap {
		if _, ok := newFieldMap[fieldName]; !ok {
			return fmt.Errorf("schema evolution: field %q (type %s) removed in schema ID %d (was present in schema ID %d)",
				fieldName, oldType, schemaID, lineage.lastSchemaID)
		}
	}

	// Check for type changes
	for fieldName, oldType := range lineage.lastFieldMap {
		newType, ok := newFieldMap[fieldName]
		if !ok {
			continue // already handled above
		}
		if oldType != newType {
			if isCompatibleAvroTypeChange(oldType, newType) {
				log.Printf("Warning: schema evolution: field %q type changed from %s to %s in schema ID %d (compatible)",
					fieldName, oldType, newType, schemaID)
			} else {
				return fmt.Errorf("schema evolution: field %q type changed from %s to %s in schema ID %d (incompatible)",
					fieldName, oldType, newType, schemaID)
			}
		}
	}

	// Check for added fields (present in new, absent in old) — this is always compatible
	for fieldName, newType := range newFieldMap {
		if _, ok := lineage.lastFieldMap[fieldName]; !ok {
			log.Printf("Warning: schema evolution: field %q (type %s) added in schema ID %d", fieldName, newType, schemaID)
		}
	}

	t.warned[warnKey] = true
	lineage.lastSchemaID = schemaID
	lineage.lastFieldMap = newFieldMap
	return nil
}

// parseAvroRecord extracts the record name and fields from a raw Avro schema JSON.
func parseAvroRecord(rawSchema string) (string, []avroFieldInfo, error) {
	var schema struct {
		Type      string `json:"type"`
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
		Fields    []struct {
			Name string          `json:"name"`
			Type json.RawMessage `json:"type"`
		} `json:"fields"`
	}
	if err := json.Unmarshal([]byte(rawSchema), &schema); err != nil {
		return "", nil, err
	}
	if schema.Type != "record" {
		return "", nil, fmt.Errorf("not a record type: %s", schema.Type)
	}

	fullName := schema.Name
	if schema.Namespace != "" {
		fullName = schema.Namespace + "." + schema.Name
	}

	var fields []avroFieldInfo
	for _, f := range schema.Fields {
		fields = append(fields, avroFieldInfo{
			Name: f.Name,
			Type: simplifyAvroType(f.Type),
		})
	}
	return fullName, fields, nil
}

// parseAvroFields extracts field names and types from a raw Avro schema JSON.
// Kept for backward compatibility with tests.
func parseAvroFields(rawSchema string) ([]avroFieldInfo, error) {
	_, fields, err := parseAvroRecord(rawSchema)
	return fields, err
}

// simplifyAvroType returns a simplified string representation of an Avro type.
func simplifyAvroType(raw json.RawMessage) string {
	// Try as simple string type: "int", "string", etc.
	var simple string
	if err := json.Unmarshal(raw, &simple); err == nil {
		return simple
	}

	// Try as union: ["null", "int"]
	var union []json.RawMessage
	if err := json.Unmarshal(raw, &union); err == nil {
		types := make([]string, 0, len(union))
		for _, u := range union {
			types = append(types, simplifyAvroType(u))
		}
		// For nullable types like ["null", "int"], return "int" (the non-null type)
		if len(types) == 2 && types[0] == "null" {
			return types[1]
		}
		if len(types) == 2 && types[1] == "null" {
			return types[0]
		}
		return "union"
	}

	// Try as complex type: {"type": "record", ...}
	var complex struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &complex); err == nil {
		return complex.Type
	}

	return "unknown"
}

// fieldsToMap converts a slice of avroFieldInfo to a name→type map.
func fieldsToMap(fields []avroFieldInfo) map[string]string {
	m := make(map[string]string, len(fields))
	for _, f := range fields {
		m[f.Name] = f.Type
	}
	return m
}

// isCompatibleAvroTypeChange returns true if changing from oldType to newType
// is a compatible Avro schema evolution (promotion).
func isCompatibleAvroTypeChange(oldType, newType string) bool {
	// Avro type promotions per the spec
	switch {
	case oldType == "int" && newType == "long":
		return true
	case oldType == "int" && newType == "float":
		return true
	case oldType == "int" && newType == "double":
		return true
	case oldType == "long" && newType == "float":
		return true
	case oldType == "long" && newType == "double":
		return true
	case oldType == "float" && newType == "double":
		return true
	case oldType == "string" && newType == "bytes":
		return true
	case oldType == "bytes" && newType == "string":
		return true
	default:
		return false
	}
}
