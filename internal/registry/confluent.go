// Package registry provides an HTTP client for the Confluent Schema Registry.
package registry

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/linkedin/goavro/v2"
)

// CachedSchema holds a parsed schema fetched from the registry.
type CachedSchema struct {
	ID         int32
	Schema     string         // raw Avro schema JSON string
	SchemaType string         // "AVRO", "PROTOBUF", etc.
	Codec      *goavro.Codec  // parsed Avro codec (nil for non-AVRO types)
}

// Client is an HTTP client for the Confluent Schema Registry.
// It caches schemas by ID since schema IDs are immutable.
type Client struct {
	BaseURL    string
	HTTPClient *http.Client
	cache      map[int32]*CachedSchema
	mu         sync.RWMutex
}

// NewClient creates a new Schema Registry client for the given base URL.
func NewClient(baseURL string) *Client {
	return &Client{
		BaseURL: baseURL,
		HTTPClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		cache: make(map[int32]*CachedSchema),
	}
}

// schemaResponse is the JSON response from GET /schemas/ids/{id}.
type schemaResponse struct {
	Schema     string `json:"schema"`
	SchemaType string `json:"schemaType"`
}

// GetSchema fetches and caches a schema by ID.
// Schema IDs are immutable in Confluent Schema Registry, so caching is safe.
func (c *Client) GetSchema(id int32) (*CachedSchema, error) {
	// Fast path: check cache with read lock
	c.mu.RLock()
	if cached, ok := c.cache[id]; ok {
		c.mu.RUnlock()
		return cached, nil
	}
	c.mu.RUnlock()

	// Slow path: fetch from registry
	url := fmt.Sprintf("%s/schemas/ids/%d", c.BaseURL, id)

	var resp *http.Response
	var err error

	// Retry on 5xx errors (up to 3 attempts with backoff)
	for attempt := 0; attempt < 3; attempt++ {
		resp, err = c.HTTPClient.Get(url)
		if err != nil {
			if attempt < 2 {
				time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
				continue
			}
			return nil, fmt.Errorf("schema registry HTTP error: %w", err)
		}
		if resp.StatusCode >= 500 {
			resp.Body.Close()
			if attempt < 2 {
				time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
				continue
			}
			return nil, fmt.Errorf("schema registry returned %d for schema ID %d", resp.StatusCode, id)
		}
		break
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return nil, fmt.Errorf("schema ID %d not found in registry", id)
	}
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("schema registry returned %d for schema ID %d: %s", resp.StatusCode, id, body)
	}

	var sr schemaResponse
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		return nil, fmt.Errorf("schema registry JSON decode error: %w", err)
	}

	// Default to AVRO if schemaType is empty (older registries)
	if sr.SchemaType == "" {
		sr.SchemaType = "AVRO"
	}

	cached := &CachedSchema{
		ID:         id,
		Schema:     sr.Schema,
		SchemaType: sr.SchemaType,
	}

	// Parse Avro codec
	if sr.SchemaType == "AVRO" {
		codec, err := goavro.NewCodec(sr.Schema)
		if err != nil {
			return nil, fmt.Errorf("cannot parse Avro schema (ID %d): %w", id, err)
		}
		cached.Codec = codec
	}

	// Store in cache
	c.mu.Lock()
	c.cache[id] = cached
	c.mu.Unlock()

	return cached, nil
}

// GetCodec returns a goavro.Codec for the given schema ID.
// Returns an error if the schema is not of type AVRO.
func (c *Client) GetCodec(id int32) (*goavro.Codec, error) {
	schema, err := c.GetSchema(id)
	if err != nil {
		return nil, err
	}
	if schema.Codec == nil {
		return nil, fmt.Errorf("schema ID %d is type %q, not AVRO", id, schema.SchemaType)
	}
	return schema.Codec, nil
}
