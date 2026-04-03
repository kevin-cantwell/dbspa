package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/linkedin/goavro/v2"
)

// --- Order dataset (plain JSON) ---

var (
	orderStatuses = []string{"pending", "confirmed", "shipped", "delivered", "cancelled"}
	orderRegions  = []string{"us-east", "us-west", "eu-west", "eu-central", "ap-south", "ap-east"}
	orderProducts = []string{"widget-a", "widget-b", "gadget-x", "gadget-y", "doohickey", "thingamajig", "whatchamacallit"}
)

func genOrder(rng *rand.Rand, seq int) map[string]any {
	qty := rng.Intn(10) + 1
	price := float64(rng.Intn(9900)+100) / 100.0
	return map[string]any{
		"order_id":    seq + 1,
		"customer_id": rng.Intn(5000) + 1,
		"product":     orderProducts[rng.Intn(len(orderProducts))],
		"quantity":    qty,
		"price":       price,
		"total":       float64(qty) * price,
		"status":      orderStatuses[rng.Intn(len(orderStatuses))],
		"region":      orderRegions[rng.Intn(len(orderRegions))],
		"created_at":  time.Now().UTC().Add(-time.Duration(rng.Intn(86400)) * time.Second).Format(time.RFC3339),
	}
}

// --- Orders CDC dataset (Debezium envelope) ---

type cdcGenerator struct {
	active map[int]*cdcOrder
	nextID int
}

type cdcOrder struct {
	ID         int
	CustomerID int
	Product    string
	Quantity   int
	Price      float64
	Total      float64
	Status     string
	Region     string
	UpdatedAt  string
}

func (o *cdcOrder) toMap() map[string]any {
	return map[string]any{
		"order_id":    o.ID,
		"customer_id": o.CustomerID,
		"product":     o.Product,
		"quantity":    o.Quantity,
		"price":       o.Price,
		"total":       o.Total,
		"status":      o.Status,
		"region":      o.Region,
		"updated_at":  o.UpdatedAt,
	}
}

func newCDCGenerator(_ *rand.Rand) *cdcGenerator {
	return &cdcGenerator{
		active: make(map[int]*cdcOrder),
		nextID: 1,
	}
}

func (g *cdcGenerator) next(rng *rand.Rand, seq int) map[string]any {
	now := time.Now().UTC().Format(time.RFC3339)
	roll := rng.Float64()

	createBias := 0.6
	if len(g.active) > 50 {
		createBias = 0.3
	}
	if len(g.active) > 200 {
		createBias = 0.15
	}

	if roll < createBias || len(g.active) < 5 {
		qty := rng.Intn(10) + 1
		price := float64(rng.Intn(9900)+100) / 100.0
		o := &cdcOrder{
			ID:         g.nextID,
			CustomerID: rng.Intn(5000) + 1,
			Product:    orderProducts[rng.Intn(len(orderProducts))],
			Quantity:   qty,
			Price:      price,
			Total:      float64(qty) * price,
			Status:     "pending",
			Region:     orderRegions[rng.Intn(len(orderRegions))],
			UpdatedAt:  now,
		}
		g.nextID++
		g.active[o.ID] = o

		return map[string]any{
			"op":    "c",
			"after": o.toMap(),
			"source": map[string]any{
				"db":    "ecommerce",
				"table": "orders",
				"ts_ms": time.Now().UnixMilli(),
			},
		}
	}

	if roll < createBias+0.55 {
		o := g.pickRandom(rng)
		if o == nil {
			return g.next(rng, seq)
		}
		before := o.toMap()
		switch o.Status {
		case "pending":
			o.Status = "confirmed"
		case "confirmed":
			o.Status = "shipped"
		case "shipped":
			o.Status = "delivered"
		case "delivered":
			return g.next(rng, seq)
		}
		o.UpdatedAt = now
		return map[string]any{
			"op":     "u",
			"before": before,
			"after":  o.toMap(),
			"source": map[string]any{
				"db":    "ecommerce",
				"table": "orders",
				"ts_ms": time.Now().UnixMilli(),
			},
		}
	}

	o := g.pickRandom(rng)
	if o == nil {
		return g.next(rng, seq)
	}
	before := o.toMap()
	delete(g.active, o.ID)
	return map[string]any{
		"op":     "d",
		"before": before,
		"source": map[string]any{
			"db":    "ecommerce",
			"table": "orders",
			"ts_ms": time.Now().UnixMilli(),
		},
	}
}

func (g *cdcGenerator) pickRandom(rng *rand.Rand) *cdcOrder {
	if len(g.active) == 0 {
		return nil
	}
	idx := rng.Intn(len(g.active))
	i := 0
	for _, v := range g.active {
		if i == idx {
			return v
		}
		i++
	}
	return nil
}

// --- API Metrics dataset ---

var (
	metricEndpoints = []string{"/api/users", "/api/orders", "/api/products", "/api/search", "/api/health", "/api/checkout", "/api/inventory"}
	metricMethods   = []string{"GET", "GET", "GET", "POST", "POST", "PUT", "DELETE"}
)

func genMetric(rng *rand.Rand, seq int) map[string]any {
	idx := rng.Intn(len(metricEndpoints))
	code := 200
	roll := rng.Float64()
	if roll > 0.95 {
		code = 500
	} else if roll > 0.90 {
		code = 404
	} else if roll > 0.85 {
		code = 400
	} else if roll > 0.80 {
		code = 201
	}
	latency := rng.Intn(50) + 2
	if code >= 400 {
		latency = rng.Intn(300) + 100
	}
	if metricEndpoints[idx] == "/api/search" {
		latency += rng.Intn(150) + 50
	}
	return map[string]any{
		"endpoint":    metricEndpoints[idx],
		"method":      metricMethods[rng.Intn(len(metricMethods))],
		"status_code": code,
		"latency_ms":  latency,
		"region":      orderRegions[rng.Intn(len(orderRegions))],
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
	}
}

// --- Clickstream dataset ---

var (
	clickPages   = []string{"/home", "/products", "/products/123", "/cart", "/checkout", "/account", "/search", "/about"}
	clickActions = []string{"view", "view", "view", "click", "click", "scroll", "scroll", "purchase", "add_to_cart"}
)

func genClick(rng *rand.Rand, seq int) map[string]any {
	return map[string]any{
		"user_id":     fmt.Sprintf("user_%d", rng.Intn(500)+1),
		"page":        clickPages[rng.Intn(len(clickPages))],
		"action":      clickActions[rng.Intn(len(clickActions))],
		"session_id":  fmt.Sprintf("sess_%04x", rng.Intn(200)+1),
		"duration_ms": rng.Intn(30000) + 100,
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
	}
}

// --- Debezium Avro encoder (OCF with typed before/after records) ---

const debeziumAvroSchema = `{
	"type": "record",
	"name": "Envelope",
	"namespace": "dbspa.test",
	"fields": [
		{"name": "op", "type": "string"},
		{"name": "before", "type": ["null", {
			"type": "record",
			"name": "Order",
			"fields": [
				{"name": "order_id", "type": "int"},
				{"name": "customer_id", "type": "int"},
				{"name": "product", "type": "string"},
				{"name": "quantity", "type": "int"},
				{"name": "price", "type": "double"},
				{"name": "total", "type": "double"},
				{"name": "status", "type": "string"},
				{"name": "region", "type": "string"},
				{"name": "updated_at", "type": "string"}
			]
		}], "default": null},
		{"name": "after", "type": ["null", "Order"], "default": null},
		{"name": "source_db", "type": "string", "default": ""},
		{"name": "source_table", "type": "string", "default": ""},
		{"name": "source_ts_ms", "type": "long", "default": 0}
	]
}`

type debeziumAvroEncoder struct {
	writer *goavro.OCFWriter
}

func newDebeziumAvroEncoder() (*debeziumAvroEncoder, error) {
	writer, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      os.Stdout,
		Schema: debeziumAvroSchema,
	})
	if err != nil {
		return nil, fmt.Errorf("debezium avro writer error: %w", err)
	}
	return &debeziumAvroEncoder{writer: writer}, nil
}

func (e *debeziumAvroEncoder) Encode(rec map[string]any) error {
	avroRec := toDebeziumAvroMap(rec)
	return e.writer.Append([]any{avroRec})
}

func (e *debeziumAvroEncoder) Close() error { return nil }

// toDebeziumAvroMap converts a CDC event map to Avro-compatible types with
// typed before/after records (not JSON strings).
func toDebeziumAvroMap(rec map[string]any) map[string]any {
	result := map[string]any{
		"op":             rec["op"],
		"before":         orderToAvroUnion(rec["before"]),
		"after":          orderToAvroUnion(rec["after"]),
		"source_db":      "",
		"source_table":   "",
		"source_ts_ms":   int64(0),
	}

	if src, ok := rec["source"].(map[string]any); ok {
		if db, ok := src["db"].(string); ok {
			result["source_db"] = db
		}
		if table, ok := src["table"].(string); ok {
			result["source_table"] = table
		}
		if tsMs, ok := src["ts_ms"].(int64); ok {
			result["source_ts_ms"] = tsMs
		}
	}

	return result
}

// orderToAvroUnion converts an order map to the Avro union format.
// null -> nil, record -> goavro.Union("dbspa.test.Order", typedMap)
func orderToAvroUnion(v any) any {
	if v == nil {
		return nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	typed := map[string]any{
		"order_id":    int32(toInt(m["order_id"])),
		"customer_id": int32(toInt(m["customer_id"])),
		"product":     m["product"],
		"quantity":    int32(toInt(m["quantity"])),
		"price":       toFloat64(m["price"]),
		"total":       toFloat64(m["total"]),
		"status":      m["status"],
		"region":      m["region"],
		"updated_at":  m["updated_at"],
	}
	return goavro.Union("dbspa.test.Order", typed)
}

// toAvroMap converts a generic map to Avro-compatible types.
// goavro expects int32 for "int" fields and specific union handling.
func toAvroMap(rec map[string]any, dataset string) map[string]any {
	switch dataset {
	case "orders":
		return map[string]any{
			"order_id":    int32(toInt(rec["order_id"])),
			"customer_id": int32(toInt(rec["customer_id"])),
			"product":     rec["product"],
			"quantity":    int32(toInt(rec["quantity"])),
			"price":       toFloat64(rec["price"]),
			"total":       toFloat64(rec["total"]),
			"status":      rec["status"],
			"region":      rec["region"],
			"created_at":  rec["created_at"],
		}
	case "metrics":
		return map[string]any{
			"endpoint":    rec["endpoint"],
			"method":      rec["method"],
			"status_code": int32(toInt(rec["status_code"])),
			"latency_ms":  int32(toInt(rec["latency_ms"])),
			"region":      rec["region"],
			"timestamp":   rec["timestamp"],
		}
	case "clickstream":
		return map[string]any{
			"user_id":     rec["user_id"],
			"page":        rec["page"],
			"action":      rec["action"],
			"session_id":  rec["session_id"],
			"duration_ms": int32(toInt(rec["duration_ms"])),
			"timestamp":   rec["timestamp"],
		}
	case "orders-cdc":
		// Serialize nested maps to JSON strings for Avro
		beforeJSON := avroUnion(rec["before"])
		afterJSON := avroUnion(rec["after"])
		sourceJSON := avroUnion(rec["source"])
		return map[string]any{
			"op":     rec["op"],
			"before": beforeJSON,
			"after":  afterJSON,
			"source": sourceJSON,
		}
	default:
		return rec
	}
}

func avroUnion(v any) any {
	if v == nil {
		return goavro.Union("null", nil)
	}
	data, _ := json.Marshal(v)
	return goavro.Union("string", string(data))
}

func toInt(v any) int {
	switch val := v.(type) {
	case int:
		return val
	case int64:
		return int(val)
	case float64:
		return int(val)
	default:
		return 0
	}
}

func toFloat64(v any) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case int:
		return float64(val)
	case int64:
		return float64(val)
	default:
		return 0
	}
}
