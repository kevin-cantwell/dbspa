//go:build ignore

// gen.go generates testdata files for DBSPA integration tests.
package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"
)

func main() {
	rng := rand.New(rand.NewSource(42))
	baseTime := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)

	genOrders(rng, baseTime)
	genOrdersCDC(rng, baseTime)
	genAPIMetrics(rng, baseTime)
	genOrdersCSV(rng, baseTime)
	genDebeziumEnvelope(baseTime)
	genMalformed()
}

func genOrders(rng *rand.Rand, baseTime time.Time) {
	f, _ := os.Create("testdata/orders.ndjson")
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)

	statuses := []string{"pending", "confirmed", "shipped", "delivered", "cancelled"}
	regions := []string{"us-east", "us-west", "eu-west", "eu-central", "ap-southeast"}
	products := []string{"widget-a", "widget-b", "gadget-x", "gadget-y", "gizmo-z"}

	for i := 1; i <= 1000; i++ {
		ts := baseTime.Add(time.Duration(i) * 30 * time.Second)
		enc.Encode(map[string]any{
			"order_id":   fmt.Sprintf("ORD-%06d", i),
			"customer_id": fmt.Sprintf("CUST-%04d", rng.Intn(200)+1),
			"product":    products[rng.Intn(len(products))],
			"quantity":   rng.Intn(10) + 1,
			"price":      float64(rng.Intn(9900)+100) / 100.0,
			"total":      0, // will be set below
			"status":     statuses[rng.Intn(len(statuses))],
			"region":     regions[rng.Intn(len(regions))],
			"created_at": ts.Format(time.RFC3339),
		})
	}
	// Fix totals by re-reading (simpler: just compute inline)
	// Actually the encoder already wrote, so let's regenerate properly
	f.Close()

	f2, _ := os.Create("testdata/orders.ndjson")
	defer f2.Close()
	enc2 := json.NewEncoder(f2)
	enc2.SetEscapeHTML(false)

	for i := 1; i <= 1000; i++ {
		ts := baseTime.Add(time.Duration(i) * 30 * time.Second)
		qty := rng.Intn(10) + 1
		price := float64(rng.Intn(9900)+100) / 100.0
		enc2.Encode(map[string]any{
			"order_id":    fmt.Sprintf("ORD-%06d", i),
			"customer_id": fmt.Sprintf("CUST-%04d", rng.Intn(200)+1),
			"product":     products[rng.Intn(len(products))],
			"quantity":    qty,
			"price":       price,
			"total":       float64(qty) * price,
			"status":      statuses[rng.Intn(len(statuses))],
			"region":      regions[rng.Intn(len(regions))],
			"created_at":  ts.Format(time.RFC3339),
		})
	}
}

func genOrdersCDC(rng *rand.Rand, baseTime time.Time) {
	f, _ := os.Create("testdata/orders_cdc.ndjson")
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)

	regions := []string{"us-east", "us-west", "eu-west", "eu-central", "ap-southeast"}
	products := []string{"widget-a", "widget-b", "gadget-x", "gadget-y", "gizmo-z"}

	type orderState struct {
		OrderID    string  `json:"order_id"`
		CustomerID string  `json:"customer_id"`
		Product    string  `json:"product"`
		Quantity   int     `json:"quantity"`
		Price      float64 `json:"price"`
		Total      float64 `json:"total"`
		Status     string  `json:"status"`
		Region     string  `json:"region"`
		CreatedAt  string  `json:"created_at"`
		UpdatedAt  string  `json:"updated_at"`
	}

	orders := make(map[string]*orderState)
	eventIdx := 0

	emit := func(op string, before, after *orderState, ts time.Time) {
		evt := map[string]any{
			"schema": nil,
			"payload": map[string]any{
				"op":     op,
				"ts_ms":  ts.UnixMilli(),
				"source": map[string]any{
					"version":   "2.5.0",
					"connector": "postgresql",
					"name":      "dbspa-test",
					"ts_ms":     ts.UnixMilli(),
					"db":        "testdb",
					"schema":    "public",
					"table":     "orders",
					"txId":      1000 + eventIdx,
					"lsn":       50000 + eventIdx*100,
				},
			},
		}
		payload := evt["payload"].(map[string]any)
		if before != nil {
			payload["before"] = before
		} else {
			payload["before"] = nil
		}
		if after != nil {
			payload["after"] = after
		} else {
			payload["after"] = nil
		}
		enc.Encode(evt)
		eventIdx++
	}

	// Phase 1: Create 80 orders (snapshot reads "r" for first 20, creates "c" for rest)
	for i := 1; i <= 80; i++ {
		ts := baseTime.Add(time.Duration(i) * 15 * time.Second)
		qty := rng.Intn(10) + 1
		price := float64(rng.Intn(9900)+100) / 100.0
		o := &orderState{
			OrderID:    fmt.Sprintf("CDC-%04d", i),
			CustomerID: fmt.Sprintf("CUST-%04d", rng.Intn(50)+1),
			Product:    products[rng.Intn(len(products))],
			Quantity:   qty,
			Price:      price,
			Total:      float64(qty) * price,
			Status:     "pending",
			Region:     regions[rng.Intn(len(regions))],
			CreatedAt:  ts.Format(time.RFC3339),
			UpdatedAt:  ts.Format(time.RFC3339),
		}
		orders[o.OrderID] = o
		op := "c"
		if i <= 20 {
			op = "r" // snapshot read
		}
		emit(op, nil, o, ts)
	}

	// Phase 2: Update 60 orders to "confirmed"
	for i := 1; i <= 60; i++ {
		key := fmt.Sprintf("CDC-%04d", i)
		o := orders[key]
		ts := baseTime.Add(time.Duration(80+i) * 15 * time.Second)
		before := *o
		o.Status = "confirmed"
		o.UpdatedAt = ts.Format(time.RFC3339)
		emit("u", &before, o, ts)
	}

	// Phase 3: Ship 40 orders
	for i := 1; i <= 40; i++ {
		key := fmt.Sprintf("CDC-%04d", i)
		o := orders[key]
		ts := baseTime.Add(time.Duration(140+i) * 15 * time.Second)
		before := *o
		o.Status = "shipped"
		o.UpdatedAt = ts.Format(time.RFC3339)
		emit("u", &before, o, ts)
	}

	// Phase 4: Cancel 10 orders (delete them)
	for i := 71; i <= 80; i++ {
		key := fmt.Sprintf("CDC-%04d", i)
		o := orders[key]
		ts := baseTime.Add(time.Duration(180+i) * 15 * time.Second)
		before := *o
		emit("d", &before, nil, ts)
	}

	// Phase 5: 10 more updates without _before (simulating REPLICA IDENTITY DEFAULT)
	for i := 41; i <= 50; i++ {
		key := fmt.Sprintf("CDC-%04d", i)
		o := orders[key]
		ts := baseTime.Add(time.Duration(260+i) * 15 * time.Second)
		o.Status = "delivered"
		o.UpdatedAt = ts.Format(time.RFC3339)
		emit("u", nil, o, ts) // no _before
	}

	// Total: 80 creates + 60 updates + 40 updates + 10 deletes + 10 partial updates = 200
}

func genAPIMetrics(rng *rand.Rand, baseTime time.Time) {
	f, _ := os.Create("testdata/api_metrics.ndjson")
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)

	endpoints := []string{
		"/api/v1/users", "/api/v1/orders", "/api/v1/products",
		"/api/v1/search", "/api/v1/health", "/api/v1/auth/login",
		"/api/v1/cart", "/api/v1/checkout",
	}
	methods := []string{"GET", "POST", "PUT", "DELETE"}
	statusCodes := []int{200, 200, 200, 200, 201, 204, 400, 401, 404, 500}

	for i := 0; i < 500; i++ {
		ts := baseTime.Add(time.Duration(i) * 5 * time.Second)
		code := statusCodes[rng.Intn(len(statusCodes))]
		enc.Encode(map[string]any{
			"timestamp":   ts.Format(time.RFC3339),
			"endpoint":    endpoints[rng.Intn(len(endpoints))],
			"method":      methods[rng.Intn(len(methods))],
			"status_code": code,
			"latency_ms":  rng.Float64()*500 + 1,
			"request_id":  fmt.Sprintf("req-%08x", rng.Int63()),
			"user_agent":  "test-client/1.0",
			"region":      []string{"us-east", "us-west", "eu-west"}[rng.Intn(3)],
		})
	}
}

func genOrdersCSV(rng *rand.Rand, baseTime time.Time) {
	f, _ := os.Create("testdata/orders.csv")
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()

	statuses := []string{"pending", "confirmed", "shipped", "delivered", "cancelled"}
	regions := []string{"us-east", "us-west", "eu-west"}

	w.Write([]string{"order_id", "customer_id", "product", "quantity", "price", "status", "region", "created_at"})
	for i := 1; i <= 100; i++ {
		ts := baseTime.Add(time.Duration(i) * time.Minute)
		qty := rng.Intn(10) + 1
		price := float64(rng.Intn(9900)+100) / 100.0
		w.Write([]string{
			fmt.Sprintf("CSV-%04d", i),
			fmt.Sprintf("CUST-%04d", rng.Intn(50)+1),
			[]string{"widget-a", "gadget-x"}[rng.Intn(2)],
			fmt.Sprintf("%d", qty),
			fmt.Sprintf("%.2f", price),
			statuses[rng.Intn(len(statuses))],
			regions[rng.Intn(len(regions))],
			ts.Format(time.RFC3339),
		})
	}
}

func genDebeziumEnvelope(baseTime time.Time) {
	f, _ := os.Create("testdata/debezium_envelope.json")
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	enc.Encode(map[string]any{
		"schema": nil,
		"payload": map[string]any{
			"before": nil,
			"after": map[string]any{
				"order_id":    "ORD-000001",
				"customer_id": "CUST-0042",
				"product":     "widget-a",
				"quantity":    3,
				"price":       29.99,
				"total":       89.97,
				"status":      "pending",
				"region":      "us-east",
				"created_at":  baseTime.Format(time.RFC3339),
				"updated_at":  baseTime.Format(time.RFC3339),
			},
			"source": map[string]any{
				"version":   "2.5.0",
				"connector": "postgresql",
				"name":      "dbspa-test",
				"ts_ms":     baseTime.UnixMilli(),
				"db":        "testdb",
				"schema":    "public",
				"table":     "orders",
				"txId":      1001,
				"lsn":       50100,
			},
			"op":    "c",
			"ts_ms": baseTime.UnixMilli(),
		},
	})
}

func genMalformed() {
	f, _ := os.Create("testdata/malformed.ndjson")
	defer f.Close()

	lines := []string{
		`{"valid":true,"name":"alice","value":42}`,
		`{"valid":true,"name":"bob","value":17}`,
		`this is not json at all`,
		`{"valid":true,"name":"charlie","value":99}`,
		`{"unclosed": "brace"`,
		``,
		`{"valid":true,"name":"diana","value":55}`,
		`[1, 2, 3]`,
		`{"valid":true,"name":"eve","value":33}`,
		`null`,
		`{"valid":true,"name":"frank","value":78}`,
		`{"nested": {"deeply": {"broken":}}}`,
		`{"valid":true,"name":"grace","value":12}`,
		`{"valid":true,"name":"heidi","value":64}`,
		`42`,
		`{"valid":true,"name":"ivan","value":91}`,
		`true`,
		`{"valid":true,"name":"judy","value":45}`,
		`{"missing_value":}`,
		`{"valid":true,"name":"karl","value":28}`,
	}
	for _, line := range lines {
		fmt.Fprintln(f, line)
	}
}
