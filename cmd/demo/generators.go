package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"time"
)

// Generator writes NDJSON records to a writer until the done channel is closed.
type Generator interface {
	Run(w io.Writer, done <-chan struct{})
}

// ECommerceGenerator simulates Debezium CDC events for an orders table.
type ECommerceGenerator struct {
	rng *rand.Rand
}

func (g *ECommerceGenerator) Run(w io.Writer, done <-chan struct{}) {
	if g.rng == nil {
		g.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	regions := []string{"us-east", "us-west", "eu-west", "ap-south"}
	nextID := 1000

	activeOrders := make(map[int]*order)

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	enc := json.NewEncoder(w)

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			now := time.Now().UTC().Format(time.RFC3339)
			roll := g.rng.Float64()

			var event map[string]interface{}

			if roll < 0.5 || len(activeOrders) < 3 {
				// Create
				o := &order{
					OrderID:    nextID,
					CustomerID: g.rng.Intn(500) + 1,
					Status:     "pending",
					Region:     regions[g.rng.Intn(len(regions))],
					Amount:     float64(g.rng.Intn(50000)+500) / 100.0,
					UpdatedAt:  now,
				}
				nextID++
				activeOrders[o.OrderID] = o
				event = map[string]interface{}{
					"op":    "c",
					"after": map[string]interface{}{"order_id": o.OrderID, "customer_id": o.CustomerID, "status": o.Status, "region": o.Region, "amount": o.Amount, "updated_at": now},
				}
			} else if roll < 0.85 {
				// Update
				o := g.pickRandom(activeOrders)
				if o == nil {
					continue
				}
				before := map[string]interface{}{"order_id": o.OrderID, "customer_id": o.CustomerID, "status": o.Status, "region": o.Region, "amount": o.Amount, "updated_at": o.UpdatedAt}
				// Advance status
				switch o.Status {
				case "pending":
					o.Status = "shipped"
				case "shipped":
					o.Status = "delivered"
				case "delivered":
					o.Status = "delivered" // no change
				}
				o.UpdatedAt = now
				after := map[string]interface{}{"order_id": o.OrderID, "customer_id": o.CustomerID, "status": o.Status, "region": o.Region, "amount": o.Amount, "updated_at": now}
				event = map[string]interface{}{
					"op":     "u",
					"before": before,
					"after":  after,
				}
			} else {
				// Delete (cancellation)
				o := g.pickRandom(activeOrders)
				if o == nil {
					continue
				}
				delete(activeOrders, o.OrderID)
				event = map[string]interface{}{
					"op":     "d",
					"before": map[string]interface{}{"order_id": o.OrderID, "customer_id": o.CustomerID, "status": o.Status, "region": o.Region, "amount": o.Amount, "updated_at": o.UpdatedAt},
				}
			}

			if err := enc.Encode(event); err != nil {
				return
			}
		}
	}
}

func (g *ECommerceGenerator) pickRandom(m map[int]*order) *order {
	if len(m) == 0 {
		return nil
	}
	idx := g.rng.Intn(len(m))
	i := 0
	for _, v := range m {
		if i == idx {
			return v
		}
		i++
	}
	return nil
}

type order struct {
	OrderID    int     `json:"order_id"`
	CustomerID int     `json:"customer_id"`
	Status     string  `json:"status"`
	Region     string  `json:"region"`
	Amount     float64 `json:"amount"`
	UpdatedAt  string  `json:"updated_at"`
}

// APIMetricsGenerator simulates API request logs.
type APIMetricsGenerator struct {
	rng *rand.Rand
}

func (g *APIMetricsGenerator) Run(w io.Writer, done <-chan struct{}) {
	if g.rng == nil {
		g.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	endpoints := []string{"/api/users", "/api/orders", "/api/health", "/api/search"}
	methods := []string{"GET", "GET", "GET", "POST"} // weighted toward GET
	statusCodes := []int{200, 200, 200, 200, 200, 201, 400, 404, 500}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	enc := json.NewEncoder(w)

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			idx := g.rng.Intn(len(endpoints))
			code := statusCodes[g.rng.Intn(len(statusCodes))]

			// Higher latency for errors and search
			latency := g.rng.Intn(50) + 5
			if code >= 400 {
				latency = g.rng.Intn(200) + 100
			}
			if endpoints[idx] == "/api/search" {
				latency += g.rng.Intn(100) + 50
			}

			rec := map[string]interface{}{
				"endpoint":    endpoints[idx],
				"method":      methods[idx],
				"status_code": code,
				"latency_ms":  latency,
				"timestamp":   time.Now().UTC().Format(time.RFC3339),
			}

			if err := enc.Encode(rec); err != nil {
				return
			}
		}
	}
}

// ClickstreamGenerator simulates user click events.
type ClickstreamGenerator struct {
	rng *rand.Rand
}

func (g *ClickstreamGenerator) Run(w io.Writer, done <-chan struct{}) {
	if g.rng == nil {
		g.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	pages := []string{"/home", "/products", "/cart", "/checkout", "/account", "/search"}
	actions := []string{"view", "view", "click", "click", "scroll", "purchase"}

	ticker := time.NewTicker(125 * time.Millisecond)
	defer ticker.Stop()

	enc := json.NewEncoder(w)

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			rec := map[string]interface{}{
				"user_id":    fmt.Sprintf("user_%d", g.rng.Intn(100)+1),
				"page":       pages[g.rng.Intn(len(pages))],
				"action":     actions[g.rng.Intn(len(actions))],
				"session_id": fmt.Sprintf("sess_%d", g.rng.Intn(20)+1),
				"timestamp":  time.Now().UTC().Format(time.RFC3339),
			}

			if err := enc.Encode(rec); err != nil {
				return
			}
		}
	}
}

// NewGenerator returns a generator by name.
func NewGenerator(name string) Generator {
	switch name {
	case "ecommerce_orders":
		return &ECommerceGenerator{}
	case "api_metrics":
		return &APIMetricsGenerator{}
	case "clickstream":
		return &ClickstreamGenerator{}
	default:
		return nil
	}
}
