package main

import (
	"fmt"
	"math/rand"
	"os"

	"github.com/parquet-go/parquet-go"
)

// OrderRow is the typed struct for Parquet schema.
type OrderRow struct {
	OrderID    int64   `parquet:"order_id"`
	CustomerID int64   `parquet:"customer_id"`
	Product    string  `parquet:"product"`
	Quantity   int64   `parquet:"quantity"`
	Price      float64 `parquet:"price"`
	Total      float64 `parquet:"total"`
	Status     string  `parquet:"status"`
	Region     string  `parquet:"region"`
	CreatedAt  string  `parquet:"created_at"`
}

// MetricRow is the typed struct for API metrics Parquet schema.
type MetricRow struct {
	Endpoint   string `parquet:"endpoint"`
	Method     string `parquet:"method"`
	StatusCode int64  `parquet:"status_code"`
	LatencyMS  int64  `parquet:"latency_ms"`
	Region     string `parquet:"region"`
	Timestamp  string `parquet:"timestamp"`
}

// ClickRow is the typed struct for clickstream Parquet schema.
type ClickRow struct {
	UserID     string `parquet:"user_id"`
	Page       string `parquet:"page"`
	Action     string `parquet:"action"`
	SessionID  string `parquet:"session_id"`
	DurationMS int64  `parquet:"duration_ms"`
	Timestamp  string `parquet:"timestamp"`
}

// writeParquet generates records and writes them to a Parquet file.
func writeParquet(dataset string, count int, rng *rand.Rand, output string) error {
	if output == "" {
		return fmt.Errorf("parquet format requires --output <file.parquet>")
	}

	f, err := os.Create(output)
	if err != nil {
		return fmt.Errorf("cannot create output file: %w", err)
	}
	defer f.Close()

	switch dataset {
	case "orders":
		return writeParquetOrders(f, count, rng)
	case "metrics":
		return writeParquetMetrics(f, count, rng)
	case "clickstream":
		return writeParquetClickstream(f, count, rng)
	case "orders-cdc":
		return fmt.Errorf("parquet format is not supported for CDC datasets (CDC is streaming-only)")
	default:
		return fmt.Errorf("unknown dataset: %s", dataset)
	}
}

func writeParquetOrders(f *os.File, count int, rng *rand.Rand) error {
	writer := parquet.NewGenericWriter[OrderRow](f)

	for i := 0; i < count; i++ {
		rec := genOrder(rng, i)
		row := OrderRow{
			OrderID:    int64(toInt(rec["order_id"])),
			CustomerID: int64(toInt(rec["customer_id"])),
			Product:    rec["product"].(string),
			Quantity:   int64(toInt(rec["quantity"])),
			Price:      toFloat64(rec["price"]),
			Total:      toFloat64(rec["total"]),
			Status:     rec["status"].(string),
			Region:     rec["region"].(string),
			CreatedAt:  rec["created_at"].(string),
		}
		if _, err := writer.Write([]OrderRow{row}); err != nil {
			return fmt.Errorf("parquet write error: %w", err)
		}
	}

	return writer.Close()
}

func writeParquetMetrics(f *os.File, count int, rng *rand.Rand) error {
	writer := parquet.NewGenericWriter[MetricRow](f)

	for i := 0; i < count; i++ {
		rec := genMetric(rng, i)
		row := MetricRow{
			Endpoint:   rec["endpoint"].(string),
			Method:     rec["method"].(string),
			StatusCode: int64(toInt(rec["status_code"])),
			LatencyMS:  int64(toInt(rec["latency_ms"])),
			Region:     rec["region"].(string),
			Timestamp:  rec["timestamp"].(string),
		}
		if _, err := writer.Write([]MetricRow{row}); err != nil {
			return fmt.Errorf("parquet write error: %w", err)
		}
	}

	return writer.Close()
}

func writeParquetClickstream(f *os.File, count int, rng *rand.Rand) error {
	writer := parquet.NewGenericWriter[ClickRow](f)

	for i := 0; i < count; i++ {
		rec := genClick(rng, i)
		row := ClickRow{
			UserID:     rec["user_id"].(string),
			Page:       rec["page"].(string),
			Action:     rec["action"].(string),
			SessionID:  rec["session_id"].(string),
			DurationMS: int64(toInt(rec["duration_ms"])),
			Timestamp:  rec["timestamp"].(string),
		}
		if _, err := writer.Write([]ClickRow{row}); err != nil {
			return fmt.Errorf("parquet write error: %w", err)
		}
	}

	return writer.Close()
}
