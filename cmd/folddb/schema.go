package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/kevin-cantwell/folddb/internal/format"
	"github.com/kevin-cantwell/folddb/internal/source"
)

func runSchema(args []string) error {
	// Determine source: URI arg or stdin
	var sourceURI string
	var formatStr string
	for i, arg := range args {
		if strings.HasPrefix(arg, "kafka://") || strings.HasPrefix(arg, "stdin://") {
			sourceURI = arg
		}
		if strings.ToUpper(arg) == "FORMAT" && i+1 < len(args) {
			formatStr = args[i+1]
		}
	}

	if sourceURI != "" && strings.HasPrefix(sourceURI, "kafka://") {
		return runSchemaKafka(sourceURI, formatStr)
	}

	// Default: read from stdin
	return runSchemaStdin(formatStr)
}

func runSchemaStdin(formatStr string) error {
	dec, err := format.NewDecoder(formatStr)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)

	// Collect schema from first 10 lines
	schema := make(map[string]*columnInfo)
	count := 0
	maxSamples := 10

	for scanner.Scan() && count < maxSamples {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		recs, err := format.DecodeAll(dec, line)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: %v\n", err)
			continue
		}
		for _, rec := range recs {
			for name, val := range rec.Columns {
				ci, ok := schema[name]
				if !ok {
					ci = &columnInfo{}
					schema[name] = ci
				}
				ci.addSample(val)
			}
		}
		count++
	}

	if count == 0 {
		fmt.Println("No records sampled (empty input).")
		return nil
	}

	printSchemaTable(schema)
	return nil
}

func runSchemaKafka(uri string, formatStr string) error {
	cfg, err := source.ParseKafkaURI(uri)
	if err != nil {
		return err
	}

	// Override offset to earliest for schema sampling
	cfg.Offset = "earliest"

	dec, err := format.NewDecoder(formatStr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kafkaSrc := source.NewKafka(ctx, cfg)
	ch := kafkaSrc.Read()

	schema := make(map[string]*columnInfo)
	count := 0
	maxSamples := 100

	for kr := range ch {
		if count >= maxSamples {
			cancel()
			break
		}
		if kr.Value == nil {
			continue
		}
		recs, err := format.DecodeAll(dec, kr.Value)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: %v\n", err)
			continue
		}
		recs = format.InjectKafkaVirtuals(recs, kr)
		for _, rec := range recs {
			for name, val := range rec.Columns {
				ci, ok := schema[name]
				if !ok {
					ci = &columnInfo{}
					schema[name] = ci
				}
				ci.addSample(val)
			}
		}
		count++
	}

	if count == 0 {
		fmt.Println("No records sampled from topic.")
		return nil
	}

	fmt.Printf("Source:    %s\n", uri)
	if formatStr != "" {
		fmt.Printf("Format:    %s\n", strings.ToUpper(formatStr))
	} else {
		fmt.Printf("Format:    NDJSON (default)\n")
	}
	fmt.Printf("Sample:    %d records\n\n", count)
	printSchemaTable(schema)
	return nil
}

type columnInfo struct {
	types   map[string]int
	samples []string
}

func (ci *columnInfo) addSample(v interface{ Type() string; String() string }) {
	if ci.types == nil {
		ci.types = make(map[string]int)
	}
	ci.types[v.Type()]++
	if len(ci.samples) < 3 {
		s := v.String()
		if len(s) > 40 {
			s = s[:37] + "..."
		}
		ci.samples = append(ci.samples, s)
	}
}

func (ci *columnInfo) inferredType() string {
	if len(ci.types) == 0 {
		return "UNKNOWN"
	}
	// Return the most common non-NULL type
	best := ""
	bestCount := 0
	for t, c := range ci.types {
		if t == "NULL" {
			continue
		}
		if c > bestCount {
			best = t
			bestCount = c
		}
	}
	if best == "" {
		return "NULL"
	}
	if ci.types["NULL"] > 0 {
		return best + " (nullable)"
	}
	return best
}

func printSchemaTable(schema map[string]*columnInfo) {
	// Sort column names: virtual columns (_prefix) last
	var cols []string
	for name := range schema {
		cols = append(cols, name)
	}
	sort.Slice(cols, func(i, j int) bool {
		iVirt := strings.HasPrefix(cols[i], "_")
		jVirt := strings.HasPrefix(cols[j], "_")
		if iVirt != jVirt {
			return !iVirt // non-virtual first
		}
		return cols[i] < cols[j]
	})

	fmt.Println("Inferred schema:")
	// Find max column name width
	maxName := 0
	maxType := 0
	for _, name := range cols {
		if len(name) > maxName {
			maxName = len(name)
		}
		t := schema[name].inferredType()
		if len(t) > maxType {
			maxType = len(t)
		}
	}

	for _, name := range cols {
		ci := schema[name]
		t := ci.inferredType()
		samples := ""
		if len(ci.samples) > 0 {
			samples = strings.Join(ci.samples, ", ")
		}
		fmt.Printf("  %-*s  %-*s  %s\n", maxName, name, maxType, t, samples)
	}
}

// Used by schema subcommand to read from stdin when piped
var _ io.Reader = os.Stdin
