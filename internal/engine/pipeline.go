package engine

import (
	"github.com/kevin-cantwell/folddb/internal/sql/ast"
)

// Pipeline represents a streaming execution pipeline that reads records
// from a source channel, applies filter and projection, and writes to a sink.
type Pipeline struct {
	Columns []ast.Column
	Where   ast.Expr // may be nil
}

// Process reads records from in, applies filter and projection, and sends
// results to out. It closes out when in is exhausted.
func (p *Pipeline) Process(in <-chan Record, out chan<- Record) error {
	defer close(out)

	for rec := range in {
		// Apply WHERE filter
		pass, err := Filter(p.Where, rec)
		if err != nil {
			// Log and skip malformed records
			continue
		}
		if !pass {
			continue
		}

		// Apply projection
		projected, err := Project(p.Columns, rec)
		if err != nil {
			// Log and skip
			continue
		}

		out <- projected
	}
	return nil
}
