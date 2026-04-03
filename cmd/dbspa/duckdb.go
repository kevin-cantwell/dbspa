package main

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/kevin-cantwell/dbspa/internal/engine"
	"github.com/kevin-cantwell/dbspa/internal/sink"
	"github.com/kevin-cantwell/dbspa/internal/source"
	"github.com/kevin-cantwell/dbspa/internal/sql/ast"
)

// duckDBInstance is the shared DuckDB connection, created lazily and reused.
var duckDBInstance *source.DuckDBSource

// getDuckDB returns the shared DuckDB source, creating it on first use.
func getDuckDB() (*source.DuckDBSource, error) {
	if duckDBInstance != nil {
		return duckDBInstance, nil
	}
	src, err := source.NewDuckDBSource()
	if err != nil {
		return nil, err
	}
	duckDBInstance = src
	return duckDBInstance, nil
}

// closeDuckDB closes the shared DuckDB connection if it was opened.
func closeDuckDB() {
	if duckDBInstance != nil {
		duckDBInstance.Close()
		duckDBInstance = nil
	}
}

// isFileSource returns true if the given URI looks like a file path or
// database URI that DuckDB can handle (not kafka://, not stdin://).
func isFileSource(uri string) bool {
	if uri == "" {
		return false
	}
	if strings.HasPrefix(uri, "kafka://") || strings.HasPrefix(uri, "stdin://") {
		return false
	}
	return source.IsDuckDBSupported(uri)
}

// translateDBSPAQueryToDuckDB rewrites a DBSPA SELECT statement so that
// the FROM clause uses DuckDB scanner functions. This is used for
// non-accumulating file queries where DuckDB can handle the entire query.
func translateDBSPAQueryToDuckDB(stmt *ast.SelectStatement) (string, error) {
	fromURI := stmt.From.URI
	duckExpr, err := source.TranslateToDuckDB(fromURI)
	if err != nil {
		return "", err
	}

	// Build SELECT clause
	var selectParts []string
	for _, col := range stmt.Columns {
		if _, ok := col.Expr.(*ast.StarExpr); ok {
			selectParts = append(selectParts, "*")
			continue
		}
		colStr := exprToDuckDBSQL(col.Expr)
		if col.Alias != "" {
			colStr += " AS " + quoteIdent(col.Alias)
		}
		selectParts = append(selectParts, colStr)
	}
	if stmt.Distinct {
		selectParts[0] = "DISTINCT " + selectParts[0]
	}

	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectParts, ", "), duckExpr)

	// Alias for the FROM source
	if stmt.FromAlias != "" {
		query += " " + quoteIdent(stmt.FromAlias)
	}

	// WHERE
	if stmt.Where != nil {
		query += " WHERE " + exprToDuckDBSQL(stmt.Where)
	}

	// GROUP BY
	if stmt.GroupBy != nil && len(stmt.GroupBy) > 0 {
		var groupParts []string
		for _, g := range stmt.GroupBy {
			groupParts = append(groupParts, exprToDuckDBSQL(g))
		}
		query += " GROUP BY " + strings.Join(groupParts, ", ")
	}

	// HAVING
	if stmt.Having != nil {
		query += " HAVING " + exprToDuckDBSQL(stmt.Having)
	}

	// ORDER BY
	if len(stmt.OrderBy) > 0 {
		var orderParts []string
		for _, ob := range stmt.OrderBy {
			part := exprToDuckDBSQL(ob.Expr)
			if ob.Desc {
				part += " DESC"
			}
			orderParts = append(orderParts, part)
		}
		query += " ORDER BY " + strings.Join(orderParts, ", ")
	}

	// LIMIT
	if stmt.Limit != nil {
		query += fmt.Sprintf(" LIMIT %d", *stmt.Limit)
	}

	return query, nil
}

// exprToDuckDBSQL converts a DBSPA AST expression to DuckDB-compatible SQL.
func exprToDuckDBSQL(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.StarExpr:
		return "*"
	case *ast.ColumnRef:
		return quoteIdent(v.Name)
	case *ast.QualifiedRef:
		return quoteIdent(v.Qualifier) + "." + quoteIdent(v.Name)
	case *ast.NumberLiteral:
		return v.Value
	case *ast.StringLiteral:
		return "'" + strings.ReplaceAll(v.Value, "'", "''") + "'"
	case *ast.BoolLiteral:
		if v.Value {
			return "TRUE"
		}
		return "FALSE"
	case *ast.NullLiteral:
		return "NULL"
	case *ast.BinaryExpr:
		return exprToDuckDBSQL(v.Left) + " " + v.Op + " " + exprToDuckDBSQL(v.Right)
	case *ast.UnaryExpr:
		return v.Op + " " + exprToDuckDBSQL(v.Expr)
	case *ast.FunctionCall:
		var args []string
		for _, a := range v.Args {
			args = append(args, exprToDuckDBSQL(a))
		}
		return v.Name + "(" + strings.Join(args, ", ") + ")"
	case *ast.CastExpr:
		return "CAST(" + exprToDuckDBSQL(v.Expr) + " AS " + v.TypeName + ")"
	case *ast.JsonAccessExpr:
		// DuckDB uses -> for JSON access, same as DBSPA
		op := "->"
		if v.AsText {
			op = "->>"
		}
		return exprToDuckDBSQL(v.Left) + op + exprToDuckDBSQL(v.Key)
	default:
		return fmt.Sprintf("%v", e)
	}
}

// quoteIdent quotes an identifier for DuckDB SQL if it contains special chars
// or is a reserved word. Simple identifiers pass through unquoted.
var simpleIdentRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func quoteIdent(name string) string {
	if simpleIdentRe.MatchString(name) {
		return name
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// runDuckDBFileQuery executes a file-source query entirely through DuckDB.
// Used for non-accumulating queries on file sources where DuckDB can handle
// everything (predicate pushdown, column pruning, ORDER BY, LIMIT).
func runDuckDBFileQuery(ctx context.Context, stmt *ast.SelectStatement) ([]engine.Record, error) {
	query, err := translateDBSPAQueryToDuckDB(stmt)
	if err != nil {
		return nil, err
	}
	// debug: fmt.Fprintf(os.Stderr, "DuckDB query: %s\n", query)

	duckSrc, err := getDuckDB()
	if err != nil {
		return nil, err
	}
	return duckSrc.Query(query)
}

// runDuckDBFileQueryToChannel executes a file-source query through DuckDB
// and streams results to a channel.
func runDuckDBFileQueryToChannel(ctx context.Context, stmt *ast.SelectStatement, ch chan<- engine.Record) error {
	query, err := translateDBSPAQueryToDuckDB(stmt)
	if err != nil {
		return err
	}
	// debug: fmt.Fprintf(os.Stderr, "DuckDB query: %s\n", query)

	duckSrc, err := getDuckDB()
	if err != nil {
		return err
	}
	return duckSrc.QueryToChannel(ctx, query, ch)
}

// runDuckDBNonAccumulating executes a file query entirely through DuckDB and
// outputs results via the standard DBSPA sink. DuckDB handles the full query
// including WHERE, ORDER BY, and LIMIT with predicate pushdown.
func runDuckDBNonAccumulating(ctx context.Context, stmt *ast.SelectStatement) error {
	query, err := translateDBSPAQueryToDuckDB(stmt)
	if err != nil {
		return fmt.Errorf("duckdb query translation error: %w", err)
	}
	// debug: fmt.Fprintf(os.Stderr, "DuckDB query: %s\n", query)

	duckSrc, err := getDuckDB()
	if err != nil {
		return err
	}

	ch := make(chan engine.Record, 256)
	go func() {
		defer close(ch)
		if err := duckSrc.QueryToChannel(ctx, query, ch); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: DuckDB query error: %v\n", err)
		}
	}()

	snk := &sink.JSONSink{Writer: os.Stdout}

	for {
		select {
		case rec, ok := <-ch:
			if !ok {
				return snk.Close()
			}
			if err := snk.Write(rec); err != nil {
				return fmt.Errorf("output error: %w", err)
			}
		case <-ctx.Done():
			return snk.Close()
		}
	}
}

// duckDBScanToChannel executes a simple SELECT * FROM <file> via DuckDB
// and streams results to a channel. Used when DuckDB just provides the source
// data and DBSPA handles aggregation.
func duckDBScanToChannel(ctx context.Context, uri string, ch chan<- engine.Record) error {
	duckExpr, err := source.TranslateToDuckDB(uri)
	if err != nil {
		return err
	}
	query := fmt.Sprintf("SELECT * FROM %s", duckExpr)
	fmt.Fprintf(os.Stderr, "DuckDB scan: %s\n", query)

	duckSrc, err := getDuckDB()
	if err != nil {
		return err
	}
	return duckSrc.QueryToChannel(ctx, query, ch)
}
