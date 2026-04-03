# SQL Parser

DBSPA uses a hand-written recursive descent parser. No parser generator is involved.

## Components

The parser lives in `internal/sql/` and consists of three packages:

| Package | Role |
|---|---|
| `internal/sql/lexer/` | Tokenizer — converts SQL text into a token stream |
| `internal/sql/parser/` | Recursive descent parser — converts tokens into an AST |
| `internal/sql/ast/` | AST node types — the structured representation of a query |

## Lexer

The lexer (`lexer.New(input)`) produces tokens one at a time via `Next()` and `Peek()`. Token types include:

- Keywords: `SELECT`, `FROM`, `WHERE`, `GROUP`, `BY`, `HAVING`, `ORDER`, `LIMIT`, `WINDOW`, `EMIT`, `EVENT`, `TIME`, `WATERMARK`, `DEDUPLICATE`, `FORMAT`, `SEED`, `JOIN`, `LEFT`, `ON`, `DISTINCT`, `AS`, `AND`, `OR`, `NOT`, `IN`, `BETWEEN`, `LIKE`, `ILIKE`, `IS`, `NULL`, `TRUE`, `FALSE`, `CASE`, `WHEN`, `THEN`, `ELSE`, `END`, `CAST`, `ASC`, `DESC`, `WITHIN`, `CAPACITY`, `TUMBLING`, `SLIDING`, `SESSION`, `EARLY`, `FINAL`
- Operators: `+`, `-`, `*`, `/`, `%`, `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`, `||`, `->`, `->>`, `::`
- Literals: integers, floats, strings (single-quoted), identifiers
- Quoted identifiers: double-quoted (`"last"`) are always treated as identifiers, never keywords

```sql
-- "last" is a column name, not the LAST() function
SELECT "last", "first" WHERE "count" > 10
```

## Parser

The parser (`parser.New(input).Parse()`) returns an `*ast.SelectStatement`. Clause ordering is rigid and must follow:

```
SELECT → FROM → JOIN → WHERE → GROUP BY → HAVING → WINDOW → EVENT TIME BY → WATERMARK → EMIT → DEDUPLICATE BY → ORDER BY → LIMIT
```

Out-of-order clauses produce a clear error:

```
Error: WINDOW clause must appear after HAVING, found at position N
```

## AST

The `ast.SelectStatement` holds all parsed clauses:

```go
type SelectStatement struct {
    Distinct    bool
    Columns     []SelectColumn   // SELECT list
    From        *TableSource     // FROM clause (URI, format, options)
    Join        *JoinClause      // JOIN clause
    Where       Expr             // WHERE condition
    GroupBy     []Expr           // GROUP BY expressions
    Having      Expr             // HAVING condition
    Window      *WindowClause    // WINDOW spec
    EventTime   Expr             // EVENT TIME BY expression
    Watermark   *Duration        // WATERMARK duration
    Emit        *EmitClause      // EMIT FINAL / EMIT EARLY
    Dedup       *DedupClause     // DEDUPLICATE BY
    OrderBy     []OrderByItem    // ORDER BY
    Limit       *int             // LIMIT
    Seed        *SeedClause      // SEED FROM
}
```

Expressions form a tree of `ast.Expr` nodes: `BinaryExpr`, `UnaryExpr`, `FuncCall`, `ColumnRef`, `QualifiedRef`, `Literal`, `JsonAccess`, `CastExpr`, `CaseExpr`, `InExpr`, `BetweenExpr`, etc.

## Error messages

The parser produces precise error messages with source position and suggestions:

```
Error: unexpected token 'GRUP' at position 84

  SELECT region, COUNT(*) FROM 'kafka://...' GRUP BY region
                                              ^^^^
  Did you mean: GROUP BY?
```

```
Error: column 'regin' not found in source schema

  Available columns: region, status, total, _op, _before, _after, ...
  Did you mean: region?
```

## GROUP BY semantics

GROUP BY accepts expressions or integer ordinals (1-indexed, referencing the SELECT list position). Column aliases from SELECT are **not** valid in GROUP BY — use the original expression or an ordinal:

```sql
-- Valid:
GROUP BY _after.region
GROUP BY 1, 2

-- Invalid (alias not allowed in GROUP BY):
SELECT _after.region AS region ... GROUP BY region
-- Use instead:
SELECT _after.region AS region ... GROUP BY 1
```

ORDER BY accepts expressions, ordinals, or aliases.
