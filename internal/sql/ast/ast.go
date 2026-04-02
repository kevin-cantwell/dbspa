// Package ast defines the abstract syntax tree for FoldDB's SQL dialect.
package ast

// Node is the interface implemented by all AST nodes.
type Node interface {
	nodeTag()
}

// Expr is the interface implemented by all expression nodes.
type Expr interface {
	Node
	exprTag()
}

// ExecSource represents a shell command whose stdout is used as a record source.
type ExecSource struct {
	Command string // the shell command to execute
}

func (*ExecSource) nodeTag() {}

// SubquerySource represents a subquery used as a source in FROM or JOIN.
// The inner query is executed to completion (materialized) before the outer
// query begins processing. Alias is mandatory — parse error if omitted.
type SubquerySource struct {
	Query *SelectStatement
	Alias string
}

func (*SubquerySource) nodeTag() {}

// SelectStatement represents a parsed SELECT query.
type SelectStatement struct {
	Distinct     bool
	Columns      []Column
	From         *TableSource    // nil means stdin (mutually exclusive with FromSubquery/FromExec)
	FromSubquery *SubquerySource // non-nil when FROM is a subquery
	FromExec     *ExecSource     // non-nil when FROM is EXEC(...)
	FromAlias    string          // optional alias for the FROM source
	Join         *JoinClause     // nil means no JOIN
	Seed         *SeedClause     // nil means no SEED FROM
	Where       Expr          // nil means no WHERE
	GroupBy     []Expr        // nil means non-accumulating
	Having      Expr          // nil means no HAVING
	Window      *WindowClause // nil means no WINDOW
	EventTime   *EventTimeClause
	Watermark   *WatermarkClause
	Emit        *EmitClause
	Deduplicate *DeduplicateClause
	OrderBy     []OrderByItem // nil means no ORDER BY
	Limit       *int          // nil means no LIMIT
}

// JoinClause represents a JOIN clause in a SELECT statement.
type JoinClause struct {
	Type      string           // "JOIN" or "LEFT JOIN"
	Source    *TableSource     // the file/URI to join against (mutually exclusive with Subquery/Exec)
	Subquery  *SubquerySource  // non-nil when JOIN source is a subquery
	Exec      *ExecSource      // non-nil when JOIN source is EXEC(...)
	Alias     string           // optional alias
	Condition Expr             // the ON expression
	Within    *string          // optional: interval bound duration string for stream-stream joins
}

func (*JoinClause) nodeTag() {}

// SeedClause represents a SEED FROM clause that bootstraps accumulators from a file.
type SeedClause struct {
	Source *TableSource // file path + optional format
	Exec   *ExecSource  // non-nil when SEED FROM is EXEC(...)
}

func (*SeedClause) nodeTag() {}

func (*SelectStatement) nodeTag() {}

// Column represents a single column in the SELECT list.
type Column struct {
	Expr  Expr
	Alias string // empty if no alias
}

// TableSource represents a FROM clause source.
type TableSource struct {
	URI           string            // e.g., 'kafka://...' or 'stdin://'
	Format        string            // e.g., "DEBEZIUM", "CSV", empty for default
	FormatOptions map[string]string // e.g., delimiter=',', header=true, quote='"'
}

// WindowClause represents a WINDOW specification.
type WindowClause struct {
	Type     string // "TUMBLING", "SLIDING", "SESSION"
	Size     string // duration literal, e.g., "1 minute"
	SlideBy  string // only for SLIDING, e.g., "5 minutes"
}

func (*WindowClause) nodeTag() {}

// EventTimeClause represents EVENT TIME BY.
type EventTimeClause struct {
	Expr Expr
}

func (*EventTimeClause) nodeTag() {}

// WatermarkClause represents a WATERMARK duration.
type WatermarkClause struct {
	Duration string
}

func (*WatermarkClause) nodeTag() {}

// EmitClause represents EMIT FINAL or EMIT EARLY.
type EmitClause struct {
	Type     string // "FINAL" or "EARLY"
	Interval string // only for EARLY, e.g., "10 seconds"
}

func (*EmitClause) nodeTag() {}

// DeduplicateClause represents DEDUPLICATE BY expr WITHIN duration.
type DeduplicateClause struct {
	Key      Expr
	Within   string // duration
	Capacity *int   // optional
}

func (*DeduplicateClause) nodeTag() {}

// OrderByItem represents a single ORDER BY expression.
type OrderByItem struct {
	Expr Expr
	Desc bool // true for DESC, false for ASC (default)
}

// --- Expression Nodes ---

// StarExpr represents * in a SELECT list.
type StarExpr struct{}

func (*StarExpr) nodeTag() {}
func (*StarExpr) exprTag() {}

// ColumnRef represents a reference to a column by name.
type ColumnRef struct {
	Name string
}

func (*ColumnRef) nodeTag() {}
func (*ColumnRef) exprTag() {}

// QualifiedRef represents a qualified column reference like alias.column.
type QualifiedRef struct {
	Qualifier     string // the table alias (e.g., "e" in "e.user_id")
	Name          string // the column name (e.g., "user_id")
	qualifiedName string // cached "Qualifier.Name" to avoid per-call allocation
}

func (*QualifiedRef) nodeTag() {}
func (*QualifiedRef) exprTag() {}

// QualifiedName returns "Qualifier.Name", caching the result to avoid
// repeated string concatenation in hot paths.
func (q *QualifiedRef) QualifiedName() string {
	if q.qualifiedName == "" {
		q.qualifiedName = q.Qualifier + "." + q.Name
	}
	return q.qualifiedName
}

// NumberLiteral represents an integer or float literal.
type NumberLiteral struct {
	Value   string // raw string representation
	IsFloat bool
}

func (*NumberLiteral) nodeTag() {}
func (*NumberLiteral) exprTag() {}

// StringLiteral represents a single-quoted string.
type StringLiteral struct {
	Value string
}

func (*StringLiteral) nodeTag() {}
func (*StringLiteral) exprTag() {}

// BoolLiteral represents TRUE or FALSE.
type BoolLiteral struct {
	Value bool
}

func (*BoolLiteral) nodeTag() {}
func (*BoolLiteral) exprTag() {}

// NullLiteral represents a NULL literal.
type NullLiteral struct{}

func (*NullLiteral) nodeTag() {}
func (*NullLiteral) exprTag() {}

// BinaryExpr represents a binary operation (e.g., a + b, a AND b).
type BinaryExpr struct {
	Left  Expr
	Op    string // "+", "-", "*", "/", "%", "=", "!=", "<", ">", "<=", ">=", "AND", "OR", "||", "LIKE", "ILIKE"
	Right Expr
}

func (*BinaryExpr) nodeTag() {}
func (*BinaryExpr) exprTag() {}

// UnaryExpr represents a unary operation (e.g., -x, NOT x).
type UnaryExpr struct {
	Op   string // "-", "+", "NOT"
	Expr Expr
}

func (*UnaryExpr) nodeTag() {}
func (*UnaryExpr) exprTag() {}

// FunctionCall represents a function invocation.
type FunctionCall struct {
	Name     string // e.g., "COUNT", "SUM", "COALESCE"
	Args     []Expr
	Distinct bool // for COUNT(DISTINCT x)
}

func (*FunctionCall) nodeTag() {}
func (*FunctionCall) exprTag() {}

// CastExpr represents CAST(expr AS type) or expr::type.
type CastExpr struct {
	Expr     Expr
	TypeName string // e.g., "INT", "FLOAT", "TEXT", "BOOLEAN", "TIMESTAMP"
}

func (*CastExpr) nodeTag() {}
func (*CastExpr) exprTag() {}

// JsonAccessExpr represents col->'key' or col->>'key'.
type JsonAccessExpr struct {
	Left    Expr
	Key     Expr   // StringLiteral or NumberLiteral for array index
	AsText  bool   // true for ->>, false for ->
}

func (*JsonAccessExpr) nodeTag() {}
func (*JsonAccessExpr) exprTag() {}

// BetweenExpr represents expr BETWEEN low AND high.
type BetweenExpr struct {
	Expr Expr
	Low  Expr
	High Expr
	Not  bool // true for NOT BETWEEN
}

func (*BetweenExpr) nodeTag() {}
func (*BetweenExpr) exprTag() {}

// InExpr represents expr IN (val1, val2, ...).
type InExpr struct {
	Expr   Expr
	Values []Expr
	Not    bool // true for NOT IN
}

func (*InExpr) nodeTag() {}
func (*InExpr) exprTag() {}

// IsExpr represents expr IS NULL, expr IS NOT NULL, etc.
type IsExpr struct {
	Expr Expr
	Not  bool   // true for IS NOT
	What string // "NULL", "TRUE", "FALSE", "DISTINCT FROM"
	From Expr   // only for IS [NOT] DISTINCT FROM
}

func (*IsExpr) nodeTag() {}
func (*IsExpr) exprTag() {}

// CaseExpr represents a CASE WHEN ... THEN ... ELSE ... END expression.
type CaseExpr struct {
	Whens []CaseWhen
	Else  Expr // nil if no ELSE
}

// CaseWhen represents a single WHEN ... THEN ... branch.
type CaseWhen struct {
	Condition Expr
	Result    Expr
}

func (*CaseExpr) nodeTag() {}
func (*CaseExpr) exprTag() {}

// IntervalLiteral represents INTERVAL '1 minute'.
type IntervalLiteral struct {
	Value string
}

func (*IntervalLiteral) nodeTag() {}
func (*IntervalLiteral) exprTag() {}
