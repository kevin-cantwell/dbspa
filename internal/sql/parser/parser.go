// Package parser implements a recursive descent parser for FoldDB's SQL dialect.
package parser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/kevin-cantwell/folddb/internal/sql/ast"
	"github.com/kevin-cantwell/folddb/internal/sql/lexer"
)

// Parser parses a token stream into an AST.
type Parser struct {
	lex *lexer.Lexer
}

// New creates a new Parser for the given SQL input.
func New(input string) *Parser {
	return &Parser{lex: lexer.New(input)}
}

// Parse parses a SELECT statement and returns the AST.
func (p *Parser) Parse() (*ast.SelectStatement, error) {
	stmt, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	tok := p.lex.Peek()
	if tok.Type != lexer.TokenEOF {
		return nil, p.errorf(tok, "unexpected token %q after statement", tok.Literal)
	}
	return stmt, nil
}

func (p *Parser) parseSelect() (*ast.SelectStatement, error) {
	if err := p.expect(lexer.TokenSelect); err != nil {
		return nil, err
	}

	stmt := &ast.SelectStatement{}

	// DISTINCT
	if p.lex.Peek().Type == lexer.TokenDistinct {
		p.lex.Next()
		stmt.Distinct = true
	}

	// SELECT list
	cols, err := p.parseSelectList()
	if err != nil {
		return nil, err
	}
	stmt.Columns = cols

	// FROM (optional)
	if p.lex.Peek().Type == lexer.TokenFrom {
		p.lex.Next()
		src, alias, err := p.parseTableSourceWithAlias()
		if err != nil {
			return nil, err
		}
		stmt.From = src
		stmt.FromAlias = alias
	}

	// FORMAT after FROM source+alias but before JOIN (e.g., FROM stdin e FORMAT DEBEZIUM JOIN ...)
	if stmt.From != nil && stmt.From.Format == "" && p.lex.Peek().Type == lexer.TokenFormat {
		if err := p.parseFormatInto(stmt.From); err != nil {
			return nil, err
		}
	}

	// JOIN / LEFT JOIN (optional)
	if p.lex.Peek().Type == lexer.TokenJoin || p.lex.Peek().Type == lexer.TokenLeft {
		join, err := p.parseJoinClause()
		if err != nil {
			return nil, err
		}
		stmt.Join = join
	}

	// FORMAT after JOIN (e.g., FROM stdin e JOIN ... FORMAT DEBEZIUM WHERE ...)
	if stmt.From != nil && stmt.From.Format == "" && p.lex.Peek().Type == lexer.TokenFormat {
		if err := p.parseFormatInto(stmt.From); err != nil {
			return nil, err
		}
	}

	// SEED FROM (optional)
	if p.lex.Peek().Type == lexer.TokenSeed {
		seed, err := p.parseSeedClause()
		if err != nil {
			return nil, err
		}
		stmt.Seed = seed
	}

	// WHERE
	if p.lex.Peek().Type == lexer.TokenWhere {
		p.lex.Next()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	// GROUP BY
	if p.lex.Peek().Type == lexer.TokenGroup {
		p.lex.Next()
		if err := p.expect(lexer.TokenBy); err != nil {
			return nil, err
		}
		exprs, err := p.parseExprList()
		if err != nil {
			return nil, err
		}
		stmt.GroupBy = exprs
	}

	// HAVING
	if p.lex.Peek().Type == lexer.TokenHaving {
		tok := p.lex.Peek()
		p.lex.Next()
		if stmt.GroupBy == nil {
			return nil, p.errorf(tok, "HAVING requires GROUP BY")
		}
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Having = expr
	}

	// WINDOW
	if p.lex.Peek().Type == lexer.TokenWindow {
		p.lex.Next()
		w, err := p.parseWindowClause()
		if err != nil {
			return nil, err
		}
		stmt.Window = w
	}

	// EVENT TIME BY
	if p.lex.Peek().Type == lexer.TokenEvent {
		p.lex.Next()
		if err := p.expect(lexer.TokenTime); err != nil {
			return nil, err
		}
		if err := p.expect(lexer.TokenBy); err != nil {
			return nil, err
		}
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.EventTime = &ast.EventTimeClause{Expr: expr}
	}

	// WATERMARK
	if p.lex.Peek().Type == lexer.TokenWatermark {
		p.lex.Next()
		tok := p.lex.Next()
		if tok.Type != lexer.TokenString {
			return nil, p.errorf(tok, "expected duration string after WATERMARK, got %q", tok.Literal)
		}
		stmt.Watermark = &ast.WatermarkClause{Duration: tok.Literal}
	}

	// EMIT
	if p.lex.Peek().Type == lexer.TokenEmit {
		p.lex.Next()
		e, err := p.parseEmitClause()
		if err != nil {
			return nil, err
		}
		stmt.Emit = e
	}

	// DEDUPLICATE BY
	if p.lex.Peek().Type == lexer.TokenDeduplicate {
		p.lex.Next()
		if err := p.expect(lexer.TokenBy); err != nil {
			return nil, err
		}
		d, err := p.parseDeduplicateClause()
		if err != nil {
			return nil, err
		}
		stmt.Deduplicate = d
	}

	// ORDER BY
	if p.lex.Peek().Type == lexer.TokenOrder {
		tok := p.lex.Peek()
		p.lex.Next()
		if err := p.expect(lexer.TokenBy); err != nil {
			return nil, err
		}
		// Reject ORDER BY on non-accumulating queries (no GROUP BY)
		if stmt.GroupBy == nil {
			return nil, p.errorf(tok, "ORDER BY is not supported on non-accumulating streaming queries (no GROUP BY). Records are emitted in arrival order.")
		}
		items, err := p.parseOrderByList()
		if err != nil {
			return nil, err
		}
		stmt.OrderBy = items
	}

	// LIMIT
	if p.lex.Peek().Type == lexer.TokenLimit {
		p.lex.Next()
		tok := p.lex.Next()
		if tok.Type != lexer.TokenInteger {
			return nil, p.errorf(tok, "expected integer after LIMIT, got %q", tok.Literal)
		}
		n, err := strconv.Atoi(tok.Literal)
		if err != nil {
			return nil, p.errorf(tok, "invalid LIMIT value: %s", tok.Literal)
		}
		stmt.Limit = &n
	}

	// Standalone FORMAT (trailing, after all other clauses)
	// This allows: SELECT ... WHERE ... FORMAT CSV
	// Also: SELECT ... FROM stdin JOIN ... WHERE ... FORMAT DEBEZIUM
	if p.lex.Peek().Type == lexer.TokenFormat {
		if stmt.From == nil {
			src, err := p.parseStandaloneFormat()
			if err != nil {
				return nil, err
			}
			stmt.From = src
		} else if stmt.From.Format == "" {
			if err := p.parseFormatInto(stmt.From); err != nil {
				return nil, err
			}
		}
	}

	return stmt, nil
}

func (p *Parser) parseStandaloneFormat() (*ast.TableSource, error) {
	p.lex.Next() // consume FORMAT
	fmtTok := p.lex.Next()
	src := &ast.TableSource{Format: strings.ToUpper(fmtTok.Literal)}
	// Parse optional format options: FORMAT CSV(key=value, ...)
	if p.lex.Peek().Type == lexer.TokenLParen {
		p.lex.Next()
		opts := make(map[string]string)
		for p.lex.Peek().Type != lexer.TokenRParen && p.lex.Peek().Type != lexer.TokenEOF {
			keyTok := p.lex.Next()
			if keyTok.Type != lexer.TokenIdent && !lexer.IsKeyword(keyTok.Type) {
				return nil, p.errorf(keyTok, "expected option name, got %q", keyTok.Literal)
			}
			key := strings.ToLower(keyTok.Literal)
			if err := p.expect(lexer.TokenEq); err != nil {
				return nil, err
			}
			valTok := p.lex.Next()
			var val string
			switch valTok.Type {
			case lexer.TokenString:
				val = valTok.Literal
			case lexer.TokenTrue:
				val = "true"
			case lexer.TokenFalse:
				val = "false"
			case lexer.TokenInteger:
				val = valTok.Literal
			case lexer.TokenIdent:
				val = valTok.Literal
			default:
				return nil, p.errorf(valTok, "expected option value, got %q", valTok.Literal)
			}
			opts[key] = val
			if p.lex.Peek().Type == lexer.TokenComma {
				p.lex.Next()
			}
		}
		if err := p.expect(lexer.TokenRParen); err != nil {
			return nil, err
		}
		src.FormatOptions = opts
	}
	return src, nil
}

// parseFormatInto consumes a FORMAT clause and writes the format name and
// options into the given TableSource. The caller must have already verified
// that the next token is TokenFormat.
func (p *Parser) parseFormatInto(src *ast.TableSource) error {
	p.lex.Next() // consume FORMAT
	fmtTok := p.lex.Next()
	src.Format = strings.ToUpper(fmtTok.Literal)
	// Parse optional format options: FORMAT CSV(key=value, ...)
	if p.lex.Peek().Type == lexer.TokenLParen {
		p.lex.Next()
		opts := make(map[string]string)
		for p.lex.Peek().Type != lexer.TokenRParen && p.lex.Peek().Type != lexer.TokenEOF {
			keyTok := p.lex.Next()
			if keyTok.Type != lexer.TokenIdent && !lexer.IsKeyword(keyTok.Type) {
				return p.errorf(keyTok, "expected option name, got %q", keyTok.Literal)
			}
			key := strings.ToLower(keyTok.Literal)
			if err := p.expect(lexer.TokenEq); err != nil {
				return err
			}
			valTok := p.lex.Next()
			var val string
			switch valTok.Type {
			case lexer.TokenString:
				val = valTok.Literal
			case lexer.TokenTrue:
				val = "true"
			case lexer.TokenFalse:
				val = "false"
			case lexer.TokenInteger:
				val = valTok.Literal
			case lexer.TokenIdent:
				val = valTok.Literal
			default:
				return p.errorf(valTok, "expected option value, got %q", valTok.Literal)
			}
			opts[key] = val
			if p.lex.Peek().Type == lexer.TokenComma {
				p.lex.Next()
			}
		}
		if err := p.expect(lexer.TokenRParen); err != nil {
			return err
		}
		src.FormatOptions = opts
	}
	return nil
}

func (p *Parser) parseSelectList() ([]ast.Column, error) {
	var cols []ast.Column
	for {
		col, err := p.parseSelectColumn()
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
		if p.lex.Peek().Type != lexer.TokenComma {
			break
		}
		p.lex.Next() // consume comma
	}
	return cols, nil
}

func (p *Parser) parseSelectColumn() (ast.Column, error) {
	// Check for *
	if p.lex.Peek().Type == lexer.TokenStar {
		p.lex.Next()
		return ast.Column{Expr: &ast.StarExpr{}}, nil
	}

	expr, err := p.parseExpr()
	if err != nil {
		return ast.Column{}, err
	}

	var alias string
	if p.lex.Peek().Type == lexer.TokenAs {
		p.lex.Next()
		tok := p.lex.Next()
		if tok.Type != lexer.TokenIdent && !lexer.IsKeyword(tok.Type) {
			return ast.Column{}, p.errorf(tok, "expected alias name after AS, got %q", tok.Literal)
		}
		alias = tok.Literal
	} else if p.lex.Peek().Type == lexer.TokenIdent {
		// Implicit alias (no AS keyword) — only if it's a plain identifier
		// and not a keyword that could start the next clause
		tok := p.lex.Peek()
		if !isClauseKeyword(tok.Type) {
			p.lex.Next()
			alias = tok.Literal
		}
	}

	return ast.Column{Expr: expr, Alias: alias}, nil
}

// parseTableSourceWithAlias parses a FROM source followed by an optional alias.
// Supports both string literals ('kafka://...') and bare identifiers (stdin).
func (p *Parser) parseTableSourceWithAlias() (*ast.TableSource, string, error) {
	tok := p.lex.Peek()

	var src *ast.TableSource
	if tok.Type == lexer.TokenString {
		// Quoted source URI: FROM 'kafka://...'
		s, err := p.parseTableSource()
		if err != nil {
			return nil, "", err
		}
		src = s
	} else if tok.Type == lexer.TokenIdent || lexer.IsKeyword(tok.Type) {
		// Bare identifier: FROM stdin
		p.lex.Next()
		uri := tok.Literal
		// Normalize: stdin -> stdin://
		if strings.ToLower(uri) == "stdin" {
			uri = "stdin://"
		}
		src = &ast.TableSource{URI: uri}
		// Check for inline FORMAT after bare identifier (e.g., FROM stdin FORMAT DEBEZIUM)
		if p.lex.Peek().Type == lexer.TokenFormat {
			if err := p.parseFormatInto(src); err != nil {
				return nil, "", err
			}
		}
	} else {
		return nil, "", p.errorf(tok, "expected source URI or identifier after FROM, got %q", tok.Literal)
	}

	// Check for optional alias (identifier that's not a clause keyword and not JOIN/LEFT)
	var alias string
	next := p.lex.Peek()
	if next.Type == lexer.TokenIdent && !isClauseKeyword(next.Type) {
		p.lex.Next()
		alias = next.Literal
	}

	return src, alias, nil
}

func (p *Parser) parseTableSource() (*ast.TableSource, error) {
	tok := p.lex.Next()
	if tok.Type != lexer.TokenString {
		return nil, p.errorf(tok, "expected source URI string after FROM, got %q", tok.Literal)
	}
	src := &ast.TableSource{URI: tok.Literal}

	if p.lex.Peek().Type == lexer.TokenFormat {
		p.lex.Next()
		fmtTok := p.lex.Next()
		src.Format = strings.ToUpper(fmtTok.Literal)

		// Parse optional format options: FORMAT CSV(key=value, ...)
		if p.lex.Peek().Type == lexer.TokenLParen {
			p.lex.Next()
			opts := make(map[string]string)
			for p.lex.Peek().Type != lexer.TokenRParen && p.lex.Peek().Type != lexer.TokenEOF {
				keyTok := p.lex.Next()
				if keyTok.Type != lexer.TokenIdent && !lexer.IsKeyword(keyTok.Type) {
					return nil, p.errorf(keyTok, "expected option name, got %q", keyTok.Literal)
				}
				key := strings.ToLower(keyTok.Literal)
				if err := p.expect(lexer.TokenEq); err != nil {
					return nil, err
				}
				valTok := p.lex.Next()
				var val string
				switch valTok.Type {
				case lexer.TokenString:
					val = valTok.Literal
				case lexer.TokenTrue:
					val = "true"
				case lexer.TokenFalse:
					val = "false"
				case lexer.TokenInteger:
					val = valTok.Literal
				case lexer.TokenIdent:
					val = valTok.Literal
				default:
					return nil, p.errorf(valTok, "expected option value, got %q", valTok.Literal)
				}
				opts[key] = val
				if p.lex.Peek().Type == lexer.TokenComma {
					p.lex.Next()
				}
			}
			if err := p.expect(lexer.TokenRParen); err != nil {
				return nil, err
			}
			src.FormatOptions = opts
		}
	}

	return src, nil
}

// parseJoinClause parses [LEFT] JOIN <source> [alias] ON <condition>
func (p *Parser) parseJoinClause() (*ast.JoinClause, error) {
	joinType := "JOIN"
	if p.lex.Peek().Type == lexer.TokenLeft {
		p.lex.Next()
		joinType = "LEFT JOIN"
	}
	if err := p.expect(lexer.TokenJoin); err != nil {
		return nil, err
	}

	// Parse join source: string literal (file path)
	tok := p.lex.Peek()
	if tok.Type != lexer.TokenString {
		return nil, p.errorf(tok, "expected file path string after JOIN, got %q", tok.Literal)
	}
	src, err := p.parseTableSource()
	if err != nil {
		return nil, err
	}

	// Parse optional alias
	var alias string
	next := p.lex.Peek()
	if next.Type == lexer.TokenIdent && !isClauseKeyword(next.Type) {
		p.lex.Next()
		alias = next.Literal
	}

	// Parse ON condition
	if err := p.expect(lexer.TokenOn); err != nil {
		return nil, err
	}
	cond, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	// Parse optional WITHIN INTERVAL '<duration>'
	var within *string
	if p.lex.Peek().Type == lexer.TokenWithin {
		p.lex.Next() // consume WITHIN
		if err := p.expect(lexer.TokenInterval); err != nil {
			return nil, err
		}
		tok := p.lex.Peek()
		if tok.Type != lexer.TokenString {
			return nil, p.errorf(tok, "expected duration string after WITHIN INTERVAL, got %q", tok.Literal)
		}
		p.lex.Next()
		within = &tok.Literal
	}

	return &ast.JoinClause{
		Type:      joinType,
		Source:    src,
		Alias:     alias,
		Condition: cond,
		Within:    within,
	}, nil
}

// parseSeedClause parses SEED FROM '<file-path>' [FORMAT <format>]
func (p *Parser) parseSeedClause() (*ast.SeedClause, error) {
	p.lex.Next() // consume SEED
	if err := p.expect(lexer.TokenFrom); err != nil {
		return nil, err
	}

	tok := p.lex.Peek()
	if tok.Type != lexer.TokenString {
		return nil, p.errorf(tok, "expected file path string after SEED FROM, got %q", tok.Literal)
	}
	src, err := p.parseTableSource()
	if err != nil {
		return nil, err
	}

	return &ast.SeedClause{Source: src}, nil
}

func (p *Parser) parseWindowClause() (*ast.WindowClause, error) {
	tok := p.lex.Next()
	wtype := strings.ToUpper(tok.Literal)
	if wtype != "TUMBLING" && wtype != "SLIDING" && wtype != "SESSION" {
		return nil, p.errorf(tok, "expected TUMBLING, SLIDING, or SESSION after WINDOW, got %q", tok.Literal)
	}

	sizeTok := p.lex.Next()
	if sizeTok.Type != lexer.TokenString {
		return nil, p.errorf(sizeTok, "expected duration string after %s, got %q", wtype, sizeTok.Literal)
	}

	w := &ast.WindowClause{Type: wtype, Size: sizeTok.Literal}

	if wtype == "SLIDING" && p.lex.Peek().Type == lexer.TokenBy {
		p.lex.Next()
		slideTok := p.lex.Next()
		if slideTok.Type != lexer.TokenString {
			return nil, p.errorf(slideTok, "expected duration string after BY, got %q", slideTok.Literal)
		}
		w.SlideBy = slideTok.Literal
	}

	return w, nil
}

func (p *Parser) parseEmitClause() (*ast.EmitClause, error) {
	tok := p.lex.Next()
	switch tok.Type {
	case lexer.TokenFinal:
		return &ast.EmitClause{Type: "FINAL"}, nil
	case lexer.TokenEarly:
		durTok := p.lex.Next()
		if durTok.Type != lexer.TokenString {
			return nil, p.errorf(durTok, "expected duration string after EMIT EARLY, got %q", durTok.Literal)
		}
		return &ast.EmitClause{Type: "EARLY", Interval: durTok.Literal}, nil
	default:
		return nil, p.errorf(tok, "expected FINAL or EARLY after EMIT, got %q", tok.Literal)
	}
}

func (p *Parser) parseDeduplicateClause() (*ast.DeduplicateClause, error) {
	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	d := &ast.DeduplicateClause{Key: expr}

	if p.lex.Peek().Type == lexer.TokenWithin {
		p.lex.Next()
		durTok := p.lex.Next()
		if durTok.Type != lexer.TokenString {
			return nil, p.errorf(durTok, "expected duration string after WITHIN, got %q", durTok.Literal)
		}
		d.Within = durTok.Literal
	}

	if p.lex.Peek().Type == lexer.TokenCapacity {
		p.lex.Next()
		capTok := p.lex.Next()
		if capTok.Type != lexer.TokenInteger {
			return nil, p.errorf(capTok, "expected integer after CAPACITY, got %q", capTok.Literal)
		}
		n, _ := strconv.Atoi(capTok.Literal)
		d.Capacity = &n
	}

	return d, nil
}

func (p *Parser) parseOrderByList() ([]ast.OrderByItem, error) {
	var items []ast.OrderByItem
	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		item := ast.OrderByItem{Expr: expr}
		if p.lex.Peek().Type == lexer.TokenAsc {
			p.lex.Next()
		} else if p.lex.Peek().Type == lexer.TokenDesc {
			p.lex.Next()
			item.Desc = true
		}
		items = append(items, item)
		if p.lex.Peek().Type != lexer.TokenComma {
			break
		}
		p.lex.Next()
	}
	return items, nil
}

func (p *Parser) parseExprList() ([]ast.Expr, error) {
	var exprs []ast.Expr
	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
		if p.lex.Peek().Type != lexer.TokenComma {
			break
		}
		p.lex.Next()
	}
	return exprs, nil
}

// parseExpr parses an expression starting at the lowest precedence (OR).
func (p *Parser) parseExpr() (ast.Expr, error) {
	return p.parseOr()
}

func (p *Parser) parseOr() (ast.Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.lex.Peek().Type == lexer.TokenOr {
		p.lex.Next()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &ast.BinaryExpr{Left: left, Op: "OR", Right: right}
	}
	return left, nil
}

func (p *Parser) parseAnd() (ast.Expr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.lex.Peek().Type == lexer.TokenAnd {
		p.lex.Next()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &ast.BinaryExpr{Left: left, Op: "AND", Right: right}
	}
	return left, nil
}

func (p *Parser) parseNot() (ast.Expr, error) {
	if p.lex.Peek().Type == lexer.TokenNot {
		p.lex.Next()
		expr, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &ast.UnaryExpr{Op: "NOT", Expr: expr}, nil
	}
	return p.parseComparison()
}

func (p *Parser) parseComparison() (ast.Expr, error) {
	left, err := p.parseConcat()
	if err != nil {
		return nil, err
	}

	tok := p.lex.Peek()

	// Handle NOT BETWEEN, NOT IN, NOT LIKE, NOT ILIKE
	if tok.Type == lexer.TokenNot {
		p.lex.Next()
		next := p.lex.Peek()
		switch next.Type {
		case lexer.TokenBetween:
			p.lex.Next()
			return p.parseBetweenRest(left, true)
		case lexer.TokenIn:
			p.lex.Next()
			return p.parseInRest(left, true)
		case lexer.TokenLike:
			p.lex.Next()
			right, err := p.parseConcat()
			if err != nil {
				return nil, err
			}
			return &ast.UnaryExpr{Op: "NOT", Expr: &ast.BinaryExpr{Left: left, Op: "LIKE", Right: right}}, nil
		case lexer.TokenIlike:
			p.lex.Next()
			right, err := p.parseConcat()
			if err != nil {
				return nil, err
			}
			return &ast.UnaryExpr{Op: "NOT", Expr: &ast.BinaryExpr{Left: left, Op: "ILIKE", Right: right}}, nil
		default:
			p.lex.Backup()
			return left, nil
		}
	}

	switch tok.Type {
	case lexer.TokenEq:
		p.lex.Next()
		right, err := p.parseConcat()
		if err != nil {
			return nil, err
		}
		return &ast.BinaryExpr{Left: left, Op: "=", Right: right}, nil
	case lexer.TokenNeq:
		p.lex.Next()
		right, err := p.parseConcat()
		if err != nil {
			return nil, err
		}
		return &ast.BinaryExpr{Left: left, Op: "!=", Right: right}, nil
	case lexer.TokenLt:
		p.lex.Next()
		right, err := p.parseConcat()
		if err != nil {
			return nil, err
		}
		return &ast.BinaryExpr{Left: left, Op: "<", Right: right}, nil
	case lexer.TokenGt:
		p.lex.Next()
		right, err := p.parseConcat()
		if err != nil {
			return nil, err
		}
		return &ast.BinaryExpr{Left: left, Op: ">", Right: right}, nil
	case lexer.TokenLtEq:
		p.lex.Next()
		right, err := p.parseConcat()
		if err != nil {
			return nil, err
		}
		return &ast.BinaryExpr{Left: left, Op: "<=", Right: right}, nil
	case lexer.TokenGtEq:
		p.lex.Next()
		right, err := p.parseConcat()
		if err != nil {
			return nil, err
		}
		return &ast.BinaryExpr{Left: left, Op: ">=", Right: right}, nil
	case lexer.TokenLike:
		p.lex.Next()
		right, err := p.parseConcat()
		if err != nil {
			return nil, err
		}
		return &ast.BinaryExpr{Left: left, Op: "LIKE", Right: right}, nil
	case lexer.TokenIlike:
		p.lex.Next()
		right, err := p.parseConcat()
		if err != nil {
			return nil, err
		}
		return &ast.BinaryExpr{Left: left, Op: "ILIKE", Right: right}, nil
	case lexer.TokenIs:
		p.lex.Next()
		return p.parseIsRest(left)
	case lexer.TokenIn:
		p.lex.Next()
		return p.parseInRest(left, false)
	case lexer.TokenBetween:
		p.lex.Next()
		return p.parseBetweenRest(left, false)
	}

	return left, nil
}

func (p *Parser) parseBetweenRest(left ast.Expr, not bool) (ast.Expr, error) {
	low, err := p.parseConcat()
	if err != nil {
		return nil, err
	}
	if err := p.expect(lexer.TokenAnd); err != nil {
		return nil, err
	}
	high, err := p.parseConcat()
	if err != nil {
		return nil, err
	}
	return &ast.BetweenExpr{Expr: left, Low: low, High: high, Not: not}, nil
}

func (p *Parser) parseInRest(left ast.Expr, not bool) (ast.Expr, error) {
	if err := p.expect(lexer.TokenLParen); err != nil {
		return nil, err
	}
	values, err := p.parseExprList()
	if err != nil {
		return nil, err
	}
	if err := p.expect(lexer.TokenRParen); err != nil {
		return nil, err
	}
	return &ast.InExpr{Expr: left, Values: values, Not: not}, nil
}

func (p *Parser) parseIsRest(left ast.Expr) (ast.Expr, error) {
	not := false
	if p.lex.Peek().Type == lexer.TokenNot {
		p.lex.Next()
		not = true
	}

	tok := p.lex.Peek()
	switch tok.Type {
	case lexer.TokenNull:
		p.lex.Next()
		return &ast.IsExpr{Expr: left, Not: not, What: "NULL"}, nil
	case lexer.TokenTrue:
		p.lex.Next()
		return &ast.IsExpr{Expr: left, Not: not, What: "TRUE"}, nil
	case lexer.TokenFalse:
		p.lex.Next()
		return &ast.IsExpr{Expr: left, Not: not, What: "FALSE"}, nil
	case lexer.TokenDistinct:
		p.lex.Next()
		if err := p.expect(lexer.TokenFrom); err != nil {
			return nil, err
		}
		from, err := p.parseConcat()
		if err != nil {
			return nil, err
		}
		return &ast.IsExpr{Expr: left, Not: not, What: "DISTINCT FROM", From: from}, nil
	default:
		return nil, p.errorf(tok, "expected NULL, TRUE, FALSE, or DISTINCT FROM after IS%s, got %q",
			map[bool]string{true: " NOT", false: ""}[not], tok.Literal)
	}
}

func (p *Parser) parseConcat() (ast.Expr, error) {
	left, err := p.parseAddSub()
	if err != nil {
		return nil, err
	}
	for p.lex.Peek().Type == lexer.TokenConcat {
		p.lex.Next()
		right, err := p.parseAddSub()
		if err != nil {
			return nil, err
		}
		left = &ast.BinaryExpr{Left: left, Op: "||", Right: right}
	}
	return left, nil
}

func (p *Parser) parseAddSub() (ast.Expr, error) {
	left, err := p.parseMulDiv()
	if err != nil {
		return nil, err
	}
	for {
		tok := p.lex.Peek()
		if tok.Type != lexer.TokenPlus && tok.Type != lexer.TokenMinus {
			break
		}
		p.lex.Next()
		right, err := p.parseMulDiv()
		if err != nil {
			return nil, err
		}
		left = &ast.BinaryExpr{Left: left, Op: tok.Literal, Right: right}
	}
	return left, nil
}

func (p *Parser) parseMulDiv() (ast.Expr, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for {
		tok := p.lex.Peek()
		if tok.Type != lexer.TokenStar && tok.Type != lexer.TokenSlash && tok.Type != lexer.TokenPercent {
			break
		}
		p.lex.Next()
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = &ast.BinaryExpr{Left: left, Op: tok.Literal, Right: right}
	}
	return left, nil
}

func (p *Parser) parseUnary() (ast.Expr, error) {
	tok := p.lex.Peek()
	if tok.Type == lexer.TokenMinus || tok.Type == lexer.TokenPlus {
		p.lex.Next()
		expr, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &ast.UnaryExpr{Op: tok.Literal, Expr: expr}, nil
	}
	return p.parsePostfix()
}

// parsePostfix handles JSON access (->, ->>) and type cast (::), which are the
// highest precedence operators (left-associative).
func (p *Parser) parsePostfix() (ast.Expr, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	for {
		tok := p.lex.Peek()
		switch tok.Type {
		case lexer.TokenArrow, lexer.TokenArrowText:
			p.lex.Next()
			asText := tok.Type == lexer.TokenArrowText
			key, err := p.parsePrimary()
			if err != nil {
				return nil, err
			}
			left = &ast.JsonAccessExpr{Left: left, Key: key, AsText: asText}
		case lexer.TokenCast:
			p.lex.Next()
			typeTok := p.lex.Next()
			typeName := strings.ToUpper(typeTok.Literal)
			left = &ast.CastExpr{Expr: left, TypeName: typeName}
		default:
			return left, nil
		}
	}
}

func (p *Parser) parsePrimary() (ast.Expr, error) {
	tok := p.lex.Peek()

	switch tok.Type {
	case lexer.TokenInteger:
		p.lex.Next()
		return &ast.NumberLiteral{Value: tok.Literal, IsFloat: false}, nil
	case lexer.TokenFloat:
		p.lex.Next()
		return &ast.NumberLiteral{Value: tok.Literal, IsFloat: true}, nil
	case lexer.TokenString:
		p.lex.Next()
		return &ast.StringLiteral{Value: tok.Literal}, nil
	case lexer.TokenTrue:
		p.lex.Next()
		return &ast.BoolLiteral{Value: true}, nil
	case lexer.TokenFalse:
		p.lex.Next()
		return &ast.BoolLiteral{Value: false}, nil
	case lexer.TokenNull:
		p.lex.Next()
		return &ast.NullLiteral{}, nil
	case lexer.TokenStar:
		p.lex.Next()
		return &ast.StarExpr{}, nil
	case lexer.TokenInterval:
		p.lex.Next()
		valTok := p.lex.Next()
		if valTok.Type != lexer.TokenString {
			return nil, p.errorf(valTok, "expected string after INTERVAL, got %q", valTok.Literal)
		}
		return &ast.IntervalLiteral{Value: valTok.Literal}, nil

	case lexer.TokenCastKw:
		return p.parseCastFunction()

	case lexer.TokenLParen:
		p.lex.Next()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expect(lexer.TokenRParen); err != nil {
			return nil, err
		}
		return expr, nil

	case lexer.TokenCase:
		return p.parseCaseExpr()

	default:
		// Check for function calls and identifiers
		if tok.Type == lexer.TokenIdent || isCallableKeyword(tok.Type) {
			return p.parseIdentOrFunction()
		}
		return nil, p.errorf(tok, "unexpected token %q", tok.Literal)
	}
}

func (p *Parser) parseCastFunction() (ast.Expr, error) {
	p.lex.Next() // consume CAST
	if err := p.expect(lexer.TokenLParen); err != nil {
		return nil, err
	}
	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if err := p.expect(lexer.TokenAs); err != nil {
		return nil, err
	}
	typeTok := p.lex.Next()
	typeName := strings.ToUpper(typeTok.Literal)
	if err := p.expect(lexer.TokenRParen); err != nil {
		return nil, err
	}
	return &ast.CastExpr{Expr: expr, TypeName: typeName}, nil
}

func (p *Parser) parseCaseExpr() (ast.Expr, error) {
	p.lex.Next() // consume CASE
	var whens []ast.CaseWhen
	for p.lex.Peek().Type == lexer.TokenWhen {
		p.lex.Next()
		cond, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expect(lexer.TokenThen); err != nil {
			return nil, err
		}
		result, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		whens = append(whens, ast.CaseWhen{Condition: cond, Result: result})
	}
	var elseExpr ast.Expr
	if p.lex.Peek().Type == lexer.TokenElse {
		p.lex.Next()
		var err error
		elseExpr, err = p.parseExpr()
		if err != nil {
			return nil, err
		}
	}
	if err := p.expect(lexer.TokenEnd); err != nil {
		return nil, err
	}
	return &ast.CaseExpr{Whens: whens, Else: elseExpr}, nil
}

func (p *Parser) parseIdentOrFunction() (ast.Expr, error) {
	tok := p.lex.Next()
	name := tok.Literal

	// Check for qualified reference: alias.column
	if p.lex.Peek().Type == lexer.TokenDot {
		p.lex.Next() // consume '.'
		colTok := p.lex.Next()
		if colTok.Type != lexer.TokenIdent && colTok.Type != lexer.TokenStar && !lexer.IsKeyword(colTok.Type) {
			return nil, p.errorf(colTok, "expected column name after %q., got %q", name, colTok.Literal)
		}
		if colTok.Type == lexer.TokenStar {
			// alias.* — return a QualifiedRef with "*" as the name
			return &ast.QualifiedRef{Qualifier: name, Name: "*"}, nil
		}
		return &ast.QualifiedRef{Qualifier: name, Name: colTok.Literal}, nil
	}

	// Check if it's a function call (followed by '(')
	if p.lex.Peek().Type == lexer.TokenLParen {
		p.lex.Next() // consume '('
		funcName := strings.ToUpper(name)

		// COUNT(*) special case
		if funcName == "COUNT" && p.lex.Peek().Type == lexer.TokenStar {
			p.lex.Next() // consume *
			if err := p.expect(lexer.TokenRParen); err != nil {
				return nil, err
			}
			return &ast.FunctionCall{Name: funcName, Args: []ast.Expr{&ast.StarExpr{}}}, nil
		}

		// DISTINCT inside aggregate
		distinct := false
		if p.lex.Peek().Type == lexer.TokenDistinct {
			p.lex.Next()
			distinct = true
		}

		// Empty args
		if p.lex.Peek().Type == lexer.TokenRParen {
			p.lex.Next()
			return &ast.FunctionCall{Name: funcName, Args: nil, Distinct: distinct}, nil
		}

		args, err := p.parseExprList()
		if err != nil {
			return nil, err
		}
		if err := p.expect(lexer.TokenRParen); err != nil {
			return nil, err
		}
		return &ast.FunctionCall{Name: funcName, Args: args, Distinct: distinct}, nil
	}

	// Plain column reference
	return &ast.ColumnRef{Name: name}, nil
}

func (p *Parser) expect(tt lexer.TokenType) error {
	tok := p.lex.Next()
	if tok.Type != tt {
		return p.errorf(tok, "expected %s, got %q", tokenTypeName(tt), tok.Literal)
	}
	return nil
}

func (p *Parser) errorf(tok lexer.Token, format string, args ...any) error {
	msg := fmt.Sprintf(format, args...)

	// Build caret line pointing to the problematic token
	input := p.lex.Input()
	lines := strings.Split(input, "\n")
	lineIdx := tok.Line - 1
	var caret string
	if lineIdx >= 0 && lineIdx < len(lines) {
		srcLine := lines[lineIdx]
		col := tok.Col - 1
		if col < 0 {
			col = 0
		}
		pad := strings.Repeat(" ", col)
		caret = fmt.Sprintf("\n  %s\n  %s^", srcLine, pad)
	}

	// Check for common typo suggestions
	suggestion := suggestFix(tok)
	if suggestion != "" {
		return fmt.Errorf("%s: %s%s\n  Hint: %s", tok.Pos(), msg, caret, suggestion)
	}

	return fmt.Errorf("%s: %s%s", tok.Pos(), msg, caret)
}

// suggestFix returns a suggestion string for common misspellings, or "".
func suggestFix(tok lexer.Token) string {
	if tok.Type == lexer.TokenIdent || tok.Type == lexer.TokenIllegal {
		upper := strings.ToUpper(tok.Literal)
		suggestions := map[string]string{
			"FORM":      "Did you mean FROM?",
			"FOMR":      "Did you mean FROM?",
			"SLECT":     "Did you mean SELECT?",
			"SELCT":     "Did you mean SELECT?",
			"SLELECT":   "Did you mean SELECT?",
			"WEHRE":     "Did you mean WHERE?",
			"WHEER":     "Did you mean WHERE?",
			"GORUP":     "Did you mean GROUP?",
			"GRUOP":     "Did you mean GROUP?",
			"GROPU":     "Did you mean GROUP?",
			"GRUOPBY":   "Did you mean GROUP BY?",
			"GROUPY":    "Did you mean GROUP BY?",
			"HAIVNG":    "Did you mean HAVING?",
			"HVIANG":    "Did you mean HAVING?",
			"ORDERY":    "Did you mean ORDER BY?",
			"ODER":      "Did you mean ORDER?",
			"LMIT":      "Did you mean LIMIT?",
			"LIMT":      "Did you mean LIMIT?",
			"WIDNOW":    "Did you mean WINDOW?",
			"WINODW":    "Did you mean WINDOW?",
			"DISTICT":   "Did you mean DISTINCT?",
			"DISTINC":   "Did you mean DISTINCT?",
			"FORAMT":    "Did you mean FORMAT?",
			"FROMAT":    "Did you mean FORMAT?",
			"DEBEZIUIM": "Did you mean DEBEZIUM?",
		}
		if s, ok := suggestions[upper]; ok {
			return s
		}
	}
	return ""
}

func isClauseKeyword(tt lexer.TokenType) bool {
	switch tt {
	case lexer.TokenFrom, lexer.TokenWhere, lexer.TokenGroup, lexer.TokenHaving,
		lexer.TokenOrder, lexer.TokenLimit, lexer.TokenWindow, lexer.TokenEmit,
		lexer.TokenEvent, lexer.TokenWatermark, lexer.TokenDeduplicate,
		lexer.TokenFormat, lexer.TokenJoin, lexer.TokenLeft, lexer.TokenOn,
		lexer.TokenWithin:
		return true
	}
	return false
}

func isCallableKeyword(tt lexer.TokenType) bool {
	switch tt {
	case lexer.TokenCount, lexer.TokenSum, lexer.TokenAvg, lexer.TokenMin,
		lexer.TokenMax, lexer.TokenMedian, lexer.TokenFirst, lexer.TokenLast,
		lexer.TokenArrayAgg, lexer.TokenApproxCountDistinct,
		lexer.TokenCoalesce, lexer.TokenNullif, lexer.TokenLength,
		lexer.TokenUpper, lexer.TokenLower, lexer.TokenTrim, lexer.TokenLtrim,
		lexer.TokenRtrim, lexer.TokenSubstr, lexer.TokenReplace,
		lexer.TokenSplitPart, lexer.TokenParseTimestamp, lexer.TokenFormatTimestamp,
		lexer.TokenNow, lexer.TokenExtract, lexer.TokenJsonKeys:
		return true
	}
	return false
}

func tokenTypeName(tt lexer.TokenType) string {
	switch tt {
	case lexer.TokenEOF:
		return "EOF"
	case lexer.TokenLParen:
		return "'('"
	case lexer.TokenRParen:
		return "')'"
	case lexer.TokenComma:
		return "','"
	case lexer.TokenBy:
		return "'BY'"
	case lexer.TokenAs:
		return "'AS'"
	case lexer.TokenAnd:
		return "'AND'"
	case lexer.TokenFrom:
		return "'FROM'"
	case lexer.TokenTime:
		return "'TIME'"
	case lexer.TokenThen:
		return "'THEN'"
	case lexer.TokenEnd:
		return "'END'"
	case lexer.TokenSelect:
		return "'SELECT'"
	case lexer.TokenJoin:
		return "'JOIN'"
	case lexer.TokenOn:
		return "'ON'"
	default:
		return fmt.Sprintf("token(%d)", tt)
	}
}
