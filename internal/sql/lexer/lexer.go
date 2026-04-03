// Package lexer provides a tokenizer for DBSPA's SQL dialect.
package lexer

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// TokenType represents the type of a lexical token.
type TokenType int

const (
	// Special tokens
	TokenEOF     TokenType = iota
	TokenIllegal           // unrecognized character or sequence

	// Literals
	TokenIdent   // unquoted identifier
	TokenString  // single-quoted string literal
	TokenInteger // integer literal
	TokenFloat   // float literal (contains '.')

	// Operators
	TokenPlus       // +
	TokenMinus      // -
	TokenStar       // *
	TokenSlash      // /
	TokenPercent    // %
	TokenEq         // =
	TokenNeq        // != or <>
	TokenLt         // <
	TokenGt         // >
	TokenLtEq       // <=
	TokenGtEq       // >=
	TokenConcat     // ||
	TokenCast       // ::
	TokenArrow      // ->
	TokenArrowText  // ->>

	// Punctuation
	TokenLParen // (
	TokenRParen // )
	TokenComma  // ,
	TokenDot    // .

	// Keywords
	TokenSelect
	TokenDistinct
	TokenFrom
	TokenWhere
	TokenGroup
	TokenBy
	TokenHaving
	TokenOrder
	TokenAsc
	TokenDesc
	TokenLimit
	TokenWindow
	TokenTumbling
	TokenSliding
	TokenSession
	TokenEmit
	TokenFinal
	TokenEarly
	TokenEvent
	TokenTime
	TokenWatermark
	TokenDeduplicate
	TokenWithin
	TokenCapacity
	TokenFormat
	TokenDebezium
	TokenCSV
	TokenAs
	TokenAnd
	TokenOr
	TokenNot
	TokenIn
	TokenBetween
	TokenIs
	TokenNull
	TokenTrue
	TokenFalse
	TokenLike
	TokenIlike
	TokenCase
	TokenWhen
	TokenThen
	TokenElse
	TokenEnd
	TokenInterval
	TokenCastKw // CAST keyword
	TokenCount
	TokenSum
	TokenAvg
	TokenMin
	TokenMax
	TokenMedian
	TokenFirst
	TokenLast
	TokenArrayAgg
	TokenApproxCountDistinct
	TokenCoalesce
	TokenNullif
	TokenLength
	TokenUpper
	TokenLower
	TokenTrim
	TokenLtrim
	TokenRtrim
	TokenSubstr
	TokenReplace
	TokenSplitPart
	TokenParseTimestamp
	TokenFormatTimestamp
	TokenNow
	TokenExtract
	TokenJsonKeys
	TokenJoin
	TokenLeft
	TokenOn
	TokenSeed
	TokenExec
	TokenStream
	TokenTable
	TokenFolddb
	TokenAvro
	TokenJson
	TokenNdjson
	TokenProtobuf
	TokenParquet
	TokenChangelog
)

var keywords = map[string]TokenType{
	"SELECT":               TokenSelect,
	"DISTINCT":             TokenDistinct,
	"FROM":                 TokenFrom,
	"WHERE":                TokenWhere,
	"GROUP":                TokenGroup,
	"BY":                   TokenBy,
	"HAVING":               TokenHaving,
	"ORDER":                TokenOrder,
	"ASC":                  TokenAsc,
	"DESC":                 TokenDesc,
	"LIMIT":                TokenLimit,
	"WINDOW":               TokenWindow,
	"TUMBLING":             TokenTumbling,
	"SLIDING":              TokenSliding,
	"SESSION":              TokenSession,
	"EMIT":                 TokenEmit,
	"FINAL":                TokenFinal,
	"EARLY":                TokenEarly,
	"EVENT":                TokenEvent,
	"TIME":                 TokenTime,
	"WATERMARK":            TokenWatermark,
	"DEDUPLICATE":          TokenDeduplicate,
	"WITHIN":               TokenWithin,
	"CAPACITY":             TokenCapacity,
	"FORMAT":               TokenFormat,
	"DEBEZIUM":             TokenDebezium,
	"CSV":                  TokenCSV,
	"AS":                   TokenAs,
	"AND":                  TokenAnd,
	"OR":                   TokenOr,
	"NOT":                  TokenNot,
	"IN":                   TokenIn,
	"BETWEEN":              TokenBetween,
	"IS":                   TokenIs,
	"NULL":                 TokenNull,
	"TRUE":                 TokenTrue,
	"FALSE":                TokenFalse,
	"LIKE":                 TokenLike,
	"ILIKE":                TokenIlike,
	"CASE":                 TokenCase,
	"WHEN":                 TokenWhen,
	"THEN":                 TokenThen,
	"ELSE":                 TokenElse,
	"END":                  TokenEnd,
	"INTERVAL":             TokenInterval,
	"CAST":                 TokenCastKw,
	"COUNT":                TokenCount,
	"SUM":                  TokenSum,
	"AVG":                  TokenAvg,
	"MIN":                  TokenMin,
	"MAX":                  TokenMax,
	"MEDIAN":               TokenMedian,
	"FIRST":                TokenFirst,
	"LAST":                 TokenLast,
	"ARRAY_AGG":            TokenArrayAgg,
	"APPROX_COUNT_DISTINCT": TokenApproxCountDistinct,
	"COALESCE":             TokenCoalesce,
	"NULLIF":               TokenNullif,
	"LENGTH":               TokenLength,
	"UPPER":                TokenUpper,
	"LOWER":                TokenLower,
	"TRIM":                 TokenTrim,
	"LTRIM":                TokenLtrim,
	"RTRIM":                TokenRtrim,
	"SUBSTR":               TokenSubstr,
	"REPLACE":              TokenReplace,
	"SPLIT_PART":           TokenSplitPart,
	"PARSE_TIMESTAMP":      TokenParseTimestamp,
	"FORMAT_TIMESTAMP":     TokenFormatTimestamp,
	"NOW":                  TokenNow,
	"EXTRACT":              TokenExtract,
	"JSON_KEYS":            TokenJsonKeys,
	"JOIN":                 TokenJoin,
	"LEFT":                 TokenLeft,
	"ON":                   TokenOn,
	"SEED":                 TokenSeed,
	"EXEC":                 TokenExec,
	"STREAM":               TokenStream,
	"TABLE":                TokenTable,
	"DBSPA":               TokenFolddb,
	"AVRO":                 TokenAvro,
	"JSON":                 TokenJson,
	"NDJSON":               TokenNdjson,
	"PROTOBUF":             TokenProtobuf,
	"PARQUET":              TokenParquet,
	"CHANGELOG":            TokenChangelog,
}

// Token represents a single lexical token with position information.
type Token struct {
	Type    TokenType
	Literal string
	Line    int
	Col     int
}

// Pos returns the token position formatted as "line:col".
func (t Token) Pos() string {
	return fmt.Sprintf("%d:%d", t.Line, t.Col)
}

// String returns a human-readable representation of the token.
func (t Token) String() string {
	if t.Type == TokenEOF {
		return "EOF"
	}
	return t.Literal
}

// Lexer tokenizes SQL input into a stream of tokens.
type Lexer struct {
	input   string
	pos     int // current byte position
	line    int
	col     int
	tokens  []Token
	current int
}

// New creates a new Lexer for the given SQL input.
func New(input string) *Lexer {
	l := &Lexer{
		input: input,
		line:  1,
		col:   1,
	}
	l.tokenize()
	return l
}

// Next returns the next token and advances the cursor.
func (l *Lexer) Next() Token {
	if l.current >= len(l.tokens) {
		return Token{Type: TokenEOF, Line: l.line, Col: l.col}
	}
	t := l.tokens[l.current]
	l.current++
	return t
}

// Peek returns the next token without advancing the cursor.
func (l *Lexer) Peek() Token {
	if l.current >= len(l.tokens) {
		return Token{Type: TokenEOF, Line: l.line, Col: l.col}
	}
	return l.tokens[l.current]
}

// Backup moves the cursor back one token.
func (l *Lexer) Backup() {
	if l.current > 0 {
		l.current--
	}
}

func (l *Lexer) tokenize() {
	for {
		l.skipWhitespace()
		if l.pos >= len(l.input) {
			l.tokens = append(l.tokens, Token{Type: TokenEOF, Line: l.line, Col: l.col})
			return
		}

		startLine, startCol := l.line, l.col
		ch := l.peekChar()

		switch {
		case ch == '-' && l.peekAt(1) == '-':
			// Line comment
			l.skipLineComment()
			continue

		case ch == '\'':
			l.tokens = append(l.tokens, l.readString(startLine, startCol))

		case ch == '"':
			// Double-quoted identifier (preserves case, avoids keyword clash)
			l.tokens = append(l.tokens, l.readQuotedIdent(startLine, startCol))

		case isDigit(ch):
			l.tokens = append(l.tokens, l.readNumber(startLine, startCol))

		case isIdentStart(ch):
			l.tokens = append(l.tokens, l.readIdentOrKeyword(startLine, startCol))

		case ch == '(':
			l.advance()
			l.tokens = append(l.tokens, Token{Type: TokenLParen, Literal: "(", Line: startLine, Col: startCol})
		case ch == ')':
			l.advance()
			l.tokens = append(l.tokens, Token{Type: TokenRParen, Literal: ")", Line: startLine, Col: startCol})
		case ch == ',':
			l.advance()
			l.tokens = append(l.tokens, Token{Type: TokenComma, Literal: ",", Line: startLine, Col: startCol})
		case ch == '.':
			l.advance()
			l.tokens = append(l.tokens, Token{Type: TokenDot, Literal: ".", Line: startLine, Col: startCol})
		case ch == '+':
			l.advance()
			l.tokens = append(l.tokens, Token{Type: TokenPlus, Literal: "+", Line: startLine, Col: startCol})
		case ch == '*':
			l.advance()
			l.tokens = append(l.tokens, Token{Type: TokenStar, Literal: "*", Line: startLine, Col: startCol})
		case ch == '/':
			l.advance()
			l.tokens = append(l.tokens, Token{Type: TokenSlash, Literal: "/", Line: startLine, Col: startCol})
		case ch == '%':
			l.advance()
			l.tokens = append(l.tokens, Token{Type: TokenPercent, Literal: "%", Line: startLine, Col: startCol})
		case ch == '=':
			l.advance()
			l.tokens = append(l.tokens, Token{Type: TokenEq, Literal: "=", Line: startLine, Col: startCol})

		case ch == '!':
			l.advance()
			if l.peekChar() == '=' {
				l.advance()
				l.tokens = append(l.tokens, Token{Type: TokenNeq, Literal: "!=", Line: startLine, Col: startCol})
			} else {
				l.tokens = append(l.tokens, Token{Type: TokenIllegal, Literal: "!", Line: startLine, Col: startCol})
			}

		case ch == '<':
			l.advance()
			if l.peekChar() == '=' {
				l.advance()
				l.tokens = append(l.tokens, Token{Type: TokenLtEq, Literal: "<=", Line: startLine, Col: startCol})
			} else if l.peekChar() == '>' {
				l.advance()
				l.tokens = append(l.tokens, Token{Type: TokenNeq, Literal: "<>", Line: startLine, Col: startCol})
			} else {
				l.tokens = append(l.tokens, Token{Type: TokenLt, Literal: "<", Line: startLine, Col: startCol})
			}

		case ch == '>':
			l.advance()
			if l.peekChar() == '=' {
				l.advance()
				l.tokens = append(l.tokens, Token{Type: TokenGtEq, Literal: ">=", Line: startLine, Col: startCol})
			} else {
				l.tokens = append(l.tokens, Token{Type: TokenGt, Literal: ">", Line: startLine, Col: startCol})
			}

		case ch == '|':
			l.advance()
			if l.peekChar() == '|' {
				l.advance()
				l.tokens = append(l.tokens, Token{Type: TokenConcat, Literal: "||", Line: startLine, Col: startCol})
			} else {
				l.tokens = append(l.tokens, Token{Type: TokenIllegal, Literal: "|", Line: startLine, Col: startCol})
			}

		case ch == ':':
			l.advance()
			if l.peekChar() == ':' {
				l.advance()
				l.tokens = append(l.tokens, Token{Type: TokenCast, Literal: "::", Line: startLine, Col: startCol})
			} else {
				l.tokens = append(l.tokens, Token{Type: TokenIllegal, Literal: ":", Line: startLine, Col: startCol})
			}

		case ch == '-':
			l.advance()
			if l.peekChar() == '>' {
				l.advance()
				if l.peekChar() == '>' {
					l.advance()
					l.tokens = append(l.tokens, Token{Type: TokenArrowText, Literal: "->>", Line: startLine, Col: startCol})
				} else {
					l.tokens = append(l.tokens, Token{Type: TokenArrow, Literal: "->", Line: startLine, Col: startCol})
				}
			} else {
				l.tokens = append(l.tokens, Token{Type: TokenMinus, Literal: "-", Line: startLine, Col: startCol})
			}

		default:
			l.advance()
			l.tokens = append(l.tokens, Token{Type: TokenIllegal, Literal: string(ch), Line: startLine, Col: startCol})
		}
	}
}

func (l *Lexer) peekChar() rune {
	if l.pos >= len(l.input) {
		return 0
	}
	r, _ := utf8.DecodeRuneInString(l.input[l.pos:])
	return r
}

func (l *Lexer) peekAt(offset int) rune {
	p := l.pos + offset
	if p >= len(l.input) {
		return 0
	}
	r, _ := utf8.DecodeRuneInString(l.input[p:])
	return r
}

func (l *Lexer) advance() rune {
	if l.pos >= len(l.input) {
		return 0
	}
	r, size := utf8.DecodeRuneInString(l.input[l.pos:])
	l.pos += size
	if r == '\n' {
		l.line++
		l.col = 1
	} else {
		l.col++
	}
	return r
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) {
		ch := l.peekChar()
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			l.advance()
		} else {
			break
		}
	}
}

func (l *Lexer) skipLineComment() {
	// skip --
	l.advance()
	l.advance()
	for l.pos < len(l.input) && l.peekChar() != '\n' {
		l.advance()
	}
}

func (l *Lexer) readString(line, col int) Token {
	l.advance() // skip opening quote
	var sb strings.Builder
	for {
		if l.pos >= len(l.input) {
			return Token{Type: TokenIllegal, Literal: "unterminated string", Line: line, Col: col}
		}
		ch := l.advance()
		if ch == '\'' {
			if l.peekChar() == '\'' {
				// escaped single quote
				l.advance()
				sb.WriteRune('\'')
			} else {
				break
			}
		} else {
			sb.WriteRune(ch)
		}
	}
	return Token{Type: TokenString, Literal: sb.String(), Line: line, Col: col}
}

// readQuotedIdent reads a double-quoted identifier: "column_name"
// Returns a TokenIdent (not a keyword) with the exact case preserved.
// Double-quote escaping: "" inside a quoted ident produces a literal ".
func (l *Lexer) readQuotedIdent(line, col int) Token {
	l.advance() // skip opening "
	var sb strings.Builder
	for {
		if l.pos >= len(l.input) {
			return Token{Type: TokenIllegal, Literal: "unterminated quoted identifier", Line: line, Col: col}
		}
		ch := l.advance()
		if ch == '"' {
			if l.peekChar() == '"' {
				// escaped double quote
				l.advance()
				sb.WriteRune('"')
			} else {
				break
			}
		} else {
			sb.WriteRune(ch)
		}
	}
	// Always return as TokenIdent, never as a keyword — this is the point
	// of quoting: "last" is the column named last, not the LAST keyword.
	return Token{Type: TokenIdent, Literal: sb.String(), Line: line, Col: col}
}

func (l *Lexer) readNumber(line, col int) Token {
	start := l.pos
	isFloat := false
	for l.pos < len(l.input) && (isDigit(l.peekChar()) || l.peekChar() == '.') {
		if l.peekChar() == '.' {
			if isFloat {
				break // second dot, stop
			}
			isFloat = true
		}
		l.advance()
	}
	literal := l.input[start:l.pos]
	if isFloat {
		return Token{Type: TokenFloat, Literal: literal, Line: line, Col: col}
	}
	return Token{Type: TokenInteger, Literal: literal, Line: line, Col: col}
}

func (l *Lexer) readIdentOrKeyword(line, col int) Token {
	start := l.pos
	for l.pos < len(l.input) && isIdentPart(l.peekChar()) {
		l.advance()
	}
	literal := l.input[start:l.pos]
	upper := strings.ToUpper(literal)

	// Check for multi-word keywords like ARRAY_AGG, SPLIT_PART, etc.
	if tt, ok := keywords[upper]; ok {
		return Token{Type: tt, Literal: upper, Line: line, Col: col}
	}
	return Token{Type: TokenIdent, Literal: literal, Line: line, Col: col}
}

func isDigit(ch rune) bool {
	return ch >= '0' && ch <= '9'
}

func isIdentStart(ch rune) bool {
	return ch == '_' || unicode.IsLetter(ch)
}

func isIdentPart(ch rune) bool {
	return ch == '_' || unicode.IsLetter(ch) || unicode.IsDigit(ch)
}

// Input returns the original SQL input string.
func (l *Lexer) Input() string {
	return l.input
}

// IsKeyword returns true if the token type is a keyword that can also be used
// as an identifier in certain contexts (e.g., column names like "name", "time").
func IsKeyword(tt TokenType) bool {
	return tt >= TokenSelect && tt <= TokenChangelog
}
