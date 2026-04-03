package lexer

import (
	"testing"
)

func TestKeywordsRecognized(t *testing.T) {
	tests := []struct {
		input    string
		wantType TokenType
	}{
		{"SELECT", TokenSelect},
		{"DISTINCT", TokenDistinct},
		{"FROM", TokenFrom},
		{"WHERE", TokenWhere},
		{"GROUP", TokenGroup},
		{"BY", TokenBy},
		{"HAVING", TokenHaving},
		{"ORDER", TokenOrder},
		{"ASC", TokenAsc},
		{"DESC", TokenDesc},
		{"LIMIT", TokenLimit},
		{"WINDOW", TokenWindow},
		{"TUMBLING", TokenTumbling},
		{"SLIDING", TokenSliding},
		{"SESSION", TokenSession},
		{"EMIT", TokenEmit},
		{"FINAL", TokenFinal},
		{"EARLY", TokenEarly},
		{"EVENT", TokenEvent},
		{"TIME", TokenTime},
		{"WATERMARK", TokenWatermark},
		{"DEDUPLICATE", TokenDeduplicate},
		{"WITHIN", TokenWithin},
		{"CAPACITY", TokenCapacity},
		{"FORMAT", TokenFormat},
		{"DEBEZIUM", TokenDebezium},
		{"AS", TokenAs},
		{"AND", TokenAnd},
		{"OR", TokenOr},
		{"NOT", TokenNot},
		{"IN", TokenIn},
		{"BETWEEN", TokenBetween},
		{"IS", TokenIs},
		{"NULL", TokenNull},
		{"TRUE", TokenTrue},
		{"FALSE", TokenFalse},
		{"LIKE", TokenLike},
		{"ILIKE", TokenIlike},
		{"CASE", TokenCase},
		{"WHEN", TokenWhen},
		{"THEN", TokenThen},
		{"ELSE", TokenElse},
		{"END", TokenEnd},
		{"INTERVAL", TokenInterval},
		{"CAST", TokenCastKw},
		{"COUNT", TokenCount},
		{"SUM", TokenSum},
		{"AVG", TokenAvg},
		{"MIN", TokenMin},
		{"MAX", TokenMax},
		{"COALESCE", TokenCoalesce},
		{"NULLIF", TokenNullif},
		{"LENGTH", TokenLength},
		{"UPPER", TokenUpper},
		{"LOWER", TokenLower},
		{"TRIM", TokenTrim},
		{"NOW", TokenNow},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			l := New(tt.input)
			tok := l.Next()
			if tok.Type != tt.wantType {
				t.Errorf("keyword %q: got type %d, want %d", tt.input, tok.Type, tt.wantType)
			}
		})
	}
}

func TestKeywordsCaseInsensitive(t *testing.T) {
	tests := []string{"select", "Select", "sElEcT"}
	for _, input := range tests {
		l := New(input)
		tok := l.Next()
		if tok.Type != TokenSelect {
			t.Errorf("input %q: expected TokenSelect, got %d", input, tok.Type)
		}
	}
}

func TestStringLiterals(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantLit  string
		wantType TokenType
	}{
		{"simple", "'hello'", "hello", TokenString},
		{"escaped quote", "'it''s'", "it's", TokenString},
		{"empty", "''", "", TokenString},
		{"with spaces", "'hello world'", "hello world", TokenString},
		{"double escaped", "'a''b''c'", "a'b'c", TokenString},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := New(tt.input)
			tok := l.Next()
			if tok.Type != tt.wantType {
				t.Errorf("type: got %d, want %d", tok.Type, tt.wantType)
			}
			if tok.Literal != tt.wantLit {
				t.Errorf("literal: got %q, want %q", tok.Literal, tt.wantLit)
			}
		})
	}
}

func TestUnterminatedString(t *testing.T) {
	l := New("'hello")
	tok := l.Next()
	if tok.Type != TokenIllegal {
		t.Errorf("expected TokenIllegal for unterminated string, got %d", tok.Type)
	}
}

func TestNumericLiterals(t *testing.T) {
	tests := []struct {
		input    string
		wantType TokenType
		wantLit  string
	}{
		{"42", TokenInteger, "42"},
		{"0", TokenInteger, "0"},
		{"123456789", TokenInteger, "123456789"},
		{"3.14", TokenFloat, "3.14"},
		{"0.5", TokenFloat, "0.5"},
		{"100.0", TokenFloat, "100.0"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			l := New(tt.input)
			tok := l.Next()
			if tok.Type != tt.wantType {
				t.Errorf("type: got %d, want %d", tok.Type, tt.wantType)
			}
			if tok.Literal != tt.wantLit {
				t.Errorf("literal: got %q, want %q", tok.Literal, tt.wantLit)
			}
		})
	}
}

func TestOperators(t *testing.T) {
	tests := []struct {
		input    string
		wantType TokenType
		wantLit  string
	}{
		{"+", TokenPlus, "+"},
		{"-", TokenMinus, "-"},
		{"*", TokenStar, "*"},
		{"/", TokenSlash, "/"},
		{"%", TokenPercent, "%"},
		{"=", TokenEq, "="},
		{"!=", TokenNeq, "!="},
		{"<>", TokenNeq, "<>"},
		{"<", TokenLt, "<"},
		{">", TokenGt, ">"},
		{"<=", TokenLtEq, "<="},
		{">=", TokenGtEq, ">="},
		{"||", TokenConcat, "||"},
		{"::", TokenCast, "::"},
		{"->", TokenArrow, "->"},
		{"->>", TokenArrowText, "->>"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			l := New(tt.input)
			tok := l.Next()
			if tok.Type != tt.wantType {
				t.Errorf("type: got %d, want %d", tok.Type, tt.wantType)
			}
			if tok.Literal != tt.wantLit {
				t.Errorf("literal: got %q, want %q", tok.Literal, tt.wantLit)
			}
		})
	}
}

func TestPositionTracking(t *testing.T) {
	l := New("SELECT name\nFROM 'test'")
	// SELECT at 1:1
	tok := l.Next()
	if tok.Line != 1 || tok.Col != 1 {
		t.Errorf("SELECT: got %d:%d, want 1:1", tok.Line, tok.Col)
	}
	// name at 1:8
	tok = l.Next()
	if tok.Line != 1 || tok.Col != 8 {
		t.Errorf("name: got %d:%d, want 1:8", tok.Line, tok.Col)
	}
	// FROM at 2:1
	tok = l.Next()
	if tok.Line != 2 || tok.Col != 1 {
		t.Errorf("FROM: got %d:%d, want 2:1", tok.Line, tok.Col)
	}
	// 'test' at 2:6
	tok = l.Next()
	if tok.Line != 2 || tok.Col != 6 {
		t.Errorf("string: got %d:%d, want 2:6", tok.Line, tok.Col)
	}
}

func TestPunctuation(t *testing.T) {
	l := New("(,).")
	expected := []struct {
		typ TokenType
		lit string
	}{
		{TokenLParen, "("},
		{TokenComma, ","},
		{TokenRParen, ")"},
		{TokenDot, "."},
	}
	for _, e := range expected {
		tok := l.Next()
		if tok.Type != e.typ || tok.Literal != e.lit {
			t.Errorf("got (%d, %q), want (%d, %q)", tok.Type, tok.Literal, e.typ, e.lit)
		}
	}
}

func TestInvalidCharacters(t *testing.T) {
	tests := []struct {
		input   string
		wantLit string
	}{
		{"@", "@"},
		{"#", "#"},
		{"|", "|"},  // single pipe
		{":", ":"},  // single colon
		{"!", "!"},  // bare bang
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			l := New(tt.input)
			tok := l.Next()
			if tok.Type != TokenIllegal {
				t.Errorf("input %q: expected TokenIllegal, got %d", tt.input, tok.Type)
			}
		})
	}
}

func TestLineComment(t *testing.T) {
	l := New("SELECT -- this is a comment\nname")
	tok := l.Next()
	if tok.Type != TokenSelect {
		t.Fatalf("expected SELECT, got %d", tok.Type)
	}
	tok = l.Next()
	if tok.Type != TokenIdent || tok.Literal != "name" {
		t.Errorf("expected ident 'name', got (%d, %q)", tok.Type, tok.Literal)
	}
}

func TestFullSelectTokenization(t *testing.T) {
	input := "SELECT name, age WHERE age > 25"
	l := New(input)
	expected := []TokenType{
		TokenSelect, TokenIdent, TokenComma, TokenIdent,
		TokenWhere, TokenIdent, TokenGt, TokenInteger, TokenEOF,
	}
	for i, want := range expected {
		tok := l.Next()
		if tok.Type != want {
			t.Errorf("token %d: got %d, want %d (lit=%q)", i, tok.Type, want, tok.Literal)
		}
	}
}

func TestPeekAndBackup(t *testing.T) {
	l := New("SELECT name")
	tok := l.Peek()
	if tok.Type != TokenSelect {
		t.Errorf("Peek: expected SELECT, got %d", tok.Type)
	}
	// Peek should not advance
	tok = l.Peek()
	if tok.Type != TokenSelect {
		t.Errorf("second Peek: expected SELECT, got %d", tok.Type)
	}
	// Next advances
	tok = l.Next()
	if tok.Type != TokenSelect {
		t.Errorf("Next: expected SELECT, got %d", tok.Type)
	}
	tok = l.Next()
	if tok.Type != TokenIdent {
		t.Errorf("Next: expected ident, got %d", tok.Type)
	}
	// Backup
	l.Backup()
	tok = l.Next()
	if tok.Type != TokenIdent || tok.Literal != "name" {
		t.Errorf("after Backup: got (%d, %q), want ident 'name'", tok.Type, tok.Literal)
	}
}

func TestIdentifierWithUnderscore(t *testing.T) {
	l := New("_after _before _op")
	for _, want := range []string{"_after", "_before", "_op"} {
		tok := l.Next()
		if tok.Type != TokenIdent || tok.Literal != want {
			t.Errorf("got (%d, %q), want ident %q", tok.Type, tok.Literal, want)
		}
	}
}

func TestIdentifierWithDollarPrefix(t *testing.T) {
	l := New("$op $before $after $source")
	for _, want := range []string{"$op", "$before", "$after", "$source"} {
		tok := l.Next()
		if tok.Type != TokenIdent || tok.Literal != want {
			t.Errorf("got (%d, %q), want ident %q", tok.Type, tok.Literal, want)
		}
	}
}

func TestEmptyInput(t *testing.T) {
	l := New("")
	tok := l.Next()
	if tok.Type != TokenEOF {
		t.Errorf("expected EOF on empty input, got %d", tok.Type)
	}
}

func TestWhitespaceOnly(t *testing.T) {
	l := New("   \t\n  ")
	tok := l.Next()
	if tok.Type != TokenEOF {
		t.Errorf("expected EOF on whitespace-only input, got %d", tok.Type)
	}
}

func TestJsonArrowChain(t *testing.T) {
	// payload->'user'->>'email'
	l := New("payload->'user'->>'email'")
	expected := []struct {
		typ TokenType
		lit string
	}{
		{TokenIdent, "payload"},
		{TokenArrow, "->"},
		{TokenString, "user"},
		{TokenArrowText, "->>"},
		{TokenString, "email"},
	}
	for _, e := range expected {
		tok := l.Next()
		if tok.Type != e.typ || tok.Literal != e.lit {
			t.Errorf("got (%d, %q), want (%d, %q)", tok.Type, tok.Literal, e.typ, e.lit)
		}
	}
}
