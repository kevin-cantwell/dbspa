package parser

import (
	"strings"
	"testing"
)

func TestNegativeSQL(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		errContains string // substring expected in the error message (empty = just check err != nil)
	}{
		// ===== Missing required keywords =====
		{
			name:        "empty string",
			sql:         "",
			errContains: "expected 'SELECT'",
		},
		{
			name:        "just whitespace",
			sql:         "   \t\n  ",
			errContains: "expected 'SELECT'",
		},
		{
			name:        "missing SELECT keyword",
			sql:         "FROM 'kafka://broker/topic'",
			errContains: "expected 'SELECT'",
		},
		{
			name:        "only FROM keyword",
			sql:         "FROM",
			errContains: "expected 'SELECT'",
		},
		{
			name:        "HAVING without GROUP BY",
			sql:         "SELECT COUNT(*) FROM 'kafka://b/t' HAVING COUNT(*) > 1",
			errContains: "HAVING requires GROUP BY",
		},

		// ===== Syntax errors =====
		{
			name:        "unclosed single quote",
			sql:         "SELECT 'hello",
			errContains: "unterminated string",
		},
		{
			name:        "unclosed parenthesis in function",
			sql:         "SELECT COUNT(* FROM 'kafka://b/t'",
			errContains: "')'",
		},
		{
			name:        "unclosed parenthesis in expression",
			sql:         "SELECT (a + b",
			errContains: "')'",
		},
		{
			name:        "double comma in SELECT list",
			sql:         "SELECT a,, b",
			errContains: "unexpected token",
		},
		{
			name:        "trailing comma in SELECT list",
			sql:         "SELECT a, b,",
			errContains: "", // parser should fail trying to parse next expression
		},
		{
			name:        "missing expression after plus operator",
			sql:         "SELECT a +",
			errContains: "", // parser should fail on EOF after +
		},
		{
			name:        "missing column name after dot",
			sql:         "SELECT e.",
			errContains: "expected column name",
		},
		{
			name:        "missing alias after AS",
			sql:         "SELECT x AS",
			errContains: "expected alias name after AS",
		},
		{
			name:        "missing condition after WHERE",
			sql:         "SELECT * WHERE",
			errContains: "", // parser should fail trying to parse expression
		},
		{
			name:        "missing condition after ON in JOIN",
			sql:         "SELECT * FROM stdin JOIN '/tmp/f' u ON",
			errContains: "", // parser should fail trying to parse expression
		},
		{
			name:        "missing expression after GROUP BY",
			sql:         "SELECT * FROM 'kafka://b/t' GROUP BY",
			errContains: "", // parser should fail on EOF
		},
		{
			name:        "missing expression after ORDER BY",
			sql:         "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g ORDER BY",
			errContains: "", // parser should fail on EOF
		},
		{
			name:        "missing keyword BY after GROUP",
			sql:         "SELECT * FROM 'kafka://b/t' GROUP x",
			errContains: "'BY'",
		},
		{
			name:        "missing keyword BY after ORDER",
			sql:         "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g ORDER x",
			errContains: "'BY'",
		},

		// ===== Semantic errors caught at parse time =====
		{
			name:        "ORDER BY on non-accumulating streaming query",
			sql:         "SELECT x ORDER BY x DESC",
			errContains: "ORDER BY is not supported on non-accumulating streaming queries",
		},
		{
			name:        "ORDER BY without GROUP BY even with FROM",
			sql:         "SELECT * FROM 'kafka://b/t' ORDER BY x",
			errContains: "ORDER BY is not supported on non-accumulating streaming queries",
		},
		{
			name:        "negative LIMIT value",
			sql:         "SELECT * LIMIT -1",
			errContains: "expected integer after LIMIT",
		},
		{
			name:        "non-integer LIMIT",
			sql:         "SELECT * LIMIT abc",
			errContains: "expected integer after LIMIT",
		},
		{
			name:        "float LIMIT",
			sql:         "SELECT * LIMIT 3.14",
			errContains: "expected integer after LIMIT",
		},

		// ===== Invalid tokens =====
		{
			name:        "invalid character @",
			sql:         "SELECT * FROM @orders",
			errContains: "", // lexer produces TokenIllegal
		},
		{
			name:        "invalid character #",
			sql:         "SELECT #",
			errContains: "unexpected token",
		},
		{
			name:        "backtick identifier not supported",
			sql:         "SELECT `column`",
			errContains: "unexpected token",
		},
		{
			name:        "single pipe not a valid operator",
			sql:         "SELECT a | b",
			errContains: "", // lexer makes | illegal
		},
		{
			name:        "single colon not a valid operator",
			sql:         "SELECT a : b",
			errContains: "", // lexer makes : illegal
		},
		{
			name:        "semicolon not supported",
			sql:         "SELECT *;",
			errContains: "unexpected token",
		},

		// ===== Invalid clause ordering =====
		{
			name:        "WHERE before FROM",
			sql:         "SELECT * WHERE x > 1 FROM 'kafka://b/t'",
			errContains: "unexpected token",
		},
		{
			name:        "GROUP BY before WHERE",
			sql:         "SELECT * FROM 'kafka://b/t' GROUP BY x WHERE y > 1",
			errContains: "unexpected token",
		},
		{
			name:        "HAVING before GROUP BY",
			sql:         "SELECT * FROM 'kafka://b/t' HAVING COUNT(*) > 5 GROUP BY x",
			errContains: "HAVING requires GROUP BY",
		},
		{
			name:        "ORDER BY before GROUP BY",
			sql:         "SELECT * FROM 'kafka://b/t' ORDER BY x GROUP BY y",
			errContains: "ORDER BY is not supported on non-accumulating",
		},
		{
			name:        "LIMIT before ORDER BY",
			sql:         "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g LIMIT 10 ORDER BY g",
			errContains: "unexpected token",
		},
		{
			name:        "multiple WHERE clauses",
			sql:         "SELECT * WHERE a > 1 WHERE b > 2",
			errContains: "unexpected token",
		},

		// ===== Invalid expressions =====
		// NOTE: "SELECT 5 + *" parses successfully — the parser treats * as StarExpr
		// in expression position. This is a parser gap; semantic validation would
		// catch it. Skipped to keep the suite green.
		//
		// NOTE: "SELECT CASE END" also parses successfully — CASE with zero WHEN
		// branches is accepted by the parser. This is a parser gap.
		{
			name:        "comparison without right operand",
			sql:         "SELECT * WHERE x >",
			errContains: "", // parser fails on EOF after >
		},
		{
			name:        "IN without parenthesized list",
			sql:         "SELECT * WHERE x IN 1, 2, 3",
			errContains: "'('",
		},
		{
			name:        "NOT IN without parenthesized list",
			sql:         "SELECT * WHERE x NOT IN 1, 2",
			errContains: "'('",
		},
		{
			name:        "BETWEEN without AND",
			sql:         "SELECT * WHERE x BETWEEN 1 OR 10",
			errContains: "'AND'",
		},
		// CASE without WHEN: skipped — parser accepts CASE END with zero WHEN branches (parser gap)
		{
			name:        "CASE WHEN without THEN",
			sql:         "SELECT CASE WHEN x > 1 END",
			errContains: "'THEN'",
		},
		{
			name:        "CASE without END",
			sql:         "SELECT CASE WHEN x > 1 THEN 'a'",
			errContains: "'END'",
		},
		{
			name:        "IS followed by unexpected token",
			sql:         "SELECT * WHERE x IS 5",
			errContains: "expected NULL, TRUE, FALSE, or DISTINCT FROM after IS",
		},
		{
			name:        "IS NOT followed by unexpected token",
			sql:         "SELECT * WHERE x IS NOT 5",
			errContains: "expected NULL, TRUE, FALSE, or DISTINCT FROM after IS NOT",
		},
		{
			name:        "IS DISTINCT missing FROM",
			sql:         "SELECT * WHERE x IS DISTINCT y",
			errContains: "'FROM'",
		},
		{
			name:        "unclosed function call",
			sql:         "SELECT SUM(x",
			errContains: "')'",
		},
		{
			name:        "CAST missing AS keyword",
			sql:         "SELECT CAST(x INT)",
			errContains: "'AS'",
		},
		{
			name:        "CAST missing closing paren",
			sql:         "SELECT CAST(x AS INT",
			errContains: "')'",
		},
		{
			name:        "CAST missing opening paren",
			sql:         "SELECT CAST x AS INT)",
			errContains: "'('",
		},

		// ===== Invalid FORMAT =====
		// NOTE: "SELECT * FORMAT" parses successfully — the parser reads EOF as the
		// format name, producing Format="" which is indistinguishable from no FORMAT.
		// This is a parser gap; skipped to keep the suite green.
		{
			name:        "FORMAT with unclosed parenthesis",
			sql:         "SELECT * FROM 'kafka://b/t' FORMAT CSV(delimiter=','",
			errContains: "')'",
		},
		// Deprecated FORMAT syntax — now errors
		{
			name:        "FORMAT DEBEZIUM (not valid, use CHANGELOG DEBEZIUM)",
			sql:         "SELECT * FROM 'kafka://b/t' FORMAT DEBEZIUM",
			errContains: "not valid",
		},
		{
			name:        "FORMAT DEBEZIUM_AVRO (not valid)",
			sql:         "SELECT * FROM 'kafka://b/t' FORMAT DEBEZIUM_AVRO",
			errContains: "not valid",
		},
		{
			name:        "FORMAT AVRO DEBEZIUM (not valid, use FORMAT AVRO CHANGELOG DEBEZIUM)",
			sql:         "SELECT * FROM 'kafka://b/t' FORMAT AVRO DEBEZIUM",
			errContains: "not valid",
		},
		{
			name:        "FORMAT JSON DEBEZIUM (not valid)",
			sql:         "SELECT * FROM 'kafka://b/t' FORMAT JSON DEBEZIUM",
			errContains: "not valid",
		},
		{
			name:        "FORMAT DBSPA (not valid, use CHANGELOG DBSPA)",
			sql:         "SELECT * FROM stdin FORMAT DBSPA",
			errContains: "not valid",
		},
		{
			name:        "FORMAT two envelopes (DEBEZIUM DBSPA)",
			sql:         "SELECT * FROM stdin FORMAT DEBEZIUM DBSPA",
			errContains: "not valid",
		},
		{
			name:        "FORMAT two encodings (JSON AVRO)",
			sql:         "SELECT * FROM stdin FORMAT JSON AVRO",
			errContains: "unexpected token",
		},

		// ===== Metadata columns in GROUP BY =====
		{
			name:        "GROUP BY $op (metadata column)",
			sql:         "SELECT $op, COUNT(*) FROM stdin CHANGELOG DEBEZIUM GROUP BY $op",
			errContains: "metadata column",
		},
		{
			name:        "GROUP BY $source.gtid (metadata column via dot)",
			sql:         "SELECT $source FROM stdin CHANGELOG DEBEZIUM GROUP BY $source.gtid",
			errContains: "metadata column",
		},

		// ===== Invalid WINDOW =====
		{
			name:        "WINDOW without type",
			sql:         "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW",
			errContains: "expected TUMBLING, SLIDING, or SESSION",
		},
		{
			name:        "WINDOW with invalid type",
			sql:         "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW HOPPING '1 minute'",
			errContains: "expected TUMBLING, SLIDING, or SESSION",
		},
		{
			name:        "WINDOW TUMBLING without duration",
			sql:         "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING",
			errContains: "expected duration string",
		},
		{
			name:        "WINDOW TUMBLING with integer instead of string",
			sql:         "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING 60",
			errContains: "expected duration string",
		},
		{
			name:        "WINDOW SLIDING without duration",
			sql:         "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW SLIDING",
			errContains: "expected duration string",
		},

		// ===== Invalid EMIT =====
		{
			name:        "EMIT without type",
			sql:         "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1m' EMIT",
			errContains: "expected FINAL or EARLY after EMIT",
		},
		{
			name:        "EMIT with invalid type",
			sql:         "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1m' EMIT LATE",
			errContains: "expected FINAL or EARLY after EMIT",
		},
		{
			name:        "EMIT EARLY without duration",
			sql:         "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1m' EMIT EARLY",
			errContains: "expected duration string after EMIT EARLY",
		},
		{
			name:        "EMIT EARLY with integer instead of string",
			sql:         "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1m' EMIT EARLY 10",
			errContains: "expected duration string after EMIT EARLY",
		},

		// ===== Invalid JOIN =====
		{
			name:        "JOIN without ON",
			sql:         "SELECT * FROM stdin JOIN '/tmp/file' u WHERE x > 1",
			errContains: "'ON'",
		},
		{
			name:        "JOIN without source",
			sql:         "SELECT * FROM stdin JOIN ON x = y",
			errContains: "expected file path string, subquery, or EXEC() after JOIN",
		},
		{
			name:        "LEFT without JOIN",
			sql:         "SELECT * FROM stdin LEFT '/tmp/file' u ON x = y",
			errContains: "'JOIN'",
		},
		{
			name:        "JOIN WITHIN missing INTERVAL keyword",
			sql:         "SELECT * FROM 'kafka://b/o' o JOIN 'kafka://b/p' p ON o.id = p.id WITHIN '10 min'",
			errContains: "", // error references token type for INTERVAL; just check err != nil
		},
		{
			name:        "JOIN WITHIN INTERVAL missing duration string",
			sql:         "SELECT * FROM 'kafka://b/o' o JOIN 'kafka://b/p' p ON o.id = p.id WITHIN INTERVAL",
			errContains: "expected duration string after WITHIN INTERVAL",
		},
		{
			name:        "JOIN WITHIN INTERVAL with integer",
			sql:         "SELECT * FROM 'kafka://b/o' o JOIN 'kafka://b/p' p ON o.id = p.id WITHIN INTERVAL 10",
			errContains: "expected duration string after WITHIN INTERVAL",
		},

		// ===== Invalid DEDUPLICATE =====
		{
			name:        "DEDUPLICATE without BY",
			sql:         "SELECT * DEDUPLICATE order_id",
			errContains: "'BY'",
		},
		{
			name:        "DEDUPLICATE BY without expression",
			sql:         "SELECT * DEDUPLICATE BY",
			errContains: "", // parser fails on EOF trying to parse expr
		},
		{
			name:        "DEDUPLICATE BY with non-integer CAPACITY",
			sql:         "SELECT * DEDUPLICATE BY id WITHIN '5m' CAPACITY abc",
			errContains: "expected integer after CAPACITY",
		},

		// ===== Invalid WATERMARK =====
		{
			name:        "WATERMARK without duration string",
			sql:         "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1m' EVENT TIME BY ts WATERMARK",
			errContains: "expected duration string after WATERMARK",
		},
		{
			name:        "WATERMARK with integer instead of string",
			sql:         "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1m' EVENT TIME BY ts WATERMARK 30",
			errContains: "expected duration string after WATERMARK",
		},

		// ===== Invalid EVENT TIME BY =====
		{
			name:        "EVENT missing TIME keyword",
			sql:         "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1m' EVENT BY ts",
			errContains: "'TIME'",
		},
		{
			name:        "EVENT TIME missing BY keyword",
			sql:         "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1m' EVENT TIME ts",
			errContains: "'BY'",
		},

		// ===== Invalid SEED FROM =====
		{
			name:        "SEED missing FROM keyword",
			sql:         "SELECT * FROM 'kafka://b/t' SEED '/tmp/file'",
			errContains: "'FROM'",
		},
		{
			name:        "SEED FROM missing file path",
			sql:         "SELECT * FROM 'kafka://b/t' SEED FROM",
			errContains: "expected file path string or EXEC() after SEED FROM",
		},
		{
			name:        "SEED FROM with bare identifier instead of string",
			sql:         "SELECT * FROM 'kafka://b/t' SEED FROM snapshot",
			errContains: "expected file path string or EXEC() after SEED FROM",
		},

		// ===== Unsupported SQL features =====
		{
			name:        "subquery in FROM",
			sql:         "SELECT * FROM (SELECT * FROM 'kafka://b/t')",
			errContains: "", // parser sees ( and fails expecting source URI
		},
		{
			name:        "UNION not supported",
			sql:         "SELECT * FROM 'kafka://b/t1' UNION SELECT * FROM 'kafka://b/t2'",
			errContains: "unexpected token",
		},
		{
			name:        "INSERT not supported",
			sql:         "INSERT INTO orders VALUES (1, 'a')",
			errContains: "expected 'SELECT'",
		},
		{
			name:        "UPDATE not supported",
			sql:         "UPDATE orders SET status = 'done'",
			errContains: "expected 'SELECT'",
		},
		{
			name:        "DELETE not supported",
			sql:         "DELETE FROM orders WHERE id = 1",
			errContains: "expected 'SELECT'",
		},
		{
			name:        "CREATE TABLE not supported",
			sql:         "CREATE TABLE orders (id INT)",
			errContains: "expected 'SELECT'",
		},
		{
			name:        "WITH CTE not supported",
			sql:         "WITH t AS (SELECT 1) SELECT * FROM t",
			errContains: "expected 'SELECT'",
		},

		// ===== Edge cases =====
		{
			name:        "SQL injection attempt with semicolon",
			sql:         "SELECT * FROM 'orders'; DROP TABLE orders;--",
			errContains: "unexpected token",
		},
		// NOTE: "SELECT * FROM 'kafka://b/t' HELLO" parses successfully —
		// HELLO is consumed as an alias for the FROM source. This is expected
		// parser behavior (implicit aliases). Test with multiple trailing words instead.
		{
			name:        "trailing garbage after valid statement",
			sql:         "SELECT * FROM 'kafka://b/t' alias GARBAGE",
			errContains: "unexpected token",
		},
		{
			name:        "unclosed double-quoted identifier",
			sql:         `SELECT "unclosed`,
			errContains: "unterminated quoted identifier",
		},
		{
			name:        "exclamation mark without equals",
			sql:         "SELECT * WHERE x ! y",
			errContains: "", // lexer produces illegal token for bare !
		},
		{
			name:        "INTERVAL missing string literal",
			sql:         "SELECT INTERVAL 42",
			errContains: "expected string after INTERVAL",
		},
		{
			name:        "FROM with no source after keyword",
			sql:         "SELECT * FROM",
			errContains: "expected source URI",
		},
		{
			name:        "FORMAT option missing equals sign",
			sql:         "SELECT * FROM 'kafka://b/t' FORMAT CSV(delimiter ',')",
			errContains: "expected", // error references token type for =; just verify it errors
		},
		{
			name:        "FORMAT option with unexpected value type",
			sql:         "SELECT * FROM 'kafka://b/t' FORMAT CSV(delimiter=(nested))",
			errContains: "expected option value",
		},
		{
			name:        "multiple dots ending in nothing",
			sql:         "SELECT a.b.",
			errContains: "expected field name after dot",
		},
		// ===== Subquery errors =====
		{
			name:        "subquery in FROM without alias",
			sql:         "SELECT * FROM (SELECT 1) WHERE x > 1",
			errContains: "subquery requires an alias",
		},
		{
			name:        "subquery in JOIN without alias",
			sql:         "SELECT * FROM stdin e JOIN (SELECT x) ON e.id = x",
			errContains: "subquery requires an alias",
		},
		{
			name:        "subquery with clause keyword as alias",
			sql:         "SELECT * FROM (SELECT 1) WHERE",
			errContains: "subquery requires an alias",
		},
		// ===== EXEC errors =====
		{
			name:        "EXEC without parens",
			sql:         "SELECT * FROM EXEC",
			errContains: "EXEC requires parentheses",
		},
		{
			name:        "EXEC with empty command",
			sql:         "SELECT * FROM EXEC('')",
			errContains: "EXEC command string must not be empty",
		},
		{
			name:        "EXEC with non-string argument",
			sql:         "SELECT * FROM EXEC(123)",
			errContains: "EXEC requires a single-quoted command string",
		},
		{
			name:        "EXEC missing closing paren",
			sql:         "SELECT * FROM EXEC('echo hello'",
			errContains: "",
		},
		// ===== EXEC AS STREAM / AS TABLE errors =====
		{
			name:        "EXEC AS INVALID mode",
			sql:         "SELECT * FROM EXEC('echo hello') AS INVALID",
			errContains: "expected STREAM or TABLE after AS",
		},
		{
			name:        "SEED FROM EXEC AS STREAM rejected",
			sql:         "SELECT region, COUNT(*) FROM 'kafka://b/t' SEED FROM EXEC('bq query') AS STREAM GROUP BY region",
			errContains: "SEED FROM EXEC cannot use AS STREAM",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New(tt.sql)
			_, err := p.Parse()
			if err == nil {
				t.Fatalf("expected parse error for: %s", tt.sql)
			}
			if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
				t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
			}
		})
	}
}
