# FoldDB v0 Comprehensive Test Plan

**Date:** 2026-03-28
**Status:** Draft
**Scope:** All features defined in SPEC-v0.md

---

## 1. Test Categories

| Category | Description | Minimum Cases |
|---|---|---|
| Parser | SQL clause parsing, operator precedence, error messages, edge case syntax | 38 |
| Type System | Coercion rules, NULL propagation, JSON type interactions, overflow | 24 |
| Accumulator | Every aggregate with inserts and retractions, empty groups, overflow, NULLs | 31 |
| Window | Tumbling/sliding/session windows, late data, watermark, window close, empty windows | 21 |
| Debezium | Every op type, malformed envelopes, missing before/after, schema changes | 15 |
| Dedup | LRU eviction, duplicate detection within/outside window, retraction interaction | 12 |
| Checkpoint | Crash recovery, offset mismatch, query fingerprint change, corrupted checkpoint | 10 |
| Output | TUI rendering, changelog NDJSON correctness, SQLite UPSERT correctness | 10 |
| Kafka Integration | Multi-partition ordering, rebalance, auth failures, deserialization errors | 14 |
| Performance | Throughput benchmarks, memory usage under load, high-cardinality GROUP BY | 5 |
| End-to-End | Complete queries from input to output verification | 10 |
| Failure Injection | Resilience under adversarial conditions, crash recovery, error handling | 25 |
| Property-Based | Invariants that must hold for all valid inputs (fuzzing) | 20 |

---

## 2. Test Cases

---

### 2.1 Parser Tests

### TC-PARSER-001: Basic SELECT with WHERE
```
Input:  echo '{"name":"alice","age":30}' | folddb "SELECT name WHERE age > 25"
Expected: {"name":"alice"}
```

### TC-PARSER-002: Missing closing quote in source URI
```
Input:  folddb "SELECT * FROM 'kafka://broker/topic"
Expected: Parse error with caret pointing to unclosed quote:
  SELECT * FROM 'kafka://broker/topic
                ^
  Error: unterminated string literal
```

### TC-PARSER-003: Misspelled keyword (GRUP BY)
```
Input:  folddb "SELECT region, COUNT(*) FROM 'kafka://b/t' GRUP BY region"
Expected: Error: unexpected token 'GRUP' at position XX
         Did you mean: GROUP BY?
```

### TC-PARSER-004: SELECT with all arithmetic operators
```
Input:  echo '{"a":10,"b":3}' | folddb "SELECT a + b, a - b, a * b, a / b, a % b"
Expected: {"a + b":13,"a - b":7,"a * b":30,"a / b":3.333...,"a % b":1}
```

### TC-PARSER-005: Operator precedence (multiplication before addition)
```
Input:  echo '{"a":2,"b":3,"c":4}' | folddb "SELECT a + b * c"
Expected: {"a + b * c":14}  (not 20)
```

### TC-PARSER-006: Parenthesized expression overrides precedence
```
Input:  echo '{"a":2,"b":3,"c":4}' | folddb "SELECT (a + b) * c"
Expected: {"(a + b) * c":20}
```

### TC-PARSER-007: JSON arrow operator chaining
```
Input:  echo '{"payload":{"user":{"email":"a@b.com"}}}' | folddb "SELECT payload->'user'->>'email' AS email"
Expected: {"email":"a@b.com"}
```

### TC-PARSER-008: JSON array index access
```
Input:  echo '{"items":[10,20,30]}' | folddb "SELECT items->0 AS first, items->2 AS last"
Expected: {"first":10,"last":30}
```

### TC-PARSER-009: Type cast with :: syntax
```
Input:  echo '{"val":"42"}' | folddb "SELECT val::int AS num"
Expected: {"num":42}
```

### TC-PARSER-010: CAST() function syntax
```
Input:  echo '{"val":"42"}' | folddb "SELECT CAST(val AS INT) AS num"
Expected: {"num":42}
```

### TC-PARSER-011: SELECT DISTINCT
```
Input:  printf '{"x":1}\n{"x":2}\n{"x":1}\n' | folddb "SELECT DISTINCT x"
Expected: Two output lines: {"x":1} and {"x":2} (order may vary)
```

### TC-PARSER-012: HAVING clause
```
Input:  printf '{"g":"a","v":1}\n{"g":"a","v":2}\n{"g":"b","v":1}\n' | folddb "SELECT g, COUNT(*) AS cnt GROUP BY g HAVING cnt > 1"
Expected: Only group "a" with cnt=2 appears
```

### TC-PARSER-013: ORDER BY rejected on non-accumulating streaming query
```
Input:  printf '{"x":3}\n{"x":1}\n{"x":2}\n' | folddb "SELECT x ORDER BY x DESC"
Expected: Parse error: "Error: ORDER BY is not supported on non-accumulating streaming queries. Add a GROUP BY clause or use LIMIT to make the query bounded."
Note:   Per Section 3.1, ORDER BY requires the query to be accumulating (GROUP BY) or bounded (LIMIT).
```

### TC-PARSER-014: LIMIT clause
```
Input:  Generate 100 JSON lines | folddb "SELECT x LIMIT 5"
Expected: Exactly 5 output lines, process terminates
```

### TC-PARSER-015: Column alias with AS
```
Input:  echo '{"name":"alice"}' | folddb "SELECT name AS username"
Expected: {"username":"alice"}
```

### TC-PARSER-016: IN operator with literal list
```
Input:  printf '{"s":"a"}\n{"s":"b"}\n{"s":"c"}\n' | folddb "SELECT s WHERE s IN ('a','c')"
Expected: Two lines: {"s":"a"} and {"s":"c"}
```

### TC-PARSER-017: BETWEEN operator
```
Input:  printf '{"x":1}\n{"x":5}\n{"x":10}\n' | folddb "SELECT x WHERE x BETWEEN 3 AND 8"
Expected: {"x":5}
```

### TC-PARSER-018: IS NULL and IS NOT NULL
```
Input:  printf '{"x":1}\n{"x":null}\n' | folddb "SELECT x WHERE x IS NOT NULL"
Expected: {"x":1}
```

### TC-PARSER-019: IS DISTINCT FROM (NULL-safe comparison)
```
Input:  printf '{"a":null,"b":null}\n{"a":1,"b":null}\n' | folddb "SELECT a, b WHERE a IS NOT DISTINCT FROM b"
Expected: First row only (NULL IS NOT DISTINCT FROM NULL is true)
```

### TC-PARSER-020: String concatenation with ||
```
Input:  echo '{"first":"Alice","last":"Smith"}' | folddb "SELECT first || ' ' || last AS full_name"
Expected: {"full_name":"Alice Smith"}
```

### TC-PARSER-021: LIKE pattern matching
```
Input:  printf '{"n":"alice"}\n{"n":"bob"}\n{"n":"alicia"}\n' | folddb "SELECT n WHERE n LIKE 'ali%'"
Expected: {"n":"alice"} and {"n":"alicia"}
```

### TC-PARSER-022: ILIKE case-insensitive pattern
```
Input:  printf '{"n":"Alice"}\n{"n":"BOB"}\n' | folddb "SELECT n WHERE n ILIKE 'alice'"
Expected: {"n":"Alice"}
```

### TC-PARSER-023: CASE WHEN expression
```
Input:  echo '{"x":5}' | folddb "SELECT CASE WHEN x > 3 THEN 'big' ELSE 'small' END AS label"
Expected: {"label":"big"}
```

### TC-PARSER-024: Nested CASE WHEN
```
Input:  echo '{"x":5}' | folddb "SELECT CASE WHEN x > 10 THEN 'huge' WHEN x > 3 THEN 'big' ELSE 'small' END AS label"
Expected: {"label":"big"}
```

### TC-PARSER-025: Multiple aggregates in one SELECT
```
Input:  printf '{"v":1}\n{"v":2}\n{"v":3}\n' | folddb "SELECT COUNT(*) AS c, SUM(v) AS s, AVG(v) AS a GROUP BY 1"
Expected: c=3, s=6, a=2.0 (or equivalent grouping)
```

### TC-PARSER-026: FROM clause with Kafka URI and FORMAT DEBEZIUM
```
Input:  folddb --dry-run "SELECT _op FROM 'kafka://localhost:9092/test.cdc' FORMAT DEBEZIUM"
Expected: Dry-run output shows Kafka source with Debezium format, no parse error
```

### TC-PARSER-027: WINDOW TUMBLING clause
```
Input:  folddb --dry-run "SELECT window_start, COUNT(*) FROM 'kafka://b/t' GROUP BY 1 WINDOW TUMBLING '1 minute'"
Expected: Dry-run shows tumbling window with 1 minute duration
```

### TC-PARSER-028: WINDOW SLIDING clause
```
Input:  folddb --dry-run "SELECT window_start, COUNT(*) FROM 'kafka://b/t' GROUP BY 1 WINDOW SLIDING '10 minutes' BY '5 minutes'"
Expected: Dry-run shows sliding window with 10m size and 5m slide
```

### TC-PARSER-029: DEDUPLICATE BY clause
```
Input:  folddb --dry-run "SELECT * FROM 'kafka://b/t' DEDUPLICATE BY order_id WITHIN 10 MINUTES"
Expected: Dry-run shows dedup filter with 10 minute window
```

### TC-PARSER-030: Empty SQL string
```
Input:  folddb ""
Expected: Parse error: empty query
```

### TC-PARSER-031: SQL with only whitespace
```
Input:  folddb "   "
Expected: Parse error: empty query
```

### TC-PARSER-032: Unclosed parenthesis
```
Input:  folddb "SELECT COUNT(* FROM 'kafka://b/t'"
Expected: Parse error with caret at position of missing closing paren
```

### TC-PARSER-033: Reserved keyword used as alias without quoting
```
Input:  echo '{"x":1}' | folddb "SELECT x AS select"
Expected: Parse error indicating 'select' is a reserved keyword. Suggest: "select"
```

### TC-PARSER-034: Quoted identifier as alias
```
Input:  echo '{"x":1}' | folddb 'SELECT x AS "select"'
Expected: {"select":1}
```

### TC-PARSER-035: EMIT clause parsing
```
Input:  folddb --dry-run "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY 1 WINDOW TUMBLING '1 minute' EMIT EARLY '10 seconds'"
Expected: Dry-run shows EMIT EARLY with 10 second interval
```

### TC-PARSER-036: EVENT TIME BY and WATERMARK parsing
```
Input:  folddb --dry-run "SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY 1 WINDOW TUMBLING '1 minute' EVENT TIME BY ts WATERMARK '30 seconds'"
Expected: Dry-run shows event time column and watermark duration
```

### TC-PARSER-037: Multiple WHERE conditions with AND/OR precedence
```
Input:  echo '{"a":1,"b":2,"c":3}' | folddb "SELECT a WHERE a = 1 OR b = 5 AND c = 3"
Expected: {"a":1} -- AND binds tighter, so (b=5 AND c=3) is false, but a=1 is true
```

### TC-PARSER-038: NOT operator precedence
```
Input:  echo '{"x":true,"y":false}' | folddb "SELECT x WHERE NOT x AND y"
Expected: No output (NOT binds to x only, so NOT true AND false = false)
```

---

### 2.2 Type System Tests

### TC-TYPE-001: Implicit int-to-float coercion in arithmetic
```
Input:  echo '{"a":5,"b":2.0}' | folddb "SELECT a / b AS result"
Expected: {"result":2.5} (not integer division)
```

### TC-TYPE-002: Explicit cast string to int
```
Input:  echo '{"x":"not_a_number"}' | folddb "SELECT x::int"
Expected: Runtime error or NULL depending on strictness. Error message should reference the value.
```

### TC-TYPE-003: NULL arithmetic propagation
```
Input:  echo '{"a":5,"b":null}' | folddb "SELECT a + b AS result"
Expected: {"result":null}
```

### TC-TYPE-004: NULL comparison propagation
```
Input:  echo '{"a":null}' | folddb "SELECT a WHERE a = null"
Expected: No output (NULL = NULL is NULL, not true. Should use IS NULL.)
```

### TC-TYPE-005: NULL in boolean logic (AND short-circuit)
```
Input:  echo '{"a":null,"b":true}' | folddb "SELECT a WHERE a AND b"
Expected: No output (NULL AND TRUE is NULL)
```

### TC-TYPE-006: NULL in boolean logic (OR short-circuit)
```
Input:  echo '{"a":null,"b":true}' | folddb "SELECT b WHERE a OR b"
Expected: {"b":true} (NULL OR TRUE is TRUE)
```

### TC-TYPE-007: COALESCE with NULL chain
```
Input:  echo '{"a":null,"b":null,"c":42}' | folddb "SELECT COALESCE(a, b, c) AS result"
Expected: {"result":42}
```

### TC-TYPE-008: NULLIF producing NULL
```
Input:  echo '{"a":5,"b":5}' | folddb "SELECT NULLIF(a, b) AS result"
Expected: {"result":null}
```

### TC-TYPE-009: Integer overflow behavior
```
Input:  echo '{"x":9223372036854775807}' | folddb "SELECT x + 1 AS result"
Expected: Either overflow error or wrap-around with warning. Must not silently produce wrong answer.
```

### TC-TYPE-010: Float precision edge case
```
Input:  echo '{"x":0.1,"y":0.2}' | folddb "SELECT x + y AS result"
Expected: {"result":0.30000000000000004} or similar IEEE 754 result (not exactly 0.3)
```

### TC-TYPE-011: JSON field access on non-JSON column
```
Input:  echo '{"x":"hello"}' | folddb "SELECT x->'key'"
Expected: Runtime error: cannot use -> operator on TEXT value
```

### TC-TYPE-012: JSON ->> returns TEXT type
```
Input:  echo '{"obj":{"num":42}}' | folddb "SELECT obj->>'num' AS val"
Expected: {"val":"42"} (text, not integer)
```

### TC-TYPE-013: Boolean to integer coercion should fail or warn
```
Input:  echo '{"x":true}' | folddb "SELECT x + 1"
Expected: Type error: cannot add BOOLEAN and INT
```

### TC-TYPE-014: Timestamp parsing from string
```
Input:  echo '{"ts":"2026-03-28T14:30:00Z"}' | folddb "SELECT PARSE_TIMESTAMP(ts, 'RFC3339') AS t"
Expected: Correctly parsed timestamp value
```

### TC-TYPE-015: Division by zero
```
Input (integer):  echo '{"a":10,"b":0}' | folddb "SELECT a / b AS result"
Expected: {"result":null} with warning logged: "integer division by zero, returning NULL"

Input (float):    echo '{"a":10.0,"b":0.0}' | folddb "SELECT a / b AS result"
Expected: {"result":Infinity} (IEEE 754: positive / +0.0 = +Inf)

Input (float):    echo '{"a":-10.0,"b":0.0}' | folddb "SELECT a / b AS result"
Expected: {"result":-Infinity} (IEEE 754: negative / +0.0 = -Inf)

Input (float):    echo '{"a":0.0,"b":0.0}' | folddb "SELECT a / b AS result"
Expected: {"result":NaN} (IEEE 754: 0.0 / 0.0 = NaN)
```

### TC-TYPE-016: NULL IN (1, 2, NULL) returns NULL
```
Input:  echo '{"x":null}' | folddb "SELECT x IN (1, 2, NULL) AS result"
Expected: {"result":null} (NULL compared to anything is NULL; IN with any NULL element and no match returns NULL)
```

### TC-TYPE-017: NULL BETWEEN 1 AND 10 returns NULL
```
Input:  echo '{"x":null}' | folddb "SELECT x BETWEEN 1 AND 10 AS result"
Expected: {"result":null} (NULL propagates through BETWEEN comparisons)
```

### TC-TYPE-018: NULL->'key' returns NULL (JSON access on NULL)
```
Input:  echo '{"x":null}' | folddb "SELECT x->'key' AS result"
Expected: {"result":null} (JSON arrow on NULL base returns NULL, not an error)
```

### TC-TYPE-019: col->>'key' where JSON value is `null` returns SQL NULL
```
Input:  echo '{"obj":{"key":null}}' | folddb "SELECT obj->>'key' AS result"
Expected: {"result":null} (SQL NULL, not the string "null")
```

### TC-TYPE-020: NULL::int returns NULL
```
Input:  echo '{"x":null}' | folddb "SELECT x::int AS result"
Expected: {"result":null} (casting NULL to any type yields NULL)
```

### TC-TYPE-021: ''::int is a type error (empty string is not NULL)
```
Input:  echo '{"x":""}' | folddb "SELECT x::int AS result"
Expected: Runtime error: cannot cast empty string to INT (empty string is not NULL)
```

### TC-TYPE-022: || concat with non-TEXT operand (implicit cast to TEXT)
```
Input:  echo '{"name":"alice","age":30}' | folddb "SELECT name || ' is ' || age AS result"
Expected: {"result":"alice is 30"} (age is implicitly cast to TEXT for concatenation)
```

### TC-TYPE-023: JSON ->> result used in arithmetic without cast is an error
```
Input:  echo '{"obj":{"num":42}}' | folddb "SELECT obj->>'num' + 1 AS result"
Expected: Type error: cannot add TEXT and INT. Use (obj->>'num')::int + 1.
Note:   ->> always returns TEXT; arithmetic requires explicit cast.
```

### TC-TYPE-024: TIMESTAMP compared to TEXT string (ISO 8601 parsing)
```
Input:  echo '{"ts":"2026-03-28T14:30:00Z"}' | folddb "SELECT ts WHERE ts > '2026-03-28T00:00:00Z'"
Expected: Row is returned. TEXT values in ISO 8601 format are implicitly parsed as TIMESTAMP for comparison.
```

---

### 2.3 Accumulator Tests

### TC-ACC-001: COUNT(*) with inserts only
```
Input:  printf '{"g":"a"}\n{"g":"a"}\n{"g":"b"}\n' | folddb "SELECT g, COUNT(*) AS c GROUP BY g"
Expected: Final state: g=a c=2, g=b c=1
```

### TC-ACC-002: COUNT(*) with insert then retract (Debezium update)
```
Setup:  Debezium stream: INSERT g=a, then UPDATE g=a->g=b
Input:  folddb "SELECT _after->>'g' AS g, COUNT(*) AS c FROM 'kafka://b/t' FORMAT DEBEZIUM GROUP BY g"
Expected: After insert: g=a c=1. After update: g=a c=0 (retracted), g=b c=1.
Note:   When count reaches zero, the key should be removed (no row emitted for g=a).
```

### TC-ACC-003: SUM with inserts and retractions
```
Setup:  Stream of (group=x, val=10), (group=x, val=20), retract(group=x, val=10)
Expected: SUM goes 10 -> 30 -> 20
```

### TC-ACC-004: SUM with NULL values
```
Input:  printf '{"g":"a","v":10}\n{"g":"a","v":null}\n{"g":"a","v":5}\n' | folddb "SELECT g, SUM(v) AS s GROUP BY g"
Expected: s=15 (NULL values are ignored in SUM, per SQL standard)
```

### TC-ACC-005: AVG correctness with retractions
```
Setup:  Insert (g=x, v=10), Insert (g=x, v=20), Retract (g=x, v=10)
Expected: AVG goes 10.0 -> 15.0 -> 20.0
```

### TC-ACC-006: AVG with all NULLs
```
Input:  printf '{"g":"a","v":null}\n{"g":"a","v":null}\n' | folddb "SELECT g, AVG(v) AS a GROUP BY g"
Expected: a=null (AVG of no non-NULL values is NULL)
```

### TC-ACC-007: MIN with retraction of the minimum value
```
Setup:  Insert (g=x, v=5), Insert (g=x, v=10), Insert (g=x, v=3), Retract (g=x, v=3)
Expected: MIN goes 5 -> 5 -> 3 -> 5 (falls back to next smallest)
```

### TC-ACC-008: MAX with retraction of the maximum value
```
Setup:  Insert (g=x, v=100), Insert (g=x, v=50), Retract (g=x, v=100)
Expected: MAX goes 100 -> 100 -> 50
```

### TC-ACC-009: MIN/MAX on empty group after all retractions
```
Setup:  Insert (g=x, v=10), Retract (g=x, v=10)
Expected: Group is removed entirely. MIN/MAX undefined on empty set = NULL or row removed.
```

### TC-ACC-010: COUNT(DISTINCT x) with duplicates and retractions
```
Setup:  Insert (g=x, v=1), Insert (g=x, v=2), Insert (g=x, v=1), Retract (g=x, v=1)
Expected: COUNT(DISTINCT) goes 1 -> 2 -> 2 -> 2 (one v=1 still present after retract)
         Then Retract (g=x, v=1) -> COUNT(DISTINCT) = 1 (only v=2 remains)
```

### TC-ACC-011: ARRAY_AGG insert and retract
```
Setup:  Insert (g=x, v='a'), Insert (g=x, v='b'), Retract (g=x, v='a')
Expected: ARRAY_AGG goes ['a'] -> ['a','b'] -> ['b']
```

### TC-ACC-012: FIRST(x) behavior on retraction
```
Setup:  Insert (g=x, v=1), Insert (g=x, v=2), Retract (g=x, v=1)
Expected: FIRST is defined as "first seen" and is NOT retractable per spec.
          FIRST should remain 1, or warn that retraction of first value is undefined.
```

### TC-ACC-013: LAST(x) updates on each insert
```
Setup:  Insert (g=x, v=1), Insert (g=x, v=2), Insert (g=x, v=3)
Expected: LAST goes 1 -> 2 -> 3
```

### TC-ACC-014: MEDIAN with odd and even number of values
```
Setup:  Insert v=1, Insert v=3, Insert v=5 (median=3), Insert v=7 (median=4)
Expected: Median of [1,3,5] = 3, Median of [1,3,5,7] = 4.0
```

### TC-ACC-015: MEDIAN with retraction
```
Setup:  Insert v=1, Insert v=3, Insert v=5 (median=3), Retract v=1 (median=4)
Expected: After retract: values are [3,5], median = 4.0
```

### TC-ACC-016: SUM overflow with large BIGINT values
```
Setup:  Insert (g=x, v=9223372036854775000), Insert (g=x, v=9223372036854775000)
Expected: Overflow error or warning. Must not silently wrap.
```

### TC-ACC-017: COUNT(*) with zero rows matching GROUP BY
```
Input:  printf '{"g":"a","v":1}\n' | folddb "SELECT g, COUNT(*) AS c GROUP BY g HAVING c > 10"
Expected: No output (HAVING filters out the only group)
```

### TC-ACC-018: Multiple aggregates on same group
```
Input:  printf '{"g":"a","v":10}\n{"g":"a","v":20}\n{"g":"a","v":30}\n' | folddb "SELECT g, COUNT(*) AS c, SUM(v) AS s, AVG(v) AS a, MIN(v) AS mn, MAX(v) AS mx GROUP BY g"
Expected: c=3, s=60, a=20.0, mn=10, mx=30
```

### TC-ACC-019: Retraction on accumulator with no prior state (offset=latest scenario)
```
Setup:  Debezium stream starts with op=u. _before has data but accumulator has never seen an insert for this key.
Expected: Warning logged: "retraction on empty accumulator for key X". Retraction is skipped. Insert part of update is applied.
```

### TC-ACC-020: GROUP BY on expression (not just column name)
```
Input:  printf '{"x":1}\n{"x":2}\n{"x":3}\n{"x":4}\n' | folddb "SELECT x % 2 AS parity, COUNT(*) AS c GROUP BY x % 2"
Expected: parity=0 c=2, parity=1 c=2
```

### TC-ACC-021: Single-element group retraction leaves group empty
```
Setup:  Insert (g=x, v=5), Retract (g=x, v=5)
Expected: Retraction of previous result emitted. Group key removed from state.
         For changelog: {"op":"+","g":"x","c":1} then {"op":"-","g":"x","c":1}
```

### TC-ACC-022: NULL as a GROUP BY key (all NULLs grouped together)
```
Input:  printf '{"g":null,"v":1}\n{"g":null,"v":2}\n{"g":"a","v":3}\n' | folddb "SELECT g, SUM(v) AS s GROUP BY g"
Expected: Two groups: g=null s=3, g=a s=3. All NULL keys are grouped into a single group.
```

### TC-ACC-023: SUM going negative from retractions (allowed, not clamped)
```
Setup:  Insert (g=x, v=5), Retract (g=x, v=10)
Expected: SUM goes 5 -> -5. Negative sums are allowed and not clamped to zero.
```

### TC-ACC-024: Retraction of NULL value from SUM (ignored)
```
Setup:  Insert (g=x, v=10), Retract (g=x, v=NULL)
Expected: SUM remains 10. NULL retraction is ignored, same as NULL insertion.
```

### TC-ACC-025: Double retraction (same value retracted twice) -- COUNT clamped at 0
```
Setup:  Insert (g=x, v=1), Retract (g=x, v=1), Retract (g=x, v=1)
Expected: COUNT goes 1 -> 0 -> 0 (clamped at 0, not -1).
Note:   SUM behavior: 1 -> 0 -> -1 (SUM is not clamped, per TC-ACC-023).
```

### TC-ACC-026: Insert after all values retracted (group re-created, fresh "+" emission)
```
Setup:  Insert (g=x, v=1), Retract (g=x, v=1), Insert (g=x, v=5)
Expected: Changelog: +{g=x,c=1}, -{g=x,c=1}, +{g=x,c=1}
          The final "+" is a fresh emission, not an update to a previous row.
```

### TC-ACC-027: ARRAY_AGG with NULL values (NULLs included in array)
```
Setup:  Insert (g=x, v='a'), Insert (g=x, v=NULL), Insert (g=x, v='b')
Expected: ARRAY_AGG = ['a', null, 'b']. NULLs are included in the array (not skipped).
```

### TC-ACC-028: ARRAY_AGG retraction with duplicate values (only one instance removed)
```
Setup:  Insert (g=x, v='a'), Insert (g=x, v='a'), Insert (g=x, v='b'), Retract (g=x, v='a')
Expected: ARRAY_AGG goes ['a'] -> ['a','a'] -> ['a','a','b'] -> ['a','b']
          Only one instance of 'a' is removed by the retraction.
```

### TC-ACC-029: FIRST(x) with NULL inputs (returns first non-NULL)
```
Setup:  Insert (g=x, v=NULL), Insert (g=x, v=NULL), Insert (g=x, v=42)
Expected: FIRST = 42 (first non-NULL value seen). If all values are NULL, FIRST = NULL.
```

### TC-ACC-030: LAST(x) with NULL inputs (returns most recent non-NULL)
```
Setup:  Insert (g=x, v=1), Insert (g=x, v=NULL), Insert (g=x, v=3), Insert (g=x, v=NULL)
Expected: LAST = 3 (most recent non-NULL value). NULLs do not overwrite a non-NULL LAST.
```

### TC-ACC-031: Retraction with mismatched value (undefined behavior -- document what actually happens)
```
Setup:  Insert (g=x, v=10), Retract (g=x, v=99) where 99 was never inserted.
Expected: UNDEFINED BEHAVIOR. Document actual outcome:
          - SUM: goes 10 -> -89 (arithmetic retraction applied regardless)
          - COUNT: goes 1 -> 0 (decrements by 1 regardless of value)
          - MIN/MAX: may return incorrect results
          Note: This scenario arises from bugs in upstream CDC. Log a warning.
```

---

### 2.4 Window Tests

### TC-WIN-001: Tumbling window basic assignment
```
Setup:  Events at t=00:00:10, t=00:00:50, t=00:01:10 with WINDOW TUMBLING '1 minute'
Expected: Window [00:00, 00:01) gets 2 events. Window [00:01, 00:02) gets 1 event.
```

### TC-WIN-002: Tumbling window emit on close
```
Setup:  EMIT FINAL (default). Watermark advances past window end.
Expected: Results emitted only after watermark passes window_end.
```

### TC-WIN-003: Sliding window overlapping assignment
```
Setup:  Event at t=00:07:30 with WINDOW SLIDING '10 minutes' BY '5 minutes'
Expected: Event belongs to windows [00:00,00:10) and [00:05,00:15)
```

### TC-WIN-004: Session window gap detection
```
Setup:  Events at t=0s, t=3s, t=4s, t=20s with WINDOW SESSION '5 minutes'
Expected: Session 1 = [0s, 4s] (3 events), Session 2 = [20s, 20s] (1 event)
          (Gap between 4s and 20s > 5 minutes? If gap is 5 min: 16s gap > 5s gap? Adjust per actual gap config)
```

### TC-WIN-005: Session window with '5 seconds' gap
```
Setup:  Events at t=0s, t=3s, t=4s, t=20s with WINDOW SESSION '5 seconds'
Expected: Session 1 covers events at 0s, 3s, 4s. Session 2 covers event at 20s.
```

### TC-WIN-006: Late data dropped when beyond watermark
```
Setup:  WATERMARK '10 seconds'. Watermark at t=60s. Event arrives with event_time=t=40s (20s late).
Expected: Event dropped. Dead-letter output if --dead-letter configured. Counter incremented for skipped records.
```

### TC-WIN-007: Late data within watermark tolerance
```
Setup:  WATERMARK '30 seconds'. Watermark at t=60s. Event arrives with event_time=t=45s (15s late).
Expected: Event is accepted and assigned to correct window.
```

### TC-WIN-008: EMIT EARLY periodic partial results
```
Setup:  WINDOW TUMBLING '1 minute' EMIT EARLY '10 seconds'. Events arrive continuously.
Expected: Partial aggregate results emitted every 10 seconds before window closes.
          Final result emitted on window close. Partial results retracted and replaced by final.
```

### TC-WIN-009: Empty window (no events in window period)
```
Setup:  WINDOW TUMBLING '1 minute'. No events between t=60s and t=120s.
Expected: No output for the empty window (not a row with count=0, unless explicitly designed otherwise).
```

### TC-WIN-010: Window with GROUP BY produces composite key
```
Setup:  WINDOW TUMBLING '1 minute' GROUP BY region. Events for us-east and us-west in same window.
Expected: Separate aggregation results per (window_start, region) combination.
```

### TC-WIN-011: window_start and window_end columns available in SELECT
```
Input:  folddb "SELECT window_start, window_end, COUNT(*) FROM 'kafka://b/t' GROUP BY 1 WINDOW TUMBLING '1 minute'"
Expected: Each output row includes correct window_start and window_end timestamps.
```

### TC-WIN-012: Window with retractions (Debezium + tumbling)
```
Setup:  Debezium stream into WINDOW TUMBLING '1 minute'. An update arrives: retract old + insert new.
Expected: Both the retraction and insertion are correctly assigned to their respective windows by event time.
```

### TC-WIN-013: Watermark advancement with multi-partition Kafka
```
Setup:  Partition 0 at event_time=100s, Partition 1 at event_time=80s.
Expected: Watermark = min(100, 80) - allowed_lateness. Window at 80s cannot close until partition 1 advances.
```

### TC-WIN-014: Processing time window (no EVENT TIME BY)
```
Setup:  WINDOW TUMBLING '1 minute' without EVENT TIME BY clause.
Expected: Windows assigned by FoldDB's receive time, not any data field.
```

### TC-WIN-015: Default watermark when EVENT TIME BY specified without WATERMARK
```
Setup:  EVENT TIME BY ts (no WATERMARK clause)
Expected: Default allowed lateness of 5 seconds is applied per spec.
```

### TC-WIN-016: Window with zero records after retractions
```
Setup:  WINDOW TUMBLING '1 minute'. Insert (v=10) at t=00:00:30, then Retract (v=10) at t=00:00:45.
Expected: Retraction emitted for the previous result of the window. No "+" emission follows.
          The window closes with no active records.
```

### TC-WIN-017: Session window gap exactly equals session timeout (exclusive boundary)
```
Setup:  WINDOW SESSION '5 seconds'. Event at t=0s, event at t=5s.
Expected: Two separate sessions. Gap of exactly 5 seconds starts a new session (exclusive boundary).
          Session 1 = [0s, 0s], Session 2 = [5s, 5s].
```

### TC-WIN-018: Tumbling window boundary alignment to UTC epoch
```
Setup:  WINDOW TUMBLING '1 hour'. Event at t=2026-03-28T14:37:00Z.
Expected: Event assigned to window [14:00:00Z, 15:00:00Z).
          Window boundaries align to UTC epoch (hour 0, not relative to first event).
```

### TC-WIN-019: EMIT EARLY with no data in window (no emission)
```
Setup:  WINDOW TUMBLING '1 minute' EMIT EARLY '10 seconds'. No events arrive during [00:00, 00:01).
Expected: No emission for the empty window, even at EMIT EARLY intervals.
```

### TC-WIN-020: EMIT EARLY interval shorter than input rate
```
Setup:  WINDOW TUMBLING '1 minute' EMIT EARLY '5 seconds'. Events arrive every 1 second.
Expected: Every 5 seconds, a retraction/insertion pair is emitted with updated partial aggregate.
          Each emission retracts the previous partial result and inserts the new one.
```

### TC-WIN-021: Late data within watermark re-opens a closed window
```
Setup:  WATERMARK '30 seconds'. Window [00:00, 00:01) has closed.
        Late event arrives with event_time=00:00:50 (within watermark tolerance).
Expected: Event is accepted. The closed window is re-opened, updated aggregate emitted.
          A retraction of the previous final result is emitted, followed by a new "+" with the updated result.
```

---

### 2.5 Debezium Tests

### TC-DBZ-001: Create event (op=c)
```
Input:  {"op":"c","before":null,"after":{"id":1,"name":"alice"},"source":{"db":"mydb","table":"users"}}
Expected: Single record emitted with diff=+1. _after accessible. _before is null. _op = 'c'.
```

### TC-DBZ-002: Update event (op=u) produces retract + insert
```
Input:  {"op":"u","before":{"id":1,"name":"alice"},"after":{"id":1,"name":"alice2"},"source":{"db":"mydb","table":"users"}}
Expected: Two internal records: (_before, diff=-1) then (_after, diff=+1).
```

### TC-DBZ-003: Delete event (op=d)
```
Input:  {"op":"d","before":{"id":1,"name":"alice"},"after":null,"source":{"db":"mydb","table":"users"}}
Expected: Single record with diff=-1 from _before. _after is null.
```

### TC-DBZ-004: Snapshot/read event (op=r)
```
Input:  {"op":"r","before":null,"after":{"id":1,"name":"alice"},"source":{"db":"mydb","table":"users"}}
Expected: Single record with diff=+1. Treated same as create.
```

### TC-DBZ-005: Update with missing "before" field
```
Input:  {"op":"u","after":{"id":1,"name":"alice2"},"source":{"db":"mydb","table":"users"}}
Note:   Some Debezium configs have REPLICA IDENTITY DEFAULT which omits "before" on updates.
Expected: Warning logged. Insert part (_after, +1) is processed. Retraction skipped (no _before to retract).
          This may cause accumulator drift -- warning should indicate this.
```

### TC-DBZ-006: Delete with missing "before" field
```
Input:  {"op":"d","after":null,"source":{"db":"mydb","table":"users"}}
Expected: Warning or error: cannot process delete without "before" data. Record sent to dead-letter if configured.
```

### TC-DBZ-007: Malformed envelope (missing "op" field)
```
Input:  {"before":null,"after":{"id":1},"source":{"db":"mydb","table":"users"}}
Expected: Deserialization error. Record skipped. Count of skipped records incremented.
```

### TC-DBZ-008: Malformed envelope (op is unknown value)
```
Input:  {"op":"x","before":null,"after":{"id":1},"source":{}}
Expected: Error: unknown Debezium op 'x'. Record skipped.
```

### TC-DBZ-009: Virtual column _table populated from source block
```
Input:  {"op":"c","after":{"id":1},"source":{"db":"mydb","table":"orders","ts_ms":1711641600000}}
Query:  SELECT _table, _db, _ts FROM ... FORMAT DEBEZIUM
Expected: _table='orders', _db='mydb', _ts=<parsed from ts_ms>
```

### TC-DBZ-010: _source virtual column contains full source JSON
```
Input:  {"op":"c","after":{"id":1},"source":{"db":"mydb","table":"orders","lsn":"0/1234","txId":42}}
Query:  SELECT _source->>'lsn' AS lsn FROM ... FORMAT DEBEZIUM
Expected: lsn='0/1234'
```

### TC-DBZ-011: Debezium tombstone record (null value, key only)
```
Input:  Kafka record with non-null key, null value (Debezium tombstone).
Expected: Record skipped gracefully with debug-level log. Not an error.
```

### TC-DBZ-012: Schema change mid-stream (new column appears)
```
Setup:  First 10 events have {id, name}. Then ALTER TABLE adds "email" column.
        Next events have {id, name, email}.
Expected: After schema change, _after includes "email" field. Queries referencing email work.
          Queries not referencing email are unaffected. No crash.
```

### TC-DBZ-013: Schema change mid-stream (column removed)
```
Setup:  First events have {id, name, email}. Column "email" is dropped.
Expected: After change, _after->>'email' returns NULL for new events. No crash.
```

### TC-DBZ-014: Debezium envelope with nested source types
```
Input:  {"op":"c","after":{"id":1,"metadata":{"tags":["a","b"],"nested":{"deep":true}}},"source":{"db":"mydb","table":"t"}}
Query:  SELECT _after->'metadata'->'nested'->>'deep' AS val FROM ... FORMAT DEBEZIUM
Expected: val='true'
```

### TC-DBZ-015: High-frequency updates to same key (rapid retract/insert cycles)
```
Setup:  100 consecutive updates (op=u) to the same key, each changing a value.
Expected: Accumulator correctly processes all 100 retract/insert pairs.
          Final state reflects the last update only. No state leak.
```

---

### 2.6 Dedup Tests

### TC-DEDUP-001: Basic deduplication within window
```
Setup:  DEDUPLICATE BY order_id WITHIN 10 MINUTES.
        Send order_id=1 at t=0, order_id=1 at t=5m.
Expected: Second record is dropped.
```

### TC-DEDUP-002: Dedup key expires after window
```
Setup:  DEDUPLICATE BY order_id WITHIN 10 MINUTES.
        Send order_id=1 at t=0, order_id=1 at t=15m.
Expected: Both records pass through (key expired after 10 minutes).
```

### TC-DEDUP-003: LRU eviction under memory pressure
```
Setup:  DEDUPLICATE BY id WITHIN 1 HOUR. Send 10 million distinct IDs.
Expected: Oldest entries are evicted from LRU when cache exceeds limit.
          Log warning about eviction. Evicted keys may produce duplicates.
```

### TC-DEDUP-004: Dedup happens before WHERE filter
```
Setup:  DEDUPLICATE BY id WITHIN 5 MINUTES. WHERE status = 'active'.
        Send (id=1, status='inactive'), then (id=1, status='active').
Expected: Second record is dropped by dedup BEFORE WHERE evaluates.
          The active record never reaches the output.
```

### TC-DEDUP-005: Dedup with NULL key
```
Setup:  DEDUPLICATE BY nullable_field WITHIN 5 MINUTES.
        Send two records where nullable_field is NULL.
Expected: Both pass through (NULL is not equal to NULL) OR both are deduped (NULL treated as a single key).
          Behavior must be documented and consistent.
```

### TC-DEDUP-006: Dedup interaction with Debezium retractions
```
Setup:  DEDUPLICATE BY order_id WITHIN 10 MINUTES on a Debezium stream.
        A create (op=c, order_id=1) arrives, then an update (op=u, order_id=1).
Expected: The update's retraction (diff=-1) for order_id=1 should NOT be deduped.
          Retractions must pass through dedup, or dedup must only apply to inserts.
          This is a critical semantic question.
```

### TC-DEDUP-007: Dedup with composite key expression
```
Setup:  DEDUPLICATE BY (user_id || '-' || session_id) WITHIN 30 MINUTES.
Expected: Dedup key is the evaluated expression, not a single column.
```

### TC-DEDUP-008: Dedup state checkpointed and restored
```
Setup:  --stateful with DEDUPLICATE BY id WITHIN 10 MINUTES.
        Send id=1, checkpoint, restart, send id=1 within window.
Expected: After restart, dedup state is restored. Second id=1 is still dropped.
```

### TC-DEDUP-009: Kafka infrastructure-level dedup (same offset redelivered)
```
Setup:  Consumer rebalance causes partition 0 offset 100 to be delivered twice.
Expected: Offset-based dedup (not user-level DEDUPLICATE BY) handles this automatically.
          No duplicate in output.
```

### TC-DEDUP-010: WITHIN duration parsing edge cases
```
Input:  DEDUPLICATE BY id WITHIN 0 SECONDS
Expected: Parse error or validation error: dedup window must be positive.
```

### TC-DEDUP-011: Dedup with NULL key (NULLs treated as equal)
```
Setup:  DEDUPLICATE BY nullable_col WITHIN 5 MINUTES.
        Send record with nullable_col=NULL at t=0, another with nullable_col=NULL at t=2m.
Expected: Second record is dropped. NULL keys are treated as equal for dedup purposes.
Note:   This resolves the ambiguity in TC-DEDUP-005 -- NULLs ARE treated as a single key.
```

### TC-DEDUP-012: Retractions bypass dedup (diff=-1 always passes through)
```
Setup:  DEDUPLICATE BY order_id WITHIN 10 MINUTES on a Debezium stream.
        Insert (order_id=1, diff=+1), then retraction (order_id=1, diff=-1).
Expected: Both records pass through. Retractions (diff=-1) are never subject to dedup filtering.
          Only insertions (diff=+1) are checked against the dedup cache.
```

---

### 2.7 Checkpoint Tests

### TC-CKPT-001: Basic checkpoint and restore
```
Setup:  --stateful. Process 1000 records from Kafka. Kill process. Restart.
Expected: On restart, resumes from checkpointed offsets. No duplicate processing of the 1000 records.
          Output from offset 1001 onward only.
```

### TC-CKPT-002: Query fingerprint mismatch on restart
```
Setup:  Run query A with --stateful, checkpoint. Change query to B, restart with --stateful.
Expected: Warning: "query fingerprint mismatch. Checkpoint is for query A, but running query B."
          Starts fresh from configured offset (not from checkpoint offsets).
```

### TC-CKPT-003: --force-replay discards old state
```
Setup:  Run query, checkpoint. Restart with --force-replay.
Expected: Old checkpoint discarded. Starts from offset=earliest (or configured offset).
```

### TC-CKPT-004: Corrupted checkpoint file (truncated)
```
Setup:  Manually truncate the checkpoint file mid-way.
Expected: FoldDB detects corruption on startup (checksum mismatch or parse error).
          Warning logged. Starts fresh.
```

### TC-CKPT-005: Crash during checkpoint flush (simulated)
```
Setup:  Kill process during the temp-file write phase of checkpoint.
Expected: On restart, the temp file is ignored (atomic rename never happened).
          Previous valid checkpoint is used. No data loss.
```

### TC-CKPT-006: Checkpoint with accumulator state (SUM, COUNT)
```
Setup:  GROUP BY region with SUM and COUNT. Process records, checkpoint.
        Restart, process more records.
Expected: Accumulator state is correctly restored. SUM and COUNT continue from checkpointed values.
```

### TC-CKPT-007: Checkpoint with proportional-state accumulators (MIN, MAX, MEDIAN)
```
Setup:  GROUP BY with MEDIAN. 1000 values per group. Checkpoint and restore.
Expected: All 1000 values per group are serialized and restored.
          MEDIAN returns correct result after restore + new data.
```

### TC-CKPT-008: Checkpoint interval configuration
```
Setup:  --checkpoint-interval 1s. Verify checkpoint file is updated approximately every 1 second.
Expected: Checkpoint file modification time updates at configured cadence.
```

### TC-CKPT-009: folddb state list / inspect / reset subcommands
```
Setup:  Run a --stateful query. Then run:
        folddb state list -> shows the query hash
        folddb state inspect <hash> -> shows metadata (query, offsets, keys, state size, last flush)
        folddb state reset <hash> -> deletes the checkpoint
Expected: All subcommands produce correct output. After reset, next run starts fresh.
```

### TC-CKPT-010: Multi-partition offset tracking
```
Setup:  3-partition topic. Process records from all partitions. Checkpoint.
        Restart, verify each partition resumes from its own checkpointed offset.
Expected: Partition 0 resumes from offset X0, partition 1 from X1, partition 2 from X2.
```

---

### 2.8 Output Tests

### TC-OUT-001: TTY detection triggers TUI mode for accumulating query
```
Setup:  Run accumulating query (GROUP BY) in a real TTY.
Expected: TUI renders table in-place (like `top`). Updates overwrite previous output.
```

### TC-OUT-002: Pipe detection triggers changelog NDJSON for accumulating query
```
Setup:  folddb "SELECT g, COUNT(*) GROUP BY g" | cat
Expected: Output is changelog NDJSON with {"op":"+","g":"a","count":1} lines.
```

### TC-OUT-003: Changelog retract-then-insert ordering
```
Setup:  Accumulating query. Group key "a" changes from count=5 to count=6.
Expected: Output lines in order:
          {"op":"-","g":"a","count":5}
          {"op":"+","g":"a","count":6}
```

### TC-OUT-004: Non-accumulating query omits op field
```
Setup:  Non-accumulating query piped.
Expected: Plain NDJSON without "op" field: {"name":"alice","age":30}
```

### TC-OUT-005: --mode changelog forces changelog even in TTY
```
Setup:  Accumulating query in TTY with --mode changelog.
Expected: Changelog NDJSON output, not TUI.
```

### TC-OUT-006: SQLite state store with accumulating query
```
Setup:  --state orders.db with GROUP BY region, SUM(total).
Expected: SQLite file created. Table "result" exists.
          PRIMARY KEY is the GROUP BY columns (region).
          Values updated via UPSERT as stream progresses.
          WAL mode enabled (concurrent reads OK).
```

### TC-OUT-007: SQLite state store with non-accumulating query
```
Setup:  --state log.db with non-accumulating query.
Expected: Records INSERTed (append-only). _rowid and _ingested_at columns added.
```

### TC-OUT-008: SQLite concurrent read during write
```
Setup:  Terminal 1: folddb --state test.db "...GROUP BY..."
        Terminal 2: sqlite3 test.db "SELECT * FROM result"
Expected: Terminal 2 can read while Terminal 1 is writing (WAL mode).
```

### TC-OUT-009: Changelog consumer reconstructs current state
```
Setup:  Pipe accumulating query output to a script that maintains a map by GROUP BY keys.
        Apply every "+" and "-" operation.
Expected: At any point, the map matches what the TUI would display.
```

### TC-OUT-010: --format csv output
```
Setup:  echo '{"a":1,"b":"hello"}' | folddb --format csv "SELECT a, b"
Expected: CSV output with header row: a,b\n1,hello
```

---

### 2.9 Kafka Integration Tests

### TC-KAFKA-001: Multi-partition consumption
```
Setup:  Topic with 4 partitions. Produce 100 records per partition.
Expected: All 400 records consumed. No duplicates. No missing records.
```

### TC-KAFKA-002: offset=earliest starts from beginning
```
Setup:  Topic with existing data. Query with offset=earliest.
Expected: All existing records processed before any new ones.
```

### TC-KAFKA-003: offset=latest skips existing data
```
Setup:  Topic with existing data. Query with offset=latest.
Expected: Only records produced after query starts are processed.
```

### TC-KAFKA-004: offset=<timestamp> seeks to timestamp
```
Setup:  Topic with records spanning 1 hour. Query with offset=<30-minutes-ago>.
Expected: Only records from the last 30 minutes are processed.
```

### TC-KAFKA-005: SASL/PLAIN authentication
```
Setup:  Kafka broker with SASL/PLAIN. Correct credentials in URI.
Expected: Successful connection and consumption.
```

### TC-KAFKA-006: Authentication failure
```
Setup:  Kafka broker with SASL/PLAIN. Wrong password.
Expected: Clear error: "authentication failed for kafka://broker:9092 -- SASL handshake error: invalid credentials"
```

### TC-KAFKA-007: Broker unreachable
```
Setup:  Kafka URI points to non-existent broker.
Expected: Error within timeout: "cannot connect to kafka://badhost:9092 -- connection refused"
```

### TC-KAFKA-008: Deserialization error mid-stream (malformed JSON)
```
Setup:  100 records, record #50 is invalid JSON: {"trunca
Expected: Records 1-49 processed. Record 50 skipped with error logged.
          Records 51-100 processed. Skipped count = 1 in stats.
```

### TC-KAFKA-009: Virtual columns populated correctly
```
Setup:  Produce record with key="mykey" to partition 2 at known offset.
Expected: _key='mykey', _partition=2, _offset=<expected>, _timestamp=<kafka timestamp>
```

### TC-KAFKA-010: Consumer group offset commit
```
Setup:  Query with group=mygroup. Process 1000 records. Stop. Check committed offset.
Expected: Committed offset >= 1000 for each partition.
```

### TC-KAFKA-011: Non-UTF-8 Kafka message key (b64: prefix encoding)
```
Setup:  Produce a record with a binary (non-UTF-8) key to Kafka.
Query:  SELECT _key FROM ...
Expected: _key is returned with "b64:" prefix followed by base64-encoded key bytes.
          e.g., _key='b64:gAAAAQ==' for binary key 0x80000001.
```

### TC-KAFKA-012: Schema registry unreachable at startup (3 retries then error)
```
Setup:  Query references Kafka topic with schema registry. Registry is unreachable.
Expected: 3 retry attempts with backoff. After 3 failures:
          Error: "schema registry unreachable at <url> after 3 attempts". Process exits.
```

### TC-KAFKA-013: Schema registry unreachable mid-stream (record routed to dead-letter)
```
Setup:  Schema registry becomes unreachable after processing starts (new schema ID encountered).
Expected: Records requiring schema lookup are routed to dead-letter topic/file.
          Processing continues for records with already-cached schemas.
          Warning logged: "schema registry unreachable, routing record to dead-letter".
```

### TC-KAFKA-014: Stalled partition excluded from watermark calculation after 30s
```
Setup:  3-partition topic. Partition 2 stops receiving messages for > 30 seconds.
Expected: After 30s of inactivity on partition 2, watermark calculation excludes it.
          Watermark advances based on min(partition 0, partition 1) only.
          When partition 2 resumes, it is re-included in watermark calculation.
```

---

### 2.10 Performance Tests

### TC-PERF-001: Throughput benchmark - simple filter/project
```
Target:  >= 200,000 records/sec (per spec)
Setup:   1M records on a Kafka topic, simple SELECT col1, col2 WHERE col3 > threshold
Measure: Records per second from start to LIMIT reached
Pass:    >= 200K rec/s on a standard 4-core machine
```

### TC-PERF-002: Throughput benchmark - GROUP BY with COUNT/SUM
```
Setup:   1M records, GROUP BY on column with 1000 distinct values, COUNT(*) + SUM(amount)
Measure: Records per second for accumulation
Pass:    >= 100K rec/s (accumulation overhead expected)
```

### TC-PERF-003: Memory usage - high cardinality GROUP BY with O(1) accumulators
```
Setup:   10M distinct GROUP BY keys, COUNT(*) + SUM(amount) only
Measure: Peak RSS memory usage
Pass:    < 500MB (approximately 50 bytes per key for 2 O(1) accumulators)
```

### TC-PERF-004: Memory usage - high cardinality GROUP BY with O(n) accumulators
```
Setup:   100K distinct GROUP BY keys, MEDIAN(amount) with 100 values per key
Measure: Peak RSS memory usage
Pass:    < 2GB. Memory warning logged when threshold exceeded.
```

### TC-PERF-005: Checkpoint serialization/deserialization speed
```
Setup:   1M GROUP BY keys with SUM/COUNT state. Checkpoint flush.
Measure: Time to serialize and write checkpoint. Time to restore on startup.
Pass:    Flush < 2 seconds. Restore < 5 seconds.
```

---

### 2.11 End-to-End Tests

### TC-E2E-001: stdin JSON filter end-to-end
```
Input:   echo '{"name":"alice","age":30}' | folddb "SELECT name WHERE age > 25"
Expected: {"name":"alice"}
Verify:  Exit code 0. Exactly one line of output.
```

### TC-E2E-002: stdin JSON aggregation with multiple groups
```
Input:   printf '{"g":"a","v":1}\n{"g":"b","v":2}\n{"g":"a","v":3}\n' | folddb "SELECT g, SUM(v) AS total GROUP BY g"
Expected: Changelog output showing final state: g=a total=4, g=b total=2
```

### TC-E2E-003: Kafka -> Debezium -> accumulating -> changelog NDJSON
```
Setup:   Kafka topic with Debezium CDC. INSERT 3 rows, UPDATE 1, DELETE 1.
Query:   SELECT _after->>'region' AS region, COUNT(*) FROM ... FORMAT DEBEZIUM GROUP BY region
Expected: Correct count per region reflecting all inserts, the update retraction, and the delete retraction.
```

### TC-E2E-004: Kafka -> windowed aggregation -> SQLite state
```
Setup:   Kafka topic with timestamped events.
Query:   --state metrics.db windowed tumbling 1 minute with COUNT and AVG.
Expected: SQLite "result" table contains one row per (window_start, group_key).
          Values match expected aggregation.
```

### TC-E2E-005: --dry-run shows query plan without execution
```
Input:   folddb --dry-run "SELECT region, COUNT(*) FROM 'kafka://b/t' FORMAT DEBEZIUM GROUP BY region"
Expected: Printed query plan showing: source=kafka, format=debezium, operators=[filter, project, accumulate].
          No Kafka connection attempted. Exit code 0.
```

### TC-E2E-006: --explain shows plan then executes
```
Input:   echo '{"x":1}' | folddb --explain "SELECT x"
Expected: Query plan printed to stderr, then results to stdout.
```

### TC-E2E-007: -f flag reads query from file
```
Setup:   Write "SELECT name WHERE age > 25" to /tmp/query.sql
Input:   echo '{"name":"alice","age":30}' | folddb -f /tmp/query.sql
Expected: {"name":"alice"}
```

### TC-E2E-008: --timeout terminates after duration
```
Input:   yes '{"x":1}' | folddb --timeout 2s "SELECT x"
Expected: Process terminates after approximately 2 seconds. Exit code 0.
```

### TC-E2E-009: LIMIT terminates after N output records
```
Input:   yes '{"x":1}' | folddb "SELECT x LIMIT 5"
Expected: Exactly 5 lines of output. Process terminates. Exit code 0.
```

### TC-E2E-010: Dead letter output captures bad records
```
Setup:   Mix of valid and invalid JSON on stdin. --dead-letter errors.ndjson.
Input:   printf '{"x":1}\nbad json\n{"x":2}\n' | folddb --dead-letter /tmp/errors.ndjson "SELECT x"
Expected: stdout has {"x":1} and {"x":2}.
          /tmp/errors.ndjson has one entry with error context for "bad json".
```

---

## 3. Failure Injection Scenarios

These scenarios test system resilience under adversarial conditions. Each should be tested and the behavior documented.

### FI-001: Kafka broker goes down mid-query
```
Setup:   Start consuming from Kafka. After 1000 records, kill the broker.
Expected: FoldDB detects connection loss within session timeout.
          Error message: "kafka connection lost: broker unavailable".
          If --stateful, checkpoint is flushed before exit (best effort).
          On restart with --stateful, resumes from last checkpoint.
```

### FI-002: Malformed JSON in middle of a stream
```
Setup:   Stream of 1000 JSON records. Record #500 is truncated: {"key":"val
Expected: Record #500 is skipped. Processing continues with #501.
          Error logged to stderr. Dead-letter captures the bad record if configured.
          Final stats show 999 processed, 1 skipped.
```

### FI-003: Clock skew between Kafka partitions
```
Setup:   Partition 0 has events with timestamps 10 minutes ahead of Partition 1.
         WINDOW TUMBLING '1 minute' with EVENT TIME BY.
Expected: Watermark advances based on the MINIMUM across partitions.
          Partition 1's slower timestamps hold back the watermark.
          Windows close correctly (not prematurely based on partition 0 alone).
```

### FI-004: Accumulator state exceeds --memory-limit
```
Setup:   --memory-limit 50MB. GROUP BY on 10M distinct keys with MEDIAN (O(n) state).
Expected: Warning logged when 50MB threshold reached.
          v0: processing continues (no spill to disk).
          Warning includes current state size and suggestion to reduce cardinality.
```

### FI-005: SQLite state file is locked by another process
```
Setup:   --state test.db. Another process holds an exclusive lock on test.db.
Expected: Error on startup: "cannot open state file test.db: database is locked".
          Does not hang indefinitely. Times out within a reasonable period.
```

### FI-006: Checkpoint file is corrupted (partial write simulated by writing garbage)
```
Setup:   Overwrite checkpoint file with random bytes.
Expected: On restart, FoldDB detects corruption.
          Warning: "checkpoint corrupted, starting fresh".
          Processing begins from scratch (or configured offset).
```

### FI-007: Debezium sends an update with no "before" field (REPLICA IDENTITY DEFAULT)
```
Setup:   Postgres table with default replica identity. Debezium sends op=u with before=null.
Expected: Warning: "update without before data -- retraction skipped, accumulator may drift".
          The _after record is still processed as an insert (+1).
          Accumulator state may be incorrect (acknowledged limitation).
```

### FI-008: Event time column is a string that cannot be parsed as a timestamp
```
Setup:   EVENT TIME BY created_at. Record has created_at="not-a-date".
Expected: Record skipped or processed with processing time fallback.
          Error logged with the unparseable value.
          Dead letter if configured.
```

### FI-009: GROUP BY on a JSON field that changes type mid-stream
```
Setup:   GROUP BY payload->>'user_id'. First 100 records: user_id is a string "123".
         Next 100 records: user_id is a number 123 (without quotes in JSON).
Expected: Since ->> returns TEXT, both should produce "123" as text.
          If using -> instead, the JSON types differ (string vs number).
          The system must not crash. Behavior (coerce or error) must be deterministic.
```

### FI-010: Consumer group rebalance during processing
```
Setup:   Two FoldDB instances in same consumer group on a 4-partition topic.
         Kill one instance, triggering rebalance.
Expected: Surviving instance picks up orphaned partitions.
          If --stateful, the surviving instance does NOT have the other's checkpoint.
          It replays from Kafka committed offsets for the reassigned partitions.
          No duplicate output for records already committed by the dead instance.
```

### FI-011: Kafka topic does not exist
```
Setup:   Query references a non-existent topic.
Expected: Error: "topic 'nonexistent' does not exist on kafka://broker:9092".
          Not a hang. Not an infinite retry loop.
```

### FI-012: stdin is closed immediately (empty input)
```
Setup:   echo -n "" | folddb "SELECT x"
Expected: Graceful exit. No output. Exit code 0 (or specific "no input" code).
```

### FI-013: Very long JSON field value (10MB single string)
```
Setup:   JSON record with a 10MB string value in one field.
Expected: Record is processed (or skipped with size warning).
          Does not cause OOM for a single record.
```

### FI-014: Rapid checkpoint + crash loop
```
Setup:   --checkpoint-interval 100ms. Process starts, checkpoints, crashes, restarts in a loop 100 times.
Expected: Each restart correctly loads the last valid checkpoint.
          No checkpoint file corruption from rapid cycles.
          Temp files from incomplete flushes are cleaned up.
```

### FI-015: Kafka record with null value (not Debezium tombstone)
```
Setup:   Plain Kafka JSON format (not Debezium). Record value is null.
Expected: Record skipped. Error: "null record value at partition X offset Y".
```

### FI-016: Unicode and special characters in field names and values
```
Setup:   {"emoji_field_\ud83d\ude00":"value","normal":"\u0000null_byte"}
Expected: Field names with unicode handled correctly.
          Null bytes in values do not cause truncation or crashes.
```

### FI-017: Disk full during checkpoint write
```
Setup:   --stateful with disk nearly full. Checkpoint flush fails.
Expected: Error logged: "checkpoint flush failed: no space left on device".
          Processing continues (checkpoint is best-effort).
          Previous valid checkpoint remains intact (atomic write pattern).
```

### FI-018: Timezone mismatch in event time parsing
```
Setup:   EVENT TIME BY ts. Records have ts in "2026-03-28T14:30:00+05:00" (not UTC).
Expected: Correctly converted to UTC internally.
          Window assignment uses UTC-normalized time.
```

### FI-019: Schema registry unreachable at startup (3 retries, backoff, exit)
```
Setup:   Query requires schema registry. Registry is down before query starts.
Expected: 3 retry attempts with exponential backoff (1s, 2s, 4s).
          After all retries exhausted: "schema registry unreachable after 3 attempts".
          Process exits with non-zero exit code. No partial processing occurs.
```

### FI-020: Schema registry new schema ID mid-stream fails to fetch (record to dead-letter)
```
Setup:   Schema registry is up. Mid-stream, a new schema ID appears in a record.
         Fetching the new schema fails (registry returns 500).
Expected: Record with unknown schema ID is routed to dead-letter.
          Processing continues for records with cached schema IDs.
          Periodic retry to fetch the new schema (not per-record retry).
```

### FI-021: Broker unreachable at startup with exponential backoff verification
```
Setup:   Kafka broker is unreachable at startup.
Expected: Retry attempts with exponential backoff: 1s, 2s, 4s, 8s, capped at 30s.
          Each attempt logged: "attempt N: cannot connect to kafka://broker:9092".
          Process eventually exits after max retry duration with clear error.
```

### FI-022: Stalled Kafka partition (no messages for 60s) -- verify watermark advancement excludes it after 30s
```
Setup:   3-partition topic with EVENT TIME BY. Partition 1 stalls (no messages for 60s).
         Partitions 0 and 2 continue receiving events.
Expected: For the first 30s, watermark is held back by partition 1.
          After 30s of inactivity, partition 1 is excluded from watermark calculation.
          Watermark advances based on partitions 0 and 2 only.
          Log warning: "partition 1 stalled for 30s, excluding from watermark".
```

### FI-023: Dead letter under heavy error rate (50% malformed records) -- verify processing continues
```
Setup:   Stream of 1000 records, 500 malformed (invalid JSON or schema mismatch).
         --dead-letter configured.
Expected: 500 good records processed successfully.
          500 bad records routed to dead-letter.
          Processing does not slow down disproportionately.
          Stats show: processed=500, dead-lettered=500.
```

### FI-024: Division by zero mid-stream in aggregation expression -- verify NULL result, processing continues
```
Setup:   Accumulating query: SELECT g, SUM(a) / SUM(b) AS ratio GROUP BY g.
         Group "x" receives records where SUM(b) reaches 0 via retractions.
Expected: ratio for group "x" becomes NULL (integer division by zero -> NULL with warning).
          Processing continues. Other groups are unaffected.
          Warning logged once per group per zero-crossing, not per record.
```

### FI-025: EVENT TIME BY on non-existent column -- verify parse-time or startup error
```
Setup:   Query: SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY 1 WINDOW TUMBLING '1 minute' EVENT TIME BY nonexistent_col
Expected: Error at parse time or startup (before processing begins):
          "Error: EVENT TIME BY column 'nonexistent_col' not found in input schema".
          For schemaless sources (plain JSON), error on first record that lacks the column.
```

---

## 4. Property-Based Test Definitions

These properties should ALWAYS hold for any valid input sequence. They are suitable for fuzzing with tools like `go-fuzz` or `rapid`.

### PBT-001: Insert/Retract Net Zero Invariant
```
Property: For any sequence of inserts and retractions that net to zero for every
          group key, all accumulators must return to their initial state (empty/zero).
Generator: Random sequence of (key, value, +1) and (key, value, -1) pairs where
           every +1 is eventually matched by a -1 with the same key and value.
Verify:   After processing the full sequence, the accumulator map is empty.
          No keys remain. No residual state.
```

### PBT-002: Changelog Reconstruction Equivalence
```
Property: The changelog output, when applied sequentially to an empty map
          (insert on "+", delete on "-"), ALWAYS produces the same result set
          as the TUI would display at that moment.
Generator: Random sequence of input records (inserts and Debezium updates/deletes).
Verify:   After every N records, snapshot the TUI state and the changelog-reconstructed
          state. They must be identical.
```

### PBT-003: Checkpoint Restore Equivalence
```
Property: For any query and input sequence, checkpoint restore + replay of remaining
          records produces IDENTICAL output to a fresh run from offset 0.
Generator: Random input sequence of length N. Checkpoint at random position K.
Verify:   Compare output of (full run) vs (run K records, checkpoint, restore, run N-K records).
          Output must be byte-identical.
```

### PBT-004: Filter Commutativity with Diff
```
Property: Filtering a retraction (diff=-1) of a record R produces a retraction
          if and only if R would have passed the filter as an insert (diff=+1).
Generator: Random WHERE predicate + random records with random diff values.
Verify:   filter(R, diff=+1) passes iff filter(R, diff=-1) passes.
```

### PBT-005: Accumulator Commutativity
```
Property: The order in which inserts arrive for a single group key does not affect
          the final accumulator result (for commutative aggregates: COUNT, SUM, AVG, MIN, MAX).
Generator: Fixed set of values. Random permutations of insert order.
Verify:   All permutations produce the same final Result().
```

### PBT-006: Accumulator Retraction Inverse
```
Property: For any accumulator, Add(v) followed by Retract(v) is equivalent to
          never having called Add(v) at all.
Generator: Random initial state (sequence of Add calls), then Add(v), then Retract(v).
Verify:   State after Add+Retract equals state before Add+Retract.
          Applies to: COUNT, SUM, AVG, MIN, MAX, MEDIAN, ARRAY_AGG, COUNT(DISTINCT).
```

### PBT-007: Window Assignment Completeness
```
Property: Every record with a valid timestamp is assigned to exactly the correct
          set of windows. No record is lost, no record is double-counted within
          a single non-overlapping window type (tumbling).
Generator: Random timestamps within a time range. Random window configurations.
Verify:   For tumbling: each record is in exactly 1 window.
          For sliding: each record is in exactly ceil(window_size / slide_size) windows.
          For session: each record is in exactly 1 session.
```

### PBT-008: Watermark Monotonicity
```
Property: The watermark never decreases. Once the watermark advances to time T,
          it never goes back to T' < T.
Generator: Random multi-partition event streams with varying rates.
Verify:   Record watermark after each input record. Assert monotonically non-decreasing.
```

### PBT-009: SQL Parse Roundtrip
```
Property: For any valid SQL string accepted by the parser, converting the AST
          back to a SQL string and re-parsing it produces an equivalent AST.
Generator: Random valid SQL strings from a grammar-based generator.
Verify:   parse(sql) == parse(unparse(parse(sql)))
```

### PBT-010: Dedup Idempotence
```
Property: Sending the same record twice through DEDUPLICATE BY within the window
          always produces exactly one output record. Sending it again after the
          window expires produces exactly one more.
Generator: Random keys, random timestamps, random WITHIN durations.
Verify:   Count of output records matches expected (1 per unique key per window).
```

### PBT-011: NULL Propagation Consistency
```
Property: Any arithmetic or comparison expression involving NULL produces NULL
          (except IS NULL, IS NOT NULL, IS DISTINCT FROM, COALESCE, and boolean
          short-circuit cases), consistent with PostgreSQL three-valued logic.
Generator: Random expressions with random NULL placements.
Verify:   Result matches PostgreSQL behavior for the same expression.
```

### PBT-012: Changelog Compaction Correctness
```
Property: At any point in a changelog stream, the set of "active" rows (those with
          a "+" not yet retracted by a "-") has at most one entry per GROUP BY key.
Generator: Any accumulating query with any input sequence.
Verify:   Parse changelog output. At no point does a key have two active "+" entries
          without an intervening "-".
```

### PBT-013: Serialization Roundtrip for All Accumulator Types
```
Property: For any accumulator in any state, Marshal() followed by Unmarshal()
          produces an accumulator with identical Result().
Generator: Random sequence of Add/Retract calls for each accumulator type.
Verify:   acc.Result() == unmarshal(acc.Marshal()).Result()
          Also verify: further Add/Retract calls produce identical results on both.
```

### PBT-014: Memory Usage Bounded for O(1) Accumulators
```
Property: For COUNT, SUM, and AVG accumulators, memory usage per group key is
          constant regardless of the number of records processed.
Generator: Process N records (N = 1, 100, 10000, 1000000) for a single group key.
Verify:   len(acc.Marshal()) is constant across all N values.
```

### PBT-015: Changelog Retraction-Insertion Atomicity
```
Property: Between any "-" emission and its corresponding "+" emission for the same
          GROUP BY key, no other key's emissions appear in the changelog output.
          Retraction/insertion pairs are atomic in the output stream.
Generator: Random accumulating queries with multiple group keys and concurrent updates.
Verify:   Parse changelog output sequentially. For every "-" for key K, the very next
          line must be a "+" for the same key K. No interleaving allowed.
```

### PBT-016: HAVING Filter Correctness Under Retraction
```
Property: If a group's aggregate value drops below the HAVING threshold due to a
          retraction, a retraction ("-") is emitted for that group's previous result.
Generator: Random HAVING predicates. Random insert/retract sequences that cause groups
           to cross the HAVING threshold in both directions.
Verify:   At every point, the set of active "+" rows in changelog all satisfy HAVING.
          Every group that transitions from satisfying to not satisfying HAVING produces a "-".
```

### PBT-017: Session Window Merge Commutativity
```
Property: For any set of events and a session timeout, the final set of sessions is
          the same regardless of the order in which events arrive.
Generator: Fixed set of (key, timestamp) pairs. Random permutations of arrival order.
Verify:   All permutations produce the same final session boundaries and member counts.
```

### PBT-018: Debezium Diff Derivation Correctness
```
Property: For any valid Debezium event sequence, the emitted diff values match the
          spec's op-to-diff mapping: op=c -> +1, op=r -> +1, op=u -> (-1, +1),
          op=d -> -1.
Generator: Random valid Debezium event sequences (create, update, delete, read).
Verify:   For each event, the emitted diff(s) exactly match the spec mapping table.
          Updates always produce exactly one retraction followed by one insertion.
```

### PBT-019: ORDER BY Stability for Accumulating Queries
```
Property: At every emission point for an accumulating query with ORDER BY, the output
          rows are sorted according to the ORDER BY specification.
Generator: Random accumulating queries with ORDER BY and random input sequences.
Verify:   After each emission batch, verify the "+" rows in current state are in the
          correct order. Ties are stable (original insertion order preserved).
```

### PBT-020: LIMIT Termination
```
Property: LIMIT N always produces exactly N output records (or fewer if input is
          exhausted) and then terminates the query.
Generator: Random queries with LIMIT N for various N. Input streams of varying lengths.
Verify:   Output count = min(N, total_input_matching_filter). Process exits after
          emitting the Nth record. No further records are read from input.
```

---

## 5. Test Infrastructure Requirements

### 5.1 Unit Test Framework
- Standard Go `testing` package with table-driven tests
- `testify/assert` for readable assertions
- `rapid` or `gopter` for property-based tests

### 5.2 Integration Test Framework
- `testcontainers-go` for Kafka + Schema Registry containers
- Dedicated Debezium test container with Postgres source
- Temp directories for checkpoint and SQLite state files

### 5.3 Performance Test Framework
- Go benchmarks (`testing.B`) for microbenchmarks
- Custom harness for end-to-end throughput measurement
- `pprof` integration for memory profiling during TC-PERF-003/004

### 5.4 Test Data Generators
- JSON record generator with configurable schema and cardinality
- Debezium envelope generator (create/update/delete sequences)
- Timestamp generator with configurable clock skew and out-of-order delivery
- Kafka producer helper for integration tests

---

*End of Test Plan*
