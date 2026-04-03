# DBSPA Rough Edges Report

Adversarial testing performed 2026-03-28 against the `dev` build.

---

## P0 - CRASH

### 1. Panic: non-GROUP-BY column in SELECT with aggregate causes nil pointer dereference

**Category:** CRASH
**Severity:** P0

When SELECT includes a column that is neither an aggregate function nor listed in GROUP BY, the engine panics with a nil pointer dereference in `changelog.go:35`.

The root cause is in `aggregate.go` `buildResult()`: it tracks non-aggregate columns with a `keyIdx` counter, but columns not in GROUP BY exceed the `keyValues` slice bounds, leaving them as nil `Value` in the map. When the changelog sink calls `.ToJSON()` on a nil `Value`, it panics.

```bash
# Reproducer 1: column c not in GROUP BY (a, b)
printf '{"a":1,"b":2,"c":3}\n{"a":1,"b":2,"c":4}' | go run ./cmd/dbspa "SELECT a, b, c, COUNT(*) AS cnt GROUP BY a, b"
# panic: runtime error: invalid memory address or nil pointer dereference

# Reproducer 2: column b not in GROUP BY (a)
printf '{"a":1,"b":2}\n{"a":1,"b":3}' | go run ./cmd/dbspa "SELECT a, b, COUNT(*) AS cnt GROUP BY a"
# panic: runtime error: invalid memory address or nil pointer dereference

# Reproducer 3: non-aggregate column ordered before GROUP BY key
printf '{"a":1,"b":2}\n{"a":1,"b":3}' | go run ./cmd/dbspa "SELECT b, a, COUNT(*) AS cnt GROUP BY a"
# panic: runtime error: invalid memory address or nil pointer dereference
```

**Expected:** Either error at parse/plan time ("column b must appear in GROUP BY or be used in an aggregate function") or use FIRST/LAST semantics for non-grouped columns. Standard SQL would reject this query.

---

## P1 - BUG (Wrong Results)

### 2. Aggregate functions without GROUP BY produce no output

**Category:** BUG
**Severity:** P1

Queries like `SELECT COUNT(*), SUM(v), AVG(v)` with no GROUP BY clause should aggregate all input rows into a single group and emit one result row. Instead, DBSPA treats the query as non-accumulating (filter/project mode), and since COUNT/SUM/AVG are not recognized as projectable expressions in that path, no output is produced.

```bash
printf '{"v":1}\n{"v":2}\n{"v":3}' | go run ./cmd/dbspa "SELECT COUNT(*), SUM(v), AVG(v)"
# Expected: {"COUNT":3,"SUM":6,"AVG":2}
# Actual: (no output)

# --dry-run confirms the engine classifies this as "Non-accumulating":
go run ./cmd/dbspa --dry-run "SELECT COUNT(*), SUM(v)" </dev/null
# Type: Non-accumulating (filter/project)
```

**Expected:** The engine should detect aggregate functions in SELECT and implicitly create a single-group aggregation, even without an explicit GROUP BY.

**Workaround:** `GROUP BY 1` (group by constant).

### 3. LIMIT 0 emits one row instead of zero

**Category:** BUG
**Severity:** P1

`LIMIT 0` should return zero rows, but it emits the first row before the limit check kicks in.

```bash
echo '{"x":1}' | go run ./cmd/dbspa "SELECT x LIMIT 0"
# Expected: (no output)
# Actual: {"x":1}

printf '{"x":1}\n{"x":2}\n{"x":3}\n{"x":4}\n{"x":5}' | go run ./cmd/dbspa "SELECT x LIMIT 0"
# Expected: (no output)
# Actual: {"x":1}
```

**Root cause:** In `main.go` `runNonAccumulatingFromRecords`, the limit check happens *after* writing the record: `snk.Write(rec)` then `count++` then `if limit != nil && count >= *limit`. When limit is 0, the first record is written before count (now 1) is checked against 0.

### 4. ORDER BY silently ignored on accumulating queries

**Category:** BUG
**Severity:** P1

ORDER BY on accumulating (GROUP BY) queries parses successfully but is silently ignored. Records are emitted in insertion/arrival order.

```bash
printf '{"g":"b","v":2}\n{"g":"a","v":1}\n{"g":"c","v":3}' | go run ./cmd/dbspa "SELECT g, SUM(v) AS total GROUP BY g ORDER BY total DESC"
# Expected output order: c(3), b(2), a(1)
# Actual output order: b(2), a(1), c(3) (arrival order)
```

**Expected:** Either apply ORDER BY to the final output, or reject it at parse time with a clear error message explaining that streaming accumulating queries emit in arrival order.

### 5. Duplicate column aliases produce duplicate JSON keys

**Category:** BUG
**Severity:** P1

When multiple SELECT columns resolve to the same alias (e.g., two COUNT expressions without explicit aliases both default to "COUNT"), the output JSON contains duplicate keys. Most JSON parsers will silently use only the last value, causing data loss.

```bash
printf '{"a":1}\n{"a":2}' | go run ./cmd/dbspa "SELECT COUNT(*), COUNT(a) GROUP BY 1"
# Output: {"op":"+","COUNT":2,"COUNT":2}
# The two COUNT columns both alias to "COUNT", producing duplicate JSON keys.
# COUNT(*) = 2, COUNT(a) = 2 happen to match here, but with NULLs they diverge.

printf '{"a":1,"b":null}\n{"a":null,"b":2}\n{"a":3,"b":3}' | go run ./cmd/dbspa "SELECT COUNT(*), COUNT(a), COUNT(b), SUM(a), SUM(b) GROUP BY 1"
# Output: {"op":"+","COUNT":0,"COUNT":0,"COUNT":0,"SUM":null,"SUM":null}
# Five columns but only two unique keys in JSON. Data is lost.
```

**Expected:** Auto-deduplicate aliases (e.g., `COUNT`, `COUNT_2`, `COUNT_3`) or error when aliases collide.

### 6. GROUP BY without any aggregate function produces no output

**Category:** BUG
**Severity:** P1

A query with GROUP BY but no aggregate function in SELECT produces zero output, even though it should conceptually group and emit unique keys.

```bash
printf '{"a":1,"b":2}\n{"a":1,"b":3}\n{"a":2,"b":4}' | go run ./cmd/dbspa "SELECT a, b GROUP BY a"
# Expected: either an error or results with first/last value of b per group
# Actual: (no output at all)
```

**Root cause:** The `isGroupEmpty` check in `aggregate.go` looks for a `COUNT(*)` accumulator at zero. When there are no aggregate columns, there's no `COUNT(*)` to check, so `isGroupEmpty` returns false. However, without any aggregate columns, `anyChanged` (line 122-126) is always false because only aggregate accumulators are checked. So the function returns early on every record and never emits.

### 7. HAVING without GROUP BY is silently ignored

**Category:** BUG
**Severity:** P1

HAVING is only meaningful with GROUP BY. When used on a non-accumulating query, it is silently ignored rather than producing an error.

```bash
echo '{"x":1}' | go run ./cmd/dbspa "SELECT x HAVING x > 100"
# Expected: error "HAVING requires GROUP BY"
# Actual: {"x":1} (HAVING ignored, row passes through)
```

### 8. Chained type cast x::int::float outputs integer, not float

**Category:** BUG
**Severity:** P1

`x::int::float` should produce a float value, but the JSON output shows an integer.

```bash
echo '{"x":"123"}' | go run ./cmd/dbspa "SELECT x::int::float AS result"
# Expected: {"result":123.0}  (or at least a float in the engine)
# Actual: {"result":123}
```

This is because `FloatValue.ToJSON()` returns `float64(123)` which JSON marshals as `123` (no decimal point). The cast is correct internally, but the output loses the type information. This can cause issues for downstream consumers that need to distinguish int from float.

---

## P2 - UX (Confusing Behavior)

### 9. DISTINCT keyword is silently ignored (parsed as column name)

**Category:** UX
**Severity:** P2

`SELECT DISTINCT x` does not deduplicate. The parser treats `DISTINCT` as a column reference, not a keyword, so it becomes equivalent to `SELECT DISTINCT, x` (selecting a column named "DISTINCT" and a column named "x").

```bash
printf '{"x":1}\n{"x":1}\n{"x":2}' | go run ./cmd/dbspa "SELECT DISTINCT x"
# Expected: {"x":1} then {"x":2}
# Actual: {"x":1} {"x":1} {"x":2} (no dedup)

go run ./cmd/dbspa --dry-run "SELECT DISTINCT x" </dev/null
# Shows: Columns: 1 - x
# DISTINCT is silently swallowed
```

**Expected:** Either support DISTINCT or return a parse error saying it's not supported.

### 10. Errors in projection silently drop rows

**Category:** UX
**Severity:** P2

When a projection expression fails (e.g., cast error, type mismatch in arithmetic), the row is silently dropped with no output and no warning.

```bash
echo '{"x":"hello"}' | go run ./cmd/dbspa "SELECT x::int AS result"
# Expected: error message or {"result":null}
# Actual: (no output, no warning)

echo '{"x":true}' | go run ./cmd/dbspa "SELECT x + 1 AS result"
# Expected: error or type coercion
# Actual: (no output, no warning)

echo '{"x":"hello"}' | go run ./cmd/dbspa "SELECT x WHERE x > 5"
# Expected: error or false comparison
# Actual: (no output, no warning - comparison error treated as filter-fail)
```

Users have no way to tell if their data was silently dropped vs. legitimately filtered. This is especially dangerous for production pipelines.

### 11. Aliased expressions not available in WHERE clause

**Category:** UX
**Severity:** P2

Column aliases defined in SELECT cannot be referenced in WHERE. The alias resolves to NULL (column not found), which silently filters out the row.

```bash
echo '{"x":5}' | go run ./cmd/dbspa "SELECT x * 2 AS y WHERE y > 1"
# Expected: {"y":10} (y = 10 > 1)
# Actual: (no output - y resolves to NULL in WHERE, NULL > 1 = NULL, row filtered)
```

**Expected:** Either resolve aliases in WHERE or produce an error "column y not found, did you mean to use the expression directly in WHERE?"

### 12. NaN and Infinity output as JSON strings

**Category:** UX
**Severity:** P2

Float division by zero produces `+Inf`, `-Inf`, or `NaN`, which are serialized as JSON strings rather than the standard JSON `null`. This produces technically valid JSON but the type changes from number to string, which breaks typed consumers.

```bash
echo '{"x":0.0}' | go run ./cmd/dbspa "SELECT 1.0/x AS result"
# Output: {"result":"+Inf"}  (string, not number)

echo '{"x":0.0}' | go run ./cmd/dbspa "SELECT 0.0/x AS result"
# Output: {"result":"NaN"}  (string, not number)
```

**Expected:** Either return `null` (like Postgres does for division by zero) or document this behavior clearly.

### 13. Integer division by zero returns NULL without warning

**Category:** UX
**Severity:** P2

```bash
echo '{"x":0}' | go run ./cmd/dbspa "SELECT 10 / x"
# Output: {"col1":null}
```

The column name defaults to `col1` (not descriptive) and the NULL is returned silently. While NULL-on-division-by-zero matches Postgres semantics, the `col1` alias is confusing.

### 14. Non-kafka FROM URI silently falls back to stdin

**Category:** UX
**Severity:** P2

When a FROM clause specifies a non-kafka URI (e.g., a file path), the engine silently reads from stdin instead. The FROM URI is ignored without any error or warning.

```bash
echo '{"x":1}' | go run ./cmd/dbspa "SELECT x FROM '/tmp/nonexistent'"
# Expected: error "file not found" or read from the file
# Actual: {"x":1} (reads from stdin, ignores FROM URI)
```

### 15. Negative LIMIT rejected but LIMIT 0 accepted (inconsistently)

**Category:** UX
**Severity:** P2

```bash
echo '{"x":1}' | go run ./cmd/dbspa "SELECT x LIMIT -1"
# Error: parse error: expected integer after LIMIT, got "-"

echo '{"x":1}' | go run ./cmd/dbspa "SELECT x LIMIT 0"
# Output: {"x":1}  (should be empty, see bug #3)
```

LIMIT 0 and LIMIT -1 should both either be rejected or both return zero rows.

### 16. Quoted identifiers (double quotes) not supported

**Category:** UX
**Severity:** P2

Standard SQL uses double quotes for identifiers with special characters. DBSPA rejects them.

```bash
echo '{"first name":"John"}' | go run ./cmd/dbspa 'SELECT "first name"'
# Error: parse error: unexpected token "\""
```

**Workaround:** None for columns with spaces or reserved-word names.

### 17. SELECT y returns null instead of error for non-existent column

**Category:** UX
**Severity:** P2

```bash
echo '{"x":1}' | go run ./cmd/dbspa "SELECT y"
# Output: {"y":null}
```

This is consistent with the schemaless/streaming philosophy (columns may appear later), but for finite stdin input where the column never appears, a warning would be helpful.

### 18. SUM on non-numeric values silently produces no output

**Category:** UX
**Severity:** P2

```bash
printf '{"v":"hello"}\n{"v":"world"}' | go run ./cmd/dbspa "SELECT SUM(v) AS total GROUP BY 1"
# Expected: error or null
# Actual: (no output at all)
```

The SUM accumulator silently ignores non-numeric values (treated as NULL), which means the `anyChanged` flag never flips, so nothing is emitted.

---

## P3 - MISSING (Feature Gaps / Nice-to-Have)

### 19. DISTINCT not implemented

**Category:** MISSING
**Severity:** P3

SQL DISTINCT is not supported. The keyword is silently consumed by the parser without effect. Workaround: use `DEDUPLICATE BY` if available.

### 20. Aggregate without GROUP BY not supported

**Category:** MISSING
**Severity:** P3

`SELECT COUNT(*) FROM ...` without GROUP BY is a common SQL pattern that should implicitly create a single global group. Currently requires `GROUP BY 1` workaround.

### 21. Subqueries not supported

**Category:** MISSING
**Severity:** P3

```bash
echo '{"x":1}' | go run ./cmd/dbspa "SELECT (SELECT x)"
# Error: parse error: unexpected token "SELECT"
```

### 22. Negative array index not supported

**Category:** MISSING
**Severity:** P3

```bash
echo '{"x":[1,2,3]}' | go run ./cmd/dbspa "SELECT x->-1 AS last"
# Error: parse error: unexpected token "-"
```

Python and many modern languages support `arr[-1]` for the last element.

### 23. JSON array access uses `->0` instead of standard `[0]` syntax

**Category:** MISSING
**Severity:** P3

```bash
echo '{"x":[1,2,3]}' | go run ./cmd/dbspa "SELECT x->0 AS first"
# Works, outputs: {"FIRST":1}
# Note: column alias defaults to "FIRST" (the function name?) rather than "first"
```

The default alias for `x->0` appears to resolve to "FIRST" instead of something like "x_0" or "col1".

### 24. No --version flag (only `version` subcommand)

**Category:** MISSING
**Severity:** P3

```bash
go run ./cmd/dbspa --version
# Error: usage: dbspa <SQL> or dbspa -f <file.sql>

go run ./cmd/dbspa version
# dbspa dev
```

---

## Test Coverage Gaps

| File | Coverage | Notes |
|------|----------|-------|
| cmd/dbspa/main.go | 0.0% | No integration tests |
| internal/engine/pipeline.go | 0.0% (Process) | Untested via unit tests |
| internal/engine/eval.go | 54.2% (Eval) | Many expression types untested |
| internal/engine/aggregate.go | 0.0% (ParseAggColumns) | Critical parsing function untested |
| internal/engine/windowed_aggregate.go | 66.7% (NewWindowedAggregateOp) | Constructor partially tested |
| internal/sink/ | 20.6% | Sink layer mostly untested |
| internal/source/ | 40.2% | Source layer partially tested |
| internal/sql/parser/ | 68.1% | Parser gaps especially around edge cases |

Key uncovered paths:
- `evalIs` (IS DISTINCT FROM path): 68% coverage
- `evalJsonAccess` (array access, AsText paths): 68.4% coverage
- `evalFunction` (COALESCE, NULLIF, NOW, TRIM variants): 69% coverage
- `compareValues` (timestamp comparison, cross-type): 55.6% coverage
- `evalHaving` (UnaryExpr branch, nested aggregates): 57.1% coverage

---

## Summary

| Severity | Count |
|----------|-------|
| P0 (Crash) | 1 |
| P1 (Wrong Results) | 8 |
| P2 (UX Issues) | 10 |
| P3 (Missing Features) | 6 |

The most critical issue is the **nil pointer panic** when SELECT includes columns not in GROUP BY (P0 #1). The next highest priority fixes are aggregate-without-GROUP-BY (#2), LIMIT 0 (#3), and silent row dropping on projection errors (#10).
