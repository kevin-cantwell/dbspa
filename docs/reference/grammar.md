# SQL Grammar (EBNF)

This is the formal grammar for DBSPA's SQL dialect in Extended Backus-Naur Form (EBNF). Every valid DBSPA query can be derived from these rules. The [parser test suites](https://github.com/kevin-cantwell/dbspa) validate that the parser accepts exactly this language.

## Notation

| Symbol | Meaning |
|---|---|
| `::=` | Is defined as |
| `\|` | Alternative |
| `[ ... ]` | Optional (zero or one) |
| `{ ... }` | Repetition (zero or more) |
| `( ... )` | Grouping |
| `'...'` | Literal keyword or symbol |
| `UPPERCASE` | SQL keyword (case-insensitive) |
| `lowercase` | Grammar rule (non-terminal) |

---

## Statements

```ebnf
statement
    ::= select_statement
```

## SELECT Statement

```ebnf
select_statement
    ::= SELECT [ DISTINCT ] select_list
        [ from_clause ]
        [ seed_clause ]
        [ join_clause ]
        [ format_clause ]
        [ where_clause ]
        [ group_by_clause ]
        [ having_clause ]
        [ window_clause ]
        [ event_time_clause ]
        [ watermark_clause ]
        [ emit_clause ]
        [ deduplicate_clause ]
        [ order_by_clause ]
        [ limit_clause ]
```

!!! note
    Clause ordering is enforced by the parser. FORMAT may also appear directly after the FROM source or after the JOIN source.

## Clauses

```ebnf
select_list
    ::= '*'
      | select_item { ',' select_item }

select_item
    ::= expression [ AS identifier ]

from_clause
    ::= FROM ( source [ format_clause ] [ identifier ]
             | exec_source [ format_clause ] [ identifier ]
             | subquery_source )

source
    ::= string_literal                (* URI: 'kafka://...', '/path/to/file' *)
      | identifier                    (* bare identifier: stdin *)

exec_source
    ::= EXEC '(' string_literal ')' [ AS ( STREAM | TABLE ) ]

subquery_source
    ::= '(' select_statement ')' identifier    (* alias is mandatory *)

seed_clause
    ::= SEED FROM ( string_literal | exec_source ) [ format_clause ]

join_clause
    ::= [ LEFT ] JOIN ( source [ format_clause ]
                       | exec_source [ format_clause ]
                       | subquery_source ) identifier ON expression
        [ WITHIN INTERVAL string_literal ]

format_clause
    ::= FORMAT encoding_name [ '(' format_options ')' ] [ envelope_name ]
      | FORMAT envelope_name

encoding_name
    ::= JSON | NDJSON | AVRO | CSV | PROTOBUF | PARQUET

envelope_name
    ::= DEBEZIUM | DBSPA

format_options
    ::= format_option { ',' format_option }

format_option
    ::= identifier '=' string_literal

where_clause
    ::= WHERE expression

group_by_clause
    ::= GROUP BY expression { ',' expression }

having_clause
    ::= HAVING expression

order_by_clause
    ::= ORDER BY order_item { ',' order_item }

order_item
    ::= expression [ ASC | DESC ]

limit_clause
    ::= LIMIT integer_literal

window_clause
    ::= WINDOW window_type

window_type
    ::= TUMBLING string_literal
      | SLIDING string_literal BY string_literal
      | SESSION string_literal

event_time_clause
    ::= EVENT TIME BY expression

watermark_clause
    ::= WATERMARK string_literal

emit_clause
    ::= EMIT FINAL
      | EMIT EARLY string_literal

deduplicate_clause
    ::= DEDUPLICATE BY expression
        [ WITHIN string_literal ]
        [ CAPACITY integer_literal ]
```

## Expressions

```ebnf
expression
    ::= or_expression

or_expression
    ::= and_expression { OR and_expression }

and_expression
    ::= not_expression { AND not_expression }

not_expression
    ::= [ NOT ] comparison_expression

comparison_expression
    ::= addition_expression
        [ comparison_operator addition_expression
        | IS [ NOT ] NULL
        | IS [ NOT ] DISTINCT FROM addition_expression
        | [ NOT ] IN '(' expression_list ')'
        | [ NOT ] BETWEEN addition_expression AND addition_expression
        | [ NOT ] LIKE addition_expression
        | [ NOT ] ILIKE addition_expression
        ]

comparison_operator
    ::= '=' | '!=' | '<>' | '<' | '>' | '<=' | '>='

addition_expression
    ::= concat_expression { ( '+' | '-' ) concat_expression }

concat_expression
    ::= multiplication_expression { '||' multiplication_expression }

multiplication_expression
    ::= unary_expression { ( '*' | '/' | '%' ) unary_expression }

unary_expression
    ::= [ '-' | '+' ] cast_expression

cast_expression
    ::= json_access_expression [ '::' type_name ]

json_access_expression
    ::= primary_expression { ( '->' | '->>' ) primary_expression }
```

## Primary Expressions

```ebnf
primary_expression
    ::= literal
      | column_ref
      | qualified_ref
      | function_call
      | case_expression
      | cast_function
      | '(' expression ')'

literal
    ::= integer_literal
      | float_literal
      | string_literal
      | TRUE
      | FALSE
      | NULL
      | INTERVAL string_literal

integer_literal
    ::= digit { digit }

float_literal
    ::= digit { digit } '.' { digit }

string_literal
    ::= "'" { character | "''" } "'"       (* single-quoted, '' for escape *)

column_ref
    ::= identifier

qualified_ref
    ::= identifier '.' identifier           (* alias.column or column.field *)

dot_notation
    ::= identifier '.' identifier { '.' identifier }
```

!!! note "Dot notation desugaring"
    `a.b` is parsed as `QualifiedRef(a, b)` — resolved as alias.column first, then JSON field access as fallback.
    `a.b.c` is parsed as `QualifiedRef(a, b) ->> "c"` — first two segments resolve as qualified ref, additional segments become JSON access (`->` for intermediate, `->>` for final).

## Functions

```ebnf
function_call
    ::= function_name '(' [ DISTINCT ] [ expression_list ] ')'
      | COUNT '(' '*' ')'
      | COUNT '(' DISTINCT expression ')'
      | EXTRACT '(' identifier FROM expression ')'

function_name
    ::= aggregate_function
      | scalar_function

aggregate_function
    ::= COUNT | SUM | AVG | MIN | MAX | MEDIAN
      | FIRST | LAST | ARRAY_AGG | APPROX_COUNT_DISTINCT

scalar_function
    ::= COALESCE | NULLIF | LENGTH | UPPER | LOWER
      | TRIM | LTRIM | RTRIM | SUBSTR | REPLACE | SPLIT_PART
      | PARSE_TIMESTAMP | FORMAT_TIMESTAMP | NOW | EXTRACT | JSON_KEYS

expression_list
    ::= expression { ',' expression }

case_expression
    ::= CASE when_clause { when_clause } [ ELSE expression ] END

when_clause
    ::= WHEN expression THEN expression

cast_function
    ::= CAST '(' expression AS type_name ')'
```

## Types

```ebnf
type_name
    ::= INT | BIGINT | FLOAT | DOUBLE | TEXT | BOOLEAN | TIMESTAMP
```

## Identifiers

```ebnf
identifier
    ::= unquoted_identifier
      | quoted_identifier

unquoted_identifier
    ::= letter { letter | digit | '_' }     (* case-insensitive, uppercased for keywords *)

quoted_identifier
    ::= '"' { character | '""' } '"'        (* preserves case, avoids keyword clash *)
```

!!! note
    Quoted identifiers (`"last"`, `"select"`) are always treated as column names, never as keywords. This follows the PostgreSQL convention.

## Operator Precedence

From highest to lowest:

| Precedence | Operators | Associativity |
|---|---|---|
| 1 (highest) | `::` (type cast) | Left |
| 2 | `->`, `->>` (JSON access) | Left |
| 3 | Unary `-`, `+` | Right |
| 4 | `*`, `/`, `%` | Left |
| 5 | `+`, `-` (binary) | Left |
| 6 | `\|\|` (string concat) | Left |
| 7 | `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`, `LIKE`, `ILIKE`, `IS`, `IN`, `BETWEEN` | Non-associative |
| 8 | `NOT` | Right |
| 9 | `AND` | Left |
| 10 (lowest) | `OR` | Left |

Parentheses override precedence.

## Reserved Words

The following words are reserved as SQL keywords and cannot be used as unquoted identifiers. Use [quoted identifiers](#identifiers) (`"word"`) to reference columns with these names.

```
AND, AS, ASC, AVRO, BETWEEN, BY, CAPACITY, CASE, CAST, COUNT, CSV, DEBEZIUM,
DEDUPLICATE, DESC, DISTINCT, EARLY, ELSE, EMIT, END, EVENT, EXEC, EXTRACT,
FALSE, FINAL, FIRST, DBSPA, FORMAT, FROM, GROUP, HAVING, ILIKE, IN, INTERVAL,
IS, JOIN, JSON, LAST, LEFT, LIKE, LIMIT, NDJSON, NOT, NULL, ON, OR, ORDER,
PARQUET, PROTOBUF, SEED, SELECT, SESSION, SLIDING, STREAM, SUM, AVG, MIN,
MAX, MEDIAN, TABLE, THEN, TIME, TRUE, TUMBLING, WATERMARK, WHEN, WHERE,
WINDOW, WITHIN
```

Function names (`LENGTH`, `UPPER`, `COALESCE`, etc.) are also reserved when followed by `(`. As bare identifiers they are treated as column references.
