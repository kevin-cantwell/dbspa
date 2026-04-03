package grammar_test

import (
	"testing"

	"github.com/kevin-cantwell/folddb/internal/sql/grammar"
	"github.com/kevin-cantwell/folddb/internal/sql/parser"
)

// positiveQueries are all SQL strings from parser_positive_test.go that MUST parse.
var positiveQueries = []string{
	"SELECT name",
	"SELECT a, b, c",
	"SELECT name AS username, age AS years",
	"SELECT name username",
	"SELECT *",
	"SELECT DISTINCT region",
	"SELECT a + b AS total",
	"SELECT price * 2 AS doubled",
	"SELECT first || ' ' || last AS full_name",
	"SELECT COUNT(*)",
	"SELECT SUM(amount) AS total",
	"SELECT AVG(score)",
	"SELECT MIN(price), MAX(price)",
	"SELECT MEDIAN(latency)",
	"SELECT FIRST(val), LAST(val)",
	"SELECT ARRAY_AGG(tag)",
	"SELECT APPROX_COUNT_DISTINCT(user_id)",
	"SELECT COUNT(DISTINCT user_id)",
	"SELECT COALESCE(a, b, c)",
	"SELECT NULLIF(a, b)",
	"SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END AS flag",
	"SELECT CASE WHEN x > 10 THEN 'huge' WHEN x > 5 THEN 'big' WHEN x > 0 THEN 'small' ELSE 'zero' END",
	"SELECT CASE WHEN x > 0 THEN 'positive' END",
	"SELECT val::INT AS num",
	"SELECT val::FLOAT",
	"SELECT CAST(val AS TEXT)",
	"SELECT col->>'field' AS val",
	"SELECT col->'nested'",
	"SELECT col->0",
	"SELECT payload->'user'->>'email' AS email",
	"SELECT data.name",
	"SELECT user.address.city",
	"SELECT (a + b) * c",
	`SELECT "last" AS surname`,
	`SELECT "select"`,
	"SELECT -x",
	"SELECT NULL",
	"SELECT TRUE, FALSE",
	"SELECT 42, 3.14",
	"SELECT 'hello world'",
	"SELECT NOW()",
	"SELECT LENGTH(name)",
	"SELECT UPPER(name), LOWER(name)",
	"SELECT TRIM(s), LTRIM(s), RTRIM(s)",
	"SELECT SUBSTR(name, 1, 3)",
	"SELECT REPLACE(name, 'old', 'new')",
	"SELECT SPLIT_PART(path, '/', 2)",
	"SELECT EXTRACT(ts)",
	"SELECT json_keys(payload)",
	"SELECT x % 2",
	"SELECT total / count",
	// FROM clause
	"SELECT * FROM 'kafka://broker:9092/topic'",
	"SELECT * FROM stdin",
	"SELECT * FROM stdin e",
	"SELECT * FROM 'kafka://b/t' FORMAT DEBEZIUM",
	"SELECT * FROM 'kafka://b/t' FORMAT CSV(delimiter='|', header=true)",
	"SELECT * FROM 'kafka://b/t' FORMAT AVRO",
	"SELECT * FROM 'kafka://b/t' FORMAT PROTOBUF(message='Order')",
	"SELECT * FROM 'kafka://b/t' FORMAT PARQUET",
	"SELECT * FROM 'kafka://b/t' FORMAT DEBEZIUM_AVRO",
	"SELECT * FROM 'kafka://broker/topic?offset=earliest&registry=http://reg:8081'",
	// FROM without FROM (standalone select)
	"SELECT name",
	// FROM stdin with format
	"SELECT * FROM stdin FORMAT DEBEZIUM",
	// JOIN
	"SELECT * FROM stdin e JOIN '/tmp/users.ndjson' u ON e.user_id = u.id",
	"SELECT * FROM stdin e LEFT JOIN '/tmp/users.csv' u ON e.uid = u.id",
	"SELECT e.id FROM stdin e JOIN '/tmp/data.csv' FORMAT CSV u ON e.id = u.id",
	"SELECT e.user_id, u.name FROM stdin e JOIN '/tmp/users.ndjson' u ON e.user_id = u.id",
	"SELECT o.id, p.id FROM 'kafka://b/orders' o JOIN 'kafka://b/payments' p ON o.order_id = p.order_id WITHIN INTERVAL '10 minutes'",
	"SELECT * FROM stdin o JOIN '/tmp/users.ndjson' u ON o.id = u.id",
	// SEED FROM
	"SELECT region, COUNT(*) FROM 'kafka://b/t' FORMAT DEBEZIUM SEED FROM '/path/to/snapshot.parquet' GROUP BY region",
	"SELECT status, COUNT(*) FROM stdin SEED FROM '/tmp/seed.csv' FORMAT CSV GROUP BY status",
	// WHERE
	"SELECT x WHERE x > 25",
	"SELECT x WHERE status = 'active'",
	"SELECT x WHERE a > 1 AND b < 10",
	"SELECT x WHERE a = 1 OR b = 2",
	"SELECT x WHERE NOT active",
	"SELECT s WHERE s IN ('a', 'b', 'c')",
	"SELECT s WHERE s NOT IN (1, 2)",
	"SELECT x WHERE x BETWEEN 10 AND 100",
	"SELECT x WHERE x IS NULL",
	"SELECT x WHERE x IS NOT NULL",
	"SELECT x WHERE x IS DISTINCT FROM y",
	"SELECT x WHERE x IS NOT DISTINCT FROM y",
	"SELECT n WHERE n LIKE 'A%'",
	"SELECT n WHERE n ILIKE '%alice%'",
	"SELECT n WHERE n NOT LIKE 'test%'",
	"SELECT e.name FROM stdin e WHERE e.status = 'active'",
	"SELECT x WHERE payload->>'type' = 'click'",
	"SELECT x WHERE (a > 1 AND b < 2) OR c = 3",
	"SELECT x WHERE a != 1",
	"SELECT x WHERE a <= 10 AND b >= 20",
	// GROUP BY
	"SELECT region, COUNT(*) FROM 'kafka://b/t' GROUP BY region",
	"SELECT a, b, COUNT(*) FROM 'kafka://b/t' GROUP BY a, b",
	"SELECT a, b, COUNT(*) FROM 'kafka://b/t' GROUP BY 1, 2",
	"SELECT _after->>'region', COUNT(*) FROM 'kafka://b/t' FORMAT DEBEZIUM GROUP BY _after->>'region'",
	// HAVING
	"SELECT region, COUNT(*) AS cnt FROM 'kafka://b/t' GROUP BY region HAVING COUNT(*) > 100",
	"SELECT region, SUM(total) FROM 'kafka://b/t' GROUP BY region HAVING SUM(total) >= 1000",
	// ORDER BY
	"SELECT region, COUNT(*) AS cnt FROM 'kafka://b/t' GROUP BY region ORDER BY cnt",
	"SELECT region, COUNT(*) AS cnt FROM 'kafka://b/t' GROUP BY region ORDER BY cnt DESC",
	"SELECT region, COUNT(*) AS cnt FROM 'kafka://b/t' GROUP BY region ORDER BY cnt ASC",
	"SELECT region, status, COUNT(*) FROM 'kafka://b/t' GROUP BY region, status ORDER BY region ASC, status DESC",
	// LIMIT
	"SELECT x LIMIT 100",
	"SELECT x LIMIT 0",
	// WINDOW
	"SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1 minute'",
	"SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW SLIDING '10 minutes' BY '5 minutes'",
	"SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW SESSION '5 minutes'",
	// EMIT
	"SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1 minute' EMIT FINAL",
	"SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1 minute' EMIT EARLY '10 seconds'",
	// EVENT TIME BY
	"SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1 minute' EVENT TIME BY ts",
	// WATERMARK
	"SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1 minute' EVENT TIME BY ts WATERMARK '30 seconds'",
	// DEDUPLICATE
	"SELECT * DEDUPLICATE BY order_id WITHIN '10 minutes'",
	"SELECT * DEDUPLICATE BY id WITHIN '5 minutes' CAPACITY 500000",
	// Standalone FORMAT
	"SELECT * FORMAT CSV(delimiter=',', header=true)",
	// Complex combinations
	"SELECT u.name, COUNT(*) AS cnt\n\t\t\t\t\tFROM stdin e\n\t\t\t\t\tJOIN '/tmp/users.ndjson' u ON e.user_id = u.id\n\t\t\t\t\tWHERE e.action = 'login'\n\t\t\t\t\tGROUP BY u.name\n\t\t\t\t\tHAVING COUNT(*) > 5\n\t\t\t\t\tORDER BY cnt DESC",
	"SELECT region, COUNT(*) AS cnt\n\t\t\t\t\tFROM 'kafka://broker/events'\n\t\t\t\t\tGROUP BY region\n\t\t\t\t\tWINDOW TUMBLING '5 minutes'\n\t\t\t\t\tEVENT TIME BY event_time\n\t\t\t\t\tWATERMARK '30 seconds'\n\t\t\t\t\tEMIT EARLY '10 seconds'",
	"SELECT e.status, COUNT(*)\n\t\t\t\t\tFROM stdin e\n\t\t\t\t\tJOIN '/tmp/users.ndjson' u ON e.uid = u.id\n\t\t\t\t\tSEED FROM '/tmp/seed.ndjson'\n\t\t\t\t\tGROUP BY e.status",
	"SELECT _after->>'region', COUNT(*) AS cnt FROM 'kafka://b/t' FORMAT DEBEZIUM GROUP BY _after->>'region' HAVING COUNT(*) > 10",
	"SELECT\n\t\t\t\t\tu.name,\n\t\t\t\t\tCOUNT(*) AS total_events,\n\t\t\t\t\tSUM(CASE WHEN e.action = 'purchase' THEN 1 ELSE 0 END) AS purchases,\n\t\t\t\t\tAVG((e.payload->>'amount')::FLOAT) AS avg_amount,\n\t\t\t\t\tMIN((e.payload->>'amount')::FLOAT) AS min_amount,\n\t\t\t\t\tMAX((e.payload->>'amount')::FLOAT) AS max_amount,\n\t\t\t\t\tCOUNT(DISTINCT e.session_id) AS sessions,\n\t\t\t\t\tFIRST(e.action) AS first_action\n\t\t\t\t\tFROM 'kafka://broker/events' e\n\t\t\t\t\tJOIN '/data/users.parquet' u ON e.user_id = u.id\n\t\t\t\t\tWHERE e.action != 'heartbeat'\n\t\t\t\t\tGROUP BY u.name\n\t\t\t\t\tHAVING COUNT(*) > 10\n\t\t\t\t\tORDER BY total_events DESC\n\t\t\t\t\tLIMIT 50",
	"SELECT o.order_id, COUNT(*) AS matches\n\t\t\t\t\tFROM 'kafka://broker/orders' o\n\t\t\t\t\tJOIN 'kafka://broker/payments' p ON o.order_id = p.order_id\n\t\t\t\t\tWITHIN INTERVAL '5 minutes'\n\t\t\t\t\tGROUP BY o.order_id\n\t\t\t\t\tWINDOW TUMBLING '1 minute'\n\t\t\t\t\tEVENT TIME BY o.created_at\n\t\t\t\t\tWATERMARK '10 seconds'\n\t\t\t\t\tEMIT FINAL",
	"SELECT DISTINCT region FROM 'kafka://broker/events' WHERE status = 'active' LIMIT 10",
	"SELECT e.id, u.name FROM stdin e LEFT JOIN '/data/users.csv' FORMAT CSV(header=true, delimiter=',') u ON e.uid = u.id",
	"SELECT * WHERE status = 'ok' DEDUPLICATE BY request_id WITHIN '1 minute' LIMIT 1000",
	"SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount)",
	"SELECT * FROM stdin e FORMAT DEBEZIUM",
	"SELECT x WHERE (payload->>'score')::INT > 90",
	"SELECT revenue - cost AS profit",
	"SELECT INTERVAL '1 hour'",
	"SELECT * FROM 'stdin://'",
	"SELECT * FROM 'kafka://b/t' FORMAT AVRO(registry='http://registry:8081')",
	`SELECT * FROM 'kafka://b/t' FORMAT CSV(delimiter='|', header=true, quote='"', null_string='')`,
	// Two-token FORMAT syntax
	"SELECT * FROM 'kafka://b/t' FORMAT AVRO DEBEZIUM",
	"SELECT * FROM 'kafka://b/t' FORMAT JSON DEBEZIUM",
	"SELECT * FROM stdin FORMAT FOLDDB",
	"SELECT * FROM stdin FORMAT JSON FOLDDB",
	"SELECT * FROM 'kafka://b/t' FORMAT AVRO(registry='http://reg:8081') DEBEZIUM",
	"SELECT a + b * c",
	"SELECT a OR b AND c",
	"SELECT NOT a AND b",
	"SELECT * FROM 'kafka://broker/topic?sasl_mechanism=PLAIN&sasl_username=user&sasl_password=pass'",
	"SELECT e.region, COUNT(*) FROM stdin e GROUP BY e.region",
	"SELECT x WHERE x IS TRUE",
	"SELECT x WHERE x NOT BETWEEN 1 AND 10",
	"SELECT n WHERE n NOT ILIKE '%test%'",
	"SELECT (col->>'amount')::FLOAT",
	"SELECT PARSE_TIMESTAMP(ts, 'RFC3339')",
	"SELECT FORMAT_TIMESTAMP(ts, 'RFC3339')",
	"SELECT COALESCE((payload->>'amount')::FLOAT, 0)",
	"SELECT SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) AS ok_count",
	// EXEC
	"SELECT * FROM EXEC('echo hello')",
	"SELECT * FROM EXEC('kubectl logs -f my-pod --output=json') WHERE level = 'ERROR'",
	`SELECT * FROM EXEC('psql -c "COPY users TO STDOUT"') u FORMAT CSV`,
	"SELECT e.id, u.name FROM stdin e JOIN EXEC('cat /tmp/users.ndjson') u ON e.user_id = u.id",
	"SELECT e.id FROM stdin e LEFT JOIN EXEC('echo {\"id\":1}') u ON e.id = u.id",
	"SELECT region, COUNT(*) FROM 'kafka://b/t' SEED FROM EXEC('bq query --format=json \"SELECT * FROM snap\"') GROUP BY region",
	"SELECT * FROM EXEC('cat data.csv') FORMAT CSV(header=true)",
	// EXEC AS STREAM / AS TABLE
	"SELECT * FROM EXEC('cat data.json') AS TABLE",
	"SELECT * FROM EXEC('tail -f /var/log/app.json') AS STREAM",
	"SELECT e.x FROM EXEC('tail -f log.json') e AS STREAM WHERE e.x > 1",
	"SELECT * FROM EXEC('cat data.csv') AS TABLE FORMAT CSV(header=true)",
	"SELECT * FROM stdin e JOIN EXEC('psql -c \"COPY users TO STDOUT\"') u AS TABLE FORMAT CSV ON e.id = u.id",
	"SELECT * FROM stdin e JOIN EXEC('tail -f events.json') u AS STREAM ON e.id = u.id",
	"SELECT region, COUNT(*) FROM 'kafka://b/t' SEED FROM EXEC('bq query') AS TABLE GROUP BY region",
	// Subqueries
	"SELECT * FROM (SELECT * FROM '/data/file.csv') t",
	"SELECT * FROM (SELECT status, COUNT(*) AS cnt GROUP BY status) t WHERE cnt > 100",
	"SELECT e.id, r.x FROM stdin e JOIN (SELECT x GROUP BY x) r ON e.id = r.x",
	"SELECT * FROM (SELECT * FROM (SELECT 1) t1) t2",
	"SELECT e.id FROM stdin e LEFT JOIN (SELECT id FROM '/tmp/ref.ndjson') r ON e.id = r.id",
}

// negativeQueries are all SQL strings from parser_negative_test.go that MUST fail.
var negativeQueries = []string{
	"",
	"   \t\n  ",
	"FROM 'kafka://broker/topic'",
	"FROM",
	"SELECT COUNT(*) FROM 'kafka://b/t' HAVING COUNT(*) > 1",
	"SELECT 'hello",
	"SELECT COUNT(* FROM 'kafka://b/t'",
	"SELECT (a + b",
	"SELECT a,, b",
	"SELECT a, b,",
	"SELECT a +",
	"SELECT e.",
	"SELECT x AS",
	"SELECT * WHERE",
	"SELECT * FROM stdin JOIN '/tmp/f' u ON",
	"SELECT * FROM 'kafka://b/t' GROUP BY",
	"SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g ORDER BY",
	"SELECT * FROM 'kafka://b/t' GROUP x",
	"SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g ORDER x",
	"SELECT x ORDER BY x DESC",
	"SELECT * FROM 'kafka://b/t' ORDER BY x",
	"SELECT * LIMIT -1",
	"SELECT * LIMIT abc",
	"SELECT * LIMIT 3.14",
	"SELECT * FROM @orders",
	"SELECT #",
	"SELECT `column`",
	"SELECT a | b",
	"SELECT a : b",
	"SELECT *;",
	"SELECT * WHERE x > 1 FROM 'kafka://b/t'",
	"SELECT * FROM 'kafka://b/t' GROUP BY x WHERE y > 1",
	"SELECT * FROM 'kafka://b/t' HAVING COUNT(*) > 5 GROUP BY x",
	"SELECT * FROM 'kafka://b/t' ORDER BY x GROUP BY y",
	"SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g LIMIT 10 ORDER BY g",
	"SELECT * WHERE a > 1 WHERE b > 2",
	"SELECT * WHERE x >",
	"SELECT * WHERE x IN 1, 2, 3",
	"SELECT * WHERE x NOT IN 1, 2",
	"SELECT * WHERE x BETWEEN 1 OR 10",
	"SELECT CASE WHEN x > 1 END",
	"SELECT CASE WHEN x > 1 THEN 'a'",
	"SELECT * WHERE x IS 5",
	"SELECT * WHERE x IS NOT 5",
	"SELECT * WHERE x IS DISTINCT y",
	"SELECT SUM(x",
	"SELECT CAST(x INT)",
	"SELECT CAST(x AS INT",
	"SELECT CAST x AS INT)",
	"SELECT * FROM 'kafka://b/t' FORMAT CSV(delimiter=','",
	"SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW",
	"SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW HOPPING '1 minute'",
	"SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING",
	"SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING 60",
	"SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW SLIDING",
	"SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1m' EMIT",
	"SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1m' EMIT LATE",
	"SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1m' EMIT EARLY",
	"SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1m' EMIT EARLY 10",
	"SELECT * FROM stdin JOIN '/tmp/file' u WHERE x > 1",
	"SELECT * FROM stdin JOIN ON x = y",
	"SELECT * FROM stdin LEFT '/tmp/file' u ON x = y",
	"SELECT * FROM 'kafka://b/o' o JOIN 'kafka://b/p' p ON o.id = p.id WITHIN '10 min'",
	"SELECT * FROM 'kafka://b/o' o JOIN 'kafka://b/p' p ON o.id = p.id WITHIN INTERVAL",
	"SELECT * FROM 'kafka://b/o' o JOIN 'kafka://b/p' p ON o.id = p.id WITHIN INTERVAL 10",
	"SELECT * DEDUPLICATE order_id",
	"SELECT * DEDUPLICATE BY",
	"SELECT * DEDUPLICATE BY id WITHIN '5m' CAPACITY abc",
	"SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1m' EVENT TIME BY ts WATERMARK",
	"SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1m' EVENT TIME BY ts WATERMARK 30",
	"SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1m' EVENT BY ts",
	"SELECT COUNT(*) FROM 'kafka://b/t' GROUP BY g WINDOW TUMBLING '1m' EVENT TIME ts",
	"SELECT * FROM 'kafka://b/t' SEED '/tmp/file'",
	"SELECT * FROM 'kafka://b/t' SEED FROM",
	"SELECT * FROM 'kafka://b/t' SEED FROM snapshot",
	"SELECT * FROM (SELECT 1) WHERE x > 1",
	"SELECT * FROM stdin e JOIN (SELECT x) ON e.id = x",
	"SELECT * FROM EXEC",
	"SELECT * FROM EXEC()",
	"SELECT * FROM EXEC(123)",
	"SELECT * FROM EXEC('echo hello') AS INVALID",
	"SELECT * FROM (SELECT 1) WHERE",
	"SELECT * FROM (SELECT * FROM 'kafka://b/t')",
	"SELECT * FROM 'kafka://b/t1' UNION SELECT * FROM 'kafka://b/t2'",
	"INSERT INTO orders VALUES (1, 'a')",
	"UPDATE orders SET status = 'done'",
	"DELETE FROM orders WHERE id = 1",
	"CREATE TABLE orders (id INT)",
	"WITH t AS (SELECT 1) SELECT * FROM t",
	"SELECT * FROM 'orders'; DROP TABLE orders;--",
	"SELECT * FROM 'kafka://b/t' alias GARBAGE",
	`SELECT "unclosed`,
	"SELECT * WHERE x ! y",
	"SELECT INTERVAL 42",
	"SELECT * FROM",
	"SELECT * FROM 'kafka://b/t' FORMAT CSV(delimiter ',')",
	"SELECT * FROM 'kafka://b/t' FORMAT CSV(delimiter=(nested))",
	"SELECT a.b.",
}

func TestGrammarConformancePositive(t *testing.T) {
	for _, sql := range positiveQueries {
		grammarOK := grammar.Validates(sql)
		p := parser.New(sql)
		_, parserErr := p.Parse()
		parserOK := parserErr == nil

		if grammarOK != parserOK {
			t.Errorf("DISAGREEMENT on: %q\n  grammar=%v parser=%v (parser err: %v)", sql, grammarOK, parserOK, parserErr)
		}
		if !grammarOK {
			t.Errorf("GRAMMAR REJECTED positive case: %q", sql)
		}
	}
}

func TestGrammarConformanceNegative(t *testing.T) {
	for _, sql := range negativeQueries {
		grammarOK := grammar.Validates(sql)
		p := parser.New(sql)
		_, parserErr := p.Parse()
		parserOK := parserErr == nil

		if grammarOK != parserOK {
			t.Errorf("DISAGREEMENT on: %q\n  grammar=%v parser=%v (parser err: %v)", sql, grammarOK, parserOK, parserErr)
		}
		if grammarOK {
			t.Errorf("GRAMMAR ACCEPTED negative case: %q", sql)
		}
	}
}
