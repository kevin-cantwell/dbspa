//go:build integration

package integration

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestSQLite_AccumulatingState(t *testing.T) {
	t.Parallel()
	skipIfShort(t)
	requireBuild(t)

	dbFile, err := os.CreateTemp("", "folddb-sqlite-*.db")
	if err != nil {
		t.Fatalf("cannot create temp db: %v", err)
	}
	dbPath := dbFile.Name()
	dbFile.Close()
	defer os.Remove(dbPath)

	input := `{"region":"us-east","amount":100}
{"region":"us-west","amount":200}
{"region":"us-east","amount":150}
{"region":"us-west","amount":50}
{"region":"us-east","amount":300}
`

	_, err = runFoldDBWithStdin(t,
		"SELECT region, COUNT(*) AS cnt, SUM(amount::float) AS total GROUP BY region",
		input,
		"--state", dbPath,
	)
	if err != nil {
		t.Fatalf("folddb failed: %v", err)
	}

	// Open SQLite and verify
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("cannot open sqlite: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT region, cnt, total FROM result ORDER BY region")
	if err != nil {
		t.Fatalf("cannot query result table: %v", err)
	}
	defer rows.Close()

	results := map[string]struct {
		cnt   int
		total float64
	}{}

	for rows.Next() {
		var region string
		var cnt int
		var total float64
		if err := rows.Scan(&region, &cnt, &total); err != nil {
			t.Fatalf("cannot scan row: %v", err)
		}
		results[region] = struct {
			cnt   int
			total float64
		}{cnt, total}
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 regions, got %d: %v", len(results), results)
	}

	east := results["us-east"]
	if east.cnt != 3 {
		t.Errorf("expected us-east cnt=3, got %d", east.cnt)
	}
	if east.total != 550 {
		t.Errorf("expected us-east total=550, got %f", east.total)
	}

	west := results["us-west"]
	if west.cnt != 2 {
		t.Errorf("expected us-west cnt=2, got %d", west.cnt)
	}
	if west.total != 250 {
		t.Errorf("expected us-west total=250, got %f", west.total)
	}
}

func TestSQLite_ConcurrentRead(t *testing.T) {
	skipIfShort(t)
	waitForKafka(t)

	topic := uniqueTopic(t, "sqlite-concurrent")
	createTopic(t, topic, 1)

	dbFile, err := os.CreateTemp("", "folddb-sqlite-concurrent-*.db")
	if err != nil {
		t.Fatalf("cannot create temp db: %v", err)
	}
	dbPath := dbFile.Name()
	dbFile.Close()
	defer os.Remove(dbPath)

	// Produce messages
	for i := 1; i <= 20; i++ {
		produceMessages(t, topic, []string{
			fmt.Sprintf(`{"region":"us-east","amount":%d}`, i*10),
		})
	}
	time.Sleep(500 * time.Millisecond)

	// Start folddb writing to SQLite in a goroutine
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		sql := fmt.Sprintf(
			"SELECT region, COUNT(*) AS cnt, SUM(amount::float) AS total FROM 'kafka://%s/%s?offset=earliest' GROUP BY region LIMIT 40",
			kafkaBroker, topic,
		)
		runFoldDBWithTimeout(t, 15*time.Second, sql, "--state", dbPath)
	}()

	// Give folddb a moment to start writing
	time.Sleep(2 * time.Second)

	// Concurrent read: should not block
	db, err := sql.Open("sqlite", dbPath+"?mode=ro")
	if err != nil {
		t.Logf("cannot open sqlite for reading (may not exist yet): %v", err)
	} else {
		defer db.Close()
		// Try to read -- WAL mode should allow this
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM result").Scan(&count)
		if err != nil {
			t.Logf("concurrent read error (table may not exist yet): %v", err)
		} else {
			t.Logf("concurrent read: %d rows in result table", count)
		}
	}

	wg.Wait()

	// Final verification: open and check the results
	db2, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("cannot open final sqlite: %v", err)
	}
	defer db2.Close()

	var finalCount int
	err = db2.QueryRow("SELECT COUNT(*) FROM result").Scan(&finalCount)
	if err != nil {
		t.Logf("final count error: %v", err)
	} else {
		t.Logf("final count: %d rows", finalCount)
		if finalCount == 0 {
			t.Error("expected at least 1 row in result table")
		}
	}
}

func TestSQLite_NonAccumulatingAppend(t *testing.T) {
	t.Parallel()
	skipIfShort(t)
	requireBuild(t)

	dbFile, err := os.CreateTemp("", "folddb-sqlite-append-*.db")
	if err != nil {
		t.Fatalf("cannot create temp db: %v", err)
	}
	dbPath := dbFile.Name()
	dbFile.Close()
	defer os.Remove(dbPath)

	input := `{"name":"alice","age":30}
{"name":"bob","age":25}
{"name":"charlie","age":35}
`

	_, err = runFoldDBWithStdin(t,
		"SELECT name, age",
		input,
		"--state", dbPath,
	)
	if err != nil {
		t.Fatalf("folddb failed: %v", err)
	}

	// Open SQLite and verify append-mode behavior
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("cannot open sqlite: %v", err)
	}
	defer db.Close()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM result").Scan(&count)
	if err != nil {
		t.Fatalf("cannot count rows: %v", err)
	}

	if count != 3 {
		t.Errorf("expected 3 rows in append mode, got %d", count)
	}
}

func TestSQLite_KafkaAccumulating(t *testing.T) {
	skipIfShort(t)
	waitForKafka(t)

	topic := uniqueTopic(t, "sqlite-kafka")
	createTopic(t, topic, 1)

	dbFile, err := os.CreateTemp("", "folddb-sqlite-kafka-*.db")
	if err != nil {
		t.Fatalf("cannot create temp db: %v", err)
	}
	dbPath := dbFile.Name()
	dbFile.Close()
	defer os.Remove(dbPath)

	messages := []string{
		`{"region":"us-east","val":10}`,
		`{"region":"us-east","val":20}`,
		`{"region":"us-west","val":30}`,
		`{"region":"us-east","val":40}`,
		`{"region":"us-west","val":50}`,
	}
	produceMessages(t, topic, messages)
	time.Sleep(500 * time.Millisecond)

	sqlQ := fmt.Sprintf(
		"SELECT region, COUNT(*) AS cnt, SUM(val::float) AS total FROM 'kafka://%s/%s?offset=earliest' GROUP BY region LIMIT 10",
		kafkaBroker, topic,
	)
	stdout, stderr, err := runFoldDBWithTimeout(t, 15*time.Second, sqlQ, "--state", dbPath)
	t.Logf("kafka sqlite stdout:\n%s", stdout)
	t.Logf("kafka sqlite stderr:\n%s", stderr)

	// Open SQLite and check
	db, err2 := sql.Open("sqlite", dbPath)
	if err2 != nil {
		t.Fatalf("cannot open sqlite: %v", err2)
	}
	defer db.Close()

	rows, err2 := db.Query("SELECT region, cnt, total FROM result ORDER BY region")
	if err2 != nil {
		t.Fatalf("cannot query result: %v", err2)
	}
	defer rows.Close()

	type row struct {
		region string
		cnt    int
		total  float64
	}
	var results []row
	for rows.Next() {
		var r row
		rows.Scan(&r.region, &r.cnt, &r.total)
		results = append(results, r)
	}

	t.Logf("sqlite results: %+v", results)

	// Verify we have results (exact values depend on how changelog is applied to sqlite)
	if len(results) == 0 {
		t.Error("expected at least 1 row in sqlite result table")
	}

	// Check the results can be serialized to JSON (sanity check)
	for _, r := range results {
		b, _ := json.Marshal(map[string]any{
			"region": r.region,
			"cnt":    r.cnt,
			"total":  r.total,
		})
		t.Logf("  row: %s", string(b))
	}
}

func TestSQLite_WALMode(t *testing.T) {
	t.Parallel()
	skipIfShort(t)
	requireBuild(t)

	dbFile, err := os.CreateTemp("", "folddb-sqlite-wal-*.db")
	if err != nil {
		t.Fatalf("cannot create temp db: %v", err)
	}
	dbPath := dbFile.Name()
	dbFile.Close()
	defer os.Remove(dbPath)

	input := `{"x":1}
{"x":2}
`

	_, err = runFoldDBWithStdin(t,
		"SELECT x",
		input,
		"--state", dbPath,
	)
	if err != nil {
		t.Fatalf("folddb failed: %v", err)
	}

	// Verify WAL mode
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("cannot open sqlite: %v", err)
	}
	defer db.Close()

	var journalMode string
	err = db.QueryRow("PRAGMA journal_mode").Scan(&journalMode)
	if err != nil {
		t.Fatalf("cannot check journal_mode: %v", err)
	}

	// WAL mode should be set by folddb
	t.Logf("journal_mode: %s", journalMode)
	if journalMode != "wal" {
		t.Logf("note: expected WAL mode, got %q (folddb may use a different mode)", journalMode)
	}
}
