package slite

import (
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// SetDefaultLogger — global logger
// ---------------------------------------------------------------------------

func TestSetDefaultLoggerGlobal(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var called int
	var lastSQL string
	SetDefaultLogger(func(sql string, args []interface{}, elapsed time.Duration, err error) {
		called++
		lastSQL = sql
	})
	defer SetDefaultLogger(nil)

	mustRes(conn.Exec("CREATE TABLE t (id INTEGER)"))
	if called == 0 {
		t.Error("global logger was not called")
	}
	if lastSQL != "CREATE TABLE t (id INTEGER)" {
		t.Errorf("lastSQL = %q", lastSQL)
	}
}

// ---------------------------------------------------------------------------
// Per-connection logger takes precedence over global
// ---------------------------------------------------------------------------

func TestPerConnLoggerPrecedence(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var globalCalled int
	var connCalled int

	SetDefaultLogger(func(sql string, args []interface{}, elapsed time.Duration, err error) {
		globalCalled++
	})
	defer SetDefaultLogger(nil)

	conn.SetLogger(func(sql string, args []interface{}, elapsed time.Duration, err error) {
		connCalled++
	})

	mustRes(conn.Exec("CREATE TABLE t (id INTEGER)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (1)"))

	if connCalled != 2 {
		t.Errorf("per-conn logger called %d times, want 2", connCalled)
	}
	if globalCalled != 0 {
		t.Errorf("global logger called %d times, want 0 (per-conn should take precedence)", globalCalled)
	}
}

// ---------------------------------------------------------------------------
// Global logger used when per-connection is nil
// ---------------------------------------------------------------------------

func TestGlobalLoggerFallback(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var globalCalled int
	SetDefaultLogger(func(sql string, args []interface{}, elapsed time.Duration, err error) {
		globalCalled++
	})
	defer SetDefaultLogger(nil)

	// No per-connection logger set — global should be used.
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER)"))
	if globalCalled == 0 {
		t.Error("global logger should have been called as fallback")
	}

	// Set per-connection, then clear it — should fall back to global again.
	globalCalled = 0
	conn.SetLogger(func(sql string, args []interface{}, elapsed time.Duration, err error) {
		// per-conn logger
	})
	mustRes(conn.Exec("INSERT INTO t VALUES (1)"))
	if globalCalled != 0 {
		t.Error("global logger should NOT have been called while per-conn is set")
	}

	conn.SetLogger(nil) // clear per-conn
	mustRes(conn.Exec("INSERT INTO t VALUES (2)"))
	if globalCalled == 0 {
		t.Error("global logger should have been called after per-conn was cleared")
	}
}

// ---------------------------------------------------------------------------
// Logger disabled when both are nil (no-op, no panic)
// ---------------------------------------------------------------------------

func TestLoggerDisabledWhenNil(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	SetDefaultLogger(nil)
	conn.SetLogger(nil)

	// Should not panic when no logger is set.
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (1)"))

	type Row struct{ ID int64 `db:"id"` }
	var row Row
	if err := conn.Get(&row, "SELECT id FROM t"); err != nil {
		t.Fatal(err)
	}
	if row.ID != 1 {
		t.Errorf("got %d, want 1", row.ID)
	}
}

// ---------------------------------------------------------------------------
// Logger receives correct SQL, args, elapsed, and error
// ---------------------------------------------------------------------------

func TestLoggerReceivesCorrectData(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	SetDefaultLogger(nil)

	type logEntry struct {
		sql     string
		args    []interface{}
		elapsed time.Duration
		err     error
	}
	var entries []logEntry
	conn.SetLogger(func(sql string, args []interface{}, elapsed time.Duration, err error) {
		entries = append(entries, logEntry{sql, args, elapsed, err})
	})

	mustRes(conn.Exec("CREATE TABLE t (name TEXT, age INTEGER)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (?, ?)", "alice", 30))
	_, _ = conn.Exec("INSERT INTO nonexistent VALUES (1)") // error case

	if len(entries) != 3 {
		t.Fatalf("got %d log entries, want 3", len(entries))
	}

	// Entry 0: CREATE TABLE
	if entries[0].sql != "CREATE TABLE t (name TEXT, age INTEGER)" {
		t.Errorf("entry 0 sql = %q", entries[0].sql)
	}
	if entries[0].err != nil {
		t.Errorf("entry 0 err = %v, want nil", entries[0].err)
	}
	if entries[0].elapsed <= 0 {
		t.Errorf("entry 0 elapsed = %v, want > 0", entries[0].elapsed)
	}

	// Entry 1: INSERT with args
	if entries[1].sql != "INSERT INTO t VALUES (?, ?)" {
		t.Errorf("entry 1 sql = %q", entries[1].sql)
	}
	if len(entries[1].args) != 2 {
		t.Errorf("entry 1 args = %v, want 2 args", entries[1].args)
	}
	if entries[1].err != nil {
		t.Errorf("entry 1 err = %v, want nil", entries[1].err)
	}

	// Entry 2: failed INSERT
	if entries[2].err == nil {
		t.Error("entry 2 err = nil, want non-nil for failed query")
	}
}

// ---------------------------------------------------------------------------
// Logger works through pool operations
// ---------------------------------------------------------------------------

func TestLoggerViaPool(t *testing.T) {
	p := newTestPool(t, 1)

	var writerCalled int
	p.WithWriter(func(c *Conn) error {
		c.SetLogger(func(sql string, args []interface{}, elapsed time.Duration, err error) {
			writerCalled++
		})
		mustRes(c.Exec("CREATE TABLE t (id INTEGER)"))
		mustRes(c.Exec("INSERT INTO t VALUES (1)"))
		return nil
	})
	if writerCalled != 2 {
		t.Errorf("writer logger called %d times, want 2", writerCalled)
	}
}
