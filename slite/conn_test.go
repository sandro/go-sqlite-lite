package slite

import (
	"errors"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Conn lifecycle
// ---------------------------------------------------------------------------

func TestNewConn(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Connection should be usable.
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (1)"))

	var row struct{ ID int64 `db:"id"` }
	if err := conn.Get(&row, "SELECT id FROM t"); err != nil {
		t.Fatal(err)
	}
	if row.ID != 1 {
		t.Errorf("got %d, want 1", row.ID)
	}
}

func TestNewConnReadonly(t *testing.T) {
	// A read-only connection should reject writes.
	conn, err := NewConn(":memory:", true)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	_, err = conn.Exec("CREATE TABLE t (id INTEGER)")
	if err == nil {
		t.Error("expected error for write on read-only connection, got nil")
	}
}

func TestNewConnBadURI(t *testing.T) {
	_, err := NewConn("/nonexistent/path/to/db.sqlite", false)
	if err == nil {
		t.Error("expected error for bad URI, got nil")
	}
}

func TestConnClose(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()

	// After close, Prepare should fail.
	_, err = conn.Prepare("SELECT 1")
	if err == nil {
		t.Error("expected error for Prepare on closed connection, got nil")
	}
}

func TestConnDoubleClose(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	conn.Close() // should not panic
}

// ---------------------------------------------------------------------------
// Exec with result values
// ---------------------------------------------------------------------------

func TestExecLastInsertId(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)"))

	res, err := conn.Exec("INSERT INTO t (name) VALUES (?)", "alice")
	if err != nil {
		t.Fatal(err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Errorf("LastInsertId = %d, want 1", id)
	}

	res, err = conn.Exec("INSERT INTO t (name) VALUES (?)", "bob")
	if err != nil {
		t.Fatal(err)
	}
	id, err = res.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	if id != 2 {
		t.Errorf("LastInsertId = %d, want 2", id)
	}
}

func TestExecRowsAffected(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER, name TEXT)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')"))

	res, err := conn.Exec("UPDATE t SET name = 'updated' WHERE id <= ?", 2)
	if err != nil {
		t.Fatal(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("RowsAffected = %d, want 2", n)
	}

	// Delete all rows.
	res, err = conn.Exec("DELETE FROM t")
	if err != nil {
		t.Fatal(err)
	}
	n, err = res.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("RowsAffected = %d, want 3", n)
	}
}

func TestExecError(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	_, err = conn.Exec("INSERT INTO nonexistent VALUES (1)")
	if err == nil {
		t.Error("expected error for bad SQL, got nil")
	}
}

// ---------------------------------------------------------------------------
// Prepare caching
// ---------------------------------------------------------------------------

func TestPrepareCaching(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER)"))

	sql := "INSERT INTO t VALUES (?)"
	stmt1, err := conn.Prepare(sql)
	if err != nil {
		t.Fatal(err)
	}
	stmt2, err := conn.Prepare(sql)
	if err != nil {
		t.Fatal(err)
	}
	// Same SQL should return the same cached statement.
	if stmt1 != stmt2 {
		t.Error("Prepare should return the same cached statement for the same SQL")
	}

	// Different SQL should return a different statement.
	stmt3, err := conn.Prepare("SELECT id FROM t")
	if err != nil {
		t.Fatal(err)
	}
	if stmt3 == stmt1 {
		t.Error("Prepare should return a different statement for different SQL")
	}
}

// ---------------------------------------------------------------------------
// Begin / Commit / Rollback (manual transactions)
// ---------------------------------------------------------------------------

func TestManualTransactionCommit(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER)"))

	if err := conn.Begin(); err != nil {
		t.Fatal(err)
	}
	mustRes(conn.Exec("INSERT INTO t VALUES (1)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (2)"))
	if err := conn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Data should be visible after commit.
	var row struct{ ID int64 `db:"id"` }
	if err := conn.Get(&row, "SELECT id FROM t ORDER BY id LIMIT 1"); err != nil {
		t.Fatal(err)
	}
	if row.ID != 1 {
		t.Errorf("got %d, want 1", row.ID)
	}
}

func TestManualTransactionRollback(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (1)")) // outside transaction

	if err := conn.Begin(); err != nil {
		t.Fatal(err)
	}
	mustRes(conn.Exec("INSERT INTO t VALUES (2)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (3)"))
	if err := conn.Rollback(); err != nil {
		t.Fatal(err)
	}

	// Only the pre-transaction row should remain.
	type Row struct{ ID int64 `db:"id"` }
	var rows []Row
	if err := conn.Select(&rows, "SELECT id FROM t"); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1 (rollback should have discarded 2 inserts)", len(rows))
	}
	if rows[0].ID != 1 {
		t.Errorf("remaining row = %d, want 1", rows[0].ID)
	}
}

// ---------------------------------------------------------------------------
// Get
// ---------------------------------------------------------------------------

func TestGetSuccess(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (name TEXT, age INTEGER)"))
	mustRes(conn.Exec("INSERT INTO t VALUES ('alice', 30)"))

	type Person struct {
		Name string `db:"name"`
		Age  int64  `db:"age"`
	}
	var p Person
	if err := conn.Get(&p, "SELECT name, age FROM t WHERE name = ?", "alice"); err != nil {
		t.Fatal(err)
	}
	if p.Name != "alice" || p.Age != 30 {
		t.Errorf("got %+v, want {Name:alice Age:30}", p)
	}
}

func TestGetErrNoRows(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER)"))

	var row struct{ ID int64 `db:"id"` }
	err = conn.Get(&row, "SELECT id FROM t WHERE id = 999")
	if !errors.Is(err, ErrNoRows) {
		t.Errorf("expected ErrNoRows, got %v", err)
	}
}

func TestGetNonPointerError(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (1)"))

	type Row struct{ ID int64 `db:"id"` }
	var row Row
	err = conn.Get(row, "SELECT id FROM t") // not a pointer
	if err == nil {
		t.Error("expected error for non-pointer dest, got nil")
	}
}

func TestGetNonStructError(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (1)"))

	var id int64
	err = conn.Get(&id, "SELECT id FROM t") // pointer to non-struct
	if err == nil {
		t.Error("expected error for non-struct pointer dest, got nil")
	}
}

// ---------------------------------------------------------------------------
// Select
// ---------------------------------------------------------------------------

func TestSelectSuccess(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (name TEXT, age INTEGER)"))
	mustRes(conn.Exec("INSERT INTO t VALUES ('alice', 30), ('bob', 25), ('charlie', 35)"))

	type Person struct {
		Name string `db:"name"`
		Age  int64  `db:"age"`
	}
	var people []Person
	if err := conn.Select(&people, "SELECT name, age FROM t ORDER BY age"); err != nil {
		t.Fatal(err)
	}
	if len(people) != 3 {
		t.Fatalf("got %d people, want 3", len(people))
	}
	if people[0].Name != "bob" || people[0].Age != 25 {
		t.Errorf("person 0 = %+v, want {bob 25}", people[0])
	}
	if people[2].Name != "charlie" || people[2].Age != 35 {
		t.Errorf("person 2 = %+v, want {charlie 35}", people[2])
	}
}

func TestSelectEmpty(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER)"))

	type Row struct{ ID int64 `db:"id"` }
	var rows []Row
	if err := conn.Select(&rows, "SELECT id FROM t"); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Errorf("got %d rows from empty table, want 0", len(rows))
	}
}

func TestSelectOverwritesExistingSlice(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (1), (2)"))

	type Row struct{ ID int64 `db:"id"` }

	// Pre-fill the slice with stale data.
	rows := []Row{{ID: 99}, {ID: 100}, {ID: 101}}

	if err := conn.Select(&rows, "SELECT id FROM t ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	// Select should have overwritten, not appended.
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2 (Select should overwrite)", len(rows))
	}
	if rows[0].ID != 1 || rows[1].ID != 2 {
		t.Errorf("got %+v, want [{1} {2}]", rows)
	}
}

func TestSelectWithArgs(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER, name TEXT)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')"))

	type Row struct {
		ID   int64  `db:"id"`
		Name string `db:"name"`
	}
	var rows []Row
	if err := conn.Select(&rows, "SELECT id, name FROM t WHERE id > ? ORDER BY id", 1); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	if rows[0].Name != "b" || rows[1].Name != "c" {
		t.Errorf("got %+v", rows)
	}
}

// ---------------------------------------------------------------------------
// RawConn escape hatch
// ---------------------------------------------------------------------------

func TestRawConn(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	raw := conn.RawConn()
	if raw == nil {
		t.Fatal("RawConn returned nil")
	}
	// Should be usable for low-level operations.
	if err := raw.Exec("CREATE TABLE t (id INTEGER)"); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// Select type validation (fixes panic on wrong dest type)
// ---------------------------------------------------------------------------

func TestSelectNonPointerError(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER)"))

	type Row struct{ ID int64 `db:"id"` }
	var rows []Row
	err = conn.Select(rows, "SELECT id FROM t") // not a pointer
	if err == nil {
		t.Error("expected error for non-pointer dest, got nil")
	}
}

func TestSelectPointerToNonSliceError(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (1)"))

	type Row struct{ ID int64 `db:"id"` }
	var row Row
	err = conn.Select(&row, "SELECT id FROM t") // pointer to struct, not slice
	if err == nil {
		t.Error("expected error for pointer-to-struct dest, got nil")
	}
}

// ---------------------------------------------------------------------------
// Get scalar hint
// ---------------------------------------------------------------------------

func TestGetScalarHint(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (1)"))

	var count int64
	err = conn.Get(&count, "SELECT COUNT(*) FROM t")
	if err == nil {
		t.Fatal("expected error for scalar dest")
	}
	if !strings.Contains(err.Error(), "row.Int64()") {
		t.Errorf("error should suggest row.Int64(), got: %s", err)
	}
}
