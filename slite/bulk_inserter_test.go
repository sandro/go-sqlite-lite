package slite

import (
	"errors"
	"testing"
)

// ---------------------------------------------------------------------------
// BulkInserter basics
// ---------------------------------------------------------------------------

func TestBulkInserterBasic(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE items (name TEXT, price REAL)"))

	bi := NewBulkInserter("INSERT INTO items (name, price)", "", conn)

	// Add several rows — each Add is (name, price) = 2 args.
	if err := bi.Add("widget", 9.99); err != nil {
		t.Fatal(err)
	}
	if err := bi.Add("gadget", 19.99); err != nil {
		t.Fatal(err)
	}
	if err := bi.Add("doohickey", 4.99); err != nil {
		t.Fatal(err)
	}
	if err := bi.Done(); err != nil {
		t.Fatal(err)
	}

	type Item struct {
		Name  string  `db:"name"`
		Price float64 `db:"price"`
	}
	var items []Item
	if err := conn.Select(&items, "SELECT name, price FROM items ORDER BY price"); err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 {
		t.Fatalf("got %d items, want 3", len(items))
	}
	if items[0].Name != "doohickey" || items[0].Price != 4.99 {
		t.Errorf("item 0 = %+v, want {doohickey 4.99}", items[0])
	}
	if items[2].Name != "gadget" || items[2].Price != 19.99 {
		t.Errorf("item 2 = %+v, want {gadget 19.99}", items[2])
	}
}

// ---------------------------------------------------------------------------
// Auto-flush at bind limit
// ---------------------------------------------------------------------------

func TestBulkInserterAutoFlush(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (a INTEGER, b INTEGER)"))

	bi := NewBulkInserter("INSERT INTO t (a, b)", "", conn)
	// Override size to a small value to trigger auto-flush without inserting
	// thousands of rows.
	bi.size = 10 // 10 bind slots → fits 5 rows of 2 args each

	// Add 5 rows (10 args) → fills exactly to the limit.
	for i := 0; i < 5; i++ {
		if err := bi.Add(i, i*10); err != nil {
			t.Fatal(err)
		}
	}

	// Nothing flushed yet (count == size, not > size). Verify by checking count.
	bi.mu.Lock()
	if bi.count != 10 {
		t.Errorf("count = %d, want 10 before overflow", bi.count)
	}
	bi.mu.Unlock()

	// Add one more row → exceeds limit, triggering auto-flush of the first 5.
	if err := bi.Add(5, 50); err != nil {
		t.Fatal(err)
	}

	// After auto-flush, the buffer should only contain the new row.
	bi.mu.Lock()
	if bi.count != 2 { // one row of 2 args
		t.Errorf("count after auto-flush = %d, want 2", bi.count)
	}
	bi.mu.Unlock()

	// The first 5 rows should already be in the database.
	type Row struct {
		A int64 `db:"a"`
		B int64 `db:"b"`
	}
	var rows []Row
	if err := conn.Select(&rows, "SELECT a, b FROM t ORDER BY a"); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 5 {
		t.Fatalf("got %d rows after auto-flush, want 5", len(rows))
	}

	// Done flushes the remaining row.
	if err := bi.Done(); err != nil {
		t.Fatal(err)
	}
	if err := conn.Select(&rows, "SELECT a, b FROM t ORDER BY a"); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 6 {
		t.Fatalf("got %d rows after Done, want 6", len(rows))
	}
}

// ---------------------------------------------------------------------------
// Done flushes remaining
// ---------------------------------------------------------------------------

func TestBulkInserterDoneFlushesRemaining(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (val TEXT)"))

	bi := NewBulkInserter("INSERT INTO t (val)", "", conn)
	if err := bi.Add("one"); err != nil {
		t.Fatal(err)
	}
	if err := bi.Add("two"); err != nil {
		t.Fatal(err)
	}

	// Before Done, no rows in the database (buffer hasn't flushed).
	type Row struct{ Val string `db:"val"` }
	var rows []Row
	if err := conn.Select(&rows, "SELECT val FROM t"); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected 0 rows before Done, got %d", len(rows))
	}

	// After Done, all buffered rows are flushed.
	if err := bi.Done(); err != nil {
		t.Fatal(err)
	}
	if err := conn.Select(&rows, "SELECT val FROM t ORDER BY val"); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows after Done, want 2", len(rows))
	}
	if rows[0].Val != "one" || rows[1].Val != "two" {
		t.Errorf("got %+v, want [{one} {two}]", rows)
	}
}

func TestBulkInserterDoneOnEmpty(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (val TEXT)"))

	bi := NewBulkInserter("INSERT INTO t (val)", "", conn)
	// Done on an empty buffer should be a no-op.
	if err := bi.Done(); err != nil {
		t.Fatalf("Done on empty buffer returned error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// onConflict clause
// ---------------------------------------------------------------------------

func TestBulkInserterOnConflict(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (1, 'original')"))

	bi := NewBulkInserter(
		"INSERT INTO t (id, name)",
		"ON CONFLICT(id) DO UPDATE SET name = excluded.name",
		conn,
	)
	// Insert a new row and upsert an existing row.
	if err := bi.Add(int64(1), "updated"); err != nil {
		t.Fatal(err)
	}
	if err := bi.Add(int64(2), "brand_new"); err != nil {
		t.Fatal(err)
	}
	if err := bi.Done(); err != nil {
		t.Fatal(err)
	}

	type Row struct {
		ID   int64  `db:"id"`
		Name string `db:"name"`
	}
	var rows []Row
	if err := conn.Select(&rows, "SELECT id, name FROM t ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	if rows[0].ID != 1 || rows[0].Name != "updated" {
		t.Errorf("row 0 = %+v, want {1 updated}", rows[0])
	}
	if rows[1].ID != 2 || rows[1].Name != "brand_new" {
		t.Errorf("row 1 = %+v, want {2 brand_new}", rows[1])
	}
}

// ---------------------------------------------------------------------------
// ErrArgsGreaterThanSize
// ---------------------------------------------------------------------------

func TestBulkInserterErrArgsGreaterThanSize(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER)"))

	bi := NewBulkInserter("INSERT INTO t (a, b, c)", "", conn)
	bi.size = 2 // only 2 bind slots

	// Trying to add 3 args when size is 2 should fail.
	err = bi.Add(1, 2, 3)
	if !errors.Is(err, ErrArgsGreaterThanSize) {
		t.Errorf("expected ErrArgsGreaterThanSize, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Empty Add is a no-op
// ---------------------------------------------------------------------------

func TestBulkInserterEmptyAddIsNoOp(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (val TEXT)"))

	bi := NewBulkInserter("INSERT INTO t (val)", "", conn)
	// Add with no arguments should be a no-op.
	if err := bi.Add(); err != nil {
		t.Fatalf("Add() with no args returned error: %v", err)
	}

	bi.mu.Lock()
	count := bi.count
	cmds := len(bi.cmds)
	bi.mu.Unlock()

	if count != 0 {
		t.Errorf("count = %d after empty Add, want 0", count)
	}
	if cmds != 0 {
		t.Errorf("cmds = %d after empty Add, want 0", cmds)
	}

	// Done should also be a no-op.
	if err := bi.Done(); err != nil {
		t.Fatalf("Done after empty Add returned error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// BulkInserter with pool via WithWriter
// ---------------------------------------------------------------------------

func TestBulkInserterWithPool(t *testing.T) {
	p := newTestPool(t, 1)
	mustRes(p.Exec("CREATE TABLE t (id INTEGER, name TEXT)"))

	// BulkInserter should be used inside WithWriter to get a *Conn.
	err := p.WithWriter(func(c *Conn) error {
		bi := NewBulkInserter("INSERT INTO t (id, name)", "", c)
		for i := 0; i < 100; i++ {
			if err := bi.Add(int64(i), "row"); err != nil {
				return err
			}
		}
		return bi.Done()
	})
	if err != nil {
		t.Fatal(err)
	}

	type Row struct {
		ID   int64  `db:"id"`
		Name string `db:"name"`
	}
	var rows []Row
	if err := p.Select(&rows, "SELECT id, name FROM t"); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 100 {
		t.Errorf("got %d rows, want 100", len(rows))
	}
}
