package slite

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// newTestPool creates a file-backed DBPool for testing. File-backed is required
// because each :memory: connection gets its own database, so readers would not
// see tables created by the writer.
func newTestPool(t *testing.T, size int) *DBPool {
	t.Helper()
	dbFile := filepath.Join(t.TempDir(), "test.db")
	p, err := NewDBPool(dbFile, size)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

// ---------------------------------------------------------------------------
// DBPool lifecycle
// ---------------------------------------------------------------------------

func TestNewDBPool(t *testing.T) {
	p := newTestPool(t, 2)

	// Pool should be usable for reads and writes.
	mustRes(p.Exec("CREATE TABLE t (id INTEGER)"))
	mustRes(p.Exec("INSERT INTO t VALUES (1)"))

	var row struct{ ID int64 `db:"id"` }
	if err := p.Get(&row, "SELECT id FROM t"); err != nil {
		t.Fatal(err)
	}
	if row.ID != 1 {
		t.Errorf("got %d, want 1", row.ID)
	}
}

func TestDBPoolClose(t *testing.T) {
	p := newTestPool(t, 1)
	p.Close()

	// All methods should return ErrPoolClosed after Close.
	_, err := p.Exec("SELECT 1")
	if !errors.Is(err, ErrPoolClosed) {
		t.Errorf("Exec after close: got %v, want ErrPoolClosed", err)
	}

	err = p.Get(&struct{ ID int64 `db:"id"` }{}, "SELECT 1 as id")
	if !errors.Is(err, ErrPoolClosed) {
		t.Errorf("Get after close: got %v, want ErrPoolClosed", err)
	}

	err = p.Select(&[]struct{ ID int64 `db:"id"` }{}, "SELECT 1 as id")
	if !errors.Is(err, ErrPoolClosed) {
		t.Errorf("Select after close: got %v, want ErrPoolClosed", err)
	}

	err = p.Query("SELECT 1", func(row *Row) error { return nil })
	if !errors.Is(err, ErrPoolClosed) {
		t.Errorf("Query after close: got %v, want ErrPoolClosed", err)
	}
}

func TestDBPoolDoubleClose(t *testing.T) {
	p := newTestPool(t, 1)
	p.Close()
	p.Close() // should not panic
}

// ---------------------------------------------------------------------------
// WithReader / WithWriter
// ---------------------------------------------------------------------------

func TestWithReader(t *testing.T) {
	p := newTestPool(t, 2)
	mustRes(p.Exec("CREATE TABLE t (id INTEGER)"))
	mustRes(p.Exec("INSERT INTO t VALUES (1)"))

	err := p.WithReader(func(c *Conn) error {
		var row struct{ ID int64 `db:"id"` }
		if err := c.Get(&row, "SELECT id FROM t"); err != nil {
			return err
		}
		if row.ID != 1 {
			t.Errorf("got %d, want 1", row.ID)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestWithWriter(t *testing.T) {
	p := newTestPool(t, 1)
	mustRes(p.Exec("CREATE TABLE t (id INTEGER)"))

	err := p.WithWriter(func(c *Conn) error {
		mustRes(c.Exec("INSERT INTO t VALUES (1)"))
		mustRes(c.Exec("INSERT INTO t VALUES (2)"))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify both rows are visible.
	type Row struct{ ID int64 `db:"id"` }
	var rows []Row
	if err := p.Select(&rows, "SELECT id FROM t ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
}

func TestWithReaderReturnsError(t *testing.T) {
	p := newTestPool(t, 1)

	sentinelErr := errors.New("reader error")
	err := p.WithReader(func(c *Conn) error {
		return sentinelErr
	})
	if !errors.Is(err, sentinelErr) {
		t.Errorf("expected sentinelErr, got %v", err)
	}
}

func TestWithWriterReturnsError(t *testing.T) {
	p := newTestPool(t, 1)

	sentinelErr := errors.New("writer error")
	err := p.WithWriter(func(c *Conn) error {
		return sentinelErr
	})
	if !errors.Is(err, sentinelErr) {
		t.Errorf("expected sentinelErr, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// WithReaderCtx / WithWriterCtx with cancelled context
// ---------------------------------------------------------------------------

func TestWithReaderCtxCancelled(t *testing.T) {
	// Pool with size=1: hold the reader, then try WithReaderCtx with
	// an already-cancelled context.
	p := newTestPool(t, 1)

	// Exhaust the single reader.
	blocker := make(chan struct{})
	go func() {
		_ = p.WithReader(func(c *Conn) error {
			<-blocker // hold the connection
			return nil
		})
	}()
	// Give the goroutine time to take the reader.
	time.Sleep(20 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	err := p.WithReaderCtx(ctx, func(c *Conn) error {
		t.Error("callback should not have been called")
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}

	close(blocker) // release the reader
}

func TestWithWriterCtxCancelled(t *testing.T) {
	p := newTestPool(t, 1)

	// Hold the writer.
	blocker := make(chan struct{})
	go func() {
		_ = p.WithWriter(func(c *Conn) error {
			<-blocker
			return nil
		})
	}()
	time.Sleep(20 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := p.WithWriterCtx(ctx, func(c *Conn) error {
		t.Error("callback should not have been called")
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}

	close(blocker)
	// Give the cleanup goroutine time to release the mutex.
	time.Sleep(20 * time.Millisecond)
}

func TestWithReaderCtxSuccess(t *testing.T) {
	p := newTestPool(t, 1)
	mustRes(p.Exec("CREATE TABLE t (id INTEGER)"))
	mustRes(p.Exec("INSERT INTO t VALUES (42)"))

	ctx := context.Background()
	err := p.WithReaderCtx(ctx, func(c *Conn) error {
		var row struct{ ID int64 `db:"id"` }
		if err := c.Get(&row, "SELECT id FROM t"); err != nil {
			return err
		}
		if row.ID != 42 {
			t.Errorf("got %d, want 42", row.ID)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// Pool.Exec / Get / Select / Query
// ---------------------------------------------------------------------------

func TestPoolExec(t *testing.T) {
	p := newTestPool(t, 1)

	res, err := p.Exec("CREATE TABLE t (id INTEGER)")
	if err != nil {
		t.Fatal(err)
	}
	// DDL typically returns 0 rows affected.
	n, _ := res.RowsAffected()
	if n != 0 {
		t.Errorf("RowsAffected for CREATE TABLE = %d, want 0", n)
	}

	res, err = p.Exec("INSERT INTO t VALUES (?)", 1)
	if err != nil {
		t.Fatal(err)
	}
	n, _ = res.RowsAffected()
	if n != 1 {
		t.Errorf("RowsAffected for INSERT = %d, want 1", n)
	}
}

func TestPoolGet(t *testing.T) {
	p := newTestPool(t, 1)
	mustRes(p.Exec("CREATE TABLE t (name TEXT, age INTEGER)"))
	mustRes(p.Exec("INSERT INTO t VALUES ('alice', 30)"))

	type Person struct {
		Name string `db:"name"`
		Age  int64  `db:"age"`
	}
	var person Person
	if err := p.Get(&person, "SELECT name, age FROM t"); err != nil {
		t.Fatal(err)
	}
	if person.Name != "alice" || person.Age != 30 {
		t.Errorf("got %+v, want {alice 30}", person)
	}
}

func TestPoolGetErrNoRows(t *testing.T) {
	p := newTestPool(t, 1)
	mustRes(p.Exec("CREATE TABLE t (id INTEGER)"))

	var row struct{ ID int64 `db:"id"` }
	err := p.Get(&row, "SELECT id FROM t WHERE id = 999")
	if !errors.Is(err, ErrNoRows) {
		t.Errorf("expected ErrNoRows, got %v", err)
	}
}

func TestPoolSelect(t *testing.T) {
	p := newTestPool(t, 1)
	mustRes(p.Exec("CREATE TABLE t (id INTEGER, name TEXT)"))
	mustRes(p.Exec("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')"))

	type Row struct {
		ID   int64  `db:"id"`
		Name string `db:"name"`
	}
	var rows []Row
	if err := p.Select(&rows, "SELECT id, name FROM t ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(rows))
	}
	if rows[0].Name != "a" || rows[2].Name != "c" {
		t.Errorf("got %+v", rows)
	}
}

func TestPoolQuery(t *testing.T) {
	p := newTestPool(t, 1)
	mustRes(p.Exec("CREATE TABLE t (id INTEGER, name TEXT)"))
	mustRes(p.Exec("INSERT INTO t VALUES (1, 'a'), (2, 'b')"))

	var names []string
	err := p.Query("SELECT name FROM t ORDER BY id", func(row *Row) error {
		names = append(names, row.Text("name"))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 || names[0] != "a" || names[1] != "b" {
		t.Errorf("got %v, want [a b]", names)
	}
}

// ---------------------------------------------------------------------------
// Tx (commit, rollback on error, rollback on panic)
// ---------------------------------------------------------------------------

func TestTxCommit(t *testing.T) {
	p := newTestPool(t, 1)
	mustRes(p.Exec("CREATE TABLE t (id INTEGER)"))

	err := p.Tx(func(c *Conn) error {
		mustRes(c.Exec("INSERT INTO t VALUES (1)"))
		mustRes(c.Exec("INSERT INTO t VALUES (2)"))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	type Row struct{ ID int64 `db:"id"` }
	var rows []Row
	if err := p.Select(&rows, "SELECT id FROM t ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2 after Tx commit", len(rows))
	}
}

func TestTxRollbackOnError(t *testing.T) {
	p := newTestPool(t, 1)
	mustRes(p.Exec("CREATE TABLE t (id INTEGER)"))
	mustRes(p.Exec("INSERT INTO t VALUES (0)")) // pre-existing row

	sentinelErr := errors.New("abort tx")
	err := p.Tx(func(c *Conn) error {
		mustRes(c.Exec("INSERT INTO t VALUES (1)"))
		mustRes(c.Exec("INSERT INTO t VALUES (2)"))
		return sentinelErr // trigger rollback
	})
	if !errors.Is(err, sentinelErr) {
		t.Errorf("expected sentinelErr, got %v", err)
	}

	// Only the pre-existing row should remain.
	type Row struct{ ID int64 `db:"id"` }
	var rows []Row
	if err := p.Select(&rows, "SELECT id FROM t"); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1 after rollback", len(rows))
	}
	if rows[0].ID != 0 {
		t.Errorf("got id=%d, want 0", rows[0].ID)
	}
}

func TestTxRollbackOnPanic(t *testing.T) {
	p := newTestPool(t, 1)
	mustRes(p.Exec("CREATE TABLE t (id INTEGER)"))
	mustRes(p.Exec("INSERT INTO t VALUES (0)")) // pre-existing row

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic to be re-raised, got nil recover")
			}
		}()
		_ = p.Tx(func(c *Conn) error {
			mustRes(c.Exec("INSERT INTO t VALUES (1)"))
			panic("oh no")
		})
	}()

	// Only the pre-existing row should remain after panic-triggered rollback.
	type Row struct{ ID int64 `db:"id"` }
	var rows []Row
	if err := p.Select(&rows, "SELECT id FROM t"); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1 after panic-triggered rollback", len(rows))
	}
}

func TestTxOnClosedPool(t *testing.T) {
	p := newTestPool(t, 1)
	p.Close()

	err := p.Tx(func(c *Conn) error {
		return nil
	})
	if !errors.Is(err, ErrPoolClosed) {
		t.Errorf("Tx on closed pool: got %v, want ErrPoolClosed", err)
	}
}

// ---------------------------------------------------------------------------
// Pool InsertValues / UpdateValues
// ---------------------------------------------------------------------------

func TestPoolInsertValues(t *testing.T) {
	p := newTestPool(t, 1)
	mustRes(p.Exec("CREATE TABLE t (name TEXT, val INTEGER)"))

	res, err := p.InsertValues("INSERT INTO t", map[string]interface{}{
		"name": "foo",
		"val":  int64(42),
	})
	if err != nil {
		t.Fatal(err)
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		t.Errorf("RowsAffected = %d, want 1", n)
	}

	type Row struct {
		Name string `db:"name"`
		Val  int64  `db:"val"`
	}
	var row Row
	if err := p.Get(&row, "SELECT name, val FROM t"); err != nil {
		t.Fatal(err)
	}
	if row.Name != "foo" || row.Val != 42 {
		t.Errorf("got %+v, want {foo 42}", row)
	}
}

func TestPoolUpdateValues(t *testing.T) {
	p := newTestPool(t, 1)
	mustRes(p.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"))
	mustRes(p.Exec("INSERT INTO t VALUES (1, 'before')"))

	res, err := p.UpdateValues("UPDATE t", map[string]interface{}{
		"name": "after",
	}, "WHERE id = ?", int64(1))
	if err != nil {
		t.Fatal(err)
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		t.Errorf("RowsAffected = %d, want 1", n)
	}

	type Row struct {
		ID   int64  `db:"id"`
		Name string `db:"name"`
	}
	var row Row
	if err := p.Get(&row, "SELECT id, name FROM t WHERE id = 1"); err != nil {
		t.Fatal(err)
	}
	if row.Name != "after" {
		t.Errorf("got %q, want %q", row.Name, "after")
	}
}

func TestPoolInsertValuesOnClosedPool(t *testing.T) {
	p := newTestPool(t, 1)
	p.Close()

	_, err := p.InsertValues("INSERT INTO t", map[string]interface{}{"a": 1})
	if !errors.Is(err, ErrPoolClosed) {
		t.Errorf("expected ErrPoolClosed, got %v", err)
	}

	_, err = p.UpdateValues("UPDATE t", map[string]interface{}{"a": 1}, "WHERE id = ?", 1)
	if !errors.Is(err, ErrPoolClosed) {
		t.Errorf("expected ErrPoolClosed, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Concurrent pool access
// ---------------------------------------------------------------------------

func TestPoolConcurrentReaders(t *testing.T) {
	p := newTestPool(t, 4)
	mustRes(p.Exec("CREATE TABLE t (id INTEGER)"))
	for i := 1; i <= 100; i++ {
		mustRes(p.Exec("INSERT INTO t VALUES (?)", i))
	}

	var wg sync.WaitGroup
	errs := make(chan error, 20)

	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			type Row struct{ ID int64 `db:"id"` }
			var rows []Row
			if err := p.Select(&rows, "SELECT id FROM t ORDER BY id"); err != nil {
				errs <- err
				return
			}
			if len(rows) != 100 {
				errs <- errors.New("wrong row count")
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent reader error: %v", err)
	}
}

func TestPoolConcurrentWriters(t *testing.T) {
	p := newTestPool(t, 2)
	mustRes(p.Exec("CREATE TABLE t (id INTEGER)"))

	var wg sync.WaitGroup
	errs := make(chan error, 50)

	// Multiple goroutines competing for the single writer.
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if _, err := p.Exec("INSERT INTO t VALUES (?)", id); err != nil {
				errs <- err
			}
		}(g)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent writer error: %v", err)
	}

	type Row struct{ ID int64 `db:"id"` }
	var rows []Row
	if err := p.Select(&rows, "SELECT id FROM t"); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 50 {
		t.Errorf("got %d rows, want 50", len(rows))
	}
}
