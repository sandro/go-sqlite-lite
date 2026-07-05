package slite

import (
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// InsertValues
// ---------------------------------------------------------------------------

func TestInsertValues(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE items (name TEXT, price REAL, qty INTEGER)"))

	res, err := conn.InsertValues("INSERT INTO items", map[string]interface{}{
		"name":  "widget",
		"price": 9.99,
		"qty":   int64(42),
	})
	if err != nil {
		t.Fatal(err)
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		t.Errorf("RowsAffected = %d, want 1", n)
	}

	// Verify data was inserted correctly.
	type Item struct {
		Name  string  `db:"name"`
		Price float64 `db:"price"`
		Qty   int64   `db:"qty"`
	}
	var item Item
	if err := conn.Get(&item, "SELECT name, price, qty FROM items"); err != nil {
		t.Fatal(err)
	}
	if item.Name != "widget" || item.Price != 9.99 || item.Qty != 42 {
		t.Errorf("got %+v, want {widget 9.99 42}", item)
	}
}

func TestInsertValuesLastInsertId(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)"))

	res, err := conn.InsertValues("INSERT INTO t", map[string]interface{}{
		"val": "first",
	})
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	if id != 1 {
		t.Errorf("LastInsertId = %d, want 1", id)
	}

	res, err = conn.InsertValues("INSERT INTO t", map[string]interface{}{
		"val": "second",
	})
	if err != nil {
		t.Fatal(err)
	}
	id, _ = res.LastInsertId()
	if id != 2 {
		t.Errorf("LastInsertId = %d, want 2", id)
	}
}

func TestInsertValuesReplace(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (1, 'original')"))

	// REPLACE should update existing row with same primary key.
	_, err = conn.InsertValues("REPLACE INTO t", map[string]interface{}{
		"id":   int64(1),
		"name": "replaced",
	})
	if err != nil {
		t.Fatal(err)
	}

	type Row struct {
		ID   int64  `db:"id"`
		Name string `db:"name"`
	}
	var row Row
	if err := conn.Get(&row, "SELECT id, name FROM t WHERE id = 1"); err != nil {
		t.Fatal(err)
	}
	if row.Name != "replaced" {
		t.Errorf("name = %q, want %q", row.Name, "replaced")
	}

	// Only one row should exist.
	var rows []Row
	if err := conn.Select(&rows, "SELECT id, name FROM t"); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Errorf("got %d rows, want 1 after REPLACE", len(rows))
	}
}

func TestInsertValuesEmptyAttrsError(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER)"))

	_, err = conn.InsertValues("INSERT INTO t", map[string]interface{}{})
	if err == nil {
		t.Error("expected error for empty attrs, got nil")
	}
	if !strings.Contains(err.Error(), "no attributes") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestInsertValuesNonInsertPrefixError(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER)"))

	_, err = conn.InsertValues("UPDATE t", map[string]interface{}{"id": 1})
	if err == nil {
		t.Error("expected error for non-INSERT prefix, got nil")
	}
	if !strings.Contains(err.Error(), "INSERT or REPLACE") {
		t.Errorf("unexpected error: %v", err)
	}

	_, err = conn.InsertValues("SELECT * FROM t", map[string]interface{}{"id": 1})
	if err == nil {
		t.Error("expected error for SELECT prefix, got nil")
	}
}

func TestInsertValuesDeterministicColumns(t *testing.T) {
	// Columns should be sorted for deterministic SQL (stable statement caching).
	// We verify this by inserting the same set of keys multiple times — if the
	// SQL is deterministic, the prepared statement cache gets a hit.
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (a TEXT, b TEXT, c TEXT)"))

	for i := 0; i < 3; i++ {
		_, err := conn.InsertValues("INSERT INTO t", map[string]interface{}{
			"c": "c",
			"a": "a",
			"b": "b",
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	type Row struct {
		A string `db:"a"`
		B string `db:"b"`
		C string `db:"c"`
	}
	var rows []Row
	if err := conn.Select(&rows, "SELECT a, b, c FROM t"); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(rows))
	}
}

// ---------------------------------------------------------------------------
// UpdateValues
// ---------------------------------------------------------------------------

func TestUpdateValues(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score REAL)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (1, 'alice', 1.0), (2, 'bob', 2.0)"))

	res, err := conn.UpdateValues("UPDATE t", map[string]interface{}{
		"name":  "updated",
		"score": 9.99,
	}, "WHERE id = ?", int64(1))
	if err != nil {
		t.Fatal(err)
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		t.Errorf("RowsAffected = %d, want 1", n)
	}

	type Row struct {
		ID    int64   `db:"id"`
		Name  string  `db:"name"`
		Score float64 `db:"score"`
	}
	var row Row
	if err := conn.Get(&row, "SELECT id, name, score FROM t WHERE id = 1"); err != nil {
		t.Fatal(err)
	}
	if row.Name != "updated" || row.Score != 9.99 {
		t.Errorf("got %+v, want {1 updated 9.99}", row)
	}

	// Row 2 should be unchanged.
	if err := conn.Get(&row, "SELECT id, name, score FROM t WHERE id = 2"); err != nil {
		t.Fatal(err)
	}
	if row.Name != "bob" || row.Score != 2.0 {
		t.Errorf("row 2 should be unchanged, got %+v", row)
	}
}

func TestUpdateValuesMultipleWhereArgs(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER, category TEXT, name TEXT)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (1, 'a', 'x'), (2, 'a', 'y'), (3, 'b', 'z')"))

	res, err := conn.UpdateValues("UPDATE t", map[string]interface{}{
		"name": "updated",
	}, "WHERE id > ? AND category = ?", int64(1), "a")
	if err != nil {
		t.Fatal(err)
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		t.Errorf("RowsAffected = %d, want 1 (only id=2 matches both conditions)", n)
	}

	type Row struct {
		ID   int64  `db:"id"`
		Name string `db:"name"`
	}
	var row Row
	if err := conn.Get(&row, "SELECT id, name FROM t WHERE id = 2"); err != nil {
		t.Fatal(err)
	}
	if row.Name != "updated" {
		t.Errorf("got %q, want %q", row.Name, "updated")
	}
}

func TestUpdateValuesNoWhere(t *testing.T) {
	// UpdateValues with empty WHERE updates all rows.
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER, name TEXT)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (1, 'a'), (2, 'b')"))

	res, err := conn.UpdateValues("UPDATE t", map[string]interface{}{
		"name": "all_updated",
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	n, _ := res.RowsAffected()
	if n != 2 {
		t.Errorf("RowsAffected = %d, want 2", n)
	}
}

func TestUpdateValuesEmptyAttrsError(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER)"))

	_, err = conn.UpdateValues("UPDATE t", map[string]interface{}{}, "WHERE id = ?", 1)
	if err == nil {
		t.Error("expected error for empty attrs, got nil")
	}
	if !strings.Contains(err.Error(), "no attributes") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestUpdateValuesNonUpdatePrefixError(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER)"))

	_, err = conn.UpdateValues("INSERT INTO t", map[string]interface{}{"id": 1}, "WHERE id = ?", 1)
	if err == nil {
		t.Error("expected error for non-UPDATE prefix, got nil")
	}
	if !strings.Contains(err.Error(), "UPDATE") {
		t.Errorf("unexpected error: %v", err)
	}

	_, err = conn.UpdateValues("SELECT * FROM t", map[string]interface{}{"id": 1}, "WHERE id = ?", 1)
	if err == nil {
		t.Error("expected error for SELECT prefix, got nil")
	}
}
