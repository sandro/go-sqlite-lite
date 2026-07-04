package slite

import (
	"errors"
	"testing"
)

func TestQueryRowAccessors(t *testing.T) {
	conn, err := NewConn("file::memory:?cache=shared", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE IF NOT EXISTS qtest (id INTEGER, name TEXT, score REAL, data BLOB)"))
	mustRes(conn.Exec("INSERT INTO qtest VALUES (42, 'alice', 3.14, x'deadbeef')"))
	mustRes(conn.Exec("INSERT INTO qtest VALUES (99, 'bob', 2.71, NULL)"))

	var seen int
	err = conn.Query("SELECT id, name, score, data FROM qtest ORDER BY id", func(row *Row) error {
		seen++
		if row.ColumnCount() != 4 {
			t.Errorf("row %d: ColumnCount = %d, want 4", seen, row.ColumnCount())
		}
		names := row.ColumnNames()
		if len(names) != 4 || names[0] != "id" || names[1] != "name" {
			t.Errorf("row %d: ColumnNames = %v", seen, names)
		}
		switch seen {
		case 1:
			if row.Int("id") != 42 {
				t.Errorf("row 1: Int(id) = %d, want 42", row.Int("id"))
			}
			if row.Text("name") != "alice" {
				t.Errorf("row 1: Text(name) = %q, want %q", row.Text("name"), "alice")
			}
			if row.Float("score") != 3.14 {
				t.Errorf("row 1: Float(score) = %f, want 3.14", row.Float("score"))
			}
			if len(row.Blob("data")) != 4 {
				t.Errorf("row 1: len(Blob(data)) = %d, want 4", len(row.Blob("data")))
			}
			if row.Value("id") != int64(42) {
				t.Errorf("row 1: Value(id) = %v, want 42", row.Value("id"))
			}
			if row.Value("name") != "alice" {
				t.Errorf("row 1: Value(name) = %v, want alice", row.Value("name"))
			}
			if row.IsNull("data") {
				t.Error("row 1: IsNull(data) = true, want false")
			}
		case 2:
			if row.Int("id") != 99 {
				t.Errorf("row 2: Int(id) = %d, want 99", row.Int("id"))
			}
			if !row.IsNull("data") {
				t.Error("row 2: IsNull(data) = false, want true")
			}
			if row.Value("data") != nil {
				t.Errorf("row 2: Value(data) = %v, want nil", row.Value("data"))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if seen != 2 {
		t.Fatalf("saw %d rows, want 2", seen)
	}
}

func TestQueryCaseInsensitive(t *testing.T) {
	conn, err := NewConn("file::memory:?cache=shared", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE IF NOT EXISTS citest (MyColumn TEXT)"))
	mustRes(conn.Exec("INSERT INTO citest VALUES ('hello')"))

	err = conn.Query("SELECT MyColumn FROM citest", func(row *Row) error {
		// Case-insensitive lookup should work
		if row.Text("mycolumn") != "hello" {
			t.Errorf("Text(mycolumn) = %q, want hello", row.Text("mycolumn"))
		}
		if row.Text("MYCOLUMN") != "hello" {
			t.Errorf("Text(MYCOLUMN) = %q, want hello", row.Text("MYCOLUMN"))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestQueryMissingColumn(t *testing.T) {
	conn, err := NewConn("file::memory:?cache=shared", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE IF NOT EXISTS mtest (id INTEGER)"))
	mustRes(conn.Exec("INSERT INTO mtest VALUES (1)"))

	err = conn.Query("SELECT id FROM mtest", func(row *Row) error {
		if row.Int("nonexistent") != 0 {
			t.Errorf("Int(nonexistent) = %d, want 0", row.Int("nonexistent"))
		}
		if row.Text("nonexistent") != "" {
			t.Errorf("Text(nonexistent) = %q, want empty", row.Text("nonexistent"))
		}
		if row.Value("nonexistent") != nil {
			t.Errorf("Value(nonexistent) = %v, want nil", row.Value("nonexistent"))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestQueryStopsOnError(t *testing.T) {
	conn, err := NewConn("file::memory:?cache=shared", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE IF NOT EXISTS etest (id INTEGER)"))
	for i := 0; i < 10; i++ {
		mustRes(conn.Exec("INSERT INTO etest VALUES (?)", i))
	}

	stopErr := errors.New("stop")
	var seen int
	err = conn.Query("SELECT id FROM etest ORDER BY id", func(row *Row) error {
		seen++
		if seen == 3 {
			return stopErr
		}
		return nil
	})
	if !errors.Is(err, stopErr) {
		t.Errorf("Query error = %v, want %v", err, stopErr)
	}
	if seen != 3 {
		t.Errorf("saw %d rows before stopping, want 3", seen)
	}
}

func TestQueryNoRows(t *testing.T) {
	conn, err := NewConn("file::memory:?cache=shared", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE IF NOT EXISTS nrtest (id INTEGER)"))

	var called int
	err = conn.Query("SELECT id FROM nrtest", func(row *Row) error {
		called++
		return nil
	})
	if err != nil {
		t.Fatalf("Query on empty table returned error: %v", err)
	}
	if called != 0 {
		t.Errorf("callback called %d times on empty result, want 0", called)
	}
}

func TestQueryScan(t *testing.T) {
	conn, err := NewConn("file::memory:?cache=shared", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE IF NOT EXISTS scantest (name TEXT, age INTEGER)"))
	mustRes(conn.Exec("INSERT INTO scantest VALUES ('alice', 30)"))
	mustRes(conn.Exec("INSERT INTO scantest VALUES ('bob', 25)"))

	type Person struct {
		Name string `db:"name"`
		Age  int64  `db:"age"`
	}
	var people []Person
	err = conn.Query("SELECT name, age FROM scantest ORDER BY age", func(row *Row) error {
		var p Person
		if err := row.Scan(&p); err != nil {
			return err
		}
		people = append(people, p)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(people) != 2 {
		t.Fatalf("got %d people, want 2", len(people))
	}
	if people[0].Name != "bob" || people[0].Age != 25 {
		t.Errorf("person 0 = %+v, want {bob 25}", people[0])
	}
	if people[1].Name != "alice" || people[1].Age != 30 {
		t.Errorf("person 1 = %+v, want {alice 30}", people[1])
	}
}

func TestQueryWithArgs(t *testing.T) {
	conn, err := NewConn("file::memory:?cache=shared", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE IF NOT EXISTS atest (id INTEGER, val TEXT)"))
	mustRes(conn.Exec("INSERT INTO atest VALUES (1, 'one')"))
	mustRes(conn.Exec("INSERT INTO atest VALUES (2, 'two')"))

	err = conn.Query("SELECT val FROM atest WHERE id = ?", func(row *Row) error {
		if row.Text("val") != "two" {
			t.Errorf("Text(val) = %q, want two", row.Text("val"))
		}
		return nil
	}, 2)
	if err != nil {
		t.Fatal(err)
	}
}
