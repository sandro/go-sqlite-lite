package slite

import (
	"reflect"
	"testing"
)

func TestIn(t *testing.T) {
	tests := []struct {
		query    string
		args     []interface{}
		wantQ    string
		wantArgs []interface{}
		wantErr  bool
	}{
		{
			query:    "SELECT * FROM t WHERE id IN (?) AND name = ?",
			args:     []interface{}{[]int{1, 2, 3}, "foo"},
			wantQ:    "SELECT * FROM t WHERE id IN (?, ?, ?) AND name = ?",
			wantArgs: []interface{}{1, 2, 3, "foo"},
		},
		{
			query:    "SELECT * FROM t WHERE id IN (?)",
			args:     []interface{}{[]int64{10}},
			wantQ:    "SELECT * FROM t WHERE id IN (?)",
			wantArgs: []interface{}{int64(10)},
		},
		{
			query:    "SELECT * FROM t WHERE name = ?",
			args:     []interface{}{"hello"},
			wantQ:    "SELECT * FROM t WHERE name = ?",
			wantArgs: []interface{}{"hello"},
		},
		{
			query:    "SELECT * FROM t WHERE a IN (?) AND b IN (?)",
			args:     []interface{}{[]string{"x", "y"}, []int{1, 2}},
			wantQ:    "SELECT * FROM t WHERE a IN (?, ?) AND b IN (?, ?)",
			wantArgs: []interface{}{"x", "y", 1, 2},
		},
		{
			query:   "SELECT * FROM t WHERE id IN (?)",
			args:    []interface{}{[]int{}},
			wantErr: true,
		},
		{
			// []byte should NOT be expanded — it's a driver.Value (BLOB).
			query:    "SELECT * FROM t WHERE data = ?",
			args:     []interface{}{[]byte("hello")},
			wantQ:    "SELECT * FROM t WHERE data = ?",
			wantArgs: []interface{}{[]byte("hello")},
		},
		{
			query:    "SELECT * FROM t",
			args:     nil,
			wantQ:    "SELECT * FROM t",
			wantArgs: []interface{}(nil),
		},
		{
			query:    "SELECT * FROM t WHERE id IN (?)",
			args:     []interface{}{[]interface{}{1, "two", 3.0}},
			wantQ:    "SELECT * FROM t WHERE id IN (?, ?, ?)",
			wantArgs: []interface{}{1, "two", 3.0},
		},
	}

	for i, tt := range tests {
		gotQ, gotArgs, err := In(tt.query, tt.args...)
		if tt.wantErr {
			if err == nil {
				t.Errorf("%d: expected error, got nil", i)
			}
			continue
		}
		if err != nil {
			t.Errorf("%d: unexpected error: %v", i, err)
			continue
		}
		if gotQ != tt.wantQ {
			t.Errorf("%d: query = %q, want %q", i, gotQ, tt.wantQ)
		}
		if !argsEqual(gotArgs, tt.wantArgs) {
			t.Errorf("%d: args = %v, want %v", i, gotArgs, tt.wantArgs)
		}
	}
}

func argsEqual(a, b []interface{}) bool {
	return reflect.DeepEqual(a, b)
}

func TestInDatabaseIntegration(t *testing.T) {
	conn, err := NewConn("file::memory:?cache=shared", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE IF NOT EXISTS in_test (id INTEGER, name TEXT)"))
	mustRes(conn.Exec("DELETE FROM in_test"))
	mustRes(conn.Exec("INSERT INTO in_test VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')"))

	ids := []int{1, 3}
	query, args, err := In("SELECT name FROM in_test WHERE id IN (?) ORDER BY id", ids)
	if err != nil {
		t.Fatal(err)
	}

	var names []string
	err = conn.Query(query, func(row *Row) error {
		names = append(names, row.Text("name"))
		return nil
	}, args...)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 || names[0] != "a" || names[1] != "c" {
		t.Fatalf("got %v, want [a c]", names)
	}
}

func TestCompileNamedQuery(t *testing.T) {
	tests := []struct {
		query   string
		want    string
		names   []string
		wantErr bool
	}{
		{"SELECT * FROM t WHERE id = :id", "SELECT * FROM t WHERE id = ?", []string{"id"}, false},
		{"INSERT INTO t (a, b) VALUES (:a, :b)", "INSERT INTO t (a, b) VALUES (?, ?)", []string{"a", "b"}, false},
		{"SELECT 'a::b'", "SELECT 'a:b'", nil, false},
		{"SELECT x := 1", "SELECT x := 1", nil, false},
		{"SELECT :name FROM t WHERE x = :x", "SELECT ? FROM t WHERE x = ?", []string{"name", "x"}, false},
		{"SELECT :user.name FROM t", "SELECT ? FROM t", []string{"user.name"}, false},
		{"SELECT * FROM t WHERE x = :", "", nil, true}, // empty name
	}

	for i, tt := range tests {
		got, names, err := compileNamedQuery(tt.query)
		if tt.wantErr {
			if err == nil {
				t.Errorf("%d: expected error for %q, got %q", i, tt.query, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("%d: unexpected error for %q: %v", i, tt.query, err)
			continue
		}
		if got != tt.want {
			t.Errorf("%d: query %q → %q, want %q", i, tt.query, got, tt.want)
		}
		if !stringSliceEqual(names, tt.names) {
			t.Errorf("%d: names %v, want %v", i, names, tt.names)
		}
	}
}

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestNamedWithStruct(t *testing.T) {
	conn, err := NewConn("file::memory:?cache=shared", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE IF NOT EXISTS named_test (name TEXT, age INTEGER)"))
	mustRes(conn.Exec("DELETE FROM named_test"))

	type Person struct {
		Name string `db:"name"`
		Age  int64  `db:"age"`
	}

	// NamedExec with struct
	p := Person{Name: "alice", Age: 30}
	if _, err := conn.NamedExec("INSERT INTO named_test (name, age) VALUES (:name, :age)", &p); err != nil {
		t.Fatal(err)
	}

	// NamedGet with struct
	var got Person
	if err := conn.NamedGet(&got, "SELECT name, age FROM named_test WHERE name = :name", &Person{Name: "alice"}); err != nil {
		t.Fatal(err)
	}
	if got.Name != "alice" || got.Age != 30 {
		t.Fatalf("got %+v, want {alice 30}", got)
	}

	// NamedSelect with struct
	p2 := Person{Name: "bob", Age: 25}
	if _, err := conn.NamedExec("INSERT INTO named_test (name, age) VALUES (:name, :age)", &p2); err != nil {
		t.Fatal(err)
	}
	var people []Person
	if err := conn.NamedSelect(&people, "SELECT name, age FROM named_test WHERE age >= :age ORDER BY age", &Person{Age: 25}); err != nil {
		t.Fatal(err)
	}
	if len(people) != 2 {
		t.Fatalf("got %d people, want 2", len(people))
	}
	if people[0].Name != "bob" || people[0].Age != 25 {
		t.Errorf("person 0 = %+v, want {bob 25}", people[0])
	}
}

func TestNamedWithMap(t *testing.T) {
	conn, err := NewConn("file::memory:?cache=shared", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE IF NOT EXISTS named_map_test (name TEXT, age INTEGER)"))
	mustRes(conn.Exec("DELETE FROM named_map_test"))

	// NamedExec with map
	m := map[string]interface{}{
		"name": "charlie",
		"age":  int64(40),
	}
	if _, err := conn.NamedExec("INSERT INTO named_map_test (name, age) VALUES (:name, :age)", m); err != nil {
		t.Fatal(err)
	}

	// NamedGet with map
	var got struct {
		Name string `db:"name"`
		Age  int64  `db:"age"`
	}
	if err := conn.NamedGet(&got, "SELECT name, age FROM named_map_test WHERE name = :name", map[string]interface{}{"name": "charlie"}); err != nil {
		t.Fatal(err)
	}
	if got.Name != "charlie" || got.Age != 40 {
		t.Fatalf("got %+v, want {charlie 40}", got)
	}
}

func TestNamedQuery(t *testing.T) {
	conn, err := NewConn("file::memory:?cache=shared", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE IF NOT EXISTS named_query_test (id INTEGER, name TEXT)"))
	mustRes(conn.Exec("DELETE FROM named_query_test"))
	mustRes(conn.Exec("INSERT INTO named_query_test VALUES (1, 'a'), (2, 'b'), (3, 'c')"))

	var names []string
	err = conn.NamedQuery("SELECT name FROM named_query_test WHERE id >= :minId ORDER BY id", map[string]interface{}{"minId": 2}, func(row *Row) error {
		names = append(names, row.Text("name"))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 || names[0] != "b" || names[1] != "c" {
		t.Fatalf("got %v, want [b c]", names)
	}
}

func TestNamedStmt(t *testing.T) {
	conn, err := NewConn("file::memory:?cache=shared", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE IF NOT EXISTS named_stmt_test (name TEXT, age INTEGER)"))
	mustRes(conn.Exec("DELETE FROM named_stmt_test"))

	type Person struct {
		Name string `db:"name"`
		Age  int64  `db:"age"`
	}

	stmt, err := conn.PrepareNamed("INSERT INTO named_stmt_test (name, age) VALUES (:name, :age)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	if _, err := stmt.Exec(&Person{Name: "dave", Age: 50}); err != nil {
		t.Fatal(err)
	}
	if _, err := stmt.Exec(&Person{Name: "eve", Age: 60}); err != nil {
		t.Fatal(err)
	}

	// Reuse the prepared statement for a Get.
	getStmt, err := conn.PrepareNamed("SELECT name, age FROM named_stmt_test WHERE age >= :age ORDER BY age")
	if err != nil {
		t.Fatal(err)
	}
	defer getStmt.Close()

	var got Person
	if err := getStmt.Get(&got, &Person{Age: 55}); err != nil {
		t.Fatal(err)
	}
	if got.Name != "eve" || got.Age != 60 {
		t.Fatalf("got %+v, want {eve 60}", got)
	}

	// Select
	var people []Person
	if err := getStmt.Select(&people, &Person{Age: 40}); err != nil {
		t.Fatal(err)
	}
	if len(people) != 2 {
		t.Fatalf("got %d, want 2", len(people))
	}
}

func TestNamedMissingParam(t *testing.T) {
	conn, err := NewConn("file::memory:?cache=shared", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE IF NOT EXISTS named_err_test (name TEXT, age INTEGER)"))
	mustRes(conn.Exec("DELETE FROM named_err_test"))

	type Person struct {
		Name string `db:"name"`
		Age  int64  `db:"age"`
	}

	// Missing param in struct (struct without :age field)
	type OnlyName struct {
		Name string `db:"name"`
	}
	_, err = conn.NamedExec("INSERT INTO named_err_test (name, age) VALUES (:name, :age)", &OnlyName{Name: "x"})
	if err == nil {
		t.Error("expected error for missing :age param, got nil")
	}

	// Missing param in map
	_, err = conn.NamedExec("INSERT INTO named_err_test (name, age) VALUES (:name, :age)", map[string]interface{}{"name": "x"})
	if err == nil {
		t.Error("expected error for missing :age param in map, got nil")
	}
}

func TestNamedDBPool(t *testing.T) {
	p, err := NewDBPool("file::memory:?cache=shared", 2)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	mustRes(p.Exec("CREATE TABLE IF NOT EXISTS named_pool_test (name TEXT, age INTEGER)"))
	mustRes(p.Exec("DELETE FROM named_pool_test"))

	type Person struct {
		Name string `db:"name"`
		Age  int64  `db:"age"`
	}

	// NamedExec
	if _, err := p.NamedExec("INSERT INTO named_pool_test (name, age) VALUES (:name, :age)", &Person{Name: "frank", Age: 35}); err != nil {
		t.Fatal(err)
	}

	// NamedGet
	var got Person
	if err := p.NamedGet(&got, "SELECT name, age FROM named_pool_test WHERE name = :name", &Person{Name: "frank"}); err != nil {
		t.Fatal(err)
	}
	if got.Name != "frank" || got.Age != 35 {
		t.Fatalf("got %+v, want {frank 35}", got)
	}

	// NamedSelect
	if _, err := p.NamedExec("INSERT INTO named_pool_test (name, age) VALUES (:name, :age)", &Person{Name: "grace", Age: 28}); err != nil {
		t.Fatal(err)
	}
	var people []Person
	if err := p.NamedSelect(&people, "SELECT name, age FROM named_pool_test WHERE age >= :age ORDER BY age", &Person{Age: 30}); err != nil {
		t.Fatal(err)
	}
	if len(people) != 1 || people[0].Name != "frank" {
		t.Fatalf("got %v, want [frank]", people)
	}

	// NamedQuery
	var names []string
	if err := p.NamedQuery("SELECT name FROM named_pool_test ORDER BY age", nil, func(row *Row) error {
		names = append(names, row.Text("name"))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 || names[0] != "grace" || names[1] != "frank" {
		t.Fatalf("got %v, want [grace frank]", names)
	}
}
