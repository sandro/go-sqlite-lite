package sqx

import (
	"log"
	"testing"
)

// import "github.com/sandro/go-sqlite-lite/sqx"

var pool *DBPool

func init() {
	var err error
	pool, err = NewDBPool("file::memory:?cache=shared", 1)
	if err != nil {
		log.Panic(err)
	}
	// pool = NewDBPool("test.db", 1)
	must(pool.Exec("create table foo (name text primary key, description text)"))
	must(pool.Exec("create table bar (name text primary key, description text, foo_id text)"))
	must(pool.Exec("create table baz (name text primary key, description text, foo_id text)"))
}

func must(err error) {
	if err != nil {
		log.Panic(err)
	}
}

func cleanTables() {
	pool.Exec("delete from foo")
	pool.Exec("delete from bar")
	pool.Exec("delete from baz")
}

func testFail(t *testing.T, err error) {
	if err != nil {
		t.Errorf("failure %s", err)
		t.FailNow()
	}
}

func TestStructScan(t *testing.T) {
	type Baz struct {
		Name        string
		Description string
		FooID       string `db:"foo_id"`
	}
	type Bar struct {
		Name        string
		Description string
		FooID       string `db:"foo_id"`
	}
	type Foo struct {
		Name        string
		Description string
		Bar         Bar
		Baz         Baz
	}
	err := pool.Exec("insert into foo values('a1', 'aOne')")
	testFail(t, err)
	err = pool.Exec("insert into bar values('b1', 'bOne', 'a1')")
	testFail(t, err)
	err = pool.Exec("insert into baz values('c1', 'cOne', 'a1')")
	testFail(t, err)

	data := Foo{}
	pool.Get(&data, "select * from foo join bar on bar.foo_id=? join baz on baz.foo_id=?", "a1", "a1")
	log.Println("From DB", data)
	var tests = []struct {
		a    string
		want string
	}{
		{data.Name, "a1"},
		{data.Description, "aOne"},
		{data.Bar.Name, "b1"},
		{data.Bar.Description, "bOne"},
		{data.Bar.FooID, "a1"},
		{data.Baz.Name, "c1"},
		{data.Baz.Description, "cOne"},
		{data.Baz.FooID, "a1"},
	}
	for i, test := range tests {
		if test.a != test.want {
			t.Errorf("%d) got %s, want %s", i, test.a, test.want)
		}
	}
}

// func TestMapScan(t *testing.T) {
// 	data := make(map[string]interface{})
// }

// test time.Time and time.Duration
