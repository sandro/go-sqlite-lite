package slite

import "testing"

func BenchmarkIn(b *testing.B) {
	ids := make([]int, 100)
	for i := range ids {
		ids[i] = i
	}
	query := "SELECT * FROM t WHERE id IN (?) AND name = ?"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = In(query, ids, "foo")
	}
}

func BenchmarkNamed(b *testing.B) {
	type Person struct {
		Name string `db:"name"`
		Age  int64  `db:"age"`
	}
	p := &Person{Name: "alice", Age: 30}
	query := "INSERT INTO users (name, age) VALUES (:name, :age)"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = Named(query, p)
	}
}

func BenchmarkNamedExec(b *testing.B) {
	conn, err := NewConn("file::memory:?cache=shared", false)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE IF NOT EXISTS bench_named (name TEXT, age INTEGER)"))

	type Person struct {
		Name string `db:"name"`
		Age  int64  `db:"age"`
	}
	p := &Person{Name: "alice", Age: 30}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.NamedExec("INSERT INTO bench_named (name, age) VALUES (:name, :age)", p); err != nil {
			b.Fatal(err)
		}
	}
}
