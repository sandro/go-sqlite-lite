//go:build sqlxonly

// sqlx benchmarks — uses modernc.org/sqlite as the underlying driver (pure Go,
// no C symbol clash with go-sqlite-lite's amalgamation).
//
// sqlx wraps database/sql and adds struct scanning via reflection.
//
// Run: go test ./slite/compbench/ -tags='sqlxonly' -bench=. -benchmem

package compbench

import (
	"testing"

	"github.com/jmoiron/sqlx"
	_ "modernc.org/sqlite"
)

func openSqlx() *sqlx.DB {
	db, err := sqlx.Open("sqlite", ":memory:")
	if err != nil {
		panic(err)
	}
	db.Exec(createSQL)
	return db
}

func seedSqlx(db *sqlx.DB) {
	db.Exec("DELETE FROM cmp")
	for i := 0; i < nRows; i++ {
		db.Exec(insertSQL, "name", "desc")
	}
}

func BenchmarkSqlxSelect(b *testing.B) {
	db := openSqlx()
	defer db.Close()
	seedSqlx(db)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var result []compareRow
		if err := db.Select(&result, selectSQL); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSqlxGet(b *testing.B) {
	db := openSqlx()
	defer db.Close()
	seedSqlx(db)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var r compareRow
		if err := db.Get(&r, query1SQL, "name"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSqlxExec(b *testing.B) {
	db := openSqlx()
	defer db.Close()
	seedSqlx(db)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Exec("DELETE FROM cmp WHERE id = ?", 1)
		if err != nil {
			b.Fatal(err)
		}
	}
}