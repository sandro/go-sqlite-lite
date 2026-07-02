//go:build mattnonly

// Mattn benchmarks — must be built separately from slite benchmarks because
// both go-sqlite-lite and mattn/go-sqlite3 compile their own C SQLite
// amalgamation, causing duplicate symbol linker errors.
//
// Run: go test ./slite/compbench/ -tags='mattnonly' -bench=. -benchmem

package compbench

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func openMattn() *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		panic(err)
	}
	db.Exec(createSQL)
	return db
}

func seedMattn(db *sql.DB) {
	db.Exec("DELETE FROM cmp")
	for i := 0; i < nRows; i++ {
		db.Exec(insertSQL, "name", "desc")
	}
}

func BenchmarkMattnQuery(b *testing.B) {
	db := openMattn()
	defer db.Close()
	seedMattn(db)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query(selectSQL)
		if err != nil {
			b.Fatal(err)
		}
		var result []compareRow
		for rows.Next() {
			var r compareRow
			if err := rows.Scan(&r.ID, &r.Name, &r.Description); err != nil {
				b.Fatal(err)
			}
			result = append(result, r)
		}
		rows.Close()
		_ = result
	}
}

func BenchmarkMattnGet(b *testing.B) {
	db := openMattn()
	defer db.Close()
	seedMattn(db)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var r compareRow
		err := db.QueryRow(query1SQL, "name").Scan(&r.ID, &r.Name, &r.Description)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMattnExec(b *testing.B) {
	db := openMattn()
	defer db.Close()
	seedMattn(db)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Exec("DELETE FROM cmp WHERE id = ?", 1)
		if err != nil {
			b.Fatal(err)
		}
	}
}