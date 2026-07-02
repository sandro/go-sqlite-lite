//go:build modernconly

// Modernc benchmarks — pure-Go SQLite, no CGO. No symbol clash with slite, but
// kept in a separate build-tag file for consistency with the mattn benchmarks.
//
// Run: go test ./slite/compbench/ -tags='modernconly' -bench=. -benchmem

package compbench

import (
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

func openModernc() *sql.DB {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		panic(err)
	}
	db.Exec(createSQL)
	return db
}

func seedModernc(db *sql.DB) {
	db.Exec("DELETE FROM cmp")
	for i := 0; i < nRows; i++ {
		db.Exec(insertSQL, "name", "desc")
	}
}

func BenchmarkModerncQuery(b *testing.B) {
	db := openModernc()
	defer db.Close()
	seedModernc(db)

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

func BenchmarkModerncGet(b *testing.B) {
	db := openModernc()
	defer db.Close()
	seedModernc(db)

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

func BenchmarkModerncExec(b *testing.B) {
	db := openModernc()
	defer db.Close()
	seedModernc(db)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Exec("DELETE FROM cmp WHERE id = ?", 1)
		if err != nil {
			b.Fatal(err)
		}
	}
}