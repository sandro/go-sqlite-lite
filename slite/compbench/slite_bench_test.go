//go:build sliteonly

// slite benchmarks — built with the go-sqlite-lite C amalgamation.
// Must be built separately from mattn benchmarks (both compile C SQLite,
// causing duplicate-symbol linker errors).
//
// Run: go test ./slite/compbench/ -tags='sliteonly' -bench=. -benchmem

package compbench

import (
	"database/sql"
	"testing"

	"github.com/sandro/go-sqlite-lite/slite"
)

// --- slite benchmarks ---

func newSlitePool(b *testing.B) *slite.DBPool {
	pool, err := slite.NewDBPool("file::memory:?cache=shared", 1)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := pool.Exec(createSQL); err != nil {
		b.Fatal(err)
	}
	if _, err := pool.Exec("DELETE FROM cmp"); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < nRows; i++ {
		if _, err := pool.Exec(insertSQL, "name", "desc"); err != nil {
			b.Fatal(err)
		}
	}
	return pool
}

func BenchmarkSliteSelect(b *testing.B) {
	pool := newSlitePool(b)
	defer pool.Close()
	dest := []compareRow{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dest = dest[:0]
		if err := pool.Select(&dest, selectSQL); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSliteQuery(b *testing.B) {
	pool := newSlitePool(b)
	defer pool.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var rows []compareRow
		err := pool.Query(selectSQL, nil, func(row *slite.Row) error {
			rows = append(rows, compareRow{
				ID:          row.Int("id"),
				Name:        row.Text("name"),
				Description: row.Text("description"),
			})
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		_ = rows
	}
}

func BenchmarkSliteGet(b *testing.B) {
	pool := newSlitePool(b)
	defer pool.Close()
	var row compareRow
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pool.Get(&row, query1SQL, "name"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSliteExec(b *testing.B) {
	pool := newSlitePool(b)
	defer pool.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := pool.Exec("DELETE FROM cmp WHERE id = ?", 1); err != nil {
			b.Fatal(err)
		}
	}
}

// avoid unused import
var _ = sql.ErrNoRows
