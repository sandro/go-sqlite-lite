// Shared types and constants for comparison benchmarks.
// No build tag — always compiled.

package compbench

type compareRow struct {
	ID          int64  `db:"id"`
	Name        string `db:"name"`
	Description string `db:"description"`
}

const (
	nRows     = 1000
	createSQL = "CREATE TABLE IF NOT EXISTS cmp (id INTEGER PRIMARY KEY, name TEXT, description TEXT)"
	insertSQL = "INSERT INTO cmp (name, description) VALUES (?, ?)"
	selectSQL = "SELECT id, name, description FROM cmp"
	query1SQL = "SELECT id, name, description FROM cmp WHERE name = ?"
)
