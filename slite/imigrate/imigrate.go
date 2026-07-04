// Package imigrate adapts slite for use with the imigrate migration library.
//
// Import this package to get a DB type that satisfies imigrate's Executor
// interface (Exec + GetVersions).
//
//	import "github.com/sandro/go-sqlite-lite/slite/imigrate"
//
//	db := imigrate.NewDB(pool)
package imigrate

import (
	"database/sql"

	"github.com/sandro/go-sqlite-lite/slite"
)

// DB wraps a *slite.DBPool to satisfy imigrate's Executor interface.
//
//	db := imigrate.NewDB(pool)
//	imigrate.Run(db, migrationDir)
type DB struct {
	pool *slite.DBPool
}

// NewDB returns a DB that delegates to pool.
func NewDB(pool *slite.DBPool) *DB {
	return &DB{pool: pool}
}

// Exec delegates to DBPool.Exec.
func (d *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return d.pool.Exec(query, args...)
}

// GetVersions executes query (which must return rows with a single "version"
// column) and returns the values as a slice of int64. The version values are
// Unix timestamps (seconds since epoch) used by imigrate to track which
// migrations have been applied.
//
//	versions, err := db.GetVersions("SELECT version FROM schema_migrations ORDER BY version")
func (d *DB) GetVersions(query string, args ...interface{}) ([]int64, error) {
	type row struct {
		Version int64
	}
	var rows []row
	if err := d.pool.Select(&rows, query, args...); err != nil {
		return nil, err
	}
	versions := make([]int64, len(rows))
	for i, r := range rows {
		versions[i] = r.Version
	}
	return versions, nil
}
