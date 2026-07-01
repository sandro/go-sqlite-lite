// Copyright 2018 The go-sqlite-lite Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqx

import (
	"testing"
)

type benchRow struct {
	ID          int64  `db:"id"`
	Name        string `db:"name"`
	Description string `db:"description"`
}

func benchPool(b *testing.B) *DBPool {
	pool, err := NewDBPool("file::memory:?cache=shared", 1)
	if err != nil {
		b.Fatal(err)
	}
	must(pool.Exec("CREATE TABLE IF NOT EXISTS bench (id INTEGER PRIMARY KEY, name TEXT, description TEXT)"))
	must(pool.Exec("DELETE FROM bench"))
	return pool
}

// BenchmarkSqxSelect measures reflection-based struct scanning via sqx.Select.
func BenchmarkSqxSelect(b *testing.B) {
	pool := benchPool(b)
	defer pool.Close()

	const nrows = 1000
	for i := 0; i < nrows; i++ {
		must(pool.Exec("INSERT INTO bench (name, description) VALUES (?, ?)", "name", "desc"))
	}

	dest := []benchRow{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dest = dest[:0]
		if err := pool.Select(&dest, "SELECT id, name, description FROM bench"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSqxGet measures a single-row reflection scan via sqx.Get.
func BenchmarkSqxGet(b *testing.B) {
	pool := benchPool(b)
	defer pool.Close()

	must(pool.Exec("INSERT INTO bench (name, description) VALUES (?, ?)", "name", "desc"))

	var row benchRow
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pool.Get(&row, "SELECT id, name, description FROM bench WHERE name = ?", "name"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBulkInserter measures inserting rows in batches via BulkInserter,
// flushing periodically as Add() hits the bind limit.
func BenchmarkBulkInserter(b *testing.B) {
	pool := benchPool(b)
	defer pool.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		must(pool.Exec("DELETE FROM bench"))
		inserter := &BulkInserter{
			prefix: "INSERT INTO bench (name, description)",
			Size:   MAX_BINDS,
			conn:   pool.wconn,
		}
		for j := 0; j < 500; j++ {
			if err := inserter.Add("name", "desc"); err != nil {
				b.Fatal(err)
			}
		}
		if err := inserter.Done(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBulkInserterBatched measures BulkInserter with a large batch and a
// single commit, exercising the multi-row VALUES path.
func BenchmarkBulkInserterBatched(b *testing.B) {
	pool := benchPool(b)
	defer pool.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		must(pool.Exec("DELETE FROM bench"))
		inserter := &BulkInserter{
			prefix: "INSERT INTO bench (name, description)",
			Size:   MAX_BINDS,
			conn:   pool.wconn,
		}
		// Fill up to the bind limit (2 args per row → Size/2 rows per batch).
		for j := 0; j < MAX_BINDS/2; j++ {
			if err := inserter.Add("name", "desc"); err != nil {
				b.Fatal(err)
			}
		}
		if err := inserter.Done(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkInsertValues measures the sqx.InsertValues map-based helper.
func BenchmarkInsertValues(b *testing.B) {
	pool := benchPool(b)
	defer pool.Close()

	attrs := map[string]interface{}{
		"name":        "hello",
		"description": "world",
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pool.InsertValues("INSERT INTO bench", attrs); err != nil {
			b.Fatal(err)
		}
	}
}