// Copyright 2018 The go-sqlite-lite Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite3

import (
	"testing"
)

// benchConn returns an in-memory connection with a simple table populated for
// benchmarks that read rows.
func benchConn(b *testing.B, nrows int) *Conn {
	b.Helper()
	c, err := Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	if err := c.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, val REAL, blob BLOB)"); err != nil {
		b.Fatal(err)
	}
	if nrows > 0 {
		s, err := c.Prepare("INSERT INTO t (name, val, blob) VALUES (?, ?, ?)")
		if err != nil {
			b.Fatal(err)
		}
		defer s.Close()
		blob := make([]byte, 64)
		for i := 0; i < nrows; i++ {
			if err := s.Bind("hello", float64(i), blob); err != nil {
				b.Fatal(err)
			}
			if err := s.Exec(); err != nil {
				b.Fatal(err)
			}
		}
	}
	return c
}

// BenchmarkOpen measures Open + Close on an in-memory database.
func BenchmarkOpen(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c, err := Open(":memory:")
		if err != nil {
			b.Fatal(err)
		}
		if err := c.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPrepare measures preparing a statement (no execution).
func BenchmarkPrepare(b *testing.B) {
	c := benchConn(b, 0)
	defer c.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, err := c.Prepare("SELECT id, name, val, blob FROM t WHERE id = ?")
		if err != nil {
			b.Fatal(err)
		}
		s.Close()
	}
}

// BenchmarkPrepareCached measures repeated Prepare of the same SQL — exercises
// the slite-level cache concept by reusing a single statement.
func BenchmarkPrepareCached(b *testing.B) {
	c := benchConn(b, 0)
	defer c.Close()
	s, err := c.Prepare("SELECT id, name, val, blob FROM t WHERE id = ?")
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Re-bind and step to completion, then reset (simulates reuse).
		if err := s.Bind(1); err != nil {
			b.Fatal(err)
		}
		_, _ = s.Step()
		s.Reset()
	}
}

// BenchmarkConnExec measures the sqlite3_exec fast path (no args, no prepare).
func BenchmarkConnExec(b *testing.B) {
	c := benchConn(b, 0)
	defer c.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Exec("CREATE TABLE IF NOT EXISTS bench (x INTEGER)"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStmtExec measures Prepare + Bind + Step-to-completion + Reset
// (the round-trip for a parameterised write).
func BenchmarkStmtExec(b *testing.B) {
	c := benchConn(b, 0)
	defer c.Close()
	if err := c.Exec("CREATE TABLE ins (id INTEGER PRIMARY KEY, name TEXT, val REAL)"); err != nil {
		b.Fatal(err)
	}
	s, err := c.Prepare("INSERT INTO ins (name, val) VALUES (?, ?)")
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Bind("hello", 3.14); err != nil {
			b.Fatal(err)
		}
		if err := s.Exec(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStepScan measures iterating rows and scanning into typed vars.
func BenchmarkStepScan(b *testing.B) {
	c := benchConn(b, 1000)
	defer c.Close()
	s, err := c.Prepare("SELECT id, name, val, blob FROM t")
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Reset(); err != nil {
			b.Fatal(err)
		}
		var id int64
		var name string
		var val float64
		var blob []byte
		for {
			hasRow, err := s.Step()
			if err != nil {
				b.Fatal(err)
			}
			if !hasRow {
				break
			}
			if err := s.Scan(&id, &name, &val, &blob); err != nil {
				b.Fatal(err)
			}
		}
		_ = id
		_ = name
		_ = val
		_ = blob
	}
}

// BenchmarkColumnText measures copying text out via C.GoStringN.
func BenchmarkColumnText(b *testing.B) {
	c := benchConn(b, 1000)
	defer c.Close()
	s, err := c.Prepare("SELECT name FROM t")
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Reset(); err != nil {
			b.Fatal(err)
		}
		for {
			hasRow, err := s.Step()
			if err != nil {
				b.Fatal(err)
			}
			if !hasRow {
				break
			}
			if _, _, err := s.ColumnText(0); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkColumnRawString measures zero-copy text retrieval referencing
// SQLite-owned memory.
func BenchmarkColumnRawString(b *testing.B) {
	c := benchConn(b, 1000)
	defer c.Close()
	s, err := c.Prepare("SELECT name FROM t")
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Reset(); err != nil {
			b.Fatal(err)
		}
		for {
			hasRow, err := s.Step()
			if err != nil {
				b.Fatal(err)
			}
			if !hasRow {
				break
			}
			if _, _, err := s.ColumnRawString(0); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkColumnBlob measures copying a BLOB out.
func BenchmarkColumnBlob(b *testing.B) {
	c := benchConn(b, 1000)
	defer c.Close()
	s, err := c.Prepare("SELECT blob FROM t")
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Reset(); err != nil {
			b.Fatal(err)
		}
		for {
			hasRow, err := s.Step()
			if err != nil {
				b.Fatal(err)
			}
			if !hasRow {
				break
			}
			if _, err := s.ColumnBlob(0); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkColumnRawBytes measures zero-copy BLOB retrieval.
func BenchmarkColumnRawBytes(b *testing.B) {
	c := benchConn(b, 1000)
	defer c.Close()
	s, err := c.Prepare("SELECT blob FROM t")
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Reset(); err != nil {
			b.Fatal(err)
		}
		for {
			hasRow, err := s.Step()
			if err != nil {
				b.Fatal(err)
			}
			if !hasRow {
				break
			}
			if _, err := s.ColumnRawBytes(0); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkBindTypes measures binding each supported type into a statement.
func BenchmarkBindTypes(b *testing.B) {
	c := benchConn(b, 0)
	defer c.Close()
	s, err := c.Prepare("INSERT INTO t (name, val, blob) VALUES (?, ?, ?)")
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	blob := make([]byte, 32)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Bind("hello", 3.14, blob); err != nil {
			b.Fatal(err)
		}
		s.ClearBindings()
	}
}
