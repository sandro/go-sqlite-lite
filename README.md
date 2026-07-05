# go-sqlite-lite

go-sqlite-lite is a SQLite driver for Go made of two layers:

- **`sqlite3`** — a lightweight, unsurprising cgo wrapper around the SQLite C
  API. Methods are little more than thin shims over `sqlite3_*` functions, so
  what you read in the SQLite documentation maps directly to what you call in
  Go. No `database/sql` layer, no connection-pool surprises, no hidden
  behavior. When a SQLite error happens, the SQLite docs are still the right
  docs.
- **`slite`** — the headline layer. A connection pool with a single enforced
  writer, a prepared-statement cache, and a struct scanner that does reflection
  once and then writes rows straight to struct fields with unsafe pointer
  arithmetic. `slite` is what you reach for to build a Go service on SQLite:
  it removes the `SQLITE_BUSY` class of errors from the write path, keeps tail
  latency tight under contention, and makes reading into structs a one-liner
  with reflection-free hot-path scanning. The benchmarks below show ~4× faster
  single-row reads and ~9× fewer allocations than `mattn/go-sqlite3` + `sqlx`,
  and zero 500s under write contention where the multi-connection model
  intermittently fails.

## Design goals

* **Lightweight** — Where it makes sense, methods are thin wrappers around
  SQLite C functions. The `sqlite3` package never hides what SQLite is doing.
* **Performance** — A prepared-statement cache and a precomputed struct scan
  plan make the common read path fast. A single enforced writer makes writes
  safe and keeps the tail latency tight.
* **Understandable** — You always know what SQLite functions are called and in
  what order.
* **Unsurprising** — Connections, PRAGMAs, transactions, bindings, and
  stepping work exactly as you'd expect with native SQLite.
* **Debuggable** — When you hit a SQLite error, the SQLite documentation is
  relevant and relatable to your Go code.
* **Ergonomic** — The `slite` package provides convenience methods
  (`Get`, `Select`, `Tx`, `WithWriter`, `InsertValues`, `Named`, `In`) for
  the common cases without hiding SQLite underneath.

Most database drivers include a layer to work with Go's `database/sql`
interface, which introduces connection pooling and behavior differences from
pure SQLite. This driver **does not** include a `database/sql` interface.
For rationale, see the FAQ below.


## Why one writer?

SQLite serializes writes internally, regardless of which driver you use.
Slite takes a stronger position: **the library enforces a single writer
connection before SQL is ever called.** All writes go through `WithWriter`
(or `Tx`, `Exec`, etc.) which holds a mutex (`wmu`); reads go through a pool
of N connections.

This sounds like the same thing SQLite would do anyway. It is not — serializing
in Go rather than letting SQLite resolve contention internally has structural
advantages:

**1. `SQLITE_BUSY` becomes impossible on the write path.** With one writer
connection, the call to `sqlite3_step` is the only writer in the process. It
never contends on SQLite's write lock, so it can never get `SQLITE_BUSY` or
`SQLITE_LOCKED`. The entire retry/backoff/error-mapping code path that a
multi-writer driver needs simply doesn't exist to fail. This is stability, not
luck.

**2. Tail latency is better under contention.** When a goroutine is blocked
waiting for the writer, it parks on a Go `sync.Mutex`, which is
scheduler-cooperative: the Go runtime parks the goroutine and frees the OS
thread for other work (reads keep running on that thread). A driver that lets
multiple writer connections contend inside SQLite leaves OS threads blocked
in cgo doing busy-wait/retry inside `sqlite3_step` — the Go scheduler can
neither reuse those threads nor see the wait. The single-writer model keeps
threads free for reads and yields cooperative parking instead of opaque
spinning.

**3. Batching composes cleanly.** Acquire the writer once, `BEGIN`, run
several statements, `COMMIT`. With one writer there is no interleaving and no
two-connection transaction deadlock to reason about. This makes batching the
natural unit of work and halves lock acquisitions when a request does several
writes.

**The honest cost** is a tiny per-write gap (one writer unlocks, the next
locks) that a multi-connection driver could fill. The gap is paid *per write*,
not per transaction, so batching closes it: one `Tx` per request amortizes the
serialization gap across all the writes in that request. In a real benchmark
of a turriate.com visit-tracking endpoint:

| Mode | Driver | req/s | p99 | failures |
|---|---:|---:|---:|---:|
| 2 writes/request (unbatched) | mattn (multi-conn) | ~5,700 | ~48ms | yes |
| 2 writes/request (unbatched) | slite (1 writer) | ~4,400 | ~24ms | 0 |
| 1 tx/request (batched)       | slite (1 writer) | ~6,500 | **~9ms** | **0** |

mattn squeezes higher raw throughput in the unbatched case by hot-potatoing
SQLite's lock across connections, but pays with a 5× worse tail and intermittent
`SQLITE_BUSY` 500s. slite parks goroutines cooperatively and, batched, beats
mattn on *both* throughput and tail latency — with zero failures.


## The struct scanner

`slite.Get` and `slite.Select` scan rows directly into structs tagged with
`db:"column_name"`. There is no manual `Scan(&a, &b, &c)` column list and no
per-row reflection.

```go
type Page struct {
    ID      string    `db:"id"`
    Slug    string    `db:"slug"`
    Content string    `db:"content"`
    Updated time.Time `db:"updated_at"`
}

var page Page
db.Get(&page, "SELECT * FROM pages WHERE slug = ?", slug)
```

How it works: reflection is used **once** per `(struct type, column set)` pair
to build a `scanPlan` — a cached mapping from each result column to a struct
field's offset and a setter function. The plan is stored in a `sync.Map`
(`planCache`) keyed by struct type and the ordered column names. From then on,
scanning a row is flat `unsafe.Pointer` writes to precomputed offsets. There is
no reflect on the hot path.

This mirrors the fast-path approach `database/sql` uses internally: reflect
once to learn the layout, then write directly. The difference is slite builds
the plan per (type, columns) and reuses it across every subsequent query of the
same shape, whereas `database/sql`+`sqlx` reflect on every row.

On the library-level read benchmark (`Get` against a primary-key lookup,
seeded with 1000 rows, Apple M1 Max):

| Driver | ns/op | allocs/op |
|---|---:|---:|
| **slite `pool.Get`** (prepared cache + scan plan) | **835** | **3** |
| mattn `db.QueryRow().Scan` (database/sql prepare cache + sqlx) | 3,568 | 27 |

slite is ~4× faster and does ~9× fewer allocations on the single-row read path.
Both drivers cache prepared statements; the gap is slite's scan plan plus its
leaner cgo boundary.

When this matters: in query-heavy workloads (many small queries per request,
or complex joins). When it doesn't: in workloads dominated by response
serialization, e.g. one sub-microsecond query followed by a 55 KB HTML
response — there the DB wrapper is ~0.07% of request time and no driver
difference can show up in throughput. Benchmark at the layer you care about.


## Getting started

```go
import (
    "github.com/sandro/go-sqlite-lite/slite"
)
```

### Pool with one writer and N readers

```go
db, err := slite.NewDBPool("file:app.db?cache=shared&mode=rwc", 10)
if err != nil { log.Fatal(err) }
defer db.Close()

// Foreign keys are enabled by default. Set WAL mode for concurrent readers.
db.Exec("PRAGMA journal_mode=WAL")
```

`NewDBPool(uri, n)` creates N read connections plus one writer connection. The
writer is reached only through `Exec` / `Tx` / `WithWriter` /
`WithWriterCtx`. Reads use the read pool.

### Executing a write

`DBPool.Exec` acquires the writer, runs the statement, and releases the writer.
Statements executed through `pool.Exec` are not cached — it is intended for
DDL, CTEs, `INSERT…SELECT`, and other one-off SQL. For cacheable write
patterns, use `InsertValues`, `UpdateValues`, or `Tx`.

```go
_, err := db.Exec(`INSERT INTO visits (id, path) VALUES (?, ?)`, id, path)
```

### Reading into a struct

```go
var page Page
err := db.Get(&page, "SELECT * FROM pages WHERE slug = ?", slug)
```

```go
var pages []Page
err := db.Select(&pages, "SELECT * FROM pages ORDER BY updated_at DESC")
```

### Iterating rows

For streaming or when you don't want a slice, use `Query` with a callback. Row
columns are typed and named.

```go
err := db.Query("SELECT id, slug FROM pages", func(row *slite.Row) error {
    id   := row.Text("id")
    slug := row.Text("slug")
    fmt.Println(id, slug)
    return nil
})
```

### Handling NULLs

`Get` and `Select` scan into structs, where NULL columns become Go zero values
(`""`, `0`, zero `time.Time`). This is fine most of the time — and the simplest
approach is to avoid the ambiguity at the schema level:

```sql
CREATE TABLE users (
    name TEXT    NOT NULL DEFAULT '',
    age  INTEGER NOT NULL DEFAULT 0
);
```

Now empty string *is* the only empty state. No NULL/zero confusion. This is the
right answer for most columns.

When your business logic doesn't care about the difference (e.g. you show
"Unknown" for both NULL and empty), just use the zero value:

```go
var user User
db.Get(&user, "SELECT name, age FROM users WHERE id = ?", id)
if user.Name == "" {
    // NULL or empty — doesn't matter, show "Unknown"
}
```

When NULL genuinely means something different from empty (e.g. "not yet set" vs
"explicitly cleared"), use pointer fields:

```go
type User struct {
    ID   int64   `db:"id"`
    Name *string `db:"name"` // nil = NULL, &"" = empty string
    Age  *int64  `db:"age"`  // nil = NULL, &0 = zero
}

var user User
db.Get(&user, "SELECT id, name, age FROM users WHERE id = ?", id)
if user.Name == nil {
    // not yet set — prompt user to fill it in
} else if *user.Name == "" {
    // explicitly cleared
}
```

Supported pointer types: `*string`, `*int64`, `*int`, `*float64`, `*bool`,
`*[]byte`. NULL columns set the pointer to nil; non-NULL columns allocate and
assign. This is the same convention as `database/sql`.

For the `Query` callback path, `Row.IsNull` is also available:

```go
db.Query("SELECT name FROM users WHERE id = ?", func(row *slite.Row) error {
    if row.IsNull("name") {
        // NULL
    } else {
        name := row.Text("name") // "" if empty, never nil
    }
    return nil
}, id)
```

### Batching writes in a transaction

Each `Exec` acquires the writer for one statement. When a request does several
writes, acquire the writer once with `Tx` and run them in one transaction.
This halves lock acquisitions and makes the writes atomic.

```go
err := db.Tx(func(c *slite.Conn) error {
    if _, err := c.Exec(`INSERT INTO visitor_ids VALUES (0)`); err != nil {
        return err
    }
    _, err := c.Exec(`INSERT INTO visits (id, path, visitor_id) VALUES (?, ?, ?)`,
        id, path, visitorID)
    return err
})
```

`Tx` begins a transaction, calls your function, commits on nil error, and rolls
back on any error or panic. The writer mutex is held for the whole call.

### `WithWriter` for non-transactional multi-statement writes

```go
err := db.WithWriter(func(c *slite.Conn) error {
    _, err := c.Exec("UPDATE counters SET n = n + 1 WHERE id = ?", id)
    return err
})
```

Like `Tx` but without an explicit `BEGIN`/`COMMIT` — useful for a sequence of
autocommit writes that should share one writer checkout.

### Named parameters

```go
type Person struct {
    Name string `db:"name"`
    Age  int    `db:"age"`
}
p := &Person{Name: "alice", Age: 30}
_, err := db.NamedExec(`INSERT INTO users (name, age) VALUES (:name, :age)`, p)
```

And `IN (?)` expansion for slice binds:

```go
query, args, err := slite.In("SELECT * FROM t WHERE id IN (?) AND name = ?", ids, "foo")
```

### Nullable types

`sql.Null*`, `guregu/null`, and any `driver.Valuer` bind efficiently. Embedded
nullable types skip the reflection path and bind directly.

### Blobs and `*[]byte`

Use `[]byte` for blob columns. A nil `[]byte` is SQL NULL; an empty `[]byte{}`
is an empty blob — the distinction is preserved without pointer indirection.

```go
type Row struct {
    ID   int64  `db:"id"`
    Data []byte `db:"data"` // nil = NULL, []byte{} = empty, []byte{...} = data
}
```

`*[]byte` (pointer to byte slice) is also supported for both binding and
scanning, so code migrating from `database/sql` — where `*[]byte` is the
standard way to handle nullable blobs — works without changes. A nil pointer
binds/scans as NULL; a non-nil pointer dereferences to the inner slice.

```go
type LegacyRow struct {
    Data *[]byte `db:"data"` // nil pointer = NULL, &[]byte{...} = data
}
```

The recommended style is plain `[]byte`. Prefer it in new code.

## Advanced features

* Prepared-statement cache per connection.
* Named parameters (`:name`) and `IN (?)` slice expansion.
* `BulkInserter` for amortized batch inserts.
* `InsertValues` / `UpdateValues` for map-based writes.
* SQLite Blob incremental IO API.
* SQLite Online Backup API.
* SQLite Session extension.
* Custom busy handler.
* Callback hooks on commit, rollback, and update.
* Compile-time authorization callbacks.
* If shared-cache mode is enabled and a statement receives `SQLITE_LOCKED`,
  SQLite's [unlock-notify](https://sqlite.org/unlock_notify.html) extension
  blocks transparently and retries when the conflicting statement finishes.
* Compiled with SQLite support for JSON1, RTREE, FTS5, GEOPOLY, STAT4, and
  SOUNDEX.
* `OFFSET`/`LIMIT` on `UPDATE` and `DELETE`.
* `RawString` and `RawBytes` to reduce copying between Go and SQLite (use with
  caution).

## `sqlite3` package

If you want the thin layer with no pool and no struct scanner, use the
`sqlite3` package directly. It maps closely to the SQLite C API.

```go
import "github.com/sandro/go-sqlite-lite/sqlite3"

conn, err := sqlite3.Open("app.db")
if err != nil { /* ... */ }
defer conn.Close()

conn.BusyTimeout(5 * time.Second)

err = conn.Exec(`CREATE TABLE student(name TEXT, age INTEGER)`)
err = conn.Exec(`INSERT INTO student VALUES (?, ?)`, "Bob", 18)

stmt, err := conn.Prepare(`SELECT name, age FROM student WHERE age = ?`, 18)
defer stmt.Close()

for {
    hasRow, err := stmt.Step()
    if err != nil { /* ... */ }
    if !hasRow { break }
    var name string
    var age int
    stmt.Scan(&name, &age)
}
```

For full `sqlite3` semantics — bindings, transactions, NULL handling, blob
IO — see the [GoDoc](https://godoc.org/github.com/sandro/go-sqlite-lite/sqlite3).

## Credit

This project is a fork of [bvinc/go-sqlite-lite](https://github.com/bvinc/go-sqlite-lite)
(by Brian Vincent), which itself began as a fork of
[mxk/go-sqlite](https://github.com/mxk/go-sqlite/). The `sqlite3` thin cgo
wrapper is their work — a clean, debuggable binding to the SQLite C API that
makes the underlying calls legible. Many thanks to **bvinc** for the original
repository and the design ideas: the `sqlite3` layer's philosophy of "you
always know what SQLite functions are being called and in what order" is what
made wrapping it with `slite` worthwhile. Without that honest foundation, the
single-writer pool and reflection-free struct scanner built on top wouldn't
be as easy to reason about.

The `slite` package — the single-writer `DBPool`, the prepared-statement
cache, the `scanPlan` unsafe-pointer struct scanner, `Tx`/`WithWriter`,
`Named`/`In`, and `BulkInserter` — is new on top of that fork.

## FAQ

**Why is there no `database/sql` interface?**

If a `database/sql` interface is required, use
https://github.com/mattn/go-sqlite3. In practice, using `database/sql` with
SQLite is painful: connection pooling adds overhead and weirdness,
`Exec("BEGIN")` transactions don't work as expected, your connection does not
correspond to SQLite's concept of a connection, PRAGMA commands don't behave as
expected, and when locking or busy errors occur it's difficult to discover
why because you don't know which connection received which SQL in what order.
slite's single-enforced-writer model deliberately removes the locking/busy
class of errors from the write path.

**What are the differences between this driver and the `mxk/go-sqlite` driver?**

This driver was forked from `mxk/go-sqlite-driver`, which hadn't been
maintained in years and used an ancient version of SQLite. A large number of
features were removed, reworked, and renamed. The `codec` feature was removed,
making it much easier to upgrade SQLite. Method behavior now lines up closely
with the SQLite C API. On top of the `sqlite3` layer, the `slite` package adds
a connection pool with one enforced writer, a prepared-statement cache, and a
reflection-free struct scanner.

**What about `crawshaw/sqlite`?**

The crawshaw driver is well thought out and solves many of the same problems.
There, shared-cache mode and WAL are defaults, the WAL synchronous mode is
changed, prepared statements are transparently cached, and connection pools
are provided. slite keeps those choices explicit (WAL is an app decision,
shared cache is an open flag) and surfaces the single-writer invariant in the
API rather than hiding it.

**Are finalizers provided to automatically close connections and statements?**

No finalizers are used. You are responsible for closing connections and
statements. Finalizers may mask locking errors during debugging that then
surface unreliably in production. Removing finalizers makes behavior
consistent.

**Is it thread safe?**

go-sqlite-lite is as thread safe as SQLite. SQLite is compiled with
`-DSQLITE_THREADSAFE=2` (Multi-thread mode): SQLite can be safely used by
multiple threads provided that no single database connection is used
simultaneously in two or more threads. This applies to goroutines — a single
connection should not be used simultaneously between two goroutines.

It is safe to use separate connection instances concurrently, even against the
same database file:

```go
// ERROR (without extra synchronization)
c, _ := sqlite3.Open("sqlite.db")
go use(c)
go use(c)
```
```go
// OK
c1, _ := sqlite3.Open("sqlite.db")
c2, _ := sqlite3.Open("sqlite.db")
go use(c1)
go use(c2)
```

Consult the SQLite documentation for more:
https://www.sqlite.org/threadsafe.html

slite's `DBPool` manages this for you: read connections are checked out from a
pool (one per goroutine at a time) and the writer is behind a mutex, so the
"one connection = one goroutine" invariant is enforced by the library.

**How do I pool connections for HTTP requests?**

Use `slite.NewDBPool(uri, n)` — that's what it's for. It gives you N read
connections plus one writer, a prepared-statement cache per connection, and
the struct scanner. Rolling your own pool is outside the scope of this package.

## License

BSD licensed.
