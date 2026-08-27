package slite

import (
	"context"
	"database/sql"
	"encoding"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/sandro/go-sqlite-lite/sqlite3"
)

// maxBinds is the maximum number of bind variables allowed in a single
// statement. It defaults to SQLite's modern default (32766, since SQLite
// 3.32); NewConn updates it from the connection's actual
// LIMIT_VARIABLE_NUMBER so the value is always correct for the running
// SQLite build.
var (
	maxBinds     = 32766
	maxBindsOnce sync.Once
)

// GetMaxBinds returns the maximum number of bind variables allowed in a
// single statement for the current SQLite build.
func GetMaxBinds() int { return maxBinds }

var defaultTimeFormats []string = []string{
	time.RFC3339,
	"2006-01-02 15:04:05",
}

// supportedFormats is the list of time layouts tried when parsing a TEXT
// column into a time.Time. Use SetSupportedTimeFormats to replace it
// thread-safely.
var supportedFormats []string = defaultTimeFormats

var timeFormatsMu sync.RWMutex

// SetSupportedTimeFormats replaces the supported time formats thread-safely.
// The caller's slice is copied so later mutation of it has no effect.
func SetSupportedTimeFormats(formats []string) {
	cp := make([]string, len(formats))
	copy(cp, formats)
	timeFormatsMu.Lock()
	supportedFormats = cp
	timeFormatsMu.Unlock()
}

// supportedTimeFormats returns a snapshot of the current formats under the
// read lock.
func supportedTimeFormats() []string {
	timeFormatsMu.RLock()
	f := supportedFormats
	timeFormatsMu.RUnlock()
	return f
}

// ErrNoRows is returned by Get when the query yields no row. It mirrors
// database/sql.ErrNoRows so callers can distinguish "no row" from other errors
// via errors.Is(err, slite.ErrNoRows).
var ErrNoRows = errors.New("slite: no row found")

// execResult captures the LastInsertId and RowsAffected at exec time so the
// returned sql.Result is a snapshot, not a live reference to the connection.
type execResult struct {
	lastInsertRowID int64
	rowsAffected    int64
}

func (r *execResult) LastInsertId() (int64, error) { return r.lastInsertRowID, nil }
func (r *execResult) RowsAffected() (int64, error) { return r.rowsAffected, nil }

// Logger is the callback invoked after every SQL execution. It receives the
// SQL text, the bind arguments, the elapsed time, and the error (nil on
// success). A nil logger suppresses logging. Set per-connection via
// Conn.SetLogger or globally via SetDefaultLogger.
type Logger func(sql string, args []interface{}, elapsed time.Duration, err error)

// defaultLogger is invoked when a Conn has no logger set. nil means no logging.
var (
	defaultLogger   Logger
	defaultLoggerMu sync.RWMutex
)

// SetDefaultLogger sets the package-level logger used by connections that have
// no per-connection logger. Pass nil to suppress logging globally.
func SetDefaultLogger(l Logger) {
	defaultLoggerMu.Lock()
	defaultLogger = l
	defaultLoggerMu.Unlock()
}

// getDefaultLogger returns the current default logger under the read lock.
func getDefaultLogger() Logger {
	defaultLoggerMu.RLock()
	l := defaultLogger
	defaultLoggerMu.RUnlock()
	return l
}

// Conn is a higher-level connection that wraps a *sqlite3.Conn with a
// statement cache, a scan-plan cache, and ergonomic query methods (Get,
// Select, Query, Exec, InsertValues, UpdateValues). The low-level
// *sqlite3.Conn is accessible via RawConn for advanced use cases (blobs,
// session changesets, custom step logic) that slite does not wrap.
type Conn struct {
	db        *sqlite3.Conn
	stmtCache map[string]*sqlite3.Stmt
	planCache map[string]*scanPlan // keyed by SQL string, like stmtCache
	logger    Logger
	loggerMu  sync.RWMutex
	closed    bool
}

// SetLogger sets the per-connection logger. Pass nil to suppress logging for
// this connection only. If nil, the package-level default logger (set via
// SetDefaultLogger) is used, if any.
func (o *Conn) SetLogger(l Logger) {
	o.loggerMu.Lock()
	o.logger = l
	o.loggerMu.Unlock()
}

// logSQL invokes the logger for this connection, if set. Falls back to the
// package-level default logger. If both are nil, it is a no-op.
func (o *Conn) logStart() time.Time {
	o.loggerMu.RLock()
	cl := o.logger
	o.loggerMu.RUnlock()
	if cl != nil || getDefaultLogger() != nil {
		return time.Now()
	}
	return time.Time{}
}

func (o *Conn) logSQL(sql string, args []interface{}, start time.Time, err error) {
	if start.IsZero() {
		return
	}
	elapsed := time.Since(start)
	o.loggerMu.RLock()
	cl := o.logger
	o.loggerMu.RUnlock()
	if cl != nil {
		cl(sql, args, elapsed, err)
	} else if dl := getDefaultLogger(); dl != nil {
		dl(sql, args, elapsed, err)
	} else {
		return // no logger, skip slow query check too
	}

	// Auto-EXPLAIN for slow queries.
	if threshold := time.Duration(slowQueryThreshold.Load()); threshold > 0 && elapsed >= threshold {
		if plan, planErr := o.ExplainPlan(sql, args...); planErr == nil && plan != "" {
			log.Printf("slite: slow query (%s): %s\n  EXPLAIN QUERY PLAN:\n  %s",
				elapsed, InterpolateSQL(sql, args...), strings.ReplaceAll(plan, "\n", "\n  "))
		}
	}
}

// RawConn returns the underlying low-level *sqlite3.Conn. This is the escape
// hatch for advanced use cases (BLOB I/O, session changesets, custom step
// loops) that slite does not wrap. Most callers should never need this.
func (o *Conn) RawConn() *sqlite3.Conn {
	return o.db
}

// NewConn opens a SQLite connection at uri. If readonly is true, the connection
// is opened with query_only enabled. An error is returned if the database
// cannot be opened or the initial PRAGMAs fail.
func NewConn(uri string, readonly bool) (*Conn, error) {
	c, err := sqlite3.Open(uri)
	if err != nil {
		return nil, fmt.Errorf("slite: failed to open %q: %w", uri, err)
	}
	c.BusyTimeout(time.Second * 5)
	if err := c.Exec("PRAGMA foreign_keys = ON;"); err != nil {
		c.Close()
		return nil, fmt.Errorf("slite: failed to enable foreign_keys: %w", err)
	}
	if readonly {
		if err := c.Exec("PRAGMA query_only = ON;"); err != nil {
			c.Close()
			return nil, fmt.Errorf("slite: failed to set query_only: %w", err)
		}
	}
	// Reflect the actual bind-variable limit for this SQLite build.
	maxBindsOnce.Do(func() {
		if lim := c.Limit(sqlite3.LIMIT_VARIABLE_NUMBER, -1); lim > 0 {
			maxBinds = lim
		}
	})
	conn := &Conn{db: c, stmtCache: make(map[string]*sqlite3.Stmt), planCache: make(map[string]*scanPlan)}
	return conn, nil
}

func (o *Conn) Exec(sql string, args ...interface{}) (sql.Result, error) {
	start := o.logStart()
	err := o.db.Exec(sql, args...)
	o.logSQL(sql, args, start, err)
	if err != nil {
		return nil, fmt.Errorf("slite: Exec: %s: %w", InterpolateSQL(sql, args...), err)
	}
	return &execResult{
		lastInsertRowID: o.db.LastInsertRowID(),
		rowsAffected:    int64(o.db.Changes()),
	}, nil
}

func (o *Conn) Prepare(sql string) (*sqlite3.Stmt, error) {
	if o.closed {
		return nil, fmt.Errorf("slite: Prepare on closed connection")
	}
	if stmt, ok := o.stmtCache[sql]; ok && stmt != nil {
		err := stmt.ClearBindings()
		return stmt, err
	}
	stmt, err := o.db.Prepare(sql)
	if err != nil {
		return stmt, err
	}
	o.stmtCache[sql] = stmt
	return stmt, err
}

func (o *Conn) Close() {
	if o.closed {
		return
	}
	o.closed = true
	for _, stmt := range o.stmtCache {
		if stmt != nil {
			stmt.Close()
		}
	}
	o.stmtCache = nil
	o.planCache = nil
	o.db.Close()
}

// Begin starts a transaction. Most callers should use Tx(func(c *Conn) error)
// instead, which handles commit/rollback automatically. For manual control,
// you can call Begin / Commit / Rollback directly.
func (o *Conn) Begin() error    { return o.db.Begin() }
func (o *Conn) Commit() error   { return o.db.Commit() }
func (o *Conn) Rollback() error { return o.db.Rollback() }

// cachedPlan returns the scan plan for (base, stmt), building and caching it
// on the Conn keyed by sqlStr. This avoids re-walking the struct and the
// global planCache string-building on every call.
func (o *Conn) cachedPlan(sqlStr string, base reflect.Type, stmt *sqlite3.Stmt) (*scanPlan, error) {
	if p, ok := o.planCache[sqlStr]; ok {
		return p, nil
	}
	colNames := stmt.ColumnNames()
	plan, err := getScanPlan(base, colNames)
	if err != nil {
		return nil, err
	}
	o.planCache[sqlStr] = plan
	return plan, nil
}

func (o *Conn) Get(dest interface{}, sql string, args ...interface{}) (retErr error) {
	start := o.logStart()
	defer func() { o.logSQL(sql, args, start, retErr) }()

	interpolated := InterpolateSQL(sql, args...)

	stmt, err := o.Prepare(sql)
	if err != nil {
		return fmt.Errorf("slite: Prepare: %s: %w", interpolated, err)
	}
	if err = stmt.Bind(args...); err != nil {
		return fmt.Errorf("slite: Bind: %s: %w", interpolated, err)
	}
	defer stmt.Reset()
	hasRow, err := stmt.Step()
	if err != nil {
		return fmt.Errorf("slite: Step: %s: %w", interpolated, err)
	}
	if !hasRow {
		return ErrNoRows
	}
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("slite: dest must be a pointer, got %T", dest)
	}
	elem := v.Elem()
	if elem.Kind() != reflect.Struct {
		return fmt.Errorf("slite: dest must point to a struct, got %s; for scalar values use Query with row.Int64(), row.Text(), etc.", elem.Kind())
	}
	plan, err := o.cachedPlan(sql, elem.Type(), stmt)
	if err != nil {
		return err
	}
	return applyPlan(plan, unsafe.Pointer(elem.UnsafeAddr()), stmt)
}

func (o *Conn) Select(dest interface{}, sql string, args ...interface{}) (retErr error) {
	start := o.logStart()
	defer func() { o.logSQL(sql, args, start, retErr) }()

	// Validate dest is a pointer to a slice.
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("slite: Select dest must be a pointer to a slice, got %T", dest)
	}
	sliceType := v.Type().Elem()
	if sliceType.Kind() != reflect.Slice {
		return fmt.Errorf("slite: Select dest must be a pointer to a slice, got pointer to %s", sliceType.Kind())
	}

	stmt, err := o.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Reset()
	if err = stmt.Bind(args...); err != nil {
		return err
	}

	indirect := v.Elem()
	base := sliceType.Elem()
	// Overwrite the destination slice rather than appending, matching the
	// usual expectation that Select fills dest with the current result set.
	indirect.Set(reflect.MakeSlice(sliceType, 0, 0))

	hasRow, err := stmt.Step()
	if err != nil {
		return err
	}
	if !hasRow {
		return nil
	}
	// Build (or fetch from Conn cache) the scan plan for this struct type +
	// column set. The plan is reused across all rows.
	plan, err := o.cachedPlan(sql, base, stmt)
	if err != nil {
		return err
	}
	for {
		vp := reflect.New(base)
		if err = applyPlan(plan, unsafe.Pointer(vp.Pointer()), stmt); err != nil {
			return err
		}
		indirect.Set(reflect.Append(indirect, vp.Elem()))
		hasRow, err = stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			break
		}
	}
	return nil
}

// Row provides typed column accessors for the current row of a query result.
// Columns are accessed by name (case-insensitive) or by index. A Row is
// reused across iterations in Query — do not retain references to it after
// the callback returns.
type Row struct {
	stmt   *sqlite3.Stmt
	colIdx map[string]int // column name → 0-based index (case-insensitive)
	cols   []string       // column names in order
}

// newRow builds a Row from a prepared statement. The column index map is
// built once and reused across rows.
func newRow(stmt *sqlite3.Stmt) *Row {
	cols := stmt.ColumnNames()
	idx := make(map[string]int, len(cols))
	for i, name := range cols {
		idx[strings.ToLower(name)] = i
	}
	return &Row{stmt: stmt, colIdx: idx, cols: cols}
}

// colIndex returns the 0-based column index for the given name, looking up
// case-insensitively. Returns -1 if the column doesn't exist.
func (r *Row) colIndex(name string) int {
	if i, ok := r.colIdx[strings.ToLower(name)]; ok {
		return i
	}
	return -1
}

// ColumnCount returns the number of columns in the result set.
func (r *Row) ColumnCount() int { return len(r.cols) }

// ColumnNames returns the names of all columns in order.
func (r *Row) ColumnNames() []string { return r.cols }

// Int returns the int64 value of the named column. The column is looked up
// case-insensitively. Returns 0 if the column doesn't exist.
func (r *Row) Int(col string) int64 {
	i := r.colIndex(col)
	if i < 0 {
		return 0
	}
	v, _, err := r.stmt.ColumnInt64(i)
	if err != nil {
		return 0
	}
	return v
}

// Text returns the string value of the named column.
func (r *Row) Text(col string) string {
	i := r.colIndex(col)
	if i < 0 {
		return ""
	}
	v, _, err := r.stmt.ColumnText(i)
	if err != nil {
		return ""
	}
	return v
}

// Float returns the float64 value of the named column.
func (r *Row) Float(col string) float64 {
	i := r.colIndex(col)
	if i < 0 {
		return 0
	}
	v, _, err := r.stmt.ColumnDouble(i)
	if err != nil {
		return 0
	}
	return v
}

// Blob returns the byte slice value of the named column.
func (r *Row) Blob(col string) []byte {
	i := r.colIndex(col)
	if i < 0 {
		return nil
	}
	v, err := r.stmt.ColumnBlob(i)
	if err != nil {
		return nil
	}
	return v
}

// Value returns the raw value of the named column as an interface{}.
// INTEGER → int64, FLOAT → float64, TEXT → string, BLOB → []byte, NULL → nil.
func (r *Row) Value(col string) interface{} {
	i := r.colIndex(col)
	if i < 0 {
		return nil
	}
	switch r.stmt.ColumnType(i) {
	case sqlite3.SQLITE_INTEGER:
		v, _, _ := r.stmt.ColumnInt64(i)
		return v
	case sqlite3.SQLITE_FLOAT:
		v, _, _ := r.stmt.ColumnDouble(i)
		return v
	case sqlite3.SQLITE_TEXT:
		v, _, _ := r.stmt.ColumnText(i)
		return v
	case sqlite3.SQLITE_BLOB:
		v, _ := r.stmt.ColumnBlob(i)
		return v
	default:
		return nil
	}
}

// IsNull returns true if the named column is NULL.
func (r *Row) IsNull(col string) bool {
	i := r.colIndex(col)
	if i < 0 {
		return true
	}
	return r.stmt.ColumnType(i) == sqlite3.SQLITE_NULL
}

// Scan scans the current row into dest, which must be a pointer to a struct.
// It reuses the cached scanPlan infrastructure — the same fast path as Get
// and Select.
func (r *Row) Scan(dest interface{}) error {
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("slite: Row.Scan dest must be a pointer, got %T", dest)
	}
	elem := v.Elem()
	if elem.Kind() != reflect.Struct {
		return fmt.Errorf("slite: Row.Scan dest must point to a struct, got %s", elem.Kind())
	}
	plan, err := getScanPlan(elem.Type(), r.cols)
	if err != nil {
		return err
	}
	return applyPlan(plan, unsafe.Pointer(elem.UnsafeAddr()), r.stmt)
}

// Query prepares and executes sql, calling f for each result row. The *Row
// passed to f is reused across iterations — do not retain references to it
// after f returns. If f returns a non-nil error, iteration stops and that
// error is returned. If the query yields no rows, f is never called and
// nil is returned (not ErrNoRows — Query is for streaming, not single-row
// lookups; use Get for that).
func (o *Conn) Query(sql string, f func(row *Row) error, args ...interface{}) (retErr error) {
	start := o.logStart()
	defer func() { o.logSQL(sql, args, start, retErr) }()

	interpolated := InterpolateSQL(sql, args...)

	stmt, err := o.Prepare(sql)
	if err != nil {
		return fmt.Errorf("slite: Prepare: %s: %w", interpolated, err)
	}
	defer stmt.Reset()
	if err = stmt.Bind(args...); err != nil {
		return fmt.Errorf("slite: Bind: %s: %w", interpolated, err)
	}

	hasRow, err := stmt.Step()
	if err != nil {
		return fmt.Errorf("slite: Step: %s: %w", interpolated, err)
	}
	if !hasRow {
		return nil
	}

	row := newRow(stmt)
	for {
		if err = f(row); err != nil {
			return err
		}
		hasRow, err = stmt.Step()
		if err != nil {
			return fmt.Errorf("slite: Step: %s: %w", interpolated, err)
		}
		if !hasRow {
			break
		}
	}
	return nil
}

// execCached is the internal write helper used by InsertValues and
// UpdateValues. It prepares (via the statement cache), handles multi-statement
// SQL (stmt.Tail), and returns a snapshot sql.Result. External callers should
// use Exec (raw SQL) or InsertValues/UpdateValues (structured writes).
func (o *Conn) execCached(sql string, args ...interface{}) (_ sql.Result, retErr error) {
	start := o.logStart()
	defer func() { o.logSQL(sql, args, start, retErr) }()

	interpolated := InterpolateSQL(sql, args...)

	stmt, err := o.Prepare(sql)
	if err != nil {
		return nil, fmt.Errorf("slite: Prepare: %s: %w", interpolated, err)
	}
	if stmt.Tail != "" {
		// Multi-statement SQL: execute the first statement, then hand the
		// tail to Exec (which uses the raw sqlite3 exec path, not the cache).
		// The tail runs with no bind args — multi-statement strings with
		// binds in the tail are not supported by the cache path; callers
		// needing that should use Conn.Exec directly.
		if err = stmt.Exec(args...); err != nil {
			return nil, fmt.Errorf("slite: Exec: %s: %w", interpolated, err)
		}
		return o.Exec(stmt.Tail)
	}
	if err = stmt.Exec(args...); err != nil {
		return nil, fmt.Errorf("slite: Exec: %s: %w", interpolated, err)
	}
	return &execResult{
		lastInsertRowID: o.db.LastInsertRowID(),
		rowsAffected:    int64(o.db.Changes()),
	}, nil
}

func (o *Conn) InsertValues(tableSQL string, attrs map[string]interface{}) (sql.Result, error) {
	if len(attrs) == 0 {
		return nil, fmt.Errorf("slite: InsertValues: no attributes to insert")
	}
	if len(attrs) > maxBinds {
		return nil, fmt.Errorf("slite: InsertValues: %d attributes exceeds bind limit %d", len(attrs), maxBinds)
	}
	// Validate the table prefix looks like an INSERT/REPLACE statement.
	trimmed := strings.TrimSpace(tableSQL)
	if !strings.HasPrefix(strings.ToUpper(trimmed), "INSERT") &&
		!strings.HasPrefix(strings.ToUpper(trimmed), "REPLACE") {
		return nil, fmt.Errorf("slite: InsertValues: tableSQL must start with INSERT or REPLACE, got %q", tableSQL)
	}
	// Sort column names for deterministic SQL (stable statement caching).
	colNames := make([]string, 0, len(attrs))
	for k := range attrs {
		colNames = append(colNames, k)
	}
	sort.Strings(colNames)
	binds := make([]string, len(colNames))
	values := make([]interface{}, 0, len(colNames))
	for i, name := range colNames {
		binds[i] = "?"
		values = append(values, attrs[name])
	}
	tableSQL += " (" + strings.Join(colNames, ",") + ")"
	tableSQL += " VALUES (" + strings.Join(binds, ",") + ")"
	return o.execCached(tableSQL, values...)
}

func (o *Conn) UpdateValues(tableSQL string, attrs map[string]interface{}, whereStr string, whereVals ...interface{}) (sql.Result, error) {
	if len(attrs) == 0 {
		return nil, fmt.Errorf("slite: UpdateValues: no attributes to update")
	}
	if len(attrs)+len(whereVals) > maxBinds {
		return nil, fmt.Errorf("slite: UpdateValues: %d bindvars exceeds limit %d", len(attrs)+len(whereVals), maxBinds)
	}
	// Validate the table prefix looks like an UPDATE statement.
	trimmed := strings.TrimSpace(tableSQL)
	if !strings.HasPrefix(strings.ToUpper(trimmed), "UPDATE") {
		return nil, fmt.Errorf("slite: UpdateValues: tableSQL must start with UPDATE, got %q", tableSQL)
	}
	// Sort column names for deterministic SQL (stable statement caching).
	colNames := make([]string, 0, len(attrs))
	for k := range attrs {
		colNames = append(colNames, k)
	}
	sort.Strings(colNames)
	values := make([]interface{}, 0, len(attrs)+len(whereVals))
	var b strings.Builder
	b.WriteString(tableSQL)
	b.WriteString(" SET ")
	for i, name := range colNames {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s=?", name)
		values = append(values, attrs[name])
	}
	b.WriteString(" ")
	b.WriteString(whereStr)
	values = append(values, whereVals...)
	return o.execCached(b.String(), values...)
}

var timeType reflect.Type = reflect.TypeOf(time.Time{})
var byteArrayType reflect.Type = reflect.TypeOf([]byte{})

var timeSetter = setTimeFromValue
var timeSetterMu sync.RWMutex

// SetTimeSetter replaces the function used to convert database values into
// time.Time. The default handles int64, float64, string, and []byte.
func SetTimeSetter(f func(val any) (time.Time, bool)) {
	timeSetterMu.Lock()
	timeSetter = f
	timeSetterMu.Unlock()
}

// getTimeSetter returns the current time setter under the read lock.
func getTimeSetter() func(val any) (time.Time, bool) {
	timeSetterMu.RLock()
	f := timeSetter
	timeSetterMu.RUnlock()
	return f
}

// typedMemmove copies a value of type t from src to dst. It is equivalent to
// *dst = *src for the concrete type t, but uses unsafe pointer copy so no
// reflect.Value is needed. The caller must guarantee dst and src point to
// memory of exactly type t.
func typedMemmove(dst, src unsafe.Pointer, t reflect.Type) {
	n := int(t.Size())
	if n <= 0 {
		return
	}
	// Use reflect.New + Set for correctness with GC pointers. This is only
	// used for the rare UnmarshalText path, not the hot scan loop, so the cost
	// is acceptable. We do a single memmove via unsafe.Slice for speed.
	copy((*[1 << 30]byte)(dst)[:n:n], (*[1 << 30]byte)(src)[:n:n])
}

// setTimeFromValue converts a database value into a time.Time. It returns
// (tm, ok); ok is false if val is nil or cannot be parsed.
func setTimeFromValue(val any) (tm time.Time, ok bool) {
	switch v := val.(type) {
	case int:
		return time.Unix(int64(v), 0), true
	case int64:
		return time.Unix(v, 0), true
	case float64:
		// Interpret a float as a Unix timestamp with sub-second precision.
		secs := int64(v)
		nsecs := int64((v - float64(secs)) * 1e9)
		if nsecs < 0 {
			nsecs = 0
		}
		return time.Unix(secs, nsecs), true
	case string:
		return parseTimeString(v)
	case []byte:
		return parseTimeString(string(v))
	}
	return time.Time{}, false
}

func parseTimeString(s string) (time.Time, bool) {
	for _, format := range supportedTimeFormats() {
		tm, err := time.Parse(format, s)
		if err == nil {
			return tm, true
		}
	}
	secs, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		return time.Unix(secs, 0), true
	}
	return time.Time{}, false
}

func getFieldName(fieldType reflect.StructField) string {
	name := fieldType.Name
	tag := fieldType.Tag.Get("db")
	if tag != "" {
		name = tag
	}
	return name
}

// implementsTextUnmarshaler reports whether t implements
// encoding.TextUnmarshaler. The method set is checked on the pointer type
// because UnmarshalText conventionally has a pointer receiver (as with
// guregu null.v4's zero.String); a value receiver would also be in the
// pointer's method set, so checking *t covers both.
func implementsTextUnmarshaler(t reflect.Type) bool {
	return reflect.PointerTo(t).Implements(textUnmarshalerType)
}

var textUnmarshalerType = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()

// scanPlan is a cached, pre-computed mapping from query result columns to
// struct fields. It is built once per (struct type, column set) pair via
// reflection, then reused across rows using unsafe pointer arithmetic — no
// per-row reflection. This mirrors the fast-path approach database/sql uses
// internally: reflect once to learn the layout, then write directly.
type scanPlan struct {
	// entries[i] describes how to fill the struct field matched to result
	// column i. A nil entry means column i is ignored (no matching field).
	entries []*scanEntry
}

// scanEntry describes a single field write. The setter writes the value of a
// stmt column directly into the struct at the field's offset.
type scanEntry struct {
	offset uintptr
	kind   reflect.Kind
	typ    reflect.Type // for time.Time and UnmarshalText types
	// setter performs the actual write. It receives an unsafe.Pointer to the
	// start of the struct (so it adds offset internally) and the column index.
	setter func(p unsafe.Pointer, stmt *sqlite3.Stmt, col int) error
}

var planCache sync.Map // map[planKey]*scanPlan

// planKey identifies a cached scan plan by struct type and the ordered set of
// column names returned by the query.
type planKey struct {
	typeID uintptr // reflect.Type pointer identity (via reflect.Type.Pointer())
	colKey string  // joined column names with a separator
}

// getScanPlan returns a cached plan for (typ, colNames), building one if needed.
func getScanPlan(typ reflect.Type, colNames []string) (*scanPlan, error) {
	var sb strings.Builder
	for i, cn := range colNames {
		if i > 0 {
			sb.WriteByte(0) // NUL separator — column names can't contain it
		}
		sb.WriteString(cn)
	}
	key := planKey{typeID: typeID(typ), colKey: sb.String()}
	if v, ok := planCache.Load(key); ok {
		return v.(*scanPlan), nil
	}
	plan, err := buildScanPlan(typ, colNames)
	if err != nil {
		return nil, err
	}
	// Store with LoadOrStore so concurrent builds of the same key resolve to one.
	if actual, loaded := planCache.LoadOrStore(key, plan); loaded {
		return actual.(*scanPlan), nil
	}
	return plan, nil
}

// typeID returns a stable identity for a reflect.Type. reflect.Type values are
// unique per type, so we can use the pointer as an identity key.
func typeID(t reflect.Type) uintptr {
	// reflect.Type has no public method returning a stable id, but Value.Pointer
	// on the reflect.Type interface works (it's the pointer to the rtype).
	v := reflect.ValueOf(t)
	if v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		return v.Pointer()
	}
	return 0
}

// buildScanPlan walks the struct tree via reflection, matching fields to columns
// by name (honoring `db` tags), case-sensitively then case-insensitively. Each
// column is consumed at most once; each field path is filled at most once, so
// duplicate column names (e.g. from JOINs) fill deeper nested structs in field
// order — preserving the existing semantics of the reflection-based scanner.
func buildScanPlan(typ reflect.Type, colNames []string) (*scanPlan, error) {
	nCols := len(colNames)
	consumed := make([]bool, nCols)
	matchedField := make(map[string]bool)
	entries := make([]*scanEntry, nCols)
	if err := walkFields(typ, colNames, consumed, matchedField, entries, "", 0); err != nil {
		return nil, err
	}
	// Warn when no query columns matched any struct fields — the caller
	// will silently get a zero-value struct, which is almost always a bug
	// (wrong db tags, wrong column aliases, wrong struct type).
	matched := 0
	for _, e := range entries {
		if e != nil {
			matched++
		}
	}
	if matched == 0 && nCols > 0 {
		log.Printf("slite: warning: query returned %d columns %v but no fields matched in %s — check your db tags", nCols, colNames, typ.Name())
	}
	return &scanPlan{entries: entries}, nil
}

// walkFields is the reflection-driven plan builder. It mirrors the old
// assignColumns/assignColumnsPath recursion exactly, but instead of reading
// column values it records field offsets and typed setters into entries.
// baseOffset is the byte offset of typ within the root struct (0 at the top
// level, accumulating as we descend into nested structs) so that every
// scanEntry.offset is relative to the root struct pointer.
func walkFields(typ reflect.Type, colNames []string, consumed []bool, matchedField map[string]bool, entries []*scanEntry, prefix string, baseOffset uintptr) error {
	for fi := 0; fi < typ.NumField(); fi++ {
		fieldType := typ.Field(fi)
		if !fieldType.IsExported() {
			continue
		}
		name := getFieldName(fieldType)
		path := prefix + fieldType.Name
		absOffset := baseOffset + fieldType.Offset
		// Scalar-like field (including time.Time and any type implementing
		// encoding.TextUnmarshaler, e.g. guregu null.v4 zero.String): try
		// direct column match. TextUnmarshaler structs must be treated as
		// scalars, not recursed into — their inner fields (like
		// sql.NullString's String/Valid) never match query columns.
		if fieldType.Type.Kind() != reflect.Struct || fieldType.Type == timeType || implementsTextUnmarshaler(fieldType.Type) {
			if !matchedField[path] {
				if idx := findColumn(colNames, consumed, name); idx >= 0 {
					entry, err := newScanEntry(fieldType, absOffset)
					if err != nil {
						return err
					}
					entries[idx] = entry
					consumed[idx] = true
					matchedField[path] = true
				}
			}
			continue
		}
		// Nested struct: recurse to match remaining unconsumed columns.
		if err := walkFields(fieldType.Type, colNames, consumed, matchedField, entries, path+".", absOffset); err != nil {
			return err
		}
	}
	return nil
}

// newScanEntry builds a scanEntry for a single field, choosing a setter based on
// the field's kind. The setter writes directly into the struct via unsafe pointer
// arithmetic — no reflect.Value, no FieldByName, no Set per row.
// offset is the absolute byte offset from the root struct pointer.
func newScanEntry(field reflect.StructField, offset uintptr) (*scanEntry, error) {
	kind := field.Type.Kind()
	entry := &scanEntry{offset: offset, kind: kind, typ: field.Type}

	switch kind {
	case reflect.Bool:
		entry.setter = func(p unsafe.Pointer, stmt *sqlite3.Stmt, col int) error {
			v, _, err := stmt.ColumnInt64(col)
			if err != nil {
				return err
			}
			*(*bool)(unsafe.Pointer(uintptr(p) + offset)) = v != 0
			return nil
		}
	case reflect.String:
		entry.setter = func(p unsafe.Pointer, stmt *sqlite3.Stmt, col int) error {
			v, _, err := stmt.ColumnText(col)
			if err != nil {
				return err
			}
			*(*string)(unsafe.Pointer(uintptr(p) + offset)) = v
			return nil
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		entry.setter = func(p unsafe.Pointer, stmt *sqlite3.Stmt, col int) error {
			v, _, err := stmt.ColumnInt64(col)
			if err != nil {
				return err
			}
			// Write via the correct int-width pointer so sign extension is right.
			switch kind {
			case reflect.Int:
				*(*int)(unsafe.Pointer(uintptr(p) + offset)) = int(v)
			case reflect.Int8:
				*(*int8)(unsafe.Pointer(uintptr(p) + offset)) = int8(v)
			case reflect.Int16:
				*(*int16)(unsafe.Pointer(uintptr(p) + offset)) = int16(v)
			case reflect.Int32:
				*(*int32)(unsafe.Pointer(uintptr(p) + offset)) = int32(v)
			case reflect.Int64:
				*(*int64)(unsafe.Pointer(uintptr(p) + offset)) = v
			}
			return nil
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		entry.setter = func(p unsafe.Pointer, stmt *sqlite3.Stmt, col int) error {
			v, _, err := stmt.ColumnInt64(col)
			if err != nil {
				return err
			}
			switch kind {
			case reflect.Uint:
				*(*uint)(unsafe.Pointer(uintptr(p) + offset)) = uint(v)
			case reflect.Uint8:
				*(*uint8)(unsafe.Pointer(uintptr(p) + offset)) = uint8(v)
			case reflect.Uint16:
				*(*uint16)(unsafe.Pointer(uintptr(p) + offset)) = uint16(v)
			case reflect.Uint32:
				*(*uint32)(unsafe.Pointer(uintptr(p) + offset)) = uint32(v)
			case reflect.Uint64:
				*(*uint64)(unsafe.Pointer(uintptr(p) + offset)) = uint64(v)
			}
			return nil
		}
	case reflect.Float32, reflect.Float64:
		entry.setter = func(p unsafe.Pointer, stmt *sqlite3.Stmt, col int) error {
			v, _, err := stmt.ColumnDouble(col)
			if err != nil {
				return err
			}
			if kind == reflect.Float32 {
				*(*float32)(unsafe.Pointer(uintptr(p) + offset)) = float32(v)
			} else {
				*(*float64)(unsafe.Pointer(uintptr(p) + offset)) = v
			}
			return nil
		}
	case reflect.Slice:
		if field.Type != byteArrayType {
			return nil, fmt.Errorf("slite: unsupported slice type %s (only []byte)", field.Type)
		}
		entry.setter = func(p unsafe.Pointer, stmt *sqlite3.Stmt, col int) error {
			v, err := stmt.ColumnBlob(col)
			if err != nil {
				return err
			}
			*(*[]byte)(unsafe.Pointer(uintptr(p) + offset)) = v
			return nil
		}
	case reflect.Struct:
		if field.Type == timeType {
			entry.setter = func(p unsafe.Pointer, stmt *sqlite3.Stmt, col int) error {
				var val interface{}
				switch typ := stmt.ColumnType(col); typ {
				case sqlite3.SQLITE_INTEGER:
					v, _, err := stmt.ColumnInt64(col)
					if err != nil {
						return err
					}
					val = v
				case sqlite3.SQLITE_FLOAT:
					v, _, err := stmt.ColumnDouble(col)
					if err != nil {
						return err
					}
					val = v
				case sqlite3.SQLITE_TEXT:
					v, _, err := stmt.ColumnText(col)
					if err != nil {
						return err
					}
					val = v
				case sqlite3.SQLITE_NULL:
					return nil
				case sqlite3.SQLITE_BLOB:
					v, err := stmt.ColumnBlob(col)
					if err != nil {
						return err
					}
					val = v
				default:
					return fmt.Errorf("slite: cannot set time for column type %d", typ)
				}
				tm, ok := getTimeSetter()(val)
				if ok && !tm.IsZero() {
					*(*time.Time)(unsafe.Pointer(uintptr(p) + offset)) = tm
				}
				return nil
			}
		} else {
			// Non-time struct: try UnmarshalText.
			if implementsTextUnmarshaler(field.Type) {
				entry.setter = func(p unsafe.Pointer, stmt *sqlite3.Stmt, col int) error {
					v, err := stmt.ColumnBlob(col)
					if err != nil {
						return err
					}
					// UnmarshalText writes into a fresh value; copy it into place.
					base := reflect.New(field.Type)
					res := base.MethodByName("UnmarshalText").Call([]reflect.Value{reflect.ValueOf(v)})
					if !res[0].IsNil() {
						if uerr, ok := res[0].Interface().(error); ok {
							return fmt.Errorf("slite: UnmarshalText failed: %w", uerr)
						}
						return fmt.Errorf("slite: UnmarshalText failed")
					}
					// Copy the unmarshaled value into the struct via unsafe.
					src := unsafe.Pointer(base.Pointer())
					dst := unsafe.Pointer(uintptr(p) + offset)
					typedMemmove(dst, src, field.Type)
					return nil
				}
			} else {
				return nil, fmt.Errorf("slite: struct field %s has no UnmarshalText and is not time.Time", field.Name)
			}
		}
	case reflect.Ptr:
		elemType := field.Type.Elem()
		elemKind := elemType.Kind()
		switch {
		case elemType == byteArrayType:
			// *[]byte: nil pointer = NULL, non-nil = blob data.
			entry.setter = func(p unsafe.Pointer, stmt *sqlite3.Stmt, col int) error {
				if stmt.ColumnType(col) == sqlite3.SQLITE_NULL {
					*(**[]byte)(unsafe.Pointer(uintptr(p) + offset)) = nil
					return nil
				}
				v, err := stmt.ColumnBlob(col)
				if err != nil {
					return err
				}
				*(**[]byte)(unsafe.Pointer(uintptr(p) + offset)) = &v
				return nil
			}
		case elemKind == reflect.String:
			entry.setter = func(p unsafe.Pointer, stmt *sqlite3.Stmt, col int) error {
				if stmt.ColumnType(col) == sqlite3.SQLITE_NULL {
					*(**string)(unsafe.Pointer(uintptr(p) + offset)) = nil
					return nil
				}
				v, _, err := stmt.ColumnText(col)
				if err != nil {
					return err
				}
				*(**string)(unsafe.Pointer(uintptr(p) + offset)) = &v
				return nil
			}
		case elemKind == reflect.Int64:
			entry.setter = func(p unsafe.Pointer, stmt *sqlite3.Stmt, col int) error {
				if stmt.ColumnType(col) == sqlite3.SQLITE_NULL {
					*(**int64)(unsafe.Pointer(uintptr(p) + offset)) = nil
					return nil
				}
				v, _, err := stmt.ColumnInt64(col)
				if err != nil {
					return err
				}
				*(**int64)(unsafe.Pointer(uintptr(p) + offset)) = &v
				return nil
			}
		case elemKind == reflect.Int:
			entry.setter = func(p unsafe.Pointer, stmt *sqlite3.Stmt, col int) error {
				if stmt.ColumnType(col) == sqlite3.SQLITE_NULL {
					*(**int)(unsafe.Pointer(uintptr(p) + offset)) = nil
					return nil
				}
				v, _, err := stmt.ColumnInt64(col)
				if err != nil {
					return err
				}
				i := int(v)
				*(**int)(unsafe.Pointer(uintptr(p) + offset)) = &i
				return nil
			}
		case elemKind == reflect.Float64:
			entry.setter = func(p unsafe.Pointer, stmt *sqlite3.Stmt, col int) error {
				if stmt.ColumnType(col) == sqlite3.SQLITE_NULL {
					*(**float64)(unsafe.Pointer(uintptr(p) + offset)) = nil
					return nil
				}
				v, _, err := stmt.ColumnDouble(col)
				if err != nil {
					return err
				}
				*(**float64)(unsafe.Pointer(uintptr(p) + offset)) = &v
				return nil
			}
		case elemKind == reflect.Bool:
			entry.setter = func(p unsafe.Pointer, stmt *sqlite3.Stmt, col int) error {
				if stmt.ColumnType(col) == sqlite3.SQLITE_NULL {
					*(**bool)(unsafe.Pointer(uintptr(p) + offset)) = nil
					return nil
				}
				v, _, err := stmt.ColumnInt64(col)
				if err != nil {
					return err
				}
				b := v != 0
				*(**bool)(unsafe.Pointer(uintptr(p) + offset)) = &b
				return nil
			}
		default:
			// Generic pointer: nil on NULL, reflect fallback otherwise.
			entry.setter = func(p unsafe.Pointer, stmt *sqlite3.Stmt, col int) error {
				if stmt.ColumnType(col) == sqlite3.SQLITE_NULL {
					*(*unsafe.Pointer)(unsafe.Pointer(uintptr(p) + offset)) = nil
					return nil
				}
				vp := reflect.NewAt(field.Type, unsafe.Pointer(uintptr(p)+offset))
				return stmt.Scan(vp.Interface())
			}
		}
	default:
		// Interface or other: fall back to stmt.Scan via reflection per row.
		entry.setter = func(p unsafe.Pointer, stmt *sqlite3.Stmt, col int) error {
			vp := reflect.NewAt(field.Type, unsafe.Pointer(uintptr(p)+offset))
			return stmt.Scan(vp.Interface())
		}
	}
	return entry, nil
}

// applyPlan writes the current row of stmt into dest (an unsafe.Pointer to the
// struct) using the precomputed plan. This is the hot path: a loop over the
// plan entries calling typed setters — zero reflection, zero allocations for
// the common scalar fields.
func applyPlan(plan *scanPlan, dest unsafe.Pointer, stmt *sqlite3.Stmt) error {
	for i, entry := range plan.entries {
		if entry == nil {
			continue
		}
		if err := entry.setter(dest, stmt, i); err != nil {
			return err
		}
	}
	return nil
}

// findColumn returns the index of the first unconsumed column whose name
// matches name (case-sensitively, then case-insensitively), or -1 if none.
func findColumn(colNames []string, consumed []bool, name string) int {
	for i, cn := range colNames {
		if !consumed[i] && cn == name {
			return i
		}
	}
	low := strings.ToLower(name)
	for i, cn := range colNames {
		if !consumed[i] && strings.ToLower(cn) == low {
			return i
		}
	}
	return -1
}

type DBPool struct {
	size  int
	conns []*Conn
	free  chan *Conn
	wconn *Conn
	wmu   sync.Mutex

	closeOnce sync.Once
	closed    bool
	mu        sync.Mutex
}

func (o *DBPool) isClosed() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.closed
}

func (o *DBPool) checkout() *Conn {
	if o.isClosed() {
		return nil
	}
	return <-o.free
}

// checkoutCtx is like checkout but returns nil if ctx is cancelled before a
// connection becomes available.
func (o *DBPool) checkoutCtx(ctx context.Context) *Conn {
	if o.isClosed() {
		return nil
	}
	select {
	case c := <-o.free:
		return c
	case <-ctx.Done():
		return nil
	}
}

func (o *DBPool) checkin(c *Conn) {
	if o.isClosed() {
		// Pool is closing/closed; the connection will be closed by
		// Close() via the conns slice. Don't double-close.
		return
	}
	o.free <- c
}

// WithReader checks out a read-only connection for the duration of f. The
// connection is returned to the pool when f completes. This is the recommended
// way to perform reads — it guarantees the connection is always returned.
func (o *DBPool) WithReader(f func(c *Conn) error) error {
	db := o.checkout()
	if db == nil {
		return ErrPoolClosed
	}
	defer o.checkin(db)
	return f(db)
}

// WithReaderCtx is like WithReader but respects ctx cancellation while
// waiting for a reader. If the context is cancelled before a reader
// becomes available, ctx.Err() is returned.
func (o *DBPool) WithReaderCtx(ctx context.Context, f func(c *Conn) error) error {
	db := o.checkoutCtx(ctx)
	if db == nil {
		if o.isClosed() {
			return ErrPoolClosed
		}
		return ctx.Err()
	}
	defer o.checkin(db)
	return f(db)
}

func (o *DBPool) checkoutWriter() *Conn {
	if o.isClosed() {
		return nil
	}
	o.wmu.Lock()
	return o.wconn
}

// checkoutWriterCtx is like checkoutWriter but returns nil if ctx is
// cancelled before the writer becomes available.
func (o *DBPool) checkoutWriterCtx(ctx context.Context) *Conn {
	if o.isClosed() {
		return nil
	}
	// Try to acquire the writer mutex in a context-aware way.
	// We spin up a goroutine to do the blocking Lock and signal via channel.
	ch := make(chan struct{})
	go func() {
		o.wmu.Lock()
		close(ch)
	}()
	select {
	case <-ch:
		return o.wconn
	case <-ctx.Done():
		// Context cancelled. The goroutine will eventually acquire the lock;
		// we must unlock it when it does so the mutex isn't left held.
		go func() {
			<-ch
			o.wmu.Unlock()
		}()
		return nil
	}
}

func (o *DBPool) checkinWriter() {
	o.wmu.Unlock()
}

func (o *DBPool) Close() {
	o.closeOnce.Do(func() {
		o.mu.Lock()
		o.closed = true
		o.mu.Unlock()

		// Drain the channel so any concurrent checkin calls don't block.
		// Then close each connection exactly once via the conns slice.
		for {
			select {
			case <-o.free:
			default:
				goto doneFree
			}
		}
	doneFree:
		for _, c := range o.conns {
			c.Close()
		}
		// Close the writer connection.
		o.wmu.Lock()
		o.wconn.Close()
		o.wmu.Unlock()
	})
}

// WithWriter checks out the single writer connection for the duration of f.
// Use this when running multiple writes that should share one writer checkout
// but do not need a SQL transaction. For atomic multi-write operations, prefer
// Tx, which also wraps f in BEGIN/COMMIT.
func (o *DBPool) WithWriter(f func(c *Conn) error) error {
	db := o.checkoutWriter()
	if db == nil {
		return ErrPoolClosed
	}
	defer o.checkinWriter()
	return f(db)
}

// WithWriterCtx is like WithWriter but respects ctx cancellation while
// waiting for the writer. If the context is cancelled before the writer
// becomes available, ctx.Err() is returned (no write is attempted).
func (o *DBPool) WithWriterCtx(ctx context.Context, f func(c *Conn) error) error {
	db := o.checkoutWriterCtx(ctx)
	if db == nil {
		if o.isClosed() {
			return ErrPoolClosed
		}
		return ctx.Err()
	}
	defer o.checkinWriter()
	return f(db)
}

// Exec runs a write statement on the single writer connection. The statement
// is NOT cached: pool.Exec is typically used for DDL, CTEs, INSERT…SELECT,
// and other one-off SQL where caching the prepared statement provides no
// benefit. Use InsertValues/UpdateValues/Tx for cacheable write patterns.
func (o *DBPool) Exec(query string, args ...interface{}) (sql.Result, error) {
	var res sql.Result
	err := o.WithWriter(func(db *Conn) error {
		var err error
		res, err = db.Exec(query, args...)
		return err
	})
	return res, err
}

func (o *DBPool) Select(dest interface{}, sql string, args ...interface{}) error {
	return o.WithReader(func(db *Conn) error {
		return db.Select(dest, sql, args...)
	})
}

func (o *DBPool) Get(dest interface{}, sql string, args ...interface{}) error {
	return o.WithReader(func(db *Conn) error {
		return db.Get(dest, sql, args...)
	})
}

func (o *DBPool) Query(sql string, f func(row *Row) error, args ...interface{}) error {
	return o.WithReader(func(db *Conn) error {
		return db.Query(sql, f, args...)
	})
}

func (o *DBPool) InsertValues(tableSQL string, attrs map[string]interface{}) (sql.Result, error) {
	db := o.checkoutWriter()
	if db == nil {
		return nil, ErrPoolClosed
	}
	defer o.checkinWriter()
	return db.InsertValues(tableSQL, attrs)
}

func (o *DBPool) UpdateValues(tableSQL string, attrs map[string]interface{}, whereStr string, whereVals ...interface{}) (sql.Result, error) {
	db := o.checkoutWriter()
	if db == nil {
		return nil, ErrPoolClosed
	}
	defer o.checkinWriter()
	return db.UpdateValues(tableSQL, attrs, whereStr, whereVals...)
}

func (o *DBPool) Tx(f func(c *Conn) error) (err error) {
	conn := o.checkoutWriter()
	if conn == nil {
		return ErrPoolClosed
	}
	defer o.checkinWriter()

	if err = conn.Begin(); err != nil {
		return fmt.Errorf("slite: failed to begin transaction: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			// Either f returned an error, or panicked. Roll back, but never
			// convert a rollback failure into a panic that masks the original
			// error/panic.
			if rbErr := conn.Rollback(); rbErr != nil {
				log.Printf("slite: rollback after error failed: %v", rbErr)
			}
		}
		if r := recover(); r != nil {
			panic(r) // re-panic so the caller still sees the original panic
		}
	}()

	if err = f(conn); err != nil {
		return err
	}
	if err = conn.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

// NewDBPool creates a pool of size read-only connections plus a single
// writer connection, all opened at uri. An error is returned if any
// connection fails to open.
func NewDBPool(uri string, size int) (*DBPool, error) {
	if size < 1 {
		return nil, fmt.Errorf("slite: NewDBPool requires size >= 1, got %d", size)
	}
	wconn, err := NewConn(uri, false)
	if err != nil {
		return nil, err
	}
	pool := &DBPool{
		size:  size,
		free:  make(chan *Conn, size),
		wconn: wconn,
	}
	for i := 0; i < size; i++ {
		conn, err := NewConn(uri, true)
		if err != nil {
			pool.Close() // clean up connections opened so far
			return nil, err
		}
		pool.conns = append(pool.conns, conn)
		pool.checkin(conn)
	}
	return pool, nil
}

type bulkInserterCommand struct {
	args []interface{}
}

type BulkInserter struct {
	prefix     string
	onConflict string
	size       int
	count      int
	mu         sync.Mutex
	cmds       []bulkInserterCommand
	conn       *Conn
}

// NewBulkInserter creates a BulkInserter that batches INSERT statements for
// the given *Conn. prefix should be in the form "INSERT INTO table (col1, col2)"
// or "INSERT OR REPLACE INTO table (col1, col2)". onConflict is appended after
// the VALUES list (e.g. "ON CONFLICT(col) DO UPDATE SET ...") or may be empty.
// The batch size defaults to GetMaxBinds().
func NewBulkInserter(prefix string, onConflict string, conn *Conn) *BulkInserter {
	return &BulkInserter{
		prefix:     prefix,
		onConflict: onConflict,
		size:       maxBinds,
		conn:       conn,
	}
}

var ErrArgsGreaterThanSize = errors.New("size cannot support so many args")

// ErrPoolClosed is returned by DBPool methods after Close has been called.
var ErrPoolClosed = errors.New("slite: connection pool is closed")

func (o *BulkInserter) Add(args ...interface{}) (err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	numArgs := len(args)
	if numArgs == 0 {
		return nil
	}
	// A single command that exceeds the bind limit can never be executed.
	if numArgs > o.size {
		return fmt.Errorf("%w: command has %d args but size is %d", ErrArgsGreaterThanSize, numArgs, o.size)
	}
	cmd := bulkInserterCommand{
		args: args,
	}
	// Flush when adding this command would *exceed* the limit, so a batch
	// can actually reach the full size rather than capping at Size-1.
	if o.count+numArgs > o.size {
		err = o.commit()
	}
	o.cmds = append(o.cmds, cmd)
	o.count += numArgs
	return err
}

func (o *BulkInserter) commit() (err error) {
	numCommands := len(o.cmds)
	if numCommands == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString(o.prefix)
	b.WriteString(" VALUES ")
	allArgs := make([]interface{}, 0, o.count)
	for i, cmd := range o.cmds {
		b.WriteString("(")
		numArgs := len(cmd.args)
		for ii, arg := range cmd.args {
			b.WriteString("?")
			if ii < numArgs-1 {
				b.WriteString(",")
			}
			allArgs = append(allArgs, arg)
		}
		b.WriteString(")")
		if i < numCommands-1 {
			b.WriteString(", ")
		}
	}
	if o.onConflict != "" {
		b.WriteString(" ")
		b.WriteString(o.onConflict)
	}
	// Bypass the statement cache: the SQL varies with the number of buffered
	// rows, so caching it would leak prepared statements unboundedly. Prepare
	// directly on the underlying sqlite3.Conn and close the stmt after use.
	stmt, err := o.conn.db.Prepare(b.String())
	if err != nil {
		return err
	}
	defer stmt.Close()
	if err = stmt.Exec(allArgs...); err != nil {
		return err
	}
	o.cmds = nil
	o.count = 0
	return nil
}

func (o *BulkInserter) Done() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.commit()
}

