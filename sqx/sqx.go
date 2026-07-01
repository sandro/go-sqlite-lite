package sqx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sandro/go-sqlite-lite/sqlite3"
)

// MaxBinds is the maximum number of bind variables allowed in a single
// statement. It defaults to SQLite's modern default (32766, since SQLite
// 3.32); NewConn updates it from the connection's actual
// LIMIT_VARIABLE_NUMBER so the value is always correct for the running
// SQLite build.
var MaxBinds = 32766

// MAX_BINDS aliases MaxBinds for backwards compatibility. It is a var, not a
// const, so it tracks the value set by NewConn.
var MAX_BINDS = MaxBinds

var defaultTimeFormats []string = []string{
	time.RFC3339,
	"2006-01-02 15:04:05",
}

// SupportedTimeFormats is the list of time layouts tried when parsing a TEXT
// column into a time.Time. It is safe to replace at any time; use
// SetSupportedTimeFormats to update it thread-safely.
var SupportedTimeFormats []string = defaultTimeFormats

var timeFormatsMu sync.RWMutex

// SetSupportedTimeFormats replaces the supported time formats thread-safely.
// The caller's slice is copied so later mutation of it has no effect.
func SetSupportedTimeFormats(formats []string) {
	cp := make([]string, len(formats))
	copy(cp, formats)
	timeFormatsMu.Lock()
	SupportedTimeFormats = cp
	timeFormatsMu.Unlock()
}

// supportedTimeFormats returns a snapshot of the current formats under the
// read lock.
func supportedTimeFormats() []string {
	timeFormatsMu.RLock()
	f := SupportedTimeFormats
	timeFormatsMu.RUnlock()
	return f
}

// check panics if the last argument is a non-nil error. It is intended for
// truly fatal, should-never-happen conditions in constructors where the caller
// has opted into panic-on-failure semantics. Data-path errors (column reads,
// binding failures) must NOT route through check; they should be returned.
func check(args ...interface{}) {
	err, ok := args[len(args)-1].(error)
	if ok && err != nil {
		log.Panic(err)
	}
}

//2006-01-02T15:04:05Z07:00
// func DBTimeShort(t time.Time) string {
// 	return t.Format("2006-01-02T15:04:05-07:00")
// }

type Conn struct {
	*sqlite3.Conn
	stmtCache map[string]*sqlite3.Stmt
	closed    bool
}

// NewConn opens a SQLite connection at uri. If readonly is true, the connection
// is opened with query_only enabled. An error is returned if the database
// cannot be opened or the initial PRAGMAs fail.
func NewConn(uri string, readonly bool) (*Conn, error) {
	c, err := sqlite3.Open(uri)
	if err != nil {
		return nil, fmt.Errorf("sqx: failed to open %q: %w", uri, err)
	}
	c.BusyTimeout(time.Second * 5)
	if err := c.Exec("PRAGMA foreign_keys = ON;"); err != nil {
		c.Close()
		return nil, fmt.Errorf("sqx: failed to enable foreign_keys: %w", err)
	}
	if readonly {
		if err := c.Exec("PRAGMA query_only = ON;"); err != nil {
			c.Close()
			return nil, fmt.Errorf("sqx: failed to set query_only: %w", err)
		}
	}
	// Reflect the actual bind-variable limit for this SQLite build.
	if lim := c.Limit(sqlite3.LIMIT_VARIABLE_NUMBER, -1); lim > 0 {
		MaxBinds = lim
		MAX_BINDS = lim
	}
	conn := &Conn{Conn: c, stmtCache: make(map[string]*sqlite3.Stmt)}
	return conn, nil
}

func (o *Conn) Exec(sql string, args ...interface{}) (sql.Result, error) {
	err := o.Conn.Exec(sql, args...)
	return o, err
}

func (o *Conn) GetVersions(query string, args ...interface{}) (versions []int64, err error) {
	vals := []struct{ Version int64 }{}
	err = o.Select(&vals, query, args...)
	if err != nil {
		return
	}
	for _, v := range vals {
		versions = append(versions, v.Version)
	}
	return
}

func (o *Conn) Prepare(sql string) (*sqlite3.Stmt, error) {
	if o.closed {
		return nil, fmt.Errorf("sqx: Prepare on closed connection")
	}
	if stmt, ok := o.stmtCache[sql]; ok && stmt != nil {
		err := stmt.ClearBindings()
		return stmt, err
	}
	stmt, err := o.Conn.Prepare(sql)
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
	o.Conn.Close()
}

func (o *Conn) Get(dest interface{}, sql string, args ...interface{}) error {
	stmt, err := o.Prepare(sql)
	if err != nil {
		log.Println("Prepare statement failed", sql, args)
		return err
	}
	if err = stmt.Bind(args...); err != nil {
		return err
	}
	defer stmt.Reset()
	hasRow, err := stmt.Step()
	if err != nil {
		return err
	}
	if !hasRow {
		return nil
	}
	value := reflect.ValueOf(dest).Elem()
	return dbToStruct(value, stmt)
}

func (o *Conn) Select(dest interface{}, sql string, args ...interface{}) error {
	stmt, err := o.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Reset()
	if err = stmt.Bind(args...); err != nil {
		return err
	}

	value := reflect.ValueOf(dest)
	indirect := reflect.Indirect(value)
	sliceElem := value.Type().Elem()
	base := sliceElem.Elem()
	// Overwrite the destination slice rather than appending, matching the
	// usual expectation that Select fills dest with the current result set.
	indirect.Set(reflect.MakeSlice(sliceElem, 0, 0))
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			break
		}
		vp := reflect.New(base)
		v := vp.Elem()
		if err = dbToStruct(v, stmt); err != nil {
			return err
		}
		indirect.Set(reflect.Append(indirect, v))
	}
	return nil
}

func (o *Conn) Exec2(sql string, args ...interface{}) (sql.Result, error) {
	stmt, err := o.Prepare(sql)
	if err != nil {
		log.Println("STMT ERR", err)
		return o, err
	}
	if stmt.Tail != "" {
		// The first statement (already prepared above) is executed via stmt.Exec,
		// then the leftover text is executed separately. Previously this
		// re-ran the *entire* sql string, double-executing the first statement
		// and never processing the tail.
		if err = stmt.Exec(args...); err != nil {
			log.Println("EXEC ERR", err, sql, args)
			return o, err
		}
		_, err = o.Exec(stmt.Tail, args...)
		return o, err
	}
	if err = stmt.Exec(args...); err != nil {
		log.Println("EXEC ERR", err, sql, args)
		return o, err
	}
	return o, nil
}

// stmt, err := conn.Prepare(`insert or replace into vendors (id, name, indexed_at, location, created_at, updated_at)
// VALUES (?, ?, ?, ?, ?, ?)`)

func (o *Conn) InsertValues(tableSQL string, attrs map[string]interface{}) (sql.Result, error) {
	if len(attrs) == 0 {
		return nil, fmt.Errorf("InsertValues: no attributes to insert")
	}
	if len(attrs) > MAX_BINDS {
		return nil, fmt.Errorf("cannot have more than %d bindvars", MAX_BINDS)
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
	return o.Exec2(tableSQL, values...)
}

// update users set x=1
func (o *Conn) UpdateValues(tableSQL string, attrs map[string]interface{}, whereStr string, whereVals ...interface{}) (sql.Result, error) {
	if len(attrs) == 0 {
		return nil, fmt.Errorf("UpdateValues: no attributes to update")
	}
	if len(attrs)+len(whereVals) > MAX_BINDS {
		return nil, fmt.Errorf("cannot have more than %d bindvars", MAX_BINDS)
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
	return o.Exec2(b.String(), values...)
}

func (o *Conn) LastInsertId() (int64, error) {
	return o.Conn.LastInsertRowID(), nil
}

func (o *Conn) RowsAffected() (int64, error) {
	return int64(o.Conn.Changes()), nil
}

var timeType reflect.Type = reflect.TypeOf(time.Time{})
var byteArrayType reflect.Type = reflect.TypeOf([]byte{})

func isTime(value reflect.Value) bool {
	return value.Type() == timeType || value.CanConvert(timeType)
}

var TimeSetter = setTimeFromValue

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
	log.Println("Could not set time from", s)
	return time.Time{}, false
}

func setTime(value reflect.Value, val interface{}) {
	tm, ok := TimeSetter(val)
	if ok && !tm.IsZero() {
		value.Set(reflect.ValueOf(tm))
	}
}

func getFieldName(fieldType reflect.StructField) string {
	name := fieldType.Name
	tag := fieldType.Tag.Get("db")
	if tag != "" {
		name = tag
	}
	return name
}

// fillField reads column index from stmt into field. It returns (filled, err).
// filled is true if the field was set. err is non-nil if reading or setting
// the column failed. A NULL column for a time.Time field is reported as
// (true, nil) so the field keeps its zero value.
func fillField(field reflect.Value, stmt *sqlite3.Stmt, colName string, index int) (bool, error) {
	switch field.Kind() {
	case reflect.Bool:
		b, _, err := stmt.ColumnInt64(index)
		if err != nil {
			return false, err
		}
		field.SetBool(b != 0)
		return true, nil
	case reflect.String:
		val, _, err := stmt.ColumnText(index)
		if err != nil {
			return false, err
		}
		field.SetString(val)
		return true, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		val, _, err := stmt.ColumnInt64(index)
		if err != nil {
			return false, err
		}
		field.SetInt(val)
		return true, nil
	case reflect.Float32, reflect.Float64:
		val, _, err := stmt.ColumnDouble(index)
		if err != nil {
			return false, err
		}
		field.SetFloat(val)
		return true, nil
	case reflect.Slice:
		if field.CanConvert(byteArrayType) {
			val, err := stmt.ColumnBlob(index)
			if err != nil {
				return false, err
			}
			field.SetBytes(val)
			return true, nil
		}
		return false, fmt.Errorf("sqx: unimplemented conversion for slice %s %v: only []byte is supported", colName, field.Type())
	case reflect.Struct:
		if isTime(field) {
			var val interface{}
			switch typ := stmt.ColumnType(index); typ {
			case sqlite3.INTEGER:
				v, _, err := stmt.ColumnInt64(index)
				if err != nil {
					return false, err
				}
				val = v
			case sqlite3.FLOAT:
				v, _, err := stmt.ColumnDouble(index)
				if err != nil {
					return false, err
				}
				val = v
			case sqlite3.TEXT:
				v, _, err := stmt.ColumnText(index)
				if err != nil {
					return false, err
				}
				val = v
			case sqlite3.NULL:
				return true, nil
			case sqlite3.BLOB:
				v, err := stmt.ColumnBlob(index)
				if err != nil {
					return false, err
				}
				val = v
			default:
				return false, fmt.Errorf("sqx: cannot set time for column %s with type %d (field %v)", colName, typ, field.Type())
			}
			setTime(field, val)
			return true, nil
		}
		// Non-time struct: try encoding.TextUnmarshaler.
		t := field.Type()
		base := reflect.New(t)
		m := base.MethodByName("UnmarshalText")
		if !m.IsZero() {
			val, err := stmt.ColumnBlob(index)
			if err != nil {
				return false, err
			}
			res := m.Call([]reflect.Value{reflect.ValueOf(val)})
			if !res[0].IsNil() {
				if uerr, ok := res[0].Interface().(error); ok {
					return false, fmt.Errorf("sqx: UnmarshalText for %s failed: %w", colName, uerr)
				}
				return false, fmt.Errorf("sqx: UnmarshalText for %s failed", colName)
			}
			field.Set(base.Elem())
			return true, nil
		}
		return false, fmt.Errorf("sqx: no UnmarshalText for struct field %s %v", colName, t)
	default:
		return false, fmt.Errorf("sqx: unknown reflection type for column %s field %v kind %s", colName, field.Type(), field.Kind())
	}
}

// dbToStruct scans the current row of stmt into value (a reflect.Value of a
// struct). Columns are matched to struct fields by name (honoring a `db` tag),
// case-insensitively, with recursive descent into nested struct fields.
//
// Matching is by column name against the set of available field names across
// the whole struct tree. Each struct field is filled at most once, so when a
// query (e.g. "SELECT * FROM foo JOIN bar") produces duplicate column names,
// the first column of a given name fills the outermost match and subsequent
// columns of the same name fill deeper nested structs in field order.
// Unmatched columns are ignored. An error is returned if a matched column
// cannot be read or converted.
func dbToStruct(value reflect.Value, stmt *sqlite3.Stmt) error {
	nCols := stmt.ColumnCount()
	// Pre-compute the column names once.
	colNames := make([]string, nCols)
	for i := 0; i < nCols; i++ {
		colNames[i] = stmt.ColumnName(i)
	}
	// Track which column ordinals have already been consumed.
	consumed := make([]bool, nCols)
	// matchedField records a field path that has already been filled, so it
	// won't be reused by a later column of the same name.
	matchedField := make(map[string]bool)
	return assignColumns(value, stmt, colNames, consumed, matchedField)
}

// assignColumns assigns unmatched columns to fields of value, recursing into
// nested struct fields. A field is matched once (tracked in matchedField by
// its dotted path); a column is consumed once (tracked in consumed).
func assignColumns(value reflect.Value, stmt *sqlite3.Stmt, colNames []string, consumed []bool, matchedField map[string]bool) error {
	t := value.Type()
	for fi := 0; fi < value.NumField(); fi++ {
		fieldType := t.Field(fi)
		if !fieldType.IsExported() {
			continue
		}
		f := value.Field(fi)
		name := getFieldName(fieldType)
		path := fieldType.Name
		// First, try to match this scalar field against an unconsumed column
		// of the same name. Only do this if the field is not itself a struct
		// that we'd rather descend into (except time.Time, which is scalar-like).
		if f.Kind() != reflect.Struct || isTime(f) {
			if !matchedField[path] {
				if idx := findColumn(colNames, consumed, name); idx >= 0 {
					if _, err := fillField(f, stmt, colNames[idx], idx); err != nil {
						return err
					}
					consumed[idx] = true
					matchedField[path] = true
				}
			}
			continue
		}
		// Nested struct: assign remaining unmatched columns to its fields.
		// Build a child path namespace by prefixing, to keep field identity
		// unique across nesting levels.
		if err := assignColumnsPath(f, stmt, colNames, consumed, matchedField, path+"."); err != nil {
			return err
		}
	}
	return nil
}

// assignColumnsPath is assignColumns with a path prefix used to namespace
// nested field identities in matchedField.
func assignColumnsPath(value reflect.Value, stmt *sqlite3.Stmt, colNames []string, consumed []bool, matchedField map[string]bool, prefix string) error {
	t := value.Type()
	for fi := 0; fi < value.NumField(); fi++ {
		fieldType := t.Field(fi)
		if !fieldType.IsExported() {
			continue
		}
		f := value.Field(fi)
		name := getFieldName(fieldType)
		path := prefix + fieldType.Name
		if f.Kind() != reflect.Struct || isTime(f) {
			if !matchedField[path] {
				if idx := findColumn(colNames, consumed, name); idx >= 0 {
					if _, err := fillField(f, stmt, colNames[idx], idx); err != nil {
						return err
					}
					consumed[idx] = true
					matchedField[path] = true
				}
			}
			continue
		}
		if err := assignColumnsPath(f, stmt, colNames, consumed, matchedField, path+"."); err != nil {
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

type SqlExecutor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Insert(list ...interface{}) error
	Delete(list ...interface{}) (int64, error)
}

type DBPool struct {
	size  int
	conns []*Conn
	free  chan *Conn
	wconn *Conn
	wfree chan *Conn

	closeOnce sync.Once
	closed    bool
	mu        sync.Mutex
}

func (o *DBPool) isClosed() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.closed
}

func (o *DBPool) Checkout() *Conn {
	if o.isClosed() {
		return nil
	}
	return <-o.free
}

// CheckoutCtx is like Checkout but returns nil if ctx is cancelled before a
// connection becomes available.
func (o *DBPool) CheckoutCtx(ctx context.Context) *Conn {
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

func (o *DBPool) Checkin(c *Conn) {
	if o.isClosed() {
		// Pool is closing/closed; close the connection directly instead of
		// returning it to a channel that may already be drained.
		if c != nil {
			c.Close()
		}
		return
	}
	o.free <- c
}

func (o *DBPool) CheckoutWriter() *Conn {
	if o.isClosed() {
		return nil
	}
	return <-o.wfree
}

// CheckoutWriterCtx is like CheckoutWriter but returns nil if ctx is
// cancelled before the writer becomes available.
func (o *DBPool) CheckoutWriterCtx(ctx context.Context) *Conn {
	if o.isClosed() {
		return nil
	}
	select {
	case c := <-o.wfree:
		return c
	case <-ctx.Done():
		return nil
	}
}

func (o *DBPool) CheckinWriter(c *Conn) {
	if o.isClosed() {
		if c != nil {
			c.Close()
		}
		return
	}
	o.wfree <- c
}

func (o *DBPool) Close() {
	o.closeOnce.Do(func() {
		o.mu.Lock()
		o.closed = true
		o.mu.Unlock()

		// Close all pooled connections. Drain the channels first so that any
		// connections checked in concurrently are closed here rather than
		// being sent to a (potentially drained) channel.
		for {
			select {
			case c := <-o.free:
				c.Close()
			default:
				goto doneFree
			}
		}
	doneFree:
		for _, c := range o.conns {
			c.Close()
		}
		// Close the writer connection (it should be sitting in wfree).
		for {
			select {
			case c := <-o.wfree:
				c.Close()
			default:
				goto doneWfree
			}
		}
	doneWfree:
		o.wconn.Close()
	})
}

func (o *DBPool) Exec(sql string, args ...interface{}) error {
	db := o.CheckoutWriter()
	if db == nil {
		return ErrPoolClosed
	}
	defer o.CheckinWriter(db)
	stmt, err := db.Prepare(sql)
	if err != nil {
		log.Println("STMT ERR", err)
		return err
	}
	if stmt.Tail != "" {
		err = db.Conn.Exec(sql, args...)
	} else {
		err = stmt.Exec(args...)
	}
	if err != nil {
		log.Println("EXEC ERR", err, args)
		return err
	}
	return err
}

func (o *DBPool) Select(dest interface{}, sql string, args ...interface{}) error {
	db := o.Checkout()
	if db == nil {
		return ErrPoolClosed
	}
	defer o.Checkin(db)
	return db.Select(dest, sql, args...)
}

func (o *DBPool) Get(dest interface{}, sql string, args ...interface{}) error {
	db := o.Checkout()
	if db == nil {
		return ErrPoolClosed
	}
	defer o.Checkin(db)
	return db.Get(dest, sql, args...)
}

func (o *DBPool) InsertValues(tableSQL string, attrs map[string]interface{}) error {
	db := o.CheckoutWriter()
	if db == nil {
		return ErrPoolClosed
	}
	defer o.CheckinWriter(db)
	_, err := db.InsertValues(tableSQL, attrs)
	return err
}

func (o *DBPool) UpdateValues(tableSQL string, attrs map[string]interface{}, whereStr string, whereVals ...interface{}) error {
	db := o.CheckoutWriter()
	if db == nil {
		return ErrPoolClosed
	}
	defer o.CheckinWriter(db)
	_, err := db.UpdateValues(tableSQL, attrs, whereStr, whereVals...)
	return err
}

func (o *DBPool) Tx(f func(c *Conn) error) (err error) {
	conn := o.CheckoutWriter()
	if conn == nil {
		return ErrPoolClosed
	}
	defer o.CheckinWriter(conn)

	if err = conn.Begin(); err != nil {
		return fmt.Errorf("sqx: failed to begin transaction: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			// Either f returned an error, or panicked. Roll back, but never
			// convert a rollback failure into a panic that masks the original
			// error/panic.
			if rbErr := conn.Rollback(); rbErr != nil {
				log.Printf("sqx: rollback after error failed: %v", rbErr)
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
	wconn, err := NewConn(uri, false)
	if err != nil {
		return nil, err
	}
	pool := &DBPool{
		size:  size,
		free:  make(chan *Conn, size),
		wfree: make(chan *Conn, 1),
		wconn: wconn,
	}
	for i := 0; i < size; i++ {
		conn, err := NewConn(uri, true)
		if err != nil {
			pool.Close() // clean up connections opened so far
			return nil, err
		}
		pool.conns = append(pool.conns, conn)
		pool.Checkin(conn)
	}
	pool.CheckinWriter(pool.wconn)
	return pool, nil
}

type BulkInserterCommand struct {
	Args []interface{}
}

type BulkInserter struct {
	prefix     string
	onConflict string
	Size       int
	count      int
	mu         sync.Mutex
	cmds       []BulkInserterCommand
	conn       *Conn
}

// NewBulkInserter returns a string builder
// prefix should be in form of "insert into table
func NewBulkInserter(prefix string, onConflict string, conn *Conn) *BulkInserter {
	inserter := &BulkInserter{
		prefix:     prefix,
		onConflict: onConflict,
		Size:       MAX_BINDS,
		conn:       conn,
	}
	return inserter
}

var ErrArgsGreaterThanSize = errors.New("size cannot support so many args")

// ErrPoolClosed is returned by DBPool methods after Close has been called.
var ErrPoolClosed = errors.New("sqx: connection pool is closed")

func (o *BulkInserter) Add(args ...interface{}) (err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	numArgs := len(args)
	if numArgs == 0 {
		return nil
	}
	// A single command that exceeds the bind limit can never be executed.
	if numArgs > o.Size {
		return fmt.Errorf("%w: command has %d args but size is %d", ErrArgsGreaterThanSize, numArgs, o.Size)
	}
	cmd := BulkInserterCommand{
		Args: args,
	}
	// Flush when adding this command would *exceed* the limit, so a batch
	// can actually reach the full size rather than capping at Size-1.
	if o.count+numArgs > o.Size {
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
		numArgs := len(cmd.Args)
		for ii, arg := range cmd.Args {
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
	stmt, err := o.conn.Conn.Prepare(b.String())
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

func InSQL[T comparable](sql string, in []T) (string, error) {
	if len(in) == 0 {
		return "", fmt.Errorf("InSQL: empty input would produce invalid \"IN ()\"")
	}
	if len(in) > MAX_BINDS {
		return "", fmt.Errorf("cannot have more than %d bindvars", MAX_BINDS)
	}
	binds := make([]string, len(in))
	for i := range in {
		binds[i] = "?"
	}
	bindStr := strings.Join(binds, ",")
	newSQL := fmt.Sprintf("%s in (%s)", sql, bindStr)
	return newSQL, nil
}
