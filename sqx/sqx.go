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
	"unsafe"

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
	stmtCache  map[string]*sqlite3.Stmt
	planCache  map[string]*scanPlan // keyed by SQL string, like stmtCache
	closed     bool
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
	conn := &Conn{Conn: c, stmtCache: make(map[string]*sqlite3.Stmt), planCache: make(map[string]*scanPlan)}
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
	o.planCache = nil
	o.Conn.Close()
}

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
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("sqx: dest must be a pointer, got %T", dest)
	}
	elem := v.Elem()
	if elem.Kind() != reflect.Struct {
		return fmt.Errorf("sqx: dest must point to a struct, got %s", elem.Kind())
	}
	plan, err := o.cachedPlan(sql, elem.Type(), stmt)
	if err != nil {
		return err
	}
	return applyPlan(plan, unsafe.Pointer(elem.UnsafeAddr()), stmt)
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

var TimeSetter = setTimeFromValue

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
	log.Println("Could not set time from", s)
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
	typeID  uintptr // reflect.Type pointer identity (via reflect.Type.Pointer())
	colKey  string  // joined column names with a separator
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
		// Scalar-like field (including time.Time): try direct column match.
		if fieldType.Type.Kind() != reflect.Struct || fieldType.Type == timeType {
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
			return nil, fmt.Errorf("sqx: unsupported slice type %s (only []byte)", field.Type)
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
				case sqlite3.INTEGER:
					v, _, err := stmt.ColumnInt64(col)
					if err != nil {
						return err
					}
					val = v
				case sqlite3.FLOAT:
					v, _, err := stmt.ColumnDouble(col)
					if err != nil {
						return err
					}
					val = v
				case sqlite3.TEXT:
					v, _, err := stmt.ColumnText(col)
					if err != nil {
						return err
					}
					val = v
				case sqlite3.NULL:
					return nil
				case sqlite3.BLOB:
					v, err := stmt.ColumnBlob(col)
					if err != nil {
						return err
					}
					val = v
				default:
					return fmt.Errorf("sqx: cannot set time for column type %d", typ)
				}
				tm, ok := TimeSetter(val)
				if ok && !tm.IsZero() {
					*(*time.Time)(unsafe.Pointer(uintptr(p) + offset)) = tm
				}
				return nil
			}
		} else {
			// Non-time struct: try UnmarshalText.
			m := reflect.New(field.Type).MethodByName("UnmarshalText")
			if !m.IsZero() {
				entry.setter = func(p unsafe.Pointer, stmt *sqlite3.Stmt, col int) error {
					v, err := stmt.ColumnBlob(col)
					if err != nil {
						return err
					}
					// UnmarshalText writes into a fresh value; copy it into place.
					base := reflect.New(field.Type)
					res := m.Call([]reflect.Value{reflect.ValueOf(v)})
					if !res[0].IsNil() {
						if uerr, ok := res[0].Interface().(error); ok {
							return fmt.Errorf("sqx: UnmarshalText failed: %w", uerr)
						}
						return fmt.Errorf("sqx: UnmarshalText failed")
					}
					// Copy the unmarshaled value into the struct via unsafe.
					src := unsafe.Pointer(base.Pointer())
					dst := unsafe.Pointer(uintptr(p) + offset)
					typedMemmove(dst, src, field.Type)
					return nil
				}
			} else {
				return nil, fmt.Errorf("sqx: struct field %s has no UnmarshalText and is not time.Time", field.Name)
			}
		}
	case reflect.Ptr:
		// nil pointer → allocate via reflect.New + convertAssign (rare path).
		entry.setter = func(p unsafe.Pointer, stmt *sqlite3.Stmt, col int) error {
			if stmt.ColumnType(col) == sqlite3.NULL {
				*(*unsafe.Pointer)(unsafe.Pointer(uintptr(p) + offset)) = nil
				return nil
			}
			// Fall back to reflection for pointer-to-struct/other types.
			vp := reflect.NewAt(field.Type, unsafe.Pointer(uintptr(p)+offset))
			return stmt.Scan(vp.Interface())
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
