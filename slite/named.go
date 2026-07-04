// Copyright 2018 The go-sqlite-lite Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package slite

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// In expands slice values in args, returning the modified query string and a
// new arg list. The query should use the ? bindVar. For each ? that corresponds
// to a slice argument (not []byte), the ? is expanded to ?, ?, ... matching the
// slice length.
//
// Example:
//
//	query, args, err := In("SELECT * FROM t WHERE id IN (?) AND name = ?", []int{1, 2, 3}, "foo")
//	// query  = "SELECT * FROM t WHERE id IN (?, ?, ?) AND name = ?"
//	// args   = []interface{}{1, 2, 3, "foo"}
func In(query string, args ...interface{}) (string, []interface{}, error) {
	// Quick path: if no args are slices, return unchanged.
	var anySlices bool
	for _, arg := range args {
		if _, ok := asSliceForIn(arg); ok {
			anySlices = true
			break
		}
	}
	if !anySlices {
		return query, args, nil
	}

	// First pass: collect metadata for each arg.
	type argMeta struct {
		v      reflect.Value // slice value (valid if length > 0)
		i      interface{}   // non-slice value
		length int           // slice length, 0 for non-slices
	}

	meta := make([]argMeta, len(args))
	flatCount := 0
	for i, arg := range args {
		if v, ok := asSliceForIn(arg); ok {
			n := v.Len()
			if n == 0 {
				return "", nil, fmt.Errorf("slite: In: empty slice passed at argument %d", i+1)
			}
			meta[i].v = v
			meta[i].length = n
			flatCount += n
		} else {
			meta[i].i = arg
			flatCount++
		}
	}

	newArgs := make([]interface{}, 0, flatCount)
	var buf strings.Builder
	buf.Grow(len(query) + 3*flatCount)

	var arg, offset int
	for {
		idx := strings.IndexByte(query[offset:], '?')
		if idx == -1 {
			break
		}

		if arg >= len(meta) {
			return "", nil, fmt.Errorf("slite: In: number of bindVars (?) exceeds arguments")
		}

		m := meta[arg]
		arg++

		if m.length == 0 {
			// Non-slice arg: keep the ? and copy query up to and including it.
			buf.WriteString(query[offset : offset+idx+1])
			offset = offset + idx + 1
			newArgs = append(newArgs, m.i)
			continue
		}

		// Slice arg: write the ? then add (len-1) more ", ?" pairs.
		buf.WriteString(query[offset : offset+idx+1])
		for j := 1; j < m.length; j++ {
			buf.WriteString(", ?")
		}
		newArgs = appendReflectSlice(newArgs, m.v, m.length)

		// Advance past the consumed ?.
		query = query[offset+idx+1:]
		offset = 0
	}

	// Write remaining query tail.
	buf.WriteString(query[offset:])

	if arg < len(meta) {
		return "", nil, fmt.Errorf("slite: In: number of bindVars (?) less than number of arguments")
	}

	return buf.String(), newArgs, nil
}

// asSliceForIn returns the reflect.Value of arg if it is a slice that should be
// expanded by In (i.e. not []byte). Returns ok=false for non-slices and []byte.
func asSliceForIn(i interface{}) (reflect.Value, bool) {
	if i == nil {
		return reflect.Value{}, false
	}
	v := reflect.ValueOf(i)
	t := v.Type()
	if t.Kind() != reflect.Slice {
		return reflect.Value{}, false
	}
	if t == reflect.TypeOf([]byte{}) {
		return reflect.Value{}, false
	}
	return v, true
}

// appendReflectSlice appends the elements of a reflect.Value slice to args.
// Common slice types are handled without per-element reflection.
func appendReflectSlice(args []interface{}, v reflect.Value, vlen int) []interface{} {
	switch val := v.Interface().(type) {
	case []interface{}:
		return append(args, val...)
	case []int:
		for i := range val {
			args = append(args, val[i])
		}
		return args
	case []int64:
		for i := range val {
			args = append(args, val[i])
		}
		return args
	case []string:
		for i := range val {
			args = append(args, val[i])
		}
		return args
	default:
		for i := 0; i < vlen; i++ {
			args = append(args, v.Index(i).Interface())
		}
		return args
	}
}

// ---------------------------------------------------------------------------
// Named parameters (:name)
// ---------------------------------------------------------------------------

// compileNamedQuery parses :name placeholders in query, replacing each with ?.
// Returns the rewritten query and the ordered list of parameter names.
// A :: escape sequence is replaced with a literal ':'.
// The := sequence is treated as a literal (not a named parameter).
func compileNamedQuery(query string) (string, []string, error) {
	var names []string
	var buf strings.Builder
	buf.Grow(len(query))

	i := 0
	for i < len(query) {
		b := query[i]

		if b != ':' {
			buf.WriteByte(b)
			i++
			continue
		}

		// We found a ':'. Check for escape sequences.
		if i+1 < len(query) {
			next := query[i+1]
			if next == ':' {
				// '::' → literal ':'
				buf.WriteByte(':')
				i += 2
				continue
			}
			if next == '=' {
				// ':=' → literal ':=' (not a named parameter)
				buf.WriteByte(':')
				buf.WriteByte('=')
				i += 2
				continue
			}
		}

		// Start of a named parameter. Consume the name.
		i++ // skip ':'
		start := i
		for i < len(query) && isNameChar(query[i]) {
			i++
		}
		if i == start {
			return "", nil, fmt.Errorf("slite: empty named parameter at position %d", start)
		}
		names = append(names, query[start:i])
		buf.WriteByte('?')
	}

	return buf.String(), names, nil
}

// isNameChar reports whether b is allowed in a named parameter (after the ':').
func isNameChar(b byte) bool {
	return (b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9') ||
		b == '_' || b == '.'
}

// Named takes a query with named parameters (:name) and an argument (struct or
// map[string]X), and returns a query with ? placeholders plus the ordered arg
// list ready to pass to Exec/Get/Select/Query.
//
// For struct arguments, fields are matched by their `db` tag, or by the
// exported field name if no tag is present. For map arguments, keys must be
// strings matching the parameter names.
//
// Example:
//
//	type User struct { Name string `db:"name"`; Age int `db:"age"` }
//	query, args, _ := Named("INSERT INTO users (name, age) VALUES (:name, :age)", &User{Name: "bob", Age: 30})
//	pool.Exec(query, args...)
func Named(query string, arg interface{}) (string, []interface{}, error) {
	bound, names, err := compileNamedQuery(query)
	if err != nil {
		return "", nil, err
	}
	args, err := bindArgs(names, arg)
	if err != nil {
		return "", nil, err
	}
	return bound, args, nil
}

// bindArgs resolves named parameter names against arg (a struct or a string-keyed
// map), returning the ordered list of positional values.
func bindArgs(names []string, arg interface{}) ([]interface{}, error) {
	if len(names) == 0 {
		return nil, nil
	}
	v := reflect.ValueOf(arg)

	// Handle string-keyed maps.
	if v.Kind() == reflect.Map && v.Type().Key().Kind() == reflect.String {
		result := make([]interface{}, len(names))
		for i, name := range names {
			val := v.MapIndex(reflect.ValueOf(name))
			if !val.IsValid() {
				return nil, fmt.Errorf("slite: named parameter %q not found in map", name)
			}
			result[i] = val.Interface()
		}
		return result, nil
	}

	// Dereference pointers to get to the struct.
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil, fmt.Errorf("slite: named query arg is a nil pointer")
		}
		v = v.Elem()
	}

	if v.Kind() == reflect.Struct {
		fields := structFieldsForNamed(v)
		result := make([]interface{}, len(names))
		for i, name := range names {
			val, ok := fields[name]
			if !ok {
				return nil, fmt.Errorf("slite: named parameter %q not found in struct %s", name, v.Type().Name())
			}
			result[i] = val
		}
		return result, nil
	}

	return nil, fmt.Errorf("slite: named query arg must be a struct or string-keyed map, got %T", arg)
}

// structFieldsForNamed builds a name→value map from a struct's exported fields.
// Names follow the `db` tag convention (falling back to the field name).
// Nested structs are included with dot-prefixed names (e.g. "Bar.FooID").
func structFieldsForNamed(v reflect.Value) map[string]interface{} {
	fields := make(map[string]interface{})
	collectStructFields(v, "", fields)
	return fields
}

func collectStructFields(v reflect.Value, prefix string, fields map[string]interface{}) {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}

		fv := v.Field(i)
		name := field.Name
		if tag := field.Tag.Get("db"); tag != "" {
			name = tag
		}
		fullName := prefix + name

		// Recurse into nested anonymous or exported struct fields.
		if fv.Kind() == reflect.Struct && field.Anonymous {
			collectStructFields(fv, prefix, fields)
			continue
		}
		if fv.Kind() == reflect.Struct {
			collectStructFields(fv, fullName+".", fields)
			continue
		}

		fields[fullName] = fv.Interface()
	}
}

// ---------------------------------------------------------------------------
// NamedStmt — a reusable prepared statement with named parameters
// ---------------------------------------------------------------------------

// NamedStmt is a prepared statement that accepts named parameters. Prepare it
// once with Conn.PrepareNamed or DBPool.PrepareNamed, then call Exec/Get/Select/
// Query with a struct or map argument whose fields/keys match the named params.
type NamedStmt struct {
	conn  *Conn
	query string // original query with :name placeholders
	bound string // compiled query with ? placeholders
	names []string
}

// Close releases the underlying prepared statement.
func (n *NamedStmt) Close() {
	// Conn.Prepare caches statements; there is nothing to close here.
	// The statement is released when the Conn is closed.
	n.conn = nil
}

// Exec executes the named statement using struct or map arg.
func (n *NamedStmt) Exec(arg interface{}) (sql.Result, error) {
	args, err := bindArgs(n.names, arg)
	if err != nil {
		return nil, err
	}
	return n.conn.Exec(n.bound, args...)
}

// Get executes the named statement and scans a single row into dest.
func (n *NamedStmt) Get(dest interface{}, arg interface{}) error {
	args, err := bindArgs(n.names, arg)
	if err != nil {
		return err
	}
	return n.conn.Get(dest, n.bound, args...)
}

// Select executes the named statement and scans all rows into dest (a pointer
// to a slice of structs).
func (n *NamedStmt) Select(dest interface{}, arg interface{}) error {
	args, err := bindArgs(n.names, arg)
	if err != nil {
		return err
	}
	return n.conn.Select(dest, n.bound, args...)
}

// Query executes the named statement, calling f for each result row.
func (n *NamedStmt) Query(arg interface{}, f func(row *Row) error) error {
	args, err := bindArgs(n.names, arg)
	if err != nil {
		return err
	}
	return n.conn.Query(n.bound, f, args...)
}

// --- Conn named methods ---

// PrepareNamed compiles a named query (:name) and returns a reusable
// NamedStmt. The underlying prepared statement is cached on the Conn.
func (o *Conn) PrepareNamed(query string) (*NamedStmt, error) {
	bound, names, err := compileNamedQuery(query)
	if err != nil {
		return nil, err
	}
	// Pre-warm the statement cache by preparing the bound query.
	if _, err := o.Prepare(bound); err != nil {
		return nil, fmt.Errorf("slite: PrepareNamed %q: %w", query, err)
	}
	return &NamedStmt{conn: o, query: query, bound: bound, names: names}, nil
}

// NamedExec executes a named query (:name) with a struct or map argument.
func (o *Conn) NamedExec(query string, arg interface{}) (sql.Result, error) {
	bound, args, err := Named(query, arg)
	if err != nil {
		return nil, err
	}
	return o.Exec(bound, args...)
}

// NamedGet executes a named query (:name) and scans a single row into dest.
func (o *Conn) NamedGet(dest interface{}, query string, arg interface{}) error {
	bound, args, err := Named(query, arg)
	if err != nil {
		return err
	}
	return o.Get(dest, bound, args...)
}

// NamedSelect executes a named query (:name) and scans all rows into dest.
func (o *Conn) NamedSelect(dest interface{}, query string, arg interface{}) error {
	bound, args, err := Named(query, arg)
	if err != nil {
		return err
	}
	return o.Select(dest, bound, args...)
}

// NamedQuery executes a named query (:name), calling f for each result row.
func (o *Conn) NamedQuery(query string, arg interface{}, f func(row *Row) error) error {
	bound, args, err := Named(query, arg)
	if err != nil {
		return err
	}
	return o.Query(bound, f, args...)
}

// --- DBPool named methods ---

// PrepareNamed compiles a named query on a writer connection and returns a
// reusable NamedStmt bound to that connection. The NamedStmt must be used with
// the same pool.
func (o *DBPool) PrepareNamed(query string) (*NamedStmt, error) {
	db := o.checkoutWriter()
	if db == nil {
		return nil, ErrPoolClosed
	}
	defer o.checkinWriter()
	return db.PrepareNamed(query)
}

// NamedExec executes a named query (:name) with a struct or map argument.
func (o *DBPool) NamedExec(query string, arg interface{}) (sql.Result, error) {
	db := o.checkoutWriter()
	if db == nil {
		return nil, ErrPoolClosed
	}
	defer o.checkinWriter()
	return db.NamedExec(query, arg)
}

// NamedGet executes a named query (:name) and scans a single row into dest.
func (o *DBPool) NamedGet(dest interface{}, query string, arg interface{}) error {
	db := o.Checkout()
	if db == nil {
		return ErrPoolClosed
	}
	defer o.Checkin(db)
	return db.NamedGet(dest, query, arg)
}

// NamedSelect executes a named query (:name) and scans all rows into dest.
func (o *DBPool) NamedSelect(dest interface{}, query string, arg interface{}) error {
	db := o.Checkout()
	if db == nil {
		return ErrPoolClosed
	}
	defer o.Checkin(db)
	return db.NamedSelect(dest, query, arg)
}

// NamedQuery executes a named query (:name), calling f for each result row.
func (o *DBPool) NamedQuery(query string, arg interface{}, f func(row *Row) error) error {
	db := o.Checkout()
	if db == nil {
		return ErrPoolClosed
	}
	defer o.Checkin(db)
	return db.NamedQuery(query, arg, f)
}
