package sqx

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sandro/go-sqlite-lite/sqlite3"
)

const MAX_BINDS = 999

var defaultTimeFormats []string = []string{
	time.RFC3339,
	"2006-01-02 15:04:05",
}

var SupportedTimeFormats []string = defaultTimeFormats

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
}

func NewConn(uri string, readonly bool) *Conn {
	c, err := sqlite3.Open(uri)
	check(err)
	c.BusyTimeout(time.Second * 5)
	c.Exec("PRAGMA foreign_keys = ON;")
	if readonly {
		c.Exec("PRAGMA query_only = ON;")
	}
	conn := Conn{Conn: c, stmtCache: make(map[string]*sqlite3.Stmt)}
	return &conn
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
	stmt, ok := o.stmtCache[sql]
	// TODO check if statement is closed before proceeding
	if ok {
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
	for _, stmt := range o.stmtCache {
		stmt.Close()
	}
	o.Conn.Close()
}

func (o *Conn) Get(dest interface{}, sql string, args ...interface{}) error {
	stmt, err := o.Prepare(sql)
	if err != nil {
		log.Println("Prepare statement failed", sql, args)
		// log.Panic(err)
		return err
	}
	err = stmt.Bind(args...)
	if err != nil {
		return err
	}
	defer stmt.Reset()
	hasRow, err := stmt.Step()
	if err != nil {
		return err
	}
	if !hasRow {
		return nil
		// log.Println("no rows")
	}
	value := reflect.ValueOf(dest).Elem()
	dbToStruct(value, stmt)
	return err
}

func (o *Conn) Select(dest interface{}, sql string, args ...interface{}) error {
	stmt, err := o.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Reset()
	stmt.Bind(args...)
	if err != nil {
		return err
	}
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			// log.Println("no row")
			break
		}
		// var id int64
		// stmt.Scan(&id)
		// log.Println("has row", stmt.ColumnNames(), id)

		value := reflect.ValueOf(dest)
		direct := reflect.Indirect(value)
		slice := value.Type().Elem()
		base := slice.Elem()
		vp := reflect.New(base)
		v := vp.Elem()
		dbToStruct(v, stmt)
		direct.Set(reflect.Append(direct, v))
	}
	return err
}

func (o *Conn) Exec2(sql string, args ...interface{}) (sql.Result, error) {
	stmt, err := o.Prepare(sql)
	if err != nil {
		log.Println("STMT ERR", err)
		return o, err
	}
	if stmt.Tail != "" {
		_, err = o.Exec(sql, args...)
	} else {
		err = stmt.Exec(args...)
	}
	if err != nil {
		log.Println("EXEC ERR", err, sql, args)
		return o, err
	}
	return o, err
}

// stmt, err := conn.Prepare(`insert or replace into vendors (id, name, indexed_at, location, created_at, updated_at)
// VALUES (?, ?, ?, ?, ?, ?)`)

func (o *Conn) InsertValues(tableSQL string, attrs map[string]interface{}) (sql.Result, error) {
	if len(attrs) > MAX_BINDS {
		return nil, fmt.Errorf("cannot have more than %d bindvars", MAX_BINDS)
	}
	colNames := make([]string, len(attrs))
	binds := make([]string, len(attrs))
	values := make([]interface{}, len(attrs))
	i := 0
	for k, v := range attrs {
		colNames[i] = k
		binds[i] = "?"
		values[i] = v
		i++
	}
	tableSQL += " (" + strings.Join(colNames, ",") + ")"
	tableSQL += "VALUES(" + strings.Join(binds, ",") + ")"
	return o.Exec2(tableSQL, values...)
}

// update users set x=1
func (o *Conn) UpdateValues(tableSQL string, attrs map[string]interface{}, whereStr string, whereVals ...interface{}) (sql.Result, error) {
	if len(attrs)+len(whereVals) > MAX_BINDS {
		return nil, fmt.Errorf("cannot have more than %d bindvars", MAX_BINDS)
	}
	colNames := make([]string, len(attrs))
	values := make([]interface{}, len(attrs))
	i := 0
	for k, v := range attrs {
		colNames[i] = k
		values[i] = v
		i++
	}
	for i, name := range colNames {
		tableSQL += fmt.Sprintf(" %s=?", name)
		if i < len(colNames)-1 {
			tableSQL += ","
		}
	}
	tableSQL += " " + whereStr
	return o.Exec2(tableSQL, append(values, whereVals...)...)
}

func (o *Conn) LastInsertId() (int64, error) {
	return o.Conn.LastInsertRowID(), nil
}

func (o *Conn) RowsAffected() (int64, error) {
	return int64(o.Conn.Changes()), nil
}

var timeType reflect.Type = reflect.TypeOf(time.Time{})
var byteArrayType reflect.Type = reflect.TypeOf([]byte{})

func quacksTime(v reflect.Value) bool {
	if v.Type() == timeType {
		return true
	}
	if v.CanConvert(timeType) {
		return true
	}
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if quacksTime(v.Field(i)) {
				return true
			}
		}
	case reflect.Ptr:
		return quacksTime(v.Elem())
	case reflect.Interface:
		return quacksTime(v.Elem())
	default:
		return v.Type() == timeType
	}
	return false
}

func isTime(value reflect.Value) bool {
	return value.Type() == timeType || value.CanConvert(timeType)
}

var TimeSetter = func(val any) time.Time {
	switch val.(type) {
	case int:
		return time.Unix(val.(int64), 0)
	case float64:
		valF := val.(float64)
		seconds := int64(valF)
		nsecs := math.Ceil(valF - float64(seconds)*1e6)
		return time.Unix(seconds, int64(nsecs))
	case string:
		txt := val.(string)
		for _, format := range SupportedTimeFormats {
			tm, err := time.Parse(format, txt)
			if err == nil {
				return tm
			}
		}
		secs, err := strconv.ParseInt(txt, 10, 64)
		if err == nil {
			return time.Unix(secs, 0)
		}
		log.Println("Could not set time from", txt)
	}
	return time.Time{}
}

func setTime(value reflect.Value, val interface{}) {
	tm := TimeSetter(val)
	if !tm.IsZero() {
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

func fillField(field reflect.Value, stmt *sqlite3.Stmt, colName string, index int) {
	switch field.Kind() {
	case reflect.Bool:
		b, _, err := stmt.ColumnInt64(index)
		check(err)
		field.SetBool(b != 0)
	case reflect.String:
		val, _, err := stmt.ColumnText(index)
		check(err)
		field.SetString(val)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		// log.Println("found int continue")
		val, _, err := stmt.ColumnInt64(index)
		field.SetInt(val)
		check(err)
	case reflect.Float32, reflect.Float64:
		val, _, err := stmt.ColumnDouble(index)
		field.SetFloat(val)
		check(err)
	case reflect.Slice:
		if field.CanConvert(byteArrayType) {
			val, err := stmt.ColumnBlob(index)
			check(err)
			field.SetBytes(val)
		} else {
			log.Println("sqx: unimplemented conversion for slice", colName, field)
		}
	case reflect.Struct:
		if isTime(field) {
			var err error
			var val interface{}
			switch typ := stmt.ColumnType(index); typ {
			case sqlite3.INTEGER:
				val, _, err = stmt.ColumnInt64(index)
			case sqlite3.FLOAT:
				val, _, err = stmt.ColumnDouble(index)
			case sqlite3.TEXT:
				val, _, err = stmt.ColumnText(index)
			default:
				log.Printf("Cannot set time for column %s with type %d (field %v)\n", colName, typ, field)
			}
			check(err)
			setTime(field, val)
		} else {
			t := field.Type()
			base := reflect.New(t)
			m := base.MethodByName("UnmarshalText")
			if !m.IsZero() {
				val, err := stmt.ColumnBlob(index)
				check(err)
				if len(val) == 0 {
					// log.Println("SKIPPING", colName, string(val), len(val))
					// continue
				}
				// log.Println("col blob", string(val))
				res := m.Call([]reflect.Value{reflect.ValueOf(val)})
				if !res[0].IsNil() {
					err = res[0].Interface().(error)
					log.Println("have error", err)
					// continue
					// check(err)
				}
				field.Set(base.Elem())
				// err = m.Call
				// z := base.Interface().(*time.Time)
				// log.Println("z", z, string(val))
				// err = z.UnmarshalJSON(val)
				// check(err)
				// log.Println(z)
			} else {
				log.Println("No UnmarshalText found", colName, field, t, base)
			}
		}
	default:
		log.Panicln("unknown reflection type", colName, field, field.Kind())
	}
}

func fillStruct(value reflect.Value, stmt *sqlite3.Stmt, colName string, index int) {
	for fi := 0; fi < value.NumField(); fi++ {
		name := getFieldName(value.Type().Field(fi))
		f := value.Field(fi)
		if name == colName || strings.ToLower(name) == colName {
			fillField(f, stmt, colName, index)
		} else if f.Kind() == reflect.Struct {
			fillStruct(f, stmt, colName, index)
		}
	}
}

// https://github.com/blockloop/scan/blob/master/scanner.go
func dbToStruct(value reflect.Value, stmt *sqlite3.Stmt) {
	// now := time.Now()
	vType := value.Type()
	nCols := stmt.ColumnCount()
	nFields := value.NumField()
	for i := 0; i < nCols; i++ {
		colName := stmt.ColumnName(i)
		matched := false
		var structFields []reflect.Value
		for fi := 0; fi < nFields; fi++ {
			fieldType := vType.Field(fi)
			name := getFieldName(fieldType)
			f := value.Field(fi)
			if name == colName || strings.ToLower(name) == colName {
				fillField(f, stmt, colName, i)
				matched = true
				break
			} else {
				if f.Kind() == reflect.Struct {
					structFields = append(structFields, f)
				}
			}
		}
		if !matched {
			for _, field := range structFields {
				fillStruct(field, stmt, colName, i)
			}
		}
	}
	// fmt.Println("dbToStruct done", time.Since(now))
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
}

func (o *DBPool) Checkout() *Conn {
	return <-o.free
}

func (o *DBPool) Checkin(c *Conn) {
	o.free <- c
}

func (o *DBPool) CheckoutWriter() *Conn {
	return <-o.wfree
}

func (o *DBPool) CheckinWriter(c *Conn) {
	o.wfree <- c
}

func (o *DBPool) Close() {
	log.Println("DBPool Close")
	// debug.PrintStack()
	close(o.free)
	for _, c := range o.conns {
		c.Close()
	}
	close(o.wfree)
	o.wconn.Close()
}

func (o *DBPool) Exec(sql string, args ...interface{}) error {
	db := o.CheckoutWriter()
	defer o.CheckinWriter(db)
	// log.Println("EXEC", sql, args)
	stmt, err := db.Prepare(sql)
	if err != nil {
		log.Println("STMT ERR", err)
		return err
	}
	// log.Println("stmt", stmt)
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
	defer o.Checkin(db)
	return db.Select(dest, sql, args...)
}

func (o *DBPool) Get(dest interface{}, sql string, args ...interface{}) error {
	db := o.Checkout()
	defer o.Checkin(db)
	return db.Get(dest, sql, args...)
}

func (o DBPool) InsertValues(tableSQL string, attrs map[string]interface{}) error {
	db := o.CheckoutWriter()
	defer o.CheckinWriter(db)
	_, err := db.InsertValues(tableSQL, attrs)
	return err
}

func (o DBPool) UpdateValues(tableSQL string, attrs map[string]interface{}, whereStr string, whereVals ...interface{}) error {
	db := o.CheckoutWriter()
	defer o.CheckinWriter(db)
	_, err := db.UpdateValues(tableSQL, attrs, whereStr, whereVals...)
	return err
}

func (o *DBPool) Tx(f func(c *Conn) error) error {
	conn := o.CheckoutWriter()
	defer o.CheckinWriter(conn)
	check(conn.Begin())

	defer func() {
		if r := recover(); r != nil {
			log.Println("recovered", r)
			check(conn.Rollback())
			panic(r)
		}
	}()

	err := f(conn)
	if err != nil {
		check(conn.Rollback())
	} else {
		check(conn.Commit())
	}
	return err
}

func NewDBPool(uri string, size int) *DBPool {
	pool := DBPool{
		size:  size,
		free:  make(chan *Conn, size),
		wfree: make(chan *Conn, 1),
		wconn: NewConn(uri, false),
	}
	for i := 0; i < size; i++ {
		// p, err := url.Parse(uri)
		// check(err)
		// q := p.Query()
		// q.Set("mode", "ro")
		// p.RawQuery = q.Encode()
		// log.Println("setting query", p.String())
		// conn := NewConn(p.String(), true)
		conn := NewConn(uri, true)
		pool.conns = append(pool.conns, conn)
		pool.Checkin(conn)
	}
	pool.CheckinWriter(pool.wconn)
	return &pool
}

type BulkInserterCommand struct {
	Args []interface{}
}

type BulkInserter struct {
	prefix string
	Size   int
	count  int
	mu     sync.Mutex
	cmds   []BulkInserterCommand
	conn   *Conn
}

// NewBulkInserter returns a string builder
// prefix should be in form of "insert into table
func NewBulkInserter(prefix string, conn *Conn) *BulkInserter {
	inserter := &BulkInserter{
		prefix: prefix,
		Size:   MAX_BINDS,
		conn:   conn,
	}
	return inserter
}

var ErrArgsGreaterThanSize = errors.New("size cannot support so many args")

func (o *BulkInserter) Add(args ...interface{}) (err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	// stmt, err := o.conn.Prepare(sql)
	// check(err)
	// stmt.Exec(args...)
	// stmt.ClearBindings()
	numArgs := len(args)
	if numArgs > o.Size {
		return ErrArgsGreaterThanSize
	}
	cmd := BulkInserterCommand{
		Args: args,
	}
	if o.count+numArgs >= o.Size {
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
	sql := strings.Builder{}
	sql.WriteString(o.prefix)
	sql.WriteString(" VALUES ")
	allArgs := []interface{}{}
	for i, cmd := range o.cmds {
		sql.WriteString("(")
		numArgs := len(cmd.Args)
		for ii, arg := range cmd.Args {
			sql.WriteString("?")
			if ii < numArgs-1 {
				sql.WriteString(",")
			}
			allArgs = append(allArgs, arg)
		}
		sql.WriteString(")")
		if i < numCommands-1 {
			sql.WriteString(", ")
		}
	}
	stmt, err := o.conn.Prepare(sql.String())
	if err != nil {
		return err
	}
	err = stmt.Exec(allArgs...)
	if err != nil {
		return err
	}
	o.cmds = nil
	o.count = 0
	return err
}

func (o *BulkInserter) Done() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.commit()
}

func intsToStr(ids []int64) (strs []string) {
	for _, id := range ids {
		strs = append(strs, strconv.FormatInt(id, 10))
	}
	return
}

func InSQL[T comparable](sql string, in []T) string {
	if len(in) > MAX_BINDS {
		return fmt.Sprintf("cannot have more than %d bindvars", MAX_BINDS)
	}
	binds := make([]string, len(in))
	for i := range in {
		binds[i] = "?"
	}
	bindStr := strings.Join(binds, ",")
	newSQL := fmt.Sprintf("%s in (%s)", sql, bindStr)
	return newSQL
}
