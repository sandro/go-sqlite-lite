package sqx

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sandro/go-sqlite-lite/sqlite3"
)

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
		log.Println("EXEC ERR", err, args)
		return o, err
	}
	return o, err
}

// stmt, err := conn.Prepare(`insert or replace into vendors (id, name, indexed_at, location, created_at, updated_at)
// VALUES (?, ?, ?, ?, ?, ?)`)

func (o *Conn) InsertValues(tableSQL string, colNames []string, values ...interface{}) (sql.Result, error) {
	tableSQL += " (" + strings.Join(colNames, ",") + ")"
	binds := []string{}
	for range colNames {
		binds = append(binds, "?")
	}
	tableSQL += "VALUES(" + strings.Join(binds, ",") + ")"
	return o.Exec2(tableSQL, values...)
}

// update users set x=1
func (o *Conn) UpdateValues(tableSQL string, where string, colNames []string, values ...interface{}) (sql.Result, error) {
	for i, name := range colNames {
		tableSQL += fmt.Sprintf(" %s=?", name)
		if i < len(colNames)-1 {
			tableSQL += ","
		}
	}
	tableSQL += " " + where
	return o.Exec2(tableSQL, values...)
}

func (o *Conn) LastInsertId() (int64, error) {
	return o.Conn.LastInsertRowID(), nil
}

func (o *Conn) RowsAffected() (int64, error) {
	return int64(o.Conn.Changes()), nil
}

// https://github.com/blockloop/scan/blob/master/scanner.go
func dbToStruct(value reflect.Value, stmt *sqlite3.Stmt) {
	// log.Println("IN dbToStruct", value)
	vType := value.Type()
	nCols := stmt.ColumnCount()
	// log.Println("vType", vType, nCols, value.NumField())
	for fi := 0; fi < value.NumField(); fi++ {
		field := vType.Field(fi)
		// log.Println("field is", field, value.Field(fi), field.Type.Kind(), value.NumField())
		if field.Type.Kind() == reflect.Struct && field.Type.String() != "time.Time" {
			// log.Println("field is struct, call dbToStruct")
			dbToStruct(value.Field(fi), stmt)
			continue
		}
		name := field.Name
		tag := field.Tag.Get("db")
		if tag != "" {
			name = tag
		}
		f := value.Field(fi)

		for i := 0; i < nCols; i++ {
			colName := stmt.ColumnName(i)
			// log.Println("name colname i", name, colName, i)
			if name == colName || strings.ToLower(name) == colName {
				// log.Println("getting colName", colName, nCols, i)
				// f := value.FieldByName(name)
				// f := value.Field(i)
				// log.Println(colName, f.Kind()) //, f.Type().Name())
				switch f.Kind() {
				case reflect.Bool:
					b, _, err := stmt.ColumnInt64(i)
					check(err)
					f.SetBool(b != 0)
				case reflect.String:
					val, _, err := stmt.ColumnText(i)
					check(err)
					f.SetString(val)
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					// log.Println("found int continue")
					val, _, err := stmt.ColumnInt64(i)
					f.SetInt(val)
					check(err)
				case reflect.Float32, reflect.Float64:
					val, _, err := stmt.ColumnDouble(i)
					f.SetFloat(val)
					check(err)
				case reflect.Slice:
					val, err := stmt.ColumnBlob(i)
					check(err)
					f.SetBytes(val)
				case reflect.Struct:
					// log.Println("case statement has struct", f.Kind())
					t := f.Type()
					base := reflect.New(t)
					m := base.MethodByName("UnmarshalText")
					if !m.IsZero() {
						val, err := stmt.ColumnBlob(i)
						check(err)
						if len(val) == 0 {
							// log.Println("SKIPPING", colName, string(val), len(val))
							continue
						}
						// log.Println("col blob", string(val))
						res := m.Call([]reflect.Value{reflect.ValueOf(val)})
						if !res[0].IsNil() {
							err = res[0].Interface().(error)
							log.Println("have error", err)
							continue
							// check(err)
						}
						f.Set(base.Elem())
						// err = m.Call
						// z := base.Interface().(*time.Time)
						// log.Println("z", z, string(val))
						// err = z.UnmarshalJSON(val)
						// check(err)
						// log.Println(z)
					} else {
						log.Println("No UnmarshalText found", colName, f, t, base)
					}
				default:
					log.Panicln("unknown reflection type", colName, f, f.Kind())
				}
				break
			}
		}
	}
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

func (o DBPool) InsertValues(tableSQL string, colNames []string, values ...interface{}) error {
	db := o.CheckoutWriter()
	defer o.CheckinWriter(db)
	_, err := db.InsertValues(tableSQL, colNames, values...)
	return err
}

func (o DBPool) UpdateValues(tableSQL, where string, colNames []string, values ...interface{}) error {
	db := o.CheckoutWriter()
	defer o.CheckinWriter(db)
	_, err := db.UpdateValues(tableSQL, where, colNames, values...)
	return err
}

func (o *DBPool) Tx(f func(c *Conn) error) {
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
	Sql  string
	Args []interface{}
}

type BulkInserter struct {
	size  int
	count int
	// conn  *Conn
	mu   sync.Mutex
	cmds []BulkInserterCommand
	pool *DBPool
}

func NewBulkInserter(size int, pool *DBPool) *BulkInserter {
	return &BulkInserter{
		size: size,
		pool: pool,
	}
}

func (o *BulkInserter) Add(sql string, args ...interface{}) {
	o.mu.Lock()
	defer o.mu.Unlock()
	// stmt, err := o.conn.Prepare(sql)
	// check(err)
	// stmt.Exec(args...)
	// stmt.ClearBindings()
	cmd := BulkInserterCommand{
		Sql:  sql,
		Args: args,
	}
	o.cmds = append(o.cmds, cmd)
	o.count++
	if o.count == o.size {
		o.commit()
		// check(o.conn.Begin())
	}
}

func (o *BulkInserter) commit() {
	conn := o.pool.CheckoutWriter()
	defer o.pool.CheckinWriter(conn)
	check(conn.Begin())
	for _, cmd := range o.cmds {
		stmt, err := conn.Prepare(cmd.Sql)
		check(err)
		stmt.Exec(cmd.Args...)
	}
	check(conn.Commit())
	o.cmds = nil
	o.count = 0
}

func (o *BulkInserter) Done() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.commit()
}

func intsToStr(ids []int64) (strs []string) {
	for _, id := range ids {
		strs = append(strs, strconv.FormatInt(id, 10))
	}
	return
}

func InSQL(sql string, in []string) string {
	binds := make([]string, len(in))
	for i := range in {
		binds[i] = "?"
	}
	bindStr := strings.Join(binds, ",")
	newSQL := fmt.Sprintf("%s in (%s)", sql, bindStr)
	return newSQL
}
