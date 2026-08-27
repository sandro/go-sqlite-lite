package slite

import (
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Scalar types: bool, int variants, uint variants, float, string, []byte
// ---------------------------------------------------------------------------

func TestScanBool(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (active INTEGER, label TEXT)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (1, 'yes')"))
	mustRes(conn.Exec("INSERT INTO t VALUES (0, 'no')"))

	type Row struct {
		Active bool   `db:"active"`
		Label  string `db:"label"`
	}
	var rows []Row
	if err := conn.Select(&rows, "SELECT active, label FROM t ORDER BY active"); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	if rows[0].Active != false || rows[0].Label != "no" {
		t.Errorf("row 0: got %+v, want {false no}", rows[0])
	}
	if rows[1].Active != true || rows[1].Label != "yes" {
		t.Errorf("row 1: got %+v, want {true yes}", rows[1])
	}
}

func TestScanIntVariants(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (val INTEGER, label TEXT)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (42, 'x')"))

	// Each struct has a second field to ensure adequate allocation size
	// for checkptr compatibility with -race.

	// int
	type RowInt struct {
		Val   int    `db:"val"`
		Label string `db:"label"`
	}
	var ri RowInt
	if err := conn.Get(&ri, "SELECT val, label FROM t"); err != nil {
		t.Fatal(err)
	}
	if ri.Val != 42 {
		t.Errorf("int: got %d, want 42", ri.Val)
	}

	// int8
	type RowInt8 struct {
		Val   int8   `db:"val"`
		Label string `db:"label"`
	}
	var ri8 RowInt8
	if err := conn.Get(&ri8, "SELECT val, label FROM t"); err != nil {
		t.Fatal(err)
	}
	if ri8.Val != 42 {
		t.Errorf("int8: got %d, want 42", ri8.Val)
	}

	// int16
	type RowInt16 struct {
		Val   int16  `db:"val"`
		Label string `db:"label"`
	}
	var ri16 RowInt16
	if err := conn.Get(&ri16, "SELECT val, label FROM t"); err != nil {
		t.Fatal(err)
	}
	if ri16.Val != 42 {
		t.Errorf("int16: got %d, want 42", ri16.Val)
	}

	// int32
	type RowInt32 struct {
		Val   int32  `db:"val"`
		Label string `db:"label"`
	}
	var ri32 RowInt32
	if err := conn.Get(&ri32, "SELECT val, label FROM t"); err != nil {
		t.Fatal(err)
	}
	if ri32.Val != 42 {
		t.Errorf("int32: got %d, want 42", ri32.Val)
	}

	// int64
	type RowInt64 struct {
		Val   int64  `db:"val"`
		Label string `db:"label"`
	}
	var ri64 RowInt64
	if err := conn.Get(&ri64, "SELECT val, label FROM t"); err != nil {
		t.Fatal(err)
	}
	if ri64.Val != 42 {
		t.Errorf("int64: got %d, want 42", ri64.Val)
	}
}

func TestScanUintVariants(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (val INTEGER, label TEXT)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (255, 'x')"))

	// uint
	type RowUint struct {
		Val   uint   `db:"val"`
		Label string `db:"label"`
	}
	var ru RowUint
	if err := conn.Get(&ru, "SELECT val, label FROM t"); err != nil {
		t.Fatal(err)
	}
	if ru.Val != 255 {
		t.Errorf("uint: got %d, want 255", ru.Val)
	}

	// uint8
	type RowUint8 struct {
		Val   uint8  `db:"val"`
		Label string `db:"label"`
	}
	var ru8 RowUint8
	if err := conn.Get(&ru8, "SELECT val, label FROM t"); err != nil {
		t.Fatal(err)
	}
	if ru8.Val != 255 {
		t.Errorf("uint8: got %d, want 255", ru8.Val)
	}

	// uint16
	type RowUint16 struct {
		Val   uint16 `db:"val"`
		Label string `db:"label"`
	}
	var ru16 RowUint16
	if err := conn.Get(&ru16, "SELECT val, label FROM t"); err != nil {
		t.Fatal(err)
	}
	if ru16.Val != 255 {
		t.Errorf("uint16: got %d, want 255", ru16.Val)
	}

	// uint32
	type RowUint32 struct {
		Val   uint32 `db:"val"`
		Label string `db:"label"`
	}
	var ru32 RowUint32
	if err := conn.Get(&ru32, "SELECT val, label FROM t"); err != nil {
		t.Fatal(err)
	}
	if ru32.Val != 255 {
		t.Errorf("uint32: got %d, want 255", ru32.Val)
	}

	// uint64
	type RowUint64 struct {
		Val   uint64 `db:"val"`
		Label string `db:"label"`
	}
	var ru64 RowUint64
	if err := conn.Get(&ru64, "SELECT val, label FROM t"); err != nil {
		t.Fatal(err)
	}
	if ru64.Val != 255 {
		t.Errorf("uint64: got %d, want 255", ru64.Val)
	}
}

func TestScanFloat32(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (val REAL, label TEXT)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (3.14, 'pi')"))

	type Row struct {
		Val   float32 `db:"val"`
		Label string  `db:"label"`
	}
	var row Row
	if err := conn.Get(&row, "SELECT val, label FROM t"); err != nil {
		t.Fatal(err)
	}
	// float32 precision: compare within epsilon.
	if row.Val < 3.13 || row.Val > 3.15 {
		t.Errorf("float32: got %f, want ~3.14", row.Val)
	}
}

func TestScanFloat64(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (val REAL)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (3.14159265358979)"))

	type Row struct{ Val float64 `db:"val"` }
	var row Row
	if err := conn.Get(&row, "SELECT val FROM t"); err != nil {
		t.Fatal(err)
	}
	if row.Val != 3.14159265358979 {
		t.Errorf("float64: got %f, want 3.14159265358979", row.Val)
	}
}

func TestScanString(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (val TEXT)"))
	mustRes(conn.Exec("INSERT INTO t VALUES ('hello world')"))

	type Row struct{ Val string `db:"val"` }
	var row Row
	if err := conn.Get(&row, "SELECT val FROM t"); err != nil {
		t.Fatal(err)
	}
	if row.Val != "hello world" {
		t.Errorf("got %q, want %q", row.Val, "hello world")
	}
}

func TestScanByteSlice(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (data BLOB)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (x'DEADBEEF')"))

	type Row struct{ Data []byte `db:"data"` }
	var row Row
	if err := conn.Get(&row, "SELECT data FROM t"); err != nil {
		t.Fatal(err)
	}
	if len(row.Data) != 4 || row.Data[0] != 0xDE || row.Data[1] != 0xAD || row.Data[2] != 0xBE || row.Data[3] != 0xEF {
		t.Errorf("got %x, want DEADBEEF", row.Data)
	}
}

func TestScanByteSliceNull(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (data BLOB)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (NULL)"))

	type Row struct{ Data []byte `db:"data"` }
	var row Row
	if err := conn.Get(&row, "SELECT data FROM t"); err != nil {
		t.Fatal(err)
	}
	if row.Data != nil {
		t.Errorf("got %v, want nil for NULL blob", row.Data)
	}
}

// ---------------------------------------------------------------------------
// time.Time scanning from different SQLite column types
// ---------------------------------------------------------------------------

func TestScanTimeFromInteger(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (created_at INTEGER)"))
	// Store a Unix timestamp: 2024-01-15 00:00:00 UTC.
	ts := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC).Unix()
	mustRes(conn.Exec("INSERT INTO t VALUES (?)", ts))

	type Row struct{ CreatedAt time.Time `db:"created_at"` }
	var row Row
	if err := conn.Get(&row, "SELECT created_at FROM t"); err != nil {
		t.Fatal(err)
	}
	if row.CreatedAt.Unix() != ts {
		t.Errorf("got %v, want Unix %d", row.CreatedAt, ts)
	}
}

func TestScanTimeFromFloat(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (created_at REAL)"))
	// Float timestamp with sub-second precision.
	ts := 1705276800.5 // some Unix time with .5 seconds
	mustRes(conn.Exec("INSERT INTO t VALUES (?)", ts))

	type Row struct{ CreatedAt time.Time `db:"created_at"` }
	var row Row
	if err := conn.Get(&row, "SELECT created_at FROM t"); err != nil {
		t.Fatal(err)
	}
	// Should have parsed the float as Unix timestamp.
	if row.CreatedAt.IsZero() {
		t.Error("expected non-zero time from float")
	}
	if row.CreatedAt.Unix() != 1705276800 {
		t.Errorf("got Unix %d, want 1705276800", row.CreatedAt.Unix())
	}
}

func TestScanTimeFromString(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// RFC3339 format.
	mustRes(conn.Exec("CREATE TABLE t1 (created_at TEXT)"))
	mustRes(conn.Exec("INSERT INTO t1 VALUES ('2024-06-15T12:30:00Z')"))

	type Row struct{ CreatedAt time.Time `db:"created_at"` }
	var row Row
	if err := conn.Get(&row, "SELECT created_at FROM t1"); err != nil {
		t.Fatal(err)
	}
	want := time.Date(2024, 6, 15, 12, 30, 0, 0, time.UTC)
	if !row.CreatedAt.Equal(want) {
		t.Errorf("RFC3339: got %v, want %v", row.CreatedAt, want)
	}

	// SQLite datetime format: "2006-01-02 15:04:05"
	mustRes(conn.Exec("CREATE TABLE t2 (created_at TEXT)"))
	mustRes(conn.Exec("INSERT INTO t2 VALUES ('2024-06-15 12:30:00')"))

	var row2 Row
	if err := conn.Get(&row2, "SELECT created_at FROM t2"); err != nil {
		t.Fatal(err)
	}
	// time.Parse with "2006-01-02 15:04:05" gives UTC.
	want2 := time.Date(2024, 6, 15, 12, 30, 0, 0, time.UTC)
	if !row2.CreatedAt.Equal(want2) {
		t.Errorf("datetime: got %v, want %v", row2.CreatedAt, want2)
	}
}

func TestScanTimeFromNull(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (created_at TEXT)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (NULL)"))

	type Row struct{ CreatedAt time.Time `db:"created_at"` }
	var row Row
	if err := conn.Get(&row, "SELECT created_at FROM t"); err != nil {
		t.Fatal(err)
	}
	if !row.CreatedAt.IsZero() {
		t.Errorf("expected zero time for NULL, got %v", row.CreatedAt)
	}
}

func TestScanTimeFromStringAsUnixTimestamp(t *testing.T) {
	// A string that looks like a Unix timestamp (numeric string).
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (created_at TEXT)"))
	mustRes(conn.Exec("INSERT INTO t VALUES ('1705276800')"))

	type Row struct{ CreatedAt time.Time `db:"created_at"` }
	var row Row
	if err := conn.Get(&row, "SELECT created_at FROM t"); err != nil {
		t.Fatal(err)
	}
	if row.CreatedAt.Unix() != 1705276800 {
		t.Errorf("got Unix %d, want 1705276800", row.CreatedAt.Unix())
	}
}

// ---------------------------------------------------------------------------
// Nullable columns via Query + Row.IsNull
// ---------------------------------------------------------------------------

// Pointer fields (*string, *int64, *float64, *bool, *int, *[]byte) are
// supported for nullable columns. nil pointer = NULL, non-nil = value.
// For columns without pointer fields, you can also use Query with IsNull.

func TestNullableColumnsViaQuery(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (name TEXT, age INTEGER)"))
	mustRes(conn.Exec("INSERT INTO t VALUES ('alice', 30)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (NULL, NULL)"))

	type Result struct {
		Name     string
		Age      int64
		NameNull bool
		AgeNull  bool
	}
	var results []Result
	err = conn.Query("SELECT name, age FROM t ORDER BY name", func(row *Row) error {
		results = append(results, Result{
			Name:     row.Text("name"),
			Age:      row.Int("age"),
			NameNull: row.IsNull("name"),
			AgeNull:  row.IsNull("age"),
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}

	// NULL row sorts first (empty string < "alice").
	if !results[0].NameNull || !results[0].AgeNull {
		t.Errorf("row 0: expected nulls, got %+v", results[0])
	}
	if results[0].Name != "" || results[0].Age != 0 {
		t.Errorf("row 0: null columns should return zero values, got %+v", results[0])
	}

	if results[1].NameNull || results[1].AgeNull {
		t.Errorf("row 1: expected non-nulls, got %+v", results[1])
	}
	if results[1].Name != "alice" || results[1].Age != 30 {
		t.Errorf("row 1: got %+v, want {alice 30 false false}", results[1])
	}
}

// ---------------------------------------------------------------------------
// Nested struct scanning
// ---------------------------------------------------------------------------

func TestScanNestedStruct(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE users (name TEXT)"))
	mustRes(conn.Exec("CREATE TABLE addresses (city TEXT, user_name TEXT)"))
	mustRes(conn.Exec("INSERT INTO users VALUES ('alice')"))
	mustRes(conn.Exec("INSERT INTO addresses VALUES ('NYC', 'alice')"))

	type Address struct {
		City string `db:"city"`
	}
	type User struct {
		Name    string  `db:"name"`
		Address Address // nested struct — columns matched by field name
	}

	var user User
	if err := conn.Get(&user, "SELECT u.name, a.city FROM users u JOIN addresses a ON a.user_name = u.name"); err != nil {
		t.Fatal(err)
	}
	if user.Name != "alice" {
		t.Errorf("Name = %q, want alice", user.Name)
	}
	if user.Address.City != "NYC" {
		t.Errorf("Address.City = %q, want NYC", user.Address.City)
	}
}

func TestScanDeeplyNestedStruct(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (a TEXT, b TEXT, c TEXT)"))
	mustRes(conn.Exec("INSERT INTO t VALUES ('x', 'y', 'z')"))

	type Level2 struct {
		C string `db:"c"`
	}
	type Level1 struct {
		B  string `db:"b"`
		L2 Level2
	}
	type Root struct {
		A  string `db:"a"`
		L1 Level1
	}

	var root Root
	if err := conn.Get(&root, "SELECT a, b, c FROM t"); err != nil {
		t.Fatal(err)
	}
	if root.A != "x" {
		t.Errorf("A = %q, want x", root.A)
	}
	if root.L1.B != "y" {
		t.Errorf("L1.B = %q, want y", root.L1.B)
	}
	if root.L1.L2.C != "z" {
		t.Errorf("L1.L2.C = %q, want z", root.L1.L2.C)
	}
}

func TestScanWithDbTag(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (user_id INTEGER, full_name TEXT)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (1, 'Alice Smith')"))

	type Row struct {
		UserID   int64  `db:"user_id"`
		FullName string `db:"full_name"`
	}
	var row Row
	if err := conn.Get(&row, "SELECT user_id, full_name FROM t"); err != nil {
		t.Fatal(err)
	}
	if row.UserID != 1 || row.FullName != "Alice Smith" {
		t.Errorf("got %+v, want {1 Alice Smith}", row)
	}
}

func TestScanIgnoresUnmatchedColumns(t *testing.T) {
	// If the query returns columns that don't match any struct field,
	// they should be silently ignored (no error).
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER, name TEXT, extra TEXT)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (1, 'alice', 'ignored')"))

	type Row struct {
		ID   int64  `db:"id"`
		Name string `db:"name"`
		// no field for "extra"
	}
	var row Row
	if err := conn.Get(&row, "SELECT id, name, extra FROM t"); err != nil {
		t.Fatal(err)
	}
	if row.ID != 1 || row.Name != "alice" {
		t.Errorf("got %+v, want {1 alice}", row)
	}
}

// ---------------------------------------------------------------------------
// SetTimeSetter — custom time parser
// ---------------------------------------------------------------------------

func TestSetTimeSetter(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (created_at TEXT)"))
	mustRes(conn.Exec("INSERT INTO t VALUES ('15/06/2024')"))

	// Set a custom time setter that handles DD/MM/YYYY format.
	SetTimeSetter(func(val any) (time.Time, bool) {
		if s, ok := val.(string); ok {
			tm, err := time.Parse("02/01/2006", s)
			if err == nil {
				return tm, true
			}
		}
		// Fall back to the default for other types.
		return setTimeFromValue(val)
	})
	defer SetTimeSetter(setTimeFromValue) // restore default

	type Row struct{ CreatedAt time.Time `db:"created_at"` }
	var row Row
	if err := conn.Get(&row, "SELECT created_at FROM t"); err != nil {
		t.Fatal(err)
	}
	want := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	if !row.CreatedAt.Equal(want) {
		t.Errorf("got %v, want %v", row.CreatedAt, want)
	}
}

// ---------------------------------------------------------------------------
// SetSupportedTimeFormats — custom format list
// ---------------------------------------------------------------------------

func TestSetSupportedTimeFormats(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (created_at TEXT)"))
	mustRes(conn.Exec("INSERT INTO t VALUES ('2024/06/15 12:30')"))

	// The default formats won't parse this. Add a custom one.
	SetSupportedTimeFormats([]string{
		"2006/01/02 15:04",
		time.RFC3339,
	})
	defer SetSupportedTimeFormats([]string{
		time.RFC3339,
		"2006-01-02 15:04:05",
	}) // restore defaults

	type Row struct{ CreatedAt time.Time `db:"created_at"` }
	var row Row
	if err := conn.Get(&row, "SELECT created_at FROM t"); err != nil {
		t.Fatal(err)
	}
	want := time.Date(2024, 6, 15, 12, 30, 0, 0, time.UTC)
	if !row.CreatedAt.Equal(want) {
		t.Errorf("got %v, want %v", row.CreatedAt, want)
	}
}

// ---------------------------------------------------------------------------
// *[]byte (pointer-to-byte-slice) scanning
// ---------------------------------------------------------------------------

func TestScanPointerByteSlice(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER, data BLOB)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (1, x'DEADBEEF')"))
	mustRes(conn.Exec("INSERT INTO t VALUES (2, NULL)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (3, x'')")) // empty blob

	type Row struct {
		ID   int64   `db:"id"`
		Data *[]byte `db:"data"`
	}

	// Non-NULL blob.
	var row1 Row
	if err := conn.Get(&row1, "SELECT id, data FROM t WHERE id = 1"); err != nil {
		t.Fatal(err)
	}
	if row1.Data == nil {
		t.Fatal("row 1: Data is nil, want *[]byte{0xDE,0xAD,0xBE,0xEF}")
	}
	if len(*row1.Data) != 4 || (*row1.Data)[0] != 0xDE || (*row1.Data)[3] != 0xEF {
		t.Errorf("row 1: Data = %x, want DEADBEEF", *row1.Data)
	}

	// NULL blob — pointer should be nil.
	var row2 Row
	if err := conn.Get(&row2, "SELECT id, data FROM t WHERE id = 2"); err != nil {
		t.Fatal(err)
	}
	if row2.Data != nil {
		t.Errorf("row 2: Data = %v, want nil for NULL blob", row2.Data)
	}

	// Empty blob — pointer should be non-nil, pointing to empty slice.
	var row3 Row
	if err := conn.Get(&row3, "SELECT id, data FROM t WHERE id = 3"); err != nil {
		t.Fatal(err)
	}
	if row3.Data == nil {
		t.Fatal("row 3: Data is nil, want *[]byte{} (empty blob)")
	}
	if len(*row3.Data) != 0 {
		t.Errorf("row 3: Data = %x, want empty slice", *row3.Data)
	}
}

func TestScanPointerString(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER, name TEXT)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (1, 'alice')"))
	mustRes(conn.Exec("INSERT INTO t VALUES (2, NULL)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (3, '')"))

	type Row struct {
		ID   int64   `db:"id"`
		Name *string `db:"name"`
	}
	var rows []Row
	if err := conn.Select(&rows, "SELECT id, name FROM t ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(rows))
	}

	// Non-NULL string.
	if rows[0].Name == nil || *rows[0].Name != "alice" {
		t.Errorf("row 0: Name = %v, want *alice", rows[0].Name)
	}
	// NULL → nil pointer.
	if rows[1].Name != nil {
		t.Errorf("row 1: Name = %v, want nil", rows[1].Name)
	}
	// Empty string → non-nil pointer to "".
	if rows[2].Name == nil || *rows[2].Name != "" {
		t.Errorf("row 2: Name = %v, want *\"\"", rows[2].Name)
	}
}

func TestScanPointerInt64(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER, val INTEGER)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (1, 42)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (2, NULL)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (3, 0)"))

	type Row struct {
		ID  int64  `db:"id"`
		Val *int64 `db:"val"`
	}
	var rows []Row
	if err := conn.Select(&rows, "SELECT id, val FROM t ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(rows))
	}

	if rows[0].Val == nil || *rows[0].Val != 42 {
		t.Errorf("row 0: Val = %v, want *42", rows[0].Val)
	}
	if rows[1].Val != nil {
		t.Errorf("row 1: Val = %v, want nil", rows[1].Val)
	}
	// 0 is a valid value, not NULL.
	if rows[2].Val == nil || *rows[2].Val != 0 {
		t.Errorf("row 2: Val = %v, want *0", rows[2].Val)
	}
}

func TestScanPointerInt(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER, val INTEGER)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (1, 99)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (2, NULL)"))

	type Row struct {
		ID  int64 `db:"id"`
		Val *int  `db:"val"`
	}
	var rows []Row
	if err := conn.Select(&rows, "SELECT id, val FROM t ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	if rows[0].Val == nil || *rows[0].Val != 99 {
		t.Errorf("row 0: Val = %v, want *99", rows[0].Val)
	}
	if rows[1].Val != nil {
		t.Errorf("row 1: Val = %v, want nil", rows[1].Val)
	}
}

func TestScanPointerFloat64(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER, val REAL)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (1, 3.14)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (2, NULL)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (3, 0.0)"))

	type Row struct {
		ID  int64    `db:"id"`
		Val *float64 `db:"val"`
	}
	var rows []Row
	if err := conn.Select(&rows, "SELECT id, val FROM t ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	if rows[0].Val == nil || *rows[0].Val != 3.14 {
		t.Errorf("row 0: Val = %v, want *3.14", rows[0].Val)
	}
	if rows[1].Val != nil {
		t.Errorf("row 1: Val = %v, want nil", rows[1].Val)
	}
	if rows[2].Val == nil || *rows[2].Val != 0.0 {
		t.Errorf("row 2: Val = %v, want *0.0", rows[2].Val)
	}
}

func TestScanPointerBool(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER, active INTEGER)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (1, 1)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (2, NULL)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (3, 0)"))

	type Row struct {
		ID     int64 `db:"id"`
		Active *bool `db:"active"`
	}
	var rows []Row
	if err := conn.Select(&rows, "SELECT id, active FROM t ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	if rows[0].Active == nil || *rows[0].Active != true {
		t.Errorf("row 0: Active = %v, want *true", rows[0].Active)
	}
	if rows[1].Active != nil {
		t.Errorf("row 1: Active = %v, want nil", rows[1].Active)
	}
	// false is a valid value, not NULL.
	if rows[2].Active == nil || *rows[2].Active != false {
		t.Errorf("row 2: Active = %v, want *false", rows[2].Active)
	}
}

// TestScanPointerAllTypes verifies all pointer types together in one struct,
// covering the common "nullable row from a LEFT JOIN" pattern.
func TestScanPointerAllTypes(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec(`CREATE TABLE t (
		id     INTEGER PRIMARY KEY,
		name   TEXT,
		count  INTEGER,
		score  REAL,
		active INTEGER,
		data   BLOB
	)`))
	mustRes(conn.Exec("INSERT INTO t VALUES (1, 'alice', 10, 3.14, 1, x'CAFE')"))
	mustRes(conn.Exec("INSERT INTO t VALUES (2, NULL, NULL, NULL, NULL, NULL)"))

	type Row struct {
		ID     int64    `db:"id"`
		Name   *string  `db:"name"`
		Count  *int64   `db:"count"`
		Score  *float64 `db:"score"`
		Active *bool    `db:"active"`
		Data   *[]byte  `db:"data"`
	}
	var rows []Row
	if err := conn.Select(&rows, "SELECT id, name, count, score, active, data FROM t ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}

	// Row 1: all non-NULL.
	r := rows[0]
	if r.Name == nil || *r.Name != "alice" {
		t.Errorf("row 0 Name = %v", r.Name)
	}
	if r.Count == nil || *r.Count != 10 {
		t.Errorf("row 0 Count = %v", r.Count)
	}
	if r.Score == nil || *r.Score != 3.14 {
		t.Errorf("row 0 Score = %v", r.Score)
	}
	if r.Active == nil || *r.Active != true {
		t.Errorf("row 0 Active = %v", r.Active)
	}
	if r.Data == nil || len(*r.Data) != 2 {
		t.Errorf("row 0 Data = %v", r.Data)
	}

	// Row 2: all NULL.
	r = rows[1]
	if r.Name != nil {
		t.Errorf("row 1 Name = %v, want nil", r.Name)
	}
	if r.Count != nil {
		t.Errorf("row 1 Count = %v, want nil", r.Count)
	}
	if r.Score != nil {
		t.Errorf("row 1 Score = %v, want nil", r.Score)
	}
	if r.Active != nil {
		t.Errorf("row 1 Active = %v, want nil", r.Active)
	}
	if r.Data != nil {
		t.Errorf("row 1 Data = %v, want nil", r.Data)
	}
}

func TestScanPointerByteSliceViaSelect(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id INTEGER, data BLOB)"))
	mustRes(conn.Exec("INSERT INTO t VALUES (1, x'CAFE')"))
	mustRes(conn.Exec("INSERT INTO t VALUES (2, NULL)"))

	type Row struct {
		ID   int64   `db:"id"`
		Data *[]byte `db:"data"`
	}

	var rows []Row
	if err := conn.Select(&rows, "SELECT id, data FROM t ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}

	// Row 1: non-NULL blob.
	if rows[0].Data == nil {
		t.Fatal("row 0: Data is nil, want *[]byte")
	}
	if len(*rows[0].Data) != 2 || (*rows[0].Data)[0] != 0xCA || (*rows[0].Data)[1] != 0xFE {
		t.Errorf("row 0: Data = %x, want CAFE", *rows[0].Data)
	}

	// Row 2: NULL blob.
	if rows[1].Data != nil {
		t.Errorf("row 1: Data = %v, want nil", rows[1].Data)
	}
}

// ---------------------------------------------------------------------------
// Mixed types in a single struct
// ---------------------------------------------------------------------------

func TestScanAllTypesInOneStruct(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec(`CREATE TABLE t (
		b INTEGER,
		i INTEGER,
		u INTEGER,
		f REAL,
		s TEXT,
		data BLOB,
		ts TEXT
	)`))
	mustRes(conn.Exec("INSERT INTO t VALUES (1, -42, 255, 3.14, 'hello', x'CAFE', '2024-06-15T12:00:00Z')"))

	type Row struct {
		B    bool      `db:"b"`
		I    int64     `db:"i"`
		U    uint32    `db:"u"`
		F    float64   `db:"f"`
		S    string    `db:"s"`
		Data []byte    `db:"data"`
		Ts   time.Time `db:"ts"`
	}
	var row Row
	if err := conn.Get(&row, "SELECT b, i, u, f, s, data, ts FROM t"); err != nil {
		t.Fatal(err)
	}
	if row.B != true {
		t.Errorf("B = %v, want true", row.B)
	}
	if row.I != -42 {
		t.Errorf("I = %d, want -42", row.I)
	}
	if row.U != 255 {
		t.Errorf("U = %d, want 255", row.U)
	}
	if row.F != 3.14 {
		t.Errorf("F = %f, want 3.14", row.F)
	}
	if row.S != "hello" {
		t.Errorf("S = %q, want hello", row.S)
	}
	if len(row.Data) != 2 || row.Data[0] != 0xCA || row.Data[1] != 0xFE {
		t.Errorf("Data = %x, want CAFE", row.Data)
	}
	want := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	if !row.Ts.Equal(want) {
		t.Errorf("Ts = %v, want %v", row.Ts, want)
	}
}

// ---------------------------------------------------------------------------
// encoding.TextUnmarshaler structs (e.g. guregu null.v4 zero.String)
// ---------------------------------------------------------------------------

// textString mimics guregu null.v4's zero.String: a struct wrapping a
// nullable string that implements encoding.TextUnmarshaler with a pointer
// receiver. It must be scanned as a scalar, not recursed into.
type textString struct {
	Value string
	Valid bool
}

func (s *textString) UnmarshalText(text []byte) error {
	s.Value = string(text)
	s.Valid = s.Value != ""
	return nil
}

func TestScanTextUnmarshalerStruct(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id TEXT, title TEXT, note TEXT)"))
	mustRes(conn.Exec("INSERT INTO t VALUES ('e1', 'OG Test Title', NULL)"))

	type Row struct {
		ID    string     `db:"id"`
		Title textString `db:"title"`
		Note  textString `db:"note"`
	}
	var row Row
	if err := conn.Get(&row, "SELECT id, title, note FROM t WHERE id='e1'"); err != nil {
		t.Fatal(err)
	}
	if row.ID != "e1" {
		t.Errorf("ID = %q, want e1", row.ID)
	}
	if row.Title.Value != "OG Test Title" || !row.Title.Valid {
		t.Errorf("Title = %+v, want {OG Test Title true}", row.Title)
	}
	// NULL column must leave the field at its zero value, not error.
	if row.Note.Value != "" || row.Note.Valid {
		t.Errorf("Note = %+v, want zero value", row.Note)
	}
}

func TestScanTextUnmarshalerStructSelect(t *testing.T) {
	conn, err := NewConn(":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE t (id TEXT, title TEXT)"))
	mustRes(conn.Exec("INSERT INTO t VALUES ('e1', 'one')"))
	mustRes(conn.Exec("INSERT INTO t VALUES ('e2', 'two')"))

	type Row struct {
		ID    string     `db:"id"`
		Title textString `db:"title"`
	}
	var rows []Row
	if err := conn.Select(&rows, "SELECT id, title FROM t ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	if rows[0].Title.Value != "one" || rows[1].Title.Value != "two" {
		t.Errorf("titles = %q, %q; want one, two", rows[0].Title.Value, rows[1].Title.Value)
	}
}
