package slite

import (
	"log"
	"os"
	"strings"
	"testing"
	"time"
)

func TestInterpolateSQL(t *testing.T) {
	tests := []struct {
		sql  string
		args []interface{}
		want string
	}{
		{
			sql:  "SELECT * FROM users WHERE name = ? AND age = ?",
			args: []interface{}{"bob", 30},
			want: "SELECT * FROM users WHERE name = 'bob' AND age = 30",
		},
		{
			sql:  "SELECT * FROM t WHERE id = ?",
			args: []interface{}{int64(42)},
			want: "SELECT * FROM t WHERE id = 42",
		},
		{
			sql:  "SELECT * FROM t WHERE data = ?",
			args: []interface{}{[]byte{0xDE, 0xAD, 0xBE, 0xEF}},
			want: "SELECT * FROM t WHERE data = X'DEADBEEF'",
		},
		{
			sql:  "INSERT INTO t VALUES (?, ?, ?)",
			args: []interface{}{nil, true, false},
			want: "INSERT INTO t VALUES (NULL, 1, 0)",
		},
		{
			sql:  "SELECT * FROM t WHERE name = ?",
			args: []interface{}{"it's a test"},
			want: "SELECT * FROM t WHERE name = 'it''s a test'",
		},
		{
			sql:  "SELECT * FROM t WHERE x = ? AND y = ?",
			args: []interface{}{3.14, float32(2.5)},
			want: "SELECT * FROM t WHERE x = 3.14 AND y = 2.5",
		},
		{
			sql:  "SELECT 1",
			args: nil,
			want: "SELECT 1",
		},
		{
			sql:  "SELECT * FROM t WHERE a = ? AND b = ?",
			args: []interface{}{"only_one"},
			want: "SELECT * FROM t WHERE a = 'only_one' AND b = ?",
		},
		{
			sql:  "SELECT * FROM t WHERE id = ?",
			args: []interface{}{uint64(999)},
			want: "SELECT * FROM t WHERE id = 999",
		},
		{
			sql:  "SELECT * FROM t WHERE id = ?",
			args: []interface{}{[]byte(nil)},
			want: "SELECT * FROM t WHERE id = NULL",
		},
	}

	for i, tt := range tests {
		got := InterpolateSQL(tt.sql, tt.args...)
		if got != tt.want {
			t.Errorf("%d: InterpolateSQL(%q, %v)\n  got:  %s\n  want: %s", i, tt.sql, tt.args, got, tt.want)
		}
	}
}

func TestExplainPlan(t *testing.T) {
	conn, err := NewConn("file::memory:?cache=shared", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE IF NOT EXISTS explain_test (id INTEGER PRIMARY KEY, name TEXT)"))

	plan, err := conn.ExplainPlan("SELECT * FROM explain_test WHERE id = ?", 1)
	if err != nil {
		t.Fatal(err)
	}
	// Should mention the table and likely a search using the primary key.
	if !containsAny(plan, "explain_test") {
		t.Errorf("ExplainPlan output doesn't mention table: %q", plan)
	}
	if plan == "" {
		t.Error("ExplainPlan returned empty string")
	}

	// Full table scan
	plan2, err := conn.ExplainPlan("SELECT * FROM explain_test WHERE name = ?", "bob")
	if err != nil {
		t.Fatal(err)
	}
	if !containsAny(plan2, "SCAN") {
		t.Errorf("expected SCAN in plan for non-indexed column: %q", plan2)
	}
}

func containsAny(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if contains(s, sub) {
			return true
		}
	}
	return false
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestDefaultLogger(t *testing.T) {
	conn, err := NewConn("file::memory:?cache=shared", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Capture log output.
	var buf strings.Builder
	log.SetOutput(&buf)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(os.Stderr)
		log.SetFlags(log.LstdFlags)
	}()

	conn.SetLogger(DefaultLogger)
	mustRes(conn.Exec("CREATE TABLE IF NOT EXISTS deflog_test (id INTEGER, name TEXT)"))
	mustRes(conn.Exec("INSERT INTO deflog_test VALUES (?, ?)", 1, "alice"))

	output := buf.String()
	if !contains(output, "[slite]") {
		t.Errorf("expected [slite] prefix in output: %q", output)
	}
	// Should show interpolated values, not ?.
	if !contains(output, "'alice'") {
		t.Errorf("expected interpolated args in output: %q", output)
	}

	// Error case
	buf.Reset()
	_, _ = conn.Exec("INSERT INTO nonexistent VALUES (1)")
	output = buf.String()
	if !contains(output, "ERROR") {
		t.Errorf("expected ERROR in output for failed query: %q", output)
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		d    time.Duration
		want string
	}{
		{500 * time.Microsecond, "500µs"},
		{1500 * time.Microsecond, "1.5ms"},
		{100 * time.Millisecond, "100.0ms"},
		{2500 * time.Millisecond, "2.50s"},
	}
	for _, tt := range tests {
		got := formatDuration(tt.d)
		if got != tt.want {
			t.Errorf("formatDuration(%v) = %q, want %q", tt.d, got, tt.want)
		}
	}
}

func TestLoggerReceivesError(t *testing.T) {
	conn, err := NewConn("file::memory:?cache=shared", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var lastErr error
	var lastSQL string
	conn.SetLogger(func(sql string, args []interface{}, elapsed time.Duration, err error) {
		lastSQL = sql
		lastErr = err
	})

	// Successful query — err should be nil.
	mustRes(conn.Exec("CREATE TABLE IF NOT EXISTS log_test (id INTEGER)"))
	if lastErr != nil {
		t.Errorf("expected nil error for successful query, got: %v", lastErr)
	}
	if lastSQL == "" {
		t.Error("logger was not called for successful query")
	}

	// Failed query — err should be non-nil.
	lastSQL = ""
	lastErr = nil
	_, _ = conn.Exec("INSERT INTO nonexistent_table VALUES (1)")
	if lastErr == nil {
		t.Error("expected non-nil error for failed query")
	}
}

func TestSlowQueryThreshold(t *testing.T) {
	conn, err := NewConn("file::memory:?cache=shared", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustRes(conn.Exec("CREATE TABLE IF NOT EXISTS slow_test (id INTEGER PRIMARY KEY, name TEXT)"))

	var logged []time.Duration
	var loggedErrs []error
	conn.SetLogger(func(sql string, args []interface{}, elapsed time.Duration, err error) {
		logged = append(logged, elapsed)
		loggedErrs = append(loggedErrs, err)
	})

	// Set threshold to 0 (trigger on every query) to test the mechanism.
	SetSlowQueryThreshold(1 * time.Nanosecond)
	defer SetSlowQueryThreshold(0)

	// Run a query — should trigger slow query logging without panic.
	mustRes(conn.Exec("INSERT INTO slow_test VALUES (1, 'test')"))

	if len(logged) == 0 {
		t.Fatal("logger was not called")
	}

	// Verify threshold getter
	SetSlowQueryThreshold(500 * time.Millisecond)
	if got := GetSlowQueryThreshold(); got != 500*time.Millisecond {
		t.Errorf("GetSlowQueryThreshold() = %v, want 500ms", got)
	}
	SetSlowQueryThreshold(0)
	if got := GetSlowQueryThreshold(); got != 0 {
		t.Errorf("GetSlowQueryThreshold() = %v, want 0", got)
	}
}

func TestInterpolateSQLTime(t *testing.T) {
	tm := time.Date(2024, 6, 15, 12, 30, 0, 0, time.UTC)
	got := InterpolateSQL("SELECT * FROM t WHERE created_at > ?", tm)
	want := "SELECT * FROM t WHERE created_at > '2024-06-15T12:30:00Z'"
	if got != want {
		t.Errorf("got:  %s\nwant: %s", got, want)
	}
}
