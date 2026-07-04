package slite

import (
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/sandro/go-sqlite-lite/sqlite3"
)

// slowQueryThreshold is stored as nanoseconds. 0 means disabled.
var slowQueryThreshold atomic.Int64

// SetSlowQueryThreshold sets the duration above which queries are considered
// slow. When a query exceeds this threshold and a logger is set, slite
// automatically runs EXPLAIN QUERY PLAN and includes the output in the log
// callback. Set to 0 to disable (the default).
func SetSlowQueryThreshold(d time.Duration) {
	slowQueryThreshold.Store(int64(d))
}

// GetSlowQueryThreshold returns the current slow query threshold.
func GetSlowQueryThreshold() time.Duration {
	return time.Duration(slowQueryThreshold.Load())
}

// DefaultLogger is a ready-made Logger that prints interpolated SQL with
// elapsed time to the standard logger. Errors are flagged with ERROR.
//
//	SetDefaultLogger(DefaultLogger)
//
//	// Output:
//	// [slite]  2.3ms SELECT * FROM users WHERE id = 42
//	// [slite] ERROR 0.1ms INSERT INTO bad_table VALUES (1): no such table: bad_table
var DefaultLogger Logger = func(sql string, args []interface{}, elapsed time.Duration, err error) {
	interpolated := InterpolateSQL(sql, args...)
	if err != nil {
		log.Printf("[slite] ERROR %s %s: %v", formatDuration(elapsed), interpolated, err)
	} else {
		log.Printf("[slite] %s %s", formatDuration(elapsed), interpolated)
	}
}

// formatDuration formats elapsed time in a human-readable way:
// microseconds for < 1ms, milliseconds for < 1s, seconds otherwise.
func formatDuration(d time.Duration) string {
	switch {
	case d < time.Millisecond:
		return fmt.Sprintf("%dµs", d.Microseconds())
	case d < time.Second:
		ms := float64(d.Microseconds()) / 1000.0
		return fmt.Sprintf("%.1fms", ms)
	default:
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
}

// InterpolateSQL replaces ? placeholders in sql with the corresponding values
// from args, producing a copy-pasteable SQLite query for debugging. The output
// is intended for human consumption (logging, error messages, pasting into the
// sqlite3 CLI) — never for execution.
//
// Supported types: string, []byte, int/int8/16/32/64, uint and variants,
// float32/float64, bool, time.Time, nil (NULL), and any type implementing
// fmt.Stringer. Unknown types are formatted with %v.
//
//	InterpolateSQL("SELECT * FROM users WHERE name = ? AND age = ?", "bob", 30)
//	// → "SELECT * FROM users WHERE name = 'bob' AND age = 30"
func InterpolateSQL(sql string, args ...interface{}) string {
	if len(args) == 0 {
		return sql
	}
	var b strings.Builder
	b.Grow(len(sql) + len(args)*8)
	arg := 0
	for i := 0; i < len(sql); i++ {
		if sql[i] != '?' || arg >= len(args) {
			b.WriteByte(sql[i])
			continue
		}
		b.WriteString(formatArg(args[arg]))
		arg++
	}
	return b.String()
}

// ExplainPlan runs EXPLAIN QUERY PLAN for the given SQL and returns the
// formatted output. This does not execute the query — it only shows SQLite's
// planned execution strategy. Useful for debugging slow queries.
//
//	plan, err := conn.ExplainPlan("SELECT * FROM users WHERE name = ?", "bob")
//	fmt.Println(plan)
//	// SCAN users
func (o *Conn) ExplainPlan(sql string, args ...interface{}) (string, error) {
	stmt, err := o.db.Prepare("EXPLAIN QUERY PLAN " + sql)
	if err != nil {
		return "", fmt.Errorf("slite: ExplainPlan: %w", err)
	}
	defer stmt.Close()
	if err = stmt.Bind(args...); err != nil {
		return "", fmt.Errorf("slite: ExplainPlan bind: %w", err)
	}
	var b strings.Builder
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return "", fmt.Errorf("slite: ExplainPlan step: %w", err)
		}
		if !hasRow {
			break
		}
		detail := stmtColumnText(stmt, 3) // column 3 is "detail"
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(detail)
	}
	return b.String(), nil
}

// stmtColumnText reads a text column, ignoring errors (best-effort for logging).
func stmtColumnText(stmt *sqlite3.Stmt, col int) string {
	v, _, _ := stmt.ColumnText(col)
	return v
}

// formatArg formats a single bind value as a SQLite literal.
func formatArg(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case string:
		return "'" + strings.ReplaceAll(val, "'", "''") + "'"
	case []byte:
		if val == nil {
			return "NULL"
		}
		return fmt.Sprintf("X'%X'", val)
	case int:
		return fmt.Sprintf("%d", val)
	case int8:
		return fmt.Sprintf("%d", val)
	case int16:
		return fmt.Sprintf("%d", val)
	case int32:
		return fmt.Sprintf("%d", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case uint:
		return fmt.Sprintf("%d", val)
	case uint8:
		return fmt.Sprintf("%d", val)
	case uint16:
		return fmt.Sprintf("%d", val)
	case uint32:
		return fmt.Sprintf("%d", val)
	case uint64:
		return fmt.Sprintf("%d", val)
	case float32:
		return fmt.Sprintf("%g", val)
	case float64:
		return fmt.Sprintf("%g", val)
	case bool:
		if val {
			return "1"
		}
		return "0"
	case time.Time:
		return "'" + val.Format(time.RFC3339) + "'"
	case fmt.Stringer:
		return "'" + strings.ReplaceAll(val.String(), "'", "''") + "'"
	default:
		return fmt.Sprintf("%v", val)
	}
}
