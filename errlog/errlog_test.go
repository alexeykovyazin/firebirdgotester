package errlog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoggerWritesAllKinds(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sql_errors.log")
	l, err := Open(path, `E:\db\EMPLOYEE.FDB`)
	if err != nil {
		t.Fatal(err)
	}
	l.Log(Entry{WorkerID: 1, Op: "InsertSales", Kind: "expected", Err: fmtErr("CHECK constraint")})
	l.Log(Entry{WorkerID: 2, Op: "BeginTx", Kind: "begin", Err: fmtErr("connection reset")})
	l.Flush()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if !strings.Contains(text, "kind=expected") || !strings.Contains(text, "op=InsertSales") {
		t.Fatalf("missing expected entry: %s", text)
	}
	if !strings.Contains(text, "kind=begin") {
		t.Fatalf("missing begin entry: %s", text)
	}
}

func TestPathFromCSV(t *testing.T) {
	if got := PathFromCSV("results.csv"); got != "results_sql_errors.log" {
		t.Fatalf("got %s", got)
	}
	if got := PathFromCSV(""); got != DefaultFile {
		t.Fatalf("got %s", got)
	}
}

func TestNilLoggerNoPanic(t *testing.T) {
	var l *Logger
	l.Log(Entry{Err: fmtErr("x")})
	l.Flush()
	_ = l.Close()
}

type plainErr string

func (e plainErr) Error() string { return string(e) }
func fmtErr(s string) error      { return plainErr(s) }
