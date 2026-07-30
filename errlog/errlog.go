package errlog

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"fb-loadgen/ops"
)

const DefaultFile = "sql_errors.log"

// Entry is one SQL/command failure line.
type Entry struct {
	Source   string // database path / DSN label
	WorkerID int
	Op       string // operation or command name
	Kind     string // expected | unexpected | begin | commit | connect | command
	Err      error
}

// Logger appends structured error lines to a file. Safe for concurrent use.
type Logger struct {
	mu     sync.Mutex
	f      *os.File
	bw     *bufWriter
	source string
	path   string
	closed bool
}

type bufWriter struct {
	f   *os.File
	buf []byte
}

func (b *bufWriter) WriteString(s string) (int, error) {
	b.buf = append(b.buf, s...)
	if len(b.buf) >= 32*1024 {
		return b.Flush()
	}
	return len(s), nil
}

func (b *bufWriter) Flush() (int, error) {
	if len(b.buf) == 0 {
		return 0, nil
	}
	n, err := b.f.Write(b.buf)
	b.buf = b.buf[:0]
	return n, err
}

// Open creates or appends an error log at path. source labels lines (abs path / DSN).
func Open(path, source string) (*Logger, error) {
	if path == "" {
		path = DefaultFile
	}
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	l := &Logger{
		f:      f,
		bw:     &bufWriter{f: f, buf: make([]byte, 0, 4096)},
		source: source,
		path:   path,
	}
	_, _ = l.bw.WriteString(fmt.Sprintf("# sql/command error log opened %s source=%s\n",
		time.Now().Format(time.RFC3339), sanitize(source)))
	_, _ = l.bw.Flush()
	return l, nil
}

// Path returns the log file path.
func (l *Logger) Path() string {
	if l == nil {
		return ""
	}
	return l.path
}

// Log writes one error entry. Nil logger or nil error is a no-op.
func (l *Logger) Log(e Entry) {
	if l == nil || e.Err == nil {
		return
	}
	kind := e.Kind
	if kind == "" {
		if ok, _ := ops.ClassifyError(e.Err); ok {
			kind = "expected"
		} else {
			kind = "unexpected"
		}
	}
	code := ops.GetErrorCode(e.Err)
	op := e.Op
	if op == "" {
		op = "-"
	}
	src := e.Source
	if src == "" {
		src = l.source
	}
	line := fmt.Sprintf("%s\tkind=%s\tworker=%d\top=%s\tcode=%s\tsource=%s\tmsg=%s\n",
		time.Now().Format(time.RFC3339Nano),
		kind,
		e.WorkerID,
		sanitize(op),
		sanitize(code),
		sanitize(src),
		sanitizeMsg(e.Err.Error()),
	)

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.bw == nil {
		return
	}
	_, _ = l.bw.WriteString(line)
}

// Flush forces buffered bytes to disk.
func (l *Logger) Flush() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.bw != nil {
		_, _ = l.bw.Flush()
	}
}

// Close flushes and closes the file.
func (l *Logger) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	if l.bw != nil {
		_, _ = l.bw.Flush()
	}
	if l.f != nil {
		return l.f.Close()
	}
	return nil
}

func sanitize(s string) string {
	s = strings.ReplaceAll(s, "\t", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	return s
}

func sanitizeMsg(s string) string {
	return sanitize(s)
}

// PathFromCSV derives a sibling *_sql_errors.log from a CSV/report base path.
func PathFromCSV(csv string) string {
	if csv == "" {
		return DefaultFile
	}
	ext := filepath.Ext(csv)
	base := strings.TrimSuffix(csv, ext)
	return base + "_sql_errors.log"
}
