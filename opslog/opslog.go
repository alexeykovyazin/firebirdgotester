// Package opslog writes the operations/transactions execution log introduced
// by the extended load mix. The default format mimics the output of the
// Firebird replication-journal printer (fb_repl_print): one line per event,
// every line carrying its transaction number in brackets, so repl-log reading
// habits (grep "^\[\d+\] START", START-without-terminal scans) keep working:
//
//	[37614237] START (offset: 64)
//	[37614237] COMMIT (offset: 288)
//
// Deliberate extensions over the replication format are documented in
// EXTENDED_LOAD_PLAN.md §7.2: ts / dur_ms / params / who / volumes, the
// COMMIT RETAINING / ROLLBACK RETAINING keywords, extended EXECUTE SQL
// (all statements, not only DDL) and the empty-brackets resolution records.
package opslog

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Format selects the on-disk representation.
type Format string

const (
	// FormatReplPrint is the fb_repl_print-style text format (default).
	FormatReplPrint Format = "repl-print"
	// FormatTSV is a machine-readable tab separated variant with the same facts.
	FormatTSV Format = "tsv"
)

// Level gates event volume (config opsLog.level).
type Level string

const (
	// LevelAll logs every statement and transaction event (default).
	LevelAll Level = "all"
	// LevelPeriodic logs only the periodic sidecar rounds (heavy SELECT,
	// bulk DML, DDL churn) and their transactions.
	LevelPeriodic Level = "periodic"
	// LevelErrors logs only failed statements/transactions.
	LevelErrors Level = "errors"
)

const (
	defaultMaxSizeMB   = 50
	defaultKeepArchive = 3
	baseName           = "ops.log"
	flushThreshold     = 32 * 1024
)

// Options configures Open.
type Options struct {
	Format        Format
	Level         Level
	MaxSizeMB     int
	KeepArchives  int
	RotateOnStart bool // shift the previous chain aside when opening (restart rename)
	RunID         string
	DumpRecords   bool // -R style JSON record dumps for bulk rounds
}

// Logger is a rotating, concurrency-safe operations log.
type Logger struct {
	mu        sync.Mutex
	f         *os.File
	buf       []byte
	path      string
	opts      Options
	seq       int   // rotation index of the current file (0 = live file)
	size      int64 // current file size
	offset    int64 // cumulative bytes written into the current file (informational offsets)
	unflushed int64
	written   map[int64]bool // pending tx starts (pairing check is done on Close over the whole chain)
	closed    bool

	txCounter atomic.Int64 // tool-wide transaction number source
}

// NextTx returns the next tool-wide transaction number.
func (l *Logger) NextTx() int64 {
	return l.txCounter.Add(1)
}

// Open creates/opens the operations log at path (typically .../ops.log).
func Open(path string, opts Options) (*Logger, error) {
	if opts.Format == "" {
		opts.Format = FormatReplPrint
	}
	if opts.Level == "" {
		opts.Level = LevelAll
	}
	if opts.MaxSizeMB <= 0 {
		opts.MaxSizeMB = defaultMaxSizeMB
	}
	if opts.KeepArchives <= 0 {
		opts.KeepArchives = defaultKeepArchive
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	l := &Logger{path: path, opts: opts, written: map[int64]bool{}}

	if opts.RotateOnStart {
		if fi, err := os.Stat(path); err == nil && fi.Size() > 0 {
			// Restart rename: shift the previous chain aside before opening.
			if err := shiftChain(path, opts.KeepArchives); err != nil {
				return nil, err
			}
		}
	}
	if err := l.openFile(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *Logger) openFile() error {
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	fi, _ := f.Stat()
	l.f = f
	l.size = 0
	if fi != nil {
		l.size = fi.Size()
	}
	l.offset = l.size
	l.seq = 0
	l.writeHeader()
	return nil
}

// writeHeader mirrors the fb_repl_print -H segment header (fixed-width Length
// slot so it can be finalized in place on Close).
func (l *Logger) writeHeader() {
	if l.opts.Format == FormatTSV {
		l.appendLine(fmt.Sprintf("# ops log opened %s\trun=%s\tformat=tsv\tlevel=%s",
			time.Now().Format(time.RFC3339), l.opts.RunID, l.opts.Level))
		return
	}
	l.buf = append(l.buf, strings.Repeat("=", 80)...)
	l.buf = append(l.buf, '\n')
	l.appendLine(fmt.Sprintf("Segment: %s", filepath.Base(l.path)))
	l.appendLine("Version: 1")
	l.appendLine(fmt.Sprintf("Guid: %s", l.opts.RunID))
	l.appendLine(fmt.Sprintf("Sequence: %d", l.seq))
	l.appendLine("State: free")
	l.appendLine(fmt.Sprintf("Length: %13d", 0))
	l.appendLine("SegmentHeaderSize: 48")
	l.buf = append(l.buf, strings.Repeat("=", 80)...)
	l.buf = append(l.buf, '\n')
}

// appendLine appends one raw line to the buffer, tracking its byte offset.
func (l *Logger) appendLine(s string) {
	l.buf = append(l.buf, s...)
	l.buf = append(l.buf, '\n')
}

// emit writes one event line under the lock. events on a nil logger are no-ops.
func (l *Logger) emit(format string, args ...any) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.f == nil {
		return
	}
	line := fmt.Sprintf(format, args...)
	l.appendLine(line)
	l.maybeRotate(len(line) + 1)
	l.maybeFlush()
}

// emitRaw is emit without the trailing newline handling for multi-part lines.
func (l *Logger) now() string { return time.Now().Format(time.RFC3339Nano) }

// esc normalizes statement text to a single line so the [txN] line grammar
// survives arbitrary SQL.
func esc(s string) string {
	if !strings.ContainsAny(s, "\\\n\r\t") {
		return s
	}
	r := strings.NewReplacer("\\", "\\\\", "\n", "\\n", "\r", "\\r", "\t", "\\t")
	return r.Replace(s)
}

// TxStartRound logs a periodic sidecar round transaction start (heavy SELECT,
// bulk DML, DDL churn). Visible at levels all and periodic.
func (l *Logger) TxStartRound(txn int64, who, params string) {
	if l == nil || !l.enabled(LevelPeriodic) {
		return
	}
	l.txStart(txn, who, params)
}

// TxStart logs a unit transaction start. Visible only at level all.
func (l *Logger) TxStart(txn int64, who, params string) {
	if l == nil || !l.enabled(LevelAll) {
		return
	}
	l.txStart(txn, who, params)
}

func (l *Logger) txStart(txn int64, who, params string) {
	switch l.opts.Format {
	case FormatTSV:
		l.emit("%s	%d	START	who=%s	params=%s", l.now(), txn, esc(who), esc(params))
	default:
		l.mu.Lock()
		l.written[txn] = true
		l.mu.Unlock()
		l.emit("[%d] START (offset: %d, ts: %s, who: %s, params: \"%s\")",
			txn, l.pendingOffset(), time.Now().Format(time.RFC3339), esc(who), esc(params))
	}
}

// RoundCommit / RoundRollback / RoundRetaining / RoundPrepare log the
// terminal events of periodic sidecar rounds (levels all + periodic).
func (l *Logger) RoundCommit(txn int64, ins, upd, del int64, dur time.Duration, failed bool) {
	if l == nil || !l.enabled(LevelPeriodic) {
		return
	}
	l.terminal(txn, "COMMIT", ins, upd, del, dur, failed)
}

func (l *Logger) RoundRollback(txn int64, method string, ins, upd, del int64, dur time.Duration, failed bool) {
	if l == nil || !l.enabled(LevelPeriodic) {
		return
	}
	name := "ROLLBACK"
	if method != "" {
		name = "ROLLBACK (" + esc(method) + ")"
	}
	l.terminalNamed(txn, name, ins, upd, del, dur, failed)
}

func (l *Logger) RoundCommitRetaining(txn int64, ins, upd, del int64, dur time.Duration, failed bool) {
	if l == nil || !l.enabled(LevelPeriodic) {
		return
	}
	l.terminal(txn, "COMMIT RETAINING", ins, upd, del, dur, failed)
}

func (l *Logger) RoundRollbackRetaining(txn int64, ins, upd, del int64, dur time.Duration, failed bool) {
	if l == nil || !l.enabled(LevelPeriodic) {
		return
	}
	l.terminal(txn, "ROLLBACK RETAINING", ins, upd, del, dur, failed)
}

// Stmt logs a statement inside a transaction (EXECUTE SQL extended to all SQL).
func (l *Logger) Stmt(txn int64, sqlText string, rows int64, dur time.Duration, failed bool) {
	if l == nil || !l.enabledFor(failed) {
		return
	}
	switch l.opts.Format {
	case FormatTSV:
		l.emit("%s\t%d\tEXECUTE SQL\t%s\trows=%d\tdur_ms=%.1f\tfailed=%v",
			l.now(), txn, esc(sqlText), rows, ms(dur), failed)
	default:
		l.emit("[%d] EXECUTE SQL %s (offset: %d, rows: %d, dur_ms: %.1f)",
			txn, esc(sqlText), l.pendingOffset(), rows, ms(dur))
	}
}

// TableOp logs a bulk record-level operation (INSERT/UPDATE/DELETE table).
func (l *Logger) TableOp(txn int64, verb, table string, rows int64, dur time.Duration, failed bool) {
	if l == nil || !l.enabledFor(failed) {
		return
	}
	switch l.opts.Format {
	case FormatTSV:
		l.emit("%s\t%d\t%s\t%s\trows=%d\tdur_ms=%.1f", l.now(), txn, verb, table, rows, ms(dur))
	default:
		l.emit("[%d] %s %s (offset: %d, rows: %d, dur_ms: %.1f)",
			txn, verb, table, l.pendingOffset(), rows, ms(dur))
	}
}

// Savepoint logs SAVE / UNDO / RELEASE.
func (l *Logger) Savepoint(txn int64, action, name string) {
	if l == nil || !l.enabled(LevelAll) {
		return
	}
	switch l.opts.Format {
	case FormatTSV:
		l.emit("%s\t%d\t%s\tname=%s", l.now(), txn, action, esc(name))
	default:
		l.emit("[%d] %s (offset: %d, name: %s)", txn, action, l.pendingOffset(), esc(name))
	}
}

// DDL logs a DDL statement (EXECUTE SQL, matching repl semantics).
func (l *Logger) DDL(txn int64, sqlText string, dur time.Duration, failed bool) {
	if l == nil || !l.enabledFor(failed) {
		return
	}
	switch l.opts.Format {
	case FormatTSV:
		l.emit("%s\t%d\tEXECUTE SQL\t%s\tdur_ms=%.1f\tfailed=%v", l.now(), txn, esc(sqlText), ms(dur), failed)
	default:
		l.emit("[%d] EXECUTE SQL %s (offset: %d, dur_ms: %.1f)", txn, esc(sqlText), l.pendingOffset(), ms(dur))
	}
}

// Commit logs the terminal event for a committed transaction (2PC included:
// log Prepare first, then Commit). ins/upd/del is the change volume.
func (l *Logger) Commit(txn int64, ins, upd, del int64, dur time.Duration, failed bool) {
	l.terminal(txn, "COMMIT", ins, upd, del, dur, failed)
}

// Rollback logs a plain rollback terminal event. method is an optional
// qualifier (e.g. "abandoned" at shutdown, "rollback_ddl").
func (l *Logger) Rollback(txn int64, method string, ins, upd, del int64, dur time.Duration, failed bool) {
	name := "ROLLBACK"
	if method != "" {
		name = "ROLLBACK (" + esc(method) + ")"
	}
	l.terminalNamed(txn, name, ins, upd, del, dur, failed)
}

// CommitRetaining logs COMMIT RETAINING (extension keyword).
func (l *Logger) CommitRetaining(txn int64, ins, upd, del int64, dur time.Duration, failed bool) {
	l.terminal(txn, "COMMIT RETAINING", ins, upd, del, dur, failed)
}

// RollbackRetaining logs ROLLBACK RETAINING (extension keyword).
func (l *Logger) RollbackRetaining(txn int64, ins, upd, del int64, dur time.Duration, failed bool) {
	l.terminal(txn, "ROLLBACK RETAINING", ins, upd, del, dur, failed)
}

// Prepare logs the two-phase prepare (start of a 2PC commit, or the limbo marker).
func (l *Logger) Prepare(txn int64, dur time.Duration, failed bool) {
	if l == nil || !l.enabledFor(failed) {
		return
	}
	switch l.opts.Format {
	case FormatTSV:
		l.emit("%s\t%d\tPREPARE\tdur_ms=%.1f", l.now(), txn, ms(dur))
	default:
		l.emit("[%d] PREPARE (offset: %d, dur_ms: %.1f)", txn, l.pendingOffset(), ms(dur))
	}
}

// Resolved logs a limbo resolution outside transaction control
// (empty brackets, mirroring repl's outside-tx actions).
func (l *Logger) Resolved(method string, trn int64, gap time.Duration) {
	if l == nil {
		return
	}
	switch l.opts.Format {
	case FormatTSV:
		l.emit("%s\t0\tRESOLVED\tmethod=%s\ttrn=%d\tgap_sec=%.1f", l.now(), esc(method), trn, gap.Seconds())
	default:
		l.emit("[] RESOLVED %s trn=%d (gap_sec: %.1f)", esc(method), trn, gap.Seconds())
	}
}

// Dropped logs a planned hard connection drop outside transaction control.
func (l *Logger) Dropped(trn int64) {
	if l == nil {
		return
	}
	switch l.opts.Format {
	case FormatTSV:
		l.emit("%s\t0\tDROPPED\ttrn=%d", l.now(), trn)
	default:
		l.emit("[] DROPPED trn=%d (socket closed without rollback)", trn)
	}
}

// RecordDump writes an -R style JSON data dump for bulk rounds.
func (l *Logger) RecordDump(txn int64, payload string) {
	if l == nil || !l.opts.DumpRecords {
		return
	}
	switch l.opts.Format {
	case FormatTSV:
		l.emit("%s\t%d\tRECORD\t%s", l.now(), txn, esc(payload))
	default:
		l.emit("[%d] RECORD %s", txn, esc(payload))
	}
}

// Block marks a flush boundary with real byte offsets (BLOCK (offset: B, length: L)).
func (l *Logger) Block() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.f == nil || l.unflushed == 0 {
		return
	}
	if l.opts.Format == FormatReplPrint {
		l.appendLine(fmt.Sprintf("BLOCK (offset: %d, length: %d)", l.offset+int64(len(l.buf)), l.unflushed))
	}
	l.flushLocked()
}

func (l *Logger) terminal(txn int64, event string, ins, upd, del int64, dur time.Duration, failed bool) {
	l.terminalNamed(txn, event, ins, upd, del, dur, failed)
}

func (l *Logger) terminalNamed(txn int64, event string, ins, upd, del int64, dur time.Duration, failed bool) {
	if l == nil || !l.enabledFor(failed) {
		return
	}
	switch l.opts.Format {
	case FormatTSV:
		l.emit("%s\t%d\t%s\tins=%d\tupd=%d\tdel=%d\tdur_ms=%.1f", l.now(), txn, event, ins, upd, del, ms(dur))
	default:
		l.emit("[%d] %s (offset: %d, ins: %d, upd: %d, del: %d, dur_ms: %.1f)",
			txn, event, l.pendingOffset(), ins, upd, del, ms(dur))
	}
	l.mu.Lock()
	delete(l.written, txn)
	l.mu.Unlock()
}

// pendingOffset returns the file offset the next buffered line will land at
// (informational, mirrors the repl journal's per-event offsets).
func (l *Logger) pendingOffset() int64 {
	return l.offset + int64(len(l.buf))
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }

// enabled reports whether an event of the given scope passes the gate.
// LevelAll: everything; LevelPeriodic: only periodic sidecar rounds
// (heavy SELECT / bulk DML / DDL churn) plus failures; LevelErrors: failures only.
func (l *Logger) enabled(scope Level) bool {
	if l == nil {
		return false
	}
	switch l.opts.Level {
	case LevelErrors:
		return false
	case LevelPeriodic:
		return scope == LevelPeriodic
	default:
		return true
	}
}

// enabledFor is the failed-event gate: failures always pass.
func (l *Logger) enabledFor(failed bool) bool {
	if l == nil {
		return false
	}
	if failed {
		return true
	}
	return l.enabled(LevelAll)
}

// maybeFlush flushes when the buffer exceeds the threshold.
func (l *Logger) maybeFlush() {
	if len(l.buf) >= flushThreshold {
		l.flushLocked()
	}
}

func (l *Logger) flushLocked() {
	if l.f == nil || len(l.buf) == 0 {
		return
	}
	n, err := l.f.Write(l.buf)
	l.offset += int64(len(l.buf))
	l.size += int64(len(l.buf))
	l.unflushed = 0
	l.buf = l.buf[:0]
	if err != nil {
		return
	}
	_ = n
}

// maybeRotate rotates when the current file exceeds MaxSizeMB.
func (l *Logger) maybeRotate(incoming int) {
	if l.size+int64(len(l.buf))+int64(incoming) <= int64(l.opts.MaxSizeMB)*1024*1024 {
		return
	}
	l.flushLocked()
	if l.f != nil {
		_ = l.f.Close()
		l.f = nil
	}
	if err := shiftChain(l.path, l.opts.KeepArchives); err != nil {
		return
	}
	_ = l.openFile()
}

// shiftChain rotates ops.log -> ops.1.log -> ... deleting beyond keep.
// Windows-safe: the caller has already closed the handle.
func shiftChain(path string, keep int) error {
	for i := keep; i >= 1; i-- {
		older := chainName(path, i)
		newer := chainName(path, i-1) // i-1 == 0 is the live file
		if i == keep {
			_ = os.Remove(older)
		}
		if _, err := os.Stat(newer); err == nil {
			if err := os.Rename(newer, older); err != nil {
				return err
			}
		}
	}
	return nil
}

func chainName(path string, i int) string {
	if i == 0 {
		return path
	}
	return fmt.Sprintf("%s.%d%s", strings.TrimSuffix(path, ".log"), i, ".log")
}

// ChainFiles lists the rotated chain newest-archive-first followed by the
// live file (live last, matching repl segment order).
func ChainFiles(path string, keep int) []string {
	var out []string
	for i := keep; i >= 1; i-- {
		p := chainName(path, i)
		if _, err := os.Stat(p); err == nil {
			out = append(out, p)
		}
	}
	if _, err := os.Stat(path); err == nil {
		out = append(out, path)
	}
	return out
}

// Close flushes, runs the START/terminal pairing check over the whole rotated
// chain (R18) — appending abandoned records — finalizes the header Length and
// closes the file.
func (l *Logger) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.flushLocked()
	l.pairingCheckLocked()
	l.finalizeHeader()
	l.closed = true
	if l.f != nil {
		if err := l.f.Close(); err != nil {
			l.f = nil
			return err
		}
		l.f = nil
	}
	return nil
}

// finalizeHeader rewrites the fixed-width Length slot in the -H style header.
func (l *Logger) finalizeHeader() {
	if l.f == nil || l.opts.Format != FormatReplPrint {
		return
	}
	// locate "Length: " within the header block (first 512 bytes)
	head := make([]byte, 512)
	n, _ := l.f.ReadAt(head, 0)
	head = head[:n]
	idx := indexBytes(head, []byte("Length: "))
	if idx < 0 {
		return
	}
	lineEnd := idx
	for lineEnd < len(head) && head[lineEnd] != '\n' {
		lineEnd++
	}
	slot := fmt.Sprintf("Length: %13d", l.size)
	if pad := (lineEnd - idx) - len(slot); pad > 0 {
		slot += strings.Repeat(" ", pad)
	}
	if len(slot) > lineEnd-idx {
		slot = slot[:lineEnd-idx]
	}
	_, _ = l.f.WriteAt([]byte(slot), int64(idx))
}

func indexBytes(b []byte, sub []byte) int {
	for i := 0; i+len(sub) <= len(b); i++ {
		if string(b[i:i+len(sub)]) == string(sub) {
			return i
		}
	}
	return -1
}

// pairingCheckLocked scans the whole rotated chain for START events without a
// terminal event and appends "[N] ROLLBACK (method: abandoned)" records for
// them to the live file. A START and its terminal may straddle a rotation
// boundary, hence the chain-wide scan (R18). Caller holds mu; the records are
// written directly (emit would re-enter the lock).
func (l *Logger) pairingCheckLocked() {
	pending := map[int64]bool{}
	for _, p := range ChainFiles(l.path, l.opts.KeepArchives) {
		data, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(data), "\n") {
			txn, event, ok := parseTxEvent(line)
			if !ok {
				continue
			}
			switch event {
			case "START":
				pending[txn] = true
			case "COMMIT", "ROLLBACK", "COMMIT RETAINING", "ROLLBACK RETAINING", "PREPARE":
				delete(pending, txn)
			}
		}
	}
	if len(pending) == 0 {
		return
	}
	if l.opts.Format == FormatReplPrint {
		for txn := range pending {
			l.appendLine(fmt.Sprintf("[%d] ROLLBACK (method: abandoned)", txn))
		}
		l.flushLocked()
	}
}

// parseTxEvent extracts (txn, event) from a repl-print event line.
func parseTxEvent(line string) (int64, string, bool) {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "[") {
		return 0, "", false
	}
	closeIdx := strings.Index(line, "]")
	if closeIdx < 0 {
		return 0, "", false
	}
	var txn int64
	if _, err := fmt.Sscanf(line[1:closeIdx], "%d", &txn); err != nil {
		return 0, "", false
	}
	rest := strings.TrimSpace(line[closeIdx+1:])
	if rest == "" {
		return txn, "", true
	}
	// event runs to the next " (" or end of line
	event := rest
	if i := strings.Index(rest, " ("); i >= 0 {
		event = rest[:i]
	}
	switch event {
	case "START", "COMMIT", "ROLLBACK", "COMMIT RETAINING", "ROLLBACK RETAINING", "PREPARE":
		return txn, event, true
	}
	return txn, "", false
}
