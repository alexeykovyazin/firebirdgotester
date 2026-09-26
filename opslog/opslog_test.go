package opslog

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func newTestLogger(t *testing.T, opts Options) (*Logger, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ops.log")
	l, err := Open(path, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l, path
}

func TestReplPrintGolden(t *testing.T) {
	l, path := newTestLogger(t, Options{Format: FormatReplPrint, Level: LevelAll, RunID: "run-1", KeepArchives: 3, MaxSizeMB: 1})

	txn := l.NextTx()
	l.TxStart(txn, "worker-4", "RC/rec_version/wait/RW; lock_timeout=5")
	l.TableOp(txn, "INSERT", "EL_BULK_ITEMS", 500, 8100000, false)
	l.Savepoint(txn, "SAVE", "SP1")
	l.Savepoint(txn, "UNDO", "SP1")
	l.Commit(txn, 2, 0, 3, 12300000, false)

	txn2 := l.NextTx()
	l.TxStart(txn2, "worker-2", "snapshot/wait/RO")
	l.Prepare(txn2, 500000, false)
	l.CommitRetaining(txn2, 0, 1, 0, 400000, false)
	_ = l.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	var events []string
	for _, ln := range lines {
		if strings.HasPrefix(ln, "[") {
			events = append(events, ln)
		}
	}
	want := []string{
		fmt.Sprintf("[%d] START (offset: ", txn),
		fmt.Sprintf("[%d] INSERT EL_BULK_ITEMS (offset: ", txn),
		fmt.Sprintf("[%d] SAVE (offset: ", txn),
		fmt.Sprintf("[%d] UNDO (offset: ", txn),
		fmt.Sprintf("[%d] COMMIT (offset: ", txn),
		fmt.Sprintf("[%d] START (offset: ", txn2),
		fmt.Sprintf("[%d] PREPARE (offset: ", txn2),
		fmt.Sprintf("[%d] COMMIT RETAINING (offset: ", txn2),
	}
	if len(events) != len(want) {
		t.Fatalf("got %d event lines, want %d:\n%s", len(events), len(want), strings.Join(events, "\n"))
	}
	for i, w := range want {
		if !strings.HasPrefix(events[i], w) {
			t.Errorf("line %d:\n got %s\nwant prefix %s", i, events[i], w)
		}
	}
	if !strings.Contains(events[0], "who: worker-4") || !strings.Contains(events[0], "params: \"RC/rec_version/wait/RW; lock_timeout=5\"") {
		t.Errorf("START line missing params/who: %s", events[0])
	}
	if !strings.Contains(events[1], "rows: 500") || !strings.Contains(events[1], "dur_ms: 8.1") {
		t.Errorf("INSERT line wrong: %s", events[1])
	}
	if !strings.Contains(events[4], "ins: 2, upd: 0, del: 3, dur_ms: 12.3") {
		t.Errorf("COMMIT line wrong: %s", events[4])
	}
	if !strings.Contains(events[6], "PREPARE") {
		t.Errorf("PREPARE line wrong: %s", events[6])
	}
	// header block present with the run id
	if !strings.Contains(string(data), "Guid: run-1") || !strings.Contains(string(data), "Segment: ops.log") {
		t.Errorf("header block missing")
	}
}

func TestTSVFactEquivalence(t *testing.T) {
	log1, p1 := newTestLogger(t, Options{Format: FormatReplPrint, Level: LevelAll, KeepArchives: 3, MaxSizeMB: 1})
	txn := log1.NextTx()
	log1.TxStart(txn, "worker-1", "RC/nowait/RW")
	log1.TableOp(txn, "DELETE", "EL_BULK_ITEMS", 120, 300000, false)
	log1.Rollback(txn, "", 0, 0, 5, 900000, false)
	_ = log1.Close()

	log2, p2 := newTestLogger(t, Options{Format: FormatTSV, Level: LevelAll, KeepArchives: 3, MaxSizeMB: 1})
	txn2 := log2.NextTx()
	log2.TxStart(txn2, "worker-1", "RC/nowait/RW")
	log2.TableOp(txn2, "DELETE", "EL_BULK_ITEMS", 120, 300000, false)
	log2.Rollback(txn2, "", 0, 0, 5, 900000, false)
	_ = log2.Close()

	facts := func(lines []string) string {
		var out []string
		for _, ln := range lines {
			ln = strings.TrimSpace(ln)
			if ln == "" || strings.HasPrefix(ln, "#") || strings.HasPrefix(ln, "=") ||
				strings.HasPrefix(ln, "Segment") || strings.HasPrefix(ln, "Version:") ||
				strings.HasPrefix(ln, "Guid:") || strings.HasPrefix(ln, "Sequence:") ||
				strings.HasPrefix(ln, "State:") || strings.HasPrefix(ln, "Length:") {
				continue
			}
			// drop timestamps (leading RFC3339Nano in tsv)
			if strings.HasPrefix(ln, "2") {
				ln = ln[strings.Index(ln, "	")+1:]
			}
			// normalize repl-print grammar to tsv-ish tokens
			ln = strings.ReplaceAll(ln, " (offset: ", "	")
			ln = strings.ReplaceAll(ln, "rows: ", "rows=")
			ln = strings.ReplaceAll(ln, "del: ", "del=")
			ln = strings.ReplaceAll(ln, "params: \"", "params=")
			ln = strings.ReplaceAll(ln, "who: ", "who=")
			for _, part := range strings.Fields(ln) {
				part = strings.TrimRight(part, ",\")")
				switch {
				case part == "START", part == "DELETE", part == "ROLLBACK",
					strings.HasPrefix(part, "who="), strings.HasPrefix(part, "params="),
					strings.Contains(part, "EL_BULK_ITEMS"),
					strings.HasPrefix(part, "rows="), strings.HasPrefix(part, "del="):
					out = append(out, part)
				}
			}
		}
		return strings.Join(out, "|")
	}
	d1, _ := os.ReadFile(p1)
	d2, _ := os.ReadFile(p2)
	f1 := facts(strings.Split(string(d1), "\n"))
	f2 := facts(strings.Split(string(d2), "\n"))
	if f1 != f2 {
		t.Errorf("facts differ:\nrepl-print: %s\ntsv:        %s", f1, f2)
	}
	if !strings.Contains(f1, "EL_BULK_ITEMS") {
		t.Errorf("facts extraction broken: %q", f1)
	}
}

func TestStatementEscaping(t *testing.T) {
	l, path := newTestLogger(t, Options{Format: FormatReplPrint, Level: LevelAll, KeepArchives: 3, MaxSizeMB: 1})
	txn := l.NextTx()
	l.TxStart(txn, "worker-1", "p")
	l.Stmt(txn, "SELECT 1\n  FROM\nRDB$DATABASE\tWHERE x='a\\b'", 1, 100000, false)
	_ = l.Close()
	data, _ := os.ReadFile(path)
	for _, ln := range strings.Split(string(data), "\n") {
		if strings.Contains(ln, "EXECUTE SQL") {
			if strings.Contains(ln, "\n") || strings.Count(ln, "EXECUTE SQL") != 1 {
				t.Fatalf("multi-line statement broke the line grammar: %q", ln)
			}
			if !strings.Contains(ln, `SELECT 1\n  FROM\nRDB$DATABASE\tWHERE x='a\\b'`) {
				t.Fatalf("escaping wrong: %q", ln)
			}
			return
		}
	}
	t.Fatal("EXECUTE SQL line not found")
}

func TestRotationAndRestartRename(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ops.log")
	l, err := Open(path, Options{Format: FormatReplPrint, Level: LevelAll, KeepArchives: 3, MaxSizeMB: 1, RunID: "r"})
	if err != nil {
		t.Fatal(err)
	}
	// write > 1MB of small events to trigger multiple rotations
	for i := 0; i < 70000; i++ {
		l.Resolved("commit", int64(i), 1)
	}
	l.Close()

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("live file missing: %v", err)
	}
	if _, err := os.Stat(strings.TrimSuffix(path, ".log") + ".1.log"); err != nil {
		t.Fatalf("first archive missing: %v", err)
	}
	if _, err := os.Stat(strings.TrimSuffix(path, ".log") + ".2.log"); err != nil {
		t.Fatalf("second archive missing: %v", err)
	}

	// restart rename: reopening with RotateOnStart shifts the chain again
	l2, err := Open(path, Options{Format: FormatReplPrint, Level: LevelAll, KeepArchives: 3, MaxSizeMB: 1, RotateOnStart: true, RunID: "r2"})
	if err != nil {
		t.Fatal(err)
	}
	l2.Close()
	if _, err := os.Stat(strings.TrimSuffix(path, ".log") + ".3.log"); err != nil {
		t.Fatalf("restart rename did not shift the chain: %v", err)
	}
}

func TestPairingCheckAcrossRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ops.log")
	l, err := Open(path, Options{Format: FormatReplPrint, Level: LevelAll, KeepArchives: 3, MaxSizeMB: 1})
	if err != nil {
		t.Fatal(err)
	}
	txn := l.NextTx()
	l.TxStart(txn, "worker-9", "p") // START in the live file...
	// force rotation so the (missing) terminal would land in another segment
	for i := 0; i < 70000; i++ {
		l.Resolved("commit", int64(i), 1)
	}
	txn2 := l.NextTx()
	l.TxStart(txn2, "worker-8", "p")
	l.Commit(txn2, 0, 0, 0, 1, false) // properly paired
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	// Close() appended the abandoned record for txn: scan all chain files
	data := ""
	for _, p := range ChainFiles(path, 3) {
		b, err := os.ReadFile(p)
		if err == nil {
			data += string(b)
		}
	}
	if !strings.Contains(data, fmt.Sprintf("[%d] ROLLBACK (method: abandoned)", txn)) {
		t.Errorf("abandoned record missing for txn %d", txn)
	}
}

func TestConcurrentWriters(t *testing.T) {
	l, path := newTestLogger(t, Options{Format: FormatReplPrint, Level: LevelAll, KeepArchives: 3, MaxSizeMB: 50})
	var wg sync.WaitGroup
	starts := make([]int64, 8)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				txn := l.NextTx()
				if i == 0 {
					starts[id] = txn
				}
				l.TxStart(txn, fmt.Sprintf("worker-%d", id), "p")
				l.Commit(txn, 1, 0, 0, 100, false)
			}
		}(w)
	}
	wg.Wait()
	_ = l.Close()
	data, _ := os.ReadFile(path)
	torn := 0
	for _, ln := range strings.Split(string(data), "\n") {
		if ln != "" && !strings.HasPrefix(ln, "[") && !strings.HasPrefix(ln, "BLOCK") &&
			!strings.HasPrefix(ln, "=") && !strings.HasPrefix(ln, "Segment") &&
			!strings.HasPrefix(ln, "Version") && !strings.HasPrefix(ln, "Guid") &&
			!strings.HasPrefix(ln, "Sequence") && !strings.HasPrefix(ln, "State") &&
			!strings.HasPrefix(ln, "Length") && !strings.HasPrefix(ln, "SegmentHeaderSize") {
			torn++
		}
	}
	if torn != 0 {
		t.Errorf("%d torn/unknown lines under concurrent writers", torn)
	}
	for _, txn := range starts {
		if !strings.Contains(string(data), fmt.Sprintf("[%d] START", txn)) {
			t.Errorf("first START of some worker missing (txn %d)", txn)
		}
	}
}

func TestLevelGate(t *testing.T) {
	// level=periodic: unit events suppressed, round events logged
	l, path := newTestLogger(t, Options{Format: FormatReplPrint, Level: LevelPeriodic, KeepArchives: 3, MaxSizeMB: 1})
	txn := l.NextTx()
	l.TxStart(txn, "worker-1", "p")
	txn2 := l.NextTx()
	l.TxStartRound(txn2, "heavy-sidecar", "p")
	l.RoundCommit(txn2, 0, 0, 0, 1, false)
	_ = l.Close()
	data, _ := os.ReadFile(path)
	if strings.Contains(string(data), "worker-1") {
		t.Errorf("unit event leaked at level=periodic")
	}
	if !strings.Contains(string(data), "heavy-sidecar") {
		t.Errorf("round event missing at level=periodic")
	}

	// failed events always pass even at level=errors
	l2, path2 := newTestLogger(t, Options{Format: FormatReplPrint, Level: LevelErrors, KeepArchives: 3, MaxSizeMB: 1})
	txn3 := l2.NextTx()
	l2.Stmt(txn3, "SELECT 1", 0, 1, true)
	txn4 := l2.NextTx()
	l2.Stmt(txn4, "SELECT 2", 1, 1, false)
	_ = l2.Close()
	d2, _ := os.ReadFile(path2)
	if !strings.Contains(string(d2), "SELECT 1") {
		t.Errorf("failed statement suppressed at level=errors")
	}
	if strings.Contains(string(d2), "SELECT 2") {
		t.Errorf("ok statement leaked at level=errors")
	}
}
