package session

import (
	"os"
	"path/filepath"
	"testing"

	"fb-loadgen/config"
)

func TestResolveReportFileRejectsTraversal(t *testing.T) {
	m := NewManager(&config.Config{MaxTotalConns: 10, Host: "localhost", Port: 3050, User: "SYSDBA", Pass: "x"})
	dir := t.TempDir()
	reportDir := filepath.Join(dir, "run1")
	if err := os.MkdirAll(reportDir, 0o755); err != nil {
		t.Fatal(err)
	}
	safe := filepath.Join(reportDir, "results_summary.txt")
	if err := os.WriteFile(safe, []byte("ok"), 0o600); err != nil {
		t.Fatal(err)
	}
	abs := filepath.Join(dir, "outside.txt")
	_ = os.WriteFile(abs, []byte("nope"), 0o600)

	s := &Session{
		ID:            "abc",
		Config:        SessionConfig{AbsPath: "x", RelPath: "x"},
		Status:        StatusIdle,
		lastReportDir: reportDir,
	}
	m.sessions["x"] = s

	if _, err := m.ResolveReportFile("abc", "../outside.txt"); err == nil {
		t.Fatal("expected traversal rejection")
	}
	if _, err := m.ResolveReportFile("abc", "results_summary.txt"); err != nil {
		t.Fatalf("expected safe file: %v", err)
	}
}

func TestReserveBudgetConcurrent(t *testing.T) {
	m := NewManager(&config.Config{MaxTotalConns: 20})
	errCh := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() {
			errCh <- m.reserveBudget(5)
		}()
	}
	ok, fail := 0, 0
	for i := 0; i < 8; i++ {
		if err := <-errCh; err != nil {
			fail++
		} else {
			ok++
		}
	}
	if ok != 4 || fail != 4 {
		t.Fatalf("expected 4 ok / 4 fail for budget 20 with chunks of 5, got ok=%d fail=%d reserved=%d", ok, fail, m.reserved.Load())
	}
}
