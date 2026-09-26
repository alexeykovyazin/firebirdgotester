package session

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"fb-loadgen/opslog"
)

const defaultReportRetention = 10

// sanitizeRelPath turns a relative DB path into a safe directory name.
func sanitizeRelPath(rel string) string {
	rel = strings.ReplaceAll(rel, `\`, `_`)
	rel = strings.ReplaceAll(rel, `/`, `_`)
	rel = strings.ReplaceAll(rel, `:`, `_`)
	rel = strings.ReplaceAll(rel, "..", "_")
	if rel == "" {
		rel = "db"
	}
	return rel
}

func (m *Manager) newReportDir(relPath string) (string, error) {
	base := filepath.Join("reports", sanitizeRelPath(relPath))
	stamp := time.Now().Format("20060102-150405")
	dir := filepath.Join(base, stamp)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	_ = pruneReportDirs(base, defaultReportRetention)
	return dir, nil
}

func pruneReportDirs(base string, keep int) error {
	entries, err := os.ReadDir(base)
	if err != nil {
		return err
	}
	var dirs []os.DirEntry
	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, e)
		}
	}
	sort.Slice(dirs, func(i, j int) bool {
		return dirs[i].Name() > dirs[j].Name()
	})
	for i := keep; i < len(dirs); i++ {
		_ = os.RemoveAll(filepath.Join(base, dirs[i].Name()))
	}
	return nil
}

// ListReports returns filenames in the session's latest report directory.
func (m *Manager) ListReports(id string) (dir string, files []string, err error) {
	s, err := m.findByID(id)
	if err != nil {
		return "", nil, err
	}
	s.mu.Lock()
	dir = s.reportDir
	if dir == "" {
		dir = s.lastReportDir
	}
	s.mu.Unlock()
	if dir == "" {
		return "", nil, fmt.Errorf("no reports for session")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return dir, nil, err
	}
	for _, e := range entries {
		if !e.IsDir() {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	return dir, files, nil
}

// ResolveReportFile returns an absolute path under the session report dir, or error if unsafe.
func (m *Manager) ResolveReportFile(id, name string) (string, error) {
	dir, _, err := m.ListReports(id)
	if err != nil {
		return "", err
	}
	if name == "" || strings.Contains(name, "..") || strings.ContainsAny(name, `/\`) {
		return "", fmt.Errorf("invalid report filename")
	}
	full := filepath.Join(dir, name)
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	absFile, err := filepath.Abs(full)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(absDir, absFile)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("path escapes report directory")
	}
	if _, err := os.Stat(absFile); err != nil {
		return "", err
	}
	return absFile, nil
}

// LogFileInfo describes one file of the ops.log chain.
type LogFileInfo struct {
	Name  string `json:"name"`
	Size  int64  `json:"size"`
	Mtime string `json:"mtime"`
}

// ListLogs returns the ops.log chain (archives newest-first, live last) for
// the session's current or last report directory.
func (m *Manager) ListLogs(id string) (dir string, files []LogFileInfo, err error) {
	s, err := m.findByID(id)
	if err != nil {
		return "", nil, err
	}
	s.mu.Lock()
	dir = s.reportDir
	if dir == "" {
		dir = s.lastReportDir
	}
	s.mu.Unlock()
	if dir == "" {
		return "", nil, fmt.Errorf("no logs for session")
	}
	for _, p := range opslog.ChainFiles(filepath.Join(dir, "ops.log"), 3) {
		fi, err := os.Stat(p)
		if err != nil {
			continue
		}
		files = append(files, LogFileInfo{
			Name:  filepath.Base(p),
			Size:  fi.Size(),
			Mtime: fi.ModTime().Format(time.RFC3339),
		})
	}
	if len(files) == 0 {
		return dir, nil, fmt.Errorf("no ops log files")
	}
	return dir, files, nil
}

// ResolveLogFile resolves a name within the ops.log chain (jail: same report
// dir, ops*.log names only).
func (m *Manager) ResolveLogFile(id, name string) (string, error) {
	if !strings.HasPrefix(name, "ops") || !strings.HasSuffix(name, ".log") {
		return "", fmt.Errorf("invalid log filename")
	}
	return m.ResolveReportFile(id, name)
}
