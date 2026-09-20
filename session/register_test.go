package session

import (
	"path/filepath"
	"testing"

	"fb-loadgen/config"
)

// An emul-registered database must restore with the oltp-emul profile even
// when a stale per-session preference says write-heavy — the schema guard
// rejects every other profile on that database.
func TestRestoreEmulSessionsForcesOltpEmulProfile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ui.json")

	abs := filepath.Join(dir, "oltpemul.fdb")
	ref := config.EmulSessionRef{AbsPath: abs, DSN: "localhost/3055:" + abs, User: "SYSDBA", Pass: "p"}

	// persisted preferences: stale write-heavy profile for this exact path
	st := config.UISettings{
		Version:       config.UISettingsVersion,
		Host:          "localhost",
		Port:          3055,
		User:          "SYSDBA",
		DiscoverDir:   `C:\db`,
		DiscoverMask:  "*.fdb",
		Pass:          "p",
		MaxTotalConns: 200,
		EmulSessions:  []config.EmulSessionRef{ref},
		Sessions: map[string]config.SessionPrefs{
			abs: {Profile: "write-heavy", ConnMin: 1, ConnMax: 2},
		},
	}
	if err := config.SaveUISettings(path, st); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{MaxTotalConns: 200, DiscoverDir: dir, Host: "localhost", Port: 3055, User: "SYSDBA", Pass: "p"}
	m := NewManager(cfg)
	m.SetSettingsPath(path)
	m.RestoreEmulSessions([]config.EmulSessionRef{ref})

	snaps := m.List()
	if len(snaps) != 1 {
		t.Fatalf("expected 1 restored session, got %d", len(snaps))
	}
	if snaps[0].Profile != "oltp-emul" {
		t.Fatalf("restored profile = %q, want oltp-emul", snaps[0].Profile)
	}
}
