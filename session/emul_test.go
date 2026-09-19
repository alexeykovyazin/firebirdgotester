package session

import (
	"context"
	"testing"

	"fb-loadgen/config"
	"fb-loadgen/emul"
)

// A connection-settings save that carries no emul session registrations
// (as the UI "Save connection" request does) must not wipe the registered
// ones — regression for the save-connection wipe bug.
func TestUpdateConnectionSettingsKeepsEmulSessions(t *testing.T) {
	m := NewManager(&config.Config{MaxTotalConns: 200, DiscoverDir: "C:\\db"})
	ref := config.EmulSessionRef{
		AbsPath: "C:\\db\\oltpemul.fdb",
		DSN:     "localhost/3055:C:\\db\\oltpemul.fdb",
		User:    "SYSDBA",
		Pass:    "masterkey",
	}
	m.emulSessions = []config.EmulSessionRef{ref}

	incoming := config.UISettings{
		Version:       config.UISettingsVersion,
		Host:          "localhost",
		Port:          3055,
		User:          "SYSDBA",
		Pass:          "masterkey",
		DiscoverDir:   "C:\\db",
		DiscoverMask:  "*.fdb",
		MaxTotalConns: 100,
		// EmulSessions deliberately empty — mirrors handleSaveConfig
	}
	if err := m.UpdateConnectionSettings(incoming, false); err != nil {
		t.Fatalf("UpdateConnectionSettings: %v", err)
	}
	if len(m.emulSessions) != 1 || m.emulSessions[0].AbsPath != ref.AbsPath {
		t.Fatalf("emul sessions wiped by settings save: %v", m.emulSessions)
	}
}

// cleanupLocked must stop the emul sidecars on every finish path —
// regression for the natural-completion sidecar leak.
func TestCleanupLockedStopsEmulSidecars(t *testing.T) {
	s := &Session{Config: SessionConfig{Profile: "oltp-emul"}}
	cancelled := false
	s.emulCancel = context.CancelFunc(func() { cancelled = true })
	s.emulState = &emul.EmulState{}
	s.emulUnits = []emul.Unit{{Name: "sp_client_order", Weight: 1}}

	s.cleanupLocked(false)

	if !cancelled {
		t.Error("emulCancel was not called by cleanupLocked")
	}
	if s.emulCancel != nil || s.emulPool != nil {
		t.Error("emul sidecar fields not cleared by cleanupLocked")
	}
	if s.emulState == nil || s.emulUnits == nil {
		t.Error("emul state/units must be retained for Completed snapshots")
	}
}
