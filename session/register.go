package session

// Out-of-root database registration: sessions created outside the discover
// root (typically by the emul provision job). Split from manager.go per
// IMPROVEMENTS_PLAN.md R8.

import (
	"fmt"
	"path/filepath"

	"fb-loadgen/config"
	"fb-loadgen/discover"
)

// RegisterDatabase adds (or refreshes) a session for an arbitrary database
// path, bypassing the discover root. Used by the emul provision job so a
// freshly provisioned database appears in the fleet without re-scanning.
func (m *Manager) RegisterDatabase(absPath, user, pass string) (Snapshot, error) {
	abs, err := filepath.Abs(absPath)
	if err != nil {
		return Snapshot{}, err
	}
	info := discover.DatabaseInfo{
		Name:    filepath.Base(abs),
		RelPath: filepath.Base(abs),
		AbsPath: abs,
	}

	// read the persisted per-session preferences BEFORE taking the manager
	// lock — LoadUISettings does file I/O and must not run under m.mu
	saved, _, _ := config.LoadUISettings(m.settingsPath)

	m.mu.Lock()
	if m.shared == nil {
		m.mu.Unlock()
		return Snapshot{}, fmt.Errorf("manager not configured")
	}
	host, port := m.shared.Host, m.shared.Port
	if sess, ok := m.sessions[abs]; ok {
		sess.mu.Lock()
		sess.Missing = false
		snap := sess.snapshotLocked()
		sess.mu.Unlock()
		m.mu.Unlock()
		return snap, nil
	}
	sc := DefaultsFromCLI(m.shared, info, host, port)
	sc.User = user
	sc.Pass = pass
	// re-apply saved per-session preferences (phases, connections, ...) but
	// NOT the profile: an emul-registered database runs oltp-emul, period —
	// the schema guard rejects anything else, and a stale write-heavy pref
	// (saved before the database was provisioned) would brick every start
	if len(saved.Sessions) > 0 {
		if p, ok := saved.Sessions[abs]; ok {
			p.Profile = "oltp-emul"
			ApplyPrefs(&sc, p)
		}
	}
	sess := &Session{ID: IDFromAbsPath(abs), Config: sc, Status: StatusIdle}
	m.sessions[abs] = sess
	m.applyEvenConnBudgetLocked()
	// persist out-of-root registrations so they survive restarts;
	// capture everything needed while the lock is held (ConnectionSettings
	// and SettingsPath take m.mu themselves — calling them here deadlocks).
	foundRef := false
	for i, ref := range m.emulSessions {
		if ref.AbsPath == abs {
			m.emulSessions[i] = config.EmulSessionRef{AbsPath: abs, DSN: sc.DSN, User: user, Pass: pass}
			foundRef = true
			break
		}
	}
	if !foundRef {
		m.emulSessions = append(m.emulSessions, config.EmulSessionRef{AbsPath: abs, DSN: sc.DSN, User: user, Pass: pass})
	}
	st := config.UISettings{
		Version:           config.UISettingsVersion,
		Host:              m.host,
		Port:              m.port,
		User:              m.user,
		Pass:              m.pass,
		DiscoverDir:       m.discoverDir,
		DiscoverMask:      m.discoverMask,
		DiscoverRecursive: m.recursive,
		MaxTotalConns:     m.maxTotal,
		EmulSessions:      m.emulSessions,
	}
	path := m.settingsPath
	m.mu.Unlock()
	_ = m.persistSettings(path, st)
	return sess.Snapshot(), nil
}

// RestoreEmulSessions re-registers previously provisioned out-of-root
// databases. Called once at UI boot, after the initial discovery.
func (m *Manager) RestoreEmulSessions(refs []config.EmulSessionRef) {
	for _, ref := range refs {
		if _, err := m.RegisterDatabase(ref.AbsPath, ref.User, ref.Pass); err != nil {
			logf("[emul] restore session %s: %v", ref.AbsPath, err)
		}
	}
}

// GetConnectionInfo returns the raw connection parameters of a session
// (used by emul endpoints to inspect that database's business_ops registry).
func (m *Manager) GetConnectionInfo(id string) (SessionConfig, error) {
	s, err := m.findByID(id)
	if err != nil {
		return SessionConfig{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Config, nil
}
