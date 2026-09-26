package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const DefaultUISettingsFile = "fb-loadgen.ui.json"

const UISettingsVersion = 2

// SessionPrefs is persisted per-database run configuration keyed by AbsPath.
// EmulSessionRef persists a session registered outside the discover root
// (typically by an emul provision job) so it survives UI restarts.
type EmulSessionRef struct {
	AbsPath string `json:"absPath"`
	DSN     string `json:"dsn"`
	User    string `json:"user"`
	Pass    string `json:"pass"`
}

type SessionPrefs struct {
	Profile      string       `json:"profile"`
	ExtendedLoad ExtendedLoad `json:"extendedLoad,omitempty"`
	ConnMin      int          `json:"connMin"`
	ConnMax      int          `json:"connMax"`
	Warmup       int          `json:"warmup"`
	Main         int          `json:"main"`
	Cooldown     int          `json:"cooldown"`
	SpikeCycles  int          `json:"spikeCycles"`
	SpikeHold    int          `json:"spikeHold"`
	ThinkMs      int          `json:"thinkMs"`
	TxTimeout    int          `json:"txTimeout"`
}

// UISettings is the persisted Firebird/UI connection configuration.
type UISettings struct {
	Version           int                     `json:"version"`
	Host              string                  `json:"host"`
	Port              int                     `json:"port"`
	User              string                  `json:"user"`
	Pass              string                  `json:"pass"`
	DiscoverDir       string                  `json:"discoverDir"`
	DiscoverMask      string                  `json:"discoverMask"`
	DiscoverRecursive bool                    `json:"discoverRecursive"`
	MaxTotalConns     int                     `json:"maxTotalConns"`
	Sessions          map[string]SessionPrefs `json:"sessions,omitempty"`
	EmulSessions      []EmulSessionRef        `json:"emulSessions,omitempty"`
}

// DefaultUISettings returns built-in defaults (port 3050).
func DefaultUISettings() UISettings {
	return UISettings{
		Version:           UISettingsVersion,
		Host:              "localhost",
		Port:              3050,
		User:              "SYSDBA",
		Pass:              "masterkey",
		DiscoverDir:       ".",
		DiscoverMask:      "*.fdb",
		DiscoverRecursive: true,
		MaxTotalConns:     200,
		Sessions:          map[string]SessionPrefs{},
	}
}

// ApplyTo overlays settings onto a Config (used at UI startup).
func (s UISettings) ApplyTo(cfg *Config) {
	if s.Host != "" {
		cfg.Host = s.Host
	}
	if s.Port > 0 {
		cfg.Port = s.Port
	}
	if s.User != "" {
		cfg.User = s.User
	}
	if s.Pass != "" {
		cfg.Pass = s.Pass
	}
	if s.DiscoverDir != "" {
		cfg.DiscoverDir = s.DiscoverDir
	}
	if s.DiscoverMask != "" {
		cfg.DiscoverMask = s.DiscoverMask
	}
	cfg.DiscoverRecursive = s.DiscoverRecursive
	if s.MaxTotalConns > 0 {
		cfg.MaxTotalConns = s.MaxTotalConns
	}
}

// UISettingsFromConfig builds UISettings from the live config.
func UISettingsFromConfig(cfg *Config) UISettings {
	return UISettings{
		Version:           UISettingsVersion,
		Host:              cfg.Host,
		Port:              cfg.Port,
		User:              cfg.User,
		Pass:              cfg.Pass,
		DiscoverDir:       cfg.DiscoverDir,
		DiscoverMask:      cfg.DiscoverMask,
		DiscoverRecursive: cfg.DiscoverRecursive,
		MaxTotalConns:     cfg.MaxTotalConns,
		Sessions:          map[string]SessionPrefs{},
	}
}

// RedactedForAPI returns a copy safe to send to the browser (no password).
func (s UISettings) RedactedForAPI() map[string]interface{} {
	return map[string]interface{}{
		"version":           s.Version,
		"host":              s.Host,
		"port":              s.Port,
		"user":              s.User,
		"hasPass":           s.Pass != "",
		"pass":              "",
		"discoverDir":       s.DiscoverDir,
		"discoverMask":      s.DiscoverMask,
		"discoverRecursive": s.DiscoverRecursive,
		"maxTotalConns":     s.MaxTotalConns,
	}
}

// MergePassKeepExisting keeps the previous password when the incoming pass is blank.
func (s UISettings) MergePassKeepExisting(prev UISettings) UISettings {
	if s.Pass == "" {
		s.Pass = prev.Pass
	}
	return s
}

// Validate checks required fields.
func (s UISettings) Validate() error {
	if s.Host == "" {
		return fmt.Errorf("host is required")
	}
	if s.Port < 1 {
		return fmt.Errorf("port must be >= 1")
	}
	if s.User == "" {
		return fmt.Errorf("user is required")
	}
	if s.DiscoverDir == "" {
		return fmt.Errorf("discoverDir is required")
	}
	if s.DiscoverMask == "" {
		return fmt.Errorf("discoverMask is required")
	}
	if s.MaxTotalConns < 1 {
		return fmt.Errorf("maxTotalConns must be >= 1")
	}
	return nil
}

// Normalize fills blanks and upgrades missing version to v2.
func (s *UISettings) Normalize() {
	def := DefaultUISettings()
	if s.Version < 1 {
		s.Version = 1
	}
	if s.Host == "" {
		s.Host = def.Host
	}
	if s.Port <= 0 {
		s.Port = def.Port
	}
	if s.User == "" {
		s.User = def.User
	}
	if s.Pass == "" && s.Version < 2 {
		// v1 files always had an explicit pass; blank means default only on migrate
		s.Pass = def.Pass
	}
	if s.DiscoverDir == "" {
		s.DiscoverDir = def.DiscoverDir
	}
	if s.DiscoverMask == "" {
		s.DiscoverMask = def.DiscoverMask
	}
	if s.MaxTotalConns <= 0 {
		s.MaxTotalConns = def.MaxTotalConns
	}
	if s.Sessions == nil {
		s.Sessions = map[string]SessionPrefs{}
	}
	// v1 had no discoverRecursive field; default true
	if s.Version < 2 {
		s.DiscoverRecursive = true
		s.Version = UISettingsVersion
	}
}

// LoadUISettings reads settings from path. Missing file returns defaults + false.
func LoadUISettings(path string) (UISettings, bool, error) {
	def := DefaultUISettings()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return def, false, nil
		}
		return def, false, err
	}
	var s UISettings
	if err := json.Unmarshal(data, &s); err != nil {
		return def, false, fmt.Errorf("parse %s: %w", path, err)
	}
	s.Normalize()
	// Preserve empty pass on v2 loads (user cleared it); only fill from default when
	// file was v1-style with empty after unmarshal of brand-new incomplete file.
	if s.Pass == "" {
		// Keep empty — ApplyTo and MergePassKeepExisting handle reuse.
		// For first-time incomplete files without pass, use default so UI still works.
		var raw map[string]json.RawMessage
		_ = json.Unmarshal(data, &raw)
		if _, ok := raw["pass"]; !ok {
			s.Pass = def.Pass
		}
	}
	return s, true, nil
}

// SaveUISettings writes settings atomically.
func SaveUISettings(path string, s UISettings) error {
	s.Version = UISettingsVersion
	if s.Sessions == nil {
		s.Sessions = map[string]SessionPrefs{}
	}
	if err := s.Validate(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
