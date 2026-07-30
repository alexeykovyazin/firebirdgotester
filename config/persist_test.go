package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestUISettingsPersistRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fb-loadgen.ui.json")

	s := UISettings{
		Version: 2, Host: "fb.local", Port: 3050, User: "SYSDBA", Pass: "secret",
		DiscoverDir: "E:/dbs", DiscoverMask: "*.fdb", DiscoverRecursive: true, MaxTotalConns: 150,
		Sessions: map[string]SessionPrefs{
			`E:\dbs\a.fdb`: {Profile: "write-heavy", ConnMin: 2, ConnMax: 10, Warmup: 5, Main: 30, Cooldown: 5, TxTimeout: 10},
		},
	}
	if err := SaveUISettings(path, s); err != nil {
		t.Fatal(err)
	}
	got, loaded, err := LoadUISettings(path)
	if err != nil || !loaded {
		t.Fatalf("load: loaded=%v err=%v", loaded, err)
	}
	if got.Host != s.Host || got.Port != s.Port || got.User != s.User || got.Pass != s.Pass {
		t.Fatalf("mismatch: %+v", got)
	}
	if got.MaxTotalConns != 150 || !got.DiscoverRecursive {
		t.Fatalf("v2 fields: %+v", got)
	}
	pref, ok := got.Sessions[`E:\dbs\a.fdb`]
	if !ok || pref.Profile != "write-heavy" || pref.ConnMax != 10 {
		t.Fatalf("sessions: %+v", got.Sessions)
	}
}

func TestLoadUISettingsMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.json")
	got, loaded, err := LoadUISettings(path)
	if err != nil || loaded {
		t.Fatalf("expected defaults, loaded=%v err=%v", loaded, err)
	}
	if got.Port != 3050 || got.User != "SYSDBA" || got.Version != 2 {
		t.Fatalf("defaults: %+v", got)
	}
	_ = os.Remove(path)
}

func TestMigrateV1ToV2(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fb-loadgen.ui.json")
	v1 := map[string]interface{}{
		"host": "old", "port": 3055, "user": "SYSDBA", "pass": "x",
		"discoverDir": ".", "discoverMask": "*.fdb",
	}
	raw, _ := json.Marshal(v1)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	got, loaded, err := LoadUISettings(path)
	if err != nil || !loaded {
		t.Fatalf("load: %v %v", loaded, err)
	}
	if got.Version != 2 {
		t.Fatalf("expected migrated version 2, got %d", got.Version)
	}
	if !got.DiscoverRecursive {
		t.Fatal("expected discoverRecursive true after migrate")
	}
	if got.MaxTotalConns != 200 {
		t.Fatalf("expected default maxTotalConns, got %d", got.MaxTotalConns)
	}
	if got.Sessions == nil {
		t.Fatal("sessions map should be non-nil")
	}
}

func TestBlankPassKeepsExisting(t *testing.T) {
	prev := UISettings{Pass: "secret"}
	incoming := UISettings{Pass: ""}
	merged := incoming.MergePassKeepExisting(prev)
	if merged.Pass != "secret" {
		t.Fatalf("expected keep secret, got %q", merged.Pass)
	}
}

func TestRedactedForAPI(t *testing.T) {
	s := UISettings{Host: "h", Port: 3050, User: "u", Pass: "secret", DiscoverDir: ".", DiscoverMask: "*.fdb", MaxTotalConns: 10}
	r := s.RedactedForAPI()
	if r["pass"] != "" {
		t.Fatal("pass should be empty")
	}
	if r["hasPass"] != true {
		t.Fatal("hasPass should be true")
	}
}
