package config

import "testing"

// The --cancel-hard-drop flag must reach the driver as query parameters on
// the connection string; without the flag the DSN stays byte-identical to
// the plain form.
func TestConnectionStringCancelHardDrop(t *testing.T) {
	base := func(c *Config) string { return "SYSDBA:masterkey@localhost:3054/C:/temp/OLTPEMUL_FB4.FDB" }

	plain := &Config{DSN: "localhost/3054:C:/temp/OLTPEMUL_FB4.FDB", User: "SYSDBA", Pass: "masterkey"}
	if got := plain.ConnectionString(); got != base(plain) {
		t.Fatalf("plain DSN changed: %q", got)
	}

	drop := &Config{DSN: "localhost/3054:C:/temp/OLTPEMUL_FB4.FDB", User: "SYSDBA", Pass: "masterkey",
		CancelHardDrop: true}
	want := base(drop) + "?cancel_hard_drop=true&cancel_hard_drop_grace=3000"
	if got := drop.ConnectionString(); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}

	dropGrace := &Config{DSN: "localhost/3054:C:/temp/OLTPEMUL_FB4.FDB", User: "SYSDBA", Pass: "masterkey",
		CancelHardDrop: true, CancelHardDropGraceMs: 1000}
	want = base(dropGrace) + "?cancel_hard_drop=true&cancel_hard_drop_grace=1000"
	if got := dropGrace.ConnectionString(); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}

	// Non-positive grace falls back to the driver default (3000).
	zero := &Config{DSN: "localhost/3054:C:/temp/OLTPEMUL_FB4.FDB", User: "SYSDBA", Pass: "masterkey",
		CancelHardDrop: true, CancelHardDropGraceMs: 0}
	want = base(zero) + "?cancel_hard_drop=true&cancel_hard_drop_grace=3000"
	if got := zero.ConnectionString(); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
