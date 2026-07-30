package discover

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDiscoverDuplicateBasenames(t *testing.T) {
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "prod", "EMPLOYEE.FDB"), []byte("a"))
	mustWrite(t, filepath.Join(root, "staging", "EMPLOYEE.FDB"), []byte("b"))
	mustWrite(t, filepath.Join(root, "other", "demo.fdb"), []byte("c"))

	got, err := Discover(Options{Root: root, Mask: "*.fdb", Recursive: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 databases, got %d: %+v", len(got), got)
	}

	ids := map[string]bool{}
	for _, d := range got {
		if ids[d.AbsPath] {
			t.Fatalf("duplicate AbsPath: %s", d.AbsPath)
		}
		ids[d.AbsPath] = true
		if d.Name == "" || d.RelPath == "" {
			t.Fatalf("missing name/relpath: %+v", d)
		}
	}

	relSet := map[string]bool{}
	for _, d := range got {
		relSet[d.RelPath] = true
	}
	if !relSet["prod/EMPLOYEE.FDB"] || !relSet["staging/EMPLOYEE.FDB"] {
		t.Fatalf("expected prod/ and staging/ relative paths, got %v", relSet)
	}
}

func TestDiscoverNonRecursive(t *testing.T) {
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "top.fdb"), []byte("t"))
	mustWrite(t, filepath.Join(root, "sub", "nested.fdb"), []byte("n"))

	got, err := Discover(Options{Root: root, Mask: "*.fdb", Recursive: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Name != "top.fdb" {
		t.Fatalf("expected only top.fdb, got %+v", got)
	}
}

func TestResolveUnderRootRejectsEscape(t *testing.T) {
	root := t.TempDir()
	_, err := ResolveUnderRoot(root, filepath.Join("..", "outside.fdb"))
	if err == nil {
		t.Fatal("expected escape rejection")
	}
}

func TestBuildDSN(t *testing.T) {
	dsn := BuildDSN("localhost", 3055, `E:\data\EMPLOYEE.FDB`)
	if dsn != `localhost/3055:E:\data\EMPLOYEE.FDB` {
		t.Fatalf("unexpected dsn: %s", dsn)
	}
}

func mustWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}
