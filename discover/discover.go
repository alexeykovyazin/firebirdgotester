package discover

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// DatabaseInfo describes a discovered Firebird database file.
type DatabaseInfo struct {
	Name    string    // basename (e.g. EMPLOYEE.FDB)
	RelPath string    // slash-normalized path relative to root
	AbsPath string    // absolute path (identity key)
	Size    int64     // bytes
	ModTime time.Time // mtime
}

// Options controls discovery behavior.
type Options struct {
	Root      string // allowlisted base directory
	Mask      string // glob mask, default *.fdb
	Recursive bool
}

var skipDirNames = map[string]bool{
	".git": true,
	".svn": true,
	".hg":  true,
}

// Discover scans Root for database files matching Mask.
// Paths that escape Root are rejected. Identity is always AbsPath.
func Discover(opts Options) ([]DatabaseInfo, error) {
	if opts.Root == "" {
		opts.Root = "."
	}
	if opts.Mask == "" {
		opts.Mask = "*.fdb"
	}

	rootAbs, err := filepath.Abs(opts.Root)
	if err != nil {
		return nil, fmt.Errorf("resolve discover root: %w", err)
	}
	rootAbs = filepath.Clean(rootAbs)

	info, err := os.Stat(rootAbs)
	if err != nil {
		return nil, fmt.Errorf("discover root: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("discover root is not a directory: %s", rootAbs)
	}

	var results []DatabaseInfo

	walkFn := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // skip unreadable entries
		}
		if d.IsDir() {
			name := d.Name()
			if skipDirNames[name] {
				return filepath.SkipDir
			}
			// Skip Firebird journal/archive directories
			lower := strings.ToLower(name)
			if strings.HasSuffix(lower, ".logarch") || strings.HasSuffix(lower, ".repllog") {
				return filepath.SkipDir
			}
			// Only skip descending when non-recursive and this is not the root itself
			if !opts.Recursive && !sameDir(path, rootAbs) {
				return filepath.SkipDir
			}
			return nil
		}

		matched, err := matchMask(d.Name(), opts.Mask)
		if err != nil || !matched {
			return nil
		}

		absPath, err := filepath.Abs(path)
		if err != nil {
			return nil
		}
		absPath = filepath.Clean(absPath)
		if !isUnderRoot(rootAbs, absPath) {
			return nil
		}

		fi, err := d.Info()
		if err != nil {
			return nil
		}

		rel, err := filepath.Rel(rootAbs, absPath)
		if err != nil {
			return nil
		}
		rel = filepath.ToSlash(rel)

		results = append(results, DatabaseInfo{
			Name:    d.Name(),
			RelPath: rel,
			AbsPath: absPath,
			Size:    fi.Size(),
			ModTime: fi.ModTime(),
		})
		return nil
	}

	if err := filepath.WalkDir(rootAbs, walkFn); err != nil {
		return nil, fmt.Errorf("walk discover root: %w", err)
	}

	return results, nil
}

// ResolveUnderRoot joins root with a relative (or absolute-under-root) path and
// ensures the result stays inside the allowlisted root.
func ResolveUnderRoot(root, userPath string) (string, error) {
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	rootAbs = filepath.Clean(rootAbs)

	var candidate string
	if filepath.IsAbs(userPath) {
		candidate = filepath.Clean(userPath)
	} else {
		candidate = filepath.Clean(filepath.Join(rootAbs, userPath))
	}

	if !isUnderRoot(rootAbs, candidate) {
		return "", fmt.Errorf("path escapes discover root: %s", userPath)
	}
	return candidate, nil
}

// BuildDSN builds a user-facing DSN host/port:absPath.
func BuildDSN(host string, port int, absPath string) string {
	if host == "" {
		host = "localhost"
	}
	if port <= 0 {
		port = 3050
	}
	return fmt.Sprintf("%s/%d:%s", host, port, absPath)
}

func isUnderRoot(root, path string) bool {
	root = filepath.Clean(root)
	path = filepath.Clean(path)
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return false
	}
	return true
}

func sameDir(a, b string) bool {
	a = filepath.Clean(a)
	b = filepath.Clean(b)
	return strings.EqualFold(a, b)
}

// matchMask supports simple globs; comparison is case-insensitive for the extension/pattern.
func matchMask(name, mask string) (bool, error) {
	ok, err := filepath.Match(mask, name)
	if err != nil {
		return false, err
	}
	if ok {
		return true, nil
	}
	// Case-insensitive fallback (Windows-friendly)
	ok, err = filepath.Match(strings.ToLower(mask), strings.ToLower(name))
	return ok, err
}
