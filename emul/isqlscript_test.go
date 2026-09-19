package emul

import (
	"strings"
	"testing"
)

func mustAsset(t *testing.T, name string) string {
	t.Helper()
	b, err := assetsFS.ReadFile("assets/" + name)
	if err != nil {
		t.Fatalf("read asset %s: %v", name, err)
	}
	return string(b)
}

func TestParseScript_Synthetic(t *testing.T) {
	tests := []struct {
		name    string
		script  string
		want    []StmtKind
		wantSQL []string
		wantErr bool
	}{
		{
			name:   "plain statements",
			script: "select 1 from rdb$database; commit;",
			want:   []StmtKind{StmtSQL, StmtCommit},
		},
		{
			name:    "caret inside string literal is not a terminator",
			script:  "insert into t values('a^b;c'); commit;",
			want:    []StmtKind{StmtSQL, StmtCommit},
			wantSQL: []string{"insert into t values('a^b;c')"},
		},
		{
			name:   "semicolon inside string literal",
			script: "insert into t values('a;b'); commit;",
			want:   []StmtKind{StmtSQL, StmtCommit},
		},
		{
			name:   "doubled quote escape",
			script: "insert into t values('it''s; fine');",
			want:   []StmtKind{StmtSQL},
		},
		{
			name:    "set term switch and glue directive",
			script:  "set term ^; execute block as begin x=1; end^set term ;^ commit;",
			want:    []StmtKind{StmtSQL, StmtCommit},
			wantSQL: []string{"execute block as begin x=1; end"},
		},
		{
			name:   "line comment with terminator",
			script: "-- comment; with semicolon\nselect 1 from rdb$database;",
			want:   []StmtKind{StmtSQL},
		},
		{
			name:   "block comment with terminators",
			script: "/* comment ; ^ */ select 1 from rdb$database;",
			want:   []StmtKind{StmtSQL},
		},
		{
			name:   "isql set commands skipped",
			script: "set bail on; set list on; select 1 from rdb$database; set term ^; execute block as begin end^set term ;^",
			want:   []StmtKind{StmtSkip, StmtSkip, StmtSQL, StmtSQL},
		},
		{
			name:    "statement preceded by trailing comment of previous one",
			script:  "set term ^; execute block as begin end^ -- trailing comment\ncreate procedure p as begin end^set term ;^ commit;",
			want:    []StmtKind{StmtSQL, StmtSQL, StmtCommit},
			wantSQL: []string{"execute block as begin end", "create procedure p as begin end", "commit"},
		},
		{
			name:   "comment-only chunk dropped",
			script: "commit; -- just a comment, no statement\n-- another\n",
			want:   []StmtKind{StmtCommit},
		},
		{
			name:   "standalone exit and quit classified",
			script: "commit; exit;",
			want:   []StmtKind{StmtCommit, StmtExit},
		},
		{
			name:    "unterminated string",
			script:  "insert into t values('oops",
			wantErr: true,
		},
		{
			name:    "unterminated block comment",
			script:  "/* never closed; select 1;",
			wantErr: true,
		},
		{
			name:    "psql body with semicolons and quotes in one chunk",
			script:  "set term ^; create procedure p as begin x = 'a;b^c'; end^set term ;^ commit;",
			want:    []StmtKind{StmtSQL, StmtCommit},
			wantSQL: []string{"create procedure p as begin x = 'a;b^c'; end"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := ParseScript(tt.script)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (%d statements)", len(stmts))
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseScript: %v", err)
			}
			if len(stmts) != len(tt.want) {
				t.Fatalf("got %d statements (%v), want %d", len(stmts), kinds(stmts), len(tt.want))
			}
			for i, k := range tt.want {
				if stmts[i].Kind != k {
					t.Errorf("stmt[%d] kind = %v, want %v (sql=%q)", i, stmts[i].Kind, k, stmts[i].SQL)
				}
			}
			for i, want := range tt.wantSQL {
				if stmts[i].SQL != want {
					t.Errorf("stmt[%d] sql = %q, want %q", i, stmts[i].SQL, want)
				}
			}
		})
	}
}

func kinds(stmts []Statement) []StmtKind {
	out := make([]StmtKind, len(stmts))
	for i, s := range stmts {
		out[i] = s.Kind
	}
	return out
}

// TestParseScript_GoldenAssets parses the five vendored oltp-emul scripts in
// full. This is the CI-level gate: a parser regression (e.g. a `^` inside a
// string literal splitting a procedure body) must fail here before it can
// corrupt a real database. Counts are exact and calibrated against upstream
// commit e9b158e8 (see assets/VERSION); refresh them when updating assets.
func TestParseScript_GoldenAssets(t *testing.T) {
	type lineAnchor struct {
		line   int
		prefix string
	}
	golden := map[string]struct {
		stmts      int
		procedures int // statements beginning "create or alter procedure"
		maxStmtKB  int // no single statement may exceed this (merge detector)
		anchors    []lineAnchor
	}{
		// Counts include only TOP-LEVEL occurrences: upstream sources contain
		// a few "create or alter procedure" texts inside string literals
		// (common_sp generates procedure DDL dynamically) that correctly do
		// not parse as statements.
		"oltp30_DDL.sql":        {396, 60, 64, []lineAnchor{{618, "recreate table settings"}, {8768, "create or alter procedure srv_random_unit_choice"}}},
		"oltp30_sp.sql":         {47, 29, 64, []lineAnchor{{397, "create or alter procedure sp_client_order"}}},
		"oltp_common_sp.sql":    {64, 38, 64, []lineAnchor{{418, "create or alter procedure fn_halt_sign"}}},
		"oltp_adjust_DDL.sql":   {29, 2, 64, nil},
		"oltp_main_filling.sql": {1533, 0, 64, nil},
		"oltp_data_filling.sql": {16518, 0, 256, nil},
	}
	for name, g := range golden {
		t.Run(name, func(t *testing.T) {
			stmts, err := ParseScript(mustAsset(t, name))
			if err != nil {
				t.Fatalf("ParseScript(%s): %v", name, err)
			}
			if g.stmts > 0 && len(stmts) != g.stmts {
				t.Errorf("got %d statements, want exactly %d", len(stmts), g.stmts)
			}
			var procs int
			var commits int
			for _, s := range stmts {
				low := strings.ToLower(s.SQL)
				if strings.HasPrefix(low, "create or alter procedure") {
					procs++
				}
				if s.Kind == StmtCommit {
					commits++
				}
				if s.Kind == StmtSQL && (strings.HasPrefix(low, "set term") || strings.HasPrefix(low, "set bail")) {
					t.Errorf("isql directive leaked as SQL at line %d: %q", s.Line, firstLine(s.SQL))
				}
				if len(s.SQL) > g.maxStmtKB*1024 {
					t.Errorf("statement at line %d is %d KB (> %d KB) - parser likely merged statements",
						s.Line, len(s.SQL)/1024, g.maxStmtKB)
				}
			}
			if procs != g.procedures {
				t.Errorf("got %d procedure statements, want exactly %d (a merged or split procedure = parser bug)", procs, g.procedures)
			}
			// spot-check statement start lines (first statement of each
			// named procedure) — catches off-by-one line drift
			for _, want := range g.anchors {
				found := false
				for _, st := range stmts {
					if st.Kind == StmtSQL && st.Line == want.line &&
						strings.HasPrefix(strings.ToLower(st.SQL), want.prefix) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("no statement at line %d starting with %q", want.line, want.prefix)
				}
			}
			if commits == 0 {
				t.Errorf("no COMMIT statements found - script likely misparsed")
			}
		})
	}
}

func TestParseScript_GoldenAssetContent(t *testing.T) {
	ddl, err := ParseScript(mustAsset(t, "oltp30_DDL.sql"))
	if err != nil {
		t.Fatalf("parse DDL: %v", err)
	}
	for _, marker := range []string{
		"recreate table business_ops",
		"create or alter procedure srv_random_unit_choice",
		"create or alter procedure sp_init_ctx",
		"recreate global temporary table tmp$perf_log",
	} {
		if !containsStatement(ddl, marker) {
			t.Errorf("DDL: marker %q not found as statement prefix", marker)
		}
	}

	sp, err := ParseScript(mustAsset(t, "oltp30_sp.sql"))
	if err != nil {
		t.Fatalf("parse SP: %v", err)
	}
	for _, marker := range []string{
		"create or alter procedure sp_client_order",
		"create or alter procedure sp_supplier_invoice",
		"create or alter procedure sp_cancel_client_order",
	} {
		if !containsStatement(sp, marker) {
			t.Errorf("SP: marker %q not found", marker)
		}
	}

	fill, err := ParseScript(mustAsset(t, "oltp_data_filling.sql"))
	if err != nil {
		t.Fatalf("parse fill: %v", err)
	}
	if !containsStatement(fill, "insert into wares select * from tmp$wares") {
		t.Errorf("fill: wares insert from GTT not found as top-level statement")
	}
}

func containsStatement(stmts []Statement, prefixLower string) bool {
	for _, s := range stmts {
		if s.Kind == StmtSQL && strings.HasPrefix(strings.ToLower(s.SQL), prefixLower) {
			return true
		}
	}
	return false
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// BenchmarkParseDataFilling guards the incremental line tracking: parsing
// the largest asset must stay linear (the old start2line implementation was
// quadratic — ~4.7 s for this file).
func BenchmarkParseDataFilling(b *testing.B) {
	script := mustAssetB(b, "oltp_data_filling.sql")
	b.SetBytes(int64(len(script)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ParseScript(script); err != nil {
			b.Fatal(err)
		}
	}
}

func mustAssetB(b *testing.B, name string) string {
	b.Helper()
	data, err := assetsFS.ReadFile("assets/" + name)
	if err != nil {
		b.Fatal(err)
	}
	return string(data)
}

// Guard against accidental asset edits: the vendored files are verbatim
// upstream copies (see NOTICE); their sizes are part of the golden contract.
func TestAssetsVerbatimSizes(t *testing.T) {
	want := map[string]int{
		"oltp30_DDL.sql":        9388,
		"oltp30_sp.sql":         5608,
		"oltp_common_sp.sql":    4710,
		"oltp_adjust_DDL.sql":   838,
		"oltp_main_filling.sql": 2248,
		"oltp_data_filling.sql": 17092,
	}
	for name, lines := range want {
		b, err := assetsFS.ReadFile("assets/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		got := strings.Count(string(b), "\n")
		if got != lines {
			t.Errorf("%s: %d lines, want %d (assets must be verbatim upstream copies)", name, got, lines)
		}
	}
}
