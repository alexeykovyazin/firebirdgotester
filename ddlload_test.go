package main

import (
	"strings"
	"testing"
)

func TestDDLhIndexSet(t *testing.T) {
	ddl := ddlhIndexDDLs()
	if len(ddl) < 10 {
		t.Fatalf("index set too small: %d", len(ddl))
	}
	for _, d := range ddl {
		if !strings.HasPrefix(d, "CREATE INDEX DLH_") {
			t.Fatalf("unexpected index DDL: %s", d)
		}
	}
	// composite mixes with different orders must both be present
	joined := strings.Join(ddl, "\n")
	for _, want := range []string{"(NUM, DT)", "(DT, NUM)", "(QTY, NAME)", "(NAME, QTY)", "(QTY, CODE)"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("composite %q missing from the index set", want)
		}
	}
}

func TestDDLhTableSeveralVarchars(t *testing.T) {
	ddl := ddlhTableDDL()
	n := strings.Count(ddl, "VARCHAR(")
	if n < 4 {
		t.Fatalf("table has %d varchar columns, want >= 4 (CLHISTNUM shape)", n)
	}
	for _, col := range []string{"NUM VARCHAR(32)", "CODE VARCHAR(16)", "NAME VARCHAR(64)", "DESCR VARCHAR(256)"} {
		if !strings.Contains(ddl, col) {
			t.Fatalf("varchar column %q missing", col)
		}
	}
}

func TestDDLhProceduresTypeOfColumn(t *testing.T) {
	sel := ddlhProcSelDDL()
	if !strings.Contains(sel, "CREATE PROCEDURE SP_DLH_SEL") {
		t.Fatal("SP_DLH_SEL not created")
	}
	if !strings.Contains(sel, "P_CODE TYPE OF COLUMN DLH_MAIN.CODE") ||
		!strings.Contains(sel, "P_NAME TYPE OF COLUMN DLH_MAIN.NAME") {
		t.Fatal("selectable procedure lacks TYPE OF COLUMN parameters")
	}
	if !strings.Contains(sel, "SUSPEND") {
		t.Fatal("selectable procedure must contain SUSPEND")
	}
	if !strings.Contains(sel, "UPDATE "+ddlhTable) {
		t.Fatal("selectable procedure must also do an update")
	}
	upd := ddlhProcUpdDDL()
	if !strings.Contains(upd, "P_NUM TYPE OF COLUMN DLH_MAIN.NUM") ||
		!strings.Contains(upd, "P_DESCR TYPE OF COLUMN DLH_MAIN.DESCR") {
		t.Fatal("executable procedure lacks TYPE OF COLUMN parameters")
	}
}

func TestDDLhWidenIncrements(t *testing.T) {
	// +1 increments toward the target: 64 -> 1024 takes exactly 960 steps
	ddl, next, ok := ddlhWidenDDL("NAME", 64, 1, 1024)
	if !ok || next != 65 || ddl != "ALTER TABLE DLH_MAIN ALTER COLUMN NAME TYPE VARCHAR(65)" {
		t.Fatalf("first widen wrong: %q next=%d ok=%v", ddl, next, ok)
	}
	// step overshoot is clamped to the max
	ddl, next, ok = ddlhWidenDDL("NAME", 1023, 5, 1024)
	if !ok || next != 1024 || ddl != "ALTER TABLE DLH_MAIN ALTER COLUMN NAME TYPE VARCHAR(1024)" {
		t.Fatalf("clamped widen wrong: %q next=%d ok=%v", ddl, next, ok)
	}
	// target reached: stop
	_, _, ok = ddlhWidenDDL("NAME", 1024, 1, 1024)
	if ok {
		t.Fatal("widening must stop at the target")
	}
}
