package emul

import (
	"errors"
	"strings"
	"testing"
)

func TestProcVictimDDLGolden(t *testing.T) {
	a := procVictimAlterDDL("A")
	for _, want := range []string{
		"ALTER PROCEDURE SP_ELT_VICTIM (IN1 INTEGER)",
		"RETURNS (OUT1 INTEGER)",
		", 'A', 'sig-A')",
	} {
		if !strings.Contains(a, want) {
			t.Fatalf("sig A missing %q:\n%s", want, a)
		}
	}
	b := procVictimAlterDDL("B")
	for _, want := range []string{
		"ALTER PROCEDURE SP_ELT_VICTIM (IN1 INTEGER, IN2 VARCHAR(32))",
		"RETURNS (OUT1 INTEGER, OUT2 VARCHAR(32))",
		", 'B', :IN2)",
	} {
		if !strings.Contains(b, want) {
			t.Fatalf("sig B missing %q:\n%s", want, b)
		}
	}
}

func TestParseProcSig(t *testing.T) {
	if parseProcSig(1) != "A" || parseProcSig(2) != "B" || parseProcSig(0) != "" {
		t.Fatalf("parseProcSig mapping wrong: %q %q %q", parseProcSig(1), parseProcSig(2), parseProcSig(0))
	}
}

func TestVictimStepNameRotation(t *testing.T) {
	want := []string{"VAL", "CNT", "NUM", "VAL_NULL", "VAL"}
	for i, w := range want {
		if got := victimStepName(i); got != w {
			t.Fatalf("victimStepName(%d) = %s, want %s", i, got, w)
		}
	}
}

// The full VAL lifecycle: 64→128→256, then a normalization UPDATE + step back
// down. Every step carries a revert that restores the model on failure.
func TestNextTypeStepValCycle(t *testing.T) {
	v := &victimState{valLen: 64}

	step := v.nextTypeStep()
	if step.ddl != "ALTER TABLE EL_DDL_VICTIM ALTER COLUMN VAL TYPE VARCHAR(128)" || v.valLen != 128 {
		t.Fatalf("step1: %q valLen=%d", step.ddl, v.valLen)
	}
	step = v.nextTypeStep() // idx1 = CNT
	if step.ddl != "ALTER TABLE EL_DDL_VICTIM ALTER COLUMN CNT TYPE BIGINT" || !v.cntBigint {
		t.Fatalf("step2: %q bigint=%v", step.ddl, v.cntBigint)
	}

	v.stepIdx = 0 // back to VAL
	v.valLen = 256
	step = v.nextTypeStep()
	if step.normalize == "" || step.ddl != "ALTER TABLE EL_DDL_VICTIM ALTER COLUMN VAL TYPE VARCHAR(128)" || v.valLen != 128 {
		t.Fatalf("down step: %q norm=%q valLen=%d", step.ddl, step.normalize, v.valLen)
	}
	step.revert()
	if v.valLen != 256 {
		t.Fatalf("revert did not restore valLen: %d", v.valLen)
	}
}

func TestNextTypeStepCntAndNull(t *testing.T) {
	v := &victimState{cntBigint: true, valNullable: true}
	v.stepIdx = 1
	step := v.nextTypeStep() // CNT down
	if step.normalize != "UPDATE EL_DDL_VICTIM SET CNT = 0" ||
		step.ddl != "ALTER TABLE EL_DDL_VICTIM ALTER COLUMN CNT TYPE INTEGER" || v.cntBigint {
		t.Fatalf("CNT down wrong: %q norm=%q bigint=%v", step.ddl, step.normalize, v.cntBigint)
	}

	v.stepIdx = 3
	step = v.nextTypeStep() // VAL_NULL: set NOT NULL with fill
	if step.normalize != "UPDATE EL_DDL_VICTIM SET VAL = COALESCE(VAL, '')" ||
		step.ddl != "ALTER TABLE EL_DDL_VICTIM ALTER COLUMN VAL SET NOT NULL" || v.valNullable {
		t.Fatalf("SET NOT NULL wrong: %q norm=%q nullable=%v", step.ddl, step.normalize, v.valNullable)
	}
	v.stepIdx = 3
	step = v.nextTypeStep() // now drop NOT NULL
	if step.ddl != "ALTER TABLE EL_DDL_VICTIM ALTER COLUMN VAL DROP NOT NULL" || !v.valNullable {
		t.Fatalf("DROP NOT NULL wrong: %q nullable=%v", step.ddl, v.valNullable)
	}
}

func TestDDLEExpectedError(t *testing.T) {
	expected := []string{
		"object SP_ELT_VICTIM is in use",
		"lock conflict on no wait transaction",
		"unsuccessful metadata update",
		"count of column list or variable list does not match SELECT list",
		"Column unknown. VAL",
		"Procedure SP_ELT_VICTIM is not defined",
		"Procedure SP_ELT_VICTIM is not selectable (it does not contain a SUSPEND statement)",
		"new length shorter than existing",
		"attempt to store a value in a data type that is too short",
		"deadlock update conflicts with concurrent update",
	}
	for _, msg := range expected {
		if !ddlExpectedError(errors.New(msg)) {
			t.Fatalf("expected-classified text not recognized: %q", msg)
		}
	}
	notExpected := []string{
		"connection refused",
		"wsarecv: An existing connection was forcibly closed",
		"arithmetic exception, numeric overflow",
	}
	for _, msg := range notExpected {
		if ddlExpectedError(errors.New(msg)) {
			t.Fatalf("non-DDL text classified as expected: %q", msg)
		}
	}
	if ddlExpectedError(nil) {
		t.Fatal("nil must not be classified")
	}
}
