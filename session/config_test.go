package session

import (
	"testing"

	"fb-loadgen/config"
	"fb-loadgen/discover"
)

func TestEvenPerDBMax(t *testing.T) {
	cases := []struct {
		total, n, want int
	}{
		{30, 0, 30},
		{30, 1, 30},
		{30, 2, 15},
		{30, 3, 10},
		{30, 4, 7},
		{30, 40, 1},
		{0, 5, 1},
		{1, 5, 1},
	}
	for _, c := range cases {
		if got := EvenPerDBMax(c.total, c.n); got != c.want {
			t.Fatalf("EvenPerDBMax(%d,%d)=%d want %d", c.total, c.n, got, c.want)
		}
	}
}

func TestIDFromAbsPathDistinct(t *testing.T) {
	a := IDFromAbsPath(`E:\data\prod\EMPLOYEE.FDB`)
	b := IDFromAbsPath(`E:\data\staging\EMPLOYEE.FDB`)
	if a == b {
		t.Fatalf("expected distinct IDs for same basename in different folders")
	}
	if a != IDFromAbsPath(`E:\data\prod\EMPLOYEE.FDB`) {
		t.Fatalf("ID should be stable")
	}
}

func TestDefaultsFromCLIPaths(t *testing.T) {
	cli := &config.Config{
		User: "SYSDBA", Pass: "masterkey",
		ConnInit: 2, ConnPeak: 10,
		Warmup: 5, Main: 10, Cooldown: 5,
		TxTimeout: 10, Profile: "write-heavy",
	}
	cli.ConnMin = 2
	cli.ConnMax = 10

	info := discover.DatabaseInfo{
		Name:    "EMPLOYEE.FDB",
		RelPath: "prod/EMPLOYEE.FDB",
		AbsPath: `E:\data\prod\EMPLOYEE.FDB`,
	}
	sc := DefaultsFromCLI(cli, info, "localhost", 3055)
	if sc.RelPath != "prod/EMPLOYEE.FDB" {
		t.Fatalf("relpath: %s", sc.RelPath)
	}
	if sc.DSN != `localhost/3055:E:\data\prod\EMPLOYEE.FDB` {
		t.Fatalf("dsn: %s", sc.DSN)
	}
	if err := sc.Validate(); err != nil {
		t.Fatal(err)
	}
	run := sc.ToRunConfig()
	if run.ConnMin != 2 || run.ConnMax != 10 {
		t.Fatalf("run config min/max: %d/%d", run.ConnMin, run.ConnMax)
	}
}
