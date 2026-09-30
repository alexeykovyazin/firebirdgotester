package summary

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"fb-loadgen/config"
	"fb-loadgen/emul"
	"fb-loadgen/metrics"
	"fb-loadgen/ops"
	"fb-loadgen/profile"
	"fb-loadgen/ramp"
	"fb-loadgen/worker"
)

// --- minimal fakes (same shape as the ramp lifecycle test stubs) -----------

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) { return &fakeConn{}, nil }

type fakeConn struct{}

func (*fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("no") }
func (*fakeConn) Close() error                        { return nil }
func (*fakeConn) Begin() (driver.Tx, error)           { return fakeTx{}, nil }

type fakeTx struct{}

func (fakeTx) Commit() error   { return nil }
func (fakeTx) Rollback() error { return nil }

var registerFake sync.Once

type stubConnector struct{}

func (stubConnector) Open() (*sql.DB, error) {
	registerFake.Do(func() { sql.Register("fbloadgen_fake_summary", fakeDriver{}) })
	db, err := sql.Open("fbloadgen_fake_summary", "test")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

func (stubConnector) Close(db *sql.DB) error {
	if db == nil {
		return nil
	}
	return db.Close()
}

type stubProfile struct{}

func (stubProfile) Name() string { return "stub" }
func (stubProfile) NextOp() func(context.Context, *sql.Tx, *ops.Cache) error {
	return func(context.Context, *sql.Tx, *ops.Cache) error { return nil }
}
func (stubProfile) NextOpWithName() (func(context.Context, *sql.Tx, *ops.Cache) error, string) {
	return stubProfile{}.NextOp(), "STUB_OP"
}
func (stubProfile) Weights() []profile.OpWeight { return nil }

// The golden text: synthetic counters in, stable sections out. The password
// must never appear (config echo is redacted).
func TestBuildTextSections(t *testing.T) {
	cfg := &config.Config{
		DSN:       "localhost/3050:C:/dbs/test.fdb",
		User:      "SYSDBA",
		Pass:      "masterkey",
		Profile:   "oltp-emul",
		ConnMin:   2,
		ConnMax:   20,
		TxTimeout: 10,
	}

	wm := worker.NewMetricsCollector()
	wm.RecordUnit("sp_pay_from_customer", 10*time.Millisecond, emul.OutcomeOK)
	wm.RecordUnit("sp_pay_from_customer", 20*time.Millisecond, emul.OutcomeFailure)
	wm.RecordUnit("srv_recalc_idx_stat", 5*time.Millisecond, emul.OutcomeOK)
	wm.RecordError(errors.New("lock conflict on no wait transaction"))
	wm.RecordError(errors.New("lock conflict on no wait transaction"))
	wm.RecordError(errors.New("cannot update erased record"))
	wm.RecordVariant(ops.Scenario{IsolationName: "snapshot/nowait"}, true)

	sched := ramp.NewScheduler(cfg, stubConnector{}, nil, stubProfile{}, wm)
	coll := metrics.NewMetricsCollector(sched, stubProfile{}, nil, wm)

	s := (&Builder{
		Collector: coll,
		Sched:     sched,
		WM:        wm,
		Emul:      nil,
		Cfg:       cfg,
		Version:   "v1.0.2",
	}).Build()

	text := s.Text()

	for _, want := range []string{
		"=== FINAL DETAILED SUMMARY ===",
		"Version: v1.0.2",
		"dsn=SYSDBA:***@localhost:3050/C:/dbs/test.fdb",
		"profile=oltp-emul conns=2..20",
		"Units (top 20 by failure):",
		"srv_recalc_idx_stat",
		"UNEXPECTED=",
		"lock conflict on no wait transaction",
		"cannot update erased record",
		"snapshot/nowait",
		"stopFailures=0 reaped=0",
		"Timeline (per-minute OK/err, avg ms):",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("summary text missing %q\n---\n%s", want, text)
		}
	}
	if strings.Contains(text, "masterkey") {
		t.Fatalf("summary text leaked the password:\n%s", text)
	}

	// The units table sorts by failure desc: the pay unit (2 failures) first.
	i := strings.Index(text, "sp_pay_from_customer")
	j := strings.Index(text, "srv_recalc_idx_stat")
	if i < 0 || j < 0 || i > j {
		t.Fatalf("units not sorted by failure desc:\n%s", text)
	}

	// JSON must marshal without nil panics (the UI serves this verbatim).
	if b, err := json.Marshal(s); err != nil || len(b) < 200 {
		t.Fatalf("marshal: err=%v len=%d", err, len(b))
	}
}
