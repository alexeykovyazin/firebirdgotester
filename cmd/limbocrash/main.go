// limbocrash is the small reproducer of plan FB5_LIMBO_CRASH_REPRO_PLAN.md
// (phase 3): Firebird 5 logging "transaction to unlink is missing in the
// attachment" and ending its process after a client prepares a transaction
// and drops the socket.
//
// Each loop: connect, start a transaction, optionally write one row, then
// complete it as -mode says:
//
//	die      op_prepare, then close the socket without detach (prepare-then-die)
//	drop     close the socket without prepare or rollback (control)
//	commit   ordinary commit (control)
//
// After the loop it checks, with a fresh connection, whether the server still
// answers, and optionally resolves the limbo transactions through the
// Services API (-resolve commit|rollback). One line per loop.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	firebirdsql "github.com/nakagami/firebirdsql"
)

func main() {
	host := flag.String("host", "localhost:3095", "server host:port")
	db := flag.String("db", `C:\repro\R2.FDB`, "database path on the server")
	user := flag.String("user", "SYSDBA", "user")
	pass := flag.String("password", os.Getenv("ISC_PASSWORD"), "password (default: ISC_PASSWORD)")
	mode := flag.String("mode", "die", "die | drop | commit")
	iso := flag.String("iso", "rc", "rc | rcnowait | snapshot | snapshotnowait")
	write := flag.Bool("write", true, "insert one row before completing")
	loops := flag.Int("loops", 1, "loops per worker")
	workers := flag.Int("workers", 1, "parallel workers")
	pause := flag.Duration("pause", 2*time.Second, "pause after each loop")
	resolve := flag.String("resolve", "none", "none | commit | rollback: resolve limbo through the Services API after each loop")
	initDB := flag.Bool("init", false, "create table LIMBO_T and exit")
	flag.Parse()

	dsn := fmt.Sprintf("%s:%s@%s/%s", *user, *pass, *host, strings.ReplaceAll(*db, `\`, "/"))
	if *initDB {
		c, err := sql.Open("firebirdsql", dsn)
		if err == nil {
			_, err = c.Exec("create table LIMBO_T (ID bigint, WORKER int, TS timestamp default localtimestamp)")
			c.Close()
		}
		fmt.Println("init:", errText(err))
		return
	}
	isoLevel := map[string]int{"rc": firebirdsql.IsoRC, "rcnowait": firebirdsql.IsoRCNoWait,
		"snapshot": firebirdsql.IsoSnapshot, "snapshotnowait": firebirdsql.IsoSnapshotNoWait}[*iso]
	level := isoLevel
	switch *mode {
	case "die":
		level += firebirdsql.LevelPrepareThenDieBase
	case "drop":
		level += firebirdsql.LevelHardDropBase
	case "commit":
	default:
		fmt.Fprintln(os.Stderr, "bad -mode")
		os.Exit(2)
	}

	var mu sync.Mutex
	out := func(format string, a ...any) {
		mu.Lock()
		defer mu.Unlock()
		fmt.Printf("%s "+format+"\n", append([]any{time.Now().Format("15:04:05.000")}, a...)...)
	}
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 1; i <= *loops; i++ {
				err := once(dsn, level, *write, w, i)
				alive, took := ping(dsn)
				out("worker=%d loop=%d mode=%s iso=%s write=%v: %s; server %s (%s)",
					w, i, *mode, *iso, *write, errText(err), alive, took.Round(time.Millisecond))
				if *resolve != "none" {
					out("worker=%d loop=%d resolve %s: %s", w, i, *resolve, resolveLimbo(*host, *user, *pass, *db, *resolve))
				}
				time.Sleep(*pause)
			}
		}(w)
	}
	wg.Wait()
}

// once runs one transaction with the given (intent-encoded) isolation level.
func once(dsn string, level int, write bool, w, i int) error {
	c, err := sql.Open("firebirdsql", dsn)
	if err != nil {
		return err
	}
	defer c.Close()
	c.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tx, err := c.BeginTx(ctx, &sql.TxOptions{Isolation: sql.IsolationLevel(level)})
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	if write {
		if _, err := tx.ExecContext(ctx, "insert into LIMBO_T (ID, WORKER) values (?, ?)", i, w); err != nil {
			return fmt.Errorf("insert: %w", err)
		}
	}
	if level >= firebirdsql.LevelHardDropBase {
		return tx.Rollback() // drop: the socket goes, no rollback is sent
	}
	return tx.Commit() // die: op_prepare, then the socket goes; commit: ordinary
}

// ping reports whether a fresh connection gets an answer.
func ping(dsn string) (string, time.Duration) {
	t0 := time.Now()
	c, err := sql.Open("firebirdsql", dsn)
	if err == nil {
		var n int
		err = c.QueryRow("select 1 from rdb$database").Scan(&n)
		c.Close()
	}
	if err != nil {
		return "DOWN: " + errText(err), time.Since(t0)
	}
	return "up", time.Since(t0)
}

func resolveLimbo(host, user, pass, db, how string) string {
	mm, err := firebirdsql.NewMaintenanceManager(host, user, pass, firebirdsql.GetDefaultServiceManagerOptions())
	if err != nil {
		return errText(err)
	}
	tids, err := mm.GetLimboTransactions(db)
	if err != nil {
		return "list: " + errText(err)
	}
	for _, t := range tids {
		if how == "commit" {
			err = mm.CommitLimboTransaction(db, t)
		} else {
			err = mm.RollbackLimboTransaction(db, t)
		}
		if err != nil {
			return fmt.Sprintf("%d: %s", t, errText(err))
		}
	}
	return fmt.Sprintf("limbo %v", tids)
}

func errText(err error) string {
	if err == nil {
		return "ok"
	}
	s := strings.ReplaceAll(err.Error(), "\n", " ")
	if len(s) > 160 {
		s = s[:160]
	}
	return s
}
