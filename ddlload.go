// ddlload subcommand: a self-contained DDL-under-load test
// (user scenario 2026-10-01, points 1-7).
//
//  1. creates a CLHISTNUM-shaped table with several VARCHAR columns,
//  2. fills 10k+ rows,
//  3. builds regular / composite / mixed int+varchar indexes,
//  4. prepares indexed and non-indexed selects, random inserts/updates, and
//     stored procedures with TYPE OF COLUMN parameters (selectable + executable),
//  5. runs them from several parallel connections — part in short
//     transactions, part holding ACTIVE CURSORS inside long transactions,
//  6. a sequential driver widens the varchar columns to --varchar-max in
//     --varchar-step increments, each ALTER in its own committed transaction,
//     under the load: small increments maximize the chance of hitting the
//     metadata-race window vs a single 64->256 jump,
//  7. other type changes / new columns can be added on the same skeleton.
//
// Expected metadata-race errors ("object in use", lock conflicts, signature
// mismatches) are counted, never propagated. The test ends with a summary.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/nakagami/firebirdsql"

	"fb-loadgen/config"
	"fb-loadgen/emul"
)

const (
	ddlhTable = "DLH_MAIN"
	ddlhWords = "alpha bravo charlie delta echo foxtrot golf hotel"
)

var ddlhWordList = strings.Fields(ddlhWords)

// --- DDL builders (pure: unit-tested) ----------------------------------------

// ddlhIndexDDLs returns the index set: single-column indexes on the short
// varchars plus composite indexes with different column orders and
// int+varchar mixes. DESCR (VARCHAR(256)) is deliberately NOT indexed: its
// UTF-8 key exceeds the 4096-page index limit ("key size exceeds
// implementation restriction"), and the non-indexed scan over it is exactly
// what point 4.1 wants.
func ddlhIndexDDLs() []string {
	t := ddlhTable
	return []string{
		`CREATE INDEX DLH_NUM ON ` + t + ` (NUM)`,
		`CREATE INDEX DLH_CODE ON ` + t + ` (CODE)`,
		`CREATE INDEX DLH_NAME ON ` + t + ` (NAME)`,
		`CREATE INDEX DLH_DT ON ` + t + ` (DT)`,
		`CREATE INDEX DLH_NUM_DT ON ` + t + ` (NUM, DT)`,
		`CREATE INDEX DLH_DT_NUM ON ` + t + ` (DT, NUM)`,
		`CREATE INDEX DLH_QTY_NAME ON ` + t + ` (QTY, NAME)`,
		`CREATE INDEX DLH_NAME_QTY ON ` + t + ` (NAME, QTY)`,
		`CREATE INDEX DLH_QTY_CODE ON ` + t + ` (QTY, CODE)`,
		`CREATE INDEX DLH_AMT_QTY ON ` + t + ` (AMT, QTY)`,
	}
}

// ddlhTableDDL creates the CLHISTNUM-shaped working table: several varchars
// of different sizes (they are the widening targets), numerics, int, time.
func ddlhTableDDL() string {
	return `CREATE TABLE ` + ddlhTable + ` (
		ID BIGINT NOT NULL PRIMARY KEY,
		NUM VARCHAR(32),
		CODE VARCHAR(16),
		NAME VARCHAR(64),
		DESCR VARCHAR(256),
		AMT NUMERIC(15,2),
		QTY INTEGER,
		DT TIMESTAMP,
		FLAG SMALLINT
	)`
}

// ddlhProcSelDDL: selectable procedure with TYPE OF COLUMN parameters; reads
// rows via FOR SELECT ... SUSPEND and touches an UPDATE inside (real work).
func ddlhProcSelDDL() string {
	return `CREATE PROCEDURE SP_DLH_SEL (
		P_CODE TYPE OF COLUMN ` + ddlhTable + `.CODE,
		P_NAME TYPE OF COLUMN ` + ddlhTable + `.NAME)
RETURNS (
		ID BIGINT,
		NUM TYPE OF COLUMN ` + ddlhTable + `.NUM,
		NAME TYPE OF COLUMN ` + ddlhTable + `.NAME,
		AMT NUMERIC(15,2))
AS BEGIN
	FOR SELECT ID, NUM, NAME, AMT FROM ` + ddlhTable + `
		  WHERE CODE STARTING WITH :P_CODE AND NAME CONTAINING :P_NAME
		  ROWS 50 INTO :ID, :NUM, :NAME, :AMT DO
	BEGIN
		SUSPEND;
	END
	UPDATE ` + ddlhTable + ` SET FLAG = 1 WHERE CODE = :P_CODE AND FLAG = 0
	ROWS 5;
END`
}

// ddlhProcUpdDDL: executable procedure with TYPE OF COLUMN parameters that
// updates random-ish rows and returns the affected count.
func ddlhProcUpdDDL() string {
	return `CREATE PROCEDURE SP_DLH_UPD (
		P_NUM TYPE OF COLUMN ` + ddlhTable + `.NUM,
		P_DESCR TYPE OF COLUMN ` + ddlhTable + `.DESCR)
RETURNS (
		UPDATED INTEGER)
AS BEGIN
	UPDATE ` + ddlhTable + ` SET DESCR = :P_DESCR, DT = CURRENT_TIMESTAMP
		  WHERE NUM = :P_NUM ROWS 20;
	UPDATED = ROW_COUNT;
	SUSPEND;
END`
}

// ddlhWidenDDL builds the ALTER for one varchar increment (+step) and the
// next length; returns ok=false when the target size is reached.
func ddlhWidenDDL(column string, cur, step, max int) (ddl string, next int, ok bool) {
	if cur >= max {
		return "", cur, false
	}
	next = cur + step
	if next > max {
		next = max
	}
	return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE VARCHAR(%d)", ddlhTable, column, next), next, true
}

// --- runner ------------------------------------------------------------------

type ddlhCounters struct {
	selShort   atomic.Int64
	selLong    atomic.Int64
	insRows    atomic.Int64
	updRows    atomic.Int64
	procSel    atomic.Int64
	procUpd    atomic.Int64
	longOpened atomic.Int64
	longCommit atomic.Int64
	longRoll   atomic.Int64
	ddlOK      atomic.Int64
	ddlInUse   atomic.Int64
	// ddlConflict: the widening rewrites row data and legitimately bumps into
	// concurrent updates/deadlocks — expected under load, retried next pass.
	ddlConflict atomic.Int64
	ddlOther    atomic.Int64
	workerErr   atomic.Int64
	errMu       sync.Mutex
	errSamples  map[string]int // up to 8 distinct truncated worker-error texts
}

// noteErr keeps up to 8 distinct truncated worker-error texts.
func (c *ddlhCounters) noteErr(err error) {
	if err == nil {
		return
	}
	msg := err.Error()
	if len(msg) > 160 {
		msg = msg[:160]
	}
	c.errMu.Lock()
	if c.errSamples == nil {
		c.errSamples = make(map[string]int)
	}
	if len(c.errSamples) < 8 {
		c.errSamples[msg]++
	}
	c.errMu.Unlock()
}

func (c *ddlhCounters) report(d time.Duration) {
	fmt.Printf(`
=== DDLLOAD SUMMARY ===
Duration: %v
Short-tx selects:        %d
Long-cursor selects:     %d
Long tx opened/commit/rb: %d / %d / %d
Insert rows:             %d
Update rows:             %d
Proc calls (sel/upd):    %d / %d
VARCHAR alters:          ok=%d inUse=%d dataConflict=%d other=%d
Worker errors:           %d
`, d, c.selShort.Load(), c.selLong.Load(), c.longOpened.Load(), c.longCommit.Load(),
		c.longRoll.Load(), c.insRows.Load(), c.updRows.Load(), c.procSel.Load(),
		c.procUpd.Load(), c.ddlOK.Load(), c.ddlInUse.Load(), c.ddlConflict.Load(),
		c.ddlOther.Load(), c.workerErr.Load())
	c.errMu.Lock()
	i := 0
	for msg, n := range c.errSamples {
		i++
		fmt.Printf("  worker err #%d (%dx): %s\n", i, n, msg)
	}
	c.errMu.Unlock()
}

func runDDLLoad(args []string) {
	fs := flag.NewFlagSet("ddlload", flag.ExitOnError)
	dsn := fs.String("dsn", "localhost/3050:C:\\data\\ddlload.fdb", "DSN host[/port]:dbpath (database is created)")
	user := fs.String("user", "SYSDBA", "DB user")
	pass := fs.String("pass", "masterkey", "DB password")
	workers := fs.Int("workers", 4, "parallel short-tx workers")
	longWorkers := fs.Int("long-workers", 2, "long-cursor-transaction workers")
	duration := fs.Duration("duration", 15*time.Minute, "total run time")
	rows := fs.Int("rows", 10000, "rows to fill before the load")
	varcharMax := fs.Int("varchar-max", 1024, "target size for widened varchars")
	varcharStep := fs.Int("varchar-step", 1, "VARCHAR width increment per ALTER")
	ddlEvery := fs.Duration("ddl-every", 200*time.Millisecond, "pause between varchar ALTERs")
	longHold := fs.Duration("long-hold", 30*time.Second, "how long a long-cursor transaction stays open")
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}
	host, port, dbPath := config.ParseDSN(*dsn)
	connStr := fmt.Sprintf("%s:%s@%s:%d/%s", *user, *pass, host, port, dbPath)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT)
		<-sig
		fmt.Println("\nddlload: interrupt received, shutting down")
		cancel()
	}()
	start := time.Now()

	// create the database when it does not exist (createdb driver variant,
	// registered next to the regular one by the emul package)
	if cdb, cerr := sql.Open("firebirdsql_createdb", connStr); cerr == nil {
		if _, e := cdb.ExecContext(ctx, "select 1 from rdb$database"); e == nil {
			fmt.Println("ddlload: database created")
		}
		cdb.Close()
	}

	admin, err := sql.Open("firebirdsql", connStr)
	if err != nil {
		fmt.Println("connect:", err)
		os.Exit(1)
	}
	defer admin.Close()
	if err := admin.PingContext(ctx); err != nil {
		fmt.Println("ping:", err)
		os.Exit(1)
	}

	// --- 1-2: table + fill ---------------------------------------------------
	setup := func(q string) { setupExec(ctx, admin, q) }
	setup(`DROP TABLE ` + ddlhTable)
	for _, sp := range []string{"SP_DLH_SEL", "SP_DLH_UPD"} {
		setup(`DROP PROCEDURE ` + sp)
	}
	for _, idx := range []string{"DLH_NUM", "DLH_CODE", "DLH_NAME", "DLH_DT",
		"DLH_NUM_DT", "DLH_DT_NUM", "DLH_QTY_NAME", "DLH_NAME_QTY", "DLH_QTY_CODE", "DLH_AMT_QTY"} {
		setup(`DROP INDEX ` + idx)
	}
	if err := noWaitTxExec(ctx, admin, ddlhTableDDL(), 10, time.Second); err != nil {
		fmt.Println("create table:", err)
		os.Exit(1)
	}
	for _, idx := range ddlhIndexDDLs() {
		if err := noWaitTxExec(ctx, admin, idx, 10, time.Second); err != nil {
			fmt.Println("create index:", err)
			os.Exit(1)
		}
	}
	if err := noWaitTxExec(ctx, admin, ddlhProcSelDDL(), 10, time.Second); err != nil {
		fmt.Println("create SP_DLH_SEL:", err)
		os.Exit(1)
	}
	if err := noWaitTxExec(ctx, admin, ddlhProcUpdDDL(), 10, time.Second); err != nil {
		fmt.Println("create SP_DLH_UPD:", err)
		os.Exit(1)
	}
	fillRows(ctx, admin, *rows)
	fmt.Printf("ddlload: table %s ready, %d rows, indexes and procedures created\n", ddlhTable, *rows)

	var c ddlhCounters
	stop := ctx.Done()

	// --- 5: parallel load -----------------------------------------------------
	var wg sync.WaitGroup
	shortModes := []string{"sel-index", "sel-scan", "write", "mixed"}
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			mode := shortModes[id%len(shortModes)]
			shortWorker(ctx, connStr, mode, &c, stop)
		}(w)
	}
	for w := 0; w < *longWorkers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			longWorker(ctx, connStr, id, *longHold, &c, stop)
		}(w)
	}

	// --- 6: varchar widening driver ------------------------------------------
	go func() {
		cols := []struct {
			name string
			cur  int
		}{
			{"NAME", 64}, {"DESCR", 256}, {"NUM", 32}, {"CODE", 16},
		}
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			col := &cols[i%len(cols)]
			i++
			ddl, next, ok := ddlhWidenDDL(col.name, col.cur, *varcharStep, *varcharMax)
			if !ok {
				return // all targets reached
			}
			// NOWAIT: a plain WAIT transaction would hang on the metadata
			// locks held by the long-cursor transactions forever — the point
			// is to retry and slip through between their holds.
			tx, txErr := admin.BeginTx(ctx, &sql.TxOptions{Isolation: emul.NoWaitIsolation})
			if txErr == nil {
				_, err = tx.ExecContext(ctx, ddl)
				if err == nil {
					err = tx.Commit()
				} else {
					_ = tx.Rollback()
				}
			} else {
				err = txErr
			}
			if err != nil {
				low := strings.ToLower(err.Error())
				if strings.Contains(low, "in use") || strings.Contains(low, "lock conflict") {
					c.ddlInUse.Add(1) // the metadata race we are hunting: retry next tick
					continue
				}
				if strings.Contains(low, "deadlock") || strings.Contains(low, "update conflicts") ||
					strings.Contains(low, "unsuccessful metadata update") {
					// widening rewrites row data and legitimately bumps into
					// concurrent updates; retried on the next pass
					c.ddlConflict.Add(1)
					continue
				}
				c.ddlOther.Add(1)
				fmt.Printf("ddlload: ALTER failed: %v\n  %s\n", err, oneLineCmd(ddl))
				continue
			}
			col.cur = next
			c.ddlOK.Add(1)
			time.Sleep(*ddlEvery)
		}
	}()

	ticker := time.NewTicker(10 * time.Second)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				fmt.Printf("ddlload: %v elapsed, sel=%d upd=%d long=%d ddl ok/inUse/other=%d/%d/%d\n",
					time.Since(start).Round(time.Second), c.selShort.Load()+c.selLong.Load(),
					c.updRows.Load(), c.longOpened.Load(), c.ddlOK.Load(), c.ddlInUse.Load(), c.ddlOther.Load())
			}
		}
	}()

	select {
	case <-stop:
	case <-time.After(*duration):
	}
	cancel()
	wg.Wait()
	c.report(time.Since(start))
}

// noWaitTxExec runs one statement in a NOWAIT transaction, retrying on
// metadata lock conflicts. A plain WAIT would hang forever when a leftover
// attachment from a killed run still holds the object.
func noWaitTxExec(ctx context.Context, db *sql.DB, q string, attempts int, pause time.Duration) error {
	for i := 0; i < attempts; i++ {
		tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: emul.NoWaitIsolation})
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, q); err != nil {
			_ = tx.Rollback()
			low := strings.ToLower(err.Error())
			if strings.Contains(low, "lock conflict") || strings.Contains(low, "no wait") ||
				strings.Contains(low, "in use") {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(pause):
				}
				continue
			}
			return err
		}
		return tx.Commit()
	}
	return fmt.Errorf("still locked after %d attempts: %s", attempts, oneLineCmd(q))
}

// setupExec drops/creates in NOWAIT transactions; absence errors are fine.
func setupExec(ctx context.Context, db *sql.DB, q string) {
	if err := noWaitTxExec(ctx, db, q, 5, time.Second); err != nil {
		low := strings.ToLower(err.Error())
		if strings.Contains(low, "does not exist") || strings.Contains(low, "not found") ||
			strings.Contains(low, "already exist") || strings.Contains(low, "is in use") {
			return
		}
		fmt.Printf("setup warning: %v\n  %s\n", err, oneLineCmd(q))
	}
}

// --- fill ---------------------------------------------------------------------

func fillRows(ctx context.Context, db *sql.DB, rows int) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		fmt.Println("fill begin:", err)
		return
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO `+ddlhTable+`
		(ID, NUM, CODE, NAME, DESCR, AMT, QTY, DT, FLAG)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		fmt.Println("fill prepare:", err)
		return
	}
	rng := rand.New(rand.NewSource(20261001))
	for i := 1; i <= rows; i++ {
		name := pickWord(rng) + "-" + pickWord(rng)
		descr := strings.Repeat(pickWord(rng)+" ", 8+rng.Intn(24))
		if _, err := stmt.ExecContext(ctx,
			i,
			fmt.Sprintf("NUM-%08d", i),
			fmt.Sprintf("C%04d", i%1000),
			name,
			descr,
			float64(rng.Intn(100000))/100.0,
			rng.Intn(500),
			time.Now().Add(-time.Duration(rng.Intn(365*24))*time.Hour),
			rng.Intn(2),
		); err != nil {
			fmt.Println("fill insert:", err)
			_ = tx.Rollback()
			return
		}
		if i%1000 == 0 {
			if err := tx.Commit(); err != nil {
				fmt.Println("fill commit:", err)
				return
			}
			tx, err = db.BeginTx(ctx, nil)
			if err != nil {
				fmt.Println("fill rebegin:", err)
				return
			}
			stmt, err = tx.PrepareContext(ctx, `INSERT INTO `+ddlhTable+`
				(ID, NUM, CODE, NAME, DESCR, AMT, QTY, DT, FLAG)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
			if err != nil {
				fmt.Println("fill reprepare:", err)
				return
			}
		}
	}
	if err := tx.Commit(); err != nil {
		fmt.Println("fill final commit:", err)
	}
}

// --- workers -------------------------------------------------------------------

func connectWorker(connStr string) *sql.DB {
	db, err := sql.Open("firebirdsql", connStr)
	if err != nil {
		return nil
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db
}

// shortWorker runs the point-4 mix in short transactions. Modes:
// sel-index (indexed selects + proc calls), sel-scan (non-indexed scans),
// write (inserts + random updates), mixed.
func shortWorker(ctx context.Context, connStr, mode string, c *ddlhCounters, stop <-chan struct{}) {
	db := connectWorker(connStr)
	if db == nil {
		c.workerErr.Add(1)
		return
	}
	defer db.Close()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		select {
		case <-stop:
			return
		default:
		}
		switch mode {
		case "sel-index":
			shortSelect(ctx, db, true, rng, c)
			shortProcCall(ctx, db, rng, c)
		case "sel-scan":
			shortSelect(ctx, db, false, rng, c)
		case "write":
			shortInsert(ctx, db, rng, c)
			shortUpdate(ctx, db, rng, c)
		default:
			switch rng.Intn(3) {
			case 0:
				shortSelect(ctx, db, rng.Intn(2) == 0, rng, c)
			case 1:
				shortUpdate(ctx, db, rng, c)
			default:
				shortProcCall(ctx, db, rng, c)
			}
		}
	}
}

func shortSelect(ctx context.Context, db *sql.DB, indexed bool, rng *rand.Rand, c *ddlhCounters) {
	var q string
	var arg any
	if indexed {
		q = `SELECT FIRST 20 ID, NUM, NAME, AMT FROM ` + ddlhTable + ` WHERE NUM = ? ORDER BY ID`
		arg = fmt.Sprintf("NUM-%08d", rng.Intn(10000)+1)
	} else {
		// CONTAINING prevents index use: a full scan over a growing table
		q = `SELECT FIRST 20 ID, NAME FROM ` + ddlhTable + ` WHERE DESCR CONTAINING ?`
		arg = "alpha"
	}
	rows, err := db.QueryContext(ctx, q, arg)
	if err != nil {
		c.workerErr.Add(1)
		return
	}
	n := 0
	for rows.Next() {
		var id int64
		var num, name string
		if indexed {
			var amt float64
			_ = rows.Scan(&id, &num, &name, &amt)
		} else {
			_ = rows.Scan(&id, &name)
		}
		n++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		c.workerErr.Add(1)
		c.noteErr(err)
		return
	}
	c.selShort.Add(int64(n))
}

func shortInsert(ctx context.Context, db *sql.DB, rng *rand.Rand, c *ddlhCounters) {
	_, err := db.ExecContext(ctx, `INSERT INTO `+ddlhTable+`
		(ID, NUM, CODE, NAME, DESCR, AMT, QTY, DT, FLAG)
		VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)`,
		time.Now().UnixNano(),
		fmt.Sprintf("INS-%08d", rng.Intn(100000000)),
		fmt.Sprintf("C%04d", rng.Intn(1000)),
		"w-"+pickWord(rng),
		strings.Repeat(pickWord(rng)+" ", 10+rng.Intn(20)),
		float64(rng.Intn(100000))/100.0,
		rng.Intn(500),
		rng.Intn(2),
	)
	if err != nil {
		c.workerErr.Add(1)
		return
	}
	c.insRows.Add(1)
}

func shortUpdate(ctx context.Context, db *sql.DB, rng *rand.Rand, c *ddlhCounters) {
	res, err := db.ExecContext(ctx,
		`UPDATE `+ddlhTable+` SET NAME = ?, DESCR = ? WHERE ID = ?`,
		"u-"+pickWord(rng),
		strings.Repeat(pickWord(rng)+" ", 10+rng.Intn(20)),
		rng.Intn(10000)+1,
	)
	if err != nil {
		c.workerErr.Add(1)
		return
	}
	if n, _ := res.RowsAffected(); n > 0 {
		c.updRows.Add(n)
	}
}

func shortProcCall(ctx context.Context, db *sql.DB, rng *rand.Rand, c *ddlhCounters) {
	rows, err := db.QueryContext(ctx, `SELECT ID, NUM, NAME, AMT FROM SP_DLH_SEL(?, ?)`,
		fmt.Sprintf("C%03d", rng.Intn(1000)), pickWord(rng))
	if err != nil {
		c.workerErr.Add(1)
		c.noteErr(err)
		return
	}
	n := 0
	for rows.Next() {
		var id int64
		var num, name string
		var amt float64
		_ = rows.Scan(&id, &num, &name, &amt)
		n++
	}
	rows.Close()
	c.procSel.Add(int64(n))

	rows2, err := db.QueryContext(ctx, `SELECT UPDATED FROM SP_DLH_UPD(?, ?)`,
		fmt.Sprintf("NUM-%08d", rng.Intn(10000)+1),
		strings.Repeat(pickWord(rng)+" ", 12))
	if err != nil {
		c.workerErr.Add(1)
		return
	}
	for rows2.Next() {
		var updated int
		_ = rows2.Scan(&updated)
		c.updRows.Add(int64(updated))
	}
	rows2.Close()
	c.procUpd.Add(1)
}

// longWorker keeps point 5.2: a transaction held open with an ACTIVE cursor —
// plain select and the selectable procedure with TYPE OF COLUMN parameters —
// fetching slowly, occasionally committing/rolling back.
func longWorker(ctx context.Context, connStr string, id int, hold time.Duration, c *ddlhCounters, stop <-chan struct{}) {
	db := connectWorker(connStr)
	if db == nil {
		c.workerErr.Add(1)
		return
	}
	defer db.Close()
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
	for {
		select {
		case <-stop:
			return
		default:
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			c.workerErr.Add(1)
			time.Sleep(time.Second)
			continue
		}
		c.longOpened.Add(1)
		var rows *sql.Rows
		var q string
		if rng.Intn(2) == 0 {
			q = `SELECT ID, NUM, NAME, DESCR FROM ` + ddlhTable + ` WHERE QTY > ? ORDER BY ID ROWS 500`
			rows, err = tx.QueryContext(ctx, q, rng.Intn(300))
		} else {
			q = `SELECT ID, NUM, NAME, AMT FROM SP_DLH_SEL(?, ?)`
			rows, err = tx.QueryContext(ctx, q, fmt.Sprintf("C%03d", rng.Intn(1000)), pickWord(rng))
		}
		if err != nil {
			_ = tx.Rollback()
			c.longRoll.Add(1)
			c.workerErr.Add(1)
			time.Sleep(time.Second)
			continue
		}
		// hold the cursor open, fetching one row every few seconds
		fetched := 0
		deadline := time.Now().Add(hold)
		for time.Now().Before(deadline) {
			select {
			case <-stop:
				// close the cursor immediately: on FB3 rows.Next() with a
				// cancelled context may hang in the driver fetch (same
				// cancel-not-honored family as the published LM report), and
				// a hung Next() blocked the whole shutdown
				rows.Close()
				_ = tx.Rollback()
				c.longRoll.Add(1)
				return
			case <-time.After(3 * time.Second):
			}
			if !rows.Next() {
				break
			}
			fetched++
		}
		rows.Close()
		c.selLong.Add(int64(fetched))
		if rng.Intn(5) == 0 {
			_ = tx.Rollback()
			c.longRoll.Add(1)
		} else {
			_ = tx.Commit()
			c.longCommit.Add(1)
		}
	}
}

func pickWord(rng *rand.Rand) string {
	return ddlhWordList[rng.Intn(len(ddlhWordList))]
}

func oneLineCmd(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\r\n", " "), "\n", " ")
}
