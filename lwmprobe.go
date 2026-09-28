package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"fb-loadgen/config"

	firebirdsql "github.com/nakagami/firebirdsql"
)

// lwmprobe reproduces the periodic connection resets seen on HQbird FB5
// servers whose LightWeight Monitoring (LWMon) shared-memory region was
// created by a different (older) HQbird build:
//
//	LWMonMemory: inconsistent shared memory type/version; found 160/2:2, expected 160/2:3
//
// Minimal by design: no tables, no inserts, no transactions. Each cycle is
// one fresh wire attachment; -action picks the single statement executed
// (or none). An optional -hold-dsn keeps one long-lived attachment open for
// the whole run — point it at the OTHER server instance on the same machine
// to pin the LWMon region in that build's version.
func runLWMProbe(args []string) {
	fs := flag.NewFlagSet("lwmprobe", flag.ExitOnError)
	dsn := fs.String("dsn", "localhost/3055:", "target DSN host[/port]:server-side-path (hammered)")
	holdDSN := fs.String("hold-dsn", "", "optional DSN opened once and held for the whole run (pin LWMon region)")
	user := fs.String("user", "SYSDBA", "DB user")
	pass := fs.String("pass", "masterkey", "DB password")
	action := fs.String("action", "select", "per-cycle statement: attach | select (RDB$DATABASE) | mon (MON$ATTACHMENTS) | monmem (MON$MEMORY_USAGE aggregate, as emul Monitor) | drop (hard socket drop via fork intent, as connDrop axis)")
	workers := fs.Int("c", 1, "parallel workers")
	idle := fs.Int("idle", 0, "persistent background connections pinging SELECT 1 every 2s (model a worker pool)")
	rate := fs.Float64("rate", 5, "cycles per second per worker")
	dur := fs.Duration("d", 60*time.Second, "run duration")
	fblog := fs.String("fblog", "", "optional firebird.log path; counts new LWMonMemory lines during the run")
	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}
	if *rate <= 0 {
		fmt.Fprintln(os.Stderr, "lwmprobe: -rate must be > 0 (a zero or negative rate produces an invalid ticker interval)")
		os.Exit(2)
	}

	driverDSN := func(d string) string {
		host, port, database := config.ParseDSN(d)
		return fmt.Sprintf("%s:%s@%s:%d/%s", *user, *pass, host, port, database)
	}
	target := driverDSN(*dsn)

	var query string
	scanCols := 1
	switch *action {
	case "attach":
	case "select":
		query = "SELECT 1 FROM RDB$DATABASE"
	case "mon":
		query = "SELECT COUNT(*) FROM MON$ATTACHMENTS"
	case "monmem":
		// the exact aggregate emul/monitor.go runs every interval
		scanCols = 4
		query = `select
				  coalesce(max(case when mon$stat_group = 0 then mon$max_memory_used end), 0),
				  coalesce(max(case when mon$stat_group = 1 then mon$max_memory_used end), 0),
				  coalesce(max(case when mon$stat_group = 2 then mon$max_memory_used end), 0),
				  coalesce(max(case when mon$stat_group = 3 then mon$max_memory_used end), 0)
				from mon$memory_usage`
	case "drop":
	default:
		fmt.Fprintf(os.Stderr, "unknown -action %q (attach|select|mon|monmem|drop)\n", *action)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Printf("lwmprobe: target=%s action=%s c=%d rate=%.1f/s d=%s hold=%s\n",
		*dsn, *action, *workers, *rate, *dur, orDefault(*holdDSN, "-"))

	var hold *sql.DB
	if *holdDSN != "" {
		h, err := openPing(driverDSN(*holdDSN))
		if err != nil {
			fmt.Printf("HOLD FAILED: %v (region may not be pinned)\n", err)
		} else {
			hold = h
			defer h.Close()
			fmt.Printf("hold open: %s\n", *holdDSN)
		}
	}

	var ok, reset, other, cycles atomic.Int64
	var idleOK, idleReset atomic.Int64
	var lwmLines, lockLines, xnetLines atomic.Int64
	var mu sync.Mutex
	var lastReset time.Time
	var bursts int
	var samples []string

	classify := func(err error) string {
		if err == nil {
			return "ok"
		}
		msg := err.Error()
		for _, s := range []string{
			"forcibly closed", "wsarecv", "wsasend", "broken pipe",
			"connection was aborted", "unsuccessful network",
			"unable to complete network request", "read tcp", "write tcp",
		} {
			if strings.Contains(strings.ToLower(msg), s) {
				return "reset"
			}
		}
		return "other"
	}

	cycle := func() {
		cycles.Add(1)
		if *action == "drop" {
			// connDrop axis model: begin a transaction marked with the fork's
			// hard-drop intent; Commit never reaches the server - the driver
			// RSTs the socket, leaving server-side cleanup to run against the
			// (possibly broken) LWMon region.
			db, err := sql.Open("firebirdsql", target)
			derr := err
			if derr == nil {
				var tx *sql.Tx
				tx, derr = db.BeginTx(ctx, &sql.TxOptions{
					Isolation: firebirdsql.LevelHardDropBase + firebirdsql.IsoRC,
				})
				if derr == nil {
					derr = tx.Commit()
				}
				db.Close()
			}
			if derr == nil || isPlannedDrop(derr) {
				ok.Add(1)
			} else {
				other.Add(1)
				mu.Lock()
				if len(samples) < 6 {
					samples = append(samples, "drop: "+derr.Error())
					fmt.Printf("[%s] drop error: %v\n", time.Now().Format("15:04:05"), derr)
				}
				mu.Unlock()
			}
			return
		}
		db, err := sql.Open("firebirdsql", target)
		if err == nil {
			err = db.Ping()
			if err == nil && query != "" {
				dest := []any{new(int), new(int), new(int), new(int)}[:scanCols]
				err = db.QueryRow(query).Scan(dest...)
			}
			db.Close()
		}
		switch classify(err) {
		case "ok":
			ok.Add(1)
		case "reset":
			reset.Add(1)
			now := time.Now()
			mu.Lock()
			if lastReset.IsZero() || now.Sub(lastReset) > 2*time.Second {
				bursts++
				fmt.Printf("[%s] reset burst #%d begins\n", now.Format("15:04:05"), bursts)
			}
			lastReset = now
			if len(samples) < 3 {
				samples = append(samples, err.Error())
				fmt.Printf("[%s] reset sample: %v\n", now.Format("15:04:05"), err)
			}
			mu.Unlock()
		default:
			other.Add(1)
			mu.Lock()
			if len(samples) < 6 {
				samples = append(samples, "other: "+err.Error())
				fmt.Printf("[%s] other error: %v\n", time.Now().Format("15:04:05"), err)
			}
			mu.Unlock()
		}
	}

	done := make(chan struct{})

	// idle: persistent connections modeling a worker pool; only they reveal
	// whether a killing mechanism murders established attachments.
	for i := 0; i < *idle; i++ {
		go func(id int) {
			for {
				select {
				case <-ctx.Done():
					return
				case <-done:
					return
				default:
				}
				db, err := openPing(target)
				if err != nil {
					if classify(err) == "reset" {
						idleReset.Add(1)
						mu.Lock()
						if len(samples) < 6 {
							samples = append(samples, fmt.Sprintf("idle%d attach: %v", id, err))
							fmt.Printf("[%s] idle%d attach reset: %v\n", time.Now().Format("15:04:05"), id, err)
						}
						mu.Unlock()
					}
					select {
					case <-ctx.Done():
						return
					case <-done:
						return
					case <-time.After(time.Second):
					}
					continue
				}
				t := time.NewTicker(2 * time.Second)
			ping:
				for {
					select {
					case <-ctx.Done():
						t.Stop()
						db.Close()
						return
					case <-done:
						t.Stop()
						db.Close()
						return
					case <-t.C:
					}
					var v int
					var err error
					// ping inside a real transaction: gives MON$ scanners
					// transaction-level state to collect, like worker units do
					tx, terr := db.BeginTx(context.Background(), nil)
					if terr != nil {
						err = terr
					} else {
						err = tx.QueryRow("SELECT 1 FROM RDB$DATABASE").Scan(&v)
						if err == nil {
							err = tx.Commit()
						} else {
							tx.Rollback()
						}
					}
					if err == nil {
						idleOK.Add(1)
						continue
					}
					t.Stop()
					db.Close()
					if classify(err) == "reset" {
						idleReset.Add(1)
						mu.Lock()
						if len(samples) < 6 {
							samples = append(samples, fmt.Sprintf("idle%d ping: %v", id, err))
							fmt.Printf("[%s] idle%d ping reset: %v\n", time.Now().Format("15:04:05"), id, err)
						}
						mu.Unlock()
					} else {
						mu.Lock()
						if len(samples) < 6 {
							samples = append(samples, fmt.Sprintf("idle%d ping other: %v", id, err))
							fmt.Printf("[%s] idle%d ping other: %v\n", time.Now().Format("15:04:05"), id, err)
						}
						mu.Unlock()
					}
					time.Sleep(time.Second)
					break ping
				}
			}
		}(i)
	}

	interval := time.Duration(float64(time.Second) / *rate)
	for w := 0; w < *workers; w++ {
		go func() {
			t := time.NewTicker(interval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-done:
					return
				case <-t.C:
					cycle()
				}
			}
		}()
	}

	if *fblog != "" {
		go watchLog(ctx, *fblog, &lwmLines, &lockLines, &xnetLines)
	}

	timeout := time.After(*dur)
	progress := time.NewTicker(5 * time.Second)
	defer progress.Stop()
runloop:
	for {
		select {
		case <-ctx.Done():
			break runloop
		case <-timeout:
			break runloop
		case <-progress.C:
			if *idle > 0 {
				fmt.Printf("[%s] cycles=%d ok=%d reset=%d other=%d idleOK=%d idleRESET=%d lwmNew=%d lockNew=%d xnetNew=%d\n",
					time.Now().Format("15:04:05"), cycles.Load(), ok.Load(), reset.Load(), other.Load(),
					idleOK.Load(), idleReset.Load(), lwmLines.Load(), lockLines.Load(), xnetLines.Load())
			} else {
				fmt.Printf("[%s] cycles=%d ok=%d reset=%d other=%d lwmNew=%d lockNew=%d xnetNew=%d\n",
					time.Now().Format("15:04:05"), cycles.Load(), ok.Load(), reset.Load(), other.Load(),
					lwmLines.Load(), lockLines.Load(), xnetLines.Load())
			}
		}
	}
	close(done)
	if hold != nil {
		hold.Close()
	}

	mu.Lock()
	s := samples
	mu.Unlock()
	fmt.Printf("\n=== lwmprobe summary: %s action=%s\n", *dsn, *action)
	fmt.Printf("cycles=%d ok=%d (%.1f%%) reset=%d (%.1f%%) other=%d\n",
		cycles.Load(), ok.Load(), pct(&ok, &cycles), reset.Load(), pct(&reset, &cycles), other.Load())
	if *idle > 0 {
		fmt.Printf("idle pool: %d conns, pings ok=%d, RESET=%d\n", *idle, idleOK.Load(), idleReset.Load())
	}
	if *fblog != "" {
		fmt.Printf("new lines in %s during run: LWMonMemory=%d FatalLock=%d XNET=%d\n",
			*fblog, lwmLines.Load(), lockLines.Load(), xnetLines.Load())
	}
	if len(s) > 0 {
		fmt.Println("samples:")
		for _, e := range s {
			fmt.Println("  " + e)
		}
	}
}

func isPlannedDrop(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "planned") &&
		(strings.Contains(s, "bad connection") || strings.Contains(s, "limbo"))
}

func openPing(driverDSN string) (*sql.DB, error) {
	db, err := sql.Open("firebirdsql", driverDSN)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func watchLog(ctx context.Context, path string, n, lockErr, xnetErr *atomic.Int64) {
	var offset int64
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		st, err := f.Stat()
		if err != nil {
			f.Close()
			continue
		}
		if st.Size() < offset { // truncated/rotated
			offset = 0
		}
		if st.Size() > offset {
			buf := make([]byte, st.Size()-offset)
			f.ReadAt(buf, offset)
			offset = st.Size()
			s := string(buf)
			n.Add(int64(strings.Count(s, "LWMonMemory")))
			lockErr.Add(int64(strings.Count(s, "Fatal lock interface error")))
			xnetErr.Add(int64(strings.Count(s, "XNET error")))
		}
		f.Close()
	}
}

func pct(part, total *atomic.Int64) float64 {
	t := total.Load()
	if t == 0 {
		return 0
	}
	return 100 * float64(part.Load()) / float64(t)
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
