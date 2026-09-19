package ui

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"fb-loadgen/config"
	"fb-loadgen/emul"
	"fb-loadgen/session"
)

// emulHandlers implement the oltp-emul endpoints for a managed session:
// the unit registry (business_ops) can be inspected and its weights
// re-balanced while the UI controls the rest of the run.

// openEmulDB connects to the session's database with a short timeout.
func openEmulDB(sc session.SessionConfig) (*sql.DB, error) {
	host, port, database := config.ParseDSN(sc.DSN)
	dsn := fmt.Sprintf("%s:%s@%s:%d/%s", sc.User, sc.Pass, host, port, database)
	db, err := sql.Open("firebirdsql", dsn)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func (s *Server) sessionOr404(w http.ResponseWriter, r *http.Request) (session.SessionConfig, bool) {
	sc, err := s.manager.GetConnectionInfo(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return session.SessionConfig{}, false
	}
	return sc, true
}

// handleEmulUnits lists the business_ops unit registry of the session's
// database: GET /api/sessions/{id}/emul/units
func (s *Server) handleEmulUnits(w http.ResponseWriter, r *http.Request) {
	sc, ok := s.sessionOr404(w, r)
	if !ok {
		return
	}
	db, err := openEmulDB(sc)
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Errorf("connect: %w", err))
		return
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := emul.SchemaGuard(ctx, db); err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	units, err := emul.LoadUnits(ctx, db)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"units": units})
}

// emulWeightsBody is the PUT /api/sessions/{id}/emul/weights payload:
// {"weights": {"SP_CLIENT_ORDER": 20, "SP_CANCEL_CLIENT_ORDER": 5}}
type emulWeightsBody struct {
	Weights map[string]int `json:"weights"`
}

// ---- live state ----

// handleEmulState serves the live oltp-emul run state of a session:
// GET /api/sessions/{id}/emul/state
func (s *Server) handleEmulState(w http.ResponseWriter, r *http.Request) {
	snap, err := s.manager.Get(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	if snap.Emul == nil {
		writeError(w, http.StatusConflict, fmt.Errorf("session has no live oltp-emul state (not running, or profile is not oltp-emul)"))
		return
	}
	writeJSON(w, http.StatusOK, snap.Emul)
}

// ---- provision jobs ----

// provisionJob tracks one async emul.Provision + emul.Fill execution.
type provisionJob struct {
	mu       sync.Mutex
	dsn      string
	ctx      context.Context
	cancel   context.CancelFunc
	done     bool
	ok       bool
	stage    string // create | scripts | settings | fill | done
	progress float64
	message  string
	started  time.Time
	finished time.Time
}

func (j *provisionJob) snapshot() map[string]any {
	j.mu.Lock()
	defer j.mu.Unlock()
	return map[string]any{
		"dsn":      j.dsn,
		"done":     j.done,
		"ok":       j.ok,
		"stage":    j.stage,
		"progress": j.progress,
		"message":  j.message,
		"started":  j.started,
		"finished": j.finished,
	}
}

func (j *provisionJob) progressf(stage string, frac float64, format string, args ...any) {
	j.mu.Lock()
	j.stage = stage
	j.progress = frac
	j.message = fmt.Sprintf(format, args...)
	j.mu.Unlock()
}

var emulJobs = struct {
	sync.Mutex
	m map[string]*provisionJob
}{m: make(map[string]*provisionJob)}

// handleEmulProvision starts an async provisioning job:
// POST /api/emul/provision {"dsn":"host/port:server-path","user","pass",
//
//	"workingMode","initDocs","pageSize"}
func (s *Server) handleEmulProvision(w http.ResponseWriter, r *http.Request) {
	var body struct {
		DSN         string `json:"dsn"`
		User        string `json:"user"`
		Pass        string `json:"pass"`
		WorkingMode string `json:"workingMode"`
		InitDocs    int    `json:"initDocs"`
		PageSize    int    `json:"pageSize"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.DSN == "" {
		writeError(w, http.StatusBadRequest, fmt.Errorf("dsn is required"))
		return
	}
	if body.User == "" {
		body.User = "SYSDBA"
	}
	if body.WorkingMode == "" {
		body.WorkingMode = "SMALL_01"
	}
	if body.PageSize == 0 {
		body.PageSize = 8192 // oltp-emul standard; 4096 cannot hold varchar(8192)
	}

	host, port, dbPath := config.ParseDSN(body.DSN)
	if root := s.manager.SharedConfig().EmulAllowDir; root != "" {
		absRoot, rerr := filepath.Abs(root)
		absPath, perr := filepath.Abs(dbPath)
		if rerr != nil || perr != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("invalid path"))
			return
		}
		if absPath != absRoot && !strings.HasPrefix(absPath, absRoot+string(filepath.Separator)) {
			writeError(w, http.StatusBadRequest,
				fmt.Errorf("provision path escapes --emul-allow-dir %q", root))
			return
		}
	}
	jobID := fmt.Sprintf("%x", time.Now().UnixNano())
	ctx, cancel := context.WithCancel(context.Background())
	job := &provisionJob{
		dsn:     body.DSN,
		ctx:     ctx,
		cancel:  cancel,
		stage:   "queued",
		started: time.Now(),
	}
	emulJobs.Lock()
	// one active job per DSN
	for _, other := range emulJobs.m {
		other.mu.Lock()
		active := !other.done && other.dsn == body.DSN
		other.mu.Unlock()
		if active {
			emulJobs.Unlock()
			writeError(w, http.StatusConflict, fmt.Errorf("a provision job for this DSN is already running"))
			return
		}
	}
	emulJobs.m[jobID] = job
	emulJobs.Unlock()

	cfg := emul.Config{
		Host: host, Port: fmt.Sprintf("%d", port),
		User: body.User, Password: body.Pass,
		DBPath: dbPath, PageSize: body.PageSize, WorkingMode: body.WorkingMode,
	}
	go func() {
		defer cancel()
		err := emul.Provision(ctx, cfg, func(format string, args ...any) {
			job.progressf("scripts", 0.4, format, args...)
		})
		if err == nil && body.InitDocs > 0 {
			var db *sql.DB
			db, err = sql.Open("firebirdsql", cfg.DSN())
			if err == nil {
				defer db.Close()
				err = emul.Fill(ctx, db, body.InitDocs, func(format string, args ...any) {
					job.progressf("fill", 0.9, format, args...)
				})
			}
		}
		job.mu.Lock()
		job.done, job.ok, job.stage = true, err == nil, "done"
		if err != nil {
			job.ok, job.message = false, err.Error()
			if ctx.Err() != nil {
				job.message = "cancelled"
			}
		} else {
			job.message = "provision complete"
		}
		job.progress, job.finished = 1, time.Now()
		job.mu.Unlock()

		if err == nil {
			if _, rerr := s.manager.RegisterDatabase(dbPath, body.User, body.Pass); rerr != nil {
				job.progressf("done", 1, "provisioned, but fleet registration failed: %v", rerr)
			}
		}
	}()

	writeJSON(w, http.StatusOK, map[string]any{"jobId": jobID})
}

// handleEmulProvisionStatus: GET /api/emul/provision/{jobId}
func (s *Server) handleEmulProvisionStatus(w http.ResponseWriter, r *http.Request) {
	emulJobs.Lock()
	job, ok := emulJobs.m[r.PathValue("jobId")]
	emulJobs.Unlock()
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("job not found"))
		return
	}
	writeJSON(w, http.StatusOK, job.snapshot())
}

// handleEmulProvisionCancel: DELETE /api/emul/provision/{jobId}
func (s *Server) handleEmulProvisionCancel(w http.ResponseWriter, r *http.Request) {
	emulJobs.Lock()
	job, ok := emulJobs.m[r.PathValue("jobId")]
	emulJobs.Unlock()
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("job not found"))
		return
	}
	job.cancel()
	writeJSON(w, http.StatusOK, job.snapshot())
}

// ---- workload profiles ----

// upstreamWorkloadProfiles are the working-mode profiles seeded by
// oltp_main_filling.sql; each keys settings like C_NUMBER_OF_AGENTS.
var upstreamWorkloadProfiles = []string{
	"DEBUG_01", "DEBUG_02", "DEBUG_03", "DEBUG_04", "DEBUG_1A",
	"SMALL_01", "SMALL_02", "SMALL_03",
	"MEDIUM_01", "MEDIUM_02", "MEDIUM_03",
	"LARGE_01", "LARGE_02", "LARGE_03",
	"HEAVY_01",
}

// handleEmulProfiles: GET /api/emul/profiles?session={id} — returns the
// workload profiles; with a session id it prefers the live list from that
// database's settings table.
func (s *Server) handleEmulProfiles(w http.ResponseWriter, r *http.Request) {
	if sessID := r.URL.Query().Get("session"); sessID != "" {
		sc, err := s.manager.GetConnectionInfo(sessID)
		if err == nil {
			if db, derr := openEmulDB(sc); derr == nil {
				ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
				rows, qerr := db.QueryContext(ctx, `select distinct working_mode from settings order by 1`)
				if qerr == nil {
					var live []string
					for rows.Next() {
						var wm string
						if rows.Scan(&wm) == nil && wm != "" {
							live = append(live, wm)
						}
					}
					rows.Close()
					cancel()
					db.Close()
					if len(live) > 0 {
						writeJSON(w, http.StatusOK, map[string]any{"profiles": live, "source": "database"})
						return
					}
				}
				cancel()
				db.Close()
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"profiles": upstreamWorkloadProfiles, "source": "fallback"})
}

// handleEmulWeights updates random_selection_weight values in business_ops
// (one transaction; 0 removes the unit from the rotation).
func (s *Server) handleEmulWeights(w http.ResponseWriter, r *http.Request) {
	sc, ok := s.sessionOr404(w, r)
	if !ok {
		return
	}
	var body emulWeightsBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if len(body.Weights) == 0 {
		writeError(w, http.StatusBadRequest, fmt.Errorf("weights must not be empty"))
		return
	}
	db, err := openEmulDB(sc)
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Errorf("connect: %w", err))
		return
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	for unit, weight := range body.Weights {
		if weight < 0 {
			writeError(w, http.StatusBadRequest, fmt.Errorf("weight for %s must be >= 0", unit))
			tx.Rollback()
			return
		}
		res, err := tx.ExecContext(ctx,
			`update business_ops set random_selection_weight = ? where upper(unit) = upper(?)`,
			weight, unit)
		if err != nil {
			tx.Rollback()
			writeError(w, http.StatusBadGateway, err)
			return
		}
		if affected, _ := res.RowsAffected(); affected == 0 {
			writeError(w, http.StatusNotFound, fmt.Errorf("unit %s not found in business_ops", unit))
			tx.Rollback()
			return
		}
	}
	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}

	units, err := emul.LoadUnits(ctx, db)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"units": units})
}
