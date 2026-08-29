package ui

import (
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"fb-loadgen/config"
	"fb-loadgen/schedule"
	"fb-loadgen/session"
)

// Version is reported by GET /api/version.
const Version = "1.2.0"

// Server serves the embedded control-plane UI and REST API.
type Server struct {
	manager    *session.Manager
	mux        *http.ServeMux
	token      string
	authAll    bool
	corsOrigin string
	apiOnly    bool
	engine     *schedule.Engine
	startedAt  time.Time
}

// New creates a UI server bound to the session manager.
func New(manager *session.Manager) *Server {
	return NewWithToken(manager, "")
}

// NewWithToken creates a UI server with an optional bearer token for mutating APIs.
func NewWithToken(manager *session.Manager, token string) *Server {
	s := &Server{
		manager:   manager,
		mux:       http.NewServeMux(),
		token:     token,
		startedAt: time.Now(),
	}
	s.routes()
	return s
}

// SetScheduleEngine attaches the schedule engine (enables /api/schedules*).
func (s *Server) SetScheduleEngine(e *schedule.Engine) {
	s.engine = e
}

// SetAuthAll requires the bearer token for read endpoints too (except
// /api/health and /metrics, which monitoring must reach).
func (s *Server) SetAuthAll(v bool) {
	s.authAll = v
}

// SetCORSOrigin enables cross-origin browser clients ("" disables CORS).
func (s *Server) SetCORSOrigin(origin string) {
	s.corsOrigin = origin
}

// SetAPIOnly serves the REST API without the embedded SPA.
func (s *Server) SetAPIOnly(v bool) {
	s.apiOnly = v
}

func (s *Server) routes() {
	static, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic(err)
	}
	staticServer := http.FileServer(http.FS(static))

	s.mux.HandleFunc("GET /api/config", s.authRead(s.handleConfig))
	s.mux.HandleFunc("PUT /api/config", s.auth(s.handleSaveConfig))
	s.mux.HandleFunc("POST /api/discover", s.auth(s.handleDiscover))
	s.mux.HandleFunc("GET /api/sessions", s.authRead(s.handleList))
	s.mux.HandleFunc("GET /api/sessions/{id}", s.authRead(s.handleGetSession))
	s.mux.HandleFunc("GET /api/fleet", s.authRead(s.handleFleet))
	s.mux.HandleFunc("PATCH /api/sessions/{id}", s.auth(s.handlePatch))
	s.mux.HandleFunc("POST /api/sessions/{id}/start", s.auth(s.handleStart))
	s.mux.HandleFunc("POST /api/sessions/{id}/pause", s.auth(s.handlePause))
	s.mux.HandleFunc("POST /api/sessions/{id}/resume", s.auth(s.handleResume))
	s.mux.HandleFunc("POST /api/sessions/{id}/stop", s.auth(s.handleStop))
	s.mux.HandleFunc("POST /api/sessions/{id}/validate", s.auth(s.handleValidate))
	s.mux.HandleFunc("DELETE /api/sessions/{id}", s.auth(s.handleRemove))
	s.mux.HandleFunc("GET /api/sessions/{id}/report", s.authRead(s.handleReportList))
	s.mux.HandleFunc("GET /api/sessions/{id}/report/{file}", s.authRead(s.handleReportDownload))
	s.mux.HandleFunc("POST /api/sessions/start-all", s.auth(s.handleStartAll))
	s.mux.HandleFunc("POST /api/sessions/stop-all", s.auth(s.handleStopAll))
	s.mux.HandleFunc("POST /api/sessions/pause-all", s.auth(s.handlePauseAll))
	s.mux.HandleFunc("POST /api/sessions/purge-missing", s.auth(s.handlePurgeMissing))
	s.mux.HandleFunc("POST /api/sessions/validate-all", s.auth(s.handleValidateAll))

	// Schedules
	s.mux.HandleFunc("GET /api/schedules", s.authRead(s.handleScheduleList))
	s.mux.HandleFunc("POST /api/schedules", s.auth(s.handleScheduleCreate))
	s.mux.HandleFunc("GET /api/schedules/{id}", s.authRead(s.handleScheduleGet))
	s.mux.HandleFunc("PATCH /api/schedules/{id}", s.auth(s.handleScheduleUpdate))
	s.mux.HandleFunc("DELETE /api/schedules/{id}", s.auth(s.handleScheduleDelete))
	s.mux.HandleFunc("POST /api/schedules/{id}/trigger", s.auth(s.handleScheduleTrigger))
	s.mux.HandleFunc("GET /api/schedules/{id}/runs", s.authRead(s.handleScheduleRuns))

	// Run history
	s.mux.HandleFunc("GET /api/runs", s.authRead(s.handleRunList))
	s.mux.HandleFunc("GET /api/runs/{id}", s.authRead(s.handleRunGet))
	s.mux.HandleFunc("POST /api/runs/{id}/cancel", s.auth(s.handleRunCancel))
	s.mux.HandleFunc("GET /api/sessions/{id}/runs", s.authRead(s.handleSessionRuns))

	// Ops & observability
	s.mux.HandleFunc("GET /api/health", s.handleHealth)
	s.mux.HandleFunc("GET /api/version", s.handleVersion)
	s.mux.HandleFunc("GET /metrics", s.handleMetrics)

	// apiOnly is consulted per-request so SetAPIOnly can be called after
	// construction.
	s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if s.apiOnly {
			writeError(w, http.StatusNotFound, fmt.Errorf("UI disabled (--api-only); see /api/ and /metrics"))
			return
		}
		staticServer.ServeHTTP(w, r)
	})
}

// authRead protects GET endpoints when --ui-auth-all is set. It re-checks
// the flag per request so SetAuthAll can be called after construction.
func (s *Server) authRead(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.authAll && s.token != "" {
			h := r.Header.Get("Authorization")
			if h != "Bearer "+s.token {
				writeError(w, http.StatusUnauthorized, fmt.Errorf("unauthorized"))
				return
			}
		}
		next(w, r)
	}
}

func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.token != "" {
			h := r.Header.Get("Authorization")
			want := "Bearer " + s.token
			if h != want {
				writeError(w, http.StatusUnauthorized, fmt.Errorf("unauthorized"))
				return
			}
		}
		next(w, r)
	}
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	return s.withCORS(s.mux)
}

func (s *Server) withCORS(next http.Handler) http.Handler {
	if s.corsOrigin == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", s.corsOrigin)
		w.Header().Add("Vary", "Origin")
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ListenAndServe starts the HTTP server.
func (s *Server) ListenAndServe(addr string) error {
	fmt.Printf("Web UI listening on http://%s\n", normalizeAddr(addr))
	return http.ListenAndServe(addr, s.mux)
}

func normalizeAddr(addr string) string {
	if strings.HasPrefix(addr, ":") {
		return "localhost" + addr
	}
	return addr
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	cfg := s.manager.SharedConfig()
	settings := s.manager.ConnectionSettings()
	out := settings.RedactedForAPI()
	out["settingsPath"] = s.manager.SettingsPath()
	out["defaultProfile"] = valueOr(cfg.Profile, "write-heavy")
	out["defaultConnMin"] = cfg.ConnMin
	out["defaultConnMax"] = cfg.ConnMax
	out["defaultWarmup"] = cfg.Warmup
	out["defaultMain"] = cfg.Main
	out["defaultCooldown"] = cfg.Cooldown
	out["authRequired"] = s.token != ""
	out["hasRunning"] = s.manager.HasRunningSessions()
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleSaveConfig(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Host              string `json:"host"`
		Port              int    `json:"port"`
		User              string `json:"user"`
		Pass              string `json:"pass"`
		DiscoverDir       string `json:"discoverDir"`
		DiscoverMask      string `json:"discoverMask"`
		DiscoverRecursive *bool  `json:"discoverRecursive"`
		MaxTotalConns     int    `json:"maxTotalConns"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	prev := s.manager.ConnectionSettings()
	settings := config.UISettings{
		Host:              body.Host,
		Port:              body.Port,
		User:              body.User,
		Pass:              body.Pass,
		DiscoverDir:       body.DiscoverDir,
		DiscoverMask:      body.DiscoverMask,
		DiscoverRecursive: prev.DiscoverRecursive,
		MaxTotalConns:     body.MaxTotalConns,
	}
	if body.DiscoverRecursive != nil {
		settings.DiscoverRecursive = *body.DiscoverRecursive
	}
	if settings.DiscoverDir == "" {
		settings.DiscoverDir = prev.DiscoverDir
	}
	if settings.DiscoverMask == "" {
		settings.DiscoverMask = "*.fdb"
	}
	warnRunning := s.manager.HasRunningSessions()
	if err := s.manager.UpdateConnectionSettings(settings, true); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"ok":          true,
		"settings":    s.manager.ConnectionSettings().RedactedForAPI(),
		"path":        s.manager.SettingsPath(),
		"warnRunning": warnRunning,
	})
}

func valueOr(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func (s *Server) handleDiscover(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Folder      string `json:"folder"`
		Mask        string `json:"mask"`
		DiscoverDir string `json:"discoverDir"`
		Recursive   *bool  `json:"recursive"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&body)
	}
	rec := true
	if body.Recursive != nil {
		rec = *body.Recursive
	} else {
		body.Recursive = &rec
	}
	snaps, err := s.manager.Discover(body.Folder, body.Mask, body.Recursive, body.DiscoverDir)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"sessions":    snaps,
		"count":       len(snaps),
		"discoverDir": s.manager.DiscoverDir(),
		"recursive":   rec,
	})
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"sessions": s.manager.List(),
		"fleet":    s.manager.Fleet(),
	})
}

func (s *Server) handleGetSession(w http.ResponseWriter, r *http.Request) {
	snap, err := s.manager.Get(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) handleFleet(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.manager.Fleet())
}

func (s *Server) handlePatch(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var patch map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	snap, err := s.manager.Patch(id, patch)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

// decodeStart reads {"timeLimitMin": N, "wait": bool}; absent/empty body
// means timeLimitMin=0 (no limit), wait=true (synchronous start).
func decodeStart(r *http.Request) (timeLimitMin int, wait bool, err error) {
	wait = true
	if r.Body == nil {
		return 0, wait, nil
	}
	var body struct {
		TimeLimitMin int   `json:"timeLimitMin"`
		Wait         *bool `json:"wait"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		if err == io.EOF {
			return 0, wait, nil
		}
		return 0, wait, fmt.Errorf("invalid body: %w", err)
	}
	if body.TimeLimitMin < 0 {
		return 0, wait, fmt.Errorf("timeLimitMin must be >= 0")
	}
	if body.Wait != nil {
		wait = *body.Wait
	}
	return body.TimeLimitMin, wait, nil
}

// decodeTimeLimit reads an optional {"timeLimitMin": N} body; absent or empty
// body means 0 (no limit).
func decodeTimeLimit(r *http.Request) (int, error) {
	tlm, _, err := decodeStart(r)
	return tlm, err
}

func (s *Server) handleStart(w http.ResponseWriter, r *http.Request) {
	timeLimitMin, wait, err := decodeStart(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	id := r.PathValue("id")
	if !wait {
		// Fire-and-forget: poll GET /api/sessions/{id} for the outcome.
		go func() { _, _ = s.manager.Start(id, timeLimitMin) }()
		snap, err := s.manager.Get(id)
		if err != nil {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]interface{}{
			"queued":  true,
			"session": snap,
		})
		return
	}
	snap, err := s.manager.Start(id, timeLimitMin)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) handlePause(w http.ResponseWriter, r *http.Request) {
	snap, err := s.manager.Pause(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) handleResume(w http.ResponseWriter, r *http.Request) {
	snap, err := s.manager.Resume(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) handleStop(w http.ResponseWriter, r *http.Request) {
	snap, err := s.manager.Stop(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) handleValidate(w http.ResponseWriter, r *http.Request) {
	snap, err := s.manager.ValidateSession(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) handleRemove(w http.ResponseWriter, r *http.Request) {
	if err := s.manager.Remove(r.PathValue("id")); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"ok":       true,
		"sessions": s.manager.List(),
	})
}

func (s *Server) handleReportList(w http.ResponseWriter, r *http.Request) {
	dir, files, err := s.manager.ListReports(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"dir":   dir,
		"files": files,
	})
}

func (s *Server) handleReportDownload(w http.ResponseWriter, r *http.Request) {
	path, err := s.manager.ResolveReportFile(r.PathValue("id"), r.PathValue("file"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	f, err := os.Open(path)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename="+filepath.Base(path))
	_, _ = io.Copy(w, f)
}

func (s *Server) handleStartAll(w http.ResponseWriter, r *http.Request) {
	timeLimitMin, err := decodeTimeLimit(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	errs := s.manager.StartAll(timeLimitMin)
	msg := make([]string, 0, len(errs))
	for _, e := range errs {
		msg = append(msg, e.Error())
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"sessions": s.manager.List(),
		"errors":   msg,
	})
}

func (s *Server) handleStopAll(w http.ResponseWriter, r *http.Request) {
	errs := s.manager.StopAll()
	msg := make([]string, 0, len(errs))
	for _, e := range errs {
		msg = append(msg, e.Error())
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"sessions": s.manager.List(),
		"errors":   msg,
	})
}

func (s *Server) handlePauseAll(w http.ResponseWriter, r *http.Request) {
	errs := s.manager.PauseAll()
	msg := make([]string, 0, len(errs))
	for _, e := range errs {
		msg = append(msg, e.Error())
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"sessions": s.manager.List(),
		"errors":   msg,
	})
}

func (s *Server) handlePurgeMissing(w http.ResponseWriter, r *http.Request) {
	n := s.manager.PurgeMissing()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"purged":   n,
		"sessions": s.manager.List(),
	})
}

func (s *Server) handleValidateAll(w http.ResponseWriter, r *http.Request) {
	errs := s.manager.ValidateAll()
	msg := make([]string, 0, len(errs))
	for _, e := range errs {
		msg = append(msg, e.Error())
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"sessions": s.manager.List(),
		"errors":   msg,
	})
}
