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

	"fb-loadgen/config"
	"fb-loadgen/session"
)

// Server serves the embedded control-plane UI and REST API.
type Server struct {
	manager *session.Manager
	mux     *http.ServeMux
	token   string
}

// New creates a UI server bound to the session manager.
func New(manager *session.Manager) *Server {
	return NewWithToken(manager, "")
}

// NewWithToken creates a UI server with an optional bearer token for mutating APIs.
func NewWithToken(manager *session.Manager, token string) *Server {
	s := &Server{
		manager: manager,
		mux:     http.NewServeMux(),
		token:   token,
	}
	s.routes()
	return s
}

func (s *Server) routes() {
	static, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic(err)
	}

	s.mux.HandleFunc("GET /api/config", s.handleConfig)
	s.mux.HandleFunc("PUT /api/config", s.auth(s.handleSaveConfig))
	s.mux.HandleFunc("POST /api/discover", s.auth(s.handleDiscover))
	s.mux.HandleFunc("GET /api/sessions", s.handleList)
	s.mux.HandleFunc("GET /api/fleet", s.handleFleet)
	s.mux.HandleFunc("PATCH /api/sessions/{id}", s.auth(s.handlePatch))
	s.mux.HandleFunc("POST /api/sessions/{id}/start", s.auth(s.handleStart))
	s.mux.HandleFunc("POST /api/sessions/{id}/pause", s.auth(s.handlePause))
	s.mux.HandleFunc("POST /api/sessions/{id}/resume", s.auth(s.handleResume))
	s.mux.HandleFunc("POST /api/sessions/{id}/stop", s.auth(s.handleStop))
	s.mux.HandleFunc("POST /api/sessions/{id}/validate", s.auth(s.handleValidate))
	s.mux.HandleFunc("DELETE /api/sessions/{id}", s.auth(s.handleRemove))
	s.mux.HandleFunc("GET /api/sessions/{id}/report", s.handleReportList)
	s.mux.HandleFunc("GET /api/sessions/{id}/report/{file}", s.handleReportDownload)
	s.mux.HandleFunc("POST /api/sessions/start-all", s.auth(s.handleStartAll))
	s.mux.HandleFunc("POST /api/sessions/stop-all", s.auth(s.handleStopAll))
	s.mux.HandleFunc("POST /api/sessions/pause-all", s.auth(s.handlePauseAll))
	s.mux.HandleFunc("POST /api/sessions/purge-missing", s.auth(s.handlePurgeMissing))
	s.mux.HandleFunc("POST /api/sessions/validate-all", s.auth(s.handleValidateAll))

	s.mux.Handle("/", http.FileServer(http.FS(static)))
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
	return s.mux
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

func (s *Server) handleStart(w http.ResponseWriter, r *http.Request) {
	snap, err := s.manager.Start(r.PathValue("id"))
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
	errs := s.manager.StartAll()
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
