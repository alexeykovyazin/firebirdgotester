package ui

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"fb-loadgen/config"
	"fb-loadgen/session"
)

// Every mutating emul route must reject unauthenticated requests when a
// token is configured; read routes must not require it (they fail with
// their own statuses instead). Guards against a future emul endpoint
// being registered without the auth wrapper (R14, IMPROVEMENTS_PLAN.md).
func TestEmulRoutesAuth(t *testing.T) {
	mgr := session.NewManager(&config.Config{MaxTotalConns: 200})
	srv := NewWithToken(mgr, "secret-token")

	mutating := []struct{ method, path string }{
		{http.MethodPut, "/api/sessions/abc/emul/weights"},
		{http.MethodPost, "/api/emul/provision"},
		{http.MethodDelete, "/api/emul/provision/abc"},
	}
	for _, r := range mutating {
		req := httptest.NewRequest(r.method, r.path, nil)
		rec := httptest.NewRecorder()
		srv.mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s %s: got %d, want 401 (mutating route missing auth wrapper)", r.method, r.path, rec.Code)
		}
	}

	reading := []struct{ method, path string }{
		{http.MethodGet, "/api/sessions/abc/emul/units"},
		{http.MethodGet, "/api/sessions/abc/emul/state"},
		{http.MethodGet, "/api/emul/provision/abc"},
		{http.MethodGet, "/api/emul/profiles"},
	}
	for _, r := range reading {
		req := httptest.NewRequest(r.method, r.path, nil)
		rec := httptest.NewRecorder()
		srv.mux.ServeHTTP(rec, req)
		if rec.Code == http.StatusUnauthorized {
			t.Errorf("%s %s: unexpected 401 — read route lost its authRead wrapper", r.method, r.path)
		}
	}
}
