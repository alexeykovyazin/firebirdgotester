package ui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"fb-loadgen/config"
	"fb-loadgen/session"
	"fb-loadgen/summary"
)

// The summary endpoint is a read route: no token required, 404 for an
// unknown session, and the payload must be the full summary JSON shape
// (follow-up plan 2026-09-29 step 4).
func TestSessionSummaryRoute(t *testing.T) {
	mgr := session.NewManager(&config.Config{MaxTotalConns: 200})
	srv := NewWithToken(mgr, "secret-token")

	req := httptest.NewRequest(http.MethodGet, "/api/sessions/nope/summary", nil)
	rec := httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown session: got %d, want 404", rec.Code)
	}

	// With a real (never-started) session the endpoint must answer 200 with
	// zero values and marshal cleanly — the JSON is served verbatim.
	snap, err := mgr.RegisterDatabase("C:/dbs/summary_test.fdb", "SYSDBA", "masterkey")
	if err != nil {
		t.Fatalf("RegisterDatabase: %v", err)
	}
	id := snap.ID

	req = httptest.NewRequest(http.MethodGet, "/api/sessions/"+id+"/summary", nil)
	rec = httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("summary: got %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}

	var sum summary.Summary
	if err := json.Unmarshal(rec.Body.Bytes(), &sum); err != nil {
		t.Fatalf("payload is not a Summary: %v", err)
	}
	if sum.GeneratedAt.IsZero() {
		t.Fatal("GeneratedAt not set")
	}
}
