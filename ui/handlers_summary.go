package ui

import (
	"net/http"
)

// handleSessionSummary serves GET /api/sessions/{id}/summary — the detailed
// run summary assembled from the live in-memory engine structures. Safe to
// call at any time: mid-run it reflects the current state, after completion
// it is the frozen final picture. Non-blocking by construction (no database
// access), so it never interferes with a running load or a stop in progress.
func (s *Server) handleSessionSummary(w http.ResponseWriter, r *http.Request) {
	sum, err := s.manager.Summary(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, sum)
}
