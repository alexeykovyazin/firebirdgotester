package ui

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
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
