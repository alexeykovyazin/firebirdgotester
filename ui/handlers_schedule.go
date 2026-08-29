package ui

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"fb-loadgen/schedule"
	"fb-loadgen/session"
)

func (s *Server) engineOr503(w http.ResponseWriter) bool {
	if s.engine == nil {
		writeError(w, http.StatusServiceUnavailable, fmt.Errorf("scheduler not attached"))
		return false
	}
	return true
}

func (s *Server) handleScheduleList(w http.ResponseWriter, r *http.Request) {
	if !s.engineOr503(w) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"schedules": s.engine.List(),
	})
}

func (s *Server) handleScheduleCreate(w http.ResponseWriter, r *http.Request) {
	if !s.engineOr503(w) {
		return
	}
	var sched schedule.Schedule
	if err := json.NewDecoder(r.Body).Decode(&sched); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid body: %w", err))
		return
	}
	out, err := s.engine.Create(sched)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (s *Server) handleScheduleGet(w http.ResponseWriter, r *http.Request) {
	if !s.engineOr503(w) {
		return
	}
	sched, ok := s.engine.Get(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("schedule not found: %s", r.PathValue("id")))
		return
	}
	writeJSON(w, http.StatusOK, sched)
}

func (s *Server) handleScheduleUpdate(w http.ResponseWriter, r *http.Request) {
	if !s.engineOr503(w) {
		return
	}
	var patch map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid body: %w", err))
		return
	}
	id := r.PathValue("id")
	out, err := s.engine.Update(id, func(sc *schedule.Schedule) { applySchedulePatch(sc, patch) })
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

// applySchedulePatch overlays a JSON patch onto a schedule (partial merge:
// absent fields untouched).
func applySchedulePatch(sc *schedule.Schedule, patch map[string]interface{}) {
	if v, ok := patch["name"].(string); ok && v != "" {
		sc.Name = v
	}
	if v, ok := patch["enabled"].(bool); ok {
		sc.Enabled = v
	}
	if v, ok := patch["notifyUrl"].(string); ok {
		sc.NotifyURL = v
	}
	if t, ok := patch["trigger"].(map[string]interface{}); ok {
		if v, ok := t["type"].(string); ok && v != "" {
			sc.Trigger.Type = schedule.TriggerType(v)
		}
		if v, ok := t["at"].(string); ok {
			sc.Trigger.At = v
		}
		if v, ok := asInt(t["everyMin"]); ok {
			sc.Trigger.EveryMin = v
		}
		if v, ok := t["cron"].(string); ok {
			sc.Trigger.Cron = v
		}
		if v, ok := t["tz"].(string); ok {
			sc.Trigger.TZ = v
		}
	}
	if tg, ok := patch["targets"].(map[string]interface{}); ok {
		if v, ok := tg["all"].(bool); ok {
			sc.Targets.All = v
		}
		if raw, ok := tg["sessionIds"].([]interface{}); ok {
			var ids []string
			for _, item := range raw {
				if sv, ok := item.(string); ok && sv != "" {
					ids = append(ids, sv)
				}
			}
			if ids != nil {
				sc.Targets.SessionIDs = ids
			}
		}
	}
	if rs, ok := patch["run"].(map[string]interface{}); ok {
		if v, ok := asInt(rs["timeLimitMin"]); ok {
			sc.Run.TimeLimitMin = v
		}
		if ov, ok := rs["overrides"].(map[string]interface{}); ok {
			applyOverridePatch(&sc.Run.Overrides, ov)
		}
	}
	if p, ok := patch["policy"].(map[string]interface{}); ok {
		if v, ok := p["ifRunning"].(string); ok && v != "" {
			sc.Policy.IfRunning = v
		}
		if v, ok := p["ifBudgetFull"].(string); ok && v != "" {
			sc.Policy.IfBudgetFull = v
		}
		if v, ok := asInt(p["waitBudgetSec"]); ok {
			sc.Policy.WaitBudgetSec = v
		}
		if v, ok := asInt(p["staggerSec"]); ok {
			sc.Policy.StaggerSec = v
		}
		if v, ok := p["catchUp"].(bool); ok {
			sc.Policy.CatchUp = v
		}
	}
}

func applyOverridePatch(o *session.RunOverrides, ov map[string]interface{}) {
	if v, ok := ov["profile"].(string); ok && v != "" {
		o.Profile = &v
	}
	if v, ok := asInt(ov["thinkMs"]); ok {
		o.ThinkMs = &v
	}
	if v, ok := asInt(ov["txTimeout"]); ok {
		o.TxTimeout = &v
	}
	if v, ok := asInt(ov["spikeCycles"]); ok {
		o.SpikeCycles = &v
	}
	if v, ok := asInt(ov["spikeHold"]); ok {
		o.SpikeHold = &v
	}
}

func (s *Server) handleScheduleDelete(w http.ResponseWriter, r *http.Request) {
	if !s.engineOr503(w) {
		return
	}
	if !s.engine.Delete(r.PathValue("id")) {
		writeError(w, http.StatusNotFound, fmt.Errorf("schedule not found: %s", r.PathValue("id")))
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"ok":        true,
		"schedules": s.engine.List(),
	})
}

func (s *Server) handleScheduleTrigger(w http.ResponseWriter, r *http.Request) {
	if !s.engineOr503(w) {
		return
	}
	id := r.PathValue("id")
	if _, ok := s.engine.Get(id); !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("schedule not found: %s", id))
		return
	}
	if err := s.engine.TriggerNow(id); err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]interface{}{
		"triggered": true,
		"schedule":  id,
		"note":      "fire is asynchronous; poll GET /api/schedules/{id} (lastFire) or GET /api/schedules/{id}/runs",
	})
}

func (s *Server) handleScheduleRuns(w http.ResponseWriter, r *http.Request) {
	if !s.engineOr503(w) {
		return
	}
	id := r.PathValue("id")
	if _, ok := s.engine.Get(id); !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("schedule not found: %s", id))
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	runs := s.manager.History().List(session.ListFilter{ScheduleID: id, Limit: orDefault(limit, 50)})
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"schedule": id,
		"runs":     runs,
	})
}

func (s *Server) handleRunList(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	runs := s.manager.History().List(session.ListFilter{
		Limit:      orDefault(limit, 100),
		SessionID:  q.Get("session"),
		ScheduleID: q.Get("schedule"),
		Since:      q.Get("since"),
	})
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"runs": runs,
	})
}

func (s *Server) handleRunGet(w http.ResponseWriter, r *http.Request) {
	run, ok := s.manager.History().Get(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("run not found: %s", r.PathValue("id")))
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) handleRunCancel(w http.ResponseWriter, r *http.Request) {
	run, ok := s.manager.CancelRun(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusBadRequest, fmt.Errorf("run not found or already finished: %s", r.PathValue("id")))
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) handleSessionRuns(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	runs := s.manager.History().List(session.ListFilter{SessionID: r.PathValue("id"), Limit: orDefault(limit, 50)})
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"session": r.PathValue("id"),
		"runs":    runs,
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"ok":        true,
		"uptimeSec": int(time.Since(s.startedAt).Seconds()),
	})
}

func (s *Server) handleVersion(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"version":   Version,
		"goVersion": runtime.Version(),
		"os":        runtime.GOOS,
		"arch":      runtime.GOARCH,
	})
}

func orDefault(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}

// asInt coerces decoded JSON numbers to int.
func asInt(v interface{}) (int, bool) {
	switch t := v.(type) {
	case float64:
		return int(t), true
	case int:
		return t, true
	case int64:
		return int(t), true
	default:
		return 0, false
	}
}

// handleMetrics renders Prometheus text exposition for fleet, sessions and
// schedules without adding a client library dependency.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	fleet := s.manager.Fleet()
	snaps := s.manager.List()

	var b strings.Builder
	gauge := func(name, help string, value float64) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n%s %s\n", name, help, name, name, formatFloat(value))
	}

	gauge("fbloadgen_uptime_seconds", "Control plane uptime.", time.Since(s.startedAt).Seconds())
	gauge("fbloadgen_fleet_sessions_running", "Sessions currently running.", float64(fleet.Running))
	gauge("fbloadgen_fleet_sessions_paused", "Sessions currently paused.", float64(fleet.Paused))
	gauge("fbloadgen_fleet_sessions_idle", "Sessions idle.", float64(fleet.Idle))
	gauge("fbloadgen_fleet_sessions_failed", "Sessions failed.", float64(fleet.Failed))
	gauge("fbloadgen_fleet_sessions_completed", "Sessions completed.", float64(fleet.Completed))
	gauge("fbloadgen_fleet_total_conns", "Live worker connections across sessions.", float64(fleet.TotalConns))
	gauge("fbloadgen_fleet_total_tps", "Aggregate transactions per second.", fleet.TotalTPS)
	gauge("fbloadgen_fleet_budget_used", "Reserved connections against budget.", float64(fleet.BudgetUsed))
	gauge("fbloadgen_fleet_budget_limit", "Configured max-total-conns.", float64(fleet.BudgetLimit))

	for _, sn := range snaps {
		labels := fmt.Sprintf("{session=%q,db=%q}", sn.ID, sn.RelPath)
		fmt.Fprintf(&b, "# TYPE fbloadgen_session_tps gauge\nfbloadgen_session_tps%s %s\n", labels, formatFloat(sn.TPS))
		fmt.Fprintf(&b, "fbloadgen_session_conns%s %d\n", labels, sn.CurrentConns)
		fmt.Fprintf(&b, "fbloadgen_session_success_total%s %d\n", labels, sn.Success)
		fmt.Fprintf(&b, "fbloadgen_session_errors_total%s %d\n", labels, sn.Errors)
		fmt.Fprintf(&b, "fbloadgen_session_latency_p50_ms%s %d\n", labels, sn.LatencyP50)
		fmt.Fprintf(&b, "fbloadgen_session_latency_p95_ms%s %d\n", labels, sn.LatencyP95)
		fmt.Fprintf(&b, "fbloadgen_session_latency_p99_ms%s %d\n", labels, sn.LatencyP99)
	}

	if s.engine != nil {
		type schedInfo struct {
			id, name, next string
			nextUnix       float64
		}
		var infos []schedInfo
		for _, sc := range s.engine.List() {
			info := schedInfo{id: sc.ID, name: sc.Name}
			if t, err := time.Parse(time.RFC3339, sc.NextRunAt); err == nil {
				info.next = sc.NextRunAt
				info.nextUnix = float64(t.Unix())
			}
			infos = append(infos, info)
		}
		for _, info := range infos {
			labels := fmt.Sprintf("{schedule=%q,name=%q}", info.id, info.name)
			fmt.Fprintf(&b, "# TYPE fbloadgen_schedule_next_run_timestamp_seconds gauge\nfbloadgen_schedule_next_run_timestamp_seconds%s %s\n", labels, formatFloat(info.nextUnix))
		}
		for id, outcomes := range s.engine.FireCounters() {
			name := ""
			if sc, ok := s.engine.Get(id); ok {
				name = sc.Name
			}
			// Deterministic outcome order for stable output.
			keys := make([]string, 0, len(outcomes))
			for k := range outcomes {
				keys = append(keys, string(k))
			}
			sort.Strings(keys)
			for _, k := range keys {
				fmt.Fprintf(&b, "fbloadgen_schedule_fires_total{schedule=%q,name=%q,outcome=%q} %d\n",
					id, name, k, outcomes[schedule.FireOutcome(k)])
			}
		}
	}

	b.WriteString("# TYPE fbloadgen_build_info gauge\n")
	fmt.Fprintf(&b, "fbloadgen_build_info{version=%q,go=%q} 1\n", Version, runtime.Version())

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(b.String()))
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}
