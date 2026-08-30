# Improvements Plan — API-first run & scheduling

Status: **implemented, 2026-08-29** (all phases; Phase 5's secure credential storage deferred — see "Implementation status" at the end).
Goal: make `fb-loadgen` fully drivable **without the UI** — an API for *running* load tests (exists today) and for **scheduling** them (missing), plus the run history and unattended-operation features that scheduling implies. The UI stays and gets parity tabs for the new features.

---

## 1. Review — current state

### What already exists

| Area | State |
|---|---|
| Control plane | `fb-loadgen --ui` serves an embedded SPA (`ui/static`) + REST API on `127.0.0.1:9000` (`--ui-addr`), optional bearer token for mutations (`--ui-token`). |
| Run API | Complete interactive control: start (with `timeLimitMin`), stop, pause, resume, validate, start-all/stop-all/pause-all, per-session PATCH, config GET/PUT, discover, report list/download (`ui/server.go`, `API.md`). |
| Session engine | In-memory `session.Manager`: status machine `Idle → Starting → Running/Paused → Stopping → Completed/Failed`, conn-budget reservation with live rebalance, time-limited runs via phase scaling (`session/manager.go`). |
| Persistence | One JSON file (`fb-loadgen.ui.json`): connection settings + per-DB run prefs. Atomic write via temp+rename (`config/persist.go`). |
| Load engine | `ramp.Scheduler` (phases, live `SetConnectionMax`, pause gate), `worker`, `profile` (write-heavy/read-heavy/spike), `ops` (SQL workload), `metrics` (collector + text/CSV reports), `errlog`. |

### Gaps versus the goal

1. **No scheduling.** Runs start only on a manual API/UI call, immediately. No one-off ("tonight 02:00"), no recurring ("every 6h"), no interval schedules, and nothing survives a process restart.
2. **No run history API.** After a run completes, the only artifacts are report files per session (`GET /api/sessions/{id}/report`). There is no queryable record of *runs* (who started it, when, with what config, what outcome), which schedules need to report "last run" and which operators need for auditing.
3. **No unattended-operation hooks.** No webhook/notification on completion or failure, no Prometheus endpoint — for scheduled overnight runs somebody or something must notice failures.
4. **`start-all` fires everything at once.** No staggering, no concurrency cap, no budget-aware queueing: with a full budget, later starts fail with `budget exceeded` instead of waiting.
5. **API ergonomics.** GETs are unauthenticated even when `--ui-token` is set; no request logging; no CORS (cross-origin API clients can't call it from a browser tool); no OpenAPI spec; `Start` is synchronous in the request path (schema gate + cache load can take seconds — long for HTTP clients, though acceptable and documented).
6. **Security notes.** DB password stored in plaintext in `fb-loadgen.ui.json`; token passed on argv (visible in process listings); no TLS story (fine for localhost, not for LAN use).
7. **Restart resilience.** Nothing marks "a run was active when the process died"; scheduled runs can't resume and there is no missed-run policy.

---

## 2. Design

### 2.1 New concepts

```
Schedule ──fires──▶ Run ──contains──▶ per-session outcomes (SessionRun)
   (when/what)       (one execution)      (status, metrics, reportDir)
```

- **Schedule** — persisted rule: *when* to fire (trigger) and *what* to run (targets + run spec), plus policy (overlap, budget, staggering).
- **Run** — one execution of a schedule or a manual bulk action. Recorded in history with per-session outcomes and report dirs.

### 2.2 New package: `schedule`

Follows the existing pattern of small packages with a manager (`session.Manager`). No changes to the load engine.

```go
type Trigger struct {
    Type     string `json:"type"`               // "once" | "interval" | "cron"
    At       string `json:"at,omitempty"`       // RFC3339, for "once"
    EveryMin int    `json:"everyMin,omitempty"` // for "interval"
    Cron     string `json:"cron,omitempty"`     // 5-field, for "cron"
    TZ       string `json:"tz,omitempty"`       // IANA name; "" = local
}

type Targets struct {
    All        bool     `json:"all,omitempty"`        // all discovered, non-missing
    SessionIDs []string `json:"sessionIds,omitempty"` // explicit subset
}

type Policy struct {
    IfRunning    string `json:"ifRunning"`              // "skip" (default) | "stopAndRun"
    IfBudgetFull string `json:"ifBudgetFull"`           // "wait" (default, with timeout) | "skip"
    WaitBudgetSec int   `json:"waitBudgetSec"`          // for "wait"; default 300
    StaggerSec   int    `json:"staggerSec"`             // delay between session starts; default 2
    CatchUp      bool   `json:"catchUp"`                // run a missed fire (process was down); default false
}

type Schedule struct {
    ID        string    `json:"id"`     // "sch_" + random 8 hex
    Name      string    `json:"name"`
    Enabled   bool      `json:"enabled"`
    Trigger   Trigger   `json:"trigger"`
    Targets   Targets   `json:"targets"`
    Run       RunSpec   `json:"run"`                  // {timeLimitMin, overrides map[string]any}
    Policy    Policy    `json:"policy"`
    CreatedAt string    `json:"createdAt"`
    NextRunAt string    `json:"nextRunAt,omitempty"`  // recomputed + persisted after each fire
    LastRun   *RunRef   `json:"lastRun,omitempty"`    // {runId, finishedAt, outcome}
}
```

**Engine**: one goroutine, 1-second tick; for each enabled schedule whose `nextRunAt <= now`: re-resolve targets against current sessions (IDs are stable SHA-256 of AbsPath), apply policy (skip if any target running / budget full), start sessions in background goroutines with `StaggerSec` spacing, create a `Run` record, compute and persist the next `nextRunAt`. Cron math via `github.com/robfig/cron/v3`'s parser/schedule (mature DST handling) — or `github.com/gorhill/cronexpr` if a parse-only dependency is preferred. Injectable `now func() time.Time` for tests.

**Persistence**: `fb-loadgen.schedules.json` next to the settings file, same atomic-write helper as `config.SaveUISettings`. Missed fires while the process was down: `catchUp=false` → skipped with a log line (and marked in history as `skipped`); a `once` schedule that missed while down is kept and marked `missed` (never auto-fired at boot unless `catchUp=true`).

### 2.3 Run history

New `run` record type in `session` (or its own small `history` package):

```json
{
  "id": "run_20260829T020003_a1b2c3d4",
  "origin": "schedule" | "manual",
  "scheduleId": "sch_1a2b3c4d",
  "startedAt": "2026-08-29T02:00:03+03:00",
  "finishedAt": "2026-08-29T02:30:05+03:00",
  "outcome": "ok" | "partial" | "failed" | "cancelled",
  "sessions": [
    {"id": "365599cb03070b90", "relPath": "db1/EMPLOYEE.FDB",
     "status": "Completed", "lastError": "", "reportDir": "reports/...",
     "tps": 11.4, "success": 43120, "errors": 2}
  ]
}
```

- Built automatically: `watchCompletion` / `Stop` already observe every session transition — hook record updates there.
- Persisted to `fb-loadgen.runs.json` (append, atomic rewrite; retention: keep last N=500 or M days, configurable, default on).
- `GET /api/runs` / `GET /api/runs/{id}` make results queryable without touching the filesystem.

### 2.4 API surface (v1 additions)

All under the existing server and token rules; new mutating routes require the bearer token like today.

| Method & path | Purpose |
|---|---|
| `GET /api/schedules` | List schedules with `nextRunAt`, `lastRun`, `enabled` |
| `POST /api/schedules` | Create; validates trigger (cron parse, `at` in future), targets resolve |
| `GET /api/schedules/{id}` | One schedule |
| `PATCH /api/schedules/{id}` | Edit fields; `{"enabled": false}` disables |
| `DELETE /api/schedules/{id}` | Delete (does not affect an in-flight run) |
| `POST /api/schedules/{id}/trigger` | Fire now, ignoring the clock; returns the run record |
| `GET /api/schedules/{id}/runs` | History for one schedule |
| `GET /api/runs?limit=&session=&since=` | Recent runs, filterable |
| `GET /api/runs/{id}` | One run with per-session outcomes |
| `POST /api/runs/{id}/cancel` | Stop all sessions belonging to that run |
| `GET /api/health` | Liveness for monitors/task schedulers |
| `GET /api/version` | Version/commit for support |

Example — nightly 30-minute write-heavy on two databases:

```bash
curl -s -X POST $BASE/api/schedules -H "Content-Type: application/json" -d '{
  "name": "nightly-30min",
  "enabled": true,
  "trigger": {"type": "cron", "cron": "0 2 * * *", "tz": "Europe/Moscow"},
  "targets": {"sessionIds": ["365599cb03070b90", "9d21aa01bb22cc33"]},
  "run": {"timeLimitMin": 30, "overrides": {"profile": "write-heavy"}},
  "policy": {"ifRunning": "skip", "staggerSec": 5}
}'
```

### 2.5 Status machine & budget interaction

- No new per-session status needed: a scheduled start goes through the same `Idle/Completed/Failed → Starting → Running` path. "Scheduled" is a property of the *schedule*, not the session (keeps the UI table honest).
- Budget: the schedule engine pre-checks `reserveBudget` headroom; on `IfBudgetFull=wait` it retries each tick up to `WaitBudgetSec`, then marks the run `failed` with a clear reason. `StaggerSec` avoids thundering-herd starts on `all` targets.
- Overlap: default `IfRunning=skip` — if any target session is still `Running`/`Starting`/`Paused` at fire time, the fire is skipped and recorded (outcome `skipped`, visible in history). This mirrors what an operator would do.

---

## 3. Roadmap

### Phase 1 — Run history (foundation, low risk)
1. `Run`/`SessionRun` types + in-memory registry in `session.Manager`, populated from existing `watchCompletion`/`Stop` transitions.
2. Persist to `fb-loadgen.runs.json` with retention; load on boot.
3. `GET /api/runs`, `GET /api/runs/{id}`, `GET /api/sessions/{id}/runs`.
4. Tests: record on natural completion, on stop, on failure; retention cutover.
- *Accepts:* after any run, `GET /api/runs` shows it with per-session outcomes and report dirs.

### Phase 2 — Schedules: engine + API (the headline feature)
1. `schedule` package: model, validation, next-run computation (cron/once/interval, TZ), tick engine, atomic store.
2. Manager wiring: fire → resolve targets → policy checks → staggered starts → run record.
3. REST endpoints from §2.4 (`/api/schedules*`, `/api/health`, `/api/version`).
4. Tests with injected clock: cron/interval/once math incl. DST boundary, skip-on-running, budget-wait timeout, catch-up=false skip, persistence round-trip, concurrent engine + API.
- *Accepts:* create a `once` schedule 2 minutes out → session starts by itself; cron schedule fires repeatedly across a restart (state survives).

### Phase 3 — Unattended operation
1. Webhook notification on run completion/failure: `POST` JSON (run record) to configured URL, retry ×3 with backoff; per-schedule `notifyUrl`, global default in settings.
2. `GET /metrics` (Prometheus text): per-session TPS/errors/latency/conns, fleet budget, schedule next-run timestamps — monitoring for overnight runs.
3. Report retention/cleanup job (delete report dirs older than N days; config, default off).
- *Accepts:* a failed scheduled run produces a webhook and a scrapeable metric.

### Phase 4 — UI parity (Schedules tab + history)
1. "Schedules" tab: list (next run countdown, last outcome), create/edit dialog (trigger picker with cron preset field, target multi-select, run spec, policy), enable/disable, "Run now", delete.
2. "Runs" view: recent runs with per-session outcomes and links into existing report downloads.
3. Keep using the same REST API — no UI-only endpoints.
- *Accepts:* everything in API.md's schedule section is clickable in the browser.

### Phase 5 — API hardening & polish
1. `--api-only` flag: serve the REST API without the SPA (headless hosts, CI).
2. Optional auth for GETs (`--ui-auth-all`) and per-origin CORS allowlist (`--cors-origin`) for browser-based API clients.
3. OpenAPI 3.1 spec (`api/openapi.yaml`) generated/maintained alongside `API.md`; add request logging middleware and `GET /api/health`.
4. Secrets: store settings password via Windows DPAPI (or `--pass-env`), so `fb-loadgen.ui.json` no longer holds plaintext; accept token via env (`FBLOADGEN_TOKEN`) as well as argv.
5. Async-start option for `POST /start` (`{"wait": false}` → return `202` with the session snapshot once `Starting` is reached) for impatient HTTP clients.
6. Restart resilience: on boot, if `runs.json` shows runs in flight, mark them `cancelled` with reason `process restarted` (honest history).

---

## 4. Explicit non-goals (for now)

- Distributed/multi-node scheduling (single process owns the fleet; a lock file warning is enough).
- Per-session weighted connection budgets (current even split works; revisit if mixed-size DBs become painful).
- Recurring *chains* (run A then B) — expressible later as two schedules with offsets; revisit on demand.
- Replacing the file-based stores with SQLite — current data volumes don't justify it.

## 5. Risks / decisions to confirm

| Risk | Mitigation |
|---|---|
| Cron edge cases (DST, timezone) | Use `robfig/cron/v3` parser rather than hand-rolled math; unit tests around DST transitions. |
| Schedule fires while operator is mid-maintenance | `IfRunning=skip` default + `enabled` flag + per-fire history entries make every fire auditable. |
| Budget starvation between overlapping schedules | Engine pre-check + `wait` policy with timeout; document that `maxTotalConns` is the fleet-wide gate. |
| `runs.json` growth | Retention (count + age) with defaults; file is tiny (KBs per 100 runs). |
| New dependency (cron parser) | Small, pure-Go, widely used; alternative: minimal 5-field parser in-repo if dependency policy forbids it. |

---

## Implementation status (2026-08-29)

| Phase | State | Where |
|---|---|---|
| 1 — Run history | **Done** | `session/run.go` (Run/SessionRun/RunHistory, persistence `fb-loadgen.runs.json`, retention, restart-cancelled runs), run attribution via `Session.runID` in `session/manager.go` |
| 2 — Schedules engine + API | **Done** | `schedule/` package (model, cron/interval/once via robfig/cron v3 + tzdata, tick engine, atomic store `fb-loadgen.schedules.json`, missed/catch-up policy), `/api/schedules*` in `ui/handlers_schedule.go` |
| 3 — Unattended operation | **Done** | `schedule/notify.go` (async webhook with HMAC + retries), `GET /metrics` (Prometheus text, no client dep), retention with schedule-referenced runs protected |
| 4 — UI parity | **Done** | `ui/static`: Sessions/Schedules/Runs tabs, schedule create/edit form, run history with per-session detail |
| 5 — Hardening | **Done** | Done: `--api-only`, `--ui-auth-all`, `--cors-origin`, async start (`"wait": false` → 202), `--schedules-file`/`--runs-file`/`--webhook-*` flags, OpenAPI 3.1 spec (`api/openapi.yaml` — 28 paths / 34 operations, validated against the live API). Deferred: secure credential storage (plaintext password in `fb-loadgen.ui.json` remains) |

Review findings folded in: fire/skip/missed semantics (`lastFire` vs run records), typed `RunOverrides` with precedence (no conn overrides — budget owns them), per-target skip with reasons for dangling/missing/unknown targets, `timeLimitMin >= 1` guard on recurring triggers, once-schedules spend themselves after one attempt, interval anchored at fire time, budget-wait runs in the engine's pending state, retention keeps `lastFire`-referenced runs, `GET /api/health` + `/api/version` served, manual starts recorded as runs (`origin: manual`), startup marks interrupted runs cancelled.

Deviations from the original plan: global webhook config moved from `UISettings` (would force a v3 settings migration) to `--webhook-url`/`--webhook-secret` flags plus per-schedule `notifyUrl`; the OpenAPI spec is hand-maintained at `api/openapi.yaml` rather than generated from code (2026-08-30: written and validated against the live API).
