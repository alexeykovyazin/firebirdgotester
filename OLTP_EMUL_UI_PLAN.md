# OLTPEMUL Web UI Tab — Build Plan (v2)

Goal: a dedicated **"OLTPEMUL"** tab in the embedded web UI covering the full lifecycle of the
oltp-emul benchmark on one database: **settings → provision → start/stop → live intermediate
results → final report → run history**, styled after the firebirdtest.com dashboard
(score + memory peaks + results table).

v2 revises the original plan after a line-by-line review against the source. Corrections are
folded in; the review's key findings were:

- The CLI (`runCLI`) does **not** go through the session engine, so sidecars must be a shared
  `emul` package helper called from both entry points — not "moved into the session engine".
- `GetOpCounts()` is a total count per op name only; per-unit success/failure/latency aggregation
  is **new collector state**, not an extension of existing data.
- Provisioned databases outside the configured discover root can never enter the fleet via
  discovery (the root guard rejected `C:/temp` live) — the provision job must register the
  session itself.
- `manager.Patch` deliberately ignores client `connMax` (even fleet budget); the tab must not
  offer an editable conn-max input.
- The settings panel's invariant/monitor intervals and working mode need new `SessionConfig`
  fields end-to-end (fields → Patch → Validate → run-config mapping).

Current state feeding this plan (verified in code):

- Tab mechanism: `nav.tabs` buttons + `.tabsection` divs (`ui/static/index.html:279`); the poll
  is extended per tab by monkey-patching the global `refresh()` (`app_schedules.js:286`);
  scripts are included in order in `index.html:436-437`.
- Start/stop/pause/resume and per-session reports already work for oltp-emul sessions
  (session engine loads the unit registry since `1052a8f`).
- Weights API exists: `GET/PUT /api/sessions/{id}/emul/units|weights`.
- No chart library in the UI (and none needed — hand-rolled SVG sparklines are enough).
- Run history (`fb-loadgen.runs.json`, `session/run.go`) stores TPS/success/errors/reportDir per
  session — no emul-specific fields. Terminal writes flow through one choke point:
  `recordFinishEntry` / `runEntryFromSnap` (`manager.go:672`).

The decisive gap stays the same: **the emul sidecars (memory monitor + invariant checks), the
score metric, and per-unit outcome aggregation exist only partially in the CLI path**
(`main.go startEmulSidecars`; per-unit outcomes don't exist anywhere yet). Backend-first ordering
below reflects that.

---

## 1. Backend — make the state exist (Phase A)

### A1. Sidecars as a shared helper in the emul package

New file `emul/sidecar.go`:

```go
// RunSidecars launches the memory monitor and the invariant check loop for
// the duration of ctx, writing into state. Used by BOTH entry points:
// runCLI (main.go) and the session engine (session/manager.go).
func RunSidecars(ctx context.Context, db *sql.DB, monitorEvery, invariantEvery time.Duration,
    logf func(format string, args ...any)) *EmulState
```

- `EmulState` lives in the emul package (mutex-guarded):

```go
type SeriesPoint struct {
    TS          time.Time `json:"ts"`
    ScorePerMin float64   `json:"scorePerMin"`
    TPS         float64   `json:"tps"`
    DBBytes     int64     `json:"dbBytes"`
}

type UnitStat struct {
    Unit      string `json:"unit"`
    Mode      string `json:"mode"`
    Kind      string `json:"kind"`
    OK        int64  `json:"ok"`
    Conflict  int64  `json:"conflict"`
    Rejected  int64  `json:"rejected"`
    Failure   int64  `json:"failure"`
    AvgMs     int64  `json:"avgMs"`
    MaxMs     int64  `json:"maxMs"`
}

type EmulState struct {
    ScorePerMin float64
    Invariant   string      // "ok" | "failed: <msg>" | "disabled: <reason>" | "not run yet"
    InvariantAt time.Time   // zero until first check
    MemPeaks    Sample      // running peaks (db/att/trn/stm bytes)
    MemSamples  []Sample    // ring buffer, cap 120
    Series      []SeriesPoint // ring buffer, cap 120
    PerUnit     []UnitStat
}
```

- Ring buffers capped at 120 points (20 min at 10 s): long benchmark runs are covered by the
  final report, not an ever-growing live chart.
- The FB3 invariant-disable reason string must be the exact one produced today
  ("server requires a snapshot+nowait transaction; invariant checks disabled on this engine
  (driver limitation)") so CLI output and UI text stay identical.
- **Both call sites:**
  - `runCLI` (main.go): replace `startEmulSidecars` with `emul.RunSidecars(...)` wiring; keep the
    final "run peaks" print by reading `state.Result()` after shutdown — CLI behavior preserved.
  - session start (session/manager.go, the `oltp-emul` branch): after `sched.Start()`, call the
    same helper with the session's run context; store the returned `*EmulState` on the Session.

### A2. Score and per-unit outcome aggregation — new collector state

`GetOpCounts()` today is a **total count per op name only** (`worker.go:725`,
`RecordTransactionNamed` at `worker.go:453`: `opCounts[opName]++` + global latency buckets).
Per-unit outcomes and latencies are new state:

- `worker.MetricsCollector` gains: `opSuccess, opError, opConflict, opRejected map[string]int64`
  and `opLatSum, opLatMax map[string]int64`, plus a recording API:

```go
// UnitOutcomeReporter is implemented by emul.UnitError.
func (mc *MetricsCollector) RecordUnit(opName string, latency time.Duration, outcome, ok bool, uerr error)
```

- The worker, on op error, checks `errors.As` for `interface{ UnitOutcome() emul.Outcome }` and
  routes to `RecordUnit`; the success path also calls it with `OutcomeOK`. `RecordTransactionNamed`
  stays for the global totals — no behavior change for EMPLOYEE profiles.
- `emul.UnitError` gains `UnitOutcome() emul.Outcome`.
- Score = successful units ÷ elapsed **main-phase** minutes, recomputed by the existing 5 s
  metrics tick. During warmup the score is still computed but flagged `phase=warmup` so the UI
  can render it muted (upstream scores only the measurement window).
- `EmulState.PerUnit` is refreshed from these maps by the session tick (or the collector exposes
  `GetUnitStats()` read directly by Snapshot/state handlers).

### A3. Snapshot extension

`session/config.go Snapshot` gains `Emul *EmulStateJSON` (nil for non-emul): score, phase flag,
invariant status, peaks, last 120 series points, per-unit table. Existing pollers ignore it.
`openapi.yaml` documents the new optional field.

## 2. Backend — new endpoints (Phase B)

| Endpoint | Purpose |
|---|---|
| `GET /api/sessions/{id}/emul/state` | Full live `EmulState` (same data as Snapshot, full per-unit table + uncapped-at-render series) |
| `POST /api/emul/provision` | Async provision job: `{dsn, workingMode, initDocs, pageSize}` → `{jobId}`; runs `emul.Provision` + `emul.Fill` with progress callback |
| `GET /api/emul/provision/{jobId}` | `{status, stage, progress, message}` (stages: create → scripts → fill) |
| `DELETE /api/emul/provision/{jobId}` | Cancel job (ctx cancel; fill checks ctx) |
| `GET /api/emul/profiles` | Workload profile names for the settings dropdown: `select distinct working_mode from settings` on the target DB, falling back to the hardcoded upstream list (DEBUG_01…HEAVY_01) |

Provision job model (concrete): in-memory `map[jobID]*provisionJob` guarded by a mutex —
`{ctx, cancel, stage, progress, message, dsn}` — one goroutine per job, **one active job per
DSN**, mutation auth required. On successful completion the job **registers the database as a
session via the manager directly** (it already holds absPath + DSN), bypassing discovery —
required because discovery rejects paths outside the configured root (verified live:
"path escapes discover root"), and there is no create-session-by-DSN endpoint.
Block `start` for a session whose DSN has an active provision job; the schema guard would fail
mid-build anyway — make the error say "provisioning in progress".

SessionConfig additions (needed by the settings panel, end-to-end):

- New fields `EmulInvariantEvery` (default 60), `EmulMonitorEvery` (default 10),
  `EmulWorkingMode` (default `SMALL_01`) on `SessionConfig`; JSON-tagged, defaulted when absent
  so old `fb-loadgen.ui.json` files keep loading (add a regression test with a v1 settings file).
- `Patch` cases for all three; `Validate` (>= 0 / non-empty); `ToRunConfigWithReportEvery` maps
  them to the new `config.Config` fields so the session-engine sidecars and the CLI share values.
- PATCH response includes the effective values so the tab re-renders what was stored.

Run history: extend `SessionRun` with `*EmulRunStats` (score, peaks, workingMode, per-unit ok
counts) written at the terminal choke point `recordFinishEntry`/`runEntryFromSnap`
(`manager.go:672`) — one optional field, no format break. The Snapshot's `Emul` section is the
source, so no new plumbing is needed.

`openapi.yaml`: document all new endpoints, the request/response schemas, and the Snapshot
`Emul` extension.

## 3. Frontend — the tab (Phase C)

`ui/static/index.html`: fourth button `<button class="tab" data-tab="oltpemul">OLTPEMUL</button>`
and `<section class="tabsection" id="tab-oltpemul">`; new `ui/static/app_emul.js` included **after
`app_schedules.js`**. Poll integration: the per-tab refresh is a monkey-patch chain today
(`const _baseRefresh = refresh; refresh = async function () { await _baseRefresh(); … }`,
`app_schedules.js:286`). `app_emul.js` adds the next chain layer — or, preferred, refactor the
chain into a 10-line `onRefresh(tab, fn)` registry (`app.js` + `app_schedules.js` touched once)
so the pattern stops growing. Render only when the tab is active.

Layout (top to bottom):

1. **Database bar** — dropdown of sessions (badge "oltpemul ✓" when `GET emul/units` returns
   200), plus "Provision new…" expander: DSN (server-side path), working-mode dropdown (from
   `/api/emul/profiles`), init-docs, page size → provision button with progress bar + cancel.
   On completion the job has already registered the session — the bar dropdown just refreshes.
2. **Settings panel** (two columns):
   - Run settings: conn **min** (editable), **max (read-only — budget-assigned, with a hint that
     `maxTotalConns` in global settings is the actual knob; PATCH ignores client connMax by
     design)**, warmup/main/cooldown or time limit, think-ms, invariant-every, monitor-every,
     working mode — PATCHes the session before start and re-renders the effective values from
     the PATCH response.
   - Unit mix editor: table of the 20 units (name, mode, kind badges, weight input), live sum,
     presets (`default`, `creation-heavy`, `no-cancellations`), Save → `PUT emul/weights`.
3. **Controls row**: Start / Pause / Resume / Stop (existing endpoints; disabled per status) and
   the phase indicator (warmup → main → cooldown with progress).
4. **Intermediate results** (live grid):
   - Big **score** number (successful ops/min) + "total units, % ok"; rendered muted with a
     "warmup — not scored" note during the warmup phase;
   - SVG **sparkline** of score per interval (hand-rolled polyline, ~40 lines of JS);
   - **Memory peaks** card (db/att/trn/stm in MB) + sparkline of db-level samples;
   - **Invariant status** line with timestamp (green OK / red failure / grey disabled);
   - **Per-unit table**: unit, mode, kind, ok / conflict / rejected / fail, avg ms — sortable,
     with a filter box.
5. **Final report** (status ∈ Completed/Failed): frozen score card, config echo (working mode,
   weights used, init docs, durations, effective connection count), links to raw report files via
   the existing `/api/sessions/{id}/report/{file}` endpoints, and **Last runs** table — date,
   score, peaks × 4, outcome — from the extended run history (the firebirdtest.com "all results"
   table, per database).

No framework, no chart lib, no build step — plain JS matching the existing files' style.

## 4. Final report correctness (Phase D)

- Freeze `EmulState` into the report directory on terminal transition: write `results_emul.txt`
  (score, per-unit table, invariant log, peaks, config echo) next to the existing `results*.txt`
  files, via the same report writer the start path already opens — the download endpoints serve
  it with zero new plumbing.
- Persist `EmulRunStats` into run history at `recordFinishEntry` (Phase B) → the "Last runs"
  comparison survives restarts.
- Also fix, while touching this area: the `phaseProgress` field is a 0–100 percent value — the
  tab must not multiply it by 100 again (observed "10000%" in a live poll).

## 5. Order, effort, risks

| Step | Scope | Est. |
|---|---|---|
| A1 | `emul.RunSidecars` shared helper + both call sites | 1 d |
| A2 | New collector state (per-unit outcomes + latency), score, worker routing | 1–1.5 d |
| A3 | Snapshot extension (+openapi) | 0.5 d |
| B | state + provision-job (with direct session registration) + profiles endpoints, SessionConfig fields, run-history field (+openapi) | 1.5–2 d |
| C | Tab markup + `app_emul.js` (forms, poll, SVG sparklines, tables) + optional `onRefresh` refactor | 2–3 d |
| D | Frozen final report + history comparison + phaseProgress fix | 0.5–1 d |

Total ≈ **6.5–9 working days**. A1–A3 unblock everything else; each phase lands independently
with CI green. Commit boundaries: after A, after B, after C, after D.

Risks / decisions:

- **Ring-buffer sizing**: 120 points cap for live charts; the final report carries the full run.
- **Provision concurrency**: one job per DSN; block `start` while a job for the same DSN is
  active, with an explicit "provisioning in progress" error.
- **connMax is fleet-managed**: the tab never offers an editable conn-max; show the effective
  value and point at `maxTotalConns`.
- **Working-mode + weights echo** stored at start time (in `EmulState`/`EmulRunStats`) so old
  reports remain interpretable after later changes.
- **Settings-file back-compat**: regression test loading a v1 `fb-loadgen.ui.json`.
- Not in scope: multi-DB simultaneous emul runs in one tab (the fleet tab covers start-all),
  provisioning on remote servers whose server-side path is unreachable for temp-file cleanup.
