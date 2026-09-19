# OLTPEMUL Web UI Tab — Build Plan

Goal: a dedicated **"OLTPEMUL"** tab in the embedded web UI covering the full lifecycle of the
oltp-emul benchmark on one database: **settings → provision → start/stop → live intermediate
results → final report → run history**, styled after the firebirdtest.com dashboard
(score + memory peaks + results table).

Current state feeding this plan (verified in code):

- Tab mechanism: `nav.tabs` buttons + `.tabsection` divs (`ui/static/index.html:279`); JS is split
  per feature (`app.js` = sessions fleet, `app_schedules.js` = schedules + runs); one global
  `refresh()` polls every 1.5 s. Adding a tab = one button + one section + one JS file.
- Start/stop/pause/resume and per-session reports already work for oltp-emul sessions
  (session engine loads the unit registry since `1052a8f`).
- Weights API exists: `GET/PUT /api/sessions/{id}/emul/units|weights`.
- No chart library in the UI (and none needed — hand-rolled SVG sparklines are enough).
- Run history (`fb-loadgen.runs.json`, `session/run.go`) stores TPS/success/errors/reportDir per
  session — no emul-specific fields.

The decisive gap: **the emul sidecars (memory monitor + invariant checks) and the score metric
exist only in the CLI path** (`main.go startEmulSidecars`). Until they live inside the session
engine, the tab has no live data to show. Backend-first ordering below reflects that.

---

## 1. Backend — make the state exist (Phase A)

### A1. Move sidecars into the session engine

New file `session/emul.go`:

```go
type EmulState struct {
    // live, updated by sidecars
    ScorePerMin      float64          // successful units / elapsed minutes (main phase)
    Invariant        string           // "ok" | "failed: <msg>" | "disabled: <reason>" | "not run yet"
    InvariantAt      time.Time
    MemPeaks         emul.Sample      // running peaks (db/att/trn/stm bytes)
    MemSamples       []emul.Sample    // ring buffer (last 120 samples) for the live chart
    Series           []SeriesPoint    // score + tps per 10 s interval for the live chart
    PerUnit          []UnitStat       // per-unit ok/conflict/rejected/failure + avg/max ms
    // run settings echo (for the report)
    WorkingMode string
    WeightsHash string
}
```

- `Session.start` (session/manager.go, the `cfgCopy.Profile == "oltp-emul"` branch): after
  `sched.Start()`, launch the same monitor + invariant loops as `startEmulSidecars` today, writing
  into the session's `EmulState` (mutex-guarded). Delete the CLI-only path in main.go — one
  implementation, both entry points benefit.
- Store `emulState *EmulState` on `Session`; `nil` for non-emul profiles.

### A2. Score and per-unit outcome aggregation

- `worker.MetricsCollector` already counts per-op success/failure by name (`GetOpCounts`). Extend
  the op-success path to also record **per-unit latency** (avg/max) — it has the duration already.
- Outcome split: `emul.UnitError` carries `Outcome`. In `ops.ClassifyError` the error passes
  through as "expected" — additionally have the worker ask for the outcome when the error
  implements `interface{ UnitOutcome() emul.Outcome }` and count per unit:
  `ok / conflict / rejected / failure`. New collector API: `GetEmulUnitStats() []UnitStat`.
- Score = sum of successful units ÷ elapsed main-phase minutes, recomputed by the existing
  5 s metrics tick. This is the headline number for the tab and the final report.

### A3. Snapshot extension

`session/config.go Snapshot` gains `Emul *EmulStateJSON` (nil for non-emul): score, invariant
status, peaks, last N series points (cap ~120), per-unit table. Existing pollers ignore it; the
new tab renders it.

## 2. Backend — new endpoints (Phase B)

| Endpoint | Purpose |
|---|---|
| `GET /api/sessions/{id}/emul/state` | Full live `EmulState` (same data as Snapshot, but uncapped series + full per-unit table) |
| `POST /api/emul/provision` | Async provision job: `{dsn, workingMode, initDocs, pageSize}` → `{jobId}`; runs `emul.Provision` + `emul.Fill` with progress callback |
| `GET /api/emul/provision/{jobId}` | `{status, stage, progress, message}` (stages: create → scripts → fill) |
| `DELETE /api/emul/provision/{jobId}` | Cancel job (ctx cancel; fill checks ctx) |
| `GET /api/emul/profiles` | The workload profile names (DEBUG_01 … HEAVY_01) for the settings dropdown — read from the target DB `settings` table or hardcode the upstream list |

Provision jobs: in-memory map + one goroutine each (mirror the ScheduleEngine's job style);
only one provision per DSN at a time; require `emul` mutation auth.

Run history: extend `SessionRun` with `*EmulRunStats` (score, peaks, workingMode, unitsOk) written
on terminal transition — one small struct, no format break (new optional field).

## 3. Frontend — the tab (Phase C)

`ui/static/index.html`: third button `<button class="tab" data-tab="oltpemul">OLTPEMUL</button>`
and `<section class="tabsection" id="tab-oltpemul">`; new `ui/static/app_emul.js` hooked into the
global `refresh()` (render only when the tab is active to keep the 1.5 s poll cheap).

Layout (top to bottom):

1. **Database bar** — dropdown of sessions (all profiles, badge "oltpemul ✓" when
   `GET emul/units` returns 200), plus "Provision new…" expander: DSN, working-mode dropdown
   (from `/api/emul/profiles`), init-docs, page size → provision button with progress bar +
   cancel (polls the job endpoint). Provisioned → "Add to fleet" (re-discover that folder).
2. **Settings panel** (two columns):
   - Run settings: conn min/max, warmup/main/cooldown (or time limit), think-ms,
     invariant-every, monitor-every — a plain form that PATCHes the session before start.
   - Unit mix editor: table of the 20 units (name, mode, kind badges, weight input), live sum,
     presets row (`default`, `creation-heavy`, `no-cancellations`), Save → `PUT emul/weights`.
3. **Controls row**: Start / Pause / Resume / Stop (existing endpoints; disable per status) and
   phase indicator (warmup → main → cooldown with progress).
4. **Intermediate results** (live grid):
   - Big **score** number (successful ops/min) + small "total units, % ok" line;
   - SVG **sparkline** of score per 10 s interval (hand-rolled polyline, ~40 lines of JS);
   - **Memory peaks** card (4 numbers in MB, db/att/trn/stm) + sparkline of db-level samples;
   - **Invariant status** line with timestamp (green OK / red failure / grey disabled);
   - **Per-unit table**: unit, mode, kind, ok / conflict / rejected / fail counts, avg ms —
     sortable by count, filter box.
5. **Final report** (appears when status ∈ Completed/Failed): score summary card (same numbers
   as live, frozen), config echo (working mode, weights used, init docs, durations, conn count —
   from the run-settings echo), links to the raw report files via the existing
   `/api/sessions/{id}/report/{file}` download endpoints, and **Last runs** table: date, score,
   peaks × 4, outcome — from the extended run history (this is the firebirdtest.com "all results"
   table, per database instead of per engine version).

No framework, no chart lib, no build step — plain JS matching the existing files' style.

## 4. Final report correctness (Phase D)

- On terminal transition (manager stop path), freeze `EmulState` into the session's
  `lastSnap`/report directory: write `results_emul.txt` (score, per-unit table, invariant log,
  peaks, config echo) next to the existing `results*.txt` files so the download endpoints serve it
  with zero new plumbing.
- Persist `EmulRunStats` into run history (Phase B) → the "Last runs" comparison survives restarts.

## 5. Order, effort, risks

| Step | Scope | Est. |
|---|---|---|
| A1+A2 | Sidecars in session engine, score, per-unit outcomes | 1.5–2 d |
| A3 | Snapshot extension | 0.5 d |
| B | state + provision-job + profiles endpoints, run-history field | 1–1.5 d |
| C | Tab markup + `app_emul.js` (forms, poll, SVG sparklines, tables) | 2–3 d |
| D | Frozen final report + history comparison | 0.5–1 d |

Total ≈ 5.5–8 working days; A1–A3 unblock everything else and each later step is independently
demoable. Commit boundaries: after A (backend green, CI green), after B, after C, after D.

Risks / decisions:

- **Ring-buffer sizing** for live series: cap at 120 points (20 min at 10 s) — long benchmark
  runs need the final report, not an ever-growing live chart.
- **Provision concurrency**: one job per DSN, and block `start` while a provision job for the same
  DSN is active (the schema guard would fail mid-build anyway — make the error message say that).
- **20-conn budget**: the fleet even-budget overrode connMax in the live test; the tab's settings
  panel must show the *effective* connMax after start (Snapshot already reports `currentConns` /
  `targetConns` — surface the target, not the form value).
- **Working-mode visibility**: store the working mode + weights echo at start time (they can be
  changed mid-fleet later) so old reports remain interpretable.
- Not in scope: multi-DB simultaneous emul runs in one tab (the fleet tab already covers that via
  start-all), provisioning on remote servers where the server-side path is unreachable for cleanup.
