# Improvement Plan — Post-Review (2026-09-19)

Source: full project review performed on 2026-09-19 against commit `50e3634`.
Every problem below was verified in source before being listed; file:line references
are to that commit and may shift as fixes land. Items are grouped into four phases
ordered by dependency and value; each phase ends with CI green and can ship alone.

Summary:

| Phase | Theme | Items | Est. |
|-------|-------|-------|------|
| 1 | Correctness: leaks & error handling | R1–R4 | 0.5–1 d |
| 2 | Lifecycle UX: time limit, status | R5–R7 | 0.5–1 d |
| 3 | Structure & performance | R8–R11 | 1–1.5 d |
| 4 | Security & hardening | R12–R14 | 1.5–2.5 d |

---

## Phase 1 — Correctness: leaks & error handling

### R1. Sidecar leak on natural completion (P1)

**Problem.** The emul sidecars (memory monitor, invariant loop, series ticker) are
cancelled and their pool closed only in the explicit `Stop()` path
(`session/manager.go:1267`). The natural-completion path — the `sched.Done()`
watcher at `manager.go:1128` — records history and calls `cleanupLocked`, but never
touches `emulCancel`/`emulPool`. Because `Stop()` refuses sessions whose status is
`Completed` (`manager.go:1250`), a **timed** oltp-emul run that finishes naturally
leaks three goroutines and one open database pool forever: the invariant loop keeps
opening transactions every N seconds against the server, and the orphaned series
ticker keeps mutating an orphaned `EmulState`. Repeating start→complete compounds
the leak. A user pressing Stop never sees this; a user letting runs finish always does.

**Design.** Make cleanup single-choked and idempotent:

1. Add to `Session`:
   ```go
   func (s *Session) stopEmulSidecarsLocked() {
       if s.emulCancel != nil { s.emulCancel(); s.emulCancel = nil }
       if s.emulPool != nil { _ = s.emulPool.Close(); s.emulPool = nil }
   }
   ```
   (caller holds `s.mu`; `Stop()` and the completion watcher both already hold it
   at the right points).
2. Call it from `cleanupLocked` — both the stop path (`manager.go:1291`) and the
   natural path (`manager.go:1143`/`1160`) run `cleanupLocked`, so one call site
   covers every finish. Remove the explicit block from `Stop()` **but keep the
   freeze-before-teardown ordering**: the freeze (`emulFrozen` capture,
   `manager.go:1265`) must run while `s.metrics` is still non-nil, so the cancel
   inside `cleanupLocked` must come after the freeze. Verify order per path:
   - stop path: freeze (1265) → … → `cleanupLocked(false)` (1292) ✓
   - natural path: `snap := snapshotLocked()` (1147, live metrics ✓) → … →
     `cleanupLocked(true)` ✓
3. `cleanupLocked(true)` is also called from the start-abort path (`manager.go:1030`)
   — there `emulCancel` is always nil (sidecars launch after the abort check), the
   nil guard covers it.
4. Guard `emulState` reads: `snapshotLocked` already handles
   `emulFrozen != nil` / live-state; after cleanup the live state simply stops
   changing (goroutines exited) — no nil-out, so Completed snapshots keep their data.

**Tests.**
- `emul`: `RunSidecars` gains a `Wait()` (internal `sync.WaitGroup` around the three
  goroutines). Unit test: launch with `nil` db and a fast series interval, cancel the
  ctx, assert `Wait()` returns within 2 s.
- `session`: lifecycle test with a stub — extract the sidecar launch/cleanup into
  `session/emul.go` (`emulRun` struct, see R8) so the test can start/complete a run
  against a fake scheduler and assert `Wait()` returns after cleanup. If the stub cost
  is too high, at minimum assert `emulCancel == nil && emulPool == nil` after
  `cleanupLocked` via a targeted unit test on a `Session` with the fields set.

**Acceptance.** A timed oltp-emul run completes naturally; afterwards
`/api/sessions/{id}` shows Completed, the invariant loop no longer opens
transactions on the database (verify via `mon$statements` or server log), and
repeated run cycles do not grow goroutine count (`/metrics` or pprof).

### R2. Provision probe leaks a pool on the idempotent path (P2)

**Problem.** `emul/provision.go:103-107`: the charset-NONE probe pool is closed only
on the not-provisioned branch; the "schema already present" early `return nil` leaks
it. Inside the UI provision job (long-lived process) every idempotent re-provision
leaks a pool and its connection.

**Design.** `defer db.Close()` immediately after a successful `sql.Open` in the probe
block (open, ping, decide, close — the main flow reopens its own pool below).

**Tests.** Existing provision flow covers it; assert via a code review check — or
simply run two idempotent provision jobs back-to-back and confirm stable connection
count in `mon$attachments`.

**Acceptance.** Two consecutive `POST /api/emul/provision` runs against the same
database leave `select count(*) from mon$attachments` unchanged after jobs finish.

### R3. Invariant loop stops permanently on transient failure (P2)

**Problem.** `emul/state.go` invariant goroutine `return`s on the first failure —
including transient ones (connection blip, restore in progress). One network hiccup
silently disables correctness checking for the rest of the run.

**Design.** Keep two distinct exits:
- **Permanent disable** only for the FB3 driver limitation
  (`EX_SNAPSHOT_ISOLATION_REQUIRED` / `EX_NOWAIT_OR_TIMEOUT_REQUIRED`) — same as today.
- **Transient failures**: increment a failure streak; `SetInvariant(fmt.Sprintf(
  "failed (attempt %d, will retry): %s", n, err))`; continue the loop. After
  **3 consecutive** failures, give up permanently with
  `"disabled: invariant check failed 3 times in a row: …"`.
- Success resets the streak.

**Tests.** Unit test with a fake `db` is awkward (driver-bound); instead extract the
decision into `invariantDecision(err error) (permanent bool)` and table-test it
(transient error, limitation error, nil).

**Acceptance.** Killing the Firebird server mid-run and restarting it leads to
invariants reporting `ok` again after recovery; the FB3 case still disables once.

### R4. Fill loop hammers `count(*)` when stalled (P3)

**Problem.** `emul/fill.go:96`: `done%checkEvery == 0` is always true while
`done == 0`, so a stall at the start (all units rejected/failed) runs a
`count(*) from doc_list` per iteration.

**Design.** Track `iterations++` per loop and gate on
`iterations%checkEvery == 0 || time.Since(lastReport) > 5*time.Second`.

**Acceptance.** With a deliberately failing stub (unit test on the counter logic,
or a manual DEBUG_01 run against a broken DSN), the count query runs at most every
`checkEvery` iterations.

---

## Phase 2 — Lifecycle UX: time limit & status

### R5. Time-limit control in the OLTPEMUL tab (P1, user-visible)

**Problem.** The tab's Start posts `{}` → `timeLimitMin = 0` → every tab-started
run is No-limit (unbounded). The Warmup/Main/Cooldown fields only act as shape
ratios in timed mode, which is not obvious — a user setting Main = 30 expects the
run to end after ~30 s of main phase. This exact confusion occurred in live use.

**Design.**
1. Add `<select id="emulTimeLimit">` to the lifecycle row: No limit / 1 / 5 / 15 /
   30 / 60 / 120 / 600 min — **default 15**, mirroring the Sessions tab; persist the
   choice per database via the existing prefs mechanism if cheap, else session-only.
2. Start handler: `POST start {"timeLimitMin": N}` from the dropdown instead of `{}`.
3. Lifecycle status line shows the countdown when timed:
   `Running — main (12:34 remaining)` — data already in
   `sess.remainingSec` / `sess.timeLimitMin`.
4. Run settings panel: rename the Main field label to "Main s (shape ratio when
   time limit set)" to make the semantics explicit.
5. `emulLifecycle()` enables Apply/Start per current rules; no change.

**Tests.** Manual API flow (PATCH → start with `timeLimitMin` → assert
`remainingSec > 0` and natural Completed transition), plus a JS syntax/lint pass.
The backend needs no changes.

**Acceptance.** Starting from the tab with "15" selected ends the run by itself at
~15 min as Completed; the countdown ticks in the lifecycle line; "No limit" behaves
as today.

### R6. Static asset cache busting (P2, user-visible)

**Problem.** Embedded UI assets have no validators, so every UI fix required a hard
reload; users kept running stale JavaScript.

**Design.** Wrap the static file handler in `ui/server.go` with a middleware that
sets `Cache-Control: no-store` on `.html`, `.js`, and `.css` responses (images can
stay cacheable). One middleware, no per-file changes.

**Acceptance.** After deploying a JS change, a normal reload picks it up
(check `Cache-Control` header via curl).

### R7. Gate probe caching in the tab (P3)

**Problem.** `app_emul.js` re-probes `GET emul/units` on every 1.5 s poll while a
session is Idle — an extra request per poll per open tab.

**Design.** Cache the gate verdict keyed by `sessionId + status`; re-probe only on
selection change or status transition into Idle/Failed (currently re-gates every
poll while Idle). Optionally run the probe only when the OLTPEMUL tab is active
(already the case for the rest of the poll).

**Acceptance.** Network tab shows the units probe once per selection change, not
per poll.

---

## Phase 3 — Structure & performance

### R8. Extract emul concerns from `session/manager.go` (P2)

**Problem.** `session/manager.go` is ~1,900 lines spanning session lifecycle, fleet
budget, history glue, and emul wiring. The natural-completion leak (R1) is a direct
consequence: the emul cleanup had to be remembered in two finish paths inside a
file too large to hold in one's head.

**Design.** New file `session/emul.go` owning everything emul-specific:
- `emulRun` type: `{ state *emul.EmulState; units []emul.Unit; cancel ctx.CancelFunc;
  pool *sql.DB; frozen *emul.EmulStateJSON }` with
  `begin(cfg, db, counts)`, `freeze()`, `stop()` — the Session holds `*emulRun`
  instead of five loose fields; `nil` = not an emul run.
- `writeEmulReport`, `emulSidecarPool`, `logf`, the start-branch provisioning +
  unit-loading code, and the Snapshot fill move with it.
- `manager.go` keeps only two call sites: `s.emulRun = beginEmulRun(...)` in the
  start branch and `s.emulRun.stopAndFreeze()` in `cleanupLocked`.
- Also move `RegisterDatabase` + `RestoreEmulSessions` into `session/register.go`.

**Tests.** Pure refactor; `go test ./...` + the R1 lifecycle test green before and
after. `git diff --stat` should show zero logic changes outside moved blocks.

**Acceptance.** `manager.go` under ~1,400 lines; all tests green; the emul fields
referenced from exactly one file.

### R9. RegisterDatabase: file I/O out of the manager lock (P2)

**Problem.** `RegisterDatabase` runs `config.LoadUISettings` (disk read) and
`persistSettings` (disk write) while holding `m.mu` — safe today (no re-entrancy),
but any future lock inside those paths deadlocks the whole control plane, and every
fleet poll stalls behind the file write.

**Design.** Read the saved prefs **before** taking `m.mu` (the only consumer is the
prefs lookup for the new session); build the updated `UISettings` inside the lock
from fields (already done); write the file after `m.mu.Unlock()`. `persistSettings`
is already called after unlock for the save path — align both.

**Acceptance.** Code review: no file I/O between `m.mu.Lock()`/`Unlock()` in
`RegisterDatabase`; fleet polling during a provision-job completion shows no stall.

### R10. Parser line tracking without O(n²) (P3)

**Problem.** `start2line(script, start)` (`emul/isqlscript.go:107,124`) rescans the
file prefix per statement — `oltp_data_filling.sql` (16,518 statements, ~700 KB)
takes ~4.7 s to parse purely from quadratic prefix rescans.

**Design.** Track `chunkStartLine` incrementally: the scanner already maintains
`line` (newline case); when a terminator is consumed and `start` moves, record
`chunkStartLine = line + <newlines inside the terminator itself, always 0>`. The
trailing chunk uses the current `line`. Remove `start2line`.

**Tests.** Extend the golden test to assert `Statement.Line` equals the known
upstream line of five spot-checked statements (e.g., `business_ops` DDL at
`oltp30_DDL.sql:624`, `srv_random_unit_choice` at `:8770`), then benchmark:
`go test -bench=ParseScript` before/after (expect the data_filling parse to drop
from ~4.7 s to <300 ms).

**Acceptance.** All golden tests green with exact line numbers; parse time of the
full asset set under 500 ms.

### R11. `writeEmulReport` file I/O out of `s.mu` (P3)

**Problem.** `session/manager.go` calls `writeEmulReport` inside `s.mu` — safe today,
but file I/O under the session lock invites deadlock the first time someone adds a
lock-taking call inside.

**Design.** Capture `st := state.JSON(...)` and the config echo under the lock;
build and write the file after `m.mu.Unlock()` — mirroring the pattern already used
for `persistSettings`.

**Acceptance.** Code review; no behavior change.

---

## Phase 4 — Security & hardening

### R12. Credential storage (P2, roadmap item)

**Problem.** Firebird credentials are stored in plaintext in `fb-loadgen.ui.json`,
and the emul persistence adds a second copy (`EmulSessionRef.Pass`).

**Design sketch (decide before implementing — three options):**
1. **OS keyring** (`github.com/99designs/keyring`): store passwords behind the OS
   credential manager (Windows Credential Manager / libsecret); `ui.json` keeps only
   a keyring reference. Best UX, new dependency, needs a fallback headless mode
   (service accounts have no unlocked keyring — fall back to file with 0600).
2. **Encrypted file with machine-bound key** (DPAPI on Windows, TPM-less fallback
   to a generated key file with 0600): no user interaction, portable across restarts.
3. **Minimal hardening**: keep plaintext but enforce `0600` on all persisted files
   and redact `EmulSessions` from `GET /api/config` (currently the pass fields are
   absent from Snapshots but `EmulSessionRef` rides along in the raw file only —
   verify nothing new serializes it).

Recommended: option 2 for v1 (matches the headless/service usage), option 1 later
if interactive users ask.

**Tests.** Round-trip: save with pass → restart → run starts without re-entry;
corrupt key file → clear error, not silent fallback to plaintext.

**Acceptance.** No plaintext password in any file under the repo/working dir
(except the key file, machine-readable only), all suites green, UI flows unchanged
for the user apart from one-time migration.

### R13. Provision path allowlist (P3)

**Problem.** `POST /api/emul/provision` (auth'd) accepts any server-side path — an
authed caller can fill the server disk with databases.

**Design.** New flag `--emul-allow-dir <root>` (default: the discover root). The
provision job and `RegisterDatabase` reject DSNs whose path escapes it — mirroring
the discovery root guard. The OLTPEMUL provision form shows the allowed root as a
placeholder.

**Acceptance.** Provisioning inside the root works; outside returns 400 with a
clear message; the existing UI flow is unchanged when the root is the default.

### R14. Audit remaining auth surface (P3)

**Problem.** New endpoints added this cycle (`emul/state`, `emul/profiles`,
provision job status) are read-tier (`authRead`); mutation endpoints are
`auth(s)`-protected — correct. But the audit should confirm: no emul endpoint
writes without `auth`; `--ui-token` unset on a LAN bind is a footgun.

**Design.** Route-table test: enumerate `server.routes()` and assert every
non-GET route is wrapped in `s.auth` (a unit test over the mux is awkward —
instead extract a route table `[]struct{method, path string, mutator bool}` and
iterate it in both `routes()` and the test).

**Acceptance.** The route test fails if a future mutating endpoint skips auth.

---

## Execution order & estimates

1. **Phase 1** first — R1 is the only correctness leak users will hit in normal
   timed usage; the rest are small and independent.
2. **Phase 2** next — closes the biggest UX gap (R5) and the stale-asset annoyance.
3. **Phase 3** after a quiet period — pure refactor, best done when no other
   changes are in flight (R8 especially).
4. **Phase 4** scheduled deliberately — R12 needs an option decision first.

Total: ~4–7 working days. Every phase lands with CI green; commits per item.

## Verification checklist per phase

- Phase 1: `go test -race ./...` green; manual timed run completes → no sidecar
  goroutines; two idempotent provisions → stable `mon$attachments` count.
- Phase 2: timed run from the tab ends as Completed at the limit; normal reload
  picks up JS changes.
- Phase 3: golden parser tests with line-number assertions; `manager.go` ≤ ~1,400
  lines; no behavior diffs.
- Phase 4: no plaintext credentials on disk; allowlist rejects escaping paths;
  route-auth test in CI.
