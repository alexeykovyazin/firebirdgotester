# Extended Load Mix Plan (heavy SELECT / bulk DML / ops log / tx variants / −plusddl)

Date: 2026-09-26 (rev. 2). Verified against commit `1109219` (HEAD, main).
Every design point below is anchored to source; file:line refs are to that commit.

Rev. 2 adds the second requirements batch: the transaction completion axis
(COMMIT / ROLLBACK / RETAINING variants / 2PC / limbo / connection drop),
savepoints, autonomous transactions, DDL inside transactions, and the
transaction-centric log schema (per-tx number, parameters, completion method,
change volume, start time; every command line references its tx number).

Rev. 3 is the code-review pass: every claim re-verified against source
(fork driver, GOROOT database/sql, ramp/worker, session/ui). Twelve findings
(R1–R12) — two of them design-breaking — are fixed in place; §6 lists each
finding with evidence and the applied fix.

Rev. 4 adopts the `fb_repl_print` output format for the operations/transactions
log (requirement of 2026-09-26): the replication-log rendering was studied live
(`C:\HQbird\Firebird40\fb_repl_print.exe` + its shipped manual `fb_repl_print.md`
+ a real journal segment from this repo). Format spec, live sample and the
opslog mapping are in §7; Phase 1 now starts with the recorded study (L0) and
L1 emits `repl-print` format by default (`tsv` kept as the machine option).

Rev. 5 is the second source-review pass (GOROOT `database/sql` mechanics, fork
connection/transaction lifecycle, TxOptions call sites). Nine findings R13–R21,
including two design corrections (connDrop collapsed to a single hard-drop
mechanism; retaining-reuse spelled out as a mandatory `BeginTx` rework), three
simplifications (limbo without aux DB; single-tier connDrop; unified
`[] RESOLVED` records), and a much fuller test matrix (Phase 5, D1–D7).
Findings are listed at the end of §6.

Goal: change the load character of the oltp-emul run mode by adding
(1) periodic heavy multi-JOIN SELECTs, (2) periodic mass INSERT/UPDATE/DELETE,
(3) an operations log with size rotation, (4) randomized transaction-parameter
variants for all operations, and (5) an opt-in `−plusddl` launch variant with
runtime DDL churn (columns, tables, triggers). All parameters configurable via
UI + config file, defaults as specified; everything logged incl. errors; logs
exposed via API.

---

## 0. Verified facts that shape the design

| # | Fact | Source |
|---|------|--------|
| V1 | Transaction options are chosen at exactly one point, before the op is picked; today: fixed `emul.TxOptions()` for oltp-emul, driver default otherwise | `worker/worker.go:270-278`, op pick at `:290` |
| V2 | Driver fork TPB presets: RC+wait, RC+nowait, RC(no_rec_version)+wait, snapshot+wait, consistency+wait, RC RO+wait, RC RO+nowait. `isc_tpb_lock_timeout=21` is defined but **never emitted**; `ReadOnly` is honored only for RC presets | fork `transaction.go:33-93` (`tpbForIsolationLevel`), `driver_go18.go:56-83`, `consts.go:163-184, 593-609`; fork pinned at `go.mod:12` |
| V3 | oltp-emul units reject only **infinite** WAIT: `sp_check_nowait_or_timeout` raises `ex_nowait_or_timeout_required` when `rdb$get_context('SYSTEM','LOCK_TIMEOUT') < 0`; WAIT with a finite LOCK_TIMEOUT passes | `emul/assets/oltp30_DDL.sql:2689-2710`, exception at `:377` |
| V4 | Snapshot (TIL=concurrency) is required only by the totals SPs (`fn_is_snapshot` guard in `SRV_MAKE_*_SALDO`), not by business units — unit-level snapshot support must be tested, not assumed | `emul/assets/oltp_common_sp.sql:1288-1291`, `emul/run.go:161-166` |
| V5 | Timer-driven DB sidecars are an established pattern (own ticker + dedicated pool + ctx cancel): invariant loop, score/series ticker, memory monitor | `emul/state.go:171-279`, `emul/monitor.go:144`, pool `session/emul.go:100-106` |
| V6 | No log rotation exists; `errlog.Logger` is append-only mutex+bufio (32KB flush) — the right template for a rotating ops log | `errlog/errlog.go:26-33, 68`; opened per run at `session/manager.go:903-908` |
| V7 | Config persistence pattern: versioned struct + `Normalize` + `Validate` + atomic tmp+rename save, loaded in `runUI` | `config/persist.go:38-50, 158-193, 225-248`, `main.go:106-117` |
| V8 | Per-session params are `SessionConfig` fields, patchable via `PATCH /api/sessions/{id}`, defaults live in `fb-loadgen.ui.json` (`SessionPrefs`) | `session/config.go:52-56, 99-101, 164-192`, `session/manager.go:575-583` |
| V9 | Log/file delivery precedent: `GET /api/sessions/{id}/report/{file}` with path jail | `ui/server.go:468-495`, `session/reports.go:86-111` |
| V10 | Latency histogram tops out at ≥1000 ms — heavy SELECTs would be indistinguishable | `worker/worker.go:540-560` |
| V11 | emul DB is `CHARACTER SET NONE` (provisioning connects `?charset=NONE`); runtime worker conns use driver default UTF8 — new SQL must use ASCII literals only | `emul/provision.go:99-117` |
| V12 | Profile registries assert exact op counts/weights; adding ops there ripples into validation. Sidecar ops avoid that entirely | `profile/read_heavy.go:124-154`, `profile/write_heavy.go:125-155` |
| V13 | Saldo invariants total the turnover logs in a snapshot tx — bulk DML must not touch business tables or it breaks `SRV_MAKE_INVNT_SALDO / SRV_MAKE_MONEY_SALDO` | `emul/invariant.go:13-37`, `emul/state.go:197-235` |
| V14 | `ops/errors.go` already classifies deadlock / lock conflict / lock timeout as expected-under-load | `ops/errors.go:160-169` |
| V15 | The fork already has wire ops `opCommitRetaining`/`opRollbackRetaining` and an internal `commitRetainging()`, but `driver.Tx.Commit()/Rollback()` are always plain — and `database/sql`'s `driver.Tx` interface takes no context/arguments. A completion method can only be signaled at BEGIN time via `TxOptions` (same encoding trick as `LevelReadCommittedNoWait = 1000`). Levels travel numerically: `driver.IsolationLevel` is `int` (GOROOT `database/sql/driver/driver.go:269`), `ctxDriverBegin` casts back without loss — encoded values reach the fork intact | fork `transaction.go:113,126,138`, `wireprotocol.go:1373,1389`, `consts.go:609` |
| V16 | The fork has no `isc_prepare_transaction` wire op — only `op_prepare_statement` for SQL statements (consts.go:532, wireprotocol.go:1436). Required for the limbo generator and the prepare phase of 2PC | absent in fork `wireprotocol.go` |
| V17 | Workers exit on dead connections (run loop `worker/worker.go:231-234`, BeginTx path `:278-283`), and **ramp never reaps exited workers** — `s.workers` only grows or truncates by target (`ramp/ramp.go:395-448`), so a dead worker silently reduces load for the rest of the run. Exit+respawn is therefore NOT a viable connDrop mechanism; the Worker keeps its `connFactory` (worker.go:128), so in-place rebuild (`connFactory.Open()`, swap `w.dbConn` under mu) is the only correct path. Per-worker pools pin exactly 1 conn — a retained context lands on the same conn the worker reuses | `worker/worker.go:128,231-234`, `ramp/ramp.go:395-448`, `db/connect.go:41-43` |
| V18 | In fork `BeginTx`, `opts.ReadOnly` **overrides** the requested isolation: ReadOnly=true always yields RC RO or RC RO_NOWAIT, regardless of the level — snapshot/consistency+RO silently degrade to RC RO today. Unknown isolation values fail loudly ("This isolation level is not supported.") | fork `driver_go18.go:56-83` |
| V19 | The fork implements neither `SessionResetter` nor `Validator`, so `database/sql` sets `keepConnOnRollback=false`: **every `tx.Rollback()` closes the driver connection** and the pool (1 conn) reopens on the next op. Explains existing reconnect churn on error paths; conns are only kept across rollback if a driver can reset/validate them | GOROOT `database/sql/sql.go` `beginDC`; fork grep for `SessionResetter\|Validator` empty |
| V20 | gfix limbo switches confirmed: `-list`, `-commit <tr\|all>`, `-rollback <tr\|all>`, `-two_phase <tr\|all>`, `-prompt` | gfix(1) man page (firebird3.0) |
| V21 | The report path jail has **no extension whitelist** (only `..`/slash rejection) and `ListReports` returns the **live** run's `reportDir` for a running session — the logs API needs new routes only, no jail changes | `session/reports.go:58-66, 86-111` |

## 0.1 Decision points (need Alexey's call before/during implementation)

- **D1 — benchmark comparability.** Points 1, 2, 4 change the oltp-emul score by construction
  (extra load + random tx params). This plan defaults them **on** (as specified) behind an
  umbrella switch `extendedLoad.enabled=true`, with `txVariants.mode=off` and periodic ops
  disabled as the "classic score" preset. firebirdtest.com score runs should use the classic preset.
- **D2 — bulk DML target.** Mass DELETE/UPDATE on `doc_data`/`wares` would break saldo invariants
  (V13). Plan: dedicated aux table `EL_BULK_ITEMS`, created per launch (same drop/recreate pattern
  as `perf_estimated` in `oltp_adjust_DDL.sql`); deletes only rows created by bulk rounds (self-cleaning).
- **D3 — ops-log volume.** Logging every unit at full emul rate fills 50 MB in minutes. Default
  `level=all` per spec, with `periodic` (sidecar ops only) and `errors` levels as fallback knobs.

---

## 1. Config surface (single source for UI, file, CLI)

New section, following V7/V8 patterns. Defaults exactly as specified in the task:

```jsonc
// fb-loadgen.ui.json, new top-level section (SessionPrefs extended; atomic save, Normalize, Validate)
"extendedLoad": {
  "enabled": true,               // umbrella; false = classic comparable score run
  "heavySelect": { "everySec": 5, "minJoins": 3 },
  "bulkDml":     { "everySec": 30, "minRows": 100, "maxRows": 1000 },
  "opsLog":      { "enabled": true, "level": "all", "maxSizeMB": 50,
                   "keepArchives": 3, "rotateOnStart": true,
                   "format": "repl-print",   // repl-print (default, §7) | tsv
                   "dumpRecords": false },    // JSON data dumps, -R analog
  "txVariants":  { "mode": "emul-safe", "lockTimeoutChoicesSec": [1, 3, 5, 10],
                   // completion weights (normalized); limbo/connDrop also rate-limited below
                   "completion": { "commit": 55, "rollback": 15, "commitRetaining": 10,
                                   "rollbackRetaining": 5, "twoPhase": 5,
                                   "limbo": 2, "connDrop": 2 },
                   "rareCompletionMinGapSec": 10,
                   "retainingChainMax": 50,          // force plain commit after N retains
                   "twoPhaseAuxDB": "<mainDbDir>/EL_2PC.FDB",
                   "savepointProb": 0.15, "autonomousCallProb": 0.10,
                   "ddlRollbackFrac": 0.20 },
  "plusDDL":     { "enabled": false, "everySec": 60, "colPrefix": "TST_",
                   "testInsertRows": 100, "testUpdateRows": 50, "testDeleteRows": 30,
                   "workTables": ["WARES", "AGENTS", "DOC_STATES"] }  // tables exist: oltp30_DDL.sql:753,929,938
}
```

Plumbing per new field: `SessionConfig` (`session/config.go:52-56` + `Validate` `:126-156`) →
`ToRunConfigWithReportEvery` (`:164-192`) → `config.Config` (`config/config.go:43-46`) →
`PATCH /api/sessions/{id}` (`session/manager.go:575-583`) → UI session panel (`ui/static/app_emul.js`).
CLI flags mirror the fields: `--plus-ddl`, `--heavy-every`, `--bulk-every`, `--bulk-min/--bulk-max`,
`--ops-log-level`, `--tx-variants`, `--extended-load=false`.

## 2. Phases

Each phase ends buildable + testable; phases are ordered by dependency.

| Phase | Theme | Items | Est. |
|-------|-------|-------|------|
| 0 | Driver fork + measurement prerequisites | F1–F6 | 1–1.5 d |
| 1 | Operations log in fb_repl_print format + rotation + log API | L0–L5 | 1.5–2 d |
| 2 | Randomized tx scenarios: isolation/RO-RW/wait + completion + savepoints/autonomous | T1–T9 | 2.5–3.5 d |
| 3 | Heavy SELECT (5 s) + bulk DML (30 s) sidecars | H1–H7 | 1.5–2 d |
| 4 | −plusddl variant (DDL churn + config/UI/API) | P1–P8 | 2–3 d |
| 5 | Tests, docs & specs | D1–D7 | 1–1.5 d |

### Phase 0 — Driver fork + prerequisites

- **F1. Fork: LOCK_TIMEOUT support.** Extend `tpbForIsolationLevel` (fork `transaction.go:33-93`)
  with a family of encoded levels: `LevelLockTimeoutBase = 2000`; value `2000+n` ⇒ TPB
  `version3, write, wait, read_committed, rec_version, isc_tpb_lock_timeout, <n as 4-byte LE>`
  (`isc_tpb_lock_timeout=21`, consts.go:184; unit = seconds per IB docs — verify against wire in F6).
  Mirror `LevelReadCommittedNoWait = 1000` precedent (consts.go:609). Levels travel numerically
  end-to-end (V15); unknown values fail loudly today (`driver_go18.go:80-83`) — deliberate
  coupling: bump the `go.mod:12` pin in the same PR. RO composition belongs to the F2 BeginTx
  rework (today `ReadOnly` overrides the level — V18).
- **F2. Fork: isolation × RO matrix + BeginTx ReadOnly fix.** Today `opts.ReadOnly` overrides the
  requested level (V18): rework the `BeginTx` branch (`driver_go18.go:56-63`) so RO composes with
  the level instead of replacing it, and add the missing presets next to `tpbForIsolationLevel`
  (transaction.go:33-93): `RC + no_rec_version + nowait`, `concurrency + nowait`,
  `concurrency + read`, `consistency + read`. Resulting variant matrix (RW | RO):
  RC/rec_version wait & nowait; RC/no_rec_version wait & nowait; snapshot wait & nowait;
  consistency wait; RC wait+timeout(n) — all in RW; RO counterparts where sensible (RC RO wait/nowait
  exist today; snapshot-RO, RC RO+timeout added).
- **F3. Unit-isolation probe.** One-off script/test running a handful of emul units under snapshot and
  consistency tx against local FB3/4/5 (ports 3053/3054/3055) to close V4 — decides whether business
  units join the full isolation matrix or stay RC-based (sidecar ops carry snapshot variants regardless).
- **F4. Latency buckets.** Extend the histogram (`worker/worker.go:540-560`) with 2 s / 5 s / 10 s /
  30 s / 60 s+ buckets; keep legacy buckets for comparability of old reports.
- **F5. Fork: completion-intent encoding.** `database/sql` passes no method/args to `Commit()`
  (V15), so encode completion intent in the begin-time `IsolationLevel`, exactly like the fork
  already encodes `1000` (consts.go:609): `5000+iso ⇒ commit-retaining`, `6000+iso ⇒
  rollback-retaining`, `7000+iso ⇒ prepare-then-die`, `8000+iso ⇒ hard-drop with open tx`.
  Levels travel numerically end-to-end (V15, `ctxutil.go:97-104`); unknown values fail loudly
  today (`driver_go18.go:80-83`) — deliberate coupling: bump the `go.mod:12` pin in the same PR.
  Mechanics per intent, all in the fork:
  - *retaining* (`5000/6000`): dispatch in `Commit()/Rollback()` (transaction.go:126/138) to the
    existing `opCommitRetaining`/`opRollbackRetaining` (wireprotocol.go:1373/1389) and leave
    `needBegin=false` — the retained context is the next tx on that conn.
  - *retaining reuse (mandatory BeginTx rework)*: `fc.begin()` (connection.go:65-69) today
    unconditionally runs `opTransaction`, which collides with the live retained context. `BeginTx`
    must return the existing `fc.tx` when `fc.tx != nil && !fc.tx.needBegin` (the reuse pattern
    already exists on every exec path, e.g. connection.go:106-111, statement.go:179), validating
    that the requested TPB matches the retained one; on mismatch, roll the retained context back
    and start fresh. Verified by D2.
  - *prepare-then-die* (`7000+iso`): `Commit()` runs the new wire `op_prepare`
    (`isc_prepare_transaction` — absent today, V16), then hard-closes the socket and returns an
    error; `database/sql` discards the conn and a limbo transaction exists (single-db limbo is
    gfix-visible — R15).
  - *hard-drop* (`8000+iso`): `Rollback()` skips `opRollback` and closes the net conn directly;
    the intent must ALSO short-circuit `Conn.Close()`'s rollback loop
    (`connection.go:81-92` iterates `fc.transactionSet` calling `Rollback()`) — otherwise pool
    teardown silently rolls the tx back before the socket dies and the "crash" degrades into an
    orderly close (R16).
- **F6. Scenario probes on FB3/4/5 (local ports 3053/3054/3055):** (a) `EXECUTE STATEMENT …
  ON EXTERNAL DATA SOURCE '<aux>' WITH COMMON TRANSACTION` really enlists the external attachment
  and commits as genuine 2PC (FB3+ — verify per ODS; the aux DB is needed only for this
  twoPhase completion — limbo does not need it: a prepared single-db tx whose conn dies is
  already limbo, R15); (b) a limbo tx created by F5 `7000+iso` (prepare, then socket death) is
  visible to `gfix -list` and resolvable online via `gfix -commit|-rollback|-two_phase <id>`
  (switches confirmed, V20; probe whether online resolution suffices or a shutdown is required —
  fallback: the fork already speaks the services manager for backup/restore, createdb.go:91-105);
  (c) autonomous SP (`IN AUTONOMOUS TRANSACTION DO`) works on all three versions;
  (d) retaining-reuse semantics: next `BeginTx` on the conn consumes the retained context, TPB
  intact (exercises the F5 rework); (e) aux DB creation reuses `emul/createdb.go` helpers
  (createdb.go:23-86; page size 4096 ⇒ driver createdb path at :48-59).

### Phase 1 — Operations log (fb_repl_print format) + rotation + API

- **L0. Repl-print format study — done during planning (2026-09-26).** The replication-journal
  printer `fb_repl_print` (local copy and its shipped manual `fb_repl_print.md` at
  `C:\HQbird\Firebird40\`) was run against a real journal in this repo
  (`EMPLOYEE.FDB.ReplLog\EMPLOYEE.FDB.journal-000000032`); the format spec, live sample and the
  opslog mapping are pinned in §7. Line grammar: `[tx#] EVENT args (offset: N)`, empty brackets =
  outside-transaction actions, `BLOCK (offset: …)` segment blocks, `-H` header
  (Segment/Version/Guid/Sequence/State/Length). Event vocabulary: START, PREPARE (2PC), COMMIT,
  ROLLBACK, SAVE, UNDO, RELEASE, SET SEQUENCE, EXECUTE SQL (DDL-only in repl logs), BLOB,
  INSERT/UPDATE/DELETE table. Implementation-time follow-up: diff exact strings against
  `fb_repl_print.cpp` in the Firebird source tree for events absent from the local journal
  (PREPARE/ROLLBACK/UNDO).
- **L1. New package `opslog` emitting fb_repl_print-format text by default** (`opsLog.format`
  = `repl-print` (§7) | `tsv` — same content, machine-readable), modeled on `errlog`
  (mutex + bufio, errlog.go:26-33); tool-wide atomic tx counter. The repl-print layout
  satisfies the log contract natively — every line carries the tx number in brackets:
  `[N] START (offset: B, ts: RFC3339, who: worker-4, params: "RC/rec_version/wait/RW;
  lock_timeout=5[, retained_from=M]")`; savepoints map 1:1 (`SAVE`/`UNDO`/`RELEASE` with
  `name:`); DDL goes through `EXECUTE SQL` (matching repl semantics); bulk DML through
  `INSERT/UPDATE/DELETE <table> (rows: K, dur_ms: T)`; completion through
  `COMMIT | ROLLBACK | COMMIT RETAINING | ROLLBACK RETAINING` with the change volume on the
  terminal line `(ins: 2, upd: 0, del: 3, dur_ms: 12.3)`; 2PC renders as `PREPARE` + `COMMIT`
  (exact repl shape); limbo is a `PREPARE` with no terminal plus an empty-bracket resolution
  record (`[] RESOLVED <method> trn=N (gap_sec: S)`); conn-drop is a `START` with no terminal
  plus `[] DROPPED trn=N (tier: 1|2)`. `BLOCK (offset: B, length: L)` marks flush boundaries
  with real byte offsets; the file header mirrors `fb_repl_print -H` (Segment/Version/Guid
  =run id/Sequence =rotation index/State/Length). `dumpRecords=true` adds `-R`-style JSON data
  dumps for bulk rounds. Same shutdown-noise filter as errlog.go:99-102. Single-line
  normalization: newlines/tabs in statement text escaped (`\n`, `\t`) so the `[txN]` line
  grammar and grep patterns survive arbitrary SQL. The pairing check on `Close` scans the whole
  rotated chain, not one file — START and its terminal may straddle a rotation boundary
  (R18); open txs logged as `[N] ROLLBACK (method: abandoned)`. Unlike a replication journal
  (single server-side writer, per-tx contiguous lines), ops.log is one shared stream with
  interleaved txs from N workers/sidecars — per-line `[txN]` attribution is the join key;
  covered by a concurrent-writers test (D1).
- **L2. Rotation.** Size check on every write; at `maxSizeMB` (default 50): flush+close, shift chain
  `ops.log → ops.1.log → ops.2.log → ops.3.log` (delete `ops.4.log`+), reopen. Windows-safe: handle is
  closed before rename. `keepArchives` default 3 (≤3 archived files kept, per task).
- **L3. Restart rename.** On `Open`: if the target exists and is non-empty → run the same shift
  immediately, then write the `fb_repl_print -H`-style header block (§7: Guid = run id,
  Sequence = rotation index) — supersedes the errlog-style single header line (errlog.go:78).
- **L4. Wiring.** CLI: path beside CSV (`errlog.PathFromCSV` pattern, errlog.go:184-191).
  Session mode: `<reportDir>/ops.log` next to `sql_errors.log` (`session/manager.go:903-908`),
  stored on `Session`, closed in `watchCompletion`/`Stop` (manager.go:1021-1024, 1191-1194).
  Hook points: `worker.executeOperation` success/failure paths (worker.go:322, 341-350) and every
  sidecar round (Phases 3-4). `level=all|periodic|errors` gates unit-per-op logging (D3).
- **L5. Log API.** `GET /api/sessions/{id}/logs` → list {name,size,mtime,rotated};
  `GET /api/sessions/{id}/logs/{name}` → content/stream. The existing jail already suffices
  (no extension whitelist, serves the live run dir — V21); only the two routes
  (`ui/server.go:72-148`, `authRead`) and `api/openapi.yaml` are new.

### Phase 2 — Randomized transaction scenarios (all four axes)

A per-transaction scenario is drawn upfront (before `BeginTx`) as four axes:
isolation × access, wait resolution, completion method, extras.

- **T1. Package `ops/txvariants.go`.** `Scenario{Isolation, ReadOnly, LockTimeoutSec, Completion,
  Savepoint, Autonomous}` + built-in matrix (F2) + `Pick(rng, constraints)`; constraints:
  kind read|write (SELECT-kind ops draw RW ∪ RO; write ops and emul units — execute-procedure —
  RW only); profile=oltp-emul ⇒ V3 (nowait or finite lock timeout). Own per-worker/per-sidecar
  `rand.Rand` — `ops/cache.go:14-25` shows a shared unguarded rng; do not repeat that.
- **T2. Reorder worker loop.** Move `profile.NextOpWithName()` (worker.go:290) above `BeginTx`
  (`:278`) — selection is pure, no tx needed — so the scenario can depend on the op kind.
  Replace the fixed if/else at worker.go:270-276 with the picker (`txVariants.mode=off`
  reproduces today's behavior bit-for-bit). The picker lives only in this hook: provisioning,
  fill (`emul/fill.go:59`) and the sidecar goroutines (`emul/state.go:209` — invariants, score
  ticker) keep their fixed `TxOptions()` — the snapshot-demanding totals and the deterministic
  fill must not be randomized.
- **T3. Axes 1–2 — isolation × RO/RW × wait/nowait/timeout.** Matrix from F2; lock-timeout
  seconds drawn from `lockTimeoutChoicesSec`. Modes: `off` (today's behavior), `emul-safe`
  (V3-constrained; default for oltp-emul), `full` (whole matrix; classic profiles).
- **T4. Axis 3 — completion method** (weights in config, normalized):
  `commit`→`tx.Commit()`; `rollback`→`tx.Rollback()`; `commitRetaining`/`rollbackRetaining`→
  F5-encoded TxOptions; the wire tx after commit-retaining IS the next tx (one transHandle per
  conn, transaction.go:95-112), so the picker pins the next scenario on that worker to the
  retained TPB and logs `retained_from` (V17); a chain longer than `retainingChainMax` is forced
  to a plain commit; `twoPhase`→ the tx additionally enlists the aux DB via `EXECUTE STATEMENT …
  ON EXTERNAL DATA SOURCE '<auxDSN>' AS USER … PASSWORD … WITH COMMON TRANSACTION` (one small
  write, works inside unit txs as an extra STMT), commit then runs as engine-side 2PC (F6a);
  `limbo`→ commit under the F5 `7000+iso` intent: prepare on the main DB, then the socket dies —
  no aux DB needed (R15); the recovery sidecar (T9) discovers and resolves it; `connDrop`→
  a single mechanism, the F5 `8000+iso` hard drop (socket closed without rollback, tx left for
  server cleanup) — a plain "rollback + orderly close" is NOT a drop scenario, because every
  rollback already discards the conn (V19, sql.go:2220) and is indistinguishable from the
  rollback completion; after a hard drop the planned-drop flag short-circuits the dead-conn
  worker exit (run loop `worker/worker.go:231-234`) and rebuilds the pool in place via the
  stored connFactory (worker.go:128) — exit+respawn is not an option because ramp never reaps
  workers (V17). `limbo`/`connDrop` additionally rate-limited by `rareCompletionMinGapSec` so
  limbo cannot accumulate faster than T9 resolves it.
- **T5. Axis 4 — extras.** `savepointProb`: inside multi-statement txs (bulk rounds, table
  lifecycle, plus a synthetic "stmt A; SAVEPOINT; stmt B; ROLLBACK TO SAVEPOINT" experiment in
  regular ops; single-statement unit txs get no savepoints) — change-volume accounting counts
  only rows surviving the savepoint rollback; `autonomousCallProb`: an extra STMT in the same tx
  calls aux SP `SP_ELT_AUTON_LOG(msg)` (`IN AUTONOMOUS TRANSACTION DO INSERT INTO EL_AUTON_LOG …`,
  created at H1) — the outer tx may commit or roll back, the autonomous row always survives; a
  validation sidecar asserts that and logs PASS/FAIL, pruning `EL_AUTON_LOG` to the last N
  rounds; `ddlRollbackFrac` applies in plusddl (P8).
- **T6. Visibility.** Scenario logged per tx to opslog (L1: TX_BEGIN params, TX_END method,
  SCENARIO events) and counted into new `variantCounts` / `completionCounts` maps; final report
  gets "Transaction variants" and "Completion methods" tables (variant × attempts × ok × fail ×
  avg-ms). `RecordUnit` semantics untouched so the emul score pipeline (`emul/state.go:120-145`)
  stays comparable.
- **T7. Error handling.** Lock-timeout/deadlock remain expected (V14) and map to the Conflict
  outcome (`emul/run.go:215-238`); limbo/connDrop errors in scenario txs are classified expected
  (planned-drop marker in `ops/errors.go`), and the tier-2 dead-conn error must steer the run
  loop to in-place pool rebuild instead of worker exit (V17); a pathological variant stays
  visible via T6 counters.
- **T8. Classic profiles.** Same picker serves read-heavy/write-heavy/spike (worker hook is
  profile-agnostic); default `txVariants.mode=full` there per task item 4 ("all operations").
- **T9. Limbo recovery sidecar.** Ticker (gap ≤ `rareCompletionMinGapSec`): `gfix -list` parse →
  resolve the oldest limbo (round-robin `-commit|-rollback|-two_phase` per config) →
  `[] RESOLVED <commit|rollback|two_phase> trn=N (gap_sec: S)` line in the opslog (§7).
  `gfix` availability checked at run start (F6b). Fork-native resolution via `op_reconnect`
  is out of scope v1.

### Phase 3 — Heavy SELECT (5 s) + bulk DML (30 s)

- **H1. Aux schema at run start.** In the emul sidecar bootstrap (precedent: `injectSettings`,
  `activateTriggers` — provision.go:155-170): create `EL_BULK_ITEMS(ID ..., PAYLOAD VARCHAR(200),
  VAL NUMERIC(18,2), CREATED_AT TIMESTAMP)` — drop/recreate per launch, in Go (D2). Note: the
  perf_estimated drop/recreate (`oltp_adjust_DDL.sql:38-66`) is the pattern to mirror, but it runs
  only at provision time and Provision skips when already provisioned (provision.go:104-111) —
  the per-launch recreate must live in this bootstrap. Also create the tiny aux DB `EL_2PC.FDB`
  in the main DB's directory via the `emul/createdb.go` helpers (createdb.go:23-86; respects
  `--emul-allow-dir` when set) for the 2PC/limbo scenarios (T4), and aux assets `EL_AUTON_LOG` +
  `SP_ELT_AUTON_LOG` (T5). All DDL/SQL ASCII-only (V11).
- **H2. Heavy SELECT op** (`emul/heavyops.go`): 3-4 table JOIN over real business tables
  (`DOC_LIST ⋈ DOC_DATA ⋈ AGENTS ⋈ WARES` — schema at oltp30_DDL.sql:685-1056) with a random
  parameter, range filter, aggregate + `ORDER BY ... ROWS N` so it touches a large fraction of
  rows; ASCII literals; verified EXPLAIN-cost note in code comment. Runs in its own tx with a
  variant from T1 — **RO and RW both allowed** (task item 4).
- **H3. Heavy sidecar goroutine**: `time.NewTicker(everySec=5)`; dedicated conn from sidecar pool
  (V5); skip tick if previous round still running (no overlap); paused when the workers are
  paused — the sidecar launcher must accept the workers' `PauseGate` (today `RunSidecars`
  takes only ctx, emul/state.go:171); logs round to opslog with variant + duration;
  `RecordTransactionNamed("HeavyJoinScan", …)` into a **separate series** (never into the emul
  unit score).
- **H4. Bulk DML op**: each tick picks random round type ∈ {insert, update, delete} and N uniform
  in [minRows..maxRows] (100..1000): insert = prepared multi-row batches in one tx; update =
  `UPDATE ... WHERE ID BETWEEN` a freshly inserted id range; delete = oldest N own rows. One
  transaction per round, RW variant; completion drawn from T4 (a rolled-back round is logged with
  its full STMT list); optional savepoints between batches (T5). Same sidecar discipline as H3
  (`RecordTransactionNamed("BulkInsert|BulkUpdate|BulkDelete", …)` + rows-affected counters).
- **H5. Reporting.** New "Extended load" section in the final report (`metrics/reporter.go`,
  `session/emul.go:70-95`): per heavy/bulk op — count, ok/fail, avg/p95 (F4 buckets), rows
  affected, variants used. Series also exposed via `GET /api/sessions/{id}/emul/state`
  (`emul/state.go:330-380`).
- **H6. Config/UI.** Fields from §1 plumbed (V8); session panel section "Extended load".
- **H7. Tests.** Sidecar unit tests with fake ticker; non-overlap and pause tests; smoke run vs
  local emul DB confirming invariants stay green with bulk DML active.

### Phase 4 — −plusddl launch variant

- **P1. Flag + plumb.** CLI `-plusddl` (`config.ParseFlags` + `Validate` config.go:182-184),
  `SessionConfig.PlusDDL`, UI checkbox; implies `extendedLoad.enabled`. Launch path:
  `manager.startInternal` emul branch (manager.go:838-859) and `runCLI` (main.go:250-285)
  start the DDL sidecar alongside `RunSidecars` (extend `emul/state.go:171-279` signature or
  add `emul.RunDDLSidecar`).
- **P2. Column churn (every 60 s).** Pick a working table from `plusDDL.workTables`; random action
  on **own** prefixed columns only (`colPrefix=TST_`): `ALTER TABLE t ADD TST_x <type>`;
  `ALTER TABLE t ALTER COLUMN TST_x TYPE …` (within safe type set); `ALTER TABLE t DROP TST_x`.
  Ownership: in-memory registry + on-start discovery
  `select rdb$field_name from rdb$relation_fields where rdb$relation_name = ? and rdb$field_name starting with 'TST_'`
  so restarts re-own leftovers. Each DDL in its own committed transaction (autocommit semantics,
  unlike provision-time `RunScript`). All DDL text + duration + result → opslog.
- **P3. Table lifecycle (alternating rounds).** Round type A (create): `CREATE TABLE TST_<n>`
  (2-5 random columns) + all six triggers `BI/AI/BU/AU/BD/AD` with test bodies (BI sets NEW.col,
  AI logs to side table `EL_DDL_LOG`, BU guards a value, AU logs, BD checks remaining rows,
  AD logs). Same round, then **in separate transaction(s)**: insert 100 → update 50 → delete 30
  rows; grouping drawn randomly each time from {all three in one tx | each in own tx |
  [insert+update][delete]} — grouping choice logged. Round type B (drop): drop one owned
  `TST_<n>` table (triggers go with it). A/B strictly alternate so created tables always die later.
- **P4. Expected-error classification.** Extend `ops/errors.go:55-130` patterns for plusddl:
  "object … is in use", metadata lock conflicts → expected-under-load; DDL-vs-units collisions are
  the point of the test, but must not trip `halt_test_on_errors` (settings /CK/, provision.go:233-258)
  nor the invariant 3-strike logic (state.go:197-235).
- **P5. Observability.** DDL round history (last N rounds: time, table, action, DDL text, tx
  grouping, rows, outcome) kept in `EmulState` + exposed via `GET /api/sessions/{id}/emul/state`;
  counters (columns added/altered/dropped, tables created/dropped, trigger firings) in the final
  report "PlusDDL" section. Errors additionally to `sql_errors.log` (existing errlog).
- **P6. Logs via API.** Covered by L5 — plusddl rounds write to the same session `ops.log`;
  `GET /api/sessions/{id}/logs` lists/returns them.
- **P7. Tests.** Column-churn and table-lifecycle round-trips vs local FB3/4/5; concurrent unit
  run during DDL churn (worker pool active) with failure-rate assertion; restart re-ownership test.
- **P8. DDL inside transactions + deliberate rollbacks.** A fraction `ddlRollbackFrac`
  (default 0.20) of column-churn actions runs in a tx that is **rolled back** after the DDL
  (Firebird DDL is transactional): the tool then verifies the column is absent (probe of
  `rdb$relation_fields`) and logs `TX_END method=rollback_ddl` with the verification result.
  Table-lifecycle rounds (P3) already mix multi-statement DML + DDL inside explicit txs.

### Phase 5 — Tests, docs & specs

- **D1. Opslog tests.** Golden test: scripted scenario sequence → exact expected repl-print text
  (fixture file); `repl-print`/`tsv` fact-equivalence (parse both, identical extracted facts);
  single-line escaping under multi-line SQL; rotation chain (statement continuity and numbering
  across rotation); concurrent writers under `-race` (no torn lines, every `[N]` accounted);
  pairing check incl. a START/terminal pair straddling a rotation boundary (R18);
  `dumpRecords` JSON shape.
- **D2. Fork-side tests** (in the IBSurgeon/firebirdsql-go repo): byte-exact TPB buffers for
  every new preset (pure `tpbForIsolationLevel` tests); decode tests for encoded levels
  (`2000+n`, `5000+`, `6000+`, `7000+`, `8000+`); `op_prepare` and hard-drop against local
  FB3/4/5; retaining reuse — `BeginTx` over a retained context, plus the TPB-mismatch path
  (R14).
- **D3. Tx-variant tests.** Constraint table: RO drawn only for read ops; `emul-safe` ⊆ V3-legal
  set; `full` matrix complete; seeded-rng smoke (N draws hit every variant); `mode=off` +
  `extendedLoad.enabled=false` reproduce today's behavior (existing smoke output comparable,
  profile validation untouched).
- **D4. Scenario integration** (local FB3/4/5, scripted weights): log↔metrics cross-check
  (`variantCounts`/`completionCounts` == paired log counts); limbo: create K → `gfix -list`
  shows exactly K → T9 resolves all → K `[] RESOLVED` lines; autonomous validation (outer
  rollback, autonomous row survives); savepoint volume accounting (survivor counts); DDL
  rollback probe (`rdb$relation_fields`).
- **D5. Lifecycle.** Sidecars honor the pause gate and are cancelled by ctx on Stop and natural
  finish (no goroutine/pool leaks — mirror the `cleanupLocked` test); ramp phases and the final
  report unaffected; the report dir contains the ops.log chain.
- **D6. API/auth.** `GET /api/sessions/{id}/logs[/{name}]`: 200 with token, 401 without
  (mirror `ui/emul_routes_test.go`); path traversal rejected.
- **D7. Docs.** `api/openapi.yaml` (logs endpoints, extendedLoad config fields, emul/state
  additions); README section "Extended load mix (−plusddl)" with the classic-preset caveat
  (§0.1 D1); opslog format note pointing at §7.

## 3. Combination matrix (task requirement: what must be verifiable)

| Axis | Variants | Implemented in | Verified by |
|------|----------|----------------|-------------|
| Isolation × access | RC/rec_version, RC/no_rec_version, snapshot, consistency × RW/RO | F1–F2 (fork, incl. the BeginTx ReadOnly fix — V18), T3 | F3/F6 probes on FB3/4/5; per-variant counters; opslog `params=` |
| Wait resolution | wait / nowait / wait+`isc_tpb_lock_timeout(n)` | F1, T3 | same; emul units stay V3-constrained |
| Completion (local) | commit, rollback, commit retaining, rollback retaining | F5 + T4 | opslog `TX_END method=`; `retained_from` chaining; "Completion methods" report table |
| Completion (distributed) | 2PC via `EXECUTE STATEMENT … WITH COMMON TRANSACTION`; limbo (prepare-then-die, single-db); recovery | T4 + T9 | `[] RESOLVED` lines (limbo found → resolved: trn id, method, gap_sec); F6a/b probes |
| Completion (connection) | hard conn drop with open tx (`F5 8000+iso`: socket closed without rollback); plain rollback is not a drop — it is its own completion (V19) | T4 | worker rebuilds pool in place and continues; `[] DROPPED` line; no worker exit (V17) |
| Savepoints | set / rollback to / release inside multi-stmt txs | T5 | volume accounting excludes rolled-back rows; `SAVEPOINT` events in opslog |
| Autonomous tx | aux SP `IN AUTONOMOUS TRANSACTION DO` vs outer commit/rollback | T5 | validation sidecar: autonomous row survives outer rollback, PASS/FAIL logged |
| DDL in tx | committed DDL vs deliberately rolled-back DDL | P2, P8 | `rdb$relation_fields` probe after rollback; `method=rollback_ddl` |
| Log contract | tx number, params, completion, change volume, start time; stmt→tx reference — rendered in fb_repl_print format (§7) | L0–L1 | START↔terminal pairing check on close; every line carries `[txN]`; `format=tsv` for machine parsing |

## 4. Risks

| Risk | Mitigation |
|------|-----------|
| DDL churn invalidates prepared statements in active workers → unit failure spikes (esp. FB3) | P4 classification + failure-rate guard; test matrix on FB3/4/5 before defaulting on |
| opslog I/O at ~10³ ops/s | bufio + level knob (D3); flush thresholds as in errlog |
| `isc_tpb_lock_timeout` semantics on wire (seconds vs param format) differ per ODS | F1 wire check in Phase 0 on FB3/4/5 |
| Bulk DELETE GC pressure | own fresh table only, oldest-first deletes, rows capped at 1000/round |
| Windows rename-with-open-handle on rotation | close→shift→reopen in opslog (L2) |
| Snapshot isolation for units unproven | V4/F3 probe; matrix gating until proven |
| Retaining semantics through `database/sql` (Commit() takes no args, V15) | begin-time TxOptions encoding (F5); retained context pinned to 1-conn worker pools (V17); F6d probe before enabling weights |
| Limbo accumulation if recovery stalls | limbo/connDrop rate-limited (T4); recovery sidecar gap ≤ `rareCompletionMinGapSec` (T9); gfix availability checked at start (F6b) |
| 2PC-via-external-statement behavior differs per ODS (FB3 vs FB5) | F6a probe; `twoPhase`/`limbo` weights forced to 0 when unsupported |
| Log volume: 2–3 events per tx at ~10³ tx/s | level knob (D3) + sampling option; rotation already designed (L2) |
| ramp never reaps exited workers (pre-existing): any unplanned dead conn silently shrinks load for the rest of the run | connDrop tier-2 rebuild must be bulletproof (T4); worker reaping is a separate follow-up fix outside this plan |
| Binary/fork coupling: new IsolationLevel intents require the updated fork (unknown levels fail loudly today) | same-PR `go.mod:12` pin bump (F1); startup probe rejects an old fork with a clear error |
| Every rollback already closes the conn (V19): frequent scenario rollbacks add connect churn to the load profile itself | intended side effect (part of the stress); visible in metrics via connection-count series |
| Retaining reuse: `fc.begin()` unconditionally opens a new wire tx — a missed reuse path collides with the retained context ("multiple simultaneous transactions") | F5 specifies the `fc.tx.needBegin` reuse branch with TPB validation (connection.go:65-69); D2 test covers reuse + mismatch |

## 5. Out of scope (explicitly)

- Multi-op transactions for regular emul units (units are single execute-procedure by design).
- Changing the score formula or warmup/main/cooldown phases.
- WAIT (infinite) tx for emul units — rejected server-side by design (V3); reachable only in
  classic profiles under `txVariants.mode=full`.
- `isc_start_multiple` multi-database attachments in the fork — 2PC is exercised via
  `EXECUTE STATEMENT … WITH COMMON TRANSACTION` instead (T4).
- Fork-native limbo resolution (`op_reconnect`) — gfix subprocess in v1 (T9).
- Emitting a real binary replication-journal segment (so `fb_repl_print` could parse the opslog
  directly) — the opslog mimics the utility's *output* format instead (§7); revisit as a stretch.

## 6. Review findings (rev. 3: R1–R12; rev. 5: R13–R21 — all fixed in place)

Every claim of rev. 2 was re-verified against source: the IBSurgeon driver fork
(`C:\Users\aleks\go\pkg\mod\github.com\!i!b!surgeon\firebirdsql-go@v0.0.0-20260828141318-badf2230e80d`),
GOROOT `database/sql`, `ramp/`, `worker/`, `session/`, `ui/`, and the emul assets.

| # | Finding | Evidence | Fix in this rev |
|---|---------|----------|-----------------|
| R1 | Plan implied snapshot/consistency+RO presets exist; in fact `opts.ReadOnly` **overrides** the level — ReadOnly+snapshot silently becomes RC RO today | fork `driver_go18.go:56-63` | F2 reworks the BeginTx RO branch (V18); matrix updated |
| R2 | connDrop assumed "worker closes pool, rebuilds, continues" without a feasibility check; ramp never reaps exited workers, so exit+respawn is impossible and in-place rebuild is mandatory | `ramp/ramp.go:395-448` (workers list only grows / truncates by target); `worker/worker.go:128` (connFactory retained) | T4/T7: planned-drop flag + in-place rebuild (V17); pre-existing degradation added to Risks |
| R3 | Plan missed that every plain `tx.Rollback()` already closes the driver conn — the fork implements neither `SessionResetter` nor `Validator`, so `keepConnOnRollback=false`; a free "orderly drop" tier and an explanation of existing reconnect churn | GOROOT `database/sql/sql.go` `beginDC`; fork grep for both interfaces empty | connDrop split into tier 1 (free) / tier 2 hard drop (V19, F5 `8000+iso`) |
| R4 | `twoPhaseAuxDB` was filed under `plusDDL` although 2PC/limbo run without plusddl | §1 config | moved into `txVariants` |
| R5 | Retaining-chain semantics unspecified: after commit-retaining the wire tx IS the next tx — an unrelated BeginTx would collide with the live wire transaction | fork `transaction.go:95-112` (one transHandle per conn) | picker pins the next scenario to the retained TPB; `retainingChainMax` cap |
| R6 | Log-API plan proposed extending the path jail; the jail has no extension whitelist and already serves the live run dir | `session/reports.go:58-66, 86-111` | L5 reduced to two routes + openapi (V21) |
| R7 | "perf_estimated drop/recreate per launch" conflated provision time with run start: Provision skips when already provisioned | `emul/provision.go:104-111`; `oltp_adjust_DDL.sql:38-66` | H1: per-launch recreate lives in the Go run-start bootstrap |
| R8 | gfix limbo switches were assumed | gfix(1) man page (firebird3.0) | confirmed `-list/-commit/-rollback/-two_phase` (V20); F6 adds online-resolution probe + services-manager fallback |
| R9 | AGENTS/DOC_STATES work-table defaults were assumed | `oltp30_DDL.sql:753,929,938` | confirmed present |
| R10 | Encoded-IsolationLevel passthrough was asserted, not proven; unknown levels would silently mis-map | GOROOT `driver/driver.go:269` (int), `sql.go` `ctxDriverBegin`; `driver_go18.go:80-83` (loud error) | verified numeric (V15); same-PR fork pin bump required (F1) |
| R11 | Line-ref drift: RecordTransactionNamed paths cited as 318 (actual 322), dead-conn exit cited only at the BeginTx path (the run-loop exit at 231-234 is the binding one), F1 pointed at "verify in F4" (latency buckets) instead of F6 | `worker/worker.go:231-234, 322, 341-350` | refs corrected |
| R12 | `op_prepare` absence had only been checked in transaction.go | consts.go:532 has only `op_prepare_statement`; wireprotocol.go:1436 | confirmed absent across the fork (V16) |
| R13 | connDrop "tier 1" (rollback + orderly close) is not a distinct scenario: every plain rollback already discards and closes the conn — tier 1 ≡ the rollback completion | `sql.go:2220` (`discardConnection := !tx.keepConnOnRollback`) | connDrop reduced to the single hard-drop mechanism (T4/§7/matrix) |
| R14 | Retaining reuse is not reachable through `BeginTx` as planned: `fc.begin()` unconditionally starts a new wire tx, which collides with the retained live context | fork `connection.go:65-69` (`newFirebirdsqlTx(..., withBegin=true)` always) | F5 adds the mandatory reuse branch (`fc.tx != nil && !fc.tx.needBegin`) with TPB validation; D2 test |
| R15 | Limbo does not need the aux DB: a prepared single-db tx whose conn dies is limbo, gfix-visible and resolvable | Firebird recovery semantics; F6b probe confirms | limbo simplified to prepare-then-die on the main DB; `EL_2PC.FDB` kept only for twoPhase |
| R16 | Hard-drop intent must also short-circuit `Conn.Close()`'s rollback loop, else pool teardown rolls the tx back before the socket dies — the crash degrades into an orderly close | fork `connection.go:81-92` (`for tx := range fc.transactionSet { tx.Rollback() }`) | F5: intent consulted by both `Rollback()` and the Close loop |
| R17 | T9 said "SCENARIO event" while L1/§7 had standardized on `[] RESOLVED …` records | text drift | unified |
| R18 | Pairing check "within one segment" breaks when a START and its terminal straddle a rotation boundary | L1/§7 | check scans the whole rotated chain |
| R19 | Multi-line SQL would break the `[txN]` line grammar (both formats) | — | single-line normalization (escape `\n`, `\t`) in L1 |
| R20 | Picker scope was implicit; provisioning, fill and sidecar goroutines must keep fixed options | `emul/fill.go:59`, `emul/state.go:209` | T2 states the exclusion with refs |
| R21 | Opslog interleaving semantics (N writers → one shared stream) differ from a repl journal's contiguous per-tx blocks | — | documented in L1; concurrent-writers test (D1) |

## 7. Appendix — fb_repl_print format (studied live, 2026-09-26)

Utility: `fb_repl_print` ships with Firebird 4/5; local copy and its manual
(`fb_repl_print.md`) live at `C:\HQbird\Firebird40\`. The replication log on disk is a
directory of binary segments (`EMPLOYEE.FDB.ReplLog\EMPLOYEE.FDB.journal-000000032` in this
repo). Live capture with `-H` (verbatim):

```
================================================================================
Segment: EMPLOYEE.FDB.journal-000000032
Version: 1
Guid: {EB7D176C-2300-4B9B-90A2-8A5B99FDEAA9}
Sequence: 32
State: free
Length: 223455
SegmentHeaderSize: 48
================================================================================


BLOCK (offset: 48, length: 207, BlockHeaderSize: 16)
[37614237] START (offset: 64)
[37614237] SAVE (offset: 65)
[37614237] SAVE (offset: 66)
[37614237] SAVE (offset: 67)
[37614237] UPDATE DEPARTMENT (orgLength: 88, newLength: 88) (offset: 80)
[37614237] RELEASE (offset: 269)
[37614237] RELEASE (offset: 270)
BLOCK (offset: 271, length: 2, BlockHeaderSize: 16)
[37614237] RELEASE (offset: 287)
[37614237] COMMIT (offset: 288)
```

### 7.1 Event vocabulary (fb_repl_print.md + live capture)

START; PREPARE (two-phase prepare); COMMIT; ROLLBACK; SAVE (savepoint set); UNDO (rollback to
savepoint); RELEASE; SET SEQUENCE gen = n; EXECUTE SQL — DDL statements only in repl logs;
BLOB id (length n) followed by a hex body; INSERT / UPDATE / DELETE table (record lengths;
`-R` dumps Old/New Data as JSON, `-F` prints the record-format table, `-B` prints blobs).
Empty brackets `[]` = action outside transaction control.

### 7.2 Opslog mapping (format = `repl-print`)

| Opslog event | Line (deliberate extensions: `ts`, `dur_ms`, `params`, volumes) |
|---|---|
| tx begin | `[N] START (offset: B, ts: RFC3339, who: worker-4, params: "RC/rec_version/wait/RW; lock_timeout=5[, retained_from=M]")` |
| statement | `[N] EXECUTE SQL <sql> (offset: B, rows: K, dur_ms: T)` — repl logs DDL-only here; we extend to all SQL. Bulk rounds additionally emit literal record lines: `[N] INSERT EL_BULK_ITEMS (rows: 500, dur_ms: 8.1)` |
| savepoint | `[N] SAVE (offset: B, name: SP1)` / `[N] UNDO (…, name: SP1)` / `[N] RELEASE (…, name: SP1)` — 1:1 with repl |
| commit | `[N] COMMIT (offset: B, ins: 2, upd: 0, del: 3, dur_ms: 12.3)` |
| rollback | `[N] ROLLBACK (…)`; at shutdown `[N] ROLLBACK (method: abandoned)` |
| commit retaining | `[N] COMMIT RETAINING (…)` / `[N] ROLLBACK RETAINING (…)` — extension keyword |
| 2PC | `[N] PREPARE (…)` then `[N] COMMIT (…)` — the exact repl two-phase shape |
| limbo | `[N] PREPARE (…)` with no terminal event; T9 resolution logs `[] RESOLVED <commit\|rollback\|two_phase> trn=N (gap_sec: S)` — empty brackets mirror repl's outside-tx actions |
| conn drop | `[N] START` with no terminal + `[] DROPPED trn=N (socket closed without rollback)` — the only distinct drop scenario; a plain rollback already discards the conn (V19) |
| file header | mirrors `-H`: Segment (file name), Version: 1, Guid (run id), Sequence (rotation index), State, Length |
| blocks | `BLOCK (offset: B, length: L)` at flush boundaries, real byte offsets |

Everything except the listed extensions stays shape-compatible with `fb_repl_print` output,
so replication-log reading habits (and grep patterns: `^\[\d+\] (?!COMMIT|ROLLBACK)`,
START-without-terminal scans for in-flight/limbo txs) work on `ops.log` unchanged.
The pairing check from L1 reduces to: within one segment, every `[N] START` has a terminal
event line with the same `[N]`.
