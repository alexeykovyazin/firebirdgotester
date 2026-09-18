# Borrowing oltp-emul into fb-loadgen — Integration Plan (v2)

Source studied: https://github.com/FirebirdSQL/oltp-emul (cloned to `E:\Projects_2026\oltp-emul`, not committed here).
UI reference: https://www.firebirdtest.com/oltp-emul-fb/ (IBSurgeon's public benchmark dashboard).

**Decisions locked (2026-09-18):**
1. **License: MIT** — Pavel Zotov confirms the test is MIT (written confirmation pending; embed his reply or a
   link to it when received). Verbatim embedding of DDL/SP scripts is approved with attribution (see §5).
2. **Target servers: Firebird 3.0, 4.0, 5.0 first.** One script flavor covers all three
   (`oltp30_DDL.sql` + `oltp30_sp.sql` + `oltp_common_sp.sql` are shared by the FB3/4/5/6 configs upstream).
   FB 2.5 (separate script flavor) and 6.0 are out of initial scope.
3. **UI: firebirdtest.com-style dashboard** — performance score, four memory-peak charts, cross-run results
   table with per-run drill-down (see §7).

Goal: give fb-loadgen a *realistic business-process* workload mode ("oltp-emul profile") that complements the
existing single-statement EMPLOYEE profiles — reusing oltp-emul's database-side assets and measurement ideas while
keeping all orchestration in Go (worker pool, ramp, UI, REST, schedules, sessions).

---

## 1. What oltp-emul actually is

There is **no compiled client**. The "load generator" is bash/batch scripts that:

1. Create the test database via isql: `oltp30_DDL.sql` (page size 8192 hardcoded) + `oltp30_sp.sql` +
   `oltp_common_sp.sql`.
2. Fill it with initial documents (`init_docs`) — essentially *creation-kind units run in a loop*
   (`srv_random_unit_choice` with `included_kinds = 'creation,state_next,service'`, removals excluded; the
   warmup logic in the same SP confirms the pattern). Dictionary seed data (wares/phrases/word patterns) comes
   from `oltp_data_filling.sql`.
3. Launch N isql sessions running a generated execute-block loop that, per iteration:
   - picks the next business operation via `srv_random_unit_choice` — weighted random over the `business_ops`
     registry (`unit, sort_prior, mode, kind, random_selection_weight`),
   - `EXECUTE PROCEDURE`s that unit — the business transaction lives entirely in server-side SPs; most units take
     no arguments (randomness is server-side: `rand()`, qdistr/pdistr quantile tables),
   - records timing into the `tmp$perf_log` GTT → fixed `perf_log` (autonomous-transaction flush preserves failed
     units' stats; commits are per unit),
   - sleeps `sleep_min..sleep_max` between transactions (UDF or shell — irrelevant for us),
   - checks stop flag (sequence `g_stop_test` / stop-file) and the `perf_watch_interval` warmup→measurement window,
   - optionally reconnects every `actions_todo_before_reconnect` iterations.
4. After `warm_time` + `test_time`, session #1 emits the final report via report SPs.

**Workload model** — a car-service supply business (C) Pavel Zotov, 2014–2019: customer orders → pooled supplier
orders → supplier invoices → stock intake → reservations → retail sales/write-offs, plus payments against
customer/supplier balances; every operation has a cancellation counterpart, so the model grows *and* churns.

| unit (SP)               | mode     | kind        | meaning                          |
|-------------------------|----------|-------------|----------------------------------|
| sp_client_order         | stock    | creation    | customer orders a set of parts   |
| sp_supplier_order       | stock    | creation    | we order from supplier           |
| sp_supplier_invoice     | stock    | creation    | supplier invoices us             |
| sp_add_invoice_to_stock | stock    | state_next  | invoice content → stock          |
| sp_customer_reserve     | stock    | state_next  | reserve parts for sale           |
| sp_reserve_write_off    | stock    | state_next  | sale + stock write-off           |
| sp_pay_to_supplier / sp_pay_from_customer | payments | creation | money movements |
| sp_cancel_*             | respective | removal   | roll an operation back           |
| srv_make_invnt_saldo / srv_make_money_saldo | service | service | **invariant self-checks** |
| srv_fill_mon, srv_fill_mon_cache_memory, srv_recalc_idx_stat | service | service | monitoring snapshots, index-stat recalc |

Selection nuance worth copying: during warmup `removal` kinds are excluded so the database grows; the
measurement phase adds cancellations so it churns.

**Measurement layer**: `tmp$perf_log` (GTT) → `perf_log` → `perf_agg` → report SPs (`report_perf_total`,
`report_perf_dynamic`, `report_perf_per_minute`, `report_perf_detailed`, `report_stat_per_units`). Headline =
**performance score: successfully completed business actions per minute**. Memory monitoring snapshots
(`srv_fill_mon*`, `mon_cache_memory`) capture `mon$memory_used` peaks at four levels — DB, attachments,
transactions, statements — which is exactly what firebirdtest.com charts.

## 2. What to borrow

| # | Borrow | Why |
|---|--------|-----|
| 1 | Business-process transactions (multi-statement SPs with document state machine + cancellations) | Current profiles are single statements; no realistic lock conflicts, garbage, or update conflicts. |
| 2 | The oltpemul schema + SPs as a provisionable benchmark database | Battle-tested workload with realistic data distributions. MIT-licensed (§5). |
| 3 | Registry-driven weighted unit mix (`business_ops`) | Maps onto our `OpWeight` concept but data-driven: weights editable via UI/REST per run. |
| 4 | Performance score (successful business actions/min) + per-unit breakdown | Statement-level op counts become comparable with the oltp-emul benchmark line — subject to §8 comparability caveat. |
| 5 | Warmup semantics (creation-only warmup, churn in measurement) | DB grows first, then conflict stress. |
| 6 | Invariant self-checks (`srv_make_invnt_saldo/money_saldo`) | Detects generator-corrupted business state; we have nothing like it. |
| 7 | `mon$memory_used` peak snapshots at 4 levels | Powers the firebirdtest.com-style memory charts; needs a monitor goroutine (see §7). |
| 8 | Exception classification (deadlock/update_conflict = normal load events; `fb_errors` list) | errlog today doesn't separate expected-under-load conflicts from real failures. |

## 3. What NOT to borrow

isql/bash orchestration, per-session log files, UDF sleeps, fbsvcmgr/gdb/gstat shell-outs, the per-FB-version
config-file matrix, and the SP-side stop machinery (`g_stop_test`, stop-file, external-table stop) — Go contexts,
ramp, and the UI replace all of it. Dropping the stop-flag machinery also drops `sp_pause` (its only consumer);
think-time is a Go sleep between transactions.

## 4. License & attribution — resolved

- MIT confirmed by Pavel Zotov (formal written confirmation pending — record it in this file when received).
- Embed the scripts verbatim under `emul/assets/`, each file keeping its original header; add `NOTICE` with
  origin repo, author credit, MIT text, and a pointer to the upstream commit the files were taken from.
- Attribution also appears in the web UI (emul panel footer) and in every emul report header.

## 5. Target design

```
emul/                      new package
  assets/                  embed.FS: oltp30_DDL.sql, oltp30_sp.sql, oltp_common_sp.sql,
                           dictionary seed portion of oltp_data_filling.sql  (verbatim copies + NOTICE)
  isqlscript.go            quote-aware script splitter: understands SET TERM ^/;^ blocks,
                           skips SET LIST/ECHO/BAIL/AUTODDL, commits after each DDL statement.
                           MUST handle `^` inside string literals (sp_split_into_words's delimiter
                           default contains one) and doubled quotes. Golden-file tests vs real scripts.
  provision.go             Provision(dsn, pageSize=8192) — driver spike decides mechanism (§6);
                           verify object counts afterwards (rdb$relations / rdb$procedures vs expected)
  fill.go                  seed dictionaries from assets, then loop creation units until init_docs;
                           progress + cancellation callbacks (no 17k-line script parsing on the hot path)
  units.go                 LoadUnits(db) from business_ops → []Unit{Name, Mode, Kind, Weight};
                           PickUnit(rng, phase) — warmup excludes kind=removal (Go-side weighted pick)
  run.go                   ExecuteUnit(ctx, tx, unit): EXECUTE PROCEDURE (no args for most units —
                           verify signatures per unit in the spike); error classification into
                           ok | conflict (deadlock 335544336, update_conflict 335544451, …) | failure
                           via the driver's GDS-code error surface (spike: wireprotocol.go collects
                           gdsCodes; confirm the public error type + errors.As)
  ctxinit.go               per-connection init: set USER_SESSION context vars the SPs read
                           (ORDER_FOR_OUR_FIRM_PERCENT, ENABLE_RESERVES_WHEN_ADD_INVOICE,
                           WORKER_SEQUENTIAL_NUMBER, …) — exact list extracted from SP sources;
                           unset context silently changes unit behavior, this step is mandatory
  monitor.go               monitor goroutine + dedicated connection (not a load worker): every N s
                           snapshot mon$memory_used peaks at 4 levels (DB/att/trn/stm), feed metrics
  invariant.go             periodic srv_make_invnt_saldo/money_saldo runs, surface pass/fail
profile/oltp_emul.go       profile built dynamically: LoadUnits → OpWeight closures calling
                           emul.ExecuteUnit. ProfileFactory gains the emul runner dependency.
                           Requires live DB at construction (units come from business_ops) — one more
                           reason provisioning is a separate action, not dry-run.
config/config.go           --profile oltp-emul --emul-dsn <host/port:server-side-path>
                           --emul-init-docs N --emul-invariant-every N --emul-reconnect-every N
session/config.go + UI     same fields in the run-config JSON schema and run form; without them
                           the UI cannot start an emul run
metrics/reporter.go        per-unit counters, score line (successful units/min — conflicts NOT counted,
                           matching oltp-emul), latency per unit; CSV gains a unit column
history/                   persist per-run emul results (score, 4 memory peaks, FB version/build,
                           config echo, status) so cross-run tables/charts survive restarts —
                           extends the existing run history store
```

Guard rails:
- **Schema guard**: at run start, verify `business_ops` exists; else fail fast with "provision first".
- **One run per oltpemul DB**: session engine refuses a second concurrent run on the same database (per-DB
  status endpoint is the hook); oltp-emul's `WORKER_SEQUENTIAL_NUMBER` separation exists for the same reason.
- **Provision action is separate** from dry-run: `fb-loadgen provision --emul-dsn ... --emul-init-docs N`
  subcommand AND `POST /api/dbs/{id}/emul/provision` (async job: create → scripts → fill, with progress and
  cancel). Dry-run keeps its contract: print config, touch nothing.

## 6. Driver spike — RESOLVED (2026-09-18)

Verified against `IBSurgeon/firebirdsql-go` (v0.0.0-20260828114643) in the module cache:
- **Database creation: already supported by the fork.** A registered driver variant
  `sql.Open("firebirdsql_createdb", dsn)` creates the database on connect. Its `opCreate` hardcodes page
  size 4096, so to match oltp-emul's page size 8192 the provisioner creates via `firebirdsql_createdb`,
  then runs the fork's services manager (`NewBackupManager` → backup to temp .fbk → restore with
  `RestoreOptions{PageSize: 8192, Replace: true}`). Fully in-driver, no fork changes, no isql dependency.
- **Error classification: solved.** `*firebirdsql.FbError` via `errors.As` exposes `GDSCodes []int`
  (+ SQLCode/SQLState/params); constants `ISCDeadlock = 335544336`, `ISCUpdateConflict = 335544451`.
- **Charset**: to verify live in phase-1 smoke (DB is created `CHARACTER SET NONE`; our connections are UTF-8).

## 7. UI — firebirdtest.com-style dashboard

The web UI gains an "OLTP-EMUL" section modeled on https://www.firebirdtest.com/oltp-emul-fb/:

1. **Performance score** headline + score-over-time chart across runs (per DB).
2. **Four memory-peak charts** (`mon$memory_used`, MB): DB level, attachments, transactions, statements —
   one series each across runs (within a run: peaks per interval for the live view).
3. **All results, table**: run date, FB version + build string (`rdb$get_context('SYSTEM','ENGINE_VERSION')`
   + server build), score, memory × 4, status (normal / aborted / invariant-failed) — matching the reference
   table's per-version columns, adapted per database instead of per server version.
4. **Per-run drill-down** (click a row): config echo (init_docs, weights, sleep, durations, attachment count),
   per-unit table (count ok/conflict/fail, avg/max ms), per-minute dynamics, exception list, invariant results,
   link to the full report file.
5. Provision/fill panel: start provision job, progress, unit registry view with editable weights
   (`PUT /api/dbs/{id}/emul/weights`), invariant status.

Data sources: live numbers from the in-memory collector; historical rows from the persistent run history
(§5 `history/`); memory peaks from `emul/monitor.go`. REST endpoints documented in `api/openapi.yaml`.

## 8. Comparability caveat (scoring)

oltp-emul measures unit elapsed server-side (dts_beg/dts_end defaults in perf_log); we measure around the client
round trip. Scores from fb-loadgen and from isql-based oltp-emul will therefore differ systematically. Two
mitigations, both in scope: (a) also write per-unit rows into their `perf_log` (via the `v_perf_log` view) when
running against an oltpemul DB, keeping the original report SPs usable for DBA-side verification; (b) acceptance
test = side-by-side run of original oltp-emul vs our profile, same machine/config, scores within stated
tolerance. Always record init_docs, weights, sleep, durations, attachment count in the report header — the score
is meaningless without them (oltp-emul encodes this in its `score_XXXXX_...` report filenames).

## 9. Phases

**Phase 0 — license artifact.** Record Pavel Zotov's MIT confirmation in this file; add NOTICE + attribution.

**Phase 0.5 — driver spike (§6).** CreateDatabase path decided and working (or fallback chosen); error
classification proven on a real deadlock; charset round-trip checked. Est: 1–2 days (fork work may add latency —
parallel-track it).

**Phase 1 — provision & fill.** Quote-aware splitter + golden tests (no server needed in CI — parse-level only;
live smoke is manual against local FB 3/4/5), `Provision()` with post-verification object counts, context-init
step, fill-via-units with progress/cancel, `provision` subcommand. Est: 3–5 days.

**Phase 2 — run mode.** Unit loading, weighted pick, profile wiring, warmup excludes removal, error
classification in the loop, metrics per unit, schema guard, one-run-per-DB guard. Deliverable: a 15-min run
produces score + per-unit CSV. Est: 2–3 days.

**Phase 3 — reporting, monitoring, UI.** Score in reporter, monitor goroutine with 4 memory levels, persistent
run history rows, dashboard (§7), weights editor + provision job in UI/REST, openapi.yaml updates. Est: 4–6 days.

**Phase 4 — optional / later.** Invariant-check UI surface refinements; perf_log writes for original report SPs
(may land in phase 3 if cheap); FB 2.5 flavor; FB 6.0 when stable; periodic reconnects (expose
reconnect-every-N; conn cycling exists); reset-via-restore action on the fork's services manager (makes nightly
scheduled runs reproducible); gstat/firebird.log post-run collection when tools are present.

## 10. Risks / gotchas

- **Splitter correctness** — `^` inside string literals, SET commands, AUTODDL commit semantics; golden tests
  mandatory; mis-parse silently corrupts the DB, so post-provision object-count verification is part of
  Provision(), not optional.
- **Driver gaps** — CreateDatabase/classification/charset are spike-gated (§6); isql fallback keeps phase 1
  unblocked either way.
- **Context variables** — unset USER_SESSION vars silently change unit behavior; ctxinit.go is mandatory, and
  the variable list must be re-checked when updating vendored scripts.
- **Concurrency** — never share one oltpemul DB between concurrent runs; enforced by the session engine.
- **Score comparability** — client-side vs server-side measurement (§8); side-by-side validation is the
  acceptance gate.
- **Fill duration** — init_docs filling is I/O heavy; must be cancellable, report progress, and the documented
  workflow for repeatable runs is "provision once, back up, restore per run" (restore action itself is phase 4).
