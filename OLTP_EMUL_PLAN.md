# Borrowing oltp-emul into fb-loadgen — Integration Plan

Source studied: https://github.com/FirebirdSQL/oltp-emul (cloned to `E:\Projects_2026\oltp-emul`, not committed here).
Goal: give fb-loadgen a *realistic business-process* workload mode ("oltp-emul profile") that complements the existing
single-statement EMPLOYEE profiles, reusing oltp-emul's database-side assets and measurement ideas while keeping all
orchestration in Go (worker pool, ramp, UI, REST, schedules, sessions).

---

## 1. What oltp-emul actually is

There is **no compiled client**. The "load generator" is a set of bash/batch scripts that:

1. Create the test database by running `oltpNN_DDL.sql` + `oltpNN_sp.sql` / `oltp_common_sp.sql` through isql
   (page size 8192 hardcoded; FB 2.5 / 3.0 / 4.0 / 5.0 / 6.0 variants).
2. Fill it with initial documents (`init_docs` count) via `oltp_data_filling.sql`.
3. Launch N isql sessions; each runs a generated "big SQL" execute-block loop that, per iteration:
   - picks the next business operation with `srv_random_unit_choice` — a **weighted random choice** over the
     `business_ops` registry table (columns: `unit, sort_prior, mode, kind, random_selection_weight`),
   - `EXECUTE PROCEDURE`s that unit (the actual business transaction lives entirely in server-side SPs),
   - records start/finish into the `tmp$perf_log` GTT (flushed to fixed `perf_log`; autonomous-transaction flush
     keeps stats of failed units),
   - sleeps a random `sleep_min..sleep_max` between transactions (UDF `SleepUDF` or shell call),
   - checks the stop flag (sequence `g_stop_test` going negative, or an external stop-file) and the
     `perf_watch_interval` window (warmup → measurement),
   - reconnects every `actions_todo_before_reconnect` iterations.
4. After `warm_time` + `test_time`, session #1 produces the final report via report SPs.

**Workload model** — a car-service supply business (C) Pavel Zotov, 2014–2019):
customer orders → pooled supplier orders → supplier invoices → stock intake → reservations → retail sales/write-offs,
plus payments against customer/supplier balances. Every operation has a cancellation counterpart
(`sp_cancel_*`), so the model both grows *and* churns.

Business-op table registry (`business_ops`):

| unit (SP)               | mode     | kind        | meaning                          |
|-------------------------|----------|-------------|----------------------------------|
| sp_client_order         | stock    | creation    | customer orders a set of parts   |
| sp_supplier_order       | stock    | creation    | we order from supplier           |
| sp_supplier_invoice     | stock    | creation    | supplier invoices us             |
| sp_add_invoice_to_stock | stock    | state_next  | invoice content → stock          |
| sp_customer_reserve     | stock    | state_next  | reserve parts for sale           |
| sp_reserve_write_off    | stock    | state_next  | sale + stock write-off           |
| sp_pay_to_supplier / sp_pay_from_customer | payments | creation | money movements |
| sp_cancel_client_order, sp_cancel_supplier_*, sp_cancel_pay_*, ... | respective | removal | roll an op back |
| srv_make_invnt_saldo / srv_make_money_saldo | service | service | **invariant self-checks** (stock/money conservation) |
| srv_fill_mon, srv_recalc_idx_stat | service | service | monitoring snapshot, index stat recalc |

Selection nuance worth copying: during warmup, `removal` kinds are excluded so the database actually *grows*;
in the measurement phase cancellations churn it.

**Measurement layer** (the second big asset): `tmp$perf_log` (GTT) → `perf_log` → `perf_agg` → report SPs
(`report_perf_total`, `report_perf_dynamic`, `report_perf_per_minute`, `report_perf_detailed`,
`report_stat_per_units`, `report_stat_per_tables`). Headline metric = **successful business actions per minute**
("score", e.g. `score_06543`), with per-unit and per-table breakdowns, exception lists (fb_errors),
firebird.log before/after diff, gstat validation, and record-version/record ratios per table.

---

## 2. What to borrow (ranked by value to fb-loadgen)

| # | Borrow | Why it beats what we have now |
|---|--------|-------------------------------|
| 1 | **Business-process transactions** (multi-statement, multi-table SPs with document state machine + cancellations) | Current profiles are single statements against EMPLOYEE; they don't produce realistic lock conflicts, garbage, or long transactions. This is the core value. |
| 2 | **The oltpemul schema + SPs as a provisionable benchmark database** | Ready-made, battle-tested workload with realistic data volumes (qdistr/pdistr quantile distributions for quantities/prices). |
| 3 | **Registry-driven weighted unit mix** (`business_ops` + weights) | Maps 1:1 onto our `profile.WeightedOp` concept but data-driven: the UI/REST could edit weights per run without code changes. |
| 4 | **"Business ops/minute" headline metric + per-unit/per-table breakdown** | Our reporter counts statement-level ops. A comparable score makes results comparable with the whole oltp-emul benchmarking community (IBSurgeon publishes such numbers). |
| 5 | **Warmup semantics: creation-only during warmup, churn (cancellations) in measurement phase** | Our warmup today just runs the same ops; oltp-emul's split grows the DB first, then stresses conflicts. |
| 6 | **Invariant self-checks** (`srv_make_invnt_saldo/money_saldo`) run as a service unit | A failed invariant = the generator corrupted business state — currently we have nothing like this. |
| 7 | **Exception classification** (deadlock/update_conflict counted as normal load events, fb_errors list) | Our errlog logs errors but doesn't classify expected-under-load conflicts vs real failures. |
| 8 | Random think-time between *transactions* and periodic reconnects | We have think-ms and conn cycling already — align defaults and expose reconnect-every-N. |

## 3. What NOT to borrow

- **isql/bash orchestration, per-session log files, PID/window management** — our Go worker pool, ramp, and UI replace all of it, and are strictly better (single binary, real concurrency control, REST/schedules).
- **UDF-based sleeps** (`SleepUDF`) — Go sleeps between transactions; also removes the UDF-installation prerequisite.
- **fbsvcmgr / gdb / gstat shell-outs** for log/crash gathering — optional later (see phase 4); not core.
- **Per-FB-version config file matrix** — one Go-side config; the only version split that matters is the SQL flavor (2.5 vs 3.0+).

## 4. License — resolve before copying anything

oltp-emul has **no LICENSE file**; readmes say "(C) Pavel Zotov, Moscow, Russia. 2014–2019", repo sits under the
FirebirdSQL GitHub org. Default is all-rights-reserved, so verbatim redistribution of `oltpNN_DDL.sql` /
`*_sp.sql` in this repo is legally ambiguous.

Options (pick before phase 1 starts):
1. **Ask Pavel Zotov / FirebirdSQL** for MIT/BSD-style permission to embed the DDL+SP scripts (both parties are in the
   Firebird community; IBSurgeon relationship makes this the easy path — recommended).
2. Reimplement a *similar* (not verbatim) schema/SPs "inspired by" the model, crediting the original design.
3. Don't redistribute: download the scripts at provision-time (pin a commit) — still derivative use, weakest option.

Whichever option: keep `NOTICE`-style attribution and a link to the original repo in the UI and docs.

---

## 5. Target design in fb-loadgen

```
emul/                      new package
  assets/                  embedded SQL (embed.FS), pending license decision
    ddl_fb3.sql ...        (verbatim copies or reimplementations, see §4)
    common_sp.sql
    sp_fb3.sql
    fill.sql
  provision.go             CreateDatabase(file, pageSize=8192) → run scripts in order;
                           isql-style script splitter: strip SET TERM ^/;^ blocks, split on ^,
                           keep SET statements out, execute statements via driver
  fill.go                  initial document population w/ progress callback (init_docs)
  units.go                 Unit{Name, SP, Mode, Kind, Weight}; LoadUnits(db) reads business_ops;
                           PickUnit(rng, phase) — Go-side weighted choice (warmup: exclude kind=removal)
  run.go                   ExecuteUnit(ctx, tx, unit) — EXECUTE PROCEDURE + param generation;
                           classify error (gdscode via errors.As on firebirdsql err) into
                           ok | conflict(deadlock/update_conflict) | failure
  invariant.go             run srv_make_invnt_saldo/money_saldo periodically, surface result
profile/oltp_emul.go       new profile: wraps emul.Runner as a Profile
worker/                    (no changes) — a "unit" is just the op body of one transaction
metrics/reporter.go        add per-unit counters + ops/min score line; CSV: unit column
ui/ + api/openapi.yaml     endpoints: POST /api/dbs/{id}/emul/provision (create+fill, async job),
                           GET .../emul/units (registry + weights), PUT .../emul/weights
config/config.go           --profile oltp-emul --emul-db <path|dsn> [--emul-init-docs N]
                           [--emul-invariant-every N] [--emul-reconnect-every N]
```

Key decisions:
- **One transaction = one business unit** (EXECUTE PROCEDURE + commit), matching oltp-emul semantics; think-time
  happens *between* transactions, in Go (no UDF).
- **Stop control stays in Go**: ramp scheduler cancels worker contexts; the SP-side stop-flag machinery
  (`g_stop_test`, stop-file) is not needed and is dropped.
- **Measurement**: keep our in-memory collector as the source of truth (survives nothing needing autonomous-tx
  flushes because Go always sees the outcome), but *also* write per-unit rows into their `perf_log` when running
  against an oltpemul DB, so the original report SPs remain usable by DBAs.
- **FB version support: 3.0+ only initially** (`oltp30/40/50/60` SPs are one flavor); 2.5 (`oltp25_*`) can follow —
  it's a separate script set the provisioner would pick by server version (`SELECT @@VERSION`-equivalent via
  `rdb$get_context`).

## 6. Phases

**Phase 0 — license decision (§4).** Blocking for verbatim embedding. Effort: an email.

**Phase 1 — provision & fill (emul package, offline-testable).**
Script splitter (`^`-terminator, SET TERM handling), provision into a temp DB in CI without a server via dry-run
parsing tests; manual smoke vs local FB 3/4/5. Deliverable: `fb-loadgen --profile oltp-emul --emul-db oltpemul.fdb
--emul-init-docs 3000 --dry-run` creates + fills the DB. Est: 2–4 days.

**Phase 2 — run mode.** Unit loading from `business_ops`, Go-side weighted pick, EXECUTE PROCEDURE loop wired into
a profile, error classification, warmup excludes `removal`, metrics per unit. Deliverable: a 15-min run produces
ops/min per unit in CSV. Est: 2–3 days.

**Phase 3 — reporting & UI.** Score (successful ops/min, total + per unit) in the reporter and web UI; unit-weight
editor in UI + REST (`PUT /api/dbs/{id}/emul/weights`); invariant-check status line; provision endpoint with
progress. Est: 3–5 days.

**Phase 4 — optional / later.** FB 2.5 script flavor; write perf_log rows for compatibility with original report
SPs; firebird.log diff + gstat after-run collection (shell out when tools present); schedule presets for nightly
oltp-emul regression runs; EDS/`actions_todo_before_reconnect`-style periodic reconnects (we have conn cycling —
expose reconnect-every-N).

## 7. Risks / gotchas

- **Script parsing**: DDL/SP scripts are written for isql (`SET TERM`, `SET BAIL`, commented isql directives).
  The splitter needs its own tests; mis-parsing a PSQL body silently breaks the DB. Mitigation: provision in one
  transaction per script; verify object counts (`SELECT COUNT(*) FROM rdb$relations/procedures` vs expected) after.
- **Driver specifics**: EXECUTE PROCEDURE with ~12 output params and `TYPE OF domain` columns is fine for
  firebirdsql, but validate early (phase 2 spike) — especially BLOB outputs used by some report SPs (those we
  avoid calling from Go; they stay isql/DBA-side).
- **Data volume**: `init_docs` filling is I/O heavy and long (their default recommendation: start 3–5k docs);
  the fill must be cancellable and report progress (UI), and `wait_for_copy`-style "backup once, restore many"
  should be documented as the workflow for repeatable runs (gbak is out of scope; document manual backup).
- **Multi-DB sessions**: oltpemul DBs are per-database files; our session engine can run several against different
  FB instances — but never share one oltpemul file between concurrent runs (worker-id separation in oltp-emul
  exists precisely to cut lock conflicts; document: 1 run = 1 oltpemul DB).
- **Comparability of score**: ops/min depends on `init_docs`, sleep settings, and weights — record all three in
  the report header the way oltp-emul's `score_XXXXX_<build>_<arch>_<dur>_<atts>` filename does.
