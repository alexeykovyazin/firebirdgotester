# IBSurgeon Firebird Load Generator (`fb-loadgen`)

**A load-testing workbench for Firebird.** One Go binary that plays two roles:

1. **A classic load generator** — weighted mixes of SELECT / INSERT / UPDATE / DELETE / stored-procedure calls against the familiar **EMPLOYEE** sample schema, with connection ramping (warmup → main → cooldown), latency/throughput/error reports and per-operation outcome tracking.
2. **A business-process benchmark (OLTP-EMUL mode)** — runs the [FirebirdSQL oltp-emul](https://github.com/FirebirdSQL/oltp-emul) model: a car-service supply business (customer orders → supplier orders → invoices → stock → reservations → sales → payments, every step cancellable) implemented as stored procedures inside the database. The generator executes its 20 business operations in a weighted random mix and scores the server in **successful business actions per minute** — the same metric the [firebirdtest.com](https://www.firebirdtest.com/oltp-emul-fb/) benchmarks publish.

Both roles are driven by one **web control plane**: a fleet of databases with Start / Stop / Pause per row, scheduled unattended runs, run history with webhooks, a Prometheus endpoint, and a dedicated **OLTPEMUL dashboard** tab with live score, per-unit truth and memory peaks.

## 📊 Presentation

The product deck is published on **GitHub Pages** — [download the PPTX](https://alexeykovyazin.github.io/firebirdgotester/IBSurgeon_LoadGenerator.pptx) · [view the PDF](https://alexeykovyazin.github.io/firebirdgotester/IBSurgeon_LoadGenerator.pdf) · [slides gallery](https://alexeykovyazin.github.io/firebirdgotester/).

<details>
<summary><b>View the slides (20)</b></summary>

| | |
|---|---|
| ![Slide 1](presentation/slides/slide-01.png) | ![Slide 2](presentation/slides/slide-02.png) |
| ![Slide 3](presentation/slides/slide-03.png) | ![Slide 4](presentation/slides/slide-04.png) |
| ![Slide 5](presentation/slides/slide-05.png) | ![Slide 6](presentation/slides/slide-06.png) |
| ![Slide 7](presentation/slides/slide-07.png) | ![Slide 8](presentation/slides/slide-08.png) |
| ![Slide 9](presentation/slides/slide-09.png) | ![Slide 10](presentation/slides/slide-10.png) |
| ![Slide 11](presentation/slides/slide-11.png) | ![Slide 12](presentation/slides/slide-12.png) |
| ![Slide 13](presentation/slides/slide-13.png) | ![Slide 14](presentation/slides/slide-14.png) |
| ![Slide 15](presentation/slides/slide-15.png) | ![Slide 16](presentation/slides/slide-16.png) |
| ![Slide 17](presentation/slides/slide-17.png) | ![Slide 18](presentation/slides/slide-18.png) |
| ![Slide 19](presentation/slides/slide-19.png) | ![Slide 20](presentation/slides/slide-20.png) |

</details>

## What's inside

**Benchmarking**

- **Four workload profiles**: `write-heavy`, `read-heavy`, `spike` (EMPLOYEE schema) and `oltp-emul` (business-process benchmark, Firebird 3.0–5.0)
- **Connection ramp**: linear warmup/cooldown; random walk between min/max in main; sawtooth for spike
- **Schema-aware operations**: respects EMPLOYEE constraints (`PO_NUMBER`, salary bounds, status transitions, FKs); expected business exceptions (`order_already_shipped`, deadlocks under load) classified separately from real failures
- **Metrics**: TPS, success/error counts, latency buckets and percentiles (p50 / p95 / p99), per-operation and per-unit breakdowns

**OLTP-EMUL mode**

- **Provisioning**: one command creates the benchmark database — verbatim upstream DDL/procedures (page size 8192), realistic dictionaries and documents, schema verification
- **Weighted unit mix**: the `business_ops` registry drives selection; edit weights live from the UI, warmup grows the database before the measurement churns it
- **Truth per unit**: every unit is one transaction with an outcome — ok / conflict (deadlock, update conflict) / rejected (business rule) / failure — plus avg/max latency
- **Invariant self-checks**: stock and money must stay conserved across all operations; a violation means the generator corrupted business state
- **Memory peaks**: `mon$` snapshots at four levels (database / attachments / transactions / statements), the metric behind the firebirdtest.com charts

**Control plane & operations**

- **Web UI control plane**: fleet table with per-row Start / Stop / Pause and a detail drawer (`--ui`)
- **OLTPEMUL tab**: provisioning with progress, run settings, live unit-mix weights, score sparkline, per-unit table, final report and run-to-run comparison
- **Multi-DB discovery**: recursive folder scan (default `*.fdb`); same basename in different subfolders are distinct sessions
- **Scheduled runs**: once / interval / cron with time zones, overlap & budget policies, staggering
- **Run history & webhooks**: every run recorded with per-session outcomes; optional signed webhook on completion
- **Live connection budget**: saving a smaller/larger total-connection limit resizes running sessions immediately
- **REST API**: everything the UI does is an endpoint — see [API.md](API.md) and [api/openapi.yaml](api/openapi.yaml); `--api-only` runs it headless; `GET /metrics` is Prometheus-ready
- **Themed UI**: light theme by default, dark toggle remembered per browser
- **Reports**: live console output plus text sidecars (`results*.txt`, `results_emul.txt`, error log)

## Prerequisites

- **Go 1.24.5+** to build (see `go.mod`); prebuilt binaries on [Releases](https://github.com/alexeykovyazin/firebirdgotester/releases)
- A running **Firebird** server
- EMPLOYEE profiles: an **EMPLOYEE**-compatible database (schema in `EMPLOYEE_metadata.sql`)
- OLTP-EMUL profile: a database **provisioned by this tool** (it creates the schema itself); targets Firebird **3.0, 4.0 and 5.0**
- Default credentials used by the tool: `SYSDBA` / `masterkey`

## Quick start

```bash
# Help
./fb-loadgen --help

# Classic: single-DB CLI run against the EMPLOYEE schema
./fb-loadgen --profile write-heavy \
  --dsn "localhost/3050:./EMPLOYEE.FDB" \
  --warmup 30 --main 120 --cooldown 20

# Dry-run: print resolved config and exit (no load)
./fb-loadgen --profile write-heavy --dry-run

# Web UI (multi-DB control plane) — binds to localhost by default
./fb-loadgen --ui
# Open http://127.0.0.1:9000
```

### OLTP-EMUL in three steps

```bash
# 1) Provision a benchmark database (creates schema + fills documents)
fb-loadgen provision \
  --dsn "127.0.0.1/3055:C:\\data\\oltpemul.fdb" \
  --working-mode SMALL_01 --init-docs 3000

# 2) Point the tool at it — CLI
fb-loadgen --profile oltp-emul \
  --dsn "127.0.0.1/3055:C:\\data\\oltpemul.fdb" \
  --warmup 30 --main 300 --cooldown 20

#    …or UI: start fb-loadgen --ui, open the OLTPEMUL tab,
#    pick the database, press Start.
```

The **OLTPEMUL tab** shows the live score, a score sparkline, `mon$` memory peaks at four levels (db / attachments / transactions / statements), invariant status and the per-unit outcome table (ok / conflict / rejected / fail with avg/max ms). On stop it freezes a `results_emul.txt` report next to the standard result files and records the run for the **Last runs** comparison.

More on the model and design decisions: [OLTP_EMUL_PLAN.md](OLTP_EMUL_PLAN.md). The vendored SQL scripts stay verbatim to upstream (MIT — credit and provenance in [`emul/assets/NOTICE`](emul/assets/NOTICE)).

## Build

```bash
git clone <this-repo>
cd firebirdgotester
go build -o fb-loadgen.exe .   # Windows
# or
go build -o fb-loadgen .
```

There is no published module install path; build from the repository root.

Alternatively, grab a prebuilt binary from [GitHub Releases](https://github.com/alexeykovyazin/firebirdgotester/releases) (Windows, Linux, macOS; see *Releases* below).

## CI & Releases

- **CI** (`.github/workflows/ci.yml`) runs on every push to `main` and every pull request: `go vet`, the full test suite (with `-race`), and a compile check on Linux, Windows, and macOS.
- **Releases** (`.github/workflows/release.yml`) are published by pushing a version tag:

```bash
git tag v1.0.0
git push origin v1.0.0
```

The release workflow cross-compiles `fb-loadgen` for `windows/amd64`, `linux/amd64`, `linux/arm64`, `darwin/amd64`, and `darwin/arm64`, packages each archive with `LICENSE` and `README.md`, generates `checksums.txt` (SHA-256), and attaches everything to an auto-annotated GitHub Release.

## Quick start (control plane)

```bash
# Web UI (multi-DB control plane) — binds to localhost by default
./fb-loadgen --ui \
  --discover-dir . \
  --discover-mask "*.fdb" \
  --host localhost --port 3050 \
  --user SYSDBA --pass masterkey
# Open http://127.0.0.1:9000
```

## Web UI (multi-DB)

Start with `--ui`. Default listen address is `127.0.0.1:9000` (localhost only). Pass `--ui-addr :9000` to bind all interfaces. Optional `--ui-token` requires `Authorization: Bearer …` on mutating APIs. The UI (IBSurgeon Firebird Load Generator) uses a light theme by default with a header ☾/☀ toggle; the choice is remembered per browser.

**Connection bar** (Host / Port / User / Password / Scan root / Max total conns): defaults are `localhost`, **3050**, `SYSDBA`, `masterkey`. **Save connection** writes `fb-loadgen.ui.json` (v2: also stores per-DB prefs). Passwords are redacted on `GET /api/config`; blank password on save keeps the stored value.

| Column | Notes |
|--------|--------|
| Database | Relative path from scan root (same basename in different folders stay distinct) |
| Status | Idle / Starting / Running / Paused / Stopping / Completed / Failed |
| Profile / Conns / TPS / Err / Phase | Live metrics; edits survive polling |
| Actions | Time-limit dropdown, Start, Stop, Pause/Resume, Edit (detail drawer), Remove |

**Detail drawer** (row click / Edit): full path, DSN, warmup/main/cooldown (shape ratios for timed runs), thinkMs/txTimeout, spike fields, time limit/remaining, latency percentiles, expected vs unexpected errors, report downloads.

**Behavior**

- **Discover** recursively scans the scan root (optional subdir filter). Initial page load lists existing sessions without auto-rescan.
- Per-row settings restore from `fb-loadgen.ui.json` on discover upsert.
- **Start** runs full schema validation, then warmup → main → cooldown. Natural end → **Completed** with retained metrics. Reports land in `reports/<relpath>/<timestamp>/`.
- **Time limit**: each row has a dropdown in the Actions cell (No limit / 1 / 5 / 15 / 30 / 60 / 120 / 600 min, remembered per database by the browser; **15 min is the default**). A chosen duration becomes that session's total run length: warmup/main/cooldown are scaled proportionally to fill it, then the session ends naturally as **Completed** (countdown in the Phase column and drawer; Pause freezes it). **No limit** runs a fixed 1-minute warmup and then stays in the main phase indefinitely until you press **Stop**. The **Time limit for all** dropdown + **Apply to all** button above the table bulk-set every row. **Start All** starts every idle row using its own per-row limit. Via API: `POST /api/sessions/{id}/start` and `POST /api/sessions/start-all` accept `{"timeLimitMin": N}` (absent/0 = the No-limit behavior; the configured warmup/main/cooldown values act as the shape ratios for scaled runs).
- Main phase connection count **random-walks ±1/sec** between min and max. Spike drives both connection sawtooth and op-mix switching.
- **Pause** freezes workers and the phase clock. **Stop** cancels immediately.
- **Start All / Stop All / Pause All / Purge missing / Validate all** batch controls. Hard budget: `--max-total-conns` (default 200) with atomic reservation. Saving a smaller/larger budget resizes running sessions live — caps and reservations move to the new even split within about a second.

### OLTPEMUL tab

The **OLTPEMUL** tab is the complete lifecycle for the business-process benchmark on one database:

- **Lifecycle row**: database picker (oltpemul-capable databases are marked), lifecycle status (Ready / Running with phase / Completed / Failed / Not-an-oltpemul-database), and Start / Pause / Resume / Stop — enabled only for valid states. Start applies the settings form first.
- **Provision panel**: create a benchmark database from scratch (DSN, working mode, init docs, page size) as an async job with progress and cancel; the result self-registers in the fleet.
- **Run settings + unit-mix editor**: all 20 business units with editable weights (saved to the database's `business_ops` registry), presets for common mixes.
- **Live results**: score (successful actions/min) with sparkline, `mon$` memory peaks at four levels, invariant status, per-unit outcome table.
- **Final report + Last runs**: frozen score with config echo, raw report downloads, and a score/memory comparison across runs.

## REST API

The web UI is a thin client over a JSON REST API served on the same address (default `http://127.0.0.1:9000`). Full instruction and examples: **[API.md](API.md)**; machine-readable OpenAPI 3.1 spec: **[api/openapi.yaml](api/openapi.yaml)**. Start with `--ui-token <secret>` to require `Authorization: Bearer <secret>` on mutating calls.

| Operation | Method & path | Body |
|-----------|---------------|------|
| List databases + fleet summary | `GET /api/sessions` | — |
| One database: status, metrics, errors | `GET /api/sessions/{id}` | — |
| Start load for one database | `POST /api/sessions/{id}/start` | `{"timeLimitMin": 15}` (absent/0 = No limit) |
| Stop load for one database | `POST /api/sessions/{id}/stop` | — |
| Pause / resume | `POST /api/sessions/{id}/pause` / `resume` | — |
| Check DB connectivity + schema | `POST /api/sessions/{id}/validate` | — |
| Start / stop all idle/active | `POST /api/sessions/start-all` / `stop-all` | same as start |
| **emul:** unit registry | `GET /api/sessions/{id}/emul/units` | — |
| **emul:** edit unit weights | `PUT /api/sessions/{id}/emul/weights` | `{"weights": {"SP_CLIENT_ORDER": 20}}` |
| **emul:** live score/state | `GET /api/sessions/{id}/emul/state` | — |
| **emul:** provision database | `POST /api/emul/provision` | `{"dsn": "...", "workingMode": "SMALL_01", "initDocs": 3000}` |
| **emul:** provision job status/cancel | `GET/DELETE /api/emul/provision/{jobId}` | — |
| **emul:** workload profiles | `GET /api/emul/profiles` | — |

`{id}` is the stable per-database identifier returned by the list call (16-hex; derived from the absolute path).

**Status values** in every session payload: `Idle` (not running), `Starting`, `Running` (working), `Paused`, `Stopping`, `Completed` (finished its schedule), `Failed` (could not start — connection or schema error in `lastError`). Connection problems during a run surface as `lastError` plus unexpected-error counters.

```bash
BASE=http://127.0.0.1:9000

# 1) List available databases (id, relPath, absPath, status, ...)
curl -s $BASE/api/sessions | python -m json.tool

# 2) Start one database for 15 minutes (timeLimitMin 0 = unlimited until stopped)
ID=<id-from-list>
curl -s -X POST $BASE/api/sessions/$ID/start   -H "Content-Type: application/json" -d '{"timeLimitMin": 15}'

# 3) Status: working / not working / connection error
curl -s $BASE/api/sessions/$ID | python -c "import json,sys; s=json.load(sys.stdin); print(s['status'], s['lastError'])"

# 4) Stop
curl -s -X POST $BASE/api/sessions/$ID/stop

# 5) oltp-emul: live score and per-unit outcomes
curl -s $BASE/api/sessions/$ID/emul/state | python -m json.tool
```

Other read endpoints: `GET /api/fleet` (aggregated counters), `GET /api/config` (redacted settings), `GET /api/sessions/{id}/report` and `GET /api/sessions/{id}/report/{file}` (result files).

## Scheduled runs & history

Runs can fire on a schedule — no UI or human needed. Schedules persist to `fb-loadgen.schedules.json` and survive restarts; every execution (scheduled or manual) lands in `fb-loadgen.runs.json` with per-session outcomes.

```bash
BASE=http://127.0.0.1:9000

# Nightly 30-minute write-heavy on two databases at 02:00 local
curl -s -X POST $BASE/api/schedules -H "Content-Type: application/json" -d '{
  "name": "nightly-30min",
  "enabled": true,
  "trigger": {"type": "cron", "cron": "0 2 * * *", "tz": "Europe/Moscow"},
  "targets": {"sessionIds": ["<id1>", "<id2>"]},
  "run": {"timeLimitMin": 30, "overrides": {"profile": "write-heavy"}},
  "policy": {"ifRunning": "skip", "staggerSec": 5}
}'

# Fire a schedule right now (asynchronous)
curl -s -X POST $BASE/api/schedules/$SID/trigger

# What happened?
curl -s "$BASE/api/runs?limit=10"
curl -s  $BASE/api/runs/<runId>
```

Policies: `ifRunning` (skip a fire whose targets are busy, or `stopAndRun`), `ifBudgetFull` (wait for connection budget up to `waitBudgetSec`, or skip), `staggerSec` spacing between per-DB starts, `catchUp` for a fire missed while the process was down. Recurring triggers require a positive `timeLimitMin`. Optional `notifyUrl` (or global `--webhook-url` + `--webhook-secret`) POSTs a signed JSON event when a run finishes. `GET /metrics` exposes fleet/session/schedule counters in Prometheus format.

The UI's **Schedules** and **Runs** tabs cover all of this interactively. Headless? Run `fb-loadgen --ui --api-only` to serve the API without the SPA.

## Example workflows

```bash
# 1. Short debug cycle
./fb-loadgen --profile write-heavy \
  --dsn "localhost/3050:./EMPLOYEE.FDB" \
  --warmup 5 --main 10 --cooldown 5 \
  --conn-init 1 --conn-peak 3 \
  --think-ms 0 --debug

# 2. Full write-heavy run with reports
./fb-loadgen \
  --dsn "localhost/3050:./EMPLOYEE.FDB" \
  --user SYSDBA --pass masterkey \
  --profile write-heavy \
  --conn-init 2 --conn-peak 20 \
  --warmup 30 --main 120 --cooldown 20 \
  --csv results.csv \
  --think-ms 50 --tx-timeout 10

# 3. Read-heavy
./fb-loadgen --profile read-heavy \
  --dsn "localhost/3050:./EMPLOYEE.FDB" \
  --conn-init 2 --conn-peak 20 \
  --warmup 30 --main 120 --cooldown 20 \
  --csv results.csv

# 4. Spike / stress
./fb-loadgen --profile spike \
  --dsn "localhost/3050:./EMPLOYEE.FDB" \
  --conn-init 5 --conn-peak 40 \
  --warmup 30 --main 120 --cooldown 20 \
  --spike-cycles 3 --spike-hold 15 \
  --think-ms 20 \
  --csv spike_results.csv

# 5. oltp-emul: provision, then run the business-process benchmark
fb-loadgen provision \
  --dsn "127.0.0.1/3055:C:\\data\\oltpemul.fdb" \
  --working-mode MEDIUM_02 --init-docs 5000
fb-loadgen --profile oltp-emul \
  --dsn "127.0.0.1/3055:C:\\data\\oltpemul.fdb" \
  --conn-init 2 --conn-peak 8 \
  --warmup 60 --main 600 --cooldown 30 \
  --emul-invariant-every 30 --emul-monitor-every 10 \
  --csv emul_results.csv
```

More echo-only examples: `example_usage.sh`.

### Local vs remote DSN

```bash
# Local
./fb-loadgen --profile write-heavy \
  --dsn "localhost/3050:/var/lib/firebird/employee.fdb"

# Remote
./fb-loadgen --profile write-heavy \
  --dsn "firebird.example.com/3050:/data/employee.fdb" \
  --user appuser --pass secret123

# host:port/database form
./fb-loadgen --profile read-heavy \
  --dsn "192.168.1.100:3050/./EMPLOYEE.FDB"
```

## Command-line options

All runtime configuration is via CLI flags (no config file, no env vars for the main binary). The `provision` subcommand has its own flags (`--dsn`, `--user`, `--pass`, `--page-size`, `--init-docs`, `--working-mode`).

### Connection

| Flag | Description | Default |
|------|-------------|---------|
| `--dsn` | Firebird DSN | `localhost/3050:./EMPLOYEE.FDB` |
| `--user` | Database user | `SYSDBA` |
| `--pass` | Database password | `masterkey` |

**Accepted DSN shapes** (parsed into `user:pass@host:port/database` for `nakagami/firebirdsql`):

| Form | Example |
|------|---------|
| `host/port:database` | `localhost/3050:./EMPLOYEE.FDB` |
| `host:port/database` | `localhost:3050/./EMPLOYEE.FDB` |
| `host/database` | `localhost/./EMPLOYEE.FDB` (port defaults to **3050**) |

### Profile

| Flag | Description | Default |
|------|-------------|---------|
| `--profile` | `write-heavy` \| `read-heavy` \| `spike` \| `oltp-emul` | **required** |

### Connection scaling

| Flag | Description | Default |
|------|-------------|---------|
| `--conn-init` / `--conn-min` | Min / initial connections (≥ 1) | `2` |
| `--conn-peak` / `--conn-max` | Max / peak connections (≥ min) | `20` |

During CLI/UI main phase (non-spike), worker count random-walks between min and max (±1 each second). If min==max, count is held steady.

### Timing (seconds)

| Flag | Description | Default |
|------|-------------|---------|
| `--warmup` | Linear ramp min → max | `30` |
| `--main` | Steady-state / walk / spike period | `120` |
| `--cooldown` | Linear drain to 0 | `20` |

CLI runs use these values directly. In the web UI, timed runs scale them proportionally to the selected duration; the **No limit** mode uses a fixed 60 s warmup followed by an endless main phase until stopped.

### Spike extras

| Flag | Description | Default |
|------|-------------|---------|
| `--spike-cycles` | Sawtooth cycles during main (≥ 1 when profile=spike) | `3` |
| `--spike-hold` | Seconds held at peak per cycle (≥ 1 when spike) | `10` |

### oltp-emul extras

| Flag | Description | Default |
|------|-------------|---------|
| `--emul-invariant-every` | Seconds between stock/money invariant self-checks | `60` (0 = off) |
| `--emul-monitor-every` | Seconds between `mon$` memory snapshots | `10` (0 = off) |

### Web UI / multi-DB

| Flag | Description | Default |
|------|-------------|---------|
| `--ui` | Start web control plane | `false` |
| `--ui-addr` | Listen address | `127.0.0.1:9000` |
| `--ui-token` | Optional bearer token for mutating APIs | _(empty)_ |
| `--discover-dir` | Allowlisted root for discovery | `.` |
| `--discover-mask` | File glob | `*.fdb` |
| `--discover-recursive` | Scan subfolders | `true` |
| `--host` / `--port` | Firebird host/port for discovered files | `localhost` / `3050` |
| `--max-total-conns` | Hard sum of running session max | `200` |

### Output & misc

| Flag | Description | Default |
|------|-------------|---------|
| `--csv` | Base path for report files | `results.csv` |
| `--report-every` | Console / file report interval (seconds) | `5` |
| `--error-log` | SQL/command error log (all failures) | `<csv>_sql_errors.log` |
| `--think-ms` | Sleep between ops per worker | `50` |
| `--tx-timeout` | Per-transaction context timeout (seconds) | `10` |
| `--dry-run` | Print config / connection string and exit | `false` |
| `--debug` | Per-operation worker debug logs | `false` |
| `--help`, `-h` | Show usage | |

## Workload profiles

### `write-heavy` — 100% writes (OLTP stress)

| Weight | Operation | Notes |
|--------|-----------|--------|
| 25% | `InsertCustomer` | `CUST_NO` via `GEN_ID(CUST_NO_GEN,1)` |
| 20% | `InsertSales` | `PO_NUMBER` = `V` + 7 digits |
| 15% | `UpdateSalesStatus` | `new` → `open` → `shipped` |
| 15% | `CallShipOrder` | `EXECUTE PROCEDURE SHIP_ORDER` |
| 10% | `UpdateEmployeeSalary` | Kept within `JOB` min/max |
| 10% | `CallAddEmpProj` | Inserts into `EMPLOYEE_PROJECT` |
| 5% | `DeleteEmpProj` | Removes project assignments |

Each op runs in its own short transaction (`BEGIN` → op → `COMMIT` / `ROLLBACK`).

### `read-heavy` — ~95% reads, 5% write

| Weight | Operation | Notes |
|--------|-----------|--------|
| 30% | `CallOrgChart` | Department hierarchy |
| 25% | `CallDeptBudget` | Recursive budget rollup |
| 15% | `CallMailLabel` | Customer address |
| 10% | `CallGetEmpProj` | Employee projects |
| 10% | `CallSubTotBudget` | Aggregate dept budgets |
| 5% | `SelectEmployeeDeptJob` | JOIN `EMPLOYEE` + `DEPARTMENT` + `JOB` |
| 5% | `UpdateDeptBudget` | Rare write |

### `spike`

- Connection count follows a **sawtooth** between mid-level `((init+peak)/2)` and `conn-peak` during the main phase
- Designed to alternate read-heavy vs write-heavy mixes around spike phases (see `profile/spike.go` and `Technical_task.md`)
- Use `--spike-cycles` and `--spike-hold` to control burst shape

### `oltp-emul` — business-process benchmark

One transaction = one business unit, executed as a stored procedure in **READ COMMITTED NO WAIT** mode. Units come from the database's `business_ops` registry — each has a mode (`stock` / `payments` / `service`) and kind (`creation` / `removal` / `state_next` / `state_back` / `service`):

| Representative units | Kind | What happens |
|----------------------|------|--------------|
| `sp_client_order` | creation | A customer orders a random set of parts |
| `sp_supplier_order` / `sp_supplier_invoice` | creation | We order from the supplier; the invoice arrives |
| `sp_add_invoice_to_stock` | state_next | Invoice content lands in stock |
| `sp_customer_reserve` / `sp_reserve_write_off` | state_next | Reserve for sale, then sell + write off |
| `sp_pay_to_supplier` / `sp_pay_from_customer` | creation | Money movements against balances |
| `sp_cancel_*` | removal | Cancel any of the above (churn in the measurement phase) |
| `srv_make_invnt_saldo` / `srv_make_money_saldo` | service | **Invariant checks**: stock and money must balance |

Key semantics:

- **One unit = one transaction**, committed on success, rolled back on any outcome other than ok
- **Deadlocks, update conflicts and lock timeouts are normal load events** (outcome *conflict*); business rejections (`ex_*` exceptions) are *rejected*; everything else is a real *failure*
- **Warmup vs measurement**: during warmup, removal units are excluded at provision time so the database grows; the measurement phase adds churn
- **Units require NOWAIT transactions** — the server-side checks reject anything else
- Only databases provisioned by this tool can run the profile (a schema guard fails fast otherwise)
- 20 units are loaded from `business_ops`; weights are editable live from the UI/API

## Connection ramp

```
Warmup     Linear: conn-min → conn-max over --warmup
Main       write/read-heavy: random walk ±1/s between min and max
           spike: sawtooth mid ↔ max for --spike-cycles
Cooldown   Linear drain max → 0 over --cooldown
Pause (UI) Freeze ops + phase clock; keep connections open
```

- Scheduler ticks every **500 ms**; walk adjusts every **1 s**
- Each worker owns **one** `database/sql` connection (`MaxOpenConns(1)`) for its lifetime
- Cancelled workers finish the current transaction, close the connection, and exit

## Schema awareness

The classic profiles target the EMPLOYEE sample DB. Schema reference: `EMPLOYEE_metadata.sql`. Design notes: `Technical_task.md`. On UI **Start**, a schema gate requires a readable `EMPLOYEE` table. The `oltp-emul` profile instead requires the oltpemul schema (`BUSINESS_OPS` registry) — see the OLTPEMUL tab's provision panel.

**Writable under load:** `CUSTOMER`, `SALES`, `EMPLOYEE` (salary), `EMPLOYEE_PROJECT`, `DEPARTMENT` (budget).

**Read-only / reference:** `COUNTRY`, `JOB`, `PROJECT`, `PROJ_DEPT_BUDGET`; `SALARY_HISTORY` is written by trigger on salary update.

**Constraint highlights:**

| Rule | Detail |
|------|--------|
| `PO_NUMBER` | Exactly 8 chars, starts with `V` |
| Salary | Must stay within `JOB.MIN_SALARY`–`JOB.MAX_SALARY` |
| Order status | Forward transitions only (`new`→`open`→`shipped`) |
| `CUST_NO` | Generated with `GEN_ID`, not invented client-side |
| Arrays | `LANGUAGE_REQ` / related SPs are excluded from load |

**Expected Firebird exceptions** (soft failures): `order_already_shipped`, `customer_on_hold`, `customer_check`, and similar business rejections — counted but not treated as fatal connection errors. The `oltp-emul` profile classifies its own outcomes per unit (see above).

## Architecture

```
CLI (--ui) → session.Manager → per-DB Session
           → discover (recursive *.fdb under allowlisted dir)
           → ui (embed.FS tabs + REST: sessions, schedules, runs, emul)
           → emul sidecars (memory monitor, invariants) per oltp-emul run

CLI (single) → config → db.ConnectionFactory (DialSettings)
             → ops.Cache → profile → ramp.Scheduler → worker
             → metrics
CLI provision → emul.Provision (scripts via quote-aware isql splitter)
              → emul.Fill (creation units)
```

| Package | Role |
|---------|------|
| `config/` | Flags, DSN parsing, validation |
| `discover/` | Recursive folder/mask scan; absPath identity |
| `session/` | Multi-DB lifecycle, schema gate, Start/Stop/Pause, run history |
| `ui/` | Embedded tabs (Sessions / Schedules / Runs / OLTPEMUL) + JSON API |
| `emul/` | oltp-emul engine: assets, script splitter, provision, fill, units, invariants, monitor |
| `db/` | Dial settings + connection factory |
| `ops/` | Cache, reads, writes, exception classification |
| `profile/` | Weighted selectors for all four profiles |
| `worker/` | Per-connection worker loop, pause gate, metrics |
| `ramp/` | Warmup / main (walk or spike) / cooldown |
| `metrics/` | Aggregation, latency buckets, reporting |

## Metrics

Collected continuously:

- Total / success / error operation counts and success rate
- Throughput (total and interval TPS)
- Latency: avg, min, max, p50, p95, p99
- Latency histogram buckets (ms): `<5`, `<10`, `<25`, `<50`, `<100`, `<250`, `<500`, `<1000`, `≥1000`
- Worker counts (max / current) and active profile name
- oltp-emul runs add: score (successful units/min), per-unit ok/conflict/rejected/failure, `mon$` memory peaks at four levels, invariant status

## Reports

When `--csv results.csv` is set:

1. **During the run** — periodic text summaries are written to `results.csv` (filename is historical; format is text).
2. **On shutdown** — `ReportAllToFile` writes sidecars:

| File | Content |
|------|---------|
| `results.csv_summary.txt` | High-level summary |
| `results.csv_final.txt` | Final totals |
| `results.csv_latency.txt` | Latency distribution |
| `results.csv_operations.txt` | Per-operation breakdown |
| `results.csv_errors.txt` | Error details |
| `results.csv_performance.txt` | Performance snapshot |
| `results.csv_status.txt` | Run status |
| `results_sql_errors.log` | Every SQL/command failure (expected + unexpected), tab-separated |
| `results_emul.txt` | oltp-emul runs: score, per-unit table, memory peaks, invariants, config echo |

UI sessions write the same stream to `reports/<db>/<timestamp>/` (downloadable from the detail drawer with other report files).

Each error-log line looks like:

```text
2026-07-31T00:01:02.123Z	kind=expected	worker=3	op=InsertSales	code=check_constraint	source=E:\db\EMPLOYEE.FDB	msg=...
```

Kinds: `expected`, `unexpected`, `begin`, `commit`, `connect`, `command`.

## Project layout

```
fb-loadgen/
├── main.go                 # CLI entry, provision subcommand, --ui branch
├── integration_test.go     # CLI smoke tests (expects built ./fb-loadgen)
├── config/config.go
├── db/                     # dial settings + connection factory
├── discover/               # recursive *.fdb discovery
├── emul/                   # oltp-emul engine (assets, splitter, provision,
│                           #   fill, units, invariants, monitor, NOTICE)
├── session/                # multi-DB SessionManager + run history
├── ui/                     # embed.FS tabs + REST API + emul endpoints
├── ops/                    # cache, reads, writes, errors, ops_test.go
├── profile/                # write_heavy, read_heavy, spike, oltp_emul
├── worker/                 # workers + pause gate + per-unit metrics
├── ramp/                   # warmup / walk / spike / cooldown
├── metrics/                # collector, reporter
├── api/openapi.yaml        # OpenAPI 3.1 spec
├── EMPLOYEE_metadata.sql   # Schema dump
├── Technical_task.md       # Original design / constraint map
├── OLTP_EMUL_PLAN.md       # oltp-emul integration design
├── example_usage.sh        # Printed example commands
├── API.md                  # REST API guide with examples
├── go.mod / go.sum
└── LICENSE                 # GNU GPL v3
```

## Testing

```bash
# All packages
go test ./...

# Skip DB-dependent / binary-dependent tests in the main package
go test -short ./...

# Race detector
go test -race ./...

# Live Firebird ops tests (skipped if DB unreachable)
# Optional overrides:
#   FIREBIRD_DSN, FIREBIRD_USER, FIREBIRD_PASS
go test ./ops/ -v
```

| Suite | What it covers |
|-------|----------------|
| `emul/isqlscript_test.go` | Golden tests: the vendored oltp-emul scripts must parse into exact statement/procedure counts |
| `emul/` unit tests | Selector weights, splitter edge cases (`^` in strings, SET TERM switching) |
| `worker/lifecycle_test.go` | Worker start/stop/pause lifecycle against a stub connector |
| `ramp/lifecycle_test.go` | Scheduler ramp/phase transitions with a stub connector |
| `ops/ops_test.go` | Cache load, table counts, read/write ops against a live DB |
| `integration_test.go` | Help output, config validation, dry-run via a pre-built `./fb-loadgen` binary |

Build the binary before non-short integration tests:

```bash
go build -o fb-loadgen .
go test -v .
```

## Dependencies

| Dependency | Version | Role |
|------------|---------|------|
| Go | 1.24.5 | Language / toolchain |
| `github.com/nakagami/firebirdsql` (IBSurgeon fork) | v0.9.17 replaced by `IBSurgeon/firebirdsql-go` | Firebird driver (`database/sql`), services manager, create-DB driver variant |
| upstream oltp-emul SQL assets | commit e9b158e8 | Benchmark schema + procedures (MIT — see `emul/assets/NOTICE`) |

Transitive: `chacha20`, `shopspring/decimal`, `golang.org/x/text`, `modernc.org/mathutil`, and related packages (see `go.sum`).

## Extending

### New operation

1. Add SQL / logic in `ops/reads.go` or `ops/writes.go`
2. Expose it on `ReadOperations` / `WriteOperations`
3. Register a weight entry in the relevant profile under `profile/`
4. Add a live test in `ops/ops_test.go` when possible

### New profile

1. Add `profile/<name>.go` implementing the `Profile` interface
2. Register it in `profile.ProfileFactory`
3. Extend `--profile` validation in `config/config.go`
4. Document weights and intended use here

## Troubleshooting

| Symptom | What to check |
|---------|----------------|
| Connection failures | Firebird running; host/port; file path in DSN; firewall |
| Auth errors | `--user` / `--pass`; Firebird user privileges |
| Schema / SQL errors | Confirm EMPLOYEE schema (`EMPLOYEE_metadata.sql`); avoid corrupted `.FDB` |
| "table BUSINESS_OPS not found" | The oltp-emul profile needs a provisioned database — run `fb-loadgen provision` first |
| High error rate | Expected SP exceptions under write load; lower `--conn-peak`; raise `--think-ms` |
| Lock contention | Fewer writers; shorter `--tx-timeout` visibility; inspect `*_errors.txt` |
| Slow debugging | `--think-ms 0 --debug --warmup 5 --main 10 --cooldown 5 --conn-init 1 --conn-peak 3` |

## License

GNU General Public License v3.0 — see [LICENSE](LICENSE). The vendored oltp-emul SQL scripts are MIT-licensed by Pavel Zotov / FirebirdSQL — see [`emul/assets/NOTICE`](emul/assets/NOTICE).

## Further reading

- [OLTP_EMUL_PLAN.md](OLTP_EMUL_PLAN.md) — how oltp-emul was integrated: what is borrowed, what is Go-side, phases and risks
- [Technical_task.md](Technical_task.md) — schema constraint map, profile design, ramp model, error strategy
- [EMPLOYEE_metadata.sql](EMPLOYEE_metadata.sql) — tables, procedures, and constraints
- [example_usage.sh](example_usage.sh) — printable CLI cookbook

## Extended load mix (−plusddl)

The `extendedLoad` section (config file `fb-loadgen.ui.json`, session panel
"Extended load", CLI flags) changes the load character on top of the regular
op mix:

- **Heavy JOIN SELECT** every `heavySelect.everySec` seconds (default 5 s):
  a 3–4 table scan over `DOC_LIST ⋈ AGENTS ⋈ DOC_DATA ⋈ WARES` with a random
  id-range filter, aggregate and sorted cut.
- **Bulk DML** every `bulkDml.everySec` seconds (default 30 s): 100–1000 rows
  inserted / updated / deleted per round on the dedicated `EL_BULK_ITEMS`
  table (business tables and the score stay untouched).
- **Randomized transaction scenarios** (`txVariants.mode` = `emul-safe` |
  `full` | `off`): every operation draws isolation × read-only ×
  wait/nowait/lock-timeout and a completion method — commit, rollback,
  COMMIT/ROLLBACK RETAINING, engine-side 2PC (via `EXECUTE STATEMENT … WITH
  COMMON TRANSACTION` against the aux database `EL_2PC.FDB`), limbo
  (prepare-then-die, resolved by the recovery sidecar) and a planned hard
  connection drop (the worker rebuilds its pool in place). `emul-safe`
  never draws infinite-wait transactions (oltp-emul units reject them).
- **Operations log** (`opsLog`): every transaction — number, parameters,
  completion method, change volume, start time — in the output format of the
  Firebird replication-journal printer (`fb_repl_print`), `tsv` available as
  a machine option. 50 MB size rotation, ≤3 archives, rename on restart,
  START/terminal pairing check at close. Exposed via
  `GET /api/sessions/{id}/logs[/{file}]`.
- **`-plusddl`** (`plusDDL.enabled`): ALTER TABLE ADD/ALTER/DROP of own
  `TST_` columns on working tables every `ddl-every` seconds, alternating
  CREATE TABLE (2–5 random columns, all six triggers BI/AI/BU/AU/BD/AD,
  grouped test DML 100/50/30) / DROP TABLE rounds, and deliberately rolled
  back DDL verified against `rdb$relation_fields`.

**Comparable scores:** the extended mix is on by default and changes the
oltp-emul score by construction. For comparable score runs (e.g.
firebirdtest.com) switch it off — `--extended-load=false` on the CLI or the
"enabled" checkbox in the UI panel; `--tx-variants off` alone keeps the
periodic sidecars off as well. See `EXTENDED_LOAD_PLAN.md` for the full
design, the fb_repl_print log format spec (§7) and the verified-facts table.

**Known interaction — retaining holds locks (verified live):** COMMIT/ROLLBACK
RETAINING keeps the transaction's lock interest after the commit, so retained
contexts hold row locks on the documents they touched. With the default
completion weights (10% retaining: 7% commit + 3% rollback retaining) the emul
units see a wave of immediate
update conflicts ("record from transaction ... is not visible / no wait") —
this is the intended load character and exactly what the completion axis is
meant to expose, but it depresses the emul score far more than the periodic
sidecars do. Limbo and hard-drop completions kill sockets on purpose; workers
rebuild their pools in place and continue (watch `[extended] limbo resolved`
and `[] RESOLVED` records in the ops log).
