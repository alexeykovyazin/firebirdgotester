# IBSurgeon Firebird Load Generator (`fb-loadgen`)

A Go CLI load simulator for **Firebird** databases, built around the classic **EMPLOYEE** sample schema. It spawns workers (one dedicated connection each), runs weighted mixes of SELECT / INSERT / UPDATE / DELETE / stored-procedure calls, ramps connections through warmup → main → cooldown, and writes latency / throughput / error reports.

## Features

- **Three workload profiles**: `write-heavy`, `read-heavy`, and `spike`
- **Connection ramp**: linear warmup/cooldown; random-walk between min/max in main; sawtooth for spike
- **Multi-DB discovery**: recursive folder scan (default `*.fdb`); same basename in different subfolders are distinct sessions
- **Web UI control plane**: table of databases with Start / Stop / Pause per row (`--ui`)
- **Per-DB time limits**: run a session for 1–600 minutes (phases scale proportionally) or unlimited — 1-minute warmup, then steady load until stopped
- **Scheduled runs**: once / interval / cron schedules with time zones, overlap & budget policies, staggering — fire runs unattended from the UI's Schedules tab or the REST API
- **Run history & webhooks**: every run (scheduled or manual) is recorded with per-session outcomes and report dirs; optional webhook on completion, Prometheus `/metrics`
- **Live connection budget**: saving a smaller/larger total-connection limit resizes running sessions immediately
- **Themed UI**: light theme by default, dark toggle remembered per browser
- **REST API**: full programmatic control (list / start / stop / status / schedules / history) — see [API.md](API.md); `--api-only` runs it headless
- **Schema-aware operations**: respects EMPLOYEE constraints (`PO_NUMBER`, salary bounds, status transitions, FKs)
- **Expected exception handling**: Firebird business exceptions (e.g. `order_already_shipped`) classified separately from real failures
- **Startup lookup cache**: preloads valid dept / employee / project / customer / job salary ranges
- **Metrics**: TPS, success/error counts, latency buckets and percentiles (p50 / p95 / p99)
- **Reports**: live console output plus multi-file text sidecars from `--csv`
- **Graceful shutdown**: `Ctrl+C` / `SIGTERM` stops the scheduler and flushes reports

## Prerequisites

- **Go 1.24.5+** (see `go.mod`)
- A running **Firebird** server
- One or more **EMPLOYEE**-compatible databases (sample `EMPLOYEE.FDB` included)
- Default credentials used by the tool: `SYSDBA` / `masterkey`

## Build

```bash
git clone <this-repo>
cd firebirdgotester
go build -o fb-loadgen.exe .   # Windows
# or
go build -o fb-loadgen .
```

There is no published module install path; build from the repository root.

## Quick start

```bash
# Help
./fb-loadgen --help

# Minimal single-DB CLI run
./fb-loadgen --profile write-heavy \
  --dsn "localhost/3050:./EMPLOYEE.FDB"

# Dry-run: print resolved config and exit (no load)
./fb-loadgen --profile write-heavy --dry-run

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

```bash
./fb-loadgen --ui \
  --discover-dir "E:/FirebirdDBs" \
  --discover-recursive \
  --host localhost --port 3050 \
  --conn-min 2 --conn-max 20 \
  --max-total-conns 200
```


## REST API

The web UI is a thin client over a JSON REST API served on the same address (default `http://127.0.0.1:9000`). Full instruction and examples: **[API.md](API.md)**. Start with `--ui-token <secret>` to require `Authorization: Bearer <secret>` on mutating calls.

| Operation | Method & path | Body |
|-----------|---------------|------|
| List databases + fleet summary | `GET /api/sessions` | — |
| One database: status, metrics, errors | `GET /api/sessions/{id}` | — |
| Start load for one database | `POST /api/sessions/{id}/start` | `{"timeLimitMin": 15}` (absent/0 = No limit) |
| Stop load for one database | `POST /api/sessions/{id}/stop` | — |
| Pause / resume | `POST /api/sessions/{id}/pause` / `resume` | — |
| Check DB connectivity + schema | `POST /api/sessions/{id}/validate` | — |
| Start / stop all idle/active | `POST /api/sessions/start-all` / `stop-all` | same as start |

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

All runtime configuration is via CLI flags (no config file, no env vars for the main binary).

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
| `--profile` | `write-heavy` \| `read-heavy` \| `spike` | **required** |

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

The tool targets the EMPLOYEE sample DB. Schema reference: `EMPLOYEE_metadata.sql`. Design notes: `Technical_task.md`. On UI **Start**, a schema gate requires a readable `EMPLOYEE` table.

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

**Expected Firebird exceptions** (soft failures): `order_already_shipped`, `customer_on_hold`, `customer_check`, and similar business rejections — counted but not treated as fatal connection errors.

## Architecture

```
CLI (--ui) → session.Manager → per-DB Session
           → discover (recursive *.fdb under allowlisted dir)
           → ui (embed.FS table + REST)

CLI (single) → config → db.ConnectionFactory (DialSettings)
             → ops.Cache → profile → ramp.Scheduler → worker
             → metrics
```

| Package | Role |
|---------|------|
| `config/` | Flags, DSN parsing, validation |
| `discover/` | Recursive folder/mask scan; absPath identity |
| `session/` | Multi-DB lifecycle, schema gate, Start/Stop/Pause |
| `ui/` | Embedded HTML table + JSON API |
| `db/` | Dial settings + connection factory |
| `ops/` | Cache, reads, writes, exception classification |
| `profile/` | Weighted selectors for the three profiles |
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

UI sessions write the same stream to `reports/<db>/<timestamp>/sql_errors.log` (downloadable from the detail drawer with other report files).

Each log line looks like:

```text
2026-07-31T00:01:02.123Z	kind=expected	worker=3	op=InsertSales	code=check_constraint	source=E:\db\EMPLOYEE.FDB	msg=...
```

Kinds: `expected`, `unexpected`, `begin`, `commit`, `connect`, `command`.

## Project layout

```
fb-loadgen/
├── main.go                 # CLI entry + --ui branch
├── integration_test.go     # CLI smoke tests (expects built ./fb-loadgen)
├── config/config.go
├── db/                     # dial settings + connection factory
├── discover/               # recursive *.fdb discovery
├── session/                # multi-DB SessionManager
├── ui/                     # embed.FS HTML table + REST API
├── ops/                    # cache, reads, writes, errors, ops_test.go
├── profile/                # write_heavy, read_heavy, spike
├── worker/                 # workers + pause gate
├── ramp/                   # warmup / walk / spike / cooldown
├── metrics/                # collector, reporter
├── EMPLOYEE.FDB            # Sample database
├── EMPLOYEE_metadata.sql   # Schema dump
├── Technical_task.md       # Original design / constraint map
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
| `ops/ops_test.go` | Cache load, table counts, read/write ops, Firebird SQL idioms against a live DB |
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
| `github.com/nakagami/firebirdsql` | v0.9.17 | Firebird driver (`database/sql`) |

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
| High error rate | Expected SP exceptions under write load; lower `--conn-peak`; raise `--think-ms` |
| Lock contention | Fewer writers; shorter `--tx-timeout` visibility; inspect `*_errors.txt` |
| Slow debugging | `--think-ms 0 --debug --warmup 5 --main 10 --cooldown 5 --conn-init 1 --conn-peak 3` |

## License

GNU General Public License v3.0 — see [LICENSE](LICENSE).

## Further reading

- [Technical_task.md](Technical_task.md) — schema constraint map, profile design, ramp model, error strategy
- [EMPLOYEE_metadata.sql](EMPLOYEE_metadata.sql) — tables, procedures, and constraints
- [example_usage.sh](example_usage.sh) — printable CLI cookbook
