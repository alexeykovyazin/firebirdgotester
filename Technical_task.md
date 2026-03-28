# FB-Loadgen — Go CLI Load Simulator for Firebird EMPLOYEE DB

## 1. Schema Analysis & Constraint Map

Before anything is built, it's essential to understand what the schema allows and what it blocks.

**Tables available for write workloads** (avoiding array fields `LANGUAGE_REQ`, `QUART_HEAD_CNT`):

| Table | Write operations | Key constraints |
|---|---|---|
| `CUSTOMER` | INSERT, UPDATE, DELETE | `CUST_NO > 1000`; `ON_HOLD` in `(NULL, '*')`; FK to `COUNTRY` |
| `SALES` | INSERT, UPDATE | `PO_NUMBER` starts with `'V'`, 8 chars; `ORDER_STATUS` in `('new','open','shipped','waiting')`; `PAID` in `('y','n')`; `DISCOUNT` 0–1; FK to `CUSTOMER`, `EMPLOYEE` |
| `EMPLOYEE` | UPDATE salary only | Salary must stay within `JOB.MIN_SALARY`–`JOB.MAX_SALARY`; UPDATE triggers `SAVE_SALARY_CHANGE` auto-insert into `SALARY_HISTORY` |
| `EMPLOYEE_PROJECT` | INSERT, DELETE | FK to `EMPLOYEE` + `PROJECT`; PK is `(EMP_NO, PROJ_ID)` |
| `DEPARTMENT` | UPDATE budget | `BUDGET` between 10001–2000000; FK self-referential |

**Tables that are read-only for simulation** (reference/lookup data we read but don't modify):

`COUNTRY`, `JOB`, `PROJECT`, `PROJ_DEPT_BUDGET`, `SALARY_HISTORY` (written only via trigger)

**Stored procedures mapped to profiles:**

| SP | Direction | Safe to call in load? | Notes |
|---|---|---|---|
| `ADD_EMP_PROJ` | write | ✅ | Inserts into `EMPLOYEE_PROJECT` |
| `DELETE_EMPLOYEE` | write | ⚠️ | Only for write-heavy; requires no open `SALES` ref |
| `DEPT_BUDGET` | read | ✅ | Recursive, good CPU load |
| `GET_EMP_PROJ` | read | ✅ | Simple cursor select |
| `MAIL_LABEL` | read | ✅ | Multi-line cursor, good I/O |
| `ORG_CHART` | read | ✅ | JOIN-heavy, great for read profile |
| `SHIP_ORDER` | write | ✅ | Updates `SALES`, raises exceptions to handle |
| `SUB_TOT_BUDGET` | read | ✅ | Aggregate, good for read profile |

**Excluded:** `ALL_LANGS`, `SHOW_LANGS` — both use `LANGUAGE_REQ[:i]` array access.

---

## 2. Project Layout

```
fb-loadgen/
├── main.go                  # Entry point, flag parsing, orchestration
├── config/
│   └── config.go            # Config struct, flag binding, validation
├── db/
│   └── connect.go           # Connection factory (nakagami/firebirdsql)
├── worker/
│   └── worker.go            # Worker goroutine lifecycle
├── profile/
│   ├── profile.go           # Profile interface
│   ├── write_heavy.go       # Profile: OLTP write-heavy
│   ├── read_heavy.go        # Profile: Read-heavy
│   └── spike.go             # Profile: Spike / burst
├── ops/
│   ├── reads.go             # All SELECT / SP read operations
│   └── writes.go            # All INSERT / UPDATE / DELETE / SP write ops
├── metrics/
│   ├── collector.go         # Atomic counters, latency histograms
│   └── reporter.go          # Console ticker + CSV writer
├── ramp/
│   └── ramp.go              # Connection ramp scheduler
└── go.mod
```

---

## 3. CLI Flags (all configuration via flags)

```
fb-loadgen [flags]

Connection:
  --dsn           string   Firebird DSN (default: "localhost/3055:./EMPLOYEE.FDB")
  --user          string   DB user (default: "SYSDBA")
  --pass          string   DB password (default: "masterkey")

Profile:
  --profile       string   Simulation profile: write-heavy | read-heavy | spike (required)

Connection scaling:
  --conn-init     int      Initial number of connections (default: 2)
  --conn-peak     int      Peak number of connections (default: 20)

Timing (all in seconds):
  --warmup        int      Ramp-up / heat period in seconds (default: 30)
  --main          int      Main steady-state period in seconds (default: 120)
  --cooldown      int      Graceful disconnect period in seconds (default: 20)

Spike profile extras:
  --spike-cycles  int      Number of spike cycles during main period (default: 3)
  --spike-hold    int      Seconds to sustain peak before dropping (default: 10)

Output:
  --csv           string   Path to CSV output file (default: "results.csv")
  --report-every  int      Console report interval in seconds (default: 5)

Misc:
  --think-ms      int      Worker think time between ops in ms (default: 50)
  --tx-timeout    int      Statement timeout in seconds (default: 10)
```

---

## 4. Profiles — Operation Mix

### Profile A: `write-heavy`

Designed for OLTP insert/update/delete stress. Each worker transaction picks one operation based on weighted random selection:

| Weight | Operation | Implementation |
|---|---|---|
| 25% | Insert new `CUSTOMER` | Direct INSERT, `CUST_NO` via `GEN_ID(CUST_NO_GEN,1)` |
| 20% | Insert new `SALES` order | Direct INSERT with `PO_NUMBER = 'V' + 7 random digits` |
| 15% | Update `SALES` status | `order_status` transition: `new→open→shipped` |
| 15% | Call `SHIP_ORDER` SP | Updates `SALES`, handle `order_already_shipped` exception |
| 10% | Update `EMPLOYEE` salary | Stay within `JOB.MIN/MAX_SALARY`; triggers `SALARY_HISTORY` |
| 10% | Call `ADD_EMP_PROJ` SP | Insert into `EMPLOYEE_PROJECT` |
| 5% | Delete `EMPLOYEE_PROJECT` row | Clean up project assignments |

Each operation runs in its own explicit transaction (`BEGIN → op → COMMIT`, or `ROLLBACK` on error).

### Profile B: `read-heavy`

Designed for OLAP-style read stress with rare writes. Workers mostly query:

| Weight | Operation | Implementation |
|---|---|---|
| 30% | Call `ORG_CHART` SP | Full dept hierarchy + employee count JOIN |
| 25% | Call `DEPT_BUDGET` SP | Recursive budget rollup |
| 15% | Call `MAIL_LABEL` SP | Customer address fetch |
| 10% | Call `GET_EMP_PROJ` SP | Cursor over employee projects |
| 10% | Call `SUB_TOT_BUDGET` SP | Aggregate dept budgets |
| 5% | SELECT with JOIN: `EMPLOYEE + DEPARTMENT + JOB` | Multi-table read |
| 5% | UPDATE `DEPARTMENT.BUDGET` | Rare write to keep cache pressure up |

All reads use `READ COMMITTED` isolation. The rare update uses its own short transaction.

### Profile C: `spike`

Starts like `read-heavy`, then at each spike cycle rapidly injects extra workers pushing `write-heavy` operations, then drains back down. The connection count follows a sawtooth wave during the main period.

**Spike behavior:**
- During warmup: ramp from `--conn-init` to a mid-level (50% of peak) with read ops
- At each spike cycle: rapidly add connections from mid-level to `--conn-peak`, sustain for `--spike-hold` seconds with full write-heavy mix, then drain back to mid-level
- Between spikes: return to read-heavy mix
- During cooldown: drain all connections gracefully

---

## 5. Connection Ramp Architecture

The `ramp` package owns a `Scheduler` that runs in its own goroutine and controls a `workerPool` channel:

```
Phase         conn-init → conn-peak (over --warmup seconds)
              Linear interpolation: every tick, spawn N new workers until peak

Main phase    Hold at conn-peak (write-heavy / read-heavy)
              For spike: run sawtooth cycles

Cooldown      Send cancel signal to workers one by one over --cooldown seconds
              Each worker finishes its current TX, closes its connection, exits
```

Each worker owns **one dedicated connection** for its entire lifetime. This maximises Firebird connection-level load realism and avoids pool-sharing artifacts.

The pool is represented as a `[]context.CancelFunc` slice — adding a worker means `go worker.Run(ctx, cancelFn, conn)`, removing means calling `cancelFn` on the oldest worker.

---

## 6. Metrics & Reporting

### In-process metrics (`metrics/collector.go`)

Using `sync/atomic` for lock-free counters:

```go
type Metrics struct {
    TxSuccess   atomic.Int64
    TxError     atomic.Int64
    // Latency histogram: buckets in ms: <5, <10, <25, <50, <100, <250, <500, <1000, >=1000
    LatBuckets  [9]atomic.Int64
    ConnCount   atomic.Int32
}
```

Each worker records its own op latency and atomically increments the relevant bucket.

### Console reporter (`metrics/reporter.go`)

Every `--report-every` seconds, prints a live summary line:

```
[00:01:35] conns=12 | TPS=47.3 | ok=5678 err=3 | lat p50=18ms p95=84ms p99=210ms
```

### CSV output

One row appended per report interval:

```csv
timestamp,elapsed_sec,connections,tps,tx_ok,tx_err,lat_p50_ms,lat_p95_ms,lat_p99_ms
2026-03-28T10:00:05Z,5,4,12.4,62,0,15,45,89
...
```

File is opened at start and fsynced on clean shutdown.

---

## 7. Error Handling Strategy

Firebird SPs raise named exceptions (`REASSIGN_SALES`, `ORDER_ALREADY_SHIPPED`, `CUSTOMER_ON_HOLD`, etc.). The `ops` layer must:

- Catch Firebird exception codes / messages and classify them as **expected** (count as `TxError` but don't log noisily) vs **unexpected** (log with full detail)
- Expected: `order_already_shipped`, `customer_on_hold`, `customer_check`, `unknown_emp_id`, `reassign_sales` — these are normal business logic rejections
- Unexpected: connection loss, lock timeout, deadlock → log + reconnect

---

## 8. Implementation Roadmap

### Phase 1 — Skeleton (Day 1)
- `go mod init fb-loadgen`
- Add `github.com/nakagami/firebirdsql`
- Implement `config/config.go` with all flags and validation
- Implement `db/connect.go` — open single connection, ping, close
- Smoke test: connect to `EMPLOYEE.FDB`, run `SELECT COUNT(*) FROM EMPLOYEE`

### Phase 2 — Operations (Day 2)
- Implement `ops/reads.go` — all SP calls and SELECT queries
- Implement `ops/writes.go` — all INSERTs, UPDATEs, DELETEs with constraint-aware data generation
- Unit test each op independently against a live DB
- Implement weighted random op selector (`profile/profile.go` interface)

### Phase 3 — Worker + Ramp (Day 3)
- Implement `worker/worker.go` — goroutine loop: pick op → execute → record metrics → think time → repeat until ctx cancelled
- Implement `ramp/ramp.go` — linear ramp scheduler, spike sawtooth scheduler
- Wire profiles A, B, C

### Phase 4 — Metrics + Output (Day 4)
- Implement `metrics/collector.go` + percentile calculation from buckets
- Implement `metrics/reporter.go` — console ticker + CSV appender
- Implement graceful shutdown: `os.Signal` → cancel root context → cooldown → flush CSV → exit

### Phase 5 — Hardening (Day 5)
- Handle all known Firebird exceptions in ops layer
- Add reconnect logic on connection drop
- Add `--dry-run` flag (connect, list what would run, exit)
- End-to-end test: all 3 profiles, verify CSV output

---

## 9. Key Technical Notes

**Data generation constraints to respect:**
- `PO_NUMBER`: always `'V' + 7 digits` (e.g. `V1234567`) — constraint `STARTING WITH 'V'`, 8 chars
- `CUST_NO`: use `GEN_ID(CUST_NO_GEN, 1)` inline in INSERT, not pre-fetched
- `SALARY` updates: must query `JOB.MIN_SALARY`/`MAX_SALARY` first and stay within bounds — the table CHECK constraint will reject anything outside
- `PERCENT_CHANGE` in `SALARY_HISTORY`: between -50 and 50 — but this is written by trigger automatically
- `DEPT_NO`: valid values are `'000'` or `'001'`–`'999'` — pick from existing rows at startup
- `PROJ_ID`: must be uppercase, pick from existing `PROJECT` rows at startup
- `ORDER_STATUS` transitions: only `'new'→'open'→'shipped'` or `'waiting'` — never set a shipped order back

**Startup cache:** On init, each worker pre-fetches a small lookup set (valid `DEPT_NO` list, `EMP_NO` list, `PROJ_ID` list, `CUST_NO` list) to use as random pick pools during the run. This avoids constant sub-selects.

**`nakagami/firebirdsql` specifics:**
- DSN format: `user:pass@host/port:path/to/DB.FDB`
- SP calls with selectable SPs use `SELECT ... FROM <sp_name>(params)` syntax
- Executable SPs use `EXECUTE PROCEDURE <sp_name>(params)`
- Firebird exception messages come through as Go `error` strings — match by substring

---

## 10. Example Invocations

```bash
# Spike profile: 5 connections → 50 peak, 60s warmup, 5 minute main, 3 spikes
fb-loadgen \
  --profile spike \
  --conn-init 5 --conn-peak 50 \
  --warmup 60 --main 300 --cooldown 30 \
  --spike-cycles 3 --spike-hold 15 \
  --csv spike_run1.csv --report-every 10

# Write-heavy: 10 → 100 connections
fb-loadgen \
  --profile write-heavy \
  --conn-init 10 --conn-peak 100 \
  --warmup 30 --main 120 --cooldown 20 \
  --csv write_run.csv

# Read-heavy: light load
fb-loadgen \
  --profile read-heavy \
  --conn-init 2 --conn-peak 20 \
  --warmup 15 --main 180 --cooldown 10 \
  --think-ms 100 --csv read_run.csv
```

---

This plan covers everything. Ready to start generating code — which phase or file do you want first?