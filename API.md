# fb-loadgen REST API Guide

Complete instruction and examples for the **IBSurgeon Firebird Load Generator** control-plane REST API.

> **Machine-readable spec:** [api/openapi.yaml](api/openapi.yaml) (OpenAPI 3.1) describes every endpoint, schema and status code below — use it for client generation, Swagger UI or contract tests.

The web UI at `http://127.0.0.1:9000` is a thin client over this API — everything the UI can do is available programmatically.

- **Base URL**: `http://127.0.0.1:9000` (set with `--ui-addr`; defaults to localhost only)
- **Content type**: `application/json` for bodies and responses
- **Authentication**: none by default. Start the server with `--ui-token <secret>` and every mutating call (POST/PUT/PATCH/DELETE) must send `Authorization: Bearer <secret>`. Read-only GETs stay open.
- **Errors**: non-2xx status with `{"error": "..."}` body, e.g. `400` for a bad request or invalid state transition, `404` for an unknown session id.

## Quick start: the four core operations

```bash
BASE=http://127.0.0.1:9000

# 1) List available databases and take one's id
curl -s $BASE/api/sessions

# 2) Start load for that database (15-minute run)
ID=365599cb03070b90
curl -s -X POST $BASE/api/sessions/$ID/start \
  -H "Content-Type: application/json" \
  -d '{"timeLimitMin": 15}'

# 3) Check status: working / not working / connection error
curl -s $BASE/api/sessions/$ID

# 4) Stop
curl -s -X POST $BASE/api/sessions/$ID/stop
```

## 1. List available databases

```
GET /api/sessions
```

Returns every discovered database plus an aggregated fleet summary.

```json
{
  "sessions": [
    {
      "id": "365599cb03070b90",
      "name": "EMPLOYEE.FDB",
      "relPath": "db1/EMPLOYEE.FDB",
      "absPath": "C:\\Database\\TEST_10\\db1\\EMPLOYEE.FDB",
      "dsn": "localhost/3055:C:\\Database\\TEST_10\\db1\\EMPLOYEE.FDB",
      "status": "Idle",
      "profile": "write-heavy",
      "connMin": 2,
      "connMax": 2,
      "warmup": 30, "main": 120, "cooldown": 20,
      "timeLimitMin": 0,
      "phase": "—",
      "currentConns": 0, "targetConns": 0,
      "tps": 0, "errors": 0, "success": 0,
      "lastError": "",
      "missing": false
    }
  ],
  "fleet": {
    "running": 0, "paused": 0, "idle": 10, "failed": 0,
    "completed": 0, "missing": 0, "databases": 10,
    "perDbMax": 2, "totalConns": 0, "totalTps": 0,
    "budgetUsed": 0, "budgetLimit": 20
  }
}
```

The `id` is stable (derived from the absolute database path) — store it and reuse it for all per-database calls.

## 2. Start a database

```
POST /api/sessions/{id}/start
Body: {"timeLimitMin": <minutes>}    // optional
```

| `timeLimitMin` | Behavior |
|---|---|
| `15` (any positive minutes) | Timed run: warmup/main/cooldown are scaled proportionally so the whole run lasts exactly that long, then the session ends as `Completed` with reports written. |
| `0` or body omitted | Unlimited: fixed 60-second warmup, then steady main load **forever** until you call stop. |

Response: the session snapshot with `status: "Running"`, `timeLimitMin`, and `remainingSec` counting down (timed runs).

```bash
# 15-minute run
curl -s -X POST $BASE/api/sessions/$ID/start \
  -H "Content-Type: application/json" -d '{"timeLimitMin": 15}'

# unlimited run (until stopped)
curl -s -X POST $BASE/api/sessions/$ID/start \
  -H "Content-Type: application/json" -d '{"timeLimitMin": 0}'
```

Notes:
- Start runs a schema gate first: the database must be reachable and contain a readable `EMPLOYEE` table. On failure the session becomes `Failed` and the reason is in `lastError`.
- The connection budget (`maxTotalConns`) is reserved at start. If the budget is exhausted you get `400` with `max-total-conns budget exceeded`.

## 3. Status of a database

```
GET /api/sessions/{id}
```

Returns one session snapshot (same shape as the list items). The fields that answer "working / not working / error":

| Field | Meaning |
|---|---|
| `status` | Lifecycle state, see table below |
| `lastError` | Text of the last failure (connection refused, schema gate failure, …). Empty when healthy. |
| `phase` | `warmup` / `main` / `cooldown` while running |
| `remainingSec` | Seconds left for timed runs (counts down; frozen while paused) |
| `currentConns` / `targetConns` | Live / target worker connections |
| `tps`, `success`, `errors`, `expectedErrors`, `unexpectedErrors` | Live throughput and error counters |
| `latencyP50/P95/P99` | Transaction latency percentiles, ms |
| `reportDir` | Folder with result files for the last/active run |

Status values:

| Status | Meaning |
|---|---|
| `Idle` | Not running |
| `Starting` | Start in progress (schema check, cache warm-up) |
| `Running` | **Working** — load is being generated |
| `Paused` | Frozen (workers and countdown paused, connections kept) |
| `Stopping` | Stop in progress |
| `Completed` | Timed run finished naturally; final metrics retained |
| `Failed` | Could not start or validate — connection/schema error in `lastError` |

Quick health one-liner:

```bash
curl -s $BASE/api/sessions/$ID | \
  python -c "import json,sys; s=json.load(sys.stdin); print(s['status'], '|', s['lastError'] or 'no errors')"
```

To actively test connectivity (instead of inferring from the last start), call validate:

```bash
curl -s -X POST $BASE/api/sessions/$ID/validate
# -> status Idle + lastError "" when reachable, status Failed + reason otherwise
```

## 4. Stop a database

```
POST /api/sessions/{id}/stop
Body: none
```

Fast-cancels the run: workers finish their current transaction, connections close, budget is released, reports are flushed. Response snapshot has `status: "Idle"`. Works from `Running`, `Paused`, and `Starting`.

---

## Reference — all endpoints

| Operation | Method & path | Body |
|---|---|---|
| List sessions + fleet | `GET /api/sessions` | — |
| One session status | `GET /api/sessions/{id}` | — |
| Start | `POST /api/sessions/{id}/start` | `{"timeLimitMin": N}` |
| Stop | `POST /api/sessions/{id}/stop` | — |
| Pause | `POST /api/sessions/{id}/pause` | — |
| Resume | `POST /api/sessions/{id}/resume` | — |
| Connectivity + schema check | `POST /api/sessions/{id}/validate` | — |
| Edit run settings | `PATCH /api/sessions/{id}` | see below |
| Remove from table | `DELETE /api/sessions/{id}` | — |
| Start all idle | `POST /api/sessions/start-all` | `{"timeLimitMin": N}` |
| Stop all active | `POST /api/sessions/stop-all` | — |
| Pause all running | `POST /api/sessions/pause-all` | — |
| Validate all | `POST /api/sessions/validate-all` | — |
| Drop missing files | `POST /api/sessions/purge-missing` | — |
| (Re)scan disk for databases | `POST /api/discover` | `{"folder":"","mask":"*.fdb","discoverDir":"C:\\Database\\TEST_10","recursive":true}` |
| Read settings (password redacted) | `GET /api/config` | — |
| Save settings | `PUT /api/config` | see below |
| Fleet counters only | `GET /api/fleet` | — |
| List report files | `GET /api/sessions/{id}/report` | — |
| Download report file | `GET /api/sessions/{id}/report/{file}` | — |
| List schedules | `GET /api/schedules` | — |
| Create schedule | `POST /api/schedules` | schedule body, see below |
| One schedule | `GET /api/schedules/{id}` | — |
| Edit schedule | `PATCH /api/schedules/{id}` | partial merge, see below |
| Delete schedule | `DELETE /api/schedules/{id}` | — |
| Fire schedule now | `POST /api/schedules/{id}/trigger` | — |
| Runs of a schedule | `GET /api/schedules/{id}/runs` | `?limit=` |
| Run history | `GET /api/runs` | `?limit=&session=&schedule=&since=` (RFC3339) |
| One run | `GET /api/runs/{id}` | — |
| Cancel a run | `POST /api/runs/{id}/cancel` | — |
| Runs of a session | `GET /api/sessions/{id}/runs` | `?limit=` |
| Health (no auth) | `GET /api/health` | — |
| Version | `GET /api/version` | — |
| Prometheus metrics | `GET /metrics` | — |

### PATCH — change run settings (idle sessions only)

```json
{
  "profile": "write-heavy",
  "connMin": 2,
  "warmup": 30, "main": 120, "cooldown": 20,
  "thinkMs": 50, "txTimeout": 10,
  "spikeCycles": 3, "spikeHold": 10
}
```

All fields optional; `warmup/main/cooldown` define the *shape ratios* used when a time limit scales the run.

### PUT /api/config — connection settings and budget

```json
{
  "host": "localhost",
  "port": 3055,
  "user": "SYSDBA",
  "pass": "",
  "discoverDir": "C:\\Database\\TEST_10",
  "discoverMask": "*.fdb",
  "maxTotalConns": 50
}
```

Blank `pass` keeps the stored password. **Changing `maxTotalConns` resizes running sessions live** — caps and reservations move to the new even split within about a second.

## Recipes

**Poll until a timed run completes**

```bash
while true; do
  S=$(curl -s $BASE/api/sessions/$ID | python -c "import json,sys;print(json.load(sys.stdin)['status'])")
  [ "$S" = "Completed" ] || [ "$S" = "Idle" ] || [ "$S" = "Failed" ] && { echo "done: $S"; break; }
  sleep 5
done
```

**Start every idle database for 30 minutes**

```bash
curl -s -X POST $BASE/api/sessions/start-all \
  -H "Content-Type: application/json" -d '{"timeLimitMin": 30}'
```

**Fleet health at a glance**

```bash
curl -s $BASE/api/fleet
# {"running":10,"idle":0,...,"budgetUsed":20,"budgetLimit":20}
```

**Fetch results of the last run**

```bash
curl -s $BASE/api/sessions/$ID/report          # list of files
curl -s -OJ $BASE/api/sessions/$ID/report/results_summary.txt
```

**Windows PowerShell equivalents**

```powershell
$BASE = "http://127.0.0.1:9000"
$ID   = (Invoke-RestMethod "$BASE/api/sessions").sessions[0].id

Invoke-RestMethod -Method Post "$BASE/api/sessions/$ID/start" `
  -ContentType "application/json" -Body '{"timeLimitMin":15}'

(Invoke-RestMethod "$BASE/api/sessions/$ID").status

Invoke-RestMethod -Method Post "$BASE/api/sessions/$ID/stop"
```

With `--ui-token`, add `-Headers @{ Authorization = "Bearer <secret>" }` (PowerShell) or `-H "Authorization: Bearer <secret>"` (curl) to mutating calls.

## Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `400 ... budget exceeded` on start | Connection budget full — stop a session or raise `maxTotalConns` via `PUT /api/config`. |
| Start returns `Failed`, `lastError: ... connection refused` | Firebird not reachable at `host:port` — check server and port. |
| Start returns `Failed`, schema error | Database is not EMPLOYEE-compatible. |
| `404 session not found` | Unknown/stale id — list sessions again (ids change only if the file moves). |
| `cannot stop session in status Idle` | Nothing running — check status first. |

## 5. Schedules — run load automatically

Schedules fire runs without anyone calling `start`. They are persisted in `fb-loadgen.schedules.json` next to the settings file and survive restarts. A missed fire (process was down) is recorded as `missed` and skipped unless the schedule sets `"catchUp": true`.

### Create

```
POST /api/schedules
```

```json
{
  "name": "nightly-30min",
  "enabled": true,
  "trigger": { "type": "cron", "cron": "0 2 * * *", "tz": "Europe/Moscow" },
  "targets": { "all": true }                     // or {"sessionIds": ["<id>", ...]}
  ,
  "run": {
    "timeLimitMin": 30,
    "overrides": { "profile": "write-heavy" }    // optional: profile, thinkMs, txTimeout, spikeCycles, spikeHold
  },
  "policy": {
    "ifRunning": "skip",                          // skip (default) | stopAndRun
    "ifBudgetFull": "wait",                       // wait (default, up to waitBudgetSec) | skip
    "waitBudgetSec": 300,
    "staggerSec": 2,                              // delay between per-DB starts
    "catchUp": false
  },
  "notifyUrl": "https://hooks.example.com/fb",   // optional per-schedule webhook
}
```

Trigger types:

| Type | Fields | Fires |
|---|---|---|
| `once` | `at` (RFC3339) | once at that time, then the schedule disables itself |
| `interval` | `everyMin` (>=1) | anchored at each fire time |
| `cron` | `cron` (5-field), optional `tz` (IANA) | per the cron spec |

Guard: recurring triggers require `run.timeLimitMin >= 1` — an unlimited run on a recurring schedule would block every later fire (with the default `ifRunning: skip`).

### Response and state

Every schedule carries `nextRunAt` and `lastFire`:

```json
{
  "id": "sch_70a8dc9e",
  "name": "e2e-once",
  "enabled": false,
  "trigger": { "type": "once", "at": "2026-08-29T20:16:12+02:00" },
  "targets": { "sessionIds": ["770b610ea43ac7d0"] },
  "run": { "timeLimitMin": 1 },
  "nextRunAt": "",
  "lastFire": { "runId": "run_2026…", "at": "2026-08-29T20:18:04+02:00", "outcome": "fired" }
}
```

`lastFire.outcome` is `fired`, `skipped` (with a `detail`: targets busy / budget), `missed`, or `failed`. A `once` schedule is spent after its first fire attempt, whatever the outcome.

### Other schedule operations

```bash
curl -s $BASE/api/schedules                                  # list
curl -s $BASE/api/schedules/$SID                             # one
curl -s -X PATCH  $BASE/api/schedules/$SID -d '{"enabled": false}'   # pause/resume/edit (partial merge)
curl -s -X POST   $BASE/api/schedules/$SID/trigger           # fire now (asynchronous)
curl -s -X DELETE $BASE/api/schedules/$SID                   # remove
curl -s "$BASE/api/schedules/$SID/runs?limit=20"             # fire history
```

## 6. Run history

Every execution — scheduled or manual (single start, start-all) — is recorded in memory and persisted to `fb-loadgen.runs.json` (retention: last 500 runs / 30 days; runs referenced by a schedule's `lastFire` are kept).

```bash
curl -s "$BASE/api/runs?limit=10"           # newest first; filters: session=, schedule=, since= (RFC3339)
curl -s  $BASE/api/runs/run_20260829T181951_9395
curl -s  $BASE/api/sessions/$ID/runs        # history of one database
```

```json
{
  "id": "run_20260829T181951_9395",
  "origin": "manual",                       // manual | schedule
  "scheduleId": "",
  "startedAt": "2026-08-29T20:19:51+02:00",
  "finishedAt": "2026-08-29T20:20:52+02:00",
  "outcome": "ok",                          // running | ok | partial | failed | cancelled | skipped
  "sessions": [
    { "id": "770b610ea43ac7d0", "relPath": "db100/EMPLOYEE.FDB", "status": "Completed",
      "tps": 29.1, "success": 1750, "errors": 0, "reportDir": "reports/db100_…/20260829-201952" }
  ]
}
```

Per-session entry `status` is the session's terminal state, `Skipped` (with `reason`: busy / missing / unknown id / budget), or `Cancelled`. Cancel an in-flight run with `POST /api/runs/{id}/cancel` — it stops all of the run's sessions.

If the process dies mid-run, the run is persisted as `cancelled` with reason "process restarted" on the next boot.

## 7. Health, version, metrics

```bash
curl -s $BASE/api/health    # {"ok":true,"uptimeSec":123} — no auth, for monitors
curl -s $BASE/api/version   # version, Go version, OS/arch
curl -s $BASE/metrics       # Prometheus text: fleet/session gauges, schedule fire counters
```

## 8. Webhooks on run completion

Set a global receiver with `--webhook-url <url>` (plus optional `--webhook-secret`), or per schedule with `notifyUrl`. On every finished run the server POSTs:

```json
{ "event": "run.finished", "run": { …same shape as GET /api/runs/{id}… } }
```

Delivery is asynchronous with 3 attempts (2s/4s backoff). When a secret is set, requests carry `X-FBLoadGen-Signature: sha256=<hex HMAC-SHA256 of the body>` — verify it before trusting the payload.

## 9. Server flags for API operation

| Flag | Meaning |
|---|---|
| `--api-only` | Serve the REST API without the embedded UI (headless hosts, CI) |
| `--ui-auth-all` | Require the bearer token for GETs too (`/api/health` and `/metrics` stay open) |
| `--cors-origin <origin>` | Allow cross-origin browser clients (sends `Access-Control-Allow-Origin`) |
| `--webhook-url` / `--webhook-secret` | Global webhook receiver + HMAC secret |
| `--schedules-file` / `--runs-file` | Store paths (defaults next to the settings file) |

Async start: `POST /api/sessions/{id}/start` accepts `"wait": false` — returns `202 {"queued": true}` immediately; poll the session status for the outcome.
