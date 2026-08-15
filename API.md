# fb-loadgen REST API Guide

Complete instruction and examples for the **IBSurgeon Firebird Load Generator** control-plane REST API.

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
