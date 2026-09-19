// Builds IBSurgeon_LoadGenerator.pptx from assets/ (diagrams + screenshots).
// Run: node build.mjs   (after rendering diagrams; see PRESENTATION_PLAN.md)
import pptxgen from "pptxgenjs";
import { readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const A = (f) => join(here, "assets", f);

// ---------- theme ----------
const ACCENT = "2563EB", TEXT = "1C2733", MUTED = "5F6F83", BG = "F5F7FA",
      PANEL = "FFFFFF", BORDER = "D8DFE8", OK = "15803D", DANGER = "DC2626",
      SOFT = "EEF2F7";
const W = 13.333, H = 7.5;

function pngSize(file) {
  const b = readFileSync(file);
  return { w: b.readUInt32BE(16), h: b.readUInt32BE(20) };
}
function fitImage(file, box) {
  const { w, h } = pngSize(file);
  const s = Math.min(box.w / w, box.h / h);
  const iw = w * s, ih = h * s;
  return { x: box.x + (box.w - iw) / 2, y: box.y + (box.h - ih) / 2, w: iw, h: ih };
}

const pres = new pptxgen();
pres.defineLayout({ name: "WIDE", width: W, height: H });
pres.layout = "WIDE";
pres.author = "IBSurgeon";
pres.title = "IBSurgeon Firebird Load Generator";

let pageNo = 1; // title slide is #1 (unnumbered); content footers show physical position
function baseSlide({ title, kicker, notes }) {
  const s = pres.addSlide();
  s.background = { color: BG };
  pageNo++;
  if (title) {
    s.addText(title, { x: 0.55, y: 0.28, w: W - 1.1, h: 0.6, fontSize: 27,
      bold: true, color: TEXT, fontFace: "Segoe UI" });
  }
  if (kicker) {
    s.addText(kicker, { x: 0.57, y: 0.86, w: W - 1.1, h: 0.34, fontSize: 13,
      color: MUTED, fontFace: "Segoe UI" });
  }
  s.addText([
    { text: "IBSurgeon Firebird Load Generator", options: { color: MUTED } },
    { text: "   |   " + pageNo, options: { color: ACCENT, bold: true } },
  ], { x: W - 3.6, y: H - 0.42, w: 3.3, h: 0.3, fontSize: 9, align: "right", fontFace: "Segoe UI" });
  if (notes) s.addNotes(notes);
  return s;
}
function panel(s, x, y, w, h) {
  s.addShape("roundRect", { x, y, w, h, fill: { color: PANEL },
    line: { color: BORDER, width: 1 }, rectRadius: 0.06 });
}
function card(s, file, x, y, w, h) {
  panel(s, x, y, w, h);
  const f = fitImage(A(file), { x: x + 0.12, y: y + 0.12, w: w - 0.24, h: h - 0.24 });
  s.addImage({ path: A(file), ...f });
}
function chip(s, text, x, y, w, opts = {}) {
  s.addShape("roundRect", { x, y, w, h: 0.42, fill: { color: opts.fill || SOFT },
    line: { color: opts.line || BORDER, width: 1 }, rectRadius: 0.2 });
  s.addText(text, { x, y, w, h: 0.42, align: "center", valign: "middle",
    fontSize: opts.size || 11.5, color: opts.color || TEXT, bold: !!opts.bold, fontFace: "Segoe UI" });
}
function codeBox(s, lines, x, y, w, h) {
  s.addShape("roundRect", { x, y, w, h, fill: { color: "1C2733" }, rectRadius: 0.06 });
  s.addText(lines.join("\n"), { x: x + 0.15, y: y + 0.1, w: w - 0.3, h: h - 0.2,
    fontSize: 10.5, color: "E7ECF3", fontFace: "Consolas", valign: "top" });
}

// ---------- 1. title ----------
{
  const s = pres.addSlide();
  s.background = { color: BG };
  s.addShape("rect", { x: 0, y: 0, w: 0.18, h: H, fill: { color: ACCENT } });
  s.addText([
    { text: "IBSurgeon ", options: { color: TEXT } },
    { text: "Firebird Load Generator", options: { color: ACCENT } },
  ], { x: 0.7, y: 1.7, w: 7.6, h: 1.6, fontSize: 40, bold: true, fontFace: "Segoe UI" });
  s.addText("Load testing for Firebird — from a one-shot CLI to a scheduled, multi-database fleet with a full REST API.",
    { x: 0.72, y: 3.15, w: 6.6, h: 1.0, fontSize: 16, color: MUTED, fontFace: "Segoe UI" });
  s.addText("EMPLOYEE-schema workloads · ramp + spike profiles · schedules · run history · Prometheus & webhooks",
    { x: 0.72, y: 4.35, w: 6.6, h: 0.8, fontSize: 12, color: MUTED, fontFace: "Segoe UI" });
  s.addShape("roundRect", { x: 7.75, y: 1.15, w: 5.1, h: 3.35, fill: { color: PANEL },
    line: { color: BORDER, width: 1 }, rectRadius: 0.06, shadow: { type: "outer", blur: 8, offset: 2, color: "1C2733", opacity: 0.18 } });
  {
    const f = fitImage(A("ui_sessions.png"), { x: 7.87, y: 1.27, w: 4.86, h: 3.11 });
    s.addImage({ path: A("ui_sessions.png"), ...f });
  }
  s.addText("Multi-database control plane — live status, one click per database",
    { x: 7.75, y: 4.6, w: 5.1, h: 0.3, fontSize: 10, color: MUTED, align: "center", fontFace: "Segoe UI" });
  s.addNotes("Single Go binary; drives real Firebird workloads against the classic EMPLOYEE sample schema. Screenshot: sanitized demo rig (C:\\Demo\\FirebirdLoad).");
}

// ---------- 2. executive summary ----------
{
  const s = baseSlide({ title: "Executive summary",
    notes: "Facts: profiles in profile/ (write-heavy, read-heavy, spike); 10+ concurrent sessions exercised in the demo rig; API surface in API.md." });
  const stats = [
    ["1", "Go binary — CLI, UI and API in one executable"],
    ["4", "workload profiles: write-heavy, read-heavy, spike, oltp-emul"],
    ["10+", "databases per control plane, one worker = one connection"],
    ["100%", "API-first: schedules, runs and metrics without the UI"],
  ];
  stats.forEach(([n, t], i) => {
    const x = 0.55 + i * 3.12;
    panel(s, x, 1.25, 2.9, 2.15);
    s.addText(n, { x: x + 0.15, y: 1.45, w: 2.6, h: 0.95, fontSize: 44, bold: true, color: ACCENT, fontFace: "Segoe UI" });
    s.addText(t, { x: x + 0.18, y: 2.42, w: 2.55, h: 0.9, fontSize: 11.5, color: TEXT, fontFace: "Segoe UI" });
  });
  s.addText("TWO WAYS TO RUN", { x: 0.57, y: 3.75, w: 4, h: 0.3, fontSize: 11, bold: true, color: MUTED, fontFace: "Segoe UI" });
  panel(s, 0.55, 4.1, 6.0, 2.6);
  s.addText("One-shot CLI", { x: 0.8, y: 4.25, w: 5.5, h: 0.35, fontSize: 14, bold: true, color: TEXT, fontFace: "Segoe UI" });
  codeBox(s, ["fb-loadgen --profile write-heavy \\", "  --dsn \"localhost/3055:./EMPLOYEE.FDB\" \\", "  --warmup 30 --main 120 --cooldown 20"], 0.8, 4.7, 5.5, 1.15);
  s.addText("Ramp, measure, report — one database, one run", { x: 0.8, y: 6.0, w: 5.5, h: 0.35, fontSize: 11, color: MUTED, fontFace: "Segoe UI" });
  panel(s, 6.8, 4.1, 6.0, 2.6);
  s.addText("Control plane", { x: 7.05, y: 4.25, w: 5.5, h: 0.35, fontSize: 14, bold: true, color: TEXT, fontFace: "Segoe UI" });
  codeBox(s, ["fb-loadgen --ui   # UI + REST API + schedules", "curl $BASE/api/sessions/$ID/start \\", "     -d '{\"timeLimitMin\": 15}'"], 7.05, 4.7, 5.5, 1.15);
  s.addText("Discover many databases, run and schedule a fleet", { x: 7.05, y: 6.0, w: 5.5, h: 0.35, fontSize: 11, color: MUTED, fontFace: "Segoe UI" });
}

// ---------- 3. big picture ----------
{
  const s = baseSlide({ title: "How it works — the life of one run",
    kicker: "One process, one worker = one dedicated connection. Gate and cache run at every start.",
    notes: "Simplification note: discovery is the control-plane view; schema gate + cache happen per session start (session/manager.go startInternal)." });
  card(s, "d1_overview.png", 0.55, 1.45, 12.25, 3.3);
  const chips = ["Discover *.fdb", "Schema gate", "Cache warm-up", "Ramp workers", "Measure", "Report"];
  chips.forEach((c, i) => chip(s, c, 0.72 + i * 2.02, 5.15, 1.86, { bold: true }));
  s.addText("Everything runs inside one Go binary — the same executable is the CLI, the control plane and the load generator.",
    { x: 0.57, y: 5.9, w: 12.2, h: 0.5, fontSize: 13, color: MUTED, align: "center", fontFace: "Segoe UI" });
}

// ---------- 4. workload model ----------
{
  const s = baseSlide({ title: "Workload model — real operation weights",
    kicker: "Schema-aware operations: inserts respect FKs, updates follow state transitions, expected business exceptions are not errors.",
    notes: "Weights are the profile definitions, not estimates: write-heavy profile/write_heavy.go:18-66, read-heavy profile/read_heavy.go (7 ops each, from the technical task)." });
  card(s, "d2a_write_heavy.png", 0.55, 1.5, 6.0, 4.4);
  card(s, "d2b_read_heavy.png", 6.8, 1.5, 6.0, 4.4);
  chip(s, "Respects EMPLOYEE constraints (PO_NUMBER, salary bounds, status transitions)", 0.9, 6.15, 5.9);
  chip(s, "Business exceptions like order_already_shipped tracked separately", 7.05, 6.15, 5.7);
}

// ---------- 5. ramp ----------
{
  const s = baseSlide({ title: "Connection ramp",
    kicker: "Warmup (linear up) → main (random walk between min and max) → cooldown (linear down). A time limit rescales all phases.",
    notes: "Defaults 30/120/20 s (config/config.go:89-91). No-limit mode: fixed 60 s warmup, then steady until stopped. Spike profile: sawtooth cycles in main." });
  const rnd = (() => { let x = 42; return () => (x = (x * 1103515245 + 12345) % 2147483648) / 2147483648; })();
  const cats = [], conns = [];
  let v = 2;
  for (let t = 0; t <= 170; t += 5) {
    cats.push(String(t));
    if (t <= 30) v = 2 + (20 - 2) * (t / 30);
    else if (t <= 150) v = Math.min(20, Math.max(2, v + (rnd() * 8 - 4)));
    else v = 2 + (20 - 2) * ((170 - t) / 20);
    conns.push(Math.round(v * 10) / 10);
  }
  s.addChart(pres.ChartType.line, [{ name: "worker connections", labels: cats, values: conns }], {
    x: 0.55, y: 1.5, w: 8.3, h: 4.9,
    chartColors: [ACCENT], lineSize: 2.5, lineSmooth: true,
    catAxisTitle: "seconds", catAxisTitleFontSize: 11, valAxisTitle: "connections",
    valAxisTitleFontSize: 11, valAxisMaxVal: 22, valAxisMinVal: 0,
    showLegend: false, catLabelFormatCode: "0", chartArea: { fill: { color: PANEL } },
    plotArea: { fill: { color: PANEL } },
  });
  const phases = [
    ["Warmup", "linear ramp to the peak", "0 → 30 s"],
    ["Main", "random walk between min/max", "30 → 150 s"],
    ["Cooldown", "graceful ramp down", "150 → 170 s"],
  ];
  phases.forEach(([b, t, d], i) => {
    panel(s, 9.1, 1.5 + i * 1.3, 3.7, 1.1);
    s.addText([{ text: b + "  ", options: { bold: true, color: TEXT } },
               { text: d, options: { color: ACCENT, fontSize: 11 } }],
      { x: 9.3, y: 1.6 + i * 1.3, w: 3.4, h: 0.4, fontSize: 13, fontFace: "Segoe UI" });
    s.addText(t, { x: 9.3, y: 2.0 + i * 1.3, w: 3.4, h: 0.5, fontSize: 11, color: MUTED, fontFace: "Segoe UI" });
  });
  chip(s, "spike profile = sawtooth cycles", 9.1, 5.5, 3.7, { bold: true });
  chip(s, "no-limit mode: 60 s warmup, then steady until stopped", 0.55, 6.55, 8.3);
}

// ---------- 6. session lifecycle ----------
{
  const s = baseSlide({ title: "Session lifecycle",
    kicker: "Every database in the fleet is a session with an explicit, observable state.",
    notes: "State machine per session/manager.go. Budget exhaustion returns a session to Idle (recorded as skipped); only gate/cache/validation failures set Failed." });
  card(s, "d4_states.png", 0.55, 1.45, 7.3, 5.35);
  const notes = [
    ["Starting", "schema gate + connection-budget reservation"],
    ["Running", "load is being generated; live TPS and latency"],
    ["Paused", "workers and countdown frozen, connections kept"],
    ["Completed", "time limit reached — final metrics retained"],
    ["Failed", "connection or schema problem, reason in lastError"],
  ];
  notes.forEach(([b, t], i) => {
    s.addText([{ text: b + " — ", options: { bold: true, color: ACCENT } },
               { text: t, options: { color: TEXT } }],
      { x: 8.15, y: 1.7 + i * 1.0, w: 4.6, h: 0.9, fontSize: 13, fontFace: "Segoe UI" });
  });
}

// ---------- 7. safety rails ----------
{
  const s = baseSlide({ title: "Safety rails & compatibility",
    notes: "Budget: maxTotalConns, even per-DB split, live resize (config/config.go:102). Driver: IBSurgeon firebirdsql-go fork of nakagami/firebirdsql." });
  const rails = [
    ["Schema gate", "a database must be reachable and EMPLOYEE-readable before any load"],
    ["Startup lookup cache", "preloads valid FK ranges — inserts never guess"],
    ["Fleet connection budget", "maxTotalConns, split evenly, resized live across running sessions"],
    ["Transaction timeouts", "per-session tx timeout; expected business exceptions classified apart from real errors"],
  ];
  rails.forEach(([b, t], i) => {
    panel(s, 0.55, 1.4 + i * 1.28, 7.4, 1.1);
    s.addText([{ text: b + " — ", options: { bold: true, color: ACCENT } },
               { text: t, options: { color: TEXT } }],
      { x: 0.8, y: 1.52 + i * 1.28, w: 6.95, h: 0.9, fontSize: 13, fontFace: "Segoe UI" });
  });
  panel(s, 8.2, 1.4, 4.6, 5.1);
  s.addText("Built on the IBSurgeon driver", { x: 8.45, y: 1.6, w: 4.1, h: 0.4, fontSize: 14, bold: true, color: ACCENT, fontFace: "Segoe UI" });
  s.addText([
    { text: "firebirdsql-go", options: { bold: true } },
    { text: " — IBSurgeon's maintained fork of the popular Go Firebird driver.\n\n", options: {} },
    { text: "Firebird wire protocol\n", options: { bold: true } },
    { text: "SRP and legacy authentication\n", options: { bold: true } },
    { text: "Wire encryption and compression\n\n", options: { bold: true } },
    { text: "Registered as driver ", options: {} },
    { text: "firebirdsql", options: { fontFace: "Consolas", fontSize: 11 } },
    { text: " for Go's database/sql.", options: {} },
  ], { x: 8.45, y: 2.05, w: 4.1, h: 4.2, fontSize: 13, color: TEXT, fontFace: "Segoe UI" });
}

// ---------- 8. metrics & reports ----------
{
  const s = baseSlide({ title: "Metrics & reports",
    kicker: "Live counters in the UI and API; durable artifacts on disk after every run.",
    notes: "Latency percentiles and error classification from worker/metrics; per-run report directories under reports/." });
  const mets = [
    ["TPS & success", "transactions per second, per session and fleet-wide"],
    ["Latency p50 / p95 / p99", "transaction latency percentiles"],
    ["Error classification", "expected (business) vs unexpected, with top operations"],
    ["SQL error log", "every failed statement logged with context"],
  ];
  mets.forEach(([b, t], i) => {
    panel(s, 0.55, 1.45 + i * 1.3, 6.6, 1.12);
    s.addText([{ text: b + " — ", options: { bold: true, color: ACCENT } },
               { text: t, options: { color: TEXT } }],
      { x: 0.8, y: 1.57 + i * 1.3, w: 6.2, h: 0.9, fontSize: 13, fontFace: "Segoe UI" });
  });
  panel(s, 7.4, 1.45, 5.4, 5.1);
  s.addText("Per-run report directory", { x: 7.65, y: 1.65, w: 4.9, h: 0.4, fontSize: 14, bold: true, color: ACCENT, fontFace: "Segoe UI" });
  codeBox(s, [
    "reports/db1_EMPLOYEE.FDB/20260829-201952/",
    "  results_summary.txt",
    "  results_performance.txt",
    "  results_operations.txt",
    "  results_status.txt",
    "  results.csv  +  results.csv_latency.txt",
    "  sql_errors.log",
    "",
    "GET /api/sessions/{id}/report/{file}",
  ], 7.65, 2.15, 4.9, 3.3);
  s.addText("Download any file over the API — no filesystem access needed.",
    { x: 7.65, y: 5.6, w: 4.9, h: 0.6, fontSize: 11, color: MUTED, fontFace: "Segoe UI" });
}

// ---------- 9. architecture ----------
{
  const s = baseSlide({ title: "Control plane architecture",
    kicker: "One Go process serves the UI, the REST API, the scheduler and the load itself.",
    notes: "Components: ui/ (embedded SPA + handlers), session/ (manager, run history), schedule/ (engine), ramp/ + worker/ (load). Driver: IBSurgeon firebirdsql-go." });
  card(s, "d5_architecture.png", 0.55, 1.45, 8.6, 5.4);
  const pts = [
    "Embedded SPA — no separate web server",
    "REST API — same process, same binary",
    "Schedule engine — persisted, restart-safe",
    "Session manager — status + connection budget",
    "Run history — records, webhooks, retention",
  ];
  pts.forEach((p, i) => {
    s.addText([{ text: "▪ ", options: { color: ACCENT, bold: true } }, { text: p }],
      { x: 9.35, y: 1.75 + i * 0.95, w: 3.5, h: 0.85, fontSize: 12.5, color: TEXT, fontFace: "Segoe UI" });
  });
}

// ---------- 10. UI approach ----------
{
  const s = baseSlide({ title: "UI approach — a thin client over the API",
    kicker: "The UI is just another API client: everything it shows comes from the same REST endpoints.",
    notes: "ui/static: vanilla JS SPA embedded via go:embed; poll interval 1.5 s; tabs Sessions/Schedules/Runs; details drawer; light/dark themes." });
  const pts = [
    ["Embedded, zero-install", "vanilla-JS SPA inside the exe — no framework, no build step"],
    ["Live, not frozen", "polls the API every 1.5 s; per-row start/pause/stop"],
    ["Schedules & Runs tabs", "create schedules, run now, inspect per-session outcomes"],
    ["Operator-friendly", "details drawer, light/dark themes, bearer-token prompt"],
  ];
  pts.forEach(([b, t], i) => {
    panel(s, 0.55, 1.45 + i * 1.32, 4.75, 1.14);
    s.addText([{ text: b, options: { bold: true, color: ACCENT } },
               { text: " — " + t, options: { color: TEXT } }],
      { x: 0.78, y: 1.57 + i * 1.32, w: 4.35, h: 0.95, fontSize: 12, fontFace: "Segoe UI" });
  });
  card(s, "d6_ui_lifecycle.png", 5.5, 1.45, 7.3, 5.4);
}

// ---------- 11. API-first ----------
{
  const s = baseSlide({ title: "API-first: everything is an endpoint",
    kicker: "The API is the product surface; the UI proves it. Full guide with curl and PowerShell recipes in API.md.",
    notes: "Auth: --ui-token (mutations), --ui-auth-all (GETs too; health and /metrics stay open). --cors-origin for browser clients. Async start: wait:false -> 202." });
  const groups = [
    ["Sessions", ["GET /api/sessions", "GET /api/sessions/{id}", "POST …/start  stop  pause  resume", "POST …/validate", "PATCH /api/sessions/{id}", "start-all / stop-all / pause-all"]],
    ["Schedules", ["GET /api/schedules", "POST /api/schedules", "PATCH / DELETE /{id}", "POST /{id}/trigger", "GET /{id}/runs"]],
    ["Runs & reports", ["GET /api/runs?session=&since=", "GET /api/runs/{id}", "POST /api/runs/{id}/cancel", "GET …/{id}/report/{file}"]],
    ["Discovery & ops", ["POST /api/discover", "GET/PUT /api/config", "GET /api/health  /api/version", "GET /metrics (Prometheus)"]],
  ];
  groups.forEach(([name, eps], i) => {
    const x = 0.55 + (i % 2) * 6.25, y = 1.45 + Math.floor(i / 2) * 2.15;
    panel(s, x, y, 6.0, 1.98);
    s.addText(name, { x: x + 0.2, y: y + 0.1, w: 5.6, h: 0.32, fontSize: 12.5, bold: true, color: ACCENT, fontFace: "Segoe UI" });
    s.addText(eps.join("\n"), { x: x + 0.2, y: y + 0.44, w: 5.6, h: 1.45, fontSize: 10,
      fontFace: "Consolas", color: TEXT, valign: "top" });
  });
  const flags = [["--ui-token", "bearer auth"], ["--ui-auth-all", "protect GETs too"], ["--api-only", "headless for CI"], ["wait:false", "async start → 202"], ["--cors-origin", "browser clients"]];
  flags.forEach(([a, b], i) => {
    chip(s, a + " — " + b, 0.55 + i * 2.48, 6.0, 2.36, { size: 10 });
  });
}

// ---------- 12. scheduling ----------
{
  const s = baseSlide({ title: "Scheduled runs — load without a human",
    kicker: "Persisted schedules fire runs automatically; every fire is recorded, skippable and budget-aware.",
    notes: "schedule/ package: cron via robfig/cron v3 + tzdata; policies in Policy struct; store fb-loadgen.schedules.json; missed fires recorded (catchUp to force). Recurring triggers require timeLimitMin >= 1." });
  card(s, "d7_schedule.png", 0.55, 1.45, 7.9, 4.15);
  const chips = [
    ["once / interval / cron + timezone", 0.55, 5.85, 3.85],
    ["ifRunning: skip or stopAndRun", 4.6, 5.85, 3.85],
    ["ifBudgetFull: wait (timeout) or skip", 8.65, 5.85, 4.15],
    ["staggerSec between per-DB starts", 0.55, 6.45, 3.85],
    ["survives restarts; catchUp for missed fires", 4.6, 6.45, 4.6],
    ["optional webhook per schedule", 9.4, 6.45, 3.4],
  ];
  chips.forEach(([t, x, y, w]) => chip(s, t, x, y, w, { size: 10.5 }));
}

// ---------- 13. history & observability ----------
{
  const s = baseSlide({ title: "Run history & observability",
    kicker: "Every execution — scheduled or manual — becomes a queryable record.",
    notes: "session/run.go: retention 500 runs / 30 days, schedule-referenced runs kept; interrupted runs marked cancelled on boot. Webhook HMAC: X-FBLoadGen-Signature." });
  card(s, "d8_history.png", 0.55, 1.5, 7.6, 3.4);
  const pts = [
    ["Per-session outcomes", "status, TPS, success/errors, report dir, skip reasons"],
    ["Retention", "500 runs / 30 days, atomic writes"],
    ["Honest history", "process restarted mid-run → run marked cancelled"],
    ["Webhooks", "HMAC-signed JSON on every finished run"],
    ["Prometheus", "fleet, per-session and schedule fire metrics"],
  ];
  pts.forEach(([b, t], i) => {
    s.addText([{ text: b + " — ", options: { bold: true, color: ACCENT } },
               { text: t, options: { color: TEXT } }],
      { x: 0.6 + (i % 2) * 3.95, y: 5.0 + Math.floor(i / 2) * 0.8, w: 3.8, h: 0.72, fontSize: 11.5, fontFace: "Segoe UI" });
  });
  panel(s, 8.4, 1.5, 4.4, 5.3);
  s.addText("Query it", { x: 8.65, y: 1.7, w: 3.9, h: 0.4, fontSize: 14, bold: true, color: ACCENT, fontFace: "Segoe UI" });
  codeBox(s, [
    "curl $BASE/api/runs?limit=10",
    "curl $BASE/api/runs/$RUN",
    "curl $BASE/api/schedules",
    "",
    "# runs?outcome=ok&since=…",
    "# per-session:",
    "curl $BASE/api/sessions/$ID/runs",
  ], 8.65, 2.2, 3.9, 2.6);
  s.addText("Outcome: running / ok / partial / failed / cancelled / skipped",
    { x: 8.65, y: 5.0, w: 3.9, h: 0.8, fontSize: 11, color: MUTED, fontFace: "Segoe UI" });
}

// ---------- 14. AI in the loop ----------
{
  const s = baseSlide({ title: "AI in the loop — agents run the load spikes",
    kicker: "The API is a machine contract: JSON everywhere, an OpenAPI spec, signed webhooks. An agent can close the loop unattended.",
    notes: "Concrete loop: the agent watches /metrics (or receives the run.finished webhook), decides a spike is needed, triggers a spike-profile schedule (run.overrides.profile = spike), reads the run record, then adapts thinkMs / txTimeout via PATCH and repeats. The OpenAPI spec (api/openapi.yaml) is the contract agent frameworks consume — no custom integration. Safety stays server-side: ifRunning=skip, budget wait, time limits cap every spike." });
  card(s, "d9_ai_loop.png", 0.55, 1.45, 12.25, 2.5);
  const pts = [
    ["Machine-readable contract", "the agent reads api/openapi.yaml — endpoints, schemas and status codes without human docs"],
    ["Headless by design", "--api-only serves JSON only; async start returns 202; /api/health for liveness"],
    ["Push, not poll", "the HMAC-signed run.finished webhook announces every completed run"],
    ["Bounded autonomy", "server-side policies cap every spike: ifRunning, budget wait, time limits"],
  ];
  pts.forEach(([b, t], i) => {
    const x = 0.55 + (i % 2) * 6.25, y = 4.25 + Math.floor(i / 2) * 1.45;
    panel(s, x, y, 6.0, 1.28);
    s.addText([{ text: b + " — ", options: { bold: true, color: ACCENT } },
               { text: t, options: { color: TEXT } }],
      { x: x + 0.22, y: y + 0.12, w: 5.6, h: 1.05, fontSize: 12, fontFace: "Segoe UI" });
  });
}

// ---------- 14b. OLTP-EMUL mode ----------
{
  const s = baseSlide({ title: "OLTP-EMUL — a business process, not just SQL",
    kicker: "Borrows the FirebirdSQL oltp-emul model (MIT, Pavel Zotov): a car-service supply business runs inside the database as stored procedures.",
    notes: "Units are executable SPs from the business_ops registry; Go picks them weighted-random and executes one per transaction in READ COMMITTED NO WAIT. Score = successful units per main-phase minute. Invariants: SRV_MAKE_INVNT_SALDO / SRV_MAKE_MONEY_SALDO must hold." });
  const flow = [
    ["Customer order", "stock · creation"],
    ["Supplier order", "stock · creation"],
    ["Supplier invoice", "stock · creation"],
    ["Invoice → stock", "stock · state_next"],
    ["Reserve / sale", "stock · state_next"],
    ["Pay supplier / customer", "payments · creation"],
    ["Every step cancellable", "removal · state_back"],
  ];
  flow.forEach(([name, meta], i) => {
    const x = 0.55 + (i % 4) * 3.12, y = 1.5 + Math.floor(i / 4) * 1.35;
    panel(s, x, y, 2.9, 1.15);
    s.addText(name, { x: x + 0.15, y: y + 0.12, w: 2.6, h: 0.45, fontSize: 12.5, bold: true, color: TEXT, fontFace: "Segoe UI" });
    s.addText(meta, { x: x + 0.15, y: y + 0.6, w: 2.6, h: 0.35, fontSize: 10.5, color: ACCENT, fontFace: "Segoe UI" });
  });
  panel(s, 9.95, 1.5, 2.85, 2.6);
  s.addText("The score", { x: 10.15, y: 1.65, w: 2.45, h: 0.35, fontSize: 14, bold: true, color: ACCENT, fontFace: "Segoe UI" });
  s.addText("successful business actions per minute — weighted random unit mix; warmup grows the database, measurement adds churn.",
    { x: 10.15, y: 2.05, w: 2.5, h: 1.9, fontSize: 11, color: TEXT, fontFace: "Segoe UI" });
  const chips = [
    "Invariants: stock and money must stay conserved — checked live",
    "Deadlocks and business rejections are expected load events",
    "mon$ memory peaks at 4 levels: db / attachments / transactions / statements",
    "Model & SQL: github.com/FirebirdSQL/oltp-emul (MIT, Pavel Zotov)",
  ];
  chips.forEach((c, i) => chip(s, c, 0.55 + (i % 2) * 6.25, 4.45 + Math.floor(i / 2) * 1.15, 6.0));
  s.addText("20 units, 1 transaction each. The database is the application — the generator decides what happens next.",
    { x: 0.57, y: 6.7, w: 12.2, h: 0.4, fontSize: 13, color: MUTED, align: "center", fontFace: "Segoe UI" });
}

// ---------- 14c. lifecycle ----------
{
  const s = baseSlide({ title: "Provision → run → report — one lifecycle",
    kicker: "The OLTPEMUL tab drives the whole flow: provision a benchmark database, run it, read the score.",
    notes: "Provision: create (page size 8192) + vendored DDL/SP scripts + settings + dictionaries + documents, then the schema guard gates every start. Scripts are verbatim upstream oltp-emul (MIT); the splitter is quote-aware with golden tests." });
  const cards = [
    ["1 · Provision", ["fb-loadgen provision \\", "  --dsn \"127.0.0.1/3055:C:\\\\data\\\\olt.fdb\" \\", "  --working-mode SMALL_01 \\", "  --init-docs 3000"], "create DB → DDL → procedures → settings → dictionaries → documents. Async job with progress; the database self-registers in the fleet."],
    ["2 · Run", ["OLTPEMUL tab → Start", "# unit mix editable live", "PUT /api/sessions/$ID/emul/weights"], "Schema guard first. Weighted random units, one transaction each, per-unit outcomes: ok / conflict / rejected / failure."],
    ["3 · Report", ["results_emul.txt", "+ live score, per-unit table,", "  memory peaks, invariants"], "Score (actions/min) frozen into the report dir and run history — compare runs over time."],
  ];
  cards.forEach(([name, lines, cap], i) => {
    const x = 0.55 + i * 4.28;
    panel(s, x, 1.5, 4.05, 4.9);
    s.addText(name, { x: x + 0.2, y: 1.68, w: 3.65, h: 0.4, fontSize: 15, bold: true, color: ACCENT, fontFace: "Segoe UI" });
    codeBox(s, lines, x + 0.2, 2.2, 3.65, i === 1 ? 1.6 : 2.2);
    s.addText(cap, { x: x + 0.2, y: 4.5, w: 3.65, h: 1.7, fontSize: 11, color: MUTED, fontFace: "Segoe UI" });
  });
  chip(s, "works headless too: the same REST API drives provisioning, starting and stopping", 0.55, 6.6, 12.2);
}

// ---------- 14d. dashboard ----------
{
  const s = baseSlide({ title: "The OLTPEMUL dashboard — truth per unit",
    kicker: "Live score, memory peaks, invariant status and the per-unit outcome table — one tab, one database, one click to start.",
    notes: "Screenshot is the live UI. Per-unit table separates ok / conflict / rejected / failure with avg and max latency. History compares scores across runs." });
  card(s, "ui_oltpemul.png", 0.55, 1.45, 12.25, 4.6);
  const chips = ["score sparkline per 10 s interval", "memory peaks: db / att / trn / stmt", "per-unit ok · conflict · rejected · fail + avg/max ms", "last runs: score comparison over time"];
  chips.forEach((c, i) => chip(s, c, 0.55 + i * 3.12, 6.25, 2.95, { bold: true }));
}

// ---------- 15. scenarios ----------
{
  const s = baseSlide({ title: "Typical scenarios",
    notes: "All three run against the same control plane; recipes adapted from API.md." });
  const cards = [
    ["Nightly soak", ["curl -X POST $BASE/api/schedules -d '{", "  \"trigger\": {\"type\":\"cron\",", "   \"cron\":\"0 2 * * *\"},", "  \"run\": {\"timeLimitMin\":30},", "  \"notifyUrl\":\"https://hooks/…\"}'"]],
    ["CI spike test", ["fb-loadgen --ui --api-only &", "curl -X POST $BASE/api/schedules/", "  $SID/trigger        # fire now", "curl $BASE/api/runs/$RUN   # outcome", "# fail the build on outcome != ok"]],
    ["Fleet health", ["curl $BASE/api/fleet", "curl $BASE/metrics | grep fbloadgen", "", "# running, budget, TPS, errors,", "# next scheduled fire — in Prometheus"]],
  ];
  cards.forEach(([name, lines], i) => {
    const x = 0.55 + i * 4.28;
    panel(s, x, 1.5, 4.05, 4.6);
    s.addText(name, { x: x + 0.2, y: 1.68, w: 3.65, h: 0.4, fontSize: 15, bold: true, color: ACCENT, fontFace: "Segoe UI" });
    codeBox(s, lines, x + 0.2, 2.2, 3.65, 3.0);
    const caps = ["Every night at 02:00, 30 minutes, webhook tells you the outcome",
                  "Headless process in CI; build fails if the run fails",
                  "One scrape shows the whole fleet's load state"];
    s.addText(caps[i], { x: x + 0.2, y: 5.3, w: 3.65, h: 0.7, fontSize: 10.5, color: MUTED, fontFace: "Segoe UI" });
  });
  s.addText("Same API drives all three — the UI, CI and cron are interchangeable clients.",
    { x: 0.57, y: 6.45, w: 12.2, h: 0.4, fontSize: 13, color: MUTED, align: "center", fontFace: "Segoe UI" });
}

// ---------- 16. roadmap ----------
{
  const s = baseSlide({ title: "Where we are, where we go",
    notes: "Done items shipped with schedules/history; roadmap from IMPROVEMENTS.md." });
  panel(s, 0.55, 1.5, 6.0, 4.7);
  s.addText("Shipped", { x: 0.8, y: 1.7, w: 5.5, h: 0.4, fontSize: 15, bold: true, color: OK, fontFace: "Segoe UI" });
  s.addText([
    "Scheduled runs (once / interval / cron)\n",
    "Run history with per-session outcomes\n",
    "Webhooks + Prometheus /metrics\n",
    "API hardening: auth, CORS, headless mode\n",
    "OpenAPI 3.1 spec for the whole API\n",
    "oltp-emul business-process mode + OLTPEMUL dashboard\n",
    "CI (vet + race + 3 OS) and multi-platform releases\n",
    "IBSurgeon driver fork under the hood",
  ].join(""), { x: 0.8, y: 2.2, w: 5.5, h: 3.6, fontSize: 13.5, color: TEXT, fontFace: "Segoe UI", lineSpacingMultiple: 1.4 });
  panel(s, 6.8, 1.5, 6.0, 4.7);
  s.addText("Next", { x: 7.05, y: 1.7, w: 5.5, h: 0.4, fontSize: 15, bold: true, color: ACCENT, fontFace: "Segoe UI" });
  s.addText([
    "Secure credential storage (no plaintext settings)\n",
    "Schedule chains (run A, then B)\n",
    "Your use case here — tell us what you need",
  ].join(""), { x: 7.05, y: 2.2, w: 5.5, h: 3.6, fontSize: 13.5, color: TEXT, fontFace: "Segoe UI", lineSpacingMultiple: 1.4 });
  s.addText([
    { text: "Docs: ", options: { bold: true, color: TEXT } },
    { text: "README.md · API.md · IMPROVEMENTS.md", options: { color: ACCENT } },
    { text: "      Start:  ", options: { bold: true, color: TEXT } },
    { text: "fb-loadgen --ui", options: { fontFace: "Consolas", color: ACCENT } },
  ], { x: 0.57, y: 6.5, w: 12.2, h: 0.4, fontSize: 13, align: "center", fontFace: "Segoe UI" });
}

// ---------- appendix: endpoint & flag reference ----------
{
  const s = baseSlide({ title: "Appendix — quick reference",
    notes: "Full tables live in API.md and README.md." });
  codeBox(s, [
    "# run",
    "fb-loadgen --profile write-heavy --dsn \"host/3050:path\\db.fdb\"",
    "fb-loadgen --ui [--api-only] [--ui-token T] [--ui-addr 127.0.0.1:9000]",
    "",
    "# control",
    "GET  /api/sessions            GET  /api/fleet",
    "POST /api/sessions/{id}/start      {\"timeLimitMin\": 15, \"wait\": false}",
    "POST /api/sessions/{id}/stop | pause | resume | validate",
    "POST /api/sessions/start-all | stop-all | pause-all",
    "",
    "# schedule + history",
    "GET|POST /api/schedules       PATCH|DELETE /api/schedules/{id}",
    "POST /api/schedules/{id}/trigger",
    "GET  /api/runs | /api/runs/{id} | POST /api/runs/{id}/cancel",
    "GET  /api/sessions/{id}/runs | /report | /report/{file}",
    "GET  /api/health | /api/version | /metrics",
  ], 0.55, 1.45, 12.25, 5.4);
}

pres.writeFile({ fileName: join(here, "IBSurgeon_LoadGenerator.pptx") })
  .then(() => console.log("OK: IBSurgeon_LoadGenerator.pptx, slides:", pageNo + 2));
