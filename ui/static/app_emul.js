// ---------- OLTPEMUL tab ----------
// Extends the global 1.5 s refresh (same monkey-patch pattern as
// app_schedules.js). Renders only while the tab is active.

const emulState = {
  sessionId: null,
  provJobId: null,
  units: [],
  lastRuns: 0,
  gateOk: null,
  livePhase: null,
};

const emulSel = () => document.getElementById("emulDb").value || null;

// ---------- helpers ----------

function emulSparkline(svgId, points, valueOf) {
  const svg = document.getElementById(svgId);
  if (!svg) return;
  svg.innerHTML = "";
  if (!points || points.length < 2) return;
  const W = 600, H = 120, pad = 4;
  const vals = points.map(valueOf);
  const max = Math.max(...vals, 1);
  const step = (W - 2 * pad) / (points.length - 1);
  const pts = vals
    .map((v, i) => `${(pad + i * step).toFixed(1)},${(H - pad - (v / max) * (H - 2 * pad)).toFixed(1)}`)
    .join(" ");
  const poly = document.createElementNS("http://www.w3.org/2000/svg", "polyline");
  poly.setAttribute("points", pts);
  poly.setAttribute("fill", "none");
  poly.setAttribute("stroke", "#2563eb");
  poly.setAttribute("stroke-width", "2");
  svg.appendChild(poly);
  // max label
  const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
  label.setAttribute("x", pad + 2);
  label.setAttribute("y", pad + 12);
  label.setAttribute("font-size", "11");
  label.setAttribute("fill", "#888");
  label.textContent = "max " + Math.round(max);
  svg.appendChild(label);
}

// thin wrapper over the global api() helper (app.js) — stringifies bodies
async function emulJson(method, path, body) {
  return api(path, { method, body: body === undefined ? undefined : JSON.stringify(body) });
}

// ---------- database dropdown + provision ----------

async function emulRefreshDbList() {
  const data = await api("/api/sessions");
  const sel = document.getElementById("emulDb");
  const prev = sel.value;
  sel.innerHTML = "";
  for (const s of data.sessions || []) {
    const opt = document.createElement("option");
    opt.value = s.id;
    const emulReady = s.profile === "oltp-emul";
    opt.textContent = `${s.relPath}${emulReady ? " ✓ oltp-emul" : ""}`;
    sel.appendChild(opt);
  }
  // Default to an oltp-emul session; never silently fall back to a
  // non-oltpemul database (Start would fail its schema guard).
  if (!prev || !data.sessions.some((s) => s.id === prev)) {
    const ready = (data.sessions || []).find((s) => s.profile === "oltp-emul");
    if (ready) sel.value = ready.id;
  }
  if (prev && data.sessions.some((s) => s.id === prev)) sel.value = prev;
  if (!sel.value && data.sessions.length) sel.value = data.sessions[0].id;
  if (sel.value !== emulState.sessionId) {
    emulState.sessionId = sel.value;
    await emulLoadSession();
    await emulGate();
  }
}

async function emulLoadProfiles() {
  const sel = document.getElementById("emulProvMode");
  try {
    const d = await emulJson("GET", "/api/emul/profiles" + (emulSel() ? "?session=" + emulSel() : ""));
    sel.innerHTML = "";
    for (const p of d.profiles || []) {
      const opt = document.createElement("option");
      opt.value = p;
      opt.textContent = p;
      sel.appendChild(opt);
    }
    sel.value = "SMALL_01";
  } catch {
    /* dropdown keeps previous content */
  }
}

async function emulProvision() {
  try {
    const d = await emulJson("POST", "/api/emul/provision", {
      dsn: document.getElementById("emulProvDsn").value.trim(),
      user: document.getElementById("emulProvUser").value.trim(),
      pass: document.getElementById("emulProvPass").value,
      workingMode: document.getElementById("emulProvMode").value,
      initDocs: Number(document.getElementById("emulProvDocs").value) || 0,
      pageSize: Number(document.getElementById("emulProvPage").value) || 8192,
    });
    emulState.provJobId = d.jobId;
  } catch (e) {
    emulSetStatus("Provision failed to start: " + e.message, "err");
  }
}

async function emulPollProvision() {
  if (!emulState.provJobId) return;
  try {
    const d = await emulJson("GET", "/api/emul/provision/" + emulState.provJobId);
    emulSetStatus(`Provisioning: ${d.stage} ${(d.progress * 100).toFixed(0)}% — ${d.message || ""}`, "info");
    if (d.done) {
      emulSetStatus(d.ok ? "Provision complete — added to fleet" : "Provision failed: " + d.message,
                    d.ok ? "ok" : "err");
      emulState.provJobId = null;
      await emulRefreshDbList();
    }
  } catch {
    emulState.provJobId = null;
  }
}

// ---------- session settings, weights, controls ----------

async function emulLoadSession() {
  const id = emulSel();
  if (!id) return;
  try {
    const s = await emulJson("GET", "/api/sessions/" + id);
    document.getElementById("emulConnMin").value = s.connMin;
    document.getElementById("emulConnMax").value = s.connMax + " (budget)";
    document.getElementById("emulWarmup").value = s.warmup;
    document.getElementById("emulMain").value = s.main;
    document.getElementById("emulCooldown").value = s.cooldown;
    document.getElementById("emulThink").value = s.thinkMs;
    document.getElementById("emulInvEvery").value = s.emulInvariantEvery || 60;
    document.getElementById("emulMonEvery").value = s.emulMonitorEvery || 10;
    emulLoadExtended(s);
    // time limit: shared preference with the Sessions-tab row dropdown
    document.getElementById("emulTimeLimit").value =
      String(rowLimits[id] !== undefined ? rowLimits[id] : 15);
    await emulLoadUnits();
  } catch {
    /* session may be transiently unavailable */
  }
}

// emulLoadExtended populates the Extended load panel from the session echo.
function emulLoadExtended(s) {
  const el = (s && s.extendedLoad) || {};
  document.getElementById("extLdEnabled").checked = el.enabled !== false;
  const hv = el.heavySelect || {};
  document.getElementById("extLdHeavyEvery").value = hv.everySec !== undefined ? hv.everySec : 5;
  const bd = el.bulkDml || {};
  document.getElementById("extLdBulkEvery").value = bd.everySec !== undefined ? bd.everySec : 30;
  document.getElementById("extLdBulkMin").value = bd.minRows || 100;
  document.getElementById("extLdBulkMax").value = bd.maxRows || 1000;
  const tv = el.txVariants || {};
  document.getElementById("extLdTxMode").value = tv.mode || "emul-safe";
  const ol = el.opsLog || {};
  document.getElementById("extLdOpsLevel").value = ol.level || "all";
  document.getElementById("extLdOpsFormat").value = ol.format || "repl-print";
  const pd = el.plusDDL || {};
  document.getElementById("extLdPlusDDL").checked = !!pd.enabled;
  document.getElementById("extLdDDLEvery").value = pd.everySec || 60;
}

// emulExtendedPayload reads the Extended load panel into a patch object.
function emulExtendedPayload() {
  const minRows = Number(document.getElementById("extLdBulkMin").value) || 100;
  const maxRows = Number(document.getElementById("extLdBulkMax").value) || 1000;
  return {
    enabled: document.getElementById("extLdEnabled").checked,
    heavySelect: { everySec: Number(document.getElementById("extLdHeavyEvery").value) || 0, minJoins: 3 },
    bulkDml: {
      everySec: Number(document.getElementById("extLdBulkEvery").value) || 0,
      minRows: minRows,
      maxRows: Math.max(minRows, maxRows),
    },
    opsLog: {
      enabled: true,
      level: document.getElementById("extLdOpsLevel").value,
      format: document.getElementById("extLdOpsFormat").value,
      maxSizeMB: 50,
      keepArchives: 3,
      rotateOnStart: true,
    },
    txVariants: {
      mode: document.getElementById("extLdTxMode").value,
      lockTimeoutChoicesSec: [1, 3, 5, 10],
      completion: { commit: 55, rollback: 15, commitRetaining: 10, rollbackRetaining: 5, twoPhase: 5, limbo: 2, connDrop: 2 },
      rareCompletionMinGapSec: 10,
      retainingChainMax: 50,
      savepointProb: 0.15,
      autonomousCallProb: 0.10,
      ddlRollbackFrac: 0.20,
    },
    plusDDL: {
      enabled: document.getElementById("extLdPlusDDL").checked,
      everySec: Number(document.getElementById("extLdDDLEvery").value) || 60,
      colPrefix: "TST_",
      testInsertRows: 100,
      testUpdateRows: 50,
      testDeleteRows: 30,
      workTables: ["WARES", "AGENTS", "DOC_STATES"],
    },
  };
}

async function emulLoadUnits() {
  const id = emulSel();
  if (!id) return;
  const body = document.getElementById("emulUnitsBody");
  body.innerHTML = "";
  try {
    const d = await emulJson("GET", "/api/sessions/" + id + "/emul/units");
    emulState.units = d.units || [];
    for (const u of emulState.units) {
      const tr = document.createElement("tr");
      const input = `<input type="number" min="0" value="${u.weight}" data-unit="${u.name}" style="width:80px" />`;
      tr.innerHTML = `<td>${u.name}</td><td>${u.kind}</td><td>${input}</td>`;
      body.appendChild(tr);
    }
  } catch (e) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="3" class="muted">${e.message}</td>`;
    body.appendChild(tr);
    emulState.units = [];
  }
}

async function emulSaveWeights() {
  const id = emulSel();
  if (!id) return;
  const weights = {};
  document.querySelectorAll("#emulUnitsBody input[data-unit]").forEach((inp) => {
    weights[inp.dataset.unit] = Number(inp.value) || 0;
  });
  try {
    await emulJson("PUT", "/api/sessions/" + id + "/emul/weights", { weights });
    emulSetStatus("Unit weights saved", "ok");
  } catch (e) {
    emulSetStatus("Save weights failed: " + e.message, "err");
  }
}

async function emulApplySettings() {
  const id = emulSel();
  if (!id) return;
  try {
    const s = await emulJson("PATCH", "/api/sessions/" + id, {
      profile: "oltp-emul",
      connMin: Number(document.getElementById("emulConnMin").value) || 2,
      warmup: Number(document.getElementById("emulWarmup").value) || 30,
      main: Number(document.getElementById("emulMain").value) || 300,
      cooldown: Number(document.getElementById("emulCooldown").value) || 20,
      thinkMs: Number(document.getElementById("emulThink").value) || 50,
      emulInvariantEvery: Number(document.getElementById("emulInvEvery").value) || 60,
      emulMonitorEvery: Number(document.getElementById("emulMonEvery").value) || 10,
      extendedLoad: emulExtendedPayload(),
    });
    document.getElementById("emulConnMax").value = s.connMax + " (budget)";
    emulSetStatus("Settings applied", "ok");
  } catch (e) {
    emulSetStatus("Apply settings failed: " + e.message, "err");
  }
}

function emulSetStatus(text, kind) {
  const el = document.getElementById("emulCtlStatus");
  const color = kind === "err" ? "red" : kind === "ok" ? "green" : kind === "info" ? "#2563eb" : "inherit";
  el.innerHTML = text ? '<span style="color:' + color + '">' + text + "</span>" : "";
}

async function emulControl(action) {
  const id = emulSel();
  if (!id) return;
  try {
    await emulJson("POST", `/api/sessions/${id}/${action}`, {});
    emulSetStatus("oltp-emul: " + action + " accepted", "info");
    toast("oltp-emul: " + action + " ok", false);
  } catch (e) {
    emulSetStatus("oltp-emul " + action + " failed: " + e.message, "err");
    toast("oltp-emul " + action + " failed: " + e.message, true);
  }
}

// emulLifecycle renders the current lifecycle stage in the left status
// line and enables/disables the control buttons accordingly.
function emulLifecycle(sess, gateOk) {
  const st = sess ? sess.status : null;
  const startBtn = document.getElementById("btnEmulStart");
  const stopBtn = document.getElementById("btnEmulStop");
  const pauseBtn = document.getElementById("btnEmulPause");
  const resumeBtn = document.getElementById("btnEmulResume");
  const applyBtn = document.getElementById("btnEmulApplySettings");

  startBtn.disabled = !gateOk || !(st === "Idle" || st === "Failed" || st === "Completed");
  stopBtn.disabled = !(st === "Running" || st === "Paused" || st === "Starting");
  pauseBtn.disabled = st !== "Running";
  resumeBtn.disabled = st !== "Paused";
  applyBtn.disabled = !(st === "Idle" || st === "Failed" || st === "Completed");

  if (emulState.provJobId) {
    emulSetStatus("Provisioning in progress…", "info");
    return;
  }
  if (!gateOk) {
    emulSetStatus("Not an oltpemul database — provision it below, then start", "err");
    return;
  }
  switch (st) {
    case "Running":
    case "Starting":
      emulSetStatus("Running — " + (emulState.livePhase || st) + emulRemainingSuffix(sess), "info");
      break;
    case "Paused":
      emulSetStatus("Paused" + emulRemainingSuffix(sess), "info");
      break;
    case "Completed":
      emulSetStatus("Completed — final report below", "ok");
      break;
    case "Failed":
      emulSetStatus("Failed: " + (sess && sess.lastError ? sess.lastError : "unknown error"), "err");
      break;
    default:
      emulSetStatus("Ready — set settings and press Start", "");
  }
}

// emulRemainingSuffix renders the countdown for time-limited runs.
function emulRemainingSuffix(sess) {
  if (!sess || !sess.timeLimitMin) return "";
  const sec = Math.max(0, Math.round(sess.remainingSec || 0));
  const mm = Math.floor(sec / 60), ss = sec % 60;
  return " · " + mm + ":" + String(ss).padStart(2, "0") + " remaining";
}

// emulGate checks the selected database has the oltpemul schema; the
// result feeds the lifecycle status and Start button state.
async function emulGate() {
  const id = emulSel();
  if (!id) return true;
  try {
    await emulJson("GET", "/api/sessions/" + id + "/emul/units");
    emulState.gateOk = true;
    return true;
  } catch (e) {
    emulState.gateOk = false;
    return false;
  }
}

// ---------- live results ----------

function emulRenderState(d, sess) {
  document.getElementById("emulLivePhase").textContent = d.phase || "";
  document.getElementById("emulScore").textContent = Math.round(d.scorePerMin).toLocaleString();
  document.getElementById("emulOk").textContent = d.okUnits;
  document.getElementById("emulTotal").textContent = d.totalUnits;
  document.getElementById("emulMemDb").textContent = Math.round(d.memPeaks.dbBytes / (1 << 20));
  const inv = document.getElementById("emulInvState");
  inv.textContent = d.invariant || "—";
  inv.style.color = d.invariant === "ok" ? "green"
    : d.invariant && (d.invariant.startsWith("failed") || d.invariant.startsWith("disabled")) ? "red"
    : "inherit";

  emulSparkline("emulScoreChart", d.series || [], (p) => p.scorePerMin);

  const body = document.getElementById("emulPerUnitBody");
  body.innerHTML = "";
  const unitRows = d.perUnit || [];
  if (!unitRows.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = '<td colspan="8" class="muted">no units executed yet</td>';
    body.appendChild(tr);
    return;
  }
  for (const u of unitRows) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${u.unit}</td><td>${u.kind}</td><td>${u.ok}</td><td>${u.conflict}</td><td>${u.rejected}</td><td>${u.failure}</td><td>${u.avgMs}</td><td>${u.maxMs}</td>`;
    body.appendChild(tr);
  }

  // final report panel: whenever a finished (or manually stopped) run has
  // frozen emul data — the data is valid regardless of how the run ended
  const fin = document.getElementById("emulFinal");
  const running = sess && (sess.status === "Running" || sess.status === "Starting" || sess.status === "Paused");
  if (sess && sess.emul && !running) {
    const e = sess.emul;
    const okPct = e.totalUnits ? ((100 * e.okUnits) / e.totalUnits).toFixed(1) : "0";
    fin.innerHTML =
      `Score <b>${Math.round(e.scorePerMin)}</b> ops/min · ${e.okUnits}/${e.totalUnits} units OK (${okPct}%) · ` +
      `peaks db=${Math.round(e.memPeaks.dbBytes / (1 << 20))}MB att=${Math.round(e.memPeaks.attBytes / (1 << 20))}MB · ` +
      `invariants ${e.invariant || "—"} · working mode ${e.workingMode || "?"}` +
      (sess.reportDir ? ` · reports: <a href="/api/sessions/${sess.id}/report">${sess.reportDir}</a>` : "");
  } else if (running) {
    fin.textContent = "— run in progress —";
  }
}

async function emulLoadRuns() {
  // throttle: once per ~10 s
  if (Date.now() - emulState.lastRuns < 10000) return;
  emulState.lastRuns = Date.now();
  try {
    const d = await emulJson("GET", "/api/runs?limit=100");
    const body = document.getElementById("emulRunsBody");
    body.innerHTML = "";
    for (const run of d.runs || []) {
      for (const sr of run.sessions || []) {
        if (!sr.emul) continue;
        const tr = document.createElement("tr");
        tr.innerHTML = `<td>${(run.startedAt || "").replace("T", " ").slice(0, 19)}</td>` +
          `<td>${sr.status}</td>` +
          `<td>${Math.round(sr.emul.scorePerMin)}</td>` +
          `<td>${sr.emul.okUnits}/${sr.emul.totalUnits}</td>` +
          `<td>${Math.round((sr.emul.memPeaks ? sr.emul.memPeaks.dbBytes : 0) / (1 << 20))} MB</td>`;
        body.appendChild(tr);
      }
    }
    if (!body.children.length) {
      const tr = document.createElement("tr");
      tr.innerHTML = '<td colspan="5" class="muted">no emul runs recorded yet</td>';
      body.appendChild(tr);
    }
  } catch {
    /* history endpoint may be busy */
  }
}

// ---------- refresh hook ----------

const _emulBaseRefresh = refresh;
refresh = async function () {
  await _emulBaseRefresh();
  if (activeTab !== "oltpemul") return;
  try {
    await emulRefreshDbList();
  } catch { /* fleet poll already reported */ }
  try {
    await emulPollProvision();
  } catch { /* job poll transient */ }
  const id = emulSel();
  if (!id) return;
  try {
    const [sess, state] = await Promise.all([
      emulJson("GET", "/api/sessions/" + id),
      emulJson("GET", "/api/sessions/" + id + "/emul/state").catch(() => null),
    ]);
    if (state) {
      emulState.livePhase = state.phase || null;
      emulRenderState(state, sess);
    } else {
      emulRenderState({}, sess);
    }
    // re-gate only where readiness can change (selection or entering a
    // state that requires a fresh verdict) — not on every poll
    if (emulState.gateSessionId !== id ||
        ((sess.status === "Idle" || sess.status === "Failed") && emulState.gateStatus !== sess.status)) {
      emulState.gateOk = await emulGate();
      emulState.gateSessionId = id;
      emulState.gateStatus = sess.status;
    }
    emulLifecycle(sess, emulState.gateOk);
    await emulLoadRuns();
  } catch { /* transient */ }
};

// ---------- event wiring ----------

document.getElementById("emulDb").addEventListener("change", () => emulLoadSession().catch(() => {}));
document.getElementById("btnEmulRefresh").addEventListener("click", async () => {
  await emulRefreshDbList().catch(() => {});
  await emulLoadProfiles().catch(() => {});
  await emulLoadSession().catch(() => {});
});document.getElementById("btnEmulProvision").addEventListener("click", () => emulProvision());
document.getElementById("btnEmulProvCancel").addEventListener("click", () => {
  if (emulState.provJobId) emulJson("DELETE", "/api/emul/provision/" + emulState.provJobId).catch(() => {});
});
document.getElementById("btnEmulWeights").addEventListener("click", () => emulSaveWeights());
document.getElementById("btnEmulApplySettings").addEventListener("click", () => emulApplySettings());
document.getElementById("emulTimeLimit").addEventListener("change", () => {
  // share the Sessions-tab per-database preference so both controls agree
  const id = emulSel();
  if (!id) return;
  rowLimits[id] = emulCurrentTimeLimit();
  try { localStorage.setItem("fb-time-limits-v2", JSON.stringify(rowLimits)); } catch { /* private mode */ }
});
document.getElementById("btnEmulStart").addEventListener("click", async () => {
  // complete lifecycle: apply the form settings, then start with the
  // selected time limit (0 = No limit)
  await emulApplySettings();
  const id = emulSel();
  if (!id) return;
  try {
    await emulJson("POST", `/api/sessions/${id}/start`, { timeLimitMin: emulCurrentTimeLimit() });
  } catch (e) {
    emulSetStatus("oltp-emul start failed: " + e.message, "err");
    toast("oltp-emul start failed: " + e.message, true);
  }
});
document.getElementById("btnEmulPause").addEventListener("click", () => emulControl("pause"));
document.getElementById("btnEmulResume").addEventListener("click", () => emulControl("resume"));
document.getElementById("btnEmulStop").addEventListener("click", () => emulControl("stop"));

emulInitTimeLimit();
emulLoadProfiles().catch(() => {});
