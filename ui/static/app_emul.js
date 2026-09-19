// ---------- OLTPEMUL tab ----------
// Extends the global 1.5 s refresh (same monkey-patch pattern as
// app_schedules.js). Renders only while the tab is active.

const emulState = {
  sessionId: null,
  provJobId: null,
  units: [],
  lastRuns: 0,
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

async function emulJson(method, path, body) {
  const headers = { "Content-Type": "application/json" };
  if (authToken) headers["Authorization"] = "Bearer " + authToken;
  const res = await fetch(path, { method, headers, body: body ? JSON.stringify(body) : undefined });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || res.statusText);
  return data;
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
    opt.textContent = `${s.relPath} (${s.profile})`;
    sel.appendChild(opt);
  }
  if (prev && data.sessions.some((s) => s.id === prev)) sel.value = prev;
  if (!sel.value && data.sessions.length) sel.value = data.sessions[0].id;
  if (sel.value !== emulState.sessionId) {
    emulState.sessionId = sel.value;
    await emulLoadSession();
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
    document.getElementById("emulProvStatus").textContent = "error: " + e.message;
  }
}

async function emulPollProvision() {
  if (!emulState.provJobId) return;
  try {
    const d = await emulJson("GET", "/api/emul/provision/" + emulState.provJobId);
    const el = document.getElementById("emulProvStatus");
    el.textContent = `${d.stage} ${(d.progress * 100).toFixed(0)}% — ${d.message || ""}`;
    if (d.done) {
      el.textContent = d.ok ? "done — session added to fleet" : "failed: " + d.message;
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
    document.getElementById("emulInv").value = s.emulInvariantEvery || 60;
    document.getElementById("emulMon").value = s.emulMonitorEvery || 10;
    await emulLoadUnits();
  } catch {
    /* session may be transiently unavailable */
  }
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
      const input = `<input type="number" min="0" value="${u.weight}" data-unit="${u.unit}" style="width:80px" />`;
      tr.innerHTML = `<td>${u.unit}</td><td>${u.kind}</td><td>${input}</td>`;
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
    document.getElementById("emulWeightsMsg").textContent = "saved";
  } catch (e) {
    document.getElementById("emulWeightsMsg").textContent = "error: " + e.message;
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
      emulInvariantEvery: Number(document.getElementById("emulInv").value) || 60,
      emulMonitorEvery: Number(document.getElementById("emulMon").value) || 10,
    });
    document.getElementById("emulConnMax").value = s.connMax + " (budget)";
    document.getElementById("emulApplyMsg").textContent = "applied";
  } catch (e) {
    document.getElementById("emulApplyMsg").textContent = "error: " + e.message;
  }
}

async function emulControl(action) {
  const id = emulSel();
  if (!id) return;
  try {
    await emulJson("POST", `/api/sessions/${id}/${action}`, {});
  } catch (e) {
    document.getElementById("emulApplyMsg").textContent = "error: " + e.message;
  }
}

// ---------- live results ----------

function emulRenderState(d, sess) {
  document.getElementById("emulLivePhase").textContent = d.phase || "";
  document.getElementById("emulScore").textContent = Math.round(d.scorePerMin);
  document.getElementById("emulOk").textContent = d.okUnits;
  document.getElementById("emulTotal").textContent = d.totalUnits;
  document.getElementById("emulMemDb").textContent = Math.round(d.memPeaks.dbBytes / (1 << 20));
  const inv = document.getElementById("emulInv");
  inv.textContent = d.invariant || "—";
  inv.style.color = d.invariant === "ok" ? "green" : d.invariant && d.invariant.startsWith("failed") ? "red" : "inherit";

  emulSparkline("emulScoreChart", d.series || [], (p) => p.scorePerMin);

  const body = document.getElementById("emulPerUnitBody");
  body.innerHTML = "";
  for (const u of d.perUnit || []) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${u.unit}</td><td>${u.kind}</td><td>${u.ok}</td><td>${u.conflict}</td><td>${u.rejected}</td><td>${u.failure}</td><td>${u.avgMs}</td><td>${u.maxMs}</td>`;
    body.appendChild(tr);
  }

  // final report panel: only meaningful once the session reached a terminal state
  const fin = document.getElementById("emulFinal");
  if (sess && (sess.status === "Completed" || sess.status === "Failed") && sess.emul) {
    const e = sess.emul;
    const okPct = e.totalUnits ? ((100 * e.okUnits) / e.totalUnits).toFixed(1) : "0";
    fin.innerHTML =
      `Score <b>${Math.round(e.scorePerMin)}</b> ops/min · ${e.okUnits}/${e.totalUnits} units OK (${okPct}%) · ` +
      `peaks db=${Math.round(e.memPeaks.dbBytes / (1 << 20))}MB att=${Math.round(e.memPeaks.attBytes / (1 << 20))}MB · ` +
      `invariants ${e.invariant || "—"} · working mode ${e.workingMode || "?"}` +
      (sess.reportDir ? ` · reports: <a href="/api/sessions/${sess.id}/report">${sess.reportDir}</a>` : "");
  } else if (sess && sess.status === "Running") {
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
    if (state) emulRenderState(state, sess);
    else emulRenderState({}, sess);
    await emulLoadRuns();
  } catch { /* transient */ }
};

// ---------- event wiring ----------

document.getElementById("emulDb").addEventListener("change", () => emulLoadSession().catch(() => {}));
document.getElementById("btnEmulRefresh").addEventListener("click", async () => {
  await emulRefreshDbList().catch(() => {});
  await emulLoadProfiles().catch(() => {});
  await emulLoadSession().catch(() => {});
});
document.getElementById("btnEmulProvision").addEventListener("click", () => emulProvision());
document.getElementById("btnEmulProvCancel").addEventListener("click", () => {
  if (emulState.provJobId) emulJson("DELETE", "/api/emul/provision/" + emulState.provJobId).catch(() => {});
});
document.getElementById("btnEmulWeights").addEventListener("click", () => emulSaveWeights());
document.getElementById("btnEmulApply").addEventListener("click", () => emulApplySettings());
document.getElementById("btnEmulStart").addEventListener("click", () => emulControl("start"));
document.getElementById("btnEmulStop").addEventListener("click", () => emulControl("stop"));

emulLoadProfiles().catch(() => {});
