const $ = (id) => document.getElementById(id);
const toastEl = $("toast");
let sessionsById = {};
let selectedId = null;
let authToken = "";

function toast(msg, isErr) {
  toastEl.textContent = msg;
  toastEl.className = "show" + (isErr ? " err" : "");
  clearTimeout(toastEl._t);
  toastEl._t = setTimeout(() => (toastEl.className = ""), 4500);
}

async function api(path, opts = {}) {
  const headers = { "Content-Type": "application/json", ...(opts.headers || {}) };
  if (authToken) headers.Authorization = "Bearer " + authToken;
  const res = await fetch(path, { ...opts, headers });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || res.statusText);
  return data;
}

function editable(status) {
  return status === "Idle" || status === "Failed" || status === "Completed";
}

function escapeHtml(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}
function escapeAttr(str) {
  return escapeHtml(str).replace(/'/g, "&#39;");
}

function profileSelect(selected) {
  return ["write-heavy", "read-heavy", "spike"]
    .map((p) => `<option value="${p}" ${p === selected ? "selected" : ""}>${p}</option>`)
    .join("");
}

function actionsHtml(s) {
  const buttons = [];
  if (editable(s.status)) {
    buttons.push(`<button data-act="edit">Edit</button>`);
    if (!s.missing) buttons.push(`<button class="primary" data-act="start">Start</button>`);
  }
  if (s.status === "Running") {
    buttons.push(`<button data-act="pause">Pause</button>`);
    buttons.push(`<button class="danger" data-act="stop">Stop</button>`);
  }
  if (s.status === "Paused") {
    buttons.push(`<button class="primary" data-act="resume">Resume</button>`);
    buttons.push(`<button class="danger" data-act="stop">Stop</button>`);
  }
  if (s.status === "Starting" || s.status === "Stopping") {
    buttons.push(`<button disabled>${s.status}…</button>`);
  }
  buttons.push(`<button class="danger" data-act="remove">Remove</button>`);
  return buttons.join("");
}

function matchesFilter(s) {
  const st = $("filterStatus").value;
  const pathQ = ($("filterPath").value || "").toLowerCase();
  if (st === "Missing" && !s.missing) return false;
  if (st && st !== "Missing" && s.status !== st) return false;
  if (pathQ) {
    const hay = ((s.relPath || "") + " " + (s.absPath || "")).toLowerCase();
    if (!hay.includes(pathQ)) return false;
  }
  return true;
}

function ensureRow(s) {
  let tr = document.querySelector(`#rows tr[data-id="${s.id}"]`);
  if (!tr) {
    tr = document.createElement("tr");
    tr.dataset.id = s.id;
    tr.innerHTML = `
      <td class="sticky-l relpath live-rel"></td>
      <td class="live-status"></td>
      <td class="live-profile"></td>
      <td class="live-conns"></td>
      <td class="live-tps"></td>
      <td class="live-err"></td>
      <td class="live-phase"></td>
      <td class="sticky-r actions live-actions"></td>`;
    $("rows").appendChild(tr);
  }
  return tr;
}

function rowIsDirtyOrFocused(tr) {
  if (tr.dataset.dirty === "1") return true;
  const ae = document.activeElement;
  return ae && tr.contains(ae) && (ae.matches("input,select,textarea"));
}

function patchRow(tr, s) {
  tr.classList.toggle("missing", !!s.missing);
  tr.classList.toggle("selected", s.id === selectedId);
  tr.title = s.absPath || "";

  const rel = tr.querySelector(".live-rel");
  rel.innerHTML = `${escapeHtml(s.relPath || s.name)}${s.missing ? " (missing)" : ""}<small>${escapeHtml(s.name || "")}</small>`;

  tr.querySelector(".live-status").innerHTML =
    `<span class="status status-${escapeAttr(s.status)}">${escapeHtml(s.status)}</span>`;
  tr.querySelector(".live-conns").textContent =
    `${s.currentConns || 0}${s.targetConns ? " / " + s.targetConns : ""}`;
  tr.querySelector(".live-tps").textContent = (s.tps || 0).toFixed(1);
  tr.querySelector(".live-err").textContent = String(s.errors || 0);
  tr.querySelector(".live-phase").textContent = s.phase || "—";
  tr.querySelector(".live-actions").innerHTML = actionsHtml(s);

  const profileCell = tr.querySelector(".live-profile");
  if (!rowIsDirtyOrFocused(tr)) {
    if (editable(s.status)) {
      const cur = profileCell.querySelector("select");
      if (!cur) {
        profileCell.innerHTML = `<select data-field="profile">${profileSelect(s.profile)}</select>`;
      } else if (cur.value !== s.profile && document.activeElement !== cur) {
        cur.innerHTML = profileSelect(s.profile);
      }
    } else {
      profileCell.textContent = s.profile;
    }
  }
}

function renderFleet(fleet) {
  if (!fleet) return;
  $("sumRunning").textContent = fleet.running || 0;
  $("sumIdle").textContent = fleet.idle || 0;
  $("sumFailed").textContent = fleet.failed || 0;
  $("sumCompleted").textContent = fleet.completed || 0;
  $("sumConns").textContent = fleet.totalConns || 0;
  $("sumTps").textContent = (fleet.totalTps || 0).toFixed(1);
  $("sumBudget").textContent = `${fleet.budgetUsed || 0}/${fleet.budgetLimit || 0}`;
  const per = fleet.perDbMax || 0;
  const dbs = fleet.databases || 0;
  $("sumPerDb").textContent = dbs ? `${per} (${dbs} DBs)` : String(per);
}

function syncTable(sessions) {
  sessionsById = {};
  const keep = new Set();
  const sorted = [...sessions].sort((a, b) => (a.absPath || "").localeCompare(b.absPath || ""));
  for (const s of sorted) {
    sessionsById[s.id] = s;
    if (!matchesFilter(s)) continue;
    keep.add(s.id);
    patchRow(ensureRow(s), s);
  }
  document.querySelectorAll("#rows tr[data-id]").forEach((tr) => {
    if (!keep.has(tr.dataset.id)) tr.remove();
  });
  if (!keep.size && !$("rows").children.length) {
    $("rows").innerHTML = `<tr><td colspan="8" class="meta">No databases — click Discover</td></tr>`;
  }
  if (selectedId && sessionsById[selectedId]) {
    fillDrawer(sessionsById[selectedId]);
  }
}

async function refresh() {
  const data = await api("/api/sessions");
  renderFleet(data.fleet);
  syncTable(data.sessions || []);
}

function fillConnectionForm(cfg) {
  $("fbHost").value = cfg.host || "localhost";
  $("fbPort").value = cfg.port || 3050;
  $("fbUser").value = cfg.user || "SYSDBA";
  $("fbPass").value = "";
  $("fbPass").placeholder = cfg.hasPass ? "blank = keep saved" : "password";
  $("fbDiscoverDir").value = cfg.discoverDir || ".";
  $("fbMaxTotal").value = cfg.maxTotalConns || 200;
  $("mask").value = cfg.discoverMask || "*.fdb";
  $("cfgMeta").textContent = `${cfg.host}:${cfg.port} · ${cfg.user} · max ${cfg.maxTotalConns}`;
  $("settingsPath").textContent = cfg.settingsPath ? `saved → ${cfg.settingsPath}` : "";
  if (cfg.authRequired && !authToken) {
    authToken = sessionStorage.getItem("fb-ui-token") || prompt("UI bearer token required:") || "";
    if (authToken) sessionStorage.setItem("fb-ui-token", authToken);
  }
}

async function loadConfig() {
  const cfg = await api("/api/config");
  fillConnectionForm(cfg);
  return cfg;
}

async function saveConfig() {
  const body = {
    host: $("fbHost").value.trim(),
    port: Number($("fbPort").value),
    user: $("fbUser").value.trim(),
    pass: $("fbPass").value,
    discoverDir: $("fbDiscoverDir").value.trim() || ".",
    discoverMask: $("mask").value.trim() || "*.fdb",
    discoverRecursive: true,
    maxTotalConns: Number($("fbMaxTotal").value) || 200,
  };
  const data = await api("/api/config", { method: "PUT", body: JSON.stringify(body) });
  const cfg = await api("/api/config");
  fillConnectionForm({ ...cfg, settingsPath: data.path });
  if (data.warnRunning) {
    $("runningBanner").classList.add("show");
  }
  toast(`Connection saved to ${data.path}`);
}

async function discover() {
  const body = {
    folder: $("folder").value.trim(),
    mask: $("mask").value.trim() || "*.fdb",
    discoverDir: $("fbDiscoverDir").value.trim() || ".",
    recursive: true,
  };
  const data = await api("/api/discover", { method: "POST", body: JSON.stringify(body) });
  if (data.discoverDir) $("fbDiscoverDir").value = data.discoverDir;
  syncTable(data.sessions || []);
  const fleet = await api("/api/fleet");
  renderFleet(fleet);
  toast(`Discovered ${data.count} database(s) under ${data.discoverDir}`);
}

function openDrawer(id) {
  selectedId = id;
  const s = sessionsById[id];
  if (!s) return;
  $("drawer").classList.add("open");
  fillDrawer(s);
  document.querySelectorAll("#rows tr").forEach((tr) => {
    tr.classList.toggle("selected", tr.dataset.id === id);
  });
}

function fillDrawer(s) {
  $("drawerTitle").textContent = s.relPath || s.name;
  $("dAbsPath").textContent = s.absPath || "";
  $("dDsn").textContent = s.dsn || "";
  const canEdit = editable(s.status);
  ["dWarmup", "dMain", "dCooldown", "dThinkMs", "dTxTimeout", "dSpikeCycles", "dSpikeHold", "dConnMin"].forEach((id) => {
    $(id).disabled = !canEdit;
  });
  $("dWarmup").value = s.warmup;
  $("dMain").value = s.main;
  $("dCooldown").value = s.cooldown;
  $("dThinkMs").value = s.thinkMs ?? 0;
  $("dTxTimeout").value = s.txTimeout ?? 10;
  $("dSpikeCycles").value = s.spikeCycles ?? 0;
  $("dSpikeHold").value = s.spikeHold ?? 0;
  $("dConnMin").value = s.connMin;
  $("dConnMax").value = s.connMax;
  $("dElapsed").textContent = `${(s.elapsedSec || 0).toFixed(1)}s · success ${s.success || 0}`;
  $("dLatency").textContent = `${s.latencyP50 || 0} / ${s.latencyP95 || 0} / ${s.latencyP99 || 0} ms`;
  $("dErrors").textContent = `${s.errors || 0} (${s.expectedErrors || 0} / ${s.unexpectedErrors || 0})`;
  $("dTopOps").textContent = (s.topOps || []).map((o) => `${o.name}:${o.count}`).join(", ") || "—";
  $("dLastError").value = s.lastError || "";
  loadReports(s.id);
}

async function loadReports(id) {
  const el = $("dReports");
  try {
    const data = await api(`/api/sessions/${id}/report`);
    if (!data.files || !data.files.length) {
      el.textContent = "No report files yet";
      return;
    }
    el.innerHTML = data.files
      .map((f) => `<a href="/api/sessions/${id}/report/${encodeURIComponent(f)}" download>${escapeHtml(f)}</a>`)
      .join("<br>");
  } catch {
    el.textContent = "No reports";
  }
}

function drawerPatch() {
  return {
    warmup: Number($("dWarmup").value),
    main: Number($("dMain").value),
    cooldown: Number($("dCooldown").value),
    thinkMs: Number($("dThinkMs").value),
    txTimeout: Number($("dTxTimeout").value),
    spikeCycles: Number($("dSpikeCycles").value),
    spikeHold: Number($("dSpikeHold").value),
    connMin: Number($("dConnMin").value),
    connMax: Number($("dConnMax").value),
  };
}

function rowPatch(tr) {
  const patch = {};
  tr.querySelectorAll("[data-field]").forEach((el) => {
    const field = el.getAttribute("data-field");
    if (el.tagName === "SELECT") patch[field] = el.value;
    else patch[field] = Number(el.value);
  });
  return patch;
}

$("rows").addEventListener("input", (ev) => {
  const tr = ev.target.closest("tr[data-id]");
  if (tr && ev.target.matches("[data-field]")) tr.dataset.dirty = "1";
});

$("rows").addEventListener("click", async (ev) => {
  const btn = ev.target.closest("button[data-act]");
  const tr = ev.target.closest("tr[data-id]");
  if (!tr) return;
  const id = tr.dataset.id;

  if (!btn) {
    openDrawer(id);
    return;
  }

  const act = btn.dataset.act;
  try {
    if (act === "edit") {
      openDrawer(id);
      return;
    }
    if (act === "start") {
      if (tr.querySelector("[data-field]")) {
        await api(`/api/sessions/${id}`, { method: "PATCH", body: JSON.stringify(rowPatch(tr)) });
        delete tr.dataset.dirty;
      }
      await api(`/api/sessions/${id}/start`, { method: "POST" });
    } else if (act === "pause") {
      await api(`/api/sessions/${id}/pause`, { method: "POST" });
    } else if (act === "resume") {
      await api(`/api/sessions/${id}/resume`, { method: "POST" });
    } else if (act === "stop") {
      btn.disabled = true;
      btn.textContent = "Stopping…";
      await api(`/api/sessions/${id}/stop`, { method: "POST" });
      toast("Stopped");
    } else if (act === "remove") {
      if (!confirm("Stop this database (if running) and remove it from the table?")) return;
      btn.disabled = true;
      const data = await api(`/api/sessions/${id}`, { method: "DELETE" });
      if (selectedId === id) {
        selectedId = null;
        $("drawer").classList.remove("open");
      }
      syncTable(data.sessions || []);
      toast("Removed");
      return;
    }
    await refresh();
  } catch (e) {
    toast(e.message, true);
    await refresh();
  }
});

$("btnCloseDrawer").addEventListener("click", () => {
  selectedId = null;
  $("drawer").classList.remove("open");
});

$("btnDrawerSave").addEventListener("click", async () => {
  if (!selectedId) return;
  try {
    await api(`/api/sessions/${selectedId}`, { method: "PATCH", body: JSON.stringify(drawerPatch()) });
    toast("Saved");
    await refresh();
  } catch (e) {
    toast(e.message, true);
  }
});

$("btnDrawerValidate").addEventListener("click", async () => {
  if (!selectedId) return;
  try {
    await api(`/api/sessions/${selectedId}/validate`, { method: "POST" });
    toast("Schema OK");
    await refresh();
  } catch (e) {
    toast(e.message, true);
    await refresh();
  }
});

$("btnDiscover").addEventListener("click", async () => {
  try {
    await discover();
  } catch (e) {
    toast(e.message, true);
  }
});

$("btnSaveConfig").addEventListener("click", async () => {
  try {
    await saveConfig();
  } catch (e) {
    toast(e.message, true);
  }
});

$("btnStartAll").addEventListener("click", async () => {
  if (!confirm("Start all idle databases?")) return;
  try {
    const data = await api("/api/sessions/start-all", { method: "POST" });
    syncTable(data.sessions || []);
    if (data.errors && data.errors.length) toast(data.errors.join("; "), true);
    else toast("Start All requested");
  } catch (e) {
    toast(e.message, true);
  }
});

$("btnStopAll").addEventListener("click", async () => {
  if (!confirm("Stop all running sessions?")) return;
  try {
    const data = await api("/api/sessions/stop-all", { method: "POST" });
    syncTable(data.sessions || []);
    toast("Stop All requested");
  } catch (e) {
    toast(e.message, true);
  }
});

$("btnPauseAll").addEventListener("click", async () => {
  try {
    const data = await api("/api/sessions/pause-all", { method: "POST" });
    syncTable(data.sessions || []);
    toast("Pause All requested");
  } catch (e) {
    toast(e.message, true);
  }
});

$("btnPurgeMissing").addEventListener("click", async () => {
  if (!confirm("Remove all missing databases from the table?")) return;
  try {
    const data = await api("/api/sessions/purge-missing", { method: "POST" });
    syncTable(data.sessions || []);
    toast(`Purged ${data.purged}`);
  } catch (e) {
    toast(e.message, true);
  }
});

$("btnValidateAll").addEventListener("click", async () => {
  try {
    const data = await api("/api/sessions/validate-all", { method: "POST" });
    syncTable(data.sessions || []);
    if (data.errors && data.errors.length) toast(data.errors.join("; "), true);
    else toast("All schemas OK");
  } catch (e) {
    toast(e.message, true);
  }
});

$("filterStatus").addEventListener("change", () => refresh().catch(() => {}));
$("filterPath").addEventListener("input", () => {
  clearTimeout($("filterPath")._t);
  $("filterPath")._t = setTimeout(() => refresh().catch(() => {}), 200);
});

(async function init() {
  await loadConfig();
  // Load existing sessions from API — do not auto-rescan
  await refresh();
  setInterval(() => refresh().catch(() => {}), 1500);
})();
