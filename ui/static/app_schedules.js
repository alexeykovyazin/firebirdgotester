// ---------- Tabs ----------
let activeTab = "sessions";
document.querySelectorAll(".tab").forEach((btn) => {
  btn.addEventListener("click", () => {
    activeTab = btn.dataset.tab;
    document.querySelectorAll(".tab").forEach((b) => b.classList.toggle("active", b === btn));
    document.querySelectorAll(".tabsection").forEach((sec) => sec.classList.toggle("active", sec.id === "tab-" + activeTab));
    refresh().catch(() => {});
  });
});

// ---------- Schedules ----------
let schedules = [];
const schedFormOpen = () => $("schedForm").classList.contains("open");

function triggerText(t) {
  if (t.type === "once") return "once @ " + (t.at || "");
  if (t.type === "interval") return "every " + t.everyMin + " min";
  return "cron " + (t.cron || "") + (t.tz ? " (" + t.tz + ")" : "");
}

function targetsText(sc) {
  if (sc.targets && sc.targets.all) return "all discovered";
  const ids = (sc.targets && sc.targets.sessionIds) || [];
  if (!ids.length) return "—";
  const names = ids.map((id) => (sessionsById[id] ? sessionsById[id].relPath : id));
  return names.length <= 2 ? names.join(", ") : names[0] + " +" + (names.length - 1) + " more";
}

function outcomeBadge(o) {
  return '<span class="status outcome-' + escapeAttr(o || "skipped") + '">' + escapeHtml(o || "—") + "</span>";
}

function renderSchedules(list) {
  schedules = list || [];
  const tbody = $("schedRows");
  const keep = new Set();
  for (const sc of schedules) {
    keep.add(sc.id);
    let tr = tbody.querySelector('tr[data-id="' + sc.id + '"]');
    if (!tr) {
      tr = document.createElement("tr");
      tr.dataset.id = sc.id;
    }
    const lf = sc.lastFire || {};
    const runTxt = (sc.run && sc.run.timeLimitMin > 0 ? sc.run.timeLimitMin + " min" : "no limit") +
      (sc.run && sc.run.overrides && sc.run.overrides.profile ? " · " + escapeHtml(sc.run.overrides.profile) : "");
    tr.innerHTML =
      "<td>" + escapeHtml(sc.name) + "</td>" +
      "<td>" + escapeHtml(triggerText(sc.trigger)) + "</td>" +
      '<td title="' + escapeAttr((sc.targets && sc.targets.sessionIds || []).join(", ")) + '">' + escapeHtml(targetsText(sc)) + "</td>" +
      "<td>" + runTxt + "</td>" +
      "<td>" + (sc.enabled ? "✓" : "—") + "</td>" +
      '<td class="mono">' + escapeHtml(sc.nextRunAt || "—") + "</td>" +
      "<td>" + outcomeBadge(lf.outcome) + ' <span class="mono">' + escapeHtml(lf.at || "") + "</span>" +
      (lf.detail ? ' <span class="meta">' + escapeHtml(lf.detail) + "</span>" : "") + "</td>" +
      '<td class="actions">' +
      '<button data-sact="run">Run now</button>' +
      '<button data-sact="edit">Edit</button>' +
      '<button data-sact="toggle">' + (sc.enabled ? "Disable" : "Enable") + "</button>" +
      '<button class="danger" data-sact="del">Delete</button></td>';
    if (!tr.parentNode) tbody.appendChild(tr);
  }
  tbody.querySelectorAll("tr[data-id]").forEach((tr) => {
    if (!keep.has(tr.dataset.id)) tr.remove();
  });
  let empty = tbody.querySelector("tr.empty");
  if (!keep.size) {
    if (!empty) {
      tbody.innerHTML = '<tr class="empty"><td colspan="8" class="empty-state">' +
        '<div class="empty-title">No schedules</div>' +
        '<div class="empty-sub">Create one to run load automatically — once, on an interval, or on a cron.</div></td></tr>';
    }
  } else if (empty) {
    empty.remove();
  }
}

function renderSchedTargets(selected) {
  const sel = new Set(selected || []);
  const box = $("sfTargets");
  const dbs = Object.values(sessionsById).filter((s) => !s.missing);
  box.innerHTML =
    '<label style="margin-bottom:4px;"><input type="checkbox" id="sfAll" ' + (sel.size ? "" : "checked") + " /> <b>All discovered databases</b></label>" +
    dbs
      .map(function (s) {
        return '<label><input type="checkbox" class="sf-db" value="' + escapeAttr(s.id) + '" ' + (sel.has(s.id) ? "checked" : "") + " /> " + escapeHtml(s.relPath || s.name) + "</label>";
      })
      .join("");
  $("sfAll").addEventListener("change", function () {
    box.querySelectorAll(".sf-db").forEach(function (cb) { cb.checked = false; });
  });
  box.querySelectorAll(".sf-db").forEach(function (cb) {
    cb.addEventListener("change", function () {
      if (cb.checked) $("sfAll").checked = false;
      else if (!box.querySelector(".sf-db:checked")) $("sfAll").checked = true;
    });
  });
}

function openSchedForm(sc) {
  $("schedForm").classList.add("open");
  $("sfEditId").textContent = sc ? "editing " + sc.id : "";
  $("sfName").value = sc ? sc.name : "";
  $("sfType").value = sc ? sc.trigger.type : "once";
  $("sfAt").value = sc && sc.trigger.at ? sc.trigger.at : "";
  $("sfEvery").value = sc && sc.trigger.everyMin ? sc.trigger.everyMin : 360;
  $("sfCron").value = sc && sc.trigger.cron ? sc.trigger.cron : "";
  $("sfTz").value = sc && sc.trigger.tz ? sc.trigger.tz : "";
  $("sfTimeLimit").value = sc && sc.run.timeLimitMin ? sc.run.timeLimitMin : 30;
  $("sfProfile").value = sc && sc.run.overrides && sc.run.overrides.profile ? sc.run.overrides.profile : "";
  $("sfIfRunning").value = (sc && sc.policy.ifRunning) || "skip";
  $("sfIfBudget").value = (sc && sc.policy.ifBudgetFull) || "wait";
  $("sfNotify").value = (sc && sc.notifyUrl) || "";
  $("sfEnabled").checked = sc ? sc.enabled : true;
  syncTriggerFields();
  renderSchedTargets(sc && sc.targets ? sc.targets.sessionIds : []);
}

function syncTriggerFields() {
  const t = $("sfType").value;
  $("sfWrapAt").style.display = t === "once" ? "" : "none";
  $("sfWrapEvery").style.display = t === "interval" ? "" : "none";
  $("sfWrapCron").style.display = t === "cron" ? "" : "none";
  $("sfWrapTz").style.display = t === "cron" ? "" : "none";
}

function localToRFC3339(v) {
  // "2026-09-01T02:00" (local intent) -> RFC3339 with offset; anything that
  // already carries a zone passes through.
  if (!v) return v;
  if (/[zZ]$|[+-]\d\d:\d\d$/.test(v)) return v;
  const d = new Date(v);
  if (isNaN(d)) return v;
  const off = -d.getTimezoneOffset();
  const sign = off >= 0 ? "+" : "-";
  const pad = (n) => String(Math.abs(n)).padStart(2, "0");
  const hh = pad(Math.floor(Math.abs(off) / 60));
  const mm = pad(Math.abs(off) % 60);
  return d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate()) +
    "T" + pad(d.getHours()) + ":" + pad(d.getMinutes()) + ":" + pad(d.getSeconds()) + sign + hh + ":" + mm;
}

function schedFormBody() {
  const type = $("sfType").value;
  const trigger = { type: type };
  if (type === "once") trigger.at = localToRFC3339($("sfAt").value.trim());
  if (type === "interval") trigger.everyMin = Number($("sfEvery").value) || 60;
  if (type === "cron") {
    trigger.cron = $("sfCron").value.trim();
    const tz = $("sfTz").value.trim();
    if (tz) trigger.tz = tz;
  }
  const all = $("sfAll").checked;
  const ids = Array.from(document.querySelectorAll("#sfTargets .sf-db:checked")).map((cb) => cb.value);
  const run = { timeLimitMin: Number($("sfTimeLimit").value) || 30 };
  if ($("sfProfile").value) run.overrides = { profile: $("sfProfile").value };
  return {
    name: $("sfName").value.trim(),
    enabled: $("sfEnabled").checked,
    trigger: trigger,
    targets: all ? { all: true } : { sessionIds: ids },
    run: run,
    policy: { ifRunning: $("sfIfRunning").value, ifBudgetFull: $("sfIfBudget").value },
    notifyUrl: $("sfNotify").value.trim(),
  };
}

$("sfType").addEventListener("change", syncTriggerFields);
$("btnNewSchedule").addEventListener("click", function () {
  if (schedFormOpen()) {
    $("schedForm").classList.remove("open");
    return;
  }
  openSchedForm(null);
});
$("btnSchedCancel").addEventListener("click", () => $("schedForm").classList.remove("open"));

$("btnSchedSave").addEventListener("click", async function () {
  const txt = $("sfEditId").textContent;
  const editId = txt.indexOf("editing ") === 0 ? txt.replace("editing ", "").trim() : "";
  try {
    const body = JSON.stringify(schedFormBody());
    if (editId) {
      await api("/api/schedules/" + editId, { method: "PATCH", body: body });
      toast("Schedule updated");
    } else {
      await api("/api/schedules", { method: "POST", body: body });
      toast("Schedule created");
    }
    $("schedForm").classList.remove("open");
    await refresh();
  } catch (e) {
    toast(e.message, true);
  }
});

$("schedRows").addEventListener("click", async function (ev) {
  const btn = ev.target.closest("button[data-sact]");
  if (!btn) return;
  const id = btn.closest("tr").dataset.id;
  try {
    if (btn.dataset.sact === "run") {
      await api("/api/schedules/" + id + "/trigger", { method: "POST", body: "{}" });
      toast("Triggered — see the Runs tab");
    } else if (btn.dataset.sact === "edit") {
      const sc = schedules.find((x) => x.id === id);
      if (sc) openSchedForm(sc);
      return;
    } else if (btn.dataset.sact === "toggle") {
      const sc = schedules.find((x) => x.id === id);
      await api("/api/schedules/" + id, { method: "PATCH", body: JSON.stringify({ enabled: !(sc && sc.enabled) }) });
    } else if (btn.dataset.sact === "del") {
      if (!confirm("Delete this schedule? An in-flight run is not affected.")) return;
      await api("/api/schedules/" + id, { method: "DELETE" });
    }
    await refresh();
  } catch (e) {
    toast(e.message, true);
  }
});

// ---------- Runs ----------
const runsExpanded = new Set();
let lastRuns = [];

function renderRuns(runs) {
  lastRuns = runs || [];
  const tbody = $("runRows");
  const keep = new Set();
  for (const r of lastRuns) {
    keep.add(r.id);
    let tr = tbody.querySelector('tr[data-id="' + r.id + '"]');
    const okCount = (r.sessions || []).filter((x) => x.status === "Completed").length;
    const main =
      '<td class="mono">' + escapeHtml(r.id) + "</td>" +
      "<td>" + escapeHtml(r.origin) + (r.scheduleId ? ' <span class="meta mono">' + escapeHtml(r.scheduleId) + "</span>" : "") + "</td>" +
      '<td class="mono">' + escapeHtml(r.startedAt || "") + "</td>" +
      '<td class="mono">' + escapeHtml(r.finishedAt || "…") + "</td>" +
      "<td>" + outcomeBadge(r.outcome) + "</td>" +
      "<td>" + (r.sessions || []).length + " (" + okCount + " completed)</td>";
    if (!tr) {
      tr = document.createElement("tr");
      tr.dataset.id = r.id;
      tr.addEventListener("click", function () {
        if (runsExpanded.has(r.id)) runsExpanded.delete(r.id);
        else runsExpanded.add(r.id);
        renderRuns(lastRuns);
      });
      tbody.appendChild(tr);
    }
    tr.innerHTML = main;
    const prevDetail = tbody.querySelector('tr[data-detail="' + r.id + '"]');
    if (prevDetail) prevDetail.remove();
    if (runsExpanded.has(r.id)) {
      const detail = document.createElement("tr");
      detail.className = "run-detail";
      detail.dataset.detail = r.id;
      detail.innerHTML = "<td colspan=\"6\">" + (
        (r.sessions || [])
          .map(function (x) {
            let line = "• <b>" + escapeHtml(x.relPath || x.sessionID || x.id) + "</b> — " + escapeHtml(x.status) +
              " · tps " + (x.tps || 0).toFixed(1) + " · ok " + (x.success || 0) + " · err " + (x.errors || 0);
            if (x.lastError) line += ' · <span style="color:var(--danger)">' + escapeHtml(x.lastError) + "</span>";
            if (x.reason) line += ' · <span class="meta">' + escapeHtml(x.reason) + "</span>";
            if (x.reportDir) line += ' · <span class="meta mono">' + escapeHtml(x.reportDir) + "</span>";
            return "<div>" + line + "</div>";
          })
          .join("") || "<span class='meta'>no sessions</span>"
      ) + "</td>";
      tr.after(detail);
    }
  }
  tbody.querySelectorAll("tr[data-id]").forEach(function (tr) {
    if (!keep.has(tr.dataset.id)) {
      const d = tbody.querySelector('tr[data-detail="' + tr.dataset.id + '"]');
      if (d) d.remove();
      tr.remove();
    }
  });
}

$("btnRunsRefresh").addEventListener("click", () => refresh().catch(() => {}));

// refresh() is called by the 1.5s poll; extend it to cover the active tab.
const _baseRefresh = refresh;
refresh = async function () {
  await _baseRefresh();
  if (activeTab === "schedules") {
    const data = await api("/api/schedules");
    renderSchedules(data.schedules || []);
  } else if (activeTab === "runs") {
    const data = await api("/api/runs?limit=50");
    renderRuns(data.runs || []);
  }
};
