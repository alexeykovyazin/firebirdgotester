// ---------- SUMMARY tab ----------
// Detailed final-run summary (FOLLOWUP_PLAN 2026-09-29): fetches
// /api/sessions/{id}/summary — an in-memory engine view, safe mid-run.
// Same monkey-patch refresh pattern as app_schedules.js / app_emul.js.

function sumFmtNs(ns) {
  ns = Number(ns || 0);
  if (ns <= 0) return "0s";
  const s = ns / 1e9;
  if (s < 60) return s.toFixed(1) + "s";
  const m = Math.floor(s / 60);
  const r = Math.round(s % 60);
  if (m < 60) return `${m}m${String(r).padStart(2, "0")}s`;
  return `${Math.floor(m / 60)}h${String(m % 60).padStart(2, "0")}m`;
}

function sumFmtT(n) {
  if (!n) return "0";
  return Number(n).toLocaleString();
}

function sumTime(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  return isNaN(d) ? "" : d.toLocaleTimeString();
}

function sumOpts() {
  const sel = document.getElementById("sumSession");
  if (!sel) return;
  const cur = sel.value;
  const rows = Object.values(sessionsById).sort((a, b) =>
    (a.name || "").localeCompare(b.name || "")
  );
  sel.innerHTML = rows.length
    ? rows
        .map(
          (s) =>
            `<option value="${escapeAttr(s.id)}">${escapeHtml(
              s.name || s.relPath || s.id
            )}</option>`
        )
        .join("")
    : `<option value="">(no databases)</option>`;
  if (cur) sel.value = cur;
}

function sumTile(label, value) {
  return `<div class="sum-tile"><span class="label">${escapeHtml(
    label
  )}</span><b>${value}</b></div>`;
}

function sumTable(headers, rows) {
  if (!rows || !rows.length) return `<div class="muted">—</div>`;
  return `<div class="emul-table-wrap" style="max-height:340px;"><table><thead><tr>${headers
    .map((h) => `<th>${escapeHtml(h)}</th>`)
    .join("")}</tr></thead><tbody>${rows.join("")}</tbody></table></div>`;
}

function sumTimeline(tl) {
  if (!tl || !tl.length) return `<div class="muted">—</div>`;
  const maxN = Math.max(...tl.map((p) => p.n || 0), 1);
  const bars = tl
    .map((p) => {
      const okH = Math.round(((p.ok || 0) / maxN) * 74);
      const errH = Math.round(((p.err || 0) / maxN) * 74);
      const avg = p.n ? Math.round((p.latSumMs || 0) / p.n) : 0;
      return `<div class="sum-col" title="m${p.minute}: ok=${p.ok} err=${p.err} avg=${avg}ms"><span class="sum-err" style="height:${errH}px"></span><span class="sum-ok" style="height:${okH}px"></span></div>`;
    })
    .join("");
  return `<div class="sum-bars">${bars}</div>`;
}

function sumRender(s) {
  const c = s.config || {};
  const t = s.totals || {};
  const lat = s.latency || {};
  const pool = s.pool || {};
  const conc = s.concurrency || {};
  const err = s.errors || {};

  const out = [];

  out.push(
    `<div class="sum-grid">` +
      sumTile("Elapsed", sumFmtNs(t.elapsed)) +
      sumTile("Units", `${sumFmtT(t.total)} (OK ${sumFmtT(t.success)}, ${Number(t.successPct || 0).toFixed(1)}%)`) +
      sumTile("TPS", Number(t.tps || 0).toFixed(1)) +
      (s.emul ? sumTile("Score (main)", `${Math.round(t.scorePerMin || 0)}/min`) : "") +
      sumTile("Workers cur/max", `${conc.workersCur || 0}/${conc.workersMax || 0}`) +
      sumTile("Conns cur/min/max", `${pool.connsCur || 0}/${pool.connsMin || 0}/${pool.connsMax || 0}`) +
      sumTile("Pool rebuilds ok/fail", `${sumFmtT(pool.rebuildsOk)}/${sumFmtT(pool.rebuildsFailed)}`) +
      (s.teardown
        ? sumTile("Stop failures / reaped", `${s.teardown.stopFailures || 0}/${s.teardown.reaped || 0} (drain ${sumFmtNs(s.teardown.drainDuration)})`)
        : "") +
      `</div>`
  );

  out.push(
    `<div class="panel"><h3>Run</h3><div class="sum-kv">` +
      `dsn=${escapeHtml(c.dsn || "-")} · profile=${escapeHtml(c.profile || "-")} · conns=${c.connMin || 0}..${c.connMax || 0} · tx-timeout=${c.txTimeoutSec || 0}s · extended=${!!c.extendedLoad} · noLimbo=${!!c.noLimbo} · hardDrop=${!!c.cancelHardDrop}(${c.hardDropGraceMs || 0}ms)` +
      `</div></div>`
  );

  const latTiles =
    sumTile("avg", sumFmtNs(lat.avg)) +
    sumTile("p50", sumFmtNs(lat.p50)) +
    sumTile("p95", sumFmtNs(lat.p95)) +
    sumTile("p99", sumFmtNs(lat.p99)) +
    sumTile("max", sumFmtNs(lat.max));
  out.push(`<div class="panel"><h3>Latency</h3><div class="sum-grid">${latTiles}</div></div>`);

  if (s.phases && s.phases.length) {
    const rows = s.phases.map(
      (p) =>
        `<tr><td>${escapeHtml(p.phase)}</td><td>${sumTime(p.start)}</td><td>${sumTime(
          p.end
        )}</td><td>${sumFmtNs(
          (new Date(p.end) - new Date(p.start)) * 1e6
        )}</td></tr>`
    );
    out.push(
      `<div class="panel"><h3>Phases (actual)</h3>${sumTable(["Phase", "Start", "End", "Duration"], rows)}</div>`
    );
  }

  const kindRows = Object.entries(err.kinds || {})
    .sort((a, b) => b[1] - a[1])
    .map(([k, v]) => `<tr><td>${escapeHtml(k)}</td><td>${sumFmtT(v)}</td></tr>`);
  const topRows = (err.top || []).map(
    (e) =>
      `<tr><td>${sumFmtT(e.count)}</td><td style="word-break:break-all;">${escapeHtml(
        e.message
      )}</td><td>${sumTime(e.firstSeen)}</td><td>${sumTime(e.lastSeen)}</td></tr>`
  );
  out.push(
    `<div class="panel"><h3>Errors</h3><div class="sum-kv">total=${sumFmtT(
      err.total
    )} · expected=${sumFmtT(err.expected)} · <b>unexpected=${sumFmtT(
      err.unexpected
    )}</b> · retryable=${sumFmtT(err.retryable)}</div>` +
      sumTable(["Kind", "Count"], kindRows) +
      (topRows.length ? sumTable(["Count", "Message", "First", "Last"], topRows) : "") +
      `</div>`
  );

  if (s.units && s.units.length) {
    const rows = s.units.map(
      (u) =>
        `<tr><td>${escapeHtml(u.unit)}</td><td>${sumFmtT(u.attempts)}</td><td>${sumFmtT(
          u.ok
        )}</td><td>${sumFmtT(u.conflict)}</td><td>${sumFmtT(u.rejected)}</td><td>${sumFmtT(
          u.failure
        )}</td><td>${u.avgMs || 0}</td><td>${u.maxMs || 0}</td></tr>`
    );
    out.push(
      `<div class="panel"><h3>Units (top 20 by failure)</h3>${sumTable(
        ["Unit", "Attempts", "OK", "Conflict", "Rejected", "Failure", "Avg ms", "Max ms"],
        rows
      )}</div>`
    );
  }

  const varRows = Object.entries(s.variants || {})
    .sort()
    .map(([k, v]) => `<tr><td>${escapeHtml(k)}</td><td>${sumFmtT(v.attempts)}</td><td>${sumFmtT(v.ok)}</td></tr>`);
  const compRows = Object.entries(s.completions || {})
    .sort()
    .map(([k, v]) => `<tr><td>${escapeHtml(k)}</td><td>${sumFmtT(v.attempts)}</td><td>${sumFmtT(v.ok)}</td></tr>`);
  if (varRows.length || compRows.length) {
    out.push(
      `<div class="emul-grid cols2"><div class="panel"><h3>Transaction variants</h3>${sumTable(
        ["Isolation", "Attempts", "OK"],
        varRows
      )}</div><div class="panel"><h3>Completion methods</h3>${sumTable(
        ["Completion", "Attempts", "OK"],
        compRows
      )}</div></div>`
    );
  }

  const ext = s.extended;
  if (ext) {
    out.push(
      `<div class="panel"><h3>Extended load</h3><div class="sum-kv">` +
        `heavy rounds=${sumFmtT(ext.heavyRounds)} (fail ${sumFmtT(ext.heavyFailures)}) · bulk ins/upd/del=${sumFmtT(
          ext.bulkInserts
        )}/${sumFmtT(ext.bulkUpdates)}/${sumFmtT(ext.bulkDeletes)} rows=${sumFmtT(ext.bulkRows)} · ddl +/~/-=${sumFmtT(
          ext.columnsAdded
        )}/${sumFmtT(ext.columnsAltered)}/${sumFmtT(ext.columnsDropped)} tables=${sumFmtT(ext.tablesCreated)}/${sumFmtT(
          ext.tablesDropped
        )}<br/>limbo: resolved=${sumFmtT(ext.limboResolved)} (commit=${sumFmtT(ext.limboCommit)} rollback=${sumFmtT(
          ext.limboRollback
        )} two_phase=${sumFmtT(ext.limboTwoPhase)}) · peak unresolved=${sumFmtT(ext.limboPeak)} · max age=${sumFmtT(
          ext.limboMaxAgeSec
        )}s` +
        `</div></div>`
    );
  }

  const em = s.emul;
  if (em) {
    const inv = em.invariantStats || {};
    out.push(
      `<div class="panel"><h3>oltp-emul</h3><div class="sum-kv">` +
        `units ok=${sumFmtT(em.okUnits)}/${sumFmtT(em.totalUnits)} · workingMode=${escapeHtml(
          em.workingMode || "-"
        )} · mem peaks db=${Math.round((em.memPeaks?.dbBytes || 0) / 1048576)}MB<br/>` +
        `invariants: verdict=${escapeHtml(em.invariant || "-")} · disabled=${!!inv.disabled} · checks=${sumFmtT(
          inv.checks
        )} ok=${sumFmtT(inv.ok)} transient=${sumFmtT(inv.transient)} hard=${sumFmtT(inv.hard)}` +
        (inv.lastReason ? `<br/>last reason: ${escapeHtml(inv.lastReason)}` : "") +
        `</div></div>`
    );
  }

  if (s.timeline && s.timeline.length) {
    out.push(`<div class="panel"><h3>Per-minute OK (green) / err (red)</h3>${sumTimeline(s.timeline)}</div>`);
  }

  return out.join("");
}

async function loadSummary() {
  const sel = document.getElementById("sumSession");
  const box = document.getElementById("sumContent");
  if (!sel || !box) return;
  const id = sel.value;
  if (!id) {
    box.innerHTML = `<span class="muted">No databases discovered.</span>`;
    return;
  }
  try {
    const s = await api(`/api/sessions/${encodeURIComponent(id)}/summary`);
    box.innerHTML = sumRender(s);
  } catch (e) {
    box.innerHTML = `<span class="muted">Summary unavailable: ${escapeHtml(e.message)}</span>`;
  }
}

$("sumSession").addEventListener("change", () => loadSummary());
$("sumRefresh").addEventListener("click", () => loadSummary());

// refresh() is called by the 1.5s poll; extend it to cover the active tab.
const _summaryBaseRefresh = refresh;
refresh = async function () {
  await _summaryBaseRefresh();
  if (activeTab !== "summary") return;
  sumOpts();
  if (!document.getElementById("sumAuto").checked) return;
  await loadSummary();
};
