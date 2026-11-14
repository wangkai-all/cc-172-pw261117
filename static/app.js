"use strict";

const $ = (id) => document.getElementById(id);
const MAX_ROWS = 200;
let cfgColor = new Map();   // config_version -> 底色类名（切换边界一眼可辨）
let lastCfgByCh = new Map();

function esc(s) {
  return String(s).replace(/[&<>"]/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}
function fmtTime(ts) {
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString("zh-CN", { hour12: false }) +
    "." + String(d.getMilliseconds()).padStart(3, "0");
}
function cfgClass(ver) {
  if (!cfgColor.has(ver)) cfgColor.set(ver, cfgColor.size % 2 === 0 ? "cfg-a" : "cfg-b");
  return cfgColor.get(ver);
}

// ---------- 渲染 ----------
function renderStatus(st) {
  const conn = $("conn");
  conn.textContent = st.connected ? "已连接模拟器" : "未连接";
  conn.className = "pill " + (st.connected ? "green" : "red");
  const ps = $("pauseState");
  ps.textContent = st.paused ? "已暂停（字节积压中）" : "运行中";
  ps.className = "pill " + (st.paused ? "amber" : "green");
  $("simaddr").textContent = "simulator " + st.simulator + " · 流偏移 " + st.stream_offset;
  $("btnPause").disabled = st.paused;
  $("btnResume").disabled = !st.paused;
}

function renderStats(s) {
  $("stSamples").textContent = s.samples;
  $("stDup").textContent = s.duplicates;
  $("stBad").textContent = s.bad_packets;
  $("stNoise").textContent = s.noise_segments + " / " + s.noise_bytes;
}

function prependRow(tbody, tr) {
  tbody.insertBefore(tr, tbody.firstChild);
  while (tbody.rows.length > MAX_ROWS) tbody.deleteRow(-1);
}

function sampleRow(s) {
  const tr = document.createElement("tr");
  tr.className = cfgClass(s.config_version) + (s.flags ? " flagrow" : "");
  const badges = (s.flags || "").split(",").filter(Boolean)
    .map((f) => `<span class="badge ${esc(f)}">${esc(f)}</span>`).join(" ");
  tr.innerHTML =
    `<td class="num">${s.id ?? ""}</td><td>${s.channel}</td><td>${s.epoch}</td>` +
    `<td class="num">${s.seq}</td><td class="hex">${esc(s.raw_hex)}</td>` +
    `<td class="num">${s.raw_value}</td>` +
    `<td class="num">${Number(s.scaled).toPrecision(6)} ${esc(s.unit)}</td>` +
    `<td>v${s.config_version} ×${s.scale}</td>` +
    `<td class="num">${s.stream_offset}</td><td>${badges}</td>`;
  return tr;
}

function errorRow(e) {
  const tr = document.createElement("tr");
  tr.innerHTML =
    `<td class="num">${e.id}</td><td><span class="badge err">${esc(e.kind)}</span></td>` +
    `<td class="num">${e.stream_offset}</td><td class="num">${e.length}</td>` +
    `<td class="hex">${esc(e.raw_hex)}</td><td>${esc(e.detail)}</td>`;
  return tr;
}

function eventRow(e) {
  const tr = document.createElement("tr");
  tr.innerHTML =
    `<td class="num">${fmtTime(e.ts)}</td><td><b>${esc(e.kind)}</b></td>` +
    `<td>${e.channel ?? ""}</td><td>${esc(e.detail)}</td>`;
  return tr;
}

function renderConfigs(cfgs) {
  const tb = document.querySelector("#cfgTable tbody");
  tb.innerHTML = "";
  for (const c of cfgs) {
    const tr = document.createElement("tr");
    tr.innerHTML =
      `<td>v${c.id}</td><td>${c.channel}</td><td class="num">${c.scale}</td>` +
      `<td>${esc(c.unit)}</td><td>${c.effective_epoch}</td><td class="num">${c.effective_seq}</td>` +
      `<td class="num">${fmtTime(c.ts)}</td>`;
    tb.appendChild(tr);
  }
}

function renderLiveChannels(samples) {
  // 每通道最新值卡片
  const latest = new Map();
  for (const s of samples) if (!latest.has(s.channel)) latest.set(s.channel, s);
  const box = $("live");
  box.innerHTML = "";
  for (const [ch, s] of [...latest.entries()].sort((a, b) => a[0] - b[0])) {
    const div = document.createElement("div");
    div.className = "card live";
    div.innerHTML =
      `<div class="num" id="live-${ch}">${Number(s.scaled).toPrecision(6)} ${esc(s.unit)}</div>` +
      `<div class="lbl">通道 ${ch} · epoch ${s.epoch} · seq ${s.seq} · 配置 v${s.config_version}</div>`;
    box.appendChild(div);
  }
}

// ---------- 初始加载 ----------
async function boot() {
  const st = await (await fetch("/api/state")).json();
  renderStatus(st.status);
  renderStats(st.stats);
  $("stOffset").textContent = st.status.stream_offset;
  renderConfigs(st.configs);
  renderLiveChannels(st.samples);
  const tb = document.querySelector("#sampleTable tbody");
  for (const s of [...st.samples].reverse()) prependRow(tb, sampleRow(s));
  const eb = document.querySelector("#errTable tbody");
  for (const e of [...st.errors].reverse()) prependRow(eb, errorRow(e));
  const vb = document.querySelector("#evtTable tbody");
  for (const e of [...st.events].reverse()) prependRow(vb, eventRow(e));
  // 预填生效序号：当前序号 + 20，方便直接验证切换边界
  if (st.channels.length) {
    const ch0 = st.channels[0];
    document.querySelector("input[name=channel]").value = ch0.channel;
    document.querySelector("input[name=effective_seq]").value =
      (ch0.last_seq + 20) & 0xffff;
  }
}

// ---------- SSE ----------
function connect() {
  const es = new EventSource("/api/stream");
  es.addEventListener("sample", (ev) => {
    const s = JSON.parse(ev.data);
    prependRow(document.querySelector("#sampleTable tbody"), sampleRow(s));
    const card = $(`live-${s.channel}`);
    if (card) {
      card.textContent = `${Number(s.scaled).toPrecision(6)} ${s.unit}`;
      card.nextElementSibling.textContent =
        `通道 ${s.channel} · epoch ${s.epoch} · seq ${s.seq} · 配置 v${s.config_version}`;
    }
    lastCfgByCh.set(s.channel, s.config_version);
  });
  es.addEventListener("error", (ev) => {
    prependRow(document.querySelector("#errTable tbody"), errorRow(JSON.parse(ev.data)));
  });
  es.addEventListener("event", (ev) => {
    prependRow(document.querySelector("#evtTable tbody"), eventRow(JSON.parse(ev.data)));
  });
  es.addEventListener("config", async () => {
    const st = await (await fetch("/api/state")).json();
    renderConfigs(st.configs);
  });
  es.addEventListener("stats", (ev) => renderStats(JSON.parse(ev.data)));
  es.addEventListener("state", (ev) => {
    const st = JSON.parse(ev.data);
    renderStatus(st);
    $("stOffset").textContent = st.stream_offset;
  });
}

// ---------- 交互 ----------
$("btnPause").onclick = () => fetch("/api/control", {
  method: "POST", headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ action: "pause" }),
});
$("btnResume").onclick = () => fetch("/api/control", {
  method: "POST", headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ action: "resume" }),
});

$("cfgForm").onsubmit = async (e) => {
  e.preventDefault();
  const f = new FormData(e.target);
  const body = {
    channel: Number(f.get("channel")),
    scale: Number(f.get("scale")),
    unit: f.get("unit"),
    effective_seq: Number(f.get("effective_seq")),
    effective_epoch: f.get("effective_epoch") === "" ? null : Number(f.get("effective_epoch")),
  };
  const r = await (await fetch("/api/config", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  })).json();
  if (!r.ok) alert("配置失败: " + (r.error || "unknown"));
};

document.querySelectorAll(".triggers .btn").forEach((b) => {
  b.onclick = async () => {
    const r = await (await fetch("/api/simulate", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ cmd: b.dataset.cmd }),
    })).json();
    $("simReply").textContent = r.ok ? `模拟器: ${r.reply}` : `触发失败: ${r.error}`;
  };
});

boot().then(connect);
