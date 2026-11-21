const $ = (id) => document.getElementById(id);

const state = {
  connected: false,
  role: null,
  username: null,
  host: null,
  port: 0,
  subscribed: false,
  udpPort: 9095,
  nodes: []
};

const history = new Map();
let selectedAgent = null;
let eventSource = null;
const maxLogLines = 300;

function escapeText(value) {
  return String(value == null ? '' : value);
}

function formatTime(iso) {
  if (!iso) return '';
  try {
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return String(iso);
    const hh = String(d.getHours()).padStart(2, '0');
    const mm = String(d.getMinutes()).padStart(2, '0');
    const ss = String(d.getSeconds()).padStart(2, '0');
    return hh + ':' + mm + ':' + ss;
  } catch (e) {
    return String(iso);
  }
}

async function api(path, options) {
  options = options || {};
  const opts = {
    method: options.method || 'GET',
    headers: Object.assign({ 'Content-Type': 'application/json' }, options.headers || {}),
    credentials: 'same-origin'
  };
  if (options.body !== undefined) {
    opts.body = JSON.stringify(options.body);
  }
  const res = await fetch(path, opts);
  let data = null;
  try {
    data = await res.json();
  } catch (e) {
    data = null;
  }
  return { ok: res.ok && (data ? data.ok !== false : true), status: res.status, data: data };
}

function showView(name) {
  document.querySelectorAll('.view').forEach((v) => v.classList.add('hidden'));
  const el = document.getElementById('view-' + name);
  if (el) el.classList.remove('hidden');
}

function setLoginError(message) {
  const el = $('login-error');
  if (!message) {
    el.classList.add('hidden');
    el.textContent = '';
    return;
  }
  el.textContent = message;
  el.classList.remove('hidden');
}

async function loadState() {
  const res = await api('/api/state');
  if (!res.ok || !res.data) return;
  const d = res.data;
  state.connected = !!d.connected;
  state.role = d.role;
  state.username = d.username;
  state.host = d.host;
  state.port = d.port;
  state.subscribed = !!d.subscribed;
  state.udpPort = d.udpPort || state.udpPort;
  state.nodes = Array.isArray(d.nodes) ? d.nodes : [];
  if (state.connected) {
    if (state.role === 'INSPECTOR') {
      showView('inspector');
      updateSessionTag();
      updateTelemetryButton();
      renderNodes();
      if (state.nodes.length === 0) loadNodes();
    } else if (state.role === 'AGENT') {
      showView('agent');
      updateAgentTag();
    }
    connectStream();
  } else {
    showView('login');
  }
}

function updateSessionTag() {
  const tag = $('session-tag');
  if (tag) {
    tag.textContent = 'INSPECTOR \u00b7 ' + (state.username || '') + ' @ ' + (state.host || '') + ':' + (state.port || '');
  }
}

function updateAgentTag() {
  const tag = $('agent-session-tag');
  if (tag) {
    tag.textContent = 'AGENT \u00b7 ' + (state.username || '');
  }
}

function updateTelemetryButton() {
  const btn = $('telemetry-btn');
  if (!btn) return;
  if (state.subscribed) {
    btn.textContent = 'UNSUBSCRIBE';
    btn.classList.add('active');
    const dot = document.createElement('span');
    dot.className = 'telemetry-dot';
    // avoid duplicate dots
    const existing = btn.querySelector('.telemetry-dot');
    if (!existing) btn.appendChild(dot);
  } else {
    btn.textContent = 'SUBSCRIBE';
    btn.classList.remove('active');
    const existing = btn.querySelector('.telemetry-dot');
    if (existing) existing.remove();
  }
}

async function loadNodes() {
  const res = await api('/api/nodes');
  if (!res.ok || !res.data) return;
  if (Array.isArray(res.data.nodes)) {
    state.nodes = res.data.nodes;
  }
  renderNodes();
}

function statusClass(status) {
  const s = String(status || '').toUpperCase();
  if (s === 'ONLINE') return 'status-online';
  if (s === 'STALE') return 'status-stale';
  if (s === 'OFFLINE') return 'status-offline';
  return '';
}

function renderNodes() {
  const body = $('nodes-body');
  if (!body) return;
  body.innerHTML = '';
  state.nodes.forEach((node) => {
    const tr = document.createElement('tr');
    if (selectedAgent === node.agentId) tr.classList.add('selected');
    tr.addEventListener('click', () => {
      selectedAgent = node.agentId;
      renderNodes();
      updateSparkline();
    });

    const cpu = typeof node.cpu === 'number' ? node.cpu : null;
    const ram = typeof node.ram === 'number' ? node.ram : null;
    const cpuPct = cpu == null ? 0 : Math.max(0, Math.min(1, cpu)) * 100;
    const ramPct = ram == null ? 0 : Math.max(0, Math.min(100, ram));

    const cells = [
      createCell('td', node.agentId || '-', 'mono tabular'),
      createStatusCell(node.status || '-'),
      createCell('td', node.location || '-', ''),
      createBarCell(cpu == null ? null : cpuPct, cpu == null ? '-' : (cpu * 100).toFixed(1) + '%'),
      createBarCell(ram == null ? null : ramPct, ram == null ? '-' : ram.toFixed(1) + '%'),
      createCell('td', '', '')
    ];
    cells.forEach((c) => tr.appendChild(c));
    body.appendChild(tr);
  });

  if (selectedAgent == null && state.nodes.length > 0) {
    selectedAgent = state.nodes[0].agentId;
    renderNodes();
    updateSparkline();
  }
  updateSparkline();
}

function createCell(tag, text, className) {
  const el = document.createElement(tag);
  el.textContent = escapeText(text);
  if (className) el.className = className;
  return el;
}

function createStatusCell(status) {
  const td = document.createElement('td');
  const span = document.createElement('span');
  span.className = 'status ' + statusClass(status);
  span.textContent = escapeText(status);
  td.appendChild(span);
  return td;
}

function createBarCell(value, label) {
  const td = document.createElement('td');
  const wrap = document.createElement('div');
  wrap.className = 'bar';
  const track = document.createElement('div');
  track.className = 'bar-track';
  const fill = document.createElement('div');
  fill.className = 'bar-fill';
  if (value != null) {
    fill.style.width = value + '%';
    if (value >= 90) fill.style.background = 'var(--status-err)';
    else if (value >= 75) fill.style.background = 'var(--status-warn)';
  } else {
    fill.style.width = '0%';
  }
  track.appendChild(fill);
  const val = document.createElement('span');
  val.className = 'bar-value tabular';
  val.textContent = escapeText(label);
  wrap.appendChild(track);
  wrap.appendChild(val);
  td.appendChild(wrap);
  return td;
}

function pushHistory(d) {
  if (!d || !d.agentId) return;
  if (!history.has(d.agentId)) {
    history.set(d.agentId, { cpu: [], ram: [] });
  }
  const h = history.get(d.agentId);
  const cpu = typeof d.cpu === 'number' ? d.cpu : 0;
  const ram = typeof d.ram === 'number' ? d.ram : 0;
  h.cpu.push(cpu);
  h.ram.push(ram);
  if (h.cpu.length > 20) h.cpu.shift();
  if (h.ram.length > 20) h.ram.shift();
}

function buildPoints(values, max) {
  const n = values.length;
  if (n === 0) return '';
  return values.map((v, i) => {
    const x = n === 1 ? 0 : (i / (n - 1)) * 200;
    const clamped = Math.max(0, Math.min(max, v));
    const y = 40 - (clamped / max) * 40;
    return x.toFixed(1) + ',' + y.toFixed(1);
  }).join(' ');
}

function updateSparkline() {
  const agent = selectedAgent;
  const h = agent ? history.get(agent) : null;
  const cpuArr = h ? h.cpu : [];
  const ramArr = h ? h.ram : [];
  const cpuPoly = document.querySelector('#cpu-spark polyline');
  const memPoly = document.querySelector('#mem-spark polyline');
  if (cpuPoly) cpuPoly.setAttribute('points', buildPoints(cpuArr, 1));
  if (memPoly) memPoly.setAttribute('points', buildPoints(ramArr, 100));
  const cpuReadout = $('cpu-readout');
  const memReadout = $('mem-readout');
  if (cpuReadout) {
    const last = cpuArr.length ? cpuArr[cpuArr.length - 1] : 0;
    cpuReadout.textContent = (last * 100).toFixed(1) + '%';
  }
  if (memReadout) {
    const last = ramArr.length ? ramArr[ramArr.length - 1] : 0;
    memReadout.textContent = last.toFixed(1) + '%';
  }
}

function updateTableTelemetry(d) {
  if (!d || !d.agentId) return;
  const idx = state.nodes.findIndex((n) => n.agentId === d.agentId);
  if (idx >= 0) {
    state.nodes[idx].cpu = d.cpu;
    state.nodes[idx].ram = d.ram;
    state.nodes[idx].status = 'ONLINE';
  } else {
    state.nodes.push({
      agentId: d.agentId,
      role: 'AGENT',
      status: 'ONLINE',
      location: '-',
      isp: '-',
      cpu: d.cpu,
      ram: d.ram
    });
  }
  renderNodes();
}

function appendLog(d) {
  const feed = $('log-feed');
  if (!feed) return;
  const line = document.createElement('div');
  line.className = 'log-line';

  const ts = document.createElement('span');
  ts.className = 'log-ts tabular';
  ts.textContent = formatTime(d && d.timestamp);

  const level = document.createElement('span');
  level.className = 'log-level ' + ((d && d.level === 'ERROR') ? 'error' : 'info');
  level.textContent = (d && d.level) ? d.level : 'INFO';

  const text = document.createElement('span');
  text.className = 'log-text';
  text.textContent = (d && (d.line || d.message || d.raw)) || '';

  line.appendChild(ts);
  line.appendChild(level);
  line.appendChild(text);
  feed.appendChild(line);
  while (feed.children.length > maxLogLines) {
    feed.removeChild(feed.firstChild);
  }
  feed.scrollTop = feed.scrollHeight;
}

function connectStream() {
  if (eventSource) {
    eventSource.close();
    eventSource = null;
  }
  eventSource = new EventSource('/api/stream');
  eventSource.addEventListener('telemetry', (e) => {
    let d = null;
    try { d = JSON.parse(e.data); } catch (err) { return; }
    pushHistory(d);
    updateTableTelemetry(d);
    updateSparkline();
  });
  eventSource.addEventListener('log', (e) => {
    let d = null;
    try { d = JSON.parse(e.data); } catch (err) { return; }
    appendLog(d);
  });
  eventSource.addEventListener('message', (e) => {
    let d = null;
    try { d = JSON.parse(e.data); } catch (err) { return; }
    appendLog({ level: 'INFO', line: (d.raw || d.content || ''), timestamp: d.timestamp });
  });
  eventSource.addEventListener('transfer', (e) => {
    let d = null;
    try { d = JSON.parse(e.data); } catch (err) { return; }
    appendLog({ level: 'INFO', line: 'Transfer ' + (d.direction || '') + ' ' + (d.filename || '') + ' ' + (d.bytesTransferred || 0) + '/' + (d.totalBytes || 0), timestamp: d.timestamp });
  });
  eventSource.addEventListener('disconnect', (e) => {
    let d = null;
    try { d = JSON.parse(e.data); } catch (err) { d = {}; }
    appendLog({ level: 'ERROR', line: 'Disconnected: ' + (d.reason || ''), timestamp: d.timestamp });
    state.connected = false;
    showView('login');
    setLoginError('Koneksi terputus: ' + (d.reason || ''));
  });
  eventSource.onerror = () => {
    // browser will auto-reconnect; keep quiet unless disconnected
  };
}

async function handleLoginSubmit(ev) {
  ev.preventDefault();
  setLoginError('');
  const body = {
    host: $('host').value.trim() || 'localhost',
    port: Number($('port').value) || 9090,
    role: $('role').value,
    username: $('username').value.trim(),
    password: $('password').value
  };
  const res = await api('/api/login', { method: 'POST', body: body });
  if (!res.ok || !res.data || !res.data.ok) {
    setLoginError((res.data && res.data.message) || 'Login gagal');
    return;
  }
  state.connected = true;
  state.role = res.data.role;
  state.username = res.data.username;
  state.host = body.host;
  state.port = body.port;
  state.subscribed = false;
  state.nodes = [];
  if (state.role === 'INSPECTOR') {
    showView('inspector');
    updateSessionTag();
    updateTelemetryButton();
    loadNodes();
  } else {
    showView('agent');
    updateAgentTag();
  }
  connectStream();
}

async function toggleTelemetry() {
  const btn = $('telemetry-btn');
  if (state.subscribed) {
    const res = await api('/api/telemetry/unsubscribe', { method: 'POST', body: {} });
    state.subscribed = false;
    state.udpPort = 0;
    updateTelemetryButton();
    if (res.data && res.data.line) appendLog({ level: 'INFO', line: res.data.line, timestamp: new Date().toISOString() });
  } else {
    const udpPort = Number($('udp-port').value) || 9095;
    const res = await api('/api/telemetry/subscribe', { method: 'POST', body: { udpPort: udpPort } });
    if (res.ok && res.data && res.data.ok) {
      state.subscribed = true;
      state.udpPort = udpPort;
      updateTelemetryButton();
    } else {
      appendLog({ level: 'ERROR', line: (res.data && res.data.message) || 'Subscribe gagal', timestamp: new Date().toISOString() });
    }
    if (res.data && res.data.line) appendLog({ level: 'INFO', line: res.data.line, timestamp: new Date().toISOString() });
  }
}

async function disconnect() {
  if (eventSource) {
    eventSource.close();
    eventSource = null;
  }
  await api('/api/logout', { method: 'POST', body: {} });
  state.connected = false;
  state.nodes = [];
  state.subscribed = false;
  selectedAgent = null;
  history.clear();
  showView('login');
}

document.addEventListener('DOMContentLoaded', () => {
  $('login-form').addEventListener('submit', handleLoginSubmit);
  const rb = $('refresh-btn');
  if (rb) rb.addEventListener('click', loadNodes);
  const tb = $('telemetry-btn');
  if (tb) tb.addEventListener('click', toggleTelemetry);
  const db = $('disconnect-btn');
  if (db) db.addEventListener('click', disconnect);
  const adb = $('agent-disconnect-btn');
  if (adb) adb.addEventListener('click', disconnect);
  loadState();
});