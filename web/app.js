const $ = (id) => document.getElementById(id);

const state = {
  connected: false,
  role: null,
  username: null,
  host: null,
  port: 0,
  subscribed: false,
  udpPort: 9095,
  nodes: [],
  stagedFiles: [],
  receivedFiles: [],
  receivedFilesDetail: [],
  messages: []
};

const history = new Map();
let selectedAgent = null;
let eventSource = null;
const maxLogLines = 300;
const SPARK_HEIGHT = 88;

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
  const token = (typeof sessionStorage !== 'undefined') ? sessionStorage.getItem('nm_sid') : null;
  const headers = Object.assign({ 'Content-Type': 'application/json' }, options.headers || {});
  if (token) headers['X-Session-Token'] = token;
  const opts = {
    method: options.method || 'GET',
    headers: headers,
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
  state.stagedFiles = Array.isArray(d.stagedFiles) ? d.stagedFiles : [];
  state.receivedFiles = Array.isArray(d.receivedFiles) ? d.receivedFiles : [];
  state.receivedFilesDetail = Array.isArray(d.receivedFilesDetail) ? d.receivedFilesDetail : [];
  if (state.connected) {
    if (state.role === 'INSPECTOR') {
      showView('inspector');
      updateSessionTag();
      updateTelemetryButton();
      renderNodes();
      updateStagedFilesUI();
      updatePushFileSelect();
      updateChatTarget();
      if (state.nodes.length === 0) loadNodes();
    } else if (state.role === 'AGENT') {
      showView('agent');
      updateAgentTag();
      renderAgentFiles();
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
  updateChatTarget();
}

function renderAgentFiles() {
  const body = $('agent-files-body');
  if (!body) return;
  body.innerHTML = '';
  const files = Array.isArray(state.receivedFilesDetail) ? state.receivedFilesDetail : [];
  if (files.length === 0) {
    const tr = document.createElement('tr');
    const td = document.createElement('td');
    td.colSpan = 4;
    td.textContent = 'Belum ada berkas yang diterima.';
    td.className = 'mono';
    tr.appendChild(td);
    body.appendChild(tr);
    return;
  }
  files.slice().reverse().forEach((f) => {
    const tr = document.createElement('tr');
    const cells = [
      createCell('td', f.filename || '-', 'mono tabular'),
      createCell('td', typeof f.bytes === 'number' ? f.bytes.toLocaleString() : '-', 'mono tabular'),
      createCell('td', f.timestamp ? formatTime(f.timestamp) : '-', 'mono tabular'),
      createCell('td', f.localPath || ('downloads/' + (state.username || '') + '/' + (f.filename || '-')), 'mono tabular')
    ];
    cells.forEach((c) => tr.appendChild(c));
    body.appendChild(tr);
  });
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
  updateDiagnosticsDropdown();
  updatePushAgentSelect();
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
    const y = SPARK_HEIGHT - (clamped / max) * SPARK_HEIGHT;
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

function updateDiagnosticsDropdown() {
  const sel = $('diag-agent');
  if (!sel) return;
  const current = sel.value;
  sel.innerHTML = '';
  state.nodes.forEach((node) => {
    if (String(node.role || '').toUpperCase() === 'AGENT') {
      const o = document.createElement('option');
      o.value = node.agentId || '';
      o.textContent = escapeText(node.agentId);
      sel.appendChild(o);
    }
  });
  if (current && [...sel.options].some((o) => o.value === current)) {
    sel.value = current;
  }
}

function showWorkspaceTab(tab) {
  document.querySelectorAll('.ws-tab').forEach((b) => {
    b.classList.toggle('active', b.dataset.tab === tab);
  });
  const diag = $('ws-diag');
  const files = $('ws-files');
  const comms = $('ws-comms');
  if (diag) diag.classList.toggle('hidden', tab !== 'diag');
  if (files) files.classList.toggle('hidden', tab !== 'files');
  if (comms) comms.classList.toggle('hidden', tab !== 'comms');
  if (tab === 'files') {
    updateStagedFilesUI();
    updatePushAgentSelect();
    updatePushFileSelect();
  }
  if (tab === 'comms') {
    renderChatFeed();
  }
}

function updateStagedFilesUI() {
  const list = $('staged-files');
  if (!list) return;
  list.innerHTML = '';
  const files = Array.isArray(state.stagedFiles) ? state.stagedFiles : [];
  if (files.length === 0) {
    const empty = document.createElement('div');
    empty.className = 'file-row';
    empty.textContent = 'No staged files';
    list.appendChild(empty);
    return;
  }
  files.forEach((name) => {
    const row = document.createElement('div');
    row.className = 'file-row';
    const n = document.createElement('span');
    n.textContent = escapeText(name);
    row.appendChild(n);
    list.appendChild(row);
  });
}

function updatePushFileSelect() {
  const sel = $('push-file-select');
  if (!sel) return;
  const current = sel.value;
  sel.innerHTML = '';
  const files = Array.isArray(state.stagedFiles) ? state.stagedFiles : [];
  files.forEach((name) => {
    const o = document.createElement('option');
    o.value = name;
    o.textContent = escapeText(name);
    sel.appendChild(o);
  });
  if (current && [...sel.options].some((o) => o.value === current)) {
    sel.value = current;
  }
}

function updatePushAgentSelect() {
  const sel = $('push-agent-select');
  if (!sel) return;
  const current = sel.value;
  sel.innerHTML = '';
  state.nodes.forEach((node) => {
    if (String(node.role || '').toUpperCase() === 'AGENT') {
      const o = document.createElement('option');
      o.value = node.agentId || '';
      o.textContent = escapeText(node.agentId);
      sel.appendChild(o);
    }
  });
  if (current && [...sel.options].some((o) => o.value === current)) {
    sel.value = current;
  }
}

function appendPushStatus(text) {
  const out = $('push-status');
  if (!out) return;
  const ts = formatTime(new Date().toISOString());
  const line = '[' + ts + '] ' + String(text);
  out.textContent = out.textContent ? out.textContent + '\n' + line : line;
  out.scrollTop = out.scrollHeight;
}

async function handleFileUpload() {
  const input = $('file-input');
  const file = input && input.files && input.files[0];
  if (!file) {
    appendPushStatus('Pilih berkas terlebih dahulu.');
    return;
  }
  const progress = $('upload-progress');
  if (progress) progress.style.width = '0%';
  let data = null;
  let status = 0;
  try {
    const body = await file.arrayBuffer();
    const res = await fetch('/api/files/upload', {
      method: 'POST',
      headers: { 'Content-Type': 'application/octet-stream', 'X-Filename': file.name },
      body: body,
      credentials: 'same-origin'
    });
    status = res.status;
    try { data = await res.json(); } catch (e) { data = null; }
  } catch (e) {
    appendPushStatus('Upload gagal: ' + String(e));
    return;
  }
  const ok = status < 400 && (data ? data.ok !== false : true);
  if (ok && data && data.filename) {
    if (!state.stagedFiles.includes(data.filename)) state.stagedFiles.push(data.filename);
    updateStagedFilesUI();
    updatePushFileSelect();
    appendPushStatus('Upload selesai: ' + data.filename + ' (' + (data.bytesTransferred || 0) + ' bytes)');
  } else {
    appendPushStatus('Upload gagal: ' + ((data && data.message) || 'tidak diketahui'));
  }
  if (progress) progress.style.width = '0%';
}

async function handleFilePush() {
  const filename = $('push-file-select') ? $('push-file-select').value : '';
  const agent = $('push-agent-select') ? $('push-agent-select').value : '';
  if (!filename) {
    appendPushStatus('Pilih berkas yang sudah di-stage.');
    return;
  }
  if (!agent) {
    appendPushStatus('Pilih Agent target.');
    return;
  }
  const res = await api('/api/files/push', { method: 'POST', body: { agentId: agent, filename } });
  if (res.ok && res.data && res.data.line) {
    appendPushStatus(res.data.line);
  } else {
    appendPushStatus('Push gagal: ' + ((res.data && res.data.message) || 'tidak diketahui'));
  }
}

function appendDiagnosticsLog(text) {
  const out = $('diag-exec-output');
  if (!out) return;
  const ts = formatTime(new Date().toISOString());
  const line = '[' + ts + '] ' + String(text);
  out.textContent = out.textContent ? out.textContent + '\n' + line : line;
  out.scrollTop = out.scrollHeight;
}

function formatSysinfoLabel(key) {
  const map = {
    os: 'Os',
    cores: 'Cores',
    ram: 'Ram',
    agentuptime: 'Agent Uptime'
  };
  const k = String(key).toLowerCase();
  return map[k] || key;
}

async function diagSysinfo() {
  const sel = $('diag-agent');
  const agent = sel ? sel.value : '';
  if (!agent) {
    appendDiagnosticsLog('Pilih Agent target terlebih dahulu.');
    return;
  }
  const out = $('diag-sysinfo-output');
  const res = await api('/api/diagnostics/sysinfo', { method: 'POST', body: { agentId: agent } });
  const lines = [];
  if (res.ok && res.data && res.data.sysinfo && Object.keys(res.data.sysinfo).length > 0) {
    Object.entries(res.data.sysinfo).forEach((entry) => {
      lines.push(formatSysinfoLabel(entry[0]) + ': ' + escapeText(entry[1]));
    });
  } else {
    lines.push('SYSINFO gagal: ' + escapeText((res.data && res.data.message) || 'tidak diketahui'));
  }
  if (out) out.textContent = lines.join('\n');
}

async function diagExec() {
  const sel = $('diag-agent');
  const agent = sel ? sel.value : '';
  const command = $('diag-exec-command').value.trim();
  if (!agent) {
    appendDiagnosticsLog('Pilih Agent target terlebih dahulu.');
    return;
  }
  if (!command) {
    appendDiagnosticsLog('Masukkan perintah terlebih dahulu.');
    return;
  }
  const res = await api('/api/diagnostics/exec', { method: 'POST', body: { agentId: agent, command } });
  if (res.ok && res.data && res.data.line) {
    let text = res.data.line.replace(/^200 EXEC [^ ]+ /, '').replace(/\\n/g, '\n');
    appendDiagnosticsLog(text || '(kosong)');
  } else {
    appendDiagnosticsLog('EXEC gagal: ' + escapeText((res.data && res.data.message) || 'tidak diketahui'));
  }
}

async function diagPing() {
  const host = $('diag-ping-host').value.trim();
  if (!host) return;
  const res = await api('/api/diagnostics/ping', { method: 'POST', body: { host } });
  appendDiagnosticsLog('PING ' + escapeText(host) + ' -> ' + escapeText((res.data && res.data.line) || (res.data && res.data.message) || 'gagal'));
}

async function diagResolve() {
  const domain = $('diag-resolve-domain').value.trim();
  if (!domain) return;
  const res = await api('/api/diagnostics/resolve', { method: 'POST', body: { domain } });
  appendDiagnosticsLog('RESOLVE ' + escapeText(domain) + ' -> ' + escapeText((res.data && res.data.line) || (res.data && res.data.message) || 'gagal'));
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

function appendAgentMessage(d) {
  const feed = $('agent-msg-feed');
  if (!feed) return;
  const line = document.createElement('div');
  line.className = 'log-line';
  const ts = document.createElement('span');
  ts.className = 'log-ts tabular';
  ts.textContent = formatTime(d && d.timestamp);
  const sender = document.createElement('span');
  sender.className = 'log-level info';
  sender.textContent = (d && (d.sender || d.type || 'MSG')).toUpperCase();
  const text = document.createElement('span');
  text.className = 'log-text';
  text.textContent = (d && (d.content || d.raw || '')) || '';
  line.appendChild(ts);
  line.appendChild(sender);
  line.appendChild(text);
  feed.appendChild(line);
  while (feed.children.length > maxLogLines) {
    feed.removeChild(feed.firstChild);
  }
  feed.scrollTop = feed.scrollHeight;
}

function createChatLine(d) {
  const line = document.createElement('div');
  line.className = 'log-line';
  const ts = document.createElement('span');
  ts.className = 'log-ts tabular';
  ts.textContent = formatTime(d && d.timestamp);
  const sender = document.createElement('span');
  sender.className = 'log-level info';
  sender.textContent = (d && (d.sender || 'MSG')).toUpperCase();
  const text = document.createElement('span');
  text.className = 'log-text';
  text.textContent = (d && (d.content || d.raw || '')) || '';
  line.appendChild(ts);
  line.appendChild(sender);
  line.appendChild(text);
  return line;
}

function appendChatFeed(id, d) {
  const feed = $(id);
  if (!feed) return;
  feed.appendChild(createChatLine(d));
  while (feed.children.length > maxLogLines) {
    feed.removeChild(feed.firstChild);
  }
  feed.scrollTop = feed.scrollHeight;
}

/** Render ulang seluruh pesan dari state ke feed Team Comms (dipakai saat tab dibuka). */
function renderChatFeed() {
  const feed = $('chat-feed');
  if (!feed) return;
  feed.innerHTML = '';
  state.messages.slice(-maxLogLines).forEach((d) => {
    feed.appendChild(createChatLine(d));
  });
  feed.scrollTop = feed.scrollHeight;
}

/** Atur status inline (hijau untuk sukses, merah untuk gagal) di bawah form kirim pesan. */
function setInlineStatus(id, ok, text) {
  const el = $(id);
  if (!el) return;
  el.textContent = text || '';
  el.className = 'inline-status ' + (ok ? 'success' : 'error');
}

function updateChatTarget() {
  const sel = $('chat-target');
  if (!sel) return;
  const current = sel.value;
  sel.innerHTML = '<option value="ALL">ALL (Broadcast)</option>';
  state.nodes.forEach((node) => {
    const o = document.createElement('option');
    o.value = node.agentId || '';
    o.textContent = (node.agentId || '') + ' (' + (node.role || '') + ')';
    sel.appendChild(o);
  });
  if (current && [...sel.options].some((o) => o.value === current)) {
    sel.value = current;
  }
}

async function sendAgentReply() {
  const recipient = $('agent-reply-recipient') ? $('agent-reply-recipient').value.trim() : 'admin';
  const message = $('agent-reply-message') ? $('agent-reply-message').value.trim() : '';
  if (!message) return;
  const target = recipient || 'admin';
  const res = await api('/api/chat/send', { method: 'POST', body: { target: target, message: message } });
  if (res.ok) {
    if ($('agent-reply-message')) $('agent-reply-message').value = '';
    appendAgentMessage({ type: 'message', sender: 'YOU -> ' + target, content: message, timestamp: new Date().toISOString() });
    setInlineStatus('agent-reply-status', true, '200 OK');
  } else {
    const reason = (res.data && res.data.message) || 'Gagal mengirim pesan';
    appendLog({ level: 'ERROR', line: reason, timestamp: new Date().toISOString() });
    setInlineStatus('agent-reply-status', false, reason);
  }
}

async function sendChat() {
  const target = $('chat-target') ? $('chat-target').value : 'ALL';
  const message = $('chat-message') ? $('chat-message').value.trim() : '';
  if (!message) return;
  const res = await api('/api/chat/send', { method: 'POST', body: { target: target, message: message } });
  if (res.ok) {
    if ($('chat-message')) $('chat-message').value = '';
    const echo = { type: 'message', sender: 'YOU -> ' + target, content: message, timestamp: new Date().toISOString() };
    state.messages.push(echo);
    renderChatFeed();
    setInlineStatus('chat-status', true, '200 OK');
  } else {
    const reason = (res.data && res.data.message) || 'Gagal mengirim pesan';
    appendLog({ level: 'ERROR', line: reason, timestamp: new Date().toISOString() });
    setInlineStatus('chat-status', false, reason);
  }
}

function connectStream() {
  if (eventSource) {
    eventSource.close();
    eventSource = null;
  }
  const sid = (typeof sessionStorage !== 'undefined') ? sessionStorage.getItem('nm_sid') : null;
  eventSource = new EventSource('/api/stream' + (sid ? '?sid=' + encodeURIComponent(sid) : ''));
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
    state.messages.push(d);
    if (state.role === 'AGENT') {
      appendAgentMessage(d);
    } else {
      const comms = $('ws-comms');
      if (!comms || !comms.classList.contains('hidden')) {
        renderChatFeed();
      }
    }
    appendLog({ level: 'INFO', line: (d.raw || d.content || ''), timestamp: d.timestamp });
  });
  eventSource.addEventListener('transfer', (e) => {
    let d = null;
    try { d = JSON.parse(e.data); } catch (err) { return; }
    const msg = 'Transfer ' + (d.direction || '') + ' ' + (d.filename || '') + ' ' + (d.bytesTransferred || 0) + '/' + (d.totalBytes || 0);
    appendLog({ level: 'INFO', line: msg, timestamp: d.timestamp });
    const prog = $('upload-progress');
    if (prog && d.totalBytes) {
      prog.style.width = Math.min(100, (d.bytesTransferred / d.totalBytes) * 100) + '%';
    }
    if (d.direction === 'PUSH') appendPushStatus(msg);
    if (state.role === 'AGENT' && d.direction === 'PUSH' && d.status === 'complete' && d.filename) {
      if (!state.receivedFiles.includes(d.filename)) {
        state.receivedFiles.push(d.filename);
      }
      const detail = {
        filename: d.filename,
        bytes: d.bytesTransferred || d.totalBytes || 0,
        timestamp: d.timestamp || new Date().toISOString(),
        localPath: 'downloads/' + (state.username || '') + '/' + d.filename
      };
      state.receivedFilesDetail.push(detail);
      renderAgentFiles();
    }
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
  state.stagedFiles = [];
  state.receivedFiles = [];
  state.receivedFilesDetail = [];
  state.messages = [];
  if (res.data && res.data.sid && typeof sessionStorage !== 'undefined') {
    sessionStorage.setItem('nm_sid', res.data.sid);
  }
  if (state.role === 'INSPECTOR') {
    showView('inspector');
    updateSessionTag();
    updateTelemetryButton();
    loadNodes();
  } else {
    showView('agent');
    updateAgentTag();
    renderAgentFiles();
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
  state.stagedFiles = [];
  state.receivedFiles = [];
  state.receivedFilesDetail = [];
  state.messages = [];
  state.subscribed = false;
  selectedAgent = null;
  history.clear();
  if (typeof sessionStorage !== 'undefined') sessionStorage.removeItem('nm_sid');
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
  const dp = $('diag-ping-btn');
  if (dp) dp.addEventListener('click', diagPing);
  const dr = $('diag-resolve-btn');
  if (dr) dr.addEventListener('click', diagResolve);
  const ds = $('diag-sysinfo-btn');
  if (ds) ds.addEventListener('click', diagSysinfo);
  const dx = $('diag-exec-btn');
  if (dx) dx.addEventListener('click', diagExec);
  document.querySelectorAll('.ws-tab').forEach((b) => {
    b.addEventListener('click', () => showWorkspaceTab(b.dataset.tab));
  });
  const fu = $('file-upload-btn');
  if (fu) fu.addEventListener('click', handleFileUpload);
  const fp = $('push-btn');
  if (fp) fp.addEventListener('click', handleFilePush);
  const cs = $('chat-send');
  if (cs) cs.addEventListener('click', sendChat);
  const ars = $('agent-reply-send');
  if (ars) ars.addEventListener('click', sendAgentReply);
  loadState();
});