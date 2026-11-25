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
  messages: [],
  knownRecipients: new Set(['admin']),
  recipientAgents: []
};

const history = new Map();
let selectedAgent = null;
let eventSource = null;
let nodesPollTimer = null;
let loadingNodes = false;
let recipientsPollTimer = null;
let loadingRecipients = false;
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

/**
 * Format pesan error dari respons server menjadi teks pengguna yang mudah dibaca.
 * Diberikan rawError (string atau object dengan pesan) serta targetAgent (string ID agent).
 * @param {string|Object} rawError - Pesan error dari server (bisa string atau res.data.message).
 * @param {string} targetAgent - ID agent target (digunakan pada teks jika error merujuk agent).
 * @returns {string} Teks error yang sudah diformat untuk tampilan Web GUI.
 */
function formatWebError(rawError, targetAgent) {
  const msg = String(rawError || '');
  // 1) Agent tidak ditemukan / 404
  if (/404|Agent Tidak Ditemukan/i.test(msg)) {
    return 'Agent "' + escapeText(targetAgent) + '" sedang OFFLINE atau tidak terhubung ke server. Periksa status node pada tabel Systems.';
  }
  // 2) Koneksi terputus saat menunggu respons 502
  if (/502|Terputus Saat Menunggu Respons/i.test(msg)) {
    return 'Koneksi ke agent "' + escapeText(targetAgent) + '" terputus di tengah proses sebelum respons selesai diterima.';
  }
  // 3) Bersihkan kode protokol mentah seperti 400 ERR, 403 ERR sisa alasan
  const cleaned = msg.replace(/^\s*\d{3}\s*/i, '').replace(/\b\d{3}\b/g, '').trim();
  return cleaned || 'Gagal';
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
      loadNodes();
      startNodesPolling();
    } else if (state.role === 'AGENT') {
      showView('agent');
      updateAgentTag();
      renderAgentFiles();
      loadRecipientNodesForAgent();
      startRecipientsPolling();
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
  if (loadingNodes) return;
  loadingNodes = true;
  try {
    const res = await api('/api/nodes');
    if (!res.ok || !res.data) return;
    const fresh = Array.isArray(res.data.nodes) ? res.data.nodes : [];
    const freshIds = new Set(fresh.map((n) => n.agentId));
    const merged = new Map(state.nodes.map((n) => [n.agentId, n]));
    fresh.forEach((n) => {
      const id = n.agentId;
      if (!id) return;
      const existing = merged.get(id);
      if (existing) {
        existing.role = n.role || existing.role;
        existing.location = n.location || existing.location;
        existing.isp = n.isp || existing.isp;
        existing.status = 'ONLINE';
      } else {
        merged.set(id, Object.assign({}, n, { status: 'ONLINE' }));
      }
    });
    state.nodes.forEach((n) => {
      if (!freshIds.has(n.agentId)) n.status = 'OFFLINE';
    });
    state.nodes = Array.from(merged.values());
    renderNodes();
    updateChatTarget();
    updateAgentRecipients();
  } finally {
    loadingNodes = false;
  }
}

/** Jalankan auto-polling node tiap 5 detik saat sesi INSPECTOR aktif. */
function startNodesPolling() {
  stopNodesPolling();
  nodesPollTimer = setInterval(() => {
    if (state.connected && state.role === 'INSPECTOR') loadNodes();
  }, 5000);
}

function stopNodesPolling() {
  if (nodesPollTimer) {
    clearInterval(nodesPollTimer);
    nodesPollTimer = null;
  }
}

/** Jalankan auto-polling daftar penerima tiap 5 detik saat sesi AGENT aktif. */
function startRecipientsPolling() {
  stopRecipientsPolling();
  recipientsPollTimer = setInterval(() => {
    if (state.connected && state.role === 'AGENT') loadRecipientNodesForAgent();
  }, 5000);
}

/** Hentikan auto-polling daftar penerima dan bersihkan timer. */
function stopRecipientsPolling() {
  if (recipientsPollTimer) {
    clearInterval(recipientsPollTimer);
    recipientsPollTimer = null;
  }
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
      createActionsCell(node)
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

/**
 * Pilih node sebagai target grafik Selected Node Telemetry.
 * Baris tabel yang aktif ditandai lewat kelas CSS `.selected`, dan tombol
 * Inspect-nya menyala lewat kelas `.active`.
 *
 * @param {string} agentId - ID node yang dipilih menjadi node aktif.
 */
function selectNodeAgent(agentId) {
  selectedAgent = agentId;
  renderNodes();
  updateSparkline();
}

/**
 * Buka tab workspace, lalu arahkan dropdown target ke node tertentu.
 * Nilai hanya dipasang bila opsi tersedia (misalnya dropdown Diagnostics
 * hanya memuat peran AGENT), sehingga tidak pernah memicu nilai kosong.
 *
 * @param {string} tab - Kunci tab: 'diag', 'files', atau 'comms'.
 * @param {string} selectId - ID elemen <select> yang akan diatur.
 * @param {string} agentId - Nilai opsi tujuan.
 */
function openWorkspaceTabFor(tab, selectId, agentId) {
  showWorkspaceTab(tab);
  const sel = $(selectId);
  if (!sel || !agentId) return;
  const options = Array.prototype.slice.call(sel.options);
  if (!options.some((o) => o.value === agentId)) return;
  sel.value = agentId;
}

/**
 * Buat satu tombol pintasan ringkas (`.btn sm`) untuk sel Actions.
 *
 * @param {string} label - Teks tombol (mis. Inspect, Msg).
 * @param {string} title - Tooltip penjelas fungsi tombol.
 * @param {Function} onClick - Handler klik dengan `stopPropagation` di dalamnya.
 * @returns {HTMLButtonElement} Tombol siap pakai.
 */
function createActionButton(label, title, onClick) {
  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = 'btn sm bordered';
  btn.textContent = label;
  btn.title = title;
  btn.addEventListener('click', onClick);
  return btn;
}

/**
 * Bangun isi sel Actions untuk satu node: Inspect dan Msg.
 * Hanya pintasan yang benar-benar menargetkan node tersebut yang ditampilkan;
 * PING/RESOLVE tidak disertakan karena dijalankan di NetMonitorServer, bukan per-agent.
 * Klik tombol dihentikan propagasinya agar tidak ikut memicu pemilihan baris
 * (kecuali Inspect yang memang bertugas memilih node).
 *
 * @param {Object} node - Data node dari `state.nodes`.
 * @returns {HTMLTableCellElement} Sel tabel Actions berisi dua tombol.
 */
function createActionsCell(node) {
  const td = document.createElement('td');
  const wrap = document.createElement('div');
  wrap.className = 'row-actions';

  const inspect = createActionButton('Inspect', 'Select this node for Selected Node Telemetry', (ev) => {
    ev.stopPropagation();
    selectNodeAgent(node.agentId);
  });
  if (selectedAgent === node.agentId) inspect.classList.add('active');

  const msg = createActionButton('Msg', 'Open Team Comms with this target', (ev) => {
    ev.stopPropagation();
    showWorkspaceTab('comms');
    const sel = $('chat-target');
    if (sel && node.agentId) {
      sel.value = node.agentId;
    }
    const panel = $('ws-comms');
    if (panel) {
      panel.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }
    const input = $('chat-message');
    if (input) {
      input.focus();
    }
  });

  // Node OFFLINE: Inspect tetap aktif agar riwayat telemetri terakhir bisa dilihat;
  // hanya Msg yang dinonaktifkan karena agent tidak dapat menerima pesan.
  if (String(node.status || '').toUpperCase() === 'OFFLINE') {
    msg.disabled = true;
    msg.title = 'Agent sedang OFFLINE';
  }

  wrap.appendChild(inspect);
  wrap.appendChild(msg);
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
  const sorted = state.nodes
    .filter((node) => String(node.role || '').toUpperCase() === 'AGENT')
    .sort((a, b) => {
      const sa = String(a.status || '').toUpperCase();
      const sb = String(b.status || '').toUpperCase();
      const rank = (s) => (s === 'ONLINE' ? 0 : s === 'OFFLINE' ? 1 : 2);
      const ra = rank(sa);
      const rb = rank(sb);
      if (ra !== rb) return ra - rb;
      return String(a.agentId || '').localeCompare(String(b.agentId || ''));
    });
  sorted.forEach((node) => {
    const o = document.createElement('option');
    o.value = node.agentId || '';
    o.textContent = escapeText((node.agentId || '') + ' (' + (node.status || 'ONLINE') + ')');
    sel.appendChild(o);
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
    // Tab Team Comms dibuka: bersihkan penanda pesan belum terbaca.
    const dot = $('comms-unread-dot');
    if (dot) dot.classList.add('hidden');
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
  const sorted = state.nodes
    .filter((node) => String(node.role || '').toUpperCase() === 'AGENT')
    .sort((a, b) => {
      const sa = String(a.status || '').toUpperCase();
      const sb = String(b.status || '').toUpperCase();
      const rank = (s) => (s === 'ONLINE' ? 0 : s === 'OFFLINE' ? 1 : 2);
      const ra = rank(sa);
      const rb = rank(sb);
      if (ra !== rb) return ra - rb;
      return String(a.agentId || '').localeCompare(String(b.agentId || ''));
    });
  sorted.forEach((node) => {
    const o = document.createElement('option');
    o.value = node.agentId || '';
    o.textContent = escapeText((node.agentId || '') + ' (' + (node.status || 'ONLINE') + ')');
    sel.appendChild(o);
  });
  if (current && [...sel.options].some((o) => o.value === current)) {
    sel.value = current;
  }
}

/**
 * Perbarui daftar pilihan <select> Target Agent (admin di posisi pertama).
 * Dilindungi guard `loadingRecipients` agar request tidak bertumpuk ketika
 * polling 5 detik dan refresh instan (focus/mousedown) terjadi bersamaan.
 */
async function updateAgentRecipients() {
  const sel = $('agent-reply-recipient');
  if (!sel) return;
  if (loadingRecipients) return;
  loadingRecipients = true;
  // Pilihan pengguna diambil sekali di awal agar tidak hilang saat render lokal
  // sementara belum memuat username dari server.
  const preferred = sel.value;
  // Dideklarasikan di sini (bukan di dalam try) agar tersedia untuk render lokal
  // maupun sinkronisasi server; deklarasi terlambat di dalam try akan melanggar
  // temporal dead zone dan membuat seluruh fungsi batal dirender.
  const myUsername = state.username || '';
  const sortIds = (ids) => [...ids].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  const render = (ids) => {
    sel.innerHTML = '';
    ids.forEach((id) => {
      const o = document.createElement('option');
      o.value = id || '';
      o.textContent = escapeText(id || '');
      sel.appendChild(o);
    });
    if (preferred && [...sel.options].some((o) => o.value === preferred)) {
      sel.value = preferred;
    } else if (sel.options.length > 0 && sel.options[0].value === 'admin') {
      sel.value = 'admin';
    }
  };
  try {
    // 1) Render cepat dari state lokal (tanpa jaringan)
    const local = new Set(['admin']);
    state.knownRecipients.forEach((r) => {
      if (r && r !== myUsername && !isSelfSenderLabel(r)) local.add(r);
    });
    state.recipientAgents.forEach((id) => {
      if (id && id !== myUsername && !isSelfSenderLabel(id)) local.add(id);
    });
    state.nodes.forEach((n) => {
      const id = n && n.agentId;
      if (id && id !== myUsername && !isSelfSenderLabel(id)) local.add(id);
    });
    render(sortIds(local));
    // 2) Sinkronkan dengan sesi aktif terbaru dari server
    const res = await api('/api/recipients');
    if (!res.ok || !res.data) return;
    const serverList = extractRecipients(res.data);
    // Simpan daftar sesi aktif dari server ke state.recipientAgents
    // (tanpa username sendiri dan tanpa label pengirim "YOU -> ...").
    const fromServer = serverList.filter((id) => id && id !== myUsername && !isSelfSenderLabel(id));
    state.recipientAgents = fromServer;
    const merged = new Set(['admin']);
    fromServer.forEach((id) => merged.add(id));
    state.knownRecipients.forEach((r) => {
      if (r && r !== myUsername && !isSelfSenderLabel(r)) merged.add(r);
    });
    state.nodes.forEach((n) => {
      const id = n && n.agentId;
      if (id && id !== myUsername && !isSelfSenderLabel(id)) merged.add(id);
    });
    render(sortIds(merged));
  } catch (e) {
    // Kegagalan jaringan: biarkan daftar lokal yang sudah dirender
  } finally {
    loadingRecipients = false;
  }
}

/**
 * Ekstrak daftar username dari respons `GET /api/recipients`.
 * Properti `recipients` (array string) dikirim oleh `WebBridgeServer`.
 *
 * @param {Object} data - Body JSON respons.
 * @returns {string[]} Daftar username; kosong bila format tidak dikenali.
 */
function extractRecipients(data) {
  if (!data) return [];
  if (Array.isArray(data)) return data.slice();
  if (Array.isArray(data.recipients)) return data.recipients.slice();
  return [];
}

/**
 * Perbarui daftar penerima dari server dan masukkan ke `state.recipientAgents`.
 * Menggantikan pemanggilan `/api/nodes` yang menolak peran AGENT (403).
 */
async function loadRecipientNodesForAgent() {
  if (state.role !== 'AGENT') return;
  await updateAgentRecipients();
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
    const token = (typeof sessionStorage !== 'undefined') ? sessionStorage.getItem('nm_sid') : null;
    const headers = { 'Content-Type': 'application/octet-stream', 'X-Filename': file.name };
    if (token) headers['X-Session-Token'] = token;
    const res = await fetch('/api/files/upload', {
      method: 'POST',
      headers: headers,
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
    appendPushStatus('Push gagal: ' + formatWebError(res.data && res.data.message, agent));
  }
}

function appendDiagnosticsLog(text, targetId) {
  const out = $(targetId || 'diag-exec-output');
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
    lines.push('SYSINFO gagal: ' + formatWebError(res.data && res.data.message, agent));
  }
  if (out) out.textContent = lines.join('\n');
}

async function diagExec() {
  const sel = $('diag-agent');
  const agent = sel ? sel.value : '';
  const command = $('diag-exec-cmd').value.trim();
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
    if (!res.data.line.startsWith('200 EXEC ')) {
      appendDiagnosticsLog('EXEC gagal: respons server tidak sesuai format EXEC (' + formatWebError(res.data.line, agent) + ')');
      return;
    }
    let text = res.data.line.replace(/^200 EXEC [^ ]+ /, '').replace(/\\n/g, '\n');
    appendDiagnosticsLog(text || '(kosong)');
  } else {
    appendDiagnosticsLog('EXEC gagal: ' + formatWebError(res.data && res.data.message, agent));
  }
}

async function diagPing() {
  const host = $('diag-host').value.trim();
  if (!host) return;
  const res = await api('/api/diagnostics/ping', { method: 'POST', body: { host } });
  appendDiagnosticsLog('PING ' + escapeText(host) + ' -> ' + (res.data && res.data.line || (res.data && res.data.message || 'gagal')), 'diag-server-output');
}

async function diagResolve() {
  const domain = $('diag-domain').value.trim();
  if (!domain) return;
  const res = await api('/api/diagnostics/resolve', { method: 'POST', body: { domain } });
  appendDiagnosticsLog('RESOLVE ' + escapeText(domain) + ' -> ' + (res.data && res.data.line || (res.data && res.data.message || 'gagal')), 'diag-server-output');
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

/**
 * Cek apakah label pengirim merupakan penanda pesan keluar ("YOU -> ...")
 * atau username sesi sendiri. Label seperti ini hanya untuk tampilan feed,
 * bukan kandidat penerima dropdown Target.
 *
 * @param {string} sender - Nilai `sender` dari entri chat.
 * @returns {boolean} true bila label bukan penerima yang sah.
 */
function isSelfSenderLabel(sender) {
  if (!sender) return false;
  const s = String(sender).trim();
  if (s.toUpperCase().startsWith('YOU ->') || s.toUpperCase().startsWith('YOU→')) {
    return true;
  }
  const me = state.username ? String(state.username).trim() : '';
  return !!me && s === me;
}

function appendAgentMessage(d) {
  const feed = $('agent-msg-feed');
  if (!feed) return;
  // Jangan masukkan label pengiriman sendiri ("YOU -> ...") ke daftar penerima.
  if (d && d.sender && !isSelfSenderLabel(d.sender)) {
    state.knownRecipients.add(d.sender);
  }
  updateAgentRecipients();
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
  const sorted = state.nodes
    .filter((node) => String(node.role || '').toUpperCase() === 'AGENT')
    .sort((a, b) => {
      const sa = String(a.status || '').toUpperCase();
      const sb = String(b.status || '').toUpperCase();
      const rank = (s) => (s === 'ONLINE' ? 0 : s === 'OFFLINE' ? 1 : 2);
      const ra = rank(sa);
      const rb = rank(sb);
      if (ra !== rb) return ra - rb;
      return String(a.agentId || '').localeCompare(String(b.agentId || ''));
    });
  sorted.forEach((node) => {
    const o = document.createElement('option');
    o.value = node.agentId || '';
    o.textContent = escapeText((node.agentId || '') + ' (' + (node.status || 'ONLINE') + ')');
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
    setInlineStatus('agent-reply-status', true, '');
  } else {
    const reason = formatWebError(res.data && res.data.message, target);
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
    setInlineStatus('chat-status', true, '');
  } else {
    const reason = formatWebError(res.data && res.data.message, target);
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
    if (d && d.sender && !isSelfSenderLabel(d.sender)) state.knownRecipients.add(d.sender);
    if (state.role === 'AGENT') updateAgentRecipients();
    state.messages.push(d);
    // Tampilkan penanda pesan baru bila pengirim bukan user sendiri dan tab aktif bukan Team Comms.
    const unreadDot = $('comms-unread-dot');
    if (unreadDot) {
      const comms = $('ws-comms');
      const commsActive = comms ? !comms.classList.contains('hidden') : true;
      const sender = d && d.sender ? String(d.sender).trim() : '';
      const me = state.username ? String(state.username).trim() : '';
      const senderIsSelf = !sender || sender === me || isSelfSenderLabel(sender);
      if (!commsActive && !senderIsSelf) {
        unreadDot.classList.remove('hidden');
      }
    }
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
    stopNodesPolling();
    stopRecipientsPolling();
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
  state.knownRecipients = new Set(['admin']);
  state.recipientAgents = [];
  if (res.data && res.data.sid && typeof sessionStorage !== 'undefined') {
    sessionStorage.setItem('nm_sid', res.data.sid);
  }
  if (state.role === 'INSPECTOR') {
    showView('inspector');
    updateSessionTag();
    updateTelemetryButton();
    loadNodes();
    startNodesPolling();
  } else {
    showView('agent');
    updateAgentTag();
    renderAgentFiles();
    loadRecipientNodesForAgent();
    startRecipientsPolling();
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
  stopNodesPolling();
  stopRecipientsPolling();
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

  // Dukungan Enter keypad pada input PING / RESOLVE
  const hostInput = $('diag-host');
  if (hostInput) hostInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') { e.preventDefault(); diagPing(); }
  });
  const domainInput = $('diag-domain');
  if (domainInput) domainInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') { e.preventDefault(); diagResolve(); }
  });

  // Dukinkan Enter pada input Remote Command
  const execInput = $('diag-exec-cmd');
  if (execInput) execInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') { e.preventDefault(); diagExec(); }
  });

  document.querySelectorAll('.ws-tab').forEach((b) => {
    b.addEventListener('click', () => showWorkspaceTab(b.dataset.tab));
  });
  const fu = $('file-upload-btn');
  if (fu) fu.addEventListener('click', handleFileUpload);
  const fp = $('push-btn');
  if (fp) fp.addEventListener('click', handleFilePush);
  const cs = $('chat-send');
  if (cs) cs.addEventListener('click', sendChat);
  const cms = $('chat-message');
  if (cms) cms.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendChat(); }
  });
  const ars = $('agent-reply-send');
  if (ars) ars.addEventListener('click', sendAgentReply);
  const arr = $('agent-reply-recipient');
  if (arr) {
    // Refresh instan saat dropdown dibuka (focus / mousedown) agar node yang
    // baru login langsung muncul tanpa menunggu siklus polling 5 detik.
    arr.addEventListener('focus', () => loadRecipientNodesForAgent());
    arr.addEventListener('mousedown', () => loadRecipientNodesForAgent());
  }
  const ams = $('agent-reply-message');
  if (ams) ams.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendAgentReply(); }
  });
  loadState();
});