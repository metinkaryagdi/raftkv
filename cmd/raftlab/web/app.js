// raftkv lab dashboard: polls /api/nodes + each node's proxied /status and
// /debug/log for current state (idempotent polling), and streams /ws/events
// for live Raft protocol events over WebSocket.

const POLL_MS = 500;
const ROLE_NAMES = ["Follower", "Candidate", "Leader"]; // Raft Role enum: 0=Follower, 1=Candidate, 2=Leader

let nodes = [];               // last /api/nodes result
let statusByID = new Map();   // id -> parsed /status
let logByID = new Map();      // id -> { lastIncludedIndex, entries }
let currentFilter = "all";
let wsConnection = null;

function setConnectionPill(state) {
  const el = document.getElementById("connection-status");
  const textEl = el.querySelector(".status-text");
  el.className = "pill " + (state === "ok" ? "pill-ok" : state === "bad" ? "pill-bad" : "pill-muted");
  textEl.textContent = state === "ok" ? "live" : state === "bad" ? "disconnected" : "connecting…";
}

async function pollOnce() {
  try {
    const res = await fetch("/api/nodes");
    if (!res.ok) throw new Error("nodes fetch failed");
    nodes = await res.json();
    setConnectionPill("ok");
  } catch {
    setConnectionPill("bad");
    nodes = [];
    statusByID.clear();
    logByID.clear();
    updateClusterStats();
    renderTopology();
    renderControls();
    renderLogMatrix();
    return;
  }

  const validIDs = new Set(nodes.map(n => n.ID));
  for (const id of statusByID.keys()) {
    if (!validIDs.has(id)) statusByID.delete(id);
  }
  for (const id of logByID.keys()) {
    if (!validIDs.has(id)) logByID.delete(id);
  }

  await Promise.all(nodes.map(async (n) => {
    if (!n.Addr) {
      statusByID.delete(n.ID);
      logByID.delete(n.ID);
      return;
    }
    try {
      const [stRes, logRes] = await Promise.all([
        fetch(`/api/nodes/${encodeURIComponent(n.ID)}/status`),
        fetch(`/api/nodes/${encodeURIComponent(n.ID)}/log`),
      ]);
      if (stRes.ok) {
        statusByID.set(n.ID, await stRes.json());
      } else {
        statusByID.delete(n.ID);
      }

      if (logRes.ok) {
        logByID.set(n.ID, await logRes.json());
      } else {
        logByID.delete(n.ID);
      }
    } catch {
      statusByID.delete(n.ID);
      logByID.delete(n.ID);
    }
  }));

  updateClusterStats();
  renderTopology();
  renderControls();
  renderLogMatrix();
}

function updateClusterStats() {
  const activeNodesCount = nodes.filter(n => n.Ready && statusByID.has(n.ID)).length;
  document.getElementById("stat-nodes-count").textContent = `${activeNodesCount} / ${nodes.length}`;

  let currentLeader = "None";
  let maxTerm = 0;
  let maxLogIndex = 0;

  for (const [id, st] of statusByID.entries()) {
    if (st) {
      if (st.role === "Leader" || st.role === 2) {
        currentLeader = id;
      }
      if (st.term > maxTerm) maxTerm = st.term;
      if (st.commitIndex > maxLogIndex) maxLogIndex = st.commitIndex;
    }
  }

  logByID.forEach((log) => {
    (log.entries || []).forEach(e => {
      if (e.Index > maxLogIndex) maxLogIndex = e.Index;
    });
  });

  const leaderEl = document.getElementById("stat-leader");
  leaderEl.textContent = currentLeader;
  leaderEl.className = "stat-value " + (currentLeader !== "None" ? "text-leader" : "");

  document.getElementById("stat-term").textContent = maxTerm;
  document.getElementById("stat-log-index").textContent = maxLogIndex;
}

// --- Topology (SVG Graph) ---

function getRoleName(st) {
  if (!st) return null;
  if (typeof st.role === "string") return st.role;
  if (typeof st.role === "number") return ROLE_NAMES[st.role] || "Follower";
  return "Follower";
}

function roleClass(roleName) {
  if (roleName === "Leader") return "leader";
  if (roleName === "Candidate") return "candidate";
  return "follower";
}

function renderTopology() {
  const svg = document.getElementById("topology");
  const w = 600, h = 320, cx = w / 2, cy = h / 2, r = Math.min(w, h) / 2 - 50;
  const n = nodes.length || 1;
  const coords = [];

  nodes.forEach((node, i) => {
    const angle = (2 * Math.PI * i) / n - Math.PI / 2;
    coords.push({
      x: cx + r * Math.cos(angle),
      y: cy + r * Math.sin(angle),
      id: node.ID,
      ready: node.Ready,
    });
  });

  const parts = [];

  // Mesh connection lines
  for (let i = 0; i < coords.length; i++) {
    for (let j = i + 1; j < coords.length; j++) {
      const c1 = coords[i], c2 = coords[j];
      const st1 = statusByID.get(c1.id), st2 = statusByID.get(c2.id);
      const activeLink = (c1.ready && st1) && (c2.ready && st2);
      parts.push(`
        <line x1="${c1.x}" y1="${c1.y}" x2="${c2.x}" y2="${c2.y}"
          class="topology-link ${activeLink ? "active" : ""}" />`);
    }
  }

  // Node circles & labels
  coords.forEach((c) => {
    const st = statusByID.get(c.id);
    const reachable = c.ready && !!st;
    const roleName = reachable ? getRoleName(st) : null;
    const cls = reachable ? roleClass(roleName) : "unreachable";
    const term = reachable ? st.term : "?";

    const isLeader = cls === "leader";

    if (isLeader) {
      parts.push(`
        <circle cx="${c.x}" cy="${c.y}" r="28" fill="none" stroke="var(--leader)" class="node-pulse" />`);
    }

    parts.push(`
      <g class="node-g" data-id="${escapeAttr(c.id)}">
        <circle cx="${c.x}" cy="${c.y}" r="26" class="node-circle node-${cls}"
          fill="var(--${cls})" ${reachable ? "" : 'stroke="var(--unreachable)" stroke-width="2" stroke-dasharray="4 3"'} opacity="${reachable ? 1 : 0.4}"/>
        <text x="${c.x}" y="${c.y - 2}" text-anchor="middle" font-size="12" font-weight="700" fill="#080c14" font-family="var(--font-mono)">${escapeText(c.id)}</text>
        <text x="${c.x}" y="${c.y + 12}" text-anchor="middle" font-size="9" font-weight="600" fill="#080c14">T:${term}</text>
      </g>`);
  });

  svg.innerHTML = parts.join("");
}

// --- Node Controls (with Event Delegation) ---

let activeControlState = {}; // id_action -> boolean

function renderControls() {
  const container = document.getElementById("node-controls");
  if (nodes.length === 0) {
    container.innerHTML = '<p class="hint">No nodes discovered yet.</p>';
    return;
  }

  const html = nodes.map((node) => {
    const st = statusByID.get(node.ID);
    const reachable = node.Ready && !!st;
    const roleName = reachable ? getRoleName(st) : "unreachable";
    const badgeCls = `badge-${roleClass(roleName)}`;

    const killPending = activeControlState[`${node.ID}_kill`]? "disabled" : "";
    const isoPending = activeControlState[`${node.ID}_isolate`]? "disabled" : "";
    const healPending = activeControlState[`${node.ID}_heal`]? "disabled" : "";

    return `
      <div class="node-row" data-id="${escapeAttr(node.ID)}">
        <div class="node-meta">
          <span class="node-id">${escapeText(node.ID)}</span>
          <span class="node-badge ${badgeCls}">${escapeText(roleName)}</span>
        </div>
        <div class="node-actions">
          <button class="btn btn-xs btn-danger-outline" data-action="kill" ${killPending} title="Kill container/pod">Kill</button>
          <button class="btn btn-xs btn-warning-outline" data-action="isolate" ${isoPending} title="Disconnect network">Isolate</button>
          <button class="btn btn-xs btn-success-outline" data-action="heal" ${healPending} title="Reconnect network">Heal</button>
        </div>
      </div>`;
  }).join("");

  container.innerHTML = html;
}

function initNodeControlsDelegation() {
  const container = document.getElementById("node-controls");
  const resultEl = document.getElementById("membership-result");

  container.addEventListener("click", async (ev) => {
    const btn = ev.target.closest("button[data-action]");
    if (!btn || btn.disabled) return;

    const row = btn.closest(".node-row");
    if (!row) return;

    const id = row.dataset.id;
    const action = btn.dataset.action;
    const stateKey = `${id}_${action}`;

    activeControlState[stateKey] = true;
    btn.disabled = true;
    const origText = btn.textContent;
    btn.textContent = "…";
    resultEl.textContent = `Sending ${action} for node ${id}…`;

    try {
      const res = await fetch(`/api/orchestrator/${action}/${encodeURIComponent(id)}`, { method: "POST" });
      const text = await res.text();
      if (!res.ok) {
        resultEl.textContent = `Orchestrator Error [${res.status}]: ${text}`;
      } else {
        resultEl.textContent = `Success: ${action} executed for ${id}`;
      }
    } catch (err) {
      resultEl.textContent = `Network Error: ${err.message}`;
    } finally {
      delete activeControlState[stateKey];
      btn.disabled = false;
      btn.textContent = origText;
      pollOnce();
    }
  });
}

// --- Log Matrix ---

function renderLogMatrix() {
  const el = document.getElementById("log-matrix");
  if (nodes.length === 0) {
    el.innerHTML = '<p class="hint">No cluster nodes discovered yet.</p>';
    return;
  }

  let maxIndex = 0;
  logByID.forEach((log) => {
    (log.entries || []).forEach((e) => { if (e.Index > maxIndex) maxIndex = e.Index; });
  });

  const cols = [];
  for (let i = 1; i <= maxIndex; i++) cols.push(i);

  let html = "<table><thead><tr><th></th>";
  cols.forEach((i) => { html += `<th>${i}</th>`; });
  html += "</tr></thead><tbody>";

  nodes.forEach((node) => {
    const log = logByID.get(node.ID);
    const st = statusByID.get(node.ID);
    const lastIncluded = log ? (log.lastIncludedIndex || 0) : 0;
    const commitIndex = st ? (st.commitIndex || 0) : 0;
    const entryByIndex = new Map((log && log.entries || []).map((e) => [e.Index, e]));

    html += `<tr><td class="row-label">${escapeText(node.ID)}</td>`;
    cols.forEach((i) => {
      let cls = "absent";
      let title = `Node ${node.ID} | Index ${i}: absent`;
      if (i <= lastIncluded) {
        cls = "compacted";
        title = `Node ${node.ID} | Index ${i}: compacted into snapshot`;
      } else if (entryByIndex.has(i)) {
        const entry = entryByIndex.get(i);
        cls = i <= commitIndex ? "committed" : "uncommitted";
        const op = entry.Command ? entry.Command.Op : "noop";
        const key = entry.Command && entry.Command.Key ? ` k:${entry.Command.Key}` : "";
        title = `Node ${node.ID} | Index ${i}: ${cls.toUpperCase()} [Term ${entry.Term}] (${op}${key})`;
      }
      html += `<td><div class="cellbox cell ${cls}" title="${escapeAttr(title)}"></div></td>`;
    });
    html += "</tr>";
  });

  html += "</tbody></table>";
  el.innerHTML = html;
}

// --- Live Event Feed (WebSocket) ---

function connectEvents() {
  if (wsConnection) return;
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  const ws = new WebSocket(`${proto}//${location.host}/ws/events`);
  wsConnection = ws;

  ws.onmessage = (msg) => {
    try {
      appendEvent(JSON.parse(msg.data));
    } catch {
      // ignore
    }
  };

  ws.onclose = () => {
    wsConnection = null;
    setTimeout(connectEvents, 2000);
  };

  ws.onerror = () => {
    if (ws) ws.close();
  };
}

function appendEvent(msg) {
  const feed = document.getElementById("event-feed");
  const e = msg.event || {};
  const kind = e.Kind || "";
  const roleName = typeof e.Role === "number" ? (ROLE_NAMES[e.Role] || e.Role) : (e.Role || "");

  if (currentFilter !== "all") {
    if (currentFilter === "leader" && !kind.includes("leader")) return;
    if (currentFilter === "election" && !kind.includes("election") && !kind.includes("vote")) return;
    if (currentFilter === "commit" && !kind.includes("commit") && !kind.includes("conf_change")) return;
  }

  const line = document.createElement("div");
  line.className = "event-line";
  const ts = new Date().toLocaleTimeString();
  const parts = [
    `<span class="ts">${ts}</span>`,
    `<span class="node-tag">[${escapeText(msg.nodeId)}]</span>`,
    `<span class="k-${escapeAttr(kind)}">${escapeText(kind)}</span>`,
    `term=${e.Term} role=${roleName}`
  ];
  if (e.Peer) parts.push(`peer=${escapeText(e.Peer)}`);
  if (e.Info) parts.push(`(${escapeText(e.Info)})`);
  line.innerHTML = parts.join(" ");

  feed.insertBefore(line, feed.firstChild);
  while (feed.children.length > 250) feed.removeChild(feed.lastChild);
}

// Event Feed Filters
document.querySelectorAll(".filter-btn").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".filter-btn").forEach(b => b.classList.remove("active"));
    btn.classList.add("active");
    currentFilter = btn.dataset.filter;
  });
});

document.getElementById("btn-clear-feed").addEventListener("click", () => {
  document.getElementById("event-feed").innerHTML = "";
});

// --- Membership Form ---

function initMembershipForm() {
  const form = document.getElementById("membership-form");
  const result = document.getElementById("membership-result");

  form.addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const action = ev.submitter ? ev.submitter.dataset.action : "add";
    const id = document.getElementById("member-id").value.trim();
    const addr = document.getElementById("member-addr").value.trim();
    if (!id) {
      result.textContent = "Error: Node ID is required.";
      return;
    }

    const path = action === "add" ? "/api/cluster/add-server" : "/api/cluster/remove-server";
    const body = action === "add" ? { id, raftAddr: addr } : { id };

    result.textContent = "Proposing configuration change to leader…";
    try {
      const res = await fetch(path, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const text = await res.text();
      result.textContent = `Response [${res.status}]: ${text}`;
    } catch (err) {
      result.textContent = "Network Error: " + err.message;
    } finally {
      pollOnce();
    }
  });
}

// --- KV Store Playground ---

function initKVPlayground() {
  const outputEl = document.getElementById("kv-output");
  const keyInput = document.getElementById("kv-key");
  const valInput = document.getElementById("kv-val");

  const runKV = async (method) => {
    const key = keyInput.value.trim();
    const val = valInput.value;
    if (!key) {
      outputEl.textContent = "Error: Key is required.";
      return;
    }

    outputEl.textContent = `Executing ${method} /kv/${key}...`;
    try {
      const opts = { method };
      if (method === "PUT") {
        opts.body = val;
      }
      const res = await fetch(`/api/kv/${encodeURIComponent(key)}`, opts);
      const text = await res.text();
      let formatted = text;
      try {
        formatted = JSON.stringify(JSON.parse(text), null, 2);
      } catch {}
      outputEl.textContent = `HTTP ${res.status} ${res.statusText}\n${formatted}`;
    } catch (err) {
      outputEl.textContent = "Error: " + err.message;
    } finally {
      pollOnce();
    }
  };

  document.getElementById("btn-kv-set").addEventListener("click", () => runKV("PUT"));
  document.getElementById("btn-kv-get").addEventListener("click", () => runKV("GET"));
  document.getElementById("btn-kv-del").addEventListener("click", () => runKV("DELETE"));
}

// --- Helper Utilities ---

function escapeText(s) {
  return String(s ?? "").replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]));
}
function escapeAttr(s) {
  return escapeText(s).replace(/"/g, "&quot;");
}

// --- Init ---

async function pollLoop() {
  await pollOnce();
  setTimeout(pollLoop, POLL_MS);
}

initNodeControlsDelegation();
initMembershipForm();
initKVPlayground();
connectEvents();
pollLoop();
