// raftkv lab dashboard: polls /api/nodes + each node's proxied /status and
// /debug/log for current state (simple, idempotent, easy to reason about),
// and streams /ws/events for "what just happened" (pure push, no polling) —
// see internal/lab's doc comment for why these are split this way.

const POLL_MS = 400;
const ROLE_NAMES = ["Follower", "Candidate", "Leader"]; // raft.Role's int encoding

let nodes = [];               // last /api/nodes result
let statusByID = new Map();   // id -> parsed /status
let logByID = new Map();      // id -> { lastIncludedIndex, entries }

function setConnectionPill(state) {
  const el = document.getElementById("connection-status");
  el.className = "pill " + (state === "ok" ? "pill-ok" : state === "bad" ? "pill-bad" : "pill-muted");
  el.textContent = state === "ok" ? "live" : state === "bad" ? "disconnected" : "connecting…";
}

async function pollOnce() {
  try {
    const res = await fetch("/api/nodes");
    nodes = await res.json();
    setConnectionPill("ok");
  } catch {
    setConnectionPill("bad");
    return;
  }

  await Promise.all(nodes.map(async (n) => {
    if (!n.Addr) return;
    try {
      const [stRes, logRes] = await Promise.all([
        fetch(`/api/nodes/${encodeURIComponent(n.ID)}/status`),
        fetch(`/api/nodes/${encodeURIComponent(n.ID)}/log`),
      ]);
      if (stRes.ok) statusByID.set(n.ID, await stRes.json());
      if (logRes.ok) logByID.set(n.ID, await logRes.json());
    } catch {
      // Transient fetch failure for one node shouldn't blank the whole dashboard.
    }
  }));

  renderTopology();
  renderControls();
  renderLogMatrix();
}

// --- Topology (SVG) ---

function roleClass(roleName) {
  if (roleName === "Leader") return "leader";
  if (roleName === "Candidate") return "candidate";
  return "follower";
}

function renderTopology() {
  const svg = document.getElementById("topology");
  const w = 600, h = 300, cx = w / 2, cy = h / 2, r = Math.min(w, h) / 2 - 60;
  const n = nodes.length || 1;
  const parts = [];

  nodes.forEach((node, i) => {
    const angle = (2 * Math.PI * i) / n - Math.PI / 2;
    const x = cx + r * Math.cos(angle);
    const y = cy + r * Math.sin(angle);
    const st = statusByID.get(node.ID);
    const reachable = node.Ready && st;
    const roleName = reachable ? (st.role || ROLE_NAMES[0]) : null;
    const cls = reachable ? roleClass(roleName) : "unreachable";
    const term = reachable ? st.term : "?";

    parts.push(`
      <g class="node-g" data-id="${escapeAttr(node.ID)}">
        <circle cx="${x}" cy="${y}" r="26" class="node-circle node-${cls}"
          fill="var(--${cls})" ${reachable ? "" : 'stroke="var(--unreachable)" stroke-width="2" stroke-dasharray="4 3"'} opacity="${reachable ? 1 : 0.35}"/>
        <text x="${x}" y="${y - 2}" text-anchor="middle" font-size="12" font-weight="600" fill="#0b1420">${escapeText(node.ID)}</text>
        <text x="${x}" y="${y + 12}" text-anchor="middle" font-size="9" fill="#0b1420">term ${term}</text>
      </g>`);
  });

  svg.innerHTML = parts.join("");
}

// --- Controls ---

function renderControls() {
  const container = document.getElementById("node-controls");
  container.innerHTML = nodes.map((node) => {
    const st = statusByID.get(node.ID);
    const roleName = node.Ready && st ? st.role : "unreachable";
    return `
      <div class="node-row" data-id="${escapeAttr(node.ID)}">
        <span class="node-id">${escapeText(node.ID)}</span>
        <span class="node-role">${escapeText(roleName)}</span>
        <button data-action="kill">Kill</button>
        <button data-action="isolate">Isolate</button>
        <button data-action="heal">Heal</button>
      </div>`;
  }).join("");

  container.querySelectorAll("button[data-action]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const id = btn.closest(".node-row").dataset.id;
      const action = btn.dataset.action;
      btn.disabled = true;
      try {
        await fetch(`/api/orchestrator/${action}/${encodeURIComponent(id)}`, { method: "POST" });
      } finally {
        btn.disabled = false;
      }
    });
  });
}

// --- Log matrix ---

function renderLogMatrix() {
  const el = document.getElementById("log-matrix");
  if (nodes.length === 0) {
    el.innerHTML = '<p class="hint">No nodes discovered yet.</p>';
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
      let title = `index ${i}: absent`;
      if (i <= lastIncluded) {
        cls = "compacted";
        title = `index ${i}: compacted into a snapshot`;
      } else if (entryByIndex.has(i)) {
        const entry = entryByIndex.get(i);
        cls = i <= commitIndex ? "committed" : "uncommitted";
        title = `index ${i}: ${cls} (${entry.Command ? entry.Command.Op : "?"})`;
      }
      html += `<td><div class="cellbox cell ${cls}" title="${escapeAttr(title)}"></div></td>`;
    });
    html += "</tr>";
  });

  html += "</tbody></table>";
  el.innerHTML = html;
}

// --- Live event feed (WebSocket) ---

function connectEvents() {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  const ws = new WebSocket(`${proto}//${location.host}/ws/events`);
  ws.onmessage = (msg) => {
    try {
      appendEvent(JSON.parse(msg.data));
    } catch {
      // ignore malformed frames
    }
  };
  ws.onclose = () => setTimeout(connectEvents, 1500); // reconnect
  ws.onerror = () => ws.close();
}

function appendEvent(msg) {
  const feed = document.getElementById("event-feed");
  const e = msg.event || {};
  const line = document.createElement("div");
  line.className = "event-line";
  const ts = new Date().toLocaleTimeString();
  const parts = [`<span class="ts">${ts}</span>`, `<b>${escapeText(msg.nodeId)}</b>`,
    `<span class="k-${escapeAttr(e.Kind)}">${escapeText(e.Kind)}</span>`,
    `term=${e.Term} role=${ROLE_NAMES[e.Role] ?? e.Role}`];
  if (e.Peer) parts.push(`peer=${escapeText(e.Peer)}`);
  if (e.Info) parts.push(`(${escapeText(e.Info)})`);
  line.innerHTML = parts.join(" ");
  feed.appendChild(line);
  while (feed.children.length > 300) feed.removeChild(feed.firstChild);
}

// --- Membership form ---

function initMembershipForm() {
  const form = document.getElementById("membership-form");
  const result = document.getElementById("membership-result");
  form.addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const action = ev.submitter ? ev.submitter.dataset.action : "add";
    const id = document.getElementById("member-id").value.trim();
    const addr = document.getElementById("member-addr").value.trim();
    if (!id) return;
    const path = action === "add" ? "/api/cluster/add-server" : "/api/cluster/remove-server";
    const body = action === "add" ? { id, raftAddr: addr } : { id };
    result.textContent = "working…";
    try {
      const res = await fetch(path, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const text = await res.text();
      result.textContent = `${res.status}: ${text}`;
    } catch (err) {
      result.textContent = "error: " + err;
    }
  });
}

// --- utils ---

function escapeText(s) {
  return String(s ?? "").replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]));
}
function escapeAttr(s) {
  return escapeText(s).replace(/"/g, "&quot;");
}

// --- init ---

// A self-scheduling loop (setTimeout after each poll completes), not
// setInterval: setInterval fires on a fixed clock regardless of whether the
// previous async pollOnce() has finished, and since each poll issues 1 + 2*N
// HTTP requests (N = node count) through the lab's proxy, a slow round trip
// lets polls pile up and overlap indefinitely — observed firsthand as the
// browser's connection pool exhausting itself (net::ERR_INSUFFICIENT_RESOURCES)
// after only a few seconds. Scheduling the next poll only once the current one
// resolves guarantees at most one in flight at a time.
async function pollLoop() {
  await pollOnce();
  setTimeout(pollLoop, POLL_MS);
}

initMembershipForm();
connectEvents();
pollLoop();
