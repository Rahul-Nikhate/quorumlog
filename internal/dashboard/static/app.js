// QuorumLog dashboard. Vanilla JS, no dependencies, same-origin API only
// (contract: docs/SCOPE.md). All server data is rendered with textContent,
// never innerHTML.
"use strict";

(() => {
  // ---------------------------------------------------------------- helpers

  const $ = (id) => document.getElementById(id);
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  const enc = encodeURIComponent;
  const isNum = (v) => typeof v === "number" && Number.isFinite(v);
  const fmtNum = (v) => (isNum(v) ? String(v) : "–");
  const isInternal = (name) => typeof name === "string" && name.startsWith("__");
  const TOPIC_RE = /^[A-Za-z0-9._-]+$/;

  // el("td", {className: "num"}, "42") -> <td class="num">42</td>
  function el(tag, props, ...children) {
    const n = document.createElement(tag);
    if (props) {
      for (const [k, v] of Object.entries(props)) {
        if (v === undefined || v === null || v === false) continue;
        if (k === "dataset") Object.assign(n.dataset, v);
        else if (k in n && typeof v !== "string") n[k] = v;
        else if (k === "className" || k === "textContent" || k === "title") n[k] = v;
        else n.setAttribute(k, v === true ? "" : String(v));
      }
    }
    for (const c of children.flat()) {
      if (c === undefined || c === null || c === false) continue;
      n.append(c instanceof Node ? c : String(c));
    }
    return n;
  }

  class ApiError extends Error {
    constructor(status, code, message) {
      super(message);
      this.status = status;
      this.code = code;
    }
  }

  // Codes after which a write may or may not have been committed.
  const UNKNOWN_OUTCOME = new Set(["TIMEOUT", "CLIENT_TIMEOUT", "NETWORK"]);

  // api performs one JSON request. Errors become ApiError with the server's
  // {"error","code"} when present, or a synthetic code (NETWORK,
  // CLIENT_TIMEOUT, ABORTED, HTTP_<status>) otherwise.
  async function api(method, path, body, opts = {}) {
    const timeoutMs = opts.timeoutMs ?? 10000;
    const ctrl = new AbortController();
    let timedOut = false;
    const timer = setTimeout(() => { timedOut = true; ctrl.abort(); }, timeoutMs);
    const onAbort = () => ctrl.abort();
    if (opts.signal) {
      if (opts.signal.aborted) ctrl.abort();
      else opts.signal.addEventListener("abort", onAbort, { once: true });
    }
    let res, text;
    const t0 = performance.now();
    try {
      res = await fetch(path, {
        method,
        headers: body === undefined ? { Accept: "application/json" } : { Accept: "application/json", "Content-Type": "application/json" },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: ctrl.signal,
        cache: "no-store",
      });
      text = await res.text();
    } catch (e) {
      if (timedOut) throw new ApiError(0, "CLIENT_TIMEOUT", `no response within ${timeoutMs} ms`);
      if (opts.signal && opts.signal.aborted) throw new ApiError(0, "ABORTED", "request cancelled");
      throw new ApiError(0, "NETWORK", "network error: " + (e && e.message ? e.message : String(e)));
    } finally {
      clearTimeout(timer);
      if (opts.signal) opts.signal.removeEventListener("abort", onAbort);
    }
    let data = null;
    if (text) {
      try { data = JSON.parse(text); } catch { data = null; }
    }
    if (!res.ok) {
      const code = data && typeof data.code === "string" && data.code ? data.code : `HTTP_${res.status}`;
      const msg = data && typeof data.error === "string" ? data.error : (text || res.statusText || "").trim().slice(0, 200);
      throw new ApiError(res.status, code, msg);
    }
    return { data: data ?? {}, status: res.status, headers: res.headers, ms: Math.round(performance.now() - t0) };
  }

  // errNode renders an error as "CODE message (HTTP 503)".
  function errNode(e) {
    if (e instanceof ApiError) {
      return el("span", null,
        el("span", { className: "err-code" }, e.code), " ",
        e.message || "",
        e.status ? ` (HTTP ${e.status})` : "");
    }
    return el("span", null, String(e && e.message ? e.message : e));
  }
  const errText = (e) => (e instanceof ApiError ? `${e.code}${e.message ? ": " + e.message : ""}` : String(e && e.message ? e.message : e));

  function setResult(node, ok, ...content) {
    node.className = "result " + (ok === true ? "ok" : ok === false ? "err" : "");
    node.replaceChildren(...content.map((c) => (c instanceof Node ? c : document.createTextNode(String(c)))));
  }

  function fmtTime(ts) {
    const d = new Date(ts);
    if (typeof ts !== "string" && typeof ts !== "number") return "";
    if (Number.isNaN(d.getTime())) return String(ts);
    const p = (n, w = 2) => String(n).padStart(w, "0");
    return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${p(d.getMilliseconds(), 3)}`;
  }

  // ----------------------------------------------------------------- state

  const params = new URLSearchParams(location.search);
  const demoOverride = params.has("demo") ? params.get("demo") !== "0" : null;
  const runTag = Math.random().toString(36).slice(2, 6);

  const S = {
    cluster: null,
    groups: [], // normalized group rows
    perNode: false, // true when /v1/cluster reports one row per (node, group)
    demo: false,
    topics: [],
    active: null,
    events: [],
    eventKeys: new Set(),
    lastLeaders: new Map(), // group -> {leader, term}
    produced: new Map(), // topic -> {all: Map(value->count), leader: Map(value->count), unknown: n}
    counter: 0,
    lastProduce: null, // {topic, body}
    tail: { gen: 0, running: false, ctrl: null, offset: 0, count: 0, hw: null },
    guide: { killed: null, oldLeader: null, oldTerm: null, killAt: 0, producerN: 0, seq: 0 },
    failures: 0,
  };

  // ------------------------------------------------------------ cluster view

  // normalizeGroups accepts the SCOPE.md shape and, defensively, an optional
  // per-row "node" (or "node_id") field. Without it every row is the view of
  // the node that answered (cluster.node_id).
  function normalizeGroups(c) {
    const rows = Array.isArray(c.groups) ? c.groups : [];
    let perNode = false;
    const out = rows.map((g) => {
      const node = g.node ?? g.node_id;
      if (node !== undefined) perNode = true;
      return {
        name: String(g.name ?? "?"),
        node: node ?? c.node_id,
        role: typeof g.role === "string" ? g.role : "",
        leader: isNum(g.leader) ? g.leader : 0,
        term: g.term,
        commit: g.commit,
        hw: g.high_watermark,
        leo: g.log_end_offset,
      };
    });
    return { rows: out, perNode };
  }

  // leaderOf returns the current {leader, term} for a group name, preferring
  // a row whose role is "leader", else the highest-term row's leader hint.
  function leaderOf(name) {
    const rows = S.groups.filter((g) => g.name === name);
    if (!rows.length) return { leader: 0, term: null, row: null };
    const lr = rows.find((g) => g.role === "leader" && nodeUp(g.node));
    if (lr) return { leader: lr.node, term: lr.term, row: lr };
    let best = rows[0];
    for (const g of rows) if (isNum(g.term) && (!isNum(best.term) || g.term > best.term)) best = g;
    const row = rows.find((g) => g.node === best.leader) || null;
    return { leader: best.leader, term: best.term, row };
  }

  function nodeUp(id) {
    const nodes = (S.cluster && Array.isArray(S.cluster.nodes)) ? S.cluster.nodes : [];
    const n = nodes.find((x) => x.id === id);
    return n ? n.up !== false : true;
  }

  function groupNames() {
    const names = [...new Set(S.groups.map((g) => g.name))];
    return names.sort((a, b) => (isInternal(a) - isInternal(b)) || a.localeCompare(b));
  }

  function renderConn(ok, text) {
    $("conn-dot").className = "dot " + (ok ? "dot-up" : "dot-down");
    $("conn-text").textContent = text;
  }

  function renderNodes() {
    const ul = $("nodes");
    const nodes = Array.isArray(S.cluster?.nodes) ? S.cluster.nodes : [];
    const leads = new Map();
    for (const name of groupNames()) {
      const { leader } = leaderOf(name);
      if (!leader) continue;
      if (!leads.has(leader)) leads.set(leader, []);
      leads.get(leader).push(name);
    }
    const items = nodes.map((n) => {
      const up = n.up !== false;
      const me = n.id === S.cluster.node_id;
      const leading = leads.get(n.id) || [];
      const actions = S.demo
        ? el("div", { className: "node-actions" },
          el("button", { type: "button", className: "danger", disabled: !up, dataset: { act: "kill", id: n.id }, "aria-label": `Kill node ${n.id}` }, "Kill"),
          el("button", { type: "button", className: "secondary", disabled: up, dataset: { act: "restart", id: n.id }, "aria-label": `Restart node ${n.id}` }, "Restart"))
        : null;
      return el("li", { className: "node" + (up ? "" : " is-down") },
        el("div", { className: "node-title" },
          el("span", { className: "dot " + (up ? "dot-up" : "dot-down"), "aria-hidden": "true" }),
          `Node ${n.id}`,
          me ? el("span", { className: "badge", title: "The node that answered /v1/cluster" }, "this node") : null),
        el("span", { className: up ? "node-state-up" : "node-state-down" }, up ? "up" : "down"),
        n.addr ? el("span", { className: "node-addr" }, n.addr) : null,
        el("span", { className: "small muted" },
          leading.length ? `leader of: ${leading.filter((x) => !isInternal(x)).concat(leading.filter(isInternal)).join(", ")}` : "leader of: –"),
        actions);
    });
    ul.replaceChildren(...items);
  }

  function renderGroups() {
    const showInternal = $("show-internal").checked;
    const tbody = $("groups").tBodies[0];
    const rows = [];
    for (const name of groupNames()) {
      if (!showInternal && isInternal(name)) continue;
      const { row: lrow } = leaderOf(name);
      const gs = S.groups.filter((g) => g.name === name).sort((a, b) => a.node - b.node);
      for (const g of gs) {
        const isLeader = g.role === "leader" && nodeUp(g.node);
        const behind = S.perNode && lrow && isNum(lrow.leo) && isNum(g.leo) && !isLeader ? Math.max(0, lrow.leo - g.leo) : null;
        const cls = [isLeader ? "leader-row" : "", isInternal(name) ? "internal" : "", nodeUp(g.node) ? "" : "down-row"].filter(Boolean).join(" ");
        rows.push(el("tr", { className: cls },
          el("td", null, name),
          el("td", null, fmtNum(g.node)),
          el("td", null, g.role || "–"),
          el("td", null, g.leader ? String(g.leader) : "none"),
          el("td", { className: "num" }, fmtNum(g.term)),
          el("td", { className: "num" }, fmtNum(g.commit)),
          el("td", { className: "num" }, fmtNum(g.hw)),
          el("td", { className: "num" }, fmtNum(g.leo)),
          el("td", { className: "num" }, behind === null ? "–" : String(behind))));
      }
    }
    if (!rows.length) rows.push(el("tr", null, el("td", { colSpan: 9, className: "muted" }, "No Raft groups reported yet.")));
    tbody.replaceChildren(...rows);
  }

  // --------------------------------------------------------------- events

  function addEvent(ev, observed) {
    const key = `${ev.ts}|${ev.node}|${ev.group}|${ev.msg}`;
    if (S.eventKeys.has(key)) return false;
    S.eventKeys.add(key);
    S.events.push({ ...ev, observed, seq: S.events.length });
    return true;
  }

  function ingestEvents(c) {
    let changed = false;
    if (Array.isArray(c.events)) {
      for (const ev of c.events) {
        if (ev && typeof ev === "object") changed = addEvent({ ts: ev.ts, node: ev.node, group: ev.group, msg: String(ev.msg ?? "") }, false) || changed;
      }
    }
    // Leader changes seen between two polls (useful even if the server's
    // event buffer is small).
    for (const name of groupNames()) {
      const cur = leaderOf(name);
      const prev = S.lastLeaders.get(name);
      if (prev && (prev.leader !== cur.leader)) {
        const msg = cur.leader
          ? `leader changed ${prev.leader || "none"} → ${cur.leader}${isNum(cur.term) ? ` (term ${cur.term})` : ""}`
          : `no leader (was ${prev.leader})`;
        changed = addEvent({ ts: new Date().toISOString(), node: cur.leader || prev.leader, group: name, msg }, true) || changed;
      }
      S.lastLeaders.set(name, { leader: cur.leader, term: cur.term });
    }
    if (S.events.length > 400) {
      S.events = S.events.slice(-300);
      S.eventKeys = new Set(S.events.map((e) => `${e.ts}|${e.node}|${e.group}|${e.msg}`));
    }
    if (changed) renderEvents();
  }

  function renderEvents() {
    const tsv = (e) => { const t = Date.parse(e.ts); return Number.isNaN(t) ? 0 : t; };
    const sorted = [...S.events].sort((a, b) => (tsv(b) - tsv(a)) || (b.seq - a.seq)).slice(0, 200);
    const items = sorted.map((e) => el("li", { className: [e.observed ? "observed" : "", /leader|elect|term/i.test(e.msg) ? "election" : ""].filter(Boolean).join(" ") },
      el("span", { className: "ev-ts" }, fmtTime(e.ts)),
      e.group ? el("code", null, e.group) : null, " ",
      e.node ? `node ${e.node}: ` : "",
      e.msg,
      e.observed ? el("span", { className: "ev-src" }, " · dashboard") : null));
    if (!items.length) items.push(el("li", { className: "muted" }, "No events yet."));
    $("events").replaceChildren(...items);
  }

  // ------------------------------------------------------------- polling

  async function pollCluster() {
    try {
      const { data } = await api("GET", "/v1/cluster", undefined, { timeoutMs: 4000 });
      S.failures = 0;
      S.cluster = data;
      const g = normalizeGroups(data);
      S.groups = g.rows;
      S.perNode = g.perNode;
      const demo = demoOverride ?? (data.demo === true);
      if (demo !== S.demo) {
        S.demo = demo;
        document.body.classList.toggle("demo", demo);
        $("demo-badge").hidden = !demo;
      }
      const nodes = Array.isArray(data.nodes) ? data.nodes : [];
      const up = nodes.filter((n) => n.up !== false).length;
      renderConn(true, `Connected to node ${fmtNum(data.node_id)}`);
      $("cluster-meta").textContent = `${up}/${nodes.length} nodes up · refreshed ${fmtTime(Date.now())}`;
      $("banner").hidden = true;
      renderNodes();
      renderGroups();
      ingestEvents(data);
    } catch (e) {
      S.failures++;
      renderConn(false, `API unreachable (${e.code || "error"})`);
      if (S.failures >= 3) {
        const b = $("banner");
        b.replaceChildren(el("strong", null, "Cannot reach /v1/cluster: "), errNode(e),
          " · retrying every second. On a free hosted instance the first request after idling can take about a minute.");
        b.hidden = false;
      }
    }
  }

  async function clusterLoop() {
    for (;;) {
      await pollCluster();
      await sleep(1000);
    }
  }

  async function refreshTopics() {
    try {
      const { data } = await api("GET", "/v1/topics", undefined, { timeoutMs: 4000 });
      const list = Array.isArray(data.topics) ? data.topics.map((t) => (typeof t === "string" ? t : t && t.name)).filter(Boolean) : [];
      S.topics = list.sort();
      if (!S.active) {
        const first = S.topics.find((t) => !isInternal(t));
        if (first) setActive(first);
      }
      renderTopics();
    } catch { /* the cluster panel already reports connectivity */ }
  }

  async function topicsLoop() {
    for (;;) {
      await refreshTopics();
      await sleep(3000);
    }
  }

  // --------------------------------------------------------------- topics

  function renderTopics() {
    const items = S.topics.map((t) => el("li", null,
      el("button", { type: "button", "aria-pressed": String(t === S.active), dataset: { topic: t } }, t)));
    if (!items.length) items.push(el("li", { className: "muted small" }, "No topics yet. Create one above."));
    $("topics").replaceChildren(...items);
  }

  function setActive(name) {
    if (name === S.active) return;
    S.active = name;
    $("active-topic").textContent = name || "none";
    for (const n of document.querySelectorAll(".active-topic-name")) n.textContent = name || "–";
    renderTopics();
    if (S.tail.running) startTail();
  }

  async function createTopic(name) {
    const r = await api("POST", "/v1/topics", { name });
    if (!S.topics.includes(name)) { S.topics.push(name); S.topics.sort(); }
    setActive(name);
    renderTopics();
    return r;
  }

  // -------------------------------------------------------------- produce

  function producedFor(topic) {
    if (!S.produced.has(topic)) S.produced.set(topic, { all: new Map(), leader: new Map(), unknown: 0 });
    return S.produced.get(topic);
  }

  function recordAck(topic, values, acks) {
    const p = producedFor(topic);
    const m = acks === "leader" ? p.leader : p.all;
    for (const v of values) m.set(v, (m.get(v) || 0) + 1);
  }

  function headerNote(headers) {
    // A forwarded response can carry the header twice ("2, 2"); show it once.
    const leader = (headers.get("X-QL-Leader") || "").split(",")[0].trim();
    const fwd = (headers.get("X-QL-Forwarded") || "").split(",")[0].trim();
    if (fwd === "true") return ` · forwarded to leader ${leader || "?"}`;
    if (leader) return ` · leader ${leader}`;
    return "";
  }

  async function produceRequest(topic, body) {
    return api("POST", `/v1/topics/${enc(topic)}/messages`, body, { timeoutMs: 15000 });
  }

  function buildBody(messages, acks, pid, seq) {
    const body = { messages, acks };
    if (pid) { body.producer_id = pid; body.sequence = seq; }
    return body;
  }

  async function onProduceSubmit(ev) {
    ev.preventDefault();
    const out = $("produce-result");
    const topic = S.active;
    if (!topic) { setResult(out, false, "Select or create a topic first."); return; }
    const count = Math.min(1000, Math.max(1, parseInt($("p-count").value, 10) || 1));
    const key = $("p-key").value;
    const tmpl = $("p-value").value;
    const acks = $("p-acks").value;
    const pid = $("p-pid").value.trim();
    const seq = Math.max(0, parseInt($("p-seq").value, 10) || 0);
    const messages = [];
    for (let i = 0; i < count; i++) {
      const n = ++S.counter;
      const m = { value: tmpl.replace(/\{n\}/g, String(n)) };
      if (key) m.key = key.replace(/\{n\}/g, String(n));
      messages.push(m);
    }
    const body = buildBody(messages, acks, pid, seq);
    S.lastProduce = { topic, body };
    $("p-retry").disabled = false;
    const ok = await sendProduce(topic, body, false);
    // Kafka-style base sequence: the next request starts after this one's records.
    if (pid && ok) $("p-seq").value = String(seq + count);
  }

  async function sendProduce(topic, body, isRetry) {
    const out = $("produce-result");
    setResult(out, null, isRetry ? "Retrying…" : "Sending…");
    try {
      const r = await produceRequest(topic, body);
      const d = r.data;
      if (!d.duplicate) recordAck(topic, body.messages.map((m) => m.value), body.acks);
      const range = d.base_offset === d.last_offset ? `offset ${fmtNum(d.base_offset)}` : `offsets ${fmtNum(d.base_offset)}–${fmtNum(d.last_offset)}`;
      setResult(out, true,
        `${d.duplicate ? "Duplicate detected, not appended again" : "Acknowledged"} (acks=${body.acks}): ${range} · ${r.ms} ms${headerNote(r.headers)}`);
      return true;
    } catch (e) {
      if (UNKNOWN_OUTCOME.has(e.code)) producedFor(topic).unknown += body.messages.length;
      setResult(out, false, errNode(e), UNKNOWN_OUTCOME.has(e.code) ? " · outcome unknown: the messages may or may not be committed" : "");
      return false;
    }
  }

  // ----------------------------------------------------------------- tail

  const TAIL_WAIT_MS = 5000;
  const TAIL_KEEP = 300;

  function tailMeta() {
    const t = S.tail;
    $("tail-meta").textContent = t.running
      ? `tailing · next offset ${t.offset} · HW ${fmtNum(t.hw)} · ${t.count} received`
      : `stopped · ${t.count} received`;
    const b = $("tail-toggle");
    b.textContent = t.running ? "Stop tail" : "Start tail";
    b.setAttribute("aria-pressed", String(t.running));
  }

  function stopTail() {
    const t = S.tail;
    t.gen++;
    t.running = false;
    if (t.ctrl) t.ctrl.abort();
    t.ctrl = null;
    tailMeta();
  }

  function startTail(fromOverride) {
    stopTail();
    const topic = S.active;
    const out = $("tail-result");
    if (!topic) { setResult(out, false, "Select or create a topic first."); return; }
    const t = S.tail;
    t.running = true;
    t.ctrl = new AbortController();
    t.count = 0;
    t.hw = null;
    setResult(out, null, "");
    $("tail").tBodies[0].replaceChildren();
    tailMeta();
    tailLoop(t.gen, topic, fromOverride || $("tail-from").value);
  }

  function tailAppend(msgs) {
    const tbody = $("tail").tBodies[0];
    const frag = document.createDocumentFragment();
    for (let i = msgs.length - 1; i >= 0; i--) {
      const m = msgs[i];
      frag.append(el("tr", { className: "fresh" },
        el("td", { className: "num" }, fmtNum(m.offset)),
        el("td", { className: "wrap" }, m.key ?? ""),
        el("td", { className: "wrap" }, m.value ?? ""),
        el("td", null, m.timestamp ? fmtTime(m.timestamp) : "")));
    }
    tbody.prepend(frag);
    while (tbody.rows.length > TAIL_KEEP) tbody.deleteRow(-1);
  }

  async function tailLoop(gen, topic, from) {
    const t = S.tail;
    const out = $("tail-result");
    t.offset = 0;
    if (from === "end") {
      for (;;) {
        try {
          const r = await api("GET", `/v1/topics/${enc(topic)}/messages?offset=0&max=1&wait_ms=0`, undefined, { signal: t.ctrl.signal });
          if (gen !== t.gen) return;
          t.offset = isNum(r.data.high_watermark) ? r.data.high_watermark : 0;
          break;
        } catch (e) {
          if (gen !== t.gen) return;
          setResult(out, false, errNode(e), " · retrying");
          await sleep(1000);
          if (gen !== t.gen) return;
        }
      }
    }
    tailMeta();
    while (gen === t.gen) {
      const t0 = performance.now();
      try {
        const r = await api("GET", `/v1/topics/${enc(topic)}/messages?offset=${t.offset}&max=100&wait_ms=${TAIL_WAIT_MS}`, undefined,
          { timeoutMs: TAIL_WAIT_MS + 5000, signal: t.ctrl.signal });
        if (gen !== t.gen) return;
        const msgs = Array.isArray(r.data.messages) ? r.data.messages : [];
        if (msgs.length) {
          tailAppend(msgs);
          t.count += msgs.length;
        }
        if (isNum(r.data.next_offset)) t.offset = r.data.next_offset;
        else if (msgs.length && isNum(msgs[msgs.length - 1].offset)) t.offset = msgs[msgs.length - 1].offset + 1;
        if (isNum(r.data.high_watermark)) {
          t.hw = r.data.high_watermark;
          // The log was reset under us (e.g. a free instance restarted): resume at its end.
          if (t.offset > t.hw && !msgs.length) t.offset = t.hw;
        }
        if (out.classList.contains("err")) setResult(out, null, "");
        tailMeta();
        // Guard against a server that ignores wait_ms: never spin.
        if (!msgs.length && performance.now() - t0 < 200) await sleep(500);
      } catch (e) {
        if (gen !== t.gen) return;
        setResult(out, false, errNode(e), " · retrying in 1 s");
        if (e.code === "BAD_REQUEST") {
          // Most likely offset out of range (the log was reset, e.g. a free
          // instance restarted): resume at the current high watermark.
          try {
            const r = await api("GET", `/v1/topics/${enc(topic)}/messages?offset=0&max=1&wait_ms=0`, undefined, { signal: t.ctrl.signal });
            if (gen === t.gen && isNum(r.data.high_watermark) && t.offset > r.data.high_watermark) t.offset = r.data.high_watermark;
          } catch { /* retried below */ }
        }
        await sleep(1000);
      }
    }
  }

  // -------------------------------------------------------- consumer groups

  function offsetInputs() {
    const group = $("o-group").value.trim();
    const topic = S.active;
    if (!group) throw new Error("Enter a group name.");
    if (!topic) throw new Error("Select or create a topic first.");
    return { group, topic };
  }

  async function onCommitOffset(ev) {
    ev.preventDefault();
    const out = $("offset-result");
    try {
      const { group, topic } = offsetInputs();
      const offset = Math.max(0, parseInt($("o-offset").value, 10) || 0);
      await api("POST", `/v1/groups/${enc(group)}/offsets`, { topic, offset });
      setResult(out, true, `Committed offset ${offset} for group "${group}" on ${topic}.`);
    } catch (e) {
      setResult(out, false, errNode(e));
    }
  }

  async function onFetchOffset() {
    const out = $("offset-result");
    try {
      const { group, topic } = offsetInputs();
      const { data } = await api("GET", `/v1/groups/${enc(group)}/offsets?topic=${enc(topic)}`);
      const off = data.offset;
      setResult(out, true, off === -1 ? `Group "${group}" has no committed offset on ${topic} (offset -1).` : `Group "${group}" on ${topic}: committed offset ${fmtNum(off)}.`);
      if (isNum(off) && off >= 0) $("o-offset").value = String(off);
    } catch (e) {
      setResult(out, false, errNode(e));
    }
  }

  // --------------------------------------------------------- demo controls

  async function nodeAction(act, id) {
    const out = $("cluster-result");
    setResult(out, null, `${act === "kill" ? "Killing" : "Restarting"} node ${id}…`);
    try {
      await api("POST", `/v1/demo/nodes/${enc(id)}/${act}`, undefined, { timeoutMs: 15000 });
      setResult(out, true, `Node ${id} ${act === "kill" ? "killed (data directory kept)" : "restarted"}.`);
      await pollCluster();
      return true;
    } catch (e) {
      setResult(out, false, errNode(e));
      throw e;
    }
  }

  // ----------------------------------------------------------- guided demo

  function stepUI(n) {
    const li = $(`step-${n}`);
    const status = li.querySelector(".step-status");
    return {
      run(text) { li.className = "running"; status.textContent = text; },
      done(text) { li.className = "done"; status.textContent = "Done: " + text; },
      fail(text) { li.className = "failed"; status.textContent = "Failed: " + text; },
    };
  }

  function guideTopic() {
    const name = $("guide-topic").value.trim();
    if (!TOPIC_RE.test(name)) throw new Error("topic names may use letters, digits, '.', '_' and '-'");
    return name;
  }
  const guideCount = () => Math.min(500, Math.max(1, parseInt($("guide-count").value, 10) || 20));

  // freshCluster fetches /v1/cluster now instead of waiting for the poller.
  async function freshCluster() {
    await pollCluster();
    return S.cluster;
  }

  // produceGuided sends n single-message requests with acks=all through an
  // idempotent producer, so retrying after an unknown outcome (for example
  // during an election) can never create a duplicate.
  async function produceGuided(topic, n, ui) {
    const g = S.guide;
    let ok = 0;
    let retries = 0;
    for (let i = 0; i < n; i++) {
      const value = `${topic}#${runTag}-${++S.counter}`;
      const pid = `dash-${runTag}-${g.producerN}`;
      const body = buildBody([{ key: `k${S.counter % 4}`, value }], "all", pid, g.seq);
      const deadline = performance.now() + 20000;
      let sawUnknown = false;
      for (;;) {
        try {
          await produceRequest(topic, body);
          recordAck(topic, [value], "all"); // duplicate:true still means "committed once"
          g.seq++;
          ok++;
          break;
        } catch (e) {
          if (UNKNOWN_OUTCOME.has(e.code)) sawUnknown = true;
          const retryable = !["BAD_REQUEST", "NOT_FOUND", "OUT_OF_ORDER_SEQUENCE"].includes(e.code);
          if (!retryable || performance.now() > deadline) {
            if (sawUnknown) producedFor(topic).unknown++;
            // Start a fresh producer so a half-known sequence can't collide.
            g.producerN++;
            g.seq = 0;
            throw Object.assign(new Error(`${ok}/${n} acknowledged, then ${errText(e)}`), { partial: ok });
          }
          retries++;
          ui.run(`${ok}/${n} acknowledged · retrying after ${e.code}…`);
          await sleep(300);
        }
      }
      if (i % 5 === 4 || i === n - 1) ui.run(`${ok}/${n} acknowledged…`);
    }
    return `${ok}/${n} messages acknowledged by a majority${retries ? ` (${retries} safe retries)` : ""}.`;
  }

  async function readAll(topic) {
    const out = [];
    let offset = 0;
    let hw = null;
    for (let page = 0; page < 1000; page++) {
      const { data } = await api("GET", `/v1/topics/${enc(topic)}/messages?offset=${offset}&max=1000&wait_ms=0`);
      if (hw === null && isNum(data.high_watermark)) hw = data.high_watermark;
      const msgs = Array.isArray(data.messages) ? data.messages : [];
      out.push(...msgs);
      const next = isNum(data.next_offset) ? data.next_offset : (msgs.length ? msgs[msgs.length - 1].offset + 1 : offset);
      if (!msgs.length || next <= offset || (hw !== null && next >= hw)) break;
      offset = next;
    }
    return { msgs: out, hw };
  }

  async function verify(topic) {
    const { msgs, hw } = await readAll(topic);
    const consumed = new Map();
    for (const m of msgs) consumed.set(m.value, (consumed.get(m.value) || 0) + 1);
    const p = producedFor(topic);
    const tally = (acked) => {
      let n = 0; let found = 0; let lost = 0;
      for (const [v, c] of acked) {
        n += c;
        const have = consumed.get(v) || 0;
        found += Math.min(c, have);
        lost += Math.max(0, c - have);
      }
      return { n, found, lost };
    };
    const all = tally(p.all);
    const leader = tally(p.leader);
    const mine = new Map();
    for (const m of [p.all, p.leader]) for (const [v, c] of m) mine.set(v, (mine.get(v) || 0) + c);
    let extra = 0; let other = 0;
    for (const [v, c] of consumed) {
      const a = mine.get(v) || 0;
      if (a === 0) other += c; else extra += Math.max(0, c - a);
    }
    const parts = [
      `Read ${msgs.length} messages (HW ${fmtNum(hw)}).`,
      `acks=all: ${all.n} acknowledged, ${all.found} found, ${all.lost} lost.`,
    ];
    if (leader.n) parts.push(`acks=leader: ${leader.n} acknowledged, ${leader.lost} lost (allowed: acks=leader only waits for the leader).`);
    if (extra) parts.push(`${extra} extra copies (retries without an idempotent producer).`);
    if (other) parts.push(`${other} messages from other producers.`);
    if (p.unknown) parts.push(`${p.unknown} messages had an unknown outcome and are not counted.`);
    if (!all.n && !leader.n) parts.push("This page has not produced to this topic yet, so there is nothing to compare.");
    return { ok: all.lost === 0, text: parts.join(" ") };
  }

  async function waitFor(cond, timeoutMs, onTick) {
    const end = performance.now() + timeoutMs;
    while (performance.now() < end) {
      await freshCluster();
      const r = cond();
      if (r) return r;
      if (onTick) onTick();
      await sleep(250);
    }
    return null;
  }

  const steps = {
    async 1(ui) {
      const topic = guideTopic();
      const r = await createTopic(topic);
      if (!S.tail.running) { $("tail-from").value = "begin"; startTail("begin"); }
      return `topic "${topic}" ${r.status === 201 ? "created" : "already existed"}; the live tail is following it.`;
    },
    async 2(ui) {
      const topic = guideTopic();
      setActive(topic);
      return produceGuided(topic, guideCount(), ui);
    },
    async 3(ui) {
      const topic = guideTopic();
      await freshCluster();
      const { leader, term } = leaderOf(topic);
      if (!leader) throw new Error(`no leader known for "${topic}" yet (did step 1 run?)`);
      S.guide.oldLeader = leader;
      S.guide.oldTerm = isNum(term) ? term : null;
      S.guide.killAt = performance.now();
      if (!S.demo) {
        S.guide.killed = leader;
        return `node ${leader} leads "${topic}" (term ${fmtNum(term)}). Stop it now (e.g. docker compose stop qlogd${leader}), then run step 4.`;
      }
      await nodeAction("kill", leader);
      S.guide.killed = leader;
      return `killed node ${leader}, the leader of "${topic}" in term ${fmtNum(term)}.`;
    },
    async 4(ui) {
      const topic = guideTopic();
      const g = S.guide;
      if (g.oldLeader === null) {
        await freshCluster();
        const cur = leaderOf(topic);
        g.oldLeader = cur.leader;
        g.oldTerm = isNum(cur.term) ? cur.term : null;
        g.killAt = performance.now();
      }
      ui.run(`waiting for a leader other than node ${g.oldLeader}…`);
      const res = await waitFor(() => {
        const cur = leaderOf(topic);
        const newer = g.oldTerm === null || !isNum(cur.term) || cur.term > g.oldTerm;
        return cur.leader && cur.leader !== g.oldLeader && nodeUp(cur.leader) && newer ? cur : null;
      }, 30000, () => ui.run(`waiting for an election… ${((performance.now() - g.killAt) / 1000).toFixed(1)} s`));
      if (!res) throw new Error("no new leader within 30 s");
      const secs = ((performance.now() - g.killAt) / 1000).toFixed(1);
      return `node ${res.leader} is the new leader in term ${fmtNum(res.term)}, visible ~${secs} s after the kill (1 s polling).`;
    },
    async 5(ui) {
      const topic = guideTopic();
      setActive(topic);
      return produceGuided(topic, guideCount(), ui);
    },
    async 6(ui) {
      const topic = guideTopic();
      await freshCluster();
      const nodes = Array.isArray(S.cluster?.nodes) ? S.cluster.nodes : [];
      let id = S.guide.killed;
      if (id === null || nodeUp(id)) {
        const down = nodes.find((n) => n.up === false);
        if (down) id = down.id;
      }
      if (id === null) throw new Error("no stopped node to restart");
      if (S.demo && !nodeUp(id)) await nodeAction("restart", id);
      else if (!S.demo && !nodeUp(id)) ui.run(`start node ${id} again (e.g. docker compose start qlogd${id}); waiting…`);
      const t0 = performance.now();
      const res = await waitFor(() => {
        if (!nodeUp(id)) return null;
        if (!S.perNode) return { measured: false };
        const { row: lrow } = leaderOf(topic);
        const mine = S.groups.find((g) => g.name === topic && g.node === id);
        if (!lrow || !mine || !isNum(lrow.leo) || !isNum(mine.leo)) return null;
        return mine.leo >= lrow.leo ? { measured: true, leo: mine.leo } : null;
      }, 60000, () => {
        const { row: lrow } = leaderOf(topic);
        const mine = S.groups.find((g) => g.name === topic && g.node === id);
        ui.run(nodeUp(id) && mine && lrow ? `node ${id} catching up: LEO ${fmtNum(mine.leo)} / ${fmtNum(lrow.leo)}` : `waiting for node ${id}…`);
      });
      if (!res) throw new Error(`node ${id} did not catch up within 60 s`);
      S.guide.killed = null;
      S.guide.oldLeader = null;
      const secs = ((performance.now() - t0) / 1000).toFixed(1);
      return res.measured
        ? `node ${id} is back and its log matches the leader (LEO ${res.leo}) after ~${secs} s.`
        : `node ${id} is up again (this API does not report per-replica offsets, so catch-up is not measured).`;
    },
    async 7(ui) {
      const topic = guideTopic();
      const r = await verify(topic);
      if (!r.ok) throw new Error(r.text);
      return (producedFor(topic).all.size ? "PASS: no acknowledged message was lost. " : "") + r.text;
    },
  };

  let guideBusy = false;
  async function runStep(n, btn) {
    if (guideBusy) return;
    guideBusy = true;
    const ui = stepUI(n);
    const buttons = document.querySelectorAll("#guide button[data-step]");
    for (const b of buttons) b.disabled = true;
    ui.run("running…");
    try {
      ui.done(await steps[n](ui));
    } catch (e) {
      ui.fail(errText(e));
    } finally {
      for (const b of buttons) b.disabled = false;
      guideBusy = false;
      btn.focus();
    }
  }

  // ---------------------------------------------------------------- wiring

  function wire() {
    $("show-internal").addEventListener("change", renderGroups);

    $("nodes").addEventListener("click", (ev) => {
      const b = ev.target.closest("button[data-act]");
      if (!b) return;
      b.disabled = true;
      nodeAction(b.dataset.act, Number(b.dataset.id)).catch(() => {});
    });

    $("events-clear").addEventListener("click", () => {
      S.events = [];
      renderEvents();
    });

    $("topic-form").addEventListener("submit", async (ev) => {
      ev.preventDefault();
      const out = $("topic-result");
      const name = $("topic-name").value.trim();
      if (!TOPIC_RE.test(name)) { setResult(out, false, "Topic names may use letters, digits, '.', '_' and '-'."); return; }
      try {
        const r = await createTopic(name);
        setResult(out, true, r.status === 201 ? `Created "${name}".` : `"${name}" already exists.`);
        $("topic-name").value = "";
      } catch (e) {
        setResult(out, false, errNode(e));
      }
    });

    $("topics").addEventListener("click", (ev) => {
      const b = ev.target.closest("button[data-topic]");
      if (b) setActive(b.dataset.topic);
    });

    $("produce-form").addEventListener("submit", onProduceSubmit);
    $("p-retry").addEventListener("click", () => {
      if (S.lastProduce) sendProduce(S.lastProduce.topic, S.lastProduce.body, true);
    });

    $("tail-toggle").addEventListener("click", () => (S.tail.running ? stopTail() : startTail()));
    $("tail-clear").addEventListener("click", () => {
      $("tail").tBodies[0].replaceChildren();
      S.tail.count = 0;
      tailMeta();
    });

    $("offset-form").addEventListener("submit", onCommitOffset);
    $("o-fetch").addEventListener("click", onFetchOffset);
    $("o-from-tail").addEventListener("click", () => { $("o-offset").value = String(S.tail.offset); });

    $("guide").addEventListener("click", (ev) => {
      const b = ev.target.closest("button[data-step]");
      if (b) runStep(Number(b.dataset.step), b);
    });
  }

  if (demoOverride !== null) {
    S.demo = demoOverride;
    document.body.classList.toggle("demo", demoOverride);
    $("demo-badge").hidden = !demoOverride;
  }
  wire();
  renderEvents();
  renderTopics();
  tailMeta();
  clusterLoop();
  topicsLoop();
})();
