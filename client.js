// collage-live client. Refreshes elements marked data-collage-fragment="<url>":
//   data-collage-interval="2s"  poll the URL on that interval
//   data-collage-push           take updates from the stream the server pushes
//   data-collage-swap="morph"   patch the existing DOM instead of replacing it
// and submits forms marked data-collage-target="<selector>" in place.
(() => {
  "use strict";
  const self = document.currentScript;
  const streamPath = self && self.dataset.collageStream;
  const transport = (self && self.dataset.collageTransport) || "sse";
  const workerPath = self && self.dataset.collageWorker;
  const TIMEOUT = 10000, MAX_BACKOFF = 60000, FALLBACK = 5000, RETRY_STREAM = 60000;

  // element → { etag, html, timer, failures, inflight }
  const state = new WeakMap();
  const stateOf = (el) => {
    let s = state.get(el);
    if (!s) state.set(el, (s = { etag: "", html: null, timer: 0, failures: 0, inflight: null }));
    return s;
  };
  const all = () => [...document.querySelectorAll("[data-collage-fragment]")];
  const byURL = (url) => all().filter((el) => el.dataset.collageFragment === url);
  const pushed = () => all().filter((el) => el.hasAttribute("data-collage-push"));

  const duration = (text) => {
    const m = /^(\d+(?:\.\d+)?)(ms|s|m)?$/.exec((text || "").trim());
    if (!m) return 0;
    return parseFloat(m[1]) * ({ ms: 1, s: 1000, m: 60000 }[m[2] || "ms"]);
  };

  // --- applying a response ------------------------------------------------------

  // The response body: hoisted items as <template data-collage-hoist> ahead of the
  // markup. Scripts in the markup are dropped — a fragment's script would run on
  // every refresh, and the development reload script would pile up.
  const parse = (body) => {
    const t = document.createElement("template");
    t.innerHTML = body;
    const head = [];
    for (const item of t.content.querySelectorAll("template[data-collage-hoist]")) {
      head.push({ area: item.dataset.collageHoist, key: item.dataset.collageKey, html: item.innerHTML });
      item.remove();
    }
    t.content.querySelectorAll("script").forEach((s) => s.remove());
    return { head, content: t.content };
  };

  // What the head already holds, by key. The page's own head carries no keys, so a
  // declaration is also skipped when an identical element is already there: the
  // page's stylesheet link and the fragment's are the same markup.
  const applied = new Map();
  const applyHead = (items) => {
    for (const item of items || []) {
      const target = item.area === "head" ? document.head : document.querySelector(`[data-collage-area="${CSS.escape(item.area)}"]`);
      if (!target) continue;
      const t = document.createElement("template");
      t.innerHTML = item.html;
      const nodes = [...t.content.children];
      if (nodes.length === 1 && nodes[0].tagName === "TITLE") {
        document.title = nodes[0].textContent;
        continue;
      }
      const previous = applied.get(item.key);
      if (previous && previous.length === nodes.length && previous.every((n, i) => n.isEqualNode(nodes[i]))) continue;
      if (!previous && nodes.every((n) => [...target.children].some((c) => c.isEqualNode(n)))) {
        applied.set(item.key, nodes);
        continue;
      }
      (previous || []).forEach((n) => n.remove());
      nodes.forEach((n) => target.appendChild(n));
      applied.set(item.key, nodes);
    }
  };

  const morph = (from, to) => {
    const a = [...from.childNodes], b = [...to.childNodes];
    b.forEach((node, i) => (i < a.length ? patch(a[i], node) : from.appendChild(node)));
    a.slice(b.length).forEach((node) => node.remove());
  };
  const patch = (x, y) => {
    if (x.nodeType !== y.nodeType || x.nodeName !== y.nodeName || (x.id || y.id) && x.id !== y.id) return x.replaceWith(y);
    if (x.nodeType !== 1) {
      if (x.nodeValue !== y.nodeValue) x.nodeValue = y.nodeValue;
      return;
    }
    if (x.isEqualNode(y)) return;
    for (const { name } of [...x.attributes]) if (!y.hasAttribute(name)) x.removeAttribute(name);
    for (const { name, value } of [...y.attributes]) if (x.getAttribute(name) !== value) x.setAttribute(name, value);
    // What the reader is typing into is theirs; the server's copy waits.
    if (x === document.activeElement && /^(INPUT|TEXTAREA|SELECT)$/.test(x.tagName)) return;
    morph(x, y);
  };

  const fresh = (el) => {
    stateOf(el).failures = 0;
    el.removeAttribute("data-collage-stale");
  };

  // swap puts html into el. etag is the ETag it came with — "" when it came from
  // somewhere that has none, a form's answer, which the next poll must not be
  // told it already holds.
  const swap = (el, html, head, etag) => {
    const s = stateOf(el);
    s.etag = etag || "";
    fresh(el);
    if (head) applyHead(head);
    if (html === s.html) return;
    const parsed = parse(html);
    applyHead(parsed.head);
    // The first copy to arrive is usually what the page was served with. Put in
    // anyway, it would flicker and cost the reader their focus.
    if (s.html === null) {
      const probe = document.createElement("div");
      probe.append(parsed.content.cloneNode(true));
      if (probe.innerHTML.trim() === el.innerHTML.trim()) {
        s.html = html;
        return;
      }
    }
    s.html = html;
    if (el.dataset.collageSwap === "morph") morph(el, parsed.content);
    else el.replaceChildren(parsed.content);
    el.dispatchEvent(new CustomEvent("collage:swap", { bubbles: true }));
  };

  const markStale = (el) => {
    stateOf(el).failures++;
    el.setAttribute("data-collage-stale", "");
    el.dispatchEvent(new CustomEvent("collage:stale", { bubbles: true }));
  };

  // --- polling ---------------------------------------------------------------------

  const fetchInto = async (el) => {
    const s = stateOf(el);
    // One request per element at a time. A newer one — a refresh(), the tab
    // coming back — replaces the one in flight rather than racing it, so an older
    // answer cannot land on top of a newer one.
    if (s.inflight) s.inflight.abort("superseded");
    const abort = (s.inflight = new AbortController());
    const timer = setTimeout(() => abort.abort("timeout"), TIMEOUT);
    try {
      const headers = { Accept: "text/html" };
      if (s.etag) headers["If-None-Match"] = s.etag;
      // no-store: the ETag is ours to send, and a 304 ours to see.
      const res = await fetch(el.dataset.collageFragment, { headers, cache: "no-store", signal: abort.signal, credentials: "same-origin" });
      if (res.status === 304) return fresh(el);
      if (!res.ok) throw new Error(res.status);
      const body = await res.text();
      if (abort.signal.aborted) return;
      swap(el, body, null, res.headers.get("ETag"));
    } catch (err) {
      if (abort.signal.reason !== "superseded") markStale(el);
    } finally {
      clearTimeout(timer);
      if (s.inflight === abort) s.inflight = null;
    }
  };

  // A setTimeout chain rather than setInterval, so a slow response is never
  // overlapped by the next request.
  const schedule = (el, every) => {
    const s = stateOf(el);
    clearTimeout(s.timer);
    if (!every || document.hidden || !el.isConnected) return;
    const wait = Math.min(every * 2 ** s.failures, Math.max(every, MAX_BACKOFF));
    s.timer = setTimeout(async () => {
      await fetchInto(el);
      schedule(el, every);
    }, wait);
  };

  let fallingBack = false;
  const intervalOf = (el) => duration(el.dataset.collageInterval) || (el.hasAttribute("data-collage-push") && (fallingBack || !streamPath) ? FALLBACK : 0);
  const pollAll = (now) => {
    for (const el of all()) {
      const every = intervalOf(el);
      if (!every) {
        clearTimeout(stateOf(el).timer);
        continue;
      }
      if (now) fetchInto(el).then(() => schedule(el, every));
      else schedule(el, every);
    }
  };
  const stopPolling = () => all().forEach((el) => clearTimeout(stateOf(el).timer));

  // --- pushing ---------------------------------------------------------------------

  const watches = () => {
    const seen = new Map();
    for (const el of pushed()) {
      const url = el.dataset.collageFragment;
      if (!seen.has(url)) seen.set(url, { url, etag: stateOf(el).etag });
    }
    return [...seen.values()];
  };

  const receive = (msg) => {
    for (const el of byURL(msg.url)) {
      if (!el.hasAttribute("data-collage-push")) continue;
      if (msg.stale) markStale(el);
      else swap(el, msg.html, msg.head, msg.etag);
    }
  };

  // The stream's state, however it is carried. While it is down, what the pushed
  // elements show is as stale as a failed poll, and says so.
  const onStatus = (status) => {
    if (status === "open") {
      // Connected: whatever changed while it was down is on its way; what did not
      // is current.
      if (fallingBack) {
        fallingBack = false;
        pollAll(false);
      }
      pushed().forEach(fresh);
    } else if (status === "error") {
      pushed().forEach((el) => el.hasAttribute("data-collage-stale") || markStale(el));
    } else if (status === "fallback") {
      if (!fallingBack) {
        fallingBack = true;
        pollAll(true);
      }
    } else if (status === "unsupported") {
      useOwnStream();
    }
  };

  // One stream for the whole browser, through a shared worker, where there is
  // one; a stream per tab otherwise. A tab holds a lock for as long as it lives,
  // so the worker learns of a tab that died without saying goodbye.
  let port = null, locked = Promise.resolve();
  const lock = "collage-live-" + Math.random().toString(36).slice(2);
  const tryWorker = () => {
    if (!workerPath || !window.SharedWorker) return false;
    try {
      const worker = new SharedWorker(workerPath, { name: "collage-live" });
      worker.onerror = useOwnStream;
      port = worker.port;
      port.onmessage = ({ data }) => (data.type === "message" ? receive(data.msg) : onStatus(data.state));
      port.start();
      // The lock first, the first message after: a worker asking for the lock
      // before this tab holds it would be granted it at once, and would take the
      // tab for one that had already gone.
      if (navigator.locks) {
        locked = new Promise((held) => navigator.locks.request(lock, () => (held(), new Promise(() => {}))));
      }
      return true;
    } catch {
      port = null;
      return false;
    }
  };

  let stream = null, failures = 0, retry = 0, own = false;
  const useOwnStream = () => {
    if (own) return;
    own = true;
    if (port) port.close();
    port = null;
    openStream();
  };

  // After three connections that failed outright, the elements that wanted pushes
  // are polled instead — and the stream is tried again now and then.
  const failed = () => {
    stream = null;
    onStatus("error");
    if (++failures >= 3) {
      onStatus("fallback");
      retry = setTimeout(() => ((failures = 2), openStream()), RETRY_STREAM);
      return;
    }
    retry = setTimeout(openStream, Math.min(1000 * 2 ** failures, MAX_BACKOFF));
  };

  const openStream = () => {
    clearTimeout(retry);
    if (stream || !streamPath || document.hidden) return;
    const list = watches();
    if (!list.length) return;
    const query = list.map((w) => "f=" + encodeURIComponent(w.etag ? w.url + "#" + w.etag : w.url)).join("&");
    const url = streamPath + (streamPath.includes("?") ? "&" : "?") + query;
    const connected = () => ((failures = 0), onStatus("open"));
    const onMessage = (data) => {
      try {
        receive(JSON.parse(data));
      } catch {}
    };
    if (transport === "ws") {
      const ws = new WebSocket(new URL(url, location.href).href.replace(/^http/, "ws"));
      ws.onopen = connected;
      ws.onmessage = (e) => onMessage(e.data);
      ws.onclose = () => stream === ws && failed();
      stream = ws;
    } else {
      const es = new EventSource(url);
      es.onopen = connected;
      es.addEventListener("fragment", (e) => onMessage(e.data));
      es.onerror = () => {
        if (stream !== es) return;
        // CONNECTING: the browser retries by itself; show what we hold as stale
        // meanwhile. CLOSED: the server refused it.
        if (es.readyState === EventSource.CLOSED) failed();
        else onStatus("error");
      };
      stream = es;
    }
  };
  const closeStream = () => {
    clearTimeout(retry);
    if (stream) {
      const s = stream;
      stream = null;
      s.close();
    }
  };

  const startPush = () => {
    if (!streamPath) return;
    if (port) locked.then(() => port && !document.hidden && port.postMessage({ type: "watch", stream: streamPath, transport, watches: watches(), lock }));
    else if (own) openStream();
  };
  const stopPush = () => {
    if (port) port.postMessage({ type: "unwatch" });
    else closeStream();
  };

  // --- forms -----------------------------------------------------------------------

  // A form marked data-collage-target posts with fetch and puts the answer — the
  // fragment an action returned — into the target. A success, or a 422 carrying
  // the form back with what was wrong, is put in; any other failure leaves the
  // target as it was and marks it stale, rather than filling it with an error
  // page. A redirect is followed as a page would follow it.
  document.addEventListener("submit", async (e) => {
    const form = e.target;
    if (!(form instanceof HTMLFormElement) || !form.dataset.collageTarget) return;
    const target = document.querySelector(form.dataset.collageTarget);
    if (!target) return;
    e.preventDefault();
    const method = (form.getAttribute("method") || "GET").toUpperCase();
    const data = new FormData(form, e.submitter);
    let action = form.action;
    const init = { method, credentials: "same-origin", headers: { Accept: "text/html" } };
    // Collage-Fetch asks collage to answer a redirect with where it leads rather
    // than the redirect itself, which fetch would follow, downloading the page
    // only for the navigation to fetch it again. Same origin only: on another, a
    // header of our own would cost a preflight the server may refuse.
    if (new URL(action, location.href).origin === location.origin) init.headers["Collage-Fetch"] = "1";
    if (method === "GET") action += (action.includes("?") ? "&" : "?") + new URLSearchParams(data);
    else init.body = data;
    try {
      const res = await fetch(action, init);
      const destination = res.status === 204 && res.headers.get("Collage-Location");
      if (destination) return location.assign(destination);
      if (res.redirected) return location.assign(res.url);
      if (!res.ok && res.status !== 422) throw new Error(res.status);
      stateOf(target).html = undefined;
      swap(target, await res.text(), null, "");
    } catch {
      markStale(target);
    }
  });

  // --- lifecycle -------------------------------------------------------------------

  // A hidden tab asks for nothing; it catches up the moment it is seen again.
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      stopPolling();
      stopPush();
    } else {
      pollAll(true);
      startPush();
    }
  });
  addEventListener("pagehide", () => port && port.postMessage({ type: "bye" }));
  addEventListener("pageshow", (e) => e.persisted && startPush());

  const start = () => {
    pollAll(false);
    if (streamPath && pushed().length && !tryWorker()) own = true;
    startPush();
  };

  window.collageLive = {
    // refresh fetches one element, or every element showing a URL, now.
    refresh(target) {
      const els = typeof target === "string" ? byURL(target) : [target];
      return Promise.all(els.map(fetchInto));
    },
    // scan picks up elements added to the page since it loaded.
    scan() {
      stopPolling();
      pollAll(false);
      if (streamPath && pushed().length && !port && !own && !tryWorker()) own = true;
      closeStream();
      startPush();
    },
  };

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", start);
  else start();
})();
