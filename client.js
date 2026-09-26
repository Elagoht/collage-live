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
  const TIMEOUT = 10000, MAX_BACKOFF = 60000, FALLBACK = 5000;

  const state = new WeakMap(); // element → { etag, html, timer, failures }
  const stateOf = (el) => {
    let s = state.get(el);
    if (!s) state.set(el, (s = { etag: "", html: null, timer: 0, failures: 0 }));
    return s;
  };
  const byURL = (url) =>
    [...document.querySelectorAll("[data-collage-fragment]")].filter((el) => el.dataset.collageFragment === url);

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
    return { head, content: t };
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

  const swap = (el, html, head) => {
    const s = stateOf(el);
    el.removeAttribute("data-collage-stale");
    s.failures = 0;
    if (head) applyHead(head);
    if (html === s.html) return;
    s.html = html;
    const parsed = parse(html);
    applyHead(parsed.head);
    if (el.dataset.collageSwap === "morph") morph(el, parsed.content.content);
    else el.replaceChildren(parsed.content.content);
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
    const abort = new AbortController();
    const timer = setTimeout(() => abort.abort(), TIMEOUT);
    try {
      const headers = { Accept: "text/html" };
      if (s.etag) headers["If-None-Match"] = s.etag;
      // no-store: the ETag is ours to send, and a 304 ours to see.
      const res = await fetch(el.dataset.collageFragment, { headers, cache: "no-store", signal: abort.signal, credentials: "same-origin" });
      if (res.status === 304) {
        el.removeAttribute("data-collage-stale");
        s.failures = 0;
        return;
      }
      if (!res.ok) throw new Error(res.status);
      s.etag = res.headers.get("ETag") || "";
      swap(el, await res.text());
    } catch {
      markStale(el);
    } finally {
      clearTimeout(timer);
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

  const intervalOf = (el) => duration(el.dataset.collageInterval) || (el.hasAttribute("data-collage-push") && fallingBack ? FALLBACK : 0);
  const pollAll = (now) => {
    for (const el of document.querySelectorAll("[data-collage-fragment]")) {
      const every = intervalOf(el);
      if (!every) continue;
      if (now) fetchInto(el).then(() => schedule(el, every));
      else schedule(el, every);
    }
  };
  const stopPolling = () => {
    for (const el of document.querySelectorAll("[data-collage-fragment]")) clearTimeout(stateOf(el).timer);
  };

  // --- pushing ---------------------------------------------------------------------

  let stream = null, fallingBack = false, failures = 0, retry = 0;
  const pushURLs = () =>
    [...new Set([...document.querySelectorAll("[data-collage-fragment][data-collage-push]")].map((el) => el.dataset.collageFragment))];

  const receive = (data) => {
    let msg;
    try {
      msg = JSON.parse(data);
    } catch {
      return;
    }
    for (const el of byURL(msg.url)) {
      if (!el.hasAttribute("data-collage-push")) continue;
      if (msg.stale) markStale(el);
      else swap(el, msg.html, msg.head);
    }
  };

  // After three connections that failed outright, the elements that wanted pushes
  // are polled instead.
  const failed = () => {
    stream = null;
    if (++failures >= 3 && !fallingBack) {
      fallingBack = true;
      pollAll(true);
      return;
    }
    if (!fallingBack) retry = setTimeout(openStream, Math.min(1000 * 2 ** failures, MAX_BACKOFF));
  };

  const openStream = () => {
    clearTimeout(retry);
    if (stream || fallingBack || !streamPath || document.hidden) return;
    const urls = pushURLs();
    if (!urls.length) return;
    const query = urls.map((u) => "f=" + encodeURIComponent(u)).join("&");
    const url = streamPath + (streamPath.includes("?") ? "&" : "?") + query;
    if (transport === "ws") {
      const ws = new WebSocket(new URL(url, location.href).href.replace(/^http/, "ws"));
      let opened = false;
      ws.onopen = () => ((opened = true), (failures = 0));
      ws.onmessage = (e) => receive(e.data);
      ws.onclose = () => {
        if (stream !== ws) return;
        if (opened) failures = 0;
        failed();
      };
      stream = ws;
    } else {
      const es = new EventSource(url);
      es.addEventListener("fragment", (e) => receive(e.data));
      es.onopen = () => (failures = 0);
      // EventSource reconnects by itself; CLOSED means the server refused it.
      es.onerror = () => {
        if (es.readyState === EventSource.CLOSED && stream === es) failed();
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

  // --- forms -----------------------------------------------------------------------

  // A form marked data-collage-target posts with fetch and puts the answer — the
  // fragment an action returned — into the target. A redirect is followed as a
  // page would follow it.
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
    if (method === "GET") action += (action.includes("?") ? "&" : "?") + new URLSearchParams(data);
    else init.body = data;
    try {
      const res = await fetch(action, init);
      if (res.redirected) return location.assign(res.url);
      stateOf(target).html = null;
      swap(target, await res.text());
    } catch {
      markStale(target);
    }
  });

  // --- lifecycle -------------------------------------------------------------------

  // A hidden tab asks for nothing; it catches up the moment it is seen again.
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      stopPolling();
      closeStream();
    } else {
      pollAll(true);
      openStream();
    }
  });

  const start = () => {
    pollAll(false);
    openStream();
  };

  window.collageLive = {
    // refresh fetches one element, or every element showing a URL, now.
    refresh(target) {
      const els = typeof target === "string" ? byURL(target) : [target];
      return Promise.all(els.map(fetchInto));
    },
    // scan picks up elements added to the page since it loaded.
    scan() {
      closeStream();
      stopPolling();
      start();
    },
  };

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", start);
  else start();
})();
