// collage-live shared worker: one push connection for every tab of the site.
//
// A browser holds at most six connections to one origin over HTTP/1.1, across all
// its tabs. A stream per tab ran out of them at the sixth, and every request after
// that waited. Here the tabs tell the worker what they watch; it opens one stream
// for the union, and hands each message to the tabs watching its URL.
"use strict";

const tabs = new Map(); // port → { urls: Set<string> }
const last = new Map(); // url → the last message received, for a tab that joins later
let config = null; // { stream, transport } as the first tab reported it
let conn = null, connKey = "", failures = 0, retry = 0, fallback = false, fallbackRetry = 0;

const broadcast = (data, url) => {
  for (const [port, tab] of tabs) if (!url || tab.urls.has(url)) port.postMessage(data);
};

// The watched set changed: reopen with the union, naming the ETag of the copy
// each fragment was last seen at, so what has not changed is not sent again.
const reconcile = () => {
  const urls = new Set();
  for (const tab of tabs.values()) tab.urls.forEach((u) => urls.add(u));
  // Kept for a tab that comes back to them, but not forever.
  if (last.size > 256) for (const url of last.keys()) if (!urls.has(url)) last.delete(url);
  const key = [...urls].sort().join("\n");
  if (key === connKey && (conn || fallback)) return;
  close();
  connKey = key;
  if (urls.size && config && !fallback) open([...urls]);
};

const close = () => {
  clearTimeout(retry);
  if (conn) {
    const c = conn;
    conn = null;
    c.close();
  }
};

const receive = (data) => {
  let msg;
  try {
    msg = JSON.parse(data);
  } catch {
    return;
  }
  if (!msg.stale) last.set(msg.url, msg);
  broadcast({ type: "message", msg }, msg.url);
};

const failed = () => {
  conn = null;
  broadcast({ type: "status", state: "error" });
  if (++failures >= 3) {
    fallback = true;
    broadcast({ type: "status", state: "fallback" });
    // Tried again now and then: a deploy's short outage should not leave the
    // tabs polling for the rest of their lives.
    fallbackRetry = setTimeout(() => {
      fallback = false;
      failures = 2;
      connKey = "";
      reconcile();
    }, 60000);
    return;
  }
  retry = setTimeout(() => {
    connKey = "";
    reconcile();
  }, Math.min(1000 * 2 ** failures, 60000));
};

const opened = () => {
  failures = 0;
  clearTimeout(fallbackRetry);
  broadcast({ type: "status", state: "open" });
};

const open = (urls) => {
  const query = urls
    .map((u) => "f=" + encodeURIComponent(last.has(u) && last.get(u).etag ? u + "#" + last.get(u).etag : u))
    .join("&");
  const url = config.stream + (config.stream.includes("?") ? "&" : "?") + query;
  if (config.transport === "ws") {
    if (typeof WebSocket === "undefined") return broadcast({ type: "status", state: "unsupported" });
    const ws = new WebSocket(new URL(url, self.location.href).href.replace(/^http/, "ws"));
    ws.onopen = opened;
    ws.onmessage = (e) => receive(e.data);
    ws.onclose = () => conn === ws && failed();
    conn = ws;
  } else {
    if (typeof EventSource === "undefined") return broadcast({ type: "status", state: "unsupported" });
    const es = new EventSource(url);
    es.onopen = opened;
    es.addEventListener("fragment", (e) => receive(e.data));
    es.onerror = () => {
      if (conn !== es) return;
      // CONNECTING: the browser is retrying by itself, and the tabs should show
      // what they hold as stale meanwhile. CLOSED: the server refused it.
      if (es.readyState === EventSource.CLOSED) failed();
      else broadcast({ type: "status", state: "error" });
    };
    conn = es;
  }
};

const drop = (port) => {
  tabs.delete(port);
  reconcile();
};

self.onconnect = (event) => {
  const port = event.ports[0];
  tabs.set(port, { urls: new Set() });
  port.onmessage = ({ data }) => {
    const tab = tabs.get(port);
    if (!tab) return;
    switch (data.type) {
      case "watch": {
        config = config || { stream: data.stream, transport: data.transport };
        tab.urls = new Set(data.watches.map((w) => w.url));
        // A tab that closes without saying goodbye — a crash, a killed process —
        // releases the lock it holds, and this request is granted then.
        if (data.lock && !tab.lock && self.navigator.locks) {
          tab.lock = data.lock;
          self.navigator.locks.request(data.lock, () => drop(port));
        }
        // What the stream already brought, for a tab that did not see it.
        for (const w of data.watches) {
          const msg = last.get(w.url);
          if (msg && msg.etag !== w.etag) port.postMessage({ type: "message", msg });
        }
        if (conn && conn.readyState === 1) port.postMessage({ type: "status", state: "open" });
        if (fallback) port.postMessage({ type: "status", state: "fallback" });
        reconcile();
        break;
      }
      case "unwatch":
        tab.urls = new Set();
        reconcile();
        break;
      case "bye":
        drop(port);
        break;
    }
  };
  port.start();
};
