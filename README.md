# elagoht/live

A collage plugin that keeps parts of a page current in the browser. It serves a
small client script that refreshes the fragments a page opened with
`WithFragmentPath` — on an interval, or when the server pushes a change over an
event stream.

```go
app, err := collage.New(&collage.Config{
	Plugins: []collage.Plugin{live.New()},
})
```

Requires collage v0.20.0 or later.

## Marking what to refresh

The layout includes the client:

```html
<head>
  {{liveClient}}
</head>
```

The page marks each part it wants kept current with the fragment's URL, built by
name, and says how:

```html
<section data-collage-fragment="{{fragmentURL "home" "cpu"}}" data-collage-push>
  {{slot "cpu"}}
</section>

<section data-collage-fragment="{{fragmentURL "home" "disks"}}" data-collage-interval="10s">
  {{slot "disks"}}
</section>

<form method="post" action="{{pageURL "home"}}" data-collage-target="#result">…</form>
```

| Attribute | |
| --- | --- |
| `data-collage-fragment="<url>"` | The fragment path whose answer fills the element |
| `data-collage-interval="2s"` | Fetch it on this interval (`500ms`, `2s`, `1m`) |
| `data-collage-push` | Take it from the stream when the server pushes it |
| `data-collage-swap="morph"` | Patch the DOM in place rather than replacing it, so a focused input keeps what is being typed |
| `data-collage-target="<selector>"` | On a form: submit with `fetch` and put the answer into the element |

The page owns the container and the fragment owns what is inside it, so one fragment
can be wrapped differently on two pages.

While an element's content cannot be trusted to be current — a poll failed, the
stream is down — it carries `data-collage-stale`, whichever way it is refreshed.
Style it:

```css
[data-collage-stale] { opacity: .5; }
```

## What the client does

- Fetches on a `setTimeout` chain, never overlapping a slow response, and one
  request per element at a time: a newer one replaces the one in flight, so an
  older answer never lands on a newer one.
- Stops in a hidden tab and refreshes the moment the tab is seen again.
- Times a request out after ten seconds; on failure it marks the element stale and
  backs off exponentially, up to a minute.
- Keeps one ETag per element, whether the content came by polling or by push —
  collage gives both the same ETag for the same body — sends it, and leaves the DOM
  alone on a `304` or an identical body.
- Leaves the DOM alone when the first copy to arrive is what the page was served
  with, so loading the page does not flicker or lose focus.
- Adds what the fragment hoisted — a stylesheet asked for with `{{stylesheet}}` —
  to the document's head, once, by its key.
- Drops `<script>` elements from the answer, so a script inside a fragment does not
  run again on every refresh.
- Dispatches `collage:swap` and `collage:stale` events on the element.

`window.collageLive.refresh(elementOrURL)` fetches now, and
`window.collageLive.scan()` picks up elements added after the page loaded.

### Forms

A form marked `data-collage-target` is submitted with `fetch`, and the answer goes
into the target when it is a success, or a `422` — the contract for a validation
failure is an action answering the form's fragment again, with the errors, and
status 422:

```go
if problems := validate(rc); len(problems) > 0 {
	rc.Set("problems", problems)
	result := collage.RenderFragment(formFragment)
	result.Status = http.StatusUnprocessableEntity
	return result, nil
}
```

Any other failure — a 500, a refused forgery token — leaves the target as it was
and marks it stale, rather than filling it with an error page. A redirect is
followed as the browser would follow it, in one request: the form sends
`Collage-Fetch`, and collage answers with where the action redirects instead of
the redirect, which `fetch` would otherwise follow and download before the
navigation fetched the page again.

## Pushing

Pushing is tag-based. A fragment's data handler already returns the tags its data
came from, and the application already calls `InvalidateTags` when that data
changes. The plugin hears the invalidation, re-renders every open fragment that
depended on one of the tags, and sends it — unless the render is the same as the
copy the reader already holds. The application never decides which reader gets
what; invalidating is the whole API.

### Sampled data: a system monitor

Data that is measured rather than changed — CPU load, disk usage, a queue's
length — is pushed by invalidating on a timer. Three things make it cheap:

```go
// One measurement, shared by every fragment that shows part of it. No tags: with
// them, every fragment would depend on every tag, and a CPU tick would re-render
// the disks too. The TTL keeps it fresh; each fragment answers to its own tag.
func stats(rc *collage.RenderContext) (monitor.Stats, error) {
	return collage.Cached(rc, "system:stats", time.Second, nil,
		func(ctx context.Context) (monitor.Stats, error) { return monitor.Collect(ctx) })
}

cpu := collage.NewFragment("cpu", "fragments/cpu.html").
	WithDataHandler(func(ctx context.Context, rc *collage.RenderContext) (any, []string, error) {
		s, err := stats(rc)
		return s.CPU, []string{"system:cpu"}, err
	}).
	Shared(). // the same for every reader: rendered once per change, not once per tab
	Build()

go func() {
	for range time.Tick(time.Second) {
		app.InvalidateTags(context.Background(), "system:cpu")
	}
}()
```

- **`Shared()`** tells collage the handler reads nothing that differs between
  readers. Without it, the plugin must assume a render may hold one reader's data
  and makes one per open tab. Do not use `Static()` for this: `Static()` also
  promises the output does not change over time, so the page would be cached and an
  export would write one moment's reading into the HTML.
- **`Cached` without tags** shares one measurement between the fragments. Give it
  tags and every fragment depends on all of them.
- **One tag per fragment**, invalidated at the rate that part changes: CPU every
  second, disks every ten.

### Whose render goes where

A render collage reports as `Shared` — the page is cached for every reader, or every
handler in the fragment is `Static()` or `Shared()`, and it holds no form token — is
made once per URL and sent to every reader of that URL. Any other render may hold
one reader's data, and is made for each connection with that connection's own
request. collage decides what `Shared` means, not this plugin.

A connection renders with the cookies it was opened with. A reader who signs out
in another tab keeps receiving what the old cookie reads until the connection is
reopened. With sessions checked on the server that is nothing — the render comes out
anonymous — but with signed, stateless cookies, set `maxStreamAge` to bound it:
the connection is closed after that long and the client reopens it at once, with
the cookies it holds then, losing nothing.

### One connection for the whole browser

A browser holds at most six connections to one origin over HTTP/1.1 — across all
its tabs, not per tab. A stream per tab would run out at the sixth, and every
request after it, the page itself included, would wait.

So the client opens the stream from a shared worker: every tab of the site tells
the worker what it watches, the worker keeps one stream open for all of them, and
hands each message to the tabs watching its URL. When the set of watched fragments
changes, the stream is reopened with the new set, naming the ETag of each copy it
already has, so nothing unchanged is sent again. A tab that closes — or crashes —
is noticed and its fragments dropped.

Where there is no shared worker, each tab opens its own stream, and closes it while
hidden; with one visible tab at a time, as on a phone, that is still one connection.

In development, collage's own reload script also listens through a shared worker,
so however many pages are open the browser holds two long-lived connections: one
for reloading, one for this stream.

HTTP/2 lifts the limit altogether — a hundred streams on one connection — but
browsers speak it only over TLS, so not on `localhost` or behind a proxy that ends
TLS before your server.

### When the stream is down

While the stream is reconnecting, pushed elements are marked stale. After three
failed connections they are polled every five seconds instead, and the stream is
tried again every minute; when it connects, the polling stops.

### The stream

```
GET /_live/stream/?f=/live/cpu&f=/live/disks%23%229f2c%22

event: fragment
id: 3
data: {"url":"/live/cpu","html":"<p>…</p>","head":[…],"etag":"\"1a2b\""}
```

Each `f` is a fragment path, optionally followed by `#` and the ETag of the copy the
client holds. Each fragment is sent as it stands when the stream opens, unless the
client already holds that copy — which covers a reconnect as well: a fragment is a
state, not a log, so nothing missed is replayed. A URL that is not a fragment path
is refused with 400.

The stream sets `Cache-Control: no-cache` and `X-Accel-Buffering: no`, pushes its
write deadline forward before each write, and sends a comment every 25 seconds when
idle. A compression middleware of your own should leave `text/event-stream` alone,
or the stream arrives only when it ends.

## Configuration

```json
{
  "elagoht/live": {
    "prefix": "/_live/",
    "noStream": false,
    "keepAlive": "25s",
    "maxFragments": 32,
    "maxStreamAge": "0s"
  }
}
```

The client is served at `<prefix>client/client.js`, its worker at
`<prefix>client/worker.js`, and the stream at `<prefix>stream/`. With `noStream`
the client only polls, and a `data-collage-push` element without an interval is
polled every five seconds. `maxStreamAge` closes a connection after that long, for
the client to reopen; zero never does.

## Other transports

[collage-websocket](https://github.com/Elagoht/collage-websocket) carries the same
messages over a WebSocket. A transport of your own calls
`Plugin.Subscribe(r, live.ParseWatches(r.URL.Query()["f"]))`, sets `Cookie()` on
its response, sends `Initial()`, then loops on `Wait`, honours `MaxStreamAge()`,
and tells the client where to connect with `UseTransport`.

## Without the client

The protocol is collage's own — fragment paths, the `<template data-collage-hoist>`
channel, ETags — so htmx or a script of your own works against the same server.

## Changes

### v0.2.2

- `collage.json`: the plugin described to editors — its template functions,
  snippets and configuration schema — for the Collage Snippets & Highlighter
  extension and any tool reading it.

### v0.2.1

- A tab coming back from the back-forward cache receives pushes again; the worker
  forgot it when it left.
- A form whose action redirects navigates in one request, with collage v0.20.0's
  `Collage-Fetch`.

### v0.2.0

- One stream for every tab of the browser, through a shared worker. A stream per
  tab used up the browser's six connections to the origin.
- Pushed elements are marked stale while the stream is down; after falling back to
  polling, the stream is tried again every minute.
- A pushed copy the client already holds is not sent: messages carry the ETag the
  fragment path would answer with, the client names what it holds when it
  connects, and an invalidation that changed nothing a fragment shows sends
  nothing.
- A form's answer goes into its target only on success or 422; any other failure
  marks the target stale.
- One request per element at a time; a form's answer no longer leaves a stale ETag
  behind.
- The first copy to arrive does not replace an identical page.
- `Subscribe` takes `[]Watch`; `ParseWatches` reads them from a request.
  `Config.MaxStreamAge`.
- Renders for different readers in one batch run in parallel.
- Fixed a panic when an invalidation arrived during shutdown.
