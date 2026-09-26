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

Requires collage v0.18.0 or later.

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
<section data-collage-fragment="{{fragmentURL "home" "cpu"}}" data-collage-interval="2s">
  {{slot "cpu"}}
</section>

<section data-collage-fragment="{{fragmentURL "home" "disks"}}" data-collage-push>
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

## What the client does

- Fetches on a `setTimeout` chain, never overlapping a slow response.
- Stops in a hidden tab and refreshes the moment the tab is seen again.
- Times a request out after ten seconds; on failure it marks the element
  `data-collage-stale` and backs off exponentially, up to a minute.
- Sends the `ETag` it holds and leaves the DOM alone on a `304`, or when the body is
  the same as the last one.
- Adds what the fragment hoisted — a stylesheet asked for with `{{stylesheet}}` —
  to the document's head, once, by its key.
- Drops `<script>` elements from the answer, so a script inside a fragment does not
  run again on every refresh.
- Opens one stream per page, whatever it watches, and falls back to polling every
  five seconds after three failed connections.
- Dispatches `collage:swap` and `collage:stale` events on the element.

`window.collageLive.refresh(elementOrURL)` fetches now, and
`window.collageLive.scan()` picks up elements added after the page loaded.

## Pushing

Pushing is tag-based. A fragment's data handler already returns the tags its data
came from, and the application already calls `InvalidateTags` when that data
changes. The plugin hears the invalidation, re-renders every open fragment that
depended on one of the tags, and sends it down the stream. The application never
decides which reader gets what; invalidating is the whole API.

Data that is sampled rather than changed is pushed by invalidating on a timer, and
one measurement reaches every reader:

```go
go func() {
	for range time.Tick(time.Second) {
		app.InvalidateTags(context.Background(), "system:cpu")
	}
}()
```

Several fragments reading the same measurement should fetch it through
`collage.Cached`, so one invalidation costs one measurement, not one per fragment.

### Whose render goes where

A render the framework reports as `Shared` — the page is cached for every reader,
or nothing in the fragment reads the request, and it holds no form token — is made
once per URL and sent to every reader of that URL. Any other render may hold one
reader's data, and is made for each connection with that connection's own request.
The framework decides what `Shared` means, not this plugin.

### The stream

```
GET /_live/stream/?f=/live/cpu&f=/live/disks

event: fragment
id: 3
data: {"url":"/live/cpu","html":"<p>…</p>","head":[{"area":"head","key":"…","html":"…"}]}
```

Each fragment is sent as it stands when the stream opens, which covers a reconnect
as well: a fragment is a state, not a log, so nothing missed is replayed. A URL that
is not a fragment path is refused with 400.

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
    "maxFragments": 32
  }
}
```

The client is served at `<prefix>client/client.js` and the stream at
`<prefix>stream/`. With `noStream` the client only polls.

## Other transports

[collage-websocket](https://github.com/Elagoht/collage-websocket) carries the same
messages over a WebSocket. A transport of your own calls `Plugin.Subscribe` for a
request, sends `Initial()`, then loops on `Wait`, and tells the client where to
connect with `UseTransport`.

## Without the client

The protocol is collage's own — fragment paths, the `<template data-collage-hoist>`
channel, ETags — so htmx or a script of your own works against the same server.
