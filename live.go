// Package live is a collage plugin that keeps parts of a page current in the
// browser: it serves a small client script that refreshes the fragments a page
// opened with WithFragmentPath, on an interval or when the server pushes a change
// over an event stream.
//
//	app, err := collage.New(&collage.Config{
//		Plugins: []collage.Plugin{client.New(), live.New(live.Options{})},
//	})
//
// It puts what it receives into the page with collage-client's collage.put, so
// the application registers elagoht/client too; {{liveClient}} includes that
// script ahead of its own. The layout includes the client, and the page marks
// what to refresh — the fragment's URL, built by name, and how:
//
//	<head>{{liveClient}}</head>
//
//	<section data-collage-fragment="{{fragmentURL "home" "cpu"}}" data-collage-interval="2s">
//	  {{slot "cpu"}}
//	</section>
//	<section data-collage-fragment="{{fragmentURL "home" "disks"}}" data-collage-push>
//	  {{slot "disks"}}
//	</section>
//
// The page owns the container and the fragment owns what is inside it, so one
// fragment can be wrapped differently on two pages.
//
// Pushing is tag-based. A fragment's data handler already returns the tags its
// data came from, and the application already calls InvalidateTags when that data
// changes; the plugin hears the invalidation, re-renders every open fragment that
// depended on one of the tags, and sends it down the stream. Nothing decides which
// reader gets what: an invalidation is the whole API. Data that is sampled rather
// than changed is pushed by invalidating on a timer:
//
//	go func() {
//		for range time.Tick(time.Second) {
//			app.InvalidateTags(ctx, "system:cpu")
//		}
//	}()
//
// The protocol the client speaks is collage's own — fragment paths, the hoist
// channel, ETags — so htmx or a script of your own works against the same server.
package live

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"log/slog"
	"strings"
	"time"

	"github.com/Elagoht/collage/pkg/collage"

	client "github.com/Elagoht/collage-client"
)

// Name is the plugin's name, and the key its configuration is found under.
const Name = "elagoht/live"

// defaultPrefix is where the plugin serves its client and its stream.
const defaultPrefix = "/_live/"

// defaultKeepAlive is how often an idle push connection sends something.
const defaultKeepAlive = 25 * time.Second

//go:embed client.js worker.js
var clientFS embed.FS

// Options configures the plugin.
type Options struct {
	// Prefix is where the client and the stream are served: the client at
	// <prefix>client/client.js, the stream at <prefix>stream/. It begins and ends
	// with a slash. Default "/_live/".
	Prefix string `json:"prefix"`
	// NoStream turns pushing off: the event stream, and a transport another
	// plugin serves through UseTransport. The client then only polls: an element
	// marked data-collage-push without an interval is polled every five seconds.
	NoStream bool `json:"noStream"`
	// KeepAlive is how often an idle push connection sends something, so a proxy
	// that closes quiet connections leaves it open: "25s", the default.
	KeepAlive collage.Duration `json:"keepAlive"`
	// MaxFragments caps how many fragments one stream may watch. Default 32;
	// negative is an error.
	MaxFragments int `json:"maxFragments"`
	// MaxStreamAge closes a stream after it has been open this long, and the
	// client reconnects at once. A stream renders with the cookies it was opened
	// with, so a reader who signed out elsewhere keeps receiving what the old
	// cookie reads until it reconnects; this bounds how long. Nothing is lost —
	// the reconnect sends what changed. Zero, the default, never closes one.
	MaxStreamAge collage.Duration `json:"maxStreamAge"`
}

// Plugin serves the client, the stream, and hears invalidations.
type Plugin struct {
	cfg  Options
	host collage.Host
	log  *slog.Logger
	hub  *hub
	// transport, when set by another plugin through UseTransport, replaces the
	// event stream: the client connects to its path with its kind instead.
	transport *Transport
}

// Transport is a push connection another plugin serves in place of the event
// stream — collage-websocket is one. The client connects to Path and treats each
// message as the stream's.
type Transport struct {
	// Kind is what the client speaks: "ws" for a WebSocket.
	Kind string
	// Path is where the transport is served, relative to the site.
	Path string
}

// New returns a plugin with opts as its starting point, which the application's
// own configuration is then decoded over.
func New(opts Options) *Plugin {
	return &Plugin{cfg: opts, hub: newHub()}
}

func (p *Plugin) Name() string    { return Name }
func (p *Plugin) Version() string { return "0.6.3" }

// UseTransport has the client push over t instead of the event stream. It is for
// the plugin serving t, which calls it from its Configure or its Init, or both;
// the last call wins. Calling it in Configure reserves the transport early (as
// collage-websocket does with only Kind set); a Path built from Prefix belongs in
// Init, where every plugin's Configure has run and Prefix and KeepAlive are
// final. Either way it runs before live's Init decides whether to serve the event
// stream, even for a transport plugin listed before live. With NoStream set,
// nothing is pushed over it either: Subscribe refuses with ErrNoStream.
func (p *Plugin) UseTransport(t Transport) {
	p.transport = &t
}

// Configure reads the configuration and adds {{liveClient}}.
//
// In Configure rather than Init because a template function has to exist before
// templates are parsed, and the function has to know the prefix.
func (p *Plugin) Configure(_ context.Context, host collage.ConfigHost) error {
	if _, ok := collage.FindPlugin[*client.Plugin](host); !ok {
		return errors.New("live: no collage-client plugin to put fragments in with; add elagoht/client")
	}
	cfg, err := collage.PluginConfig(host, p.cfg)
	if err != nil {
		return err
	}
	p.cfg = cfg
	if err := p.applyDefaults(); err != nil {
		return err
	}
	if !strings.HasPrefix(p.cfg.Prefix, "/") || !strings.HasSuffix(p.cfg.Prefix, "/") {
		return fmt.Errorf("live: prefix %q must begin and end with a slash", p.cfg.Prefix)
	}
	return host.AddRenderFunc("liveClient", func(rc *collage.RenderContext) any { // any: html/template.FuncMap's own value type
		return func() (template.HTML, error) { return p.clientTag(rc) }
	})
}

func (p *Plugin) applyDefaults() error {
	if p.cfg.KeepAlive < 0 || p.cfg.MaxStreamAge < 0 {
		return errors.New("live: keepAlive and maxStreamAge must not be negative")
	}
	if p.cfg.MaxFragments < 0 {
		return fmt.Errorf("live: maxFragments %d is negative", p.cfg.MaxFragments)
	}
	if p.cfg.Prefix == "" {
		p.cfg.Prefix = defaultPrefix
	}
	if p.cfg.KeepAlive == 0 {
		p.cfg.KeepAlive = collage.Duration(defaultKeepAlive)
	}
	if p.cfg.MaxFragments == 0 {
		p.cfg.MaxFragments = 32
	}
	return nil
}

// Init serves the client and, unless it is off or another transport replaces it,
// the stream.
func (p *Plugin) Init(_ context.Context, host collage.Host) error {
	p.host = host
	p.log = host.Logger()
	client, err := fs.Sub(clientFS, ".")
	if err != nil {
		return err
	}
	if err := host.Mount(p.clientPrefix(), client); err != nil {
		return fmt.Errorf("live: serve the client: %w", err)
	}
	if p.cfg.NoStream || p.transport != nil {
		return nil
	}
	if err := host.Handle(p.StreamPath(), &sseHandler{plugin: p}); err != nil {
		return fmt.Errorf("live: serve the stream: %w", err)
	}
	return nil
}

// Shutdown releases nothing: the streams were closed by CloseStreams, before the
// server stopped.
func (p *Plugin) Shutdown(context.Context) error { return nil }

// CloseStreams ends every open subscription, so the server's shutdown, which waits
// for open requests, is not kept waiting by a stream that would never end.
func (p *Plugin) CloseStreams() { p.hub.close() }

// OnCacheInvalidate re-renders and pushes every watched fragment that depended on
// one of the invalidated tags.
//
// The rendering happens off the invalidating goroutine. InvalidateTags is often
// called from an action answering a form, and that reader should not wait for
// every open page on the site to be re-rendered before seeing its redirect.
// Invalidations arriving while a batch renders are merged into the next one.
func (p *Plugin) OnCacheInvalidate(_ context.Context, ev *collage.CacheInvalidateEvent) error {
	if len(ev.Tags) == 0 {
		return nil
	}
	p.hub.queue(p, ev.Tags)
	return nil
}

// MaxStreamAge is how long a push connection may stay open before it is closed
// for the client to reconnect; zero is forever. A transport of another plugin
// honours it as the event stream does. See Options.MaxStreamAge. Like KeepAlive
// and Prefix, it is final once live's Configure has run: read it from Init.
func (p *Plugin) MaxStreamAge() time.Duration { return time.Duration(p.cfg.MaxStreamAge) }

// KeepAlive is how often an idle push connection sends something, so a proxy
// that closes quiet connections leaves it open. A transport of another plugin
// pings at this interval as the event stream does. See Options.KeepAlive. It is
// final once live's Configure has run, so a transport reads it from its Init,
// not its Configure; before that it is the configured-in-Go value or the default.
func (p *Plugin) KeepAlive() time.Duration {
	if p.cfg.KeepAlive <= 0 {
		return defaultKeepAlive
	}
	return time.Duration(p.cfg.KeepAlive)
}

// Prefix is where the plugin serves its client and its stream, beginning and
// ending with a slash: "/_live/" by default. A transport of another plugin serves
// its connection under it. See Options.Prefix. It is final once live's Configure
// has run, so a transport reads it from its Init, not its Configure.
func (p *Plugin) Prefix() string { return p.prefix() }

// ClientPath is the URL the client script is served at.
func (p *Plugin) ClientPath() string { return p.clientPrefix() + "client.js" }

// StreamPath is the URL the event stream is served at.
func (p *Plugin) StreamPath() string { return p.prefix() + "stream/" }

func (p *Plugin) prefix() string {
	if p.cfg.Prefix == "" {
		return defaultPrefix
	}
	return p.cfg.Prefix
}

func (p *Plugin) clientPrefix() string { return p.prefix() + "client/" }

// clientTag is {{liveClient}}: collage-client's script element, then live's,
// naming the push connection the client should open. Both are deferred, so the
// client runs first. A layout that also calls {{clientjs}} loads it once.
func (p *Plugin) clientTag(rc *collage.RenderContext) (template.HTML, error) {
	own, err := client.Tag(rc)
	if err != nil {
		return "", fmt.Errorf("live: %w", err)
	}
	var b strings.Builder
	b.WriteString(string(own))
	b.WriteString(`<script src="`)
	b.WriteString(template.HTMLEscapeString(p.ClientPath()))
	b.WriteString(`" data-collage-worker="`)
	b.WriteString(template.HTMLEscapeString(p.clientPrefix() + "worker.js"))
	b.WriteString(`"`)
	switch {
	case p.cfg.NoStream:
		// Pushing is off, over any transport: the client polls.
	case p.transport != nil:
		b.WriteString(` data-collage-stream="`)
		b.WriteString(template.HTMLEscapeString(p.transport.Path))
		b.WriteString(`" data-collage-transport="`)
		b.WriteString(template.HTMLEscapeString(p.transport.Kind))
		b.WriteString(`"`)
	default:
		b.WriteString(` data-collage-stream="`)
		b.WriteString(template.HTMLEscapeString(p.StreamPath()))
		b.WriteString(`" data-collage-transport="sse"`)
	}
	b.WriteString(` defer></script>`)
	return template.HTML(b.String()), nil // elements assembled from client's tag and escaped values
}

// ErrClosed is returned by Subscription.Wait, and by Subscribe, once the
// application is shutting down.
var ErrClosed = errors.New("live: closed")
