// Package live is a collage plugin that keeps parts of a page current in the
// browser: it serves a small client script that refreshes the fragments a page
// opened with WithFragmentPath, on an interval or when the server pushes a change
// over an event stream.
//
//	app, err := collage.New(&collage.Config{
//		Plugins: []collage.Plugin{live.New()},
//	})
//
// The layout includes the client, and the page marks what to refresh — the
// fragment's URL, built by name, and how:
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
)

// Name is the plugin's name, and the key its configuration is found under.
const Name = "elagoht/live"

// DefaultPrefix is where the plugin serves its client and its stream.
const DefaultPrefix = "/_live/"

//go:embed client.js worker.js
var clientFS embed.FS

// Config is the plugin's configuration.
type Config struct {
	// Prefix is where the client and the stream are served: the client at
	// <prefix>client/client.js, the stream at <prefix>stream/. It begins and ends
	// with a slash. Default "/_live/".
	Prefix string `json:"prefix"`
	// NoStream turns the event stream off. The client then only polls: an element
	// marked data-collage-push without an interval is polled every five seconds.
	NoStream bool `json:"noStream"`
	// KeepAlive is how often an idle stream sends a comment, so a proxy that
	// closes quiet connections leaves it open. Default 25s.
	KeepAlive Duration `json:"keepAlive"`
	// MaxFragments caps how many fragments one stream may watch. Default 32.
	MaxFragments int `json:"maxFragments"`
	// MaxStreamAge closes a stream after it has been open this long, and the
	// client reconnects at once. A stream renders with the cookies it was opened
	// with, so a reader who signed out elsewhere keeps receiving what the old
	// cookie reads until it reconnects; this bounds how long. Nothing is lost —
	// the reconnect sends what changed. Zero, the default, never closes one.
	MaxStreamAge Duration `json:"maxStreamAge"`
}

// Duration is a time.Duration that reads from JSON as a string: "25s", "1m".
type Duration time.Duration

// UnmarshalJSON reads a duration written the way time.ParseDuration reads it.
func (d *Duration) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("live: duration %s: %w", b, err)
	}
	*d = Duration(parsed)
	return nil
}

// Plugin serves the client, the stream, and hears invalidations.
type Plugin struct {
	cfg  Config
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

// New returns a plugin configured entirely from the application.
func New() *Plugin { return NewWith(Config{}) }

// NewWith returns a plugin with cfg as its starting point, which the application's
// own configuration is then decoded over.
func NewWith(cfg Config) *Plugin {
	return &Plugin{cfg: cfg, hub: newHub()}
}

func (p *Plugin) Name() string    { return Name }
func (p *Plugin) Version() string { return "0.2.0" }

// UseTransport has the client push over t instead of the event stream. It is for
// the plugin serving t, and must be called before the application is built.
func (p *Plugin) UseTransport(t Transport) {
	p.transport = &t
}

// Configure reads the configuration and adds {{liveClient}}.
//
// In Configure rather than Init because a template function has to exist before
// templates are parsed, and the function has to know the prefix.
func (p *Plugin) Configure(_ context.Context, host collage.ConfigHost) error {
	if err := host.Config(&p.cfg); err != nil {
		return err
	}
	p.applyDefaults()
	if !strings.HasPrefix(p.cfg.Prefix, "/") || !strings.HasSuffix(p.cfg.Prefix, "/") {
		return fmt.Errorf("live: prefix %q must begin and end with a slash", p.cfg.Prefix)
	}
	return host.AddTemplateFunc("liveClient", p.clientTag)
}

func (p *Plugin) applyDefaults() {
	if p.cfg.Prefix == "" {
		p.cfg.Prefix = DefaultPrefix
	}
	if p.cfg.KeepAlive <= 0 {
		p.cfg.KeepAlive = Duration(25 * time.Second)
	}
	if p.cfg.MaxFragments <= 0 {
		p.cfg.MaxFragments = 32
	}
}

// Init serves the client and, unless it is off or another transport replaces it,
// the stream.
func (p *Plugin) Init(_ context.Context, host collage.Host) error {
	p.host = host
	p.log = host.Logger()
	if p.cfg.Prefix == "" {
		// Registered with RegisterPlugin, which runs no Configure: the defaults,
		// and the configuration, are read here instead. {{liveClient}} does not
		// exist then; the layout links ClientPath itself.
		if err := host.Config(&p.cfg); err != nil {
			return err
		}
		p.applyDefaults()
	}
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
// honours it as the event stream does. See Config.MaxStreamAge.
func (p *Plugin) MaxStreamAge() time.Duration { return time.Duration(p.cfg.MaxStreamAge) }

// ClientPath is the URL the client script is served at.
func (p *Plugin) ClientPath() string { return p.clientPrefix() + "client.js" }

// StreamPath is the URL the event stream is served at.
func (p *Plugin) StreamPath() string { return p.prefix() + "stream/" }

func (p *Plugin) prefix() string {
	if p.cfg.Prefix == "" {
		return DefaultPrefix
	}
	return p.cfg.Prefix
}

func (p *Plugin) clientPrefix() string { return p.prefix() + "client/" }

// clientTag is {{liveClient}}: the script element, naming the push connection
// the client should open.
func (p *Plugin) clientTag() template.HTML {
	var b strings.Builder
	b.WriteString(`<script src="`)
	b.WriteString(template.HTMLEscapeString(p.ClientPath()))
	b.WriteString(`" data-collage-worker="`)
	b.WriteString(template.HTMLEscapeString(p.clientPrefix() + "worker.js"))
	b.WriteString(`"`)
	switch {
	case p.transport != nil:
		b.WriteString(` data-collage-stream="`)
		b.WriteString(template.HTMLEscapeString(p.transport.Path))
		b.WriteString(`" data-collage-transport="`)
		b.WriteString(template.HTMLEscapeString(p.transport.Kind))
		b.WriteString(`"`)
	case !p.cfg.NoStream:
		b.WriteString(` data-collage-stream="`)
		b.WriteString(template.HTMLEscapeString(p.StreamPath()))
		b.WriteString(`" data-collage-transport="sse"`)
	}
	b.WriteString(` defer></script>`)
	return template.HTML(b.String()) // any: an element this function assembled from escaped values
}

// ErrClosed is returned by Subscription.Wait, and by Subscribe, once the
// application is shutting down.
var ErrClosed = errors.New("live: closed")
