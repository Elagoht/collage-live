package live

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/Elagoht/collage/pkg/collage"
)

// Message is one fragment, as the client applies it: the element whose
// data-collage-fragment is URL gets HTML, and Head goes into the document's head.
// It is the same on every transport.
type Message struct {
	URL  string     `json:"url"`
	HTML string     `json:"html,omitempty"`
	Head []HeadItem `json:"head,omitempty"`
	// ETag is the ETag a request to URL would be answered with for this HTML, so
	// the client keeps one ETag per element whether the content came by polling
	// or by push.
	ETag string `json:"etag,omitempty"`
	// Stale reports that the fragment could not be rendered. The client keeps
	// what it shows and marks it stale; why is in the server's log, not here.
	Stale bool `json:"stale,omitempty"`
}

// HeadItem is one hoisted declaration: the area it was declared for, the key the
// page's head deduplicated it by, and its markup.
type HeadItem struct {
	Area string `json:"area"`
	Key  string `json:"key"`
	HTML string `json:"html"`
}

// Watch is one fragment a subscription watches: its URL, and the ETag of the copy
// the client already shows, if it knows one.
type Watch struct {
	URL  string
	ETag string
}

// ParseWatches reads the watches a client sent, one per value: the fragment's URL,
// optionally followed by "#" and the ETag it holds — "/live/cpu#\"1a2b\"". A URL
// path cannot contain "#", so the split is unambiguous. A transport passes it the
// request's "f" query values.
func ParseWatches(values []string) []Watch {
	watches := make([]Watch, 0, len(values))
	for _, value := range values {
		url, etag, _ := strings.Cut(value, "#")
		watches = append(watches, Watch{URL: url, ETag: etag})
	}
	return watches
}

// ErrTooManyFragments is returned by Subscribe for more URLs than MaxFragments.
var ErrTooManyFragments = errors.New("live: too many fragments for one stream")

// ErrNoFragments is returned by Subscribe given no URL to watch.
var ErrNoFragments = errors.New("live: no fragment to watch")

// Subscription is one reader watching some fragments: what an open stream or
// WebSocket holds. A transport subscribes, sets the Cookie and sends Initial, then
// loops on Wait and sends what it returns, until Wait fails or the connection
// ends, and then calls Close.
//
// It renders with the request it was made from, cookies included, for as long as
// it lives. A reader who signs out in another tab keeps receiving what the old
// cookie reads until the stream reconnects; Config.MaxStreamAge bounds that.
type Subscription struct {
	hub     *hub
	request *http.Request
	initial []Message
	cookie  *http.Cookie

	mu      sync.Mutex
	tags    map[string][]string // url → the tags its last render depended on
	sent    map[string]string   // url → the ETag of the copy the client holds
	pending map[string]Message
	order   []string
	ready   chan struct{}
	closed  bool
}

// Subscribe starts watching fragment paths, as {{fragmentURL}} built them, for the
// reader r came from.
//
// Each is rendered once now, for r: that is how the subscription learns which tags
// the fragment depends on, and it hands the client the current state, which may
// have moved on since the page was served — or since a stream dropped and
// reconnected. A watch whose ETag matches the render is left out of Initial: the
// client already shows it. Replaying the messages a reconnecting client missed
// would be pointless: a fragment is a state, not a log.
//
// A URL that is not a fragment path fails the subscription with
// collage.ErrUnknownFragmentPath, so nothing is reachable over a stream that is
// not reachable over HTTP.
func (p *Plugin) Subscribe(r *http.Request, watches []Watch) (*Subscription, error) {
	if p.host == nil {
		return nil, errors.New("live: the plugin has not been initialised")
	}
	watches = dedupe(watches)
	if len(watches) == 0 {
		return nil, ErrNoFragments
	}
	if max := p.cfg.MaxFragments; max > 0 && len(watches) > max {
		return nil, fmt.Errorf("%w: %d, at most %d", ErrTooManyFragments, len(watches), max)
	}

	s := &Subscription{
		hub:     p.hub,
		request: r,
		tags:    make(map[string][]string, len(watches)),
		sent:    make(map[string]string, len(watches)),
		pending: make(map[string]Message),
		ready:   make(chan struct{}, 1),
	}
	for _, w := range watches {
		render, err := p.host.RenderFragment(r, collage.FragmentRequest{Path: w.URL})
		if errors.Is(err, collage.ErrUnknownFragmentPath) {
			return nil, err
		}
		if err != nil {
			p.log.Warn("live: render failed", "url", w.URL, "err", err)
			s.initial = append(s.initial, Message{URL: w.URL, Stale: true})
			s.tags[w.URL] = nil
			continue
		}
		if render.Cookie != nil && s.cookie == nil {
			s.cookie = render.Cookie
		}
		s.tags[w.URL] = render.DependencyTags
		s.sent[w.URL] = render.ETag
		if w.ETag != "" && w.ETag == render.ETag {
			continue
		}
		s.initial = append(s.initial, messageOf(w.URL, render))
	}
	if !p.hub.add(s) {
		return nil, ErrClosed
	}
	return s, nil
}

// Initial is each watched fragment as it stands now, to send first — except those
// the client said it already shows.
func (s *Subscription) Initial() []Message { return s.initial }

// Cookie is the forgery cookie the forms in the fragments need, when the request
// carried none. A transport sets it on its response before the first message, or
// a form in a pushed fragment is refused when submitted. Nil when there is none.
func (s *Subscription) Cookie() *http.Cookie { return s.cookie }

// Wait blocks until one or more watched fragments changed, and returns them — one
// message per fragment however many times it changed meanwhile, since only the
// latest state matters. It returns ctx's error when ctx ends and ErrClosed when
// the application is shutting down.
func (s *Subscription) Wait(ctx context.Context) ([]Message, error) {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return nil, ErrClosed
		}
		if len(s.order) > 0 {
			out := make([]Message, 0, len(s.order))
			for _, url := range s.order {
				out = append(out, s.pending[url])
			}
			s.order = s.order[:0]
			clear(s.pending)
			s.mu.Unlock()
			return out, nil
		}
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.ready:
		}
	}
}

// Close stops watching. It is safe to call more than once.
func (s *Subscription) Close() {
	s.hub.remove(s)
	s.shut()
}

func (s *Subscription) shut() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.signal()
}

func (s *Subscription) signal() {
	select {
	case s.ready <- struct{}{}:
	default:
	}
}

// watching returns the watched URLs whose last render depended on one of tags.
func (s *Subscription) watching(tags map[string]bool) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var urls []string
	for url, deps := range s.tags {
		for _, tag := range deps {
			if tags[tag] {
				urls = append(urls, url)
				break
			}
		}
	}
	sort.Strings(urls)
	return urls
}

// deliver queues a fresh render of url, unless the client already holds that
// copy: an invalidation that changed nothing this fragment shows sends nothing.
func (s *Subscription) deliver(url string, render *collage.FragmentRender) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.tags[url] = render.DependencyTags
	if s.sent[url] == render.ETag {
		s.mu.Unlock()
		return
	}
	s.sent[url] = render.ETag
	s.queueLocked(messageOf(url, render))
	s.mu.Unlock()
	s.signal()
}

// deliverStale queues a message saying url could not be rendered.
func (s *Subscription) deliverStale(url string) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	// Whatever the client holds now, the next good render is news to it.
	delete(s.sent, url)
	s.queueLocked(Message{URL: url, Stale: true})
	s.mu.Unlock()
	s.signal()
}

func (s *Subscription) queueLocked(msg Message) {
	if _, queued := s.pending[msg.URL]; !queued {
		s.order = append(s.order, msg.URL)
	}
	s.pending[msg.URL] = msg
}

// hub is every open subscription, and the one worker that re-renders for them.
type hub struct {
	mu     sync.Mutex
	subs   map[*Subscription]struct{}
	closed bool

	// tags waiting for the worker. One worker, draining them a batch at a time,
	// rather than a goroutine per invalidation: two renders of one fragment for
	// one reader running side by side can finish in either order, and the older
	// one arriving last would leave the reader looking at the past. Within a
	// batch each reader's fragment is rendered once, so those renders run in
	// parallel without that risk.
	tags    map[string]bool
	wake    chan struct{}
	started bool
}

func newHub() *hub {
	return &hub{subs: make(map[*Subscription]struct{}), tags: make(map[string]bool), wake: make(chan struct{}, 1)}
}

// queue hands tags to the worker, starting it the first time.
func (h *hub) queue(p *Plugin, tags []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	for _, tag := range tags {
		h.tags[tag] = true
	}
	if !h.started {
		h.started = true
		go h.work(p)
	}
	// Under the lock, because close closes wake under it: a send made after
	// unlocking could land on a closed channel, and that panics. It never blocks.
	select {
	case h.wake <- struct{}{}:
	default:
	}
}

// work re-renders for every batch of tags queued since the last, until the hub
// closes.
func (h *hub) work(p *Plugin) {
	for range h.wake {
		h.mu.Lock()
		if h.closed {
			h.mu.Unlock()
			return
		}
		touched := h.tags
		h.tags = make(map[string]bool)
		h.mu.Unlock()
		if len(touched) > 0 {
			h.invalidate(p, touched)
		}
	}
}

func (h *hub) add(s *Subscription) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false
	}
	h.subs[s] = struct{}{}
	return true
}

func (h *hub) remove(s *Subscription) {
	h.mu.Lock()
	delete(h.subs, s)
	h.mu.Unlock()
}

func (h *hub) snapshot() []*Subscription {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]*Subscription, 0, len(h.subs))
	for s := range h.subs {
		out = append(out, s)
	}
	return out
}

func (h *hub) close() {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.closed = true
	if h.started {
		close(h.wake)
	}
	subs := make([]*Subscription, 0, len(h.subs))
	for s := range h.subs {
		subs = append(subs, s)
	}
	clear(h.subs)
	h.mu.Unlock()
	for _, s := range subs {
		s.shut()
	}
}

// invalidate re-renders every watched fragment that depended on one of touched,
// and queues it for its subscription.
//
// A render that is Shared — the same for every reader — is made once per URL and
// handed to everyone watching that URL. Any other render may hold one reader's
// data: a session, a cookie, a name. It is made for each subscription, with that
// subscription's own request, and never handed to another. Getting this wrong
// would send one reader's page to another, which is why the framework, not this
// plugin, decides what Shared means.
//
// The renders run on a bounded number of goroutines. A URL is rendered first for
// one of its readers; only when that render is not Shared is it rendered for the
// others, each once. Batches still run one after another.
func (h *hub) invalidate(p *Plugin, touched map[string]bool) {
	readers := make(map[string][]*Subscription)
	var urls []string
	for _, s := range h.snapshot() {
		for _, url := range s.watching(touched) {
			if _, seen := readers[url]; !seen {
				urls = append(urls, url)
			}
			readers[url] = append(readers[url], s)
		}
	}
	sort.Strings(urls)

	// Two rounds, so no render waits on a slot another is holding: first one
	// reader per URL, then — for the URLs whose render was not Shared — the rest.
	type job struct {
		s   *Subscription
		url string
	}
	var mu sync.Mutex
	var rest []job
	parallel(len(urls), func(i int) {
		url := urls[i]
		subs := readers[url]
		first, ok := h.render(p, subs[0], url)
		if ok && first.Shared {
			for _, s := range subs[1:] {
				s.deliver(url, first)
			}
			return
		}
		mu.Lock()
		for _, s := range subs[1:] {
			rest = append(rest, job{s, url})
		}
		mu.Unlock()
	})
	parallel(len(rest), func(i int) { h.render(p, rest[i].s, rest[i].url) })
}

// parallel runs fn for 0..n-1 on at most GOMAXPROCS goroutines, and returns when
// all have finished.
func parallel(n int, fn func(int)) {
	limit := make(chan struct{}, runtime.GOMAXPROCS(0))
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		limit <- struct{}{}
		go func() {
			defer func() { <-limit; wg.Done() }()
			fn(i)
		}()
	}
	wg.Wait()
}

// render renders url for s and delivers it, or delivers a stale message.
func (h *hub) render(p *Plugin, s *Subscription, url string) (*collage.FragmentRender, bool) {
	render, err := p.host.RenderFragment(s.request, collage.FragmentRequest{Path: url})
	if err != nil {
		if s.request.Context().Err() == nil {
			p.log.Warn("live: render failed", "url", url, "err", err)
			s.deliverStale(url)
		}
		return nil, false
	}
	s.deliver(url, render)
	return render, true
}

func messageOf(url string, render *collage.FragmentRender) Message {
	msg := Message{URL: url, HTML: string(render.HTML), ETag: render.ETag}
	for _, item := range render.Head {
		msg.Head = append(msg.Head, HeadItem{Area: item.Area, Key: item.Key, HTML: string(item.HTML)})
	}
	return msg
}

func dedupe(watches []Watch) []Watch {
	seen := make(map[string]bool, len(watches))
	out := make([]Watch, 0, len(watches))
	for _, w := range watches {
		if w.URL != "" && !seen[w.URL] {
			seen[w.URL] = true
			out = append(out, w)
		}
	}
	return out
}
