package live

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
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

// ErrTooManyFragments is returned by Subscribe for more URLs than MaxFragments.
var ErrTooManyFragments = errors.New("live: too many fragments for one stream")

// ErrNoFragments is returned by Subscribe given no URL to watch.
var ErrNoFragments = errors.New("live: no fragment to watch")

// Subscription is one reader watching some fragments: what an open stream or
// WebSocket holds. A transport subscribes, sends Initial and the Cookie, then loops
// on Wait and sends what it returns, until Wait fails or the connection ends, and
// then calls Close.
type Subscription struct {
	hub     *hub
	request *http.Request
	initial []Message
	cookie  *http.Cookie

	mu      sync.Mutex
	tags    map[string][]string // url → the tags its last render depended on
	pending map[string]Message
	order   []string
	ready   chan struct{}
	closed  bool
}

// Subscribe starts watching urls — fragment paths, as {{fragmentURL}} built them —
// for the reader r came from.
//
// Each is rendered once now, for r: that is how the subscription learns which tags
// the fragment depends on, and it hands the client the current state, which may
// have moved on since the page was served — or since a stream dropped and
// reconnected. Replaying the messages a reconnecting client missed would be
// pointless: a fragment is a state, not a log, and only the latest one matters.
//
// A URL that is not a fragment path fails the subscription with
// collage.ErrUnknownFragmentPath, so nothing is reachable over a stream that is
// not reachable over HTTP.
func (p *Plugin) Subscribe(r *http.Request, urls []string) (*Subscription, error) {
	if p.host == nil {
		return nil, errors.New("live: the plugin has not been initialised")
	}
	urls = dedupe(urls)
	if len(urls) == 0 {
		return nil, ErrNoFragments
	}
	if max := p.cfg.MaxFragments; max > 0 && len(urls) > max {
		return nil, fmt.Errorf("%w: %d, at most %d", ErrTooManyFragments, len(urls), max)
	}

	s := &Subscription{
		hub:     p.hub,
		request: r,
		tags:    make(map[string][]string, len(urls)),
		pending: make(map[string]Message),
		ready:   make(chan struct{}, 1),
	}
	for _, url := range urls {
		render, err := p.host.RenderFragment(r, collage.FragmentRequest{Path: url})
		if errors.Is(err, collage.ErrUnknownFragmentPath) {
			return nil, err
		}
		if err != nil {
			p.log.Warn("live: render failed", "url", url, "err", err)
			s.initial = append(s.initial, Message{URL: url, Stale: true})
			s.tags[url] = nil
			continue
		}
		if render.Cookie != nil && s.cookie == nil {
			s.cookie = render.Cookie
		}
		s.tags[url] = render.DependencyTags
		s.initial = append(s.initial, messageOf(url, render))
	}
	if !p.hub.add(s) {
		return nil, ErrClosed
	}
	return s, nil
}

// Initial is each watched fragment as it stands now, to send first.
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

// push queues msg and records the tags its render depended on.
func (s *Subscription) push(msg Message, tags []string, retag bool) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	if retag {
		s.tags[msg.URL] = tags
	}
	if _, queued := s.pending[msg.URL]; !queued {
		s.order = append(s.order, msg.URL)
	}
	s.pending[msg.URL] = msg
	s.mu.Unlock()
	s.signal()
}

// hub is every open subscription, and the one worker that re-renders for them.
type hub struct {
	mu     sync.Mutex
	subs   map[*Subscription]struct{}
	closed bool

	// tags waiting for the worker. One worker, draining them all at once, rather
	// than a goroutine per invalidation: two renders of one fragment running side
	// by side can finish in either order, and the older one arriving last would
	// leave the reader looking at the past.
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
	if h.closed {
		h.mu.Unlock()
		return
	}
	for _, tag := range tags {
		h.tags[tag] = true
	}
	if !h.started {
		h.started = true
		go h.work(p)
	}
	h.mu.Unlock()
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

// invalidate re-renders every watched fragment that depended on one of tags, and
// queues it for its subscription.
//
// A render that is Shared — the same for every reader — is made once per URL and
// handed to everyone watching that URL. Any other render may hold one reader's
// data: a session, a cookie, a name. It is made for each subscription, with that
// subscription's own request, and never handed to another. Getting this wrong
// would send one reader's page to another, which is why the framework, not this
// plugin, decides what Shared means.
func (h *hub) invalidate(p *Plugin, touched map[string]bool) {
	shared := make(map[string]*collage.FragmentRender)
	for _, s := range h.snapshot() {
		for _, url := range s.watching(touched) {
			if render, ok := shared[url]; ok {
				s.push(messageOf(url, render), render.DependencyTags, true)
				continue
			}
			render, err := p.host.RenderFragment(s.request, collage.FragmentRequest{Path: url})
			if err != nil {
				if s.request.Context().Err() == nil {
					p.log.Warn("live: render failed", "url", url, "err", err)
					s.push(Message{URL: url, Stale: true}, nil, false)
				}
				continue
			}
			if render.Shared {
				shared[url] = render
			}
			s.push(messageOf(url, render), render.DependencyTags, true)
		}
	}
}

func messageOf(url string, render *collage.FragmentRender) Message {
	msg := Message{URL: url, HTML: string(render.HTML)}
	for _, item := range render.Head {
		msg.Head = append(msg.Head, HeadItem{Area: item.Area, Key: item.Key, HTML: string(item.HTML)})
	}
	return msg
}

func dedupe(urls []string) []string {
	seen := make(map[string]bool, len(urls))
	out := make([]string, 0, len(urls))
	for _, url := range urls {
		if url != "" && !seen[url] {
			seen[url] = true
			out = append(out, url)
		}
	}
	return out
}
