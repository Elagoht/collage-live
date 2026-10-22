package live_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	client "github.com/Elagoht/collage-client"
	live "github.com/Elagoht/collage-live"
	"github.com/Elagoht/collage/pkg/collage"
	"github.com/Elagoht/collage/pkg/collagetest"
)

// site is a page with a CPU panel pushed on the "system:cpu" tag, a greeting that
// reads the reader's cookie, and a fixed footer.
type site struct {
	app    *collage.App
	plugin *live.Plugin
	cpu    atomic.Int64
	server *httptest.Server
}

func newSite(t *testing.T, plugin *live.Plugin, extra ...collagetest.SiteOption) *site {
	t.Helper()
	s := &site{plugin: plugin}
	cpu := collage.NewFragment("cpu", "cpu.html").WithData(collage.LoadTagged(func(context.Context, *collage.RenderContext) (int64, []string, error) {
		return s.cpu.Load(), []string{"system:cpu"}, nil
	})).Build()
	hello := collage.NewFragment("hello", "hello.html").WithData(collage.LoadTagged(func(_ context.Context, rc *collage.RenderContext) (string, []string, error) {
		name := "stranger"
		if c, err := rc.Request.Cookie("user"); err == nil {
			name = c.Value
		}
		return fmt.Sprintf("%s %d", name, s.cpu.Load()), []string{"greeting"}, nil
	})).Build()
	footer := collage.NewFragment("footer", "footer.html").Build()
	page := collage.NewFragment("page", "page.html").
		WithSlotFragment("cpu", cpu).WithSlotFragment("hello", hello).WithSlotFragment("footer", footer).Build()
	app := collagetest.NewSite(t, append([]collagetest.SiteOption{
		collagetest.WithoutHome(),
		collagetest.WithTemplates(map[string]string{
			"page.html":   `<html><head>{{liveClient}}</head><body>{{slot "cpu"}}{{slot "hello"}}{{slot "footer"}}</body></html>`,
			"cpu.html":    `<p>cpu {{.}}</p>`,
			"hello.html":  `<p>hello {{.}}</p>`,
			"footer.html": `<p>footer</p>`,
		}),
		collagetest.WithRegister(collage.NewPage("home").WithContent(page).WithPath("en", "/").
			WithFragmentPath("en", "/live/cpu", cpu).
			WithFragmentPath("en", "/live/hello", hello).
			WithFragmentPath("en", "/live/footer", footer).Build()),
		collagetest.WithPlugins(client.New(), plugin),
	}, extra...)...).App
	s.app = app
	s.server = httptest.NewServer(app.Handler())
	t.Cleanup(s.server.Close)
	return s
}

// events reads a stream's fragment events.
type events struct {
	t      *testing.T
	res    *http.Response
	lines  *bufio.Scanner
	cancel context.CancelFunc
}

func (s *site) open(t *testing.T, query string, cookie *http.Cookie) *events {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, s.server.URL+"/_live/stream/?"+query, nil)
	if cookie != nil {
		req.AddCookie(cookie)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	e := &events{t: t, res: res, lines: bufio.NewScanner(res.Body), cancel: cancel}
	t.Cleanup(func() { cancel(); res.Body.Close() })
	return e
}

// next returns the next fragment message, failing the test after a while.
func (e *events) next() live.Message {
	e.t.Helper()
	got := make(chan live.Message, 1)
	go func() {
		for e.lines.Scan() {
			line := e.lines.Text()
			if data, ok := strings.CutPrefix(line, "data: "); ok {
				var msg live.Message
				if json.Unmarshal([]byte(data), &msg) == nil {
					got <- msg
					return
				}
			}
		}
		close(got)
	}()
	select {
	case msg, ok := <-got:
		if !ok {
			e.t.Fatal("stream ended")
		}
		return msg
	case <-time.After(3 * time.Second):
		e.t.Fatal("no message within 3s")
	}
	return live.Message{}
}

func TestClientTagAndScript(t *testing.T) {
	s := newSite(t, live.New(live.Options{}))
	res, err := http.Get(s.server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	body := readAll(t, res)
	// collage-client's script first, at its content-addressed URL, then live's:
	// both deferred, so they run in that order.
	want := `.js" defer></script><script src="/_live/client/client.js" data-collage-worker="/_live/client/worker.js" data-collage-stream="/_live/stream/" data-collage-transport="sse" defer></script>`
	if !strings.Contains(body, want) || !strings.Contains(body, `<head><script src="/_client/client.`) {
		t.Errorf("page does not include collage-client's script, then live's:\n%s", body)
	}

	res, err = http.Get(s.server.URL + "/_live/client/client.js")
	if err != nil {
		t.Fatal(err)
	}
	if script := readAll(t, res); res.StatusCode != http.StatusOK || !strings.Contains(script, "data-collage-fragment") {
		t.Errorf("client.js = %d, %d bytes", res.StatusCode, len(script))
	}
	res, err = http.Get(s.server.URL + "/_live/client/worker.js")
	if err != nil {
		t.Fatal(err)
	}
	if script := readAll(t, res); res.StatusCode != http.StatusOK || !strings.Contains(script, "onconnect") {
		t.Errorf("worker.js = %d, %d bytes", res.StatusCode, len(script))
	}
}

func TestNoStream(t *testing.T) {
	s := newSite(t, live.New(live.Options{NoStream: true}))
	res, err := http.Get(s.server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	if body := readAll(t, res); strings.Contains(body, "data-collage-stream") {
		t.Errorf("a client without a stream names one:\n%s", body)
	}
	if res, _ := http.Get(s.server.URL + "/_live/stream/?f=/live/cpu"); res.StatusCode != http.StatusNotFound {
		t.Errorf("stream with NoStream = %d, want 404", res.StatusCode)
	}
}

// A null duration in configuration leaves the default, as an absent one does.
func TestNullDurationLeavesTheDefault(t *testing.T) {
	plugin := live.New(live.Options{})
	newSite(t, plugin, collagetest.WithPluginConfig(live.Name, `{"keepAlive":null,"maxStreamAge":null}`))
	if plugin.KeepAlive() != 25*time.Second || plugin.MaxStreamAge() != 0 {
		t.Errorf("KeepAlive %v, MaxStreamAge %v: want the defaults", plugin.KeepAlive(), plugin.MaxStreamAge())
	}
}

// reader is a plugin listed before live, as a transport is, that reads live's
// push settings in its own Init.
type reader struct {
	lv              *live.Plugin
	prefix          string
	keepAlive, aged time.Duration
}

func (*reader) Name() string                   { return "test/reader" }
func (*reader) Version() string                { return "0" }
func (*reader) Shutdown(context.Context) error { return nil }
func (r *reader) Init(context.Context, collage.Host) error {
	r.prefix, r.keepAlive, r.aged = r.lv.Prefix(), r.lv.KeepAlive(), r.lv.MaxStreamAge()
	return nil
}

// The push settings a transport of another plugin reads are final from any
// plugin's Init — one listed before live included — once every Configure has run.
func TestAccessors(t *testing.T) {
	plugin := live.New(live.Options{})
	if plugin.KeepAlive() != 25*time.Second || plugin.Prefix() != "/_live/" {
		t.Errorf("before configuration: KeepAlive %v, Prefix %q", plugin.KeepAlive(), plugin.Prefix())
	}
	r := &reader{lv: plugin}
	collagetest.NewSite(t, collagetest.WithTemplates(map[string]string{"layout.html": `<html><head>{{liveClient}}</head><body>{{slot "content"}}</body></html>`}),
		collagetest.WithPlugins(r, client.New(), plugin),
		collagetest.WithPluginConfig(live.Name, `{"keepAlive":"5s","prefix":"/push/","maxStreamAge":"1h"}`))
	if r.keepAlive != 5*time.Second || r.prefix != "/push/" || r.aged != time.Hour {
		t.Errorf("read in an earlier plugin's Init: KeepAlive %v, Prefix %q, MaxStreamAge %v", r.keepAlive, r.prefix, r.aged)
	}
}

// With noStream set, Subscribe refuses: a transport's endpoint cannot keep
// pushing what the operator turned off.
func TestSubscribeRefusedWithNoStream(t *testing.T) {
	plugin := live.New(live.Options{})
	plugin.UseTransport(live.Transport{Kind: "ws", Path: "/_live/ws/"})
	newSite(t, plugin, collagetest.WithPluginConfig(live.Name, `{"noStream":true}`))
	_, err := plugin.Subscribe(httptest.NewRequest(http.MethodGet, "/", nil), live.ParseWatches([]string{"/live/cpu"}))
	if !errors.Is(err, live.ErrNoStream) {
		t.Fatalf("Subscribe = %v, want ErrNoStream", err)
	}
}

// A negative number, or a duration written as a number, stops the application.
func TestRefusals(t *testing.T) {
	for _, config := range []string{`{"maxFragments":-1}`, `{"keepAlive":25}`, `{"maxStreamAge":"-1s"}`, `{"prefix":"live"}`} {
		if _, err := collagetest.TrySite(t, collagetest.WithPlugins(client.New(), live.New(live.Options{})),
			collagetest.WithPluginConfig(live.Name, config)); err == nil {
			t.Errorf("%s: the application started", config)
		}
	}
}

// Without collage-client, which puts what live receives into the page, the
// application does not start, wherever client would have been listed.
func TestRequiresClient(t *testing.T) {
	_, err := collagetest.TrySite(t, collagetest.WithPlugins(live.New(live.Options{})))
	if err == nil || !strings.Contains(err.Error(), "add elagoht/client") {
		t.Errorf("without collage-client: %v, want the application refused", err)
	}
	collagetest.NewSite(t, collagetest.WithPlugins(live.New(live.Options{}), client.New()))
}

// noStream turns push off even when another plugin's transport is in use: the
// client only polls, and names no push connection.
func TestNoStreamWinsOverATransport(t *testing.T) {
	plugin := live.New(live.Options{})
	plugin.UseTransport(live.Transport{Kind: "ws", Path: "/_live/ws/"})
	s := newSite(t, plugin, collagetest.WithPluginConfig(live.Name, `{"noStream":true}`))
	res, err := http.Get(s.server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	if body := readAll(t, res); strings.Contains(body, "data-collage-stream") || strings.Contains(body, "data-collage-transport") {
		t.Errorf("noStream with a transport still names a push connection:\n%s", body)
	}
}

// The stream sends each fragment as it stands, then pushes it again when a tag it
// depends on is invalidated — and only then.
func TestStream_PushesOnInvalidation(t *testing.T) {
	s := newSite(t, live.New(live.Options{}))
	s.cpu.Store(10)
	e := s.open(t, "f=/live/cpu&f=/live/footer", nil)
	if ct := e.res.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q", ct)
	}
	if got := e.res.Header.Get("X-Accel-Buffering"); got != "no" {
		t.Errorf("X-Accel-Buffering = %q, want no", got)
	}
	first := map[string]string{}
	for range 2 {
		msg := e.next()
		first[msg.URL] = msg.HTML
	}
	if first["/live/cpu"] != "<p>cpu 10</p>" || first["/live/footer"] != "<p>footer</p>" {
		t.Fatalf("initial state = %v", first)
	}

	s.cpu.Store(55)
	if err := s.app.InvalidateTags(context.Background(), "system:cpu"); err != nil {
		t.Fatal(err)
	}
	msg := e.next()
	if msg.URL != "/live/cpu" || msg.HTML != "<p>cpu 55</p>" {
		t.Errorf("pushed %+v, want the new cpu", msg)
	}

	// A tag nothing watched pushes nothing; the next message is the next cpu.
	_ = s.app.InvalidateTags(context.Background(), "unrelated")
	s.cpu.Store(70)
	_ = s.app.InvalidateTags(context.Background(), "system:cpu")
	if msg := e.next(); msg.URL != "/live/cpu" || msg.HTML != "<p>cpu 70</p>" {
		t.Errorf("pushed %+v, want cpu 70", msg)
	}
}

// A fragment that reads the reader's cookie is rendered for each reader. Sending
// one reader's render to another is the failure this plugin must never have.
func TestStream_PersonalFragmentsStayPersonal(t *testing.T) {
	s := newSite(t, live.New(live.Options{}))
	ada := s.open(t, "f=/live/hello", &http.Cookie{Name: "user", Value: "ada"})
	bo := s.open(t, "f=/live/hello", &http.Cookie{Name: "user", Value: "bo"})
	if got := ada.next().HTML; got != "<p>hello ada 0</p>" {
		t.Fatalf("ada's initial = %q", got)
	}
	if got := bo.next().HTML; got != "<p>hello bo 0</p>" {
		t.Fatalf("bo's initial = %q", got)
	}
	s.cpu.Store(1)
	_ = s.app.InvalidateTags(context.Background(), "greeting")
	if got := ada.next().HTML; got != "<p>hello ada 1</p>" {
		t.Errorf("ada was pushed %q", got)
	}
	if got := bo.next().HTML; got != "<p>hello bo 1</p>" {
		t.Errorf("bo was pushed %q", got)
	}
}

// A Shared render is pushed to the readers of its URL on its own host, and is
// made with nothing of any one reader's in it. Before, the first reader's render
// went to every reader of the URL: their cookie, and their host's origin.
func TestStream_SharedPushStaysOnItsHost(t *testing.T) {
	var n atomic.Int64
	tenant := collage.NewFragment("tenant", "t.html").WithData(collage.LoadTagged(func(_ context.Context, rc *collage.RenderContext) (string, []string, error) {
		who := "anon"
		if c, err := rc.Request.Cookie("user"); err == nil {
			who = c.Value
		}
		return fmt.Sprintf("host=%s user=%s n=%d", rc.Request.Host, who, n.Load()), []string{"news"}, nil
	})).Build()
	page := collage.NewFragment("page", "page.html").WithSlotFragment("tenant", tenant).Build()
	app := collagetest.NewSite(t,
		collagetest.WithoutHome(),
		collagetest.WithTemplates(map[string]string{
			"page.html": `<html><head>{{liveClient}}</head><body>{{slot "tenant"}}</body></html>`,
			"t.html":    `<p>{{.}}</p>`,
		}),
		collagetest.WithRegister(collage.NewPage("home").WithContent(page).WithPath("en", "/").
			WithFragmentPath("en", "/live/tenant", tenant).Incremental(time.Minute).Build()),
		collagetest.WithPlugins(client.New(), live.New(live.Options{})),
	).App
	s := &site{app: app, server: httptest.NewServer(app.Handler())}
	t.Cleanup(s.server.Close)

	openAt := func(host, user string) *events {
		ctx, cancel := context.WithCancel(context.Background())
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, s.server.URL+"/_live/stream/?f=/live/tenant", nil)
		req.Host = host
		req.AddCookie(&http.Cookie{Name: "user", Value: user})
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		t.Cleanup(func() { cancel(); res.Body.Close() })
		return &events{t: t, res: res, lines: bufio.NewScanner(res.Body), cancel: cancel}
	}
	a, b, a2 := openAt("a.test", "alice"), openAt("b.test", "bob"), openAt("a.test", "carol")
	a.next()
	b.next()
	a2.next()
	n.Store(1)
	_ = app.InvalidateTags(context.Background(), "news")
	for _, c := range []struct {
		e    *events
		want string
	}{{a, "<p>host=a.test user=anon n=1</p>"}, {b, "<p>host=b.test user=anon n=1</p>"}, {a2, "<p>host=a.test user=anon n=1</p>"}} {
		if got := c.e.next().HTML; got != c.want {
			t.Errorf("pushed %q, want %q", got, c.want)
		}
	}
}

// Nothing is reachable over the stream that is not reachable over HTTP.
func TestStream_RefusesWhatIsNotAFragmentPath(t *testing.T) {
	s := newSite(t, live.New(live.Options{}))
	for _, query := range []string{"f=/", "f=/nope", "", "f=https://example.com/live/cpu"} {
		res, err := http.Get(s.server.URL + "/_live/stream/?" + query)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest {
			t.Errorf("?%s = %d, want 400", query, res.StatusCode)
		}
	}
	s2 := newSite(t, live.New(live.Options{MaxFragments: 1}))
	res, _ := http.Get(s2.server.URL + "/_live/stream/?f=/live/cpu&f=/live/footer")
	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("more than MaxFragments = %d, want 400", res.StatusCode)
	}
}

// Shutting down ends every stream first, so the server is not kept waiting.
func TestCloseStreams(t *testing.T) {
	plugin := live.New(live.Options{})
	s := newSite(t, plugin)
	e := s.open(t, "f=/live/footer", nil)
	e.next()
	plugin.CloseStreams()
	done := make(chan struct{})
	go func() {
		for e.lines.Scan() {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the stream is still open after CloseStreams")
	}
	res, _ := http.Get(s.server.URL + "/_live/stream/?f=/live/footer")
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("a stream opened after CloseStreams = %d, want 503", res.StatusCode)
	}
}

// Wait hands back one message per fragment however often it changed meanwhile.
func TestSubscription_Coalesces(t *testing.T) {
	plugin := live.New(live.Options{})
	s := newSite(t, plugin)
	sub, err := plugin.Subscribe(httptest.NewRequest(http.MethodGet, "/", nil), live.ParseWatches([]string{"/live/cpu", "/live/cpu"}))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if n := len(sub.Initial()); n != 1 {
		t.Errorf("Initial has %d messages for one fragment asked twice", n)
	}
	for i := range 5 {
		s.cpu.Store(int64(i))
		_ = s.app.InvalidateTags(context.Background(), "system:cpu")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var last live.Message
	for last.HTML != "<p>cpu 4</p>" {
		msgs, err := sub.Wait(ctx)
		if err != nil {
			t.Fatalf("Wait: %v (last %q)", err, last.HTML)
		}
		if len(msgs) != 1 {
			t.Fatalf("Wait returned %d messages for one fragment", len(msgs))
		}
		last = msgs[0]
	}
	sub.Close()
	if _, err := sub.Wait(ctx); !errors.Is(err, live.ErrClosed) {
		t.Errorf("Wait after Close = %v, want ErrClosed", err)
	}
}

func readAll(t *testing.T, res *http.Response) string {
	t.Helper()
	defer res.Body.Close()
	var b strings.Builder
	sc := bufio.NewScanner(res.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		b.WriteString(sc.Text())
		b.WriteString("\n")
	}
	return b.String()
}

// A watch carrying the ETag of what the client shows is not sent again when the
// stream opens; one whose copy is out of date is.
func TestStream_SkipsWhatTheClientHolds(t *testing.T) {
	s := newSite(t, live.New(live.Options{}))
	s.cpu.Store(1)
	e := s.open(t, "f=/live/cpu&f=/live/footer", nil)
	etags := map[string]string{}
	for range 2 {
		msg := e.next()
		if msg.ETag == "" {
			t.Fatalf("message without an ETag: %+v", msg)
		}
		etags[msg.URL] = msg.ETag
	}
	res, err := http.Get(s.server.URL + "/live/footer")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if got := res.Header.Get("ETag"); got != etags["/live/footer"] {
		t.Errorf("pushed ETag %q, fragment path ETag %q: a client could not tell them apart", etags["/live/footer"], got)
	}

	s.cpu.Store(2)
	again := s.open(t, "f="+url.QueryEscape("/live/footer#"+etags["/live/footer"])+"&f="+url.QueryEscape("/live/cpu#"+etags["/live/cpu"]), nil)
	msg := again.next()
	if msg.URL != "/live/cpu" || msg.HTML != "<p>cpu 2</p>" {
		t.Errorf("first message %+v, want only the cpu, which changed", msg)
	}
}

// An invalidation that changed nothing a fragment shows sends nothing.
func TestStream_UnchangedIsNotPushed(t *testing.T) {
	s := newSite(t, live.New(live.Options{}))
	s.cpu.Store(5)
	e := s.open(t, "f=/live/cpu", nil)
	e.next()
	_ = s.app.InvalidateTags(context.Background(), "system:cpu") // still 5
	s.cpu.Store(6)
	_ = s.app.InvalidateTags(context.Background(), "system:cpu")
	if msg := e.next(); msg.HTML != "<p>cpu 6</p>" {
		t.Errorf("pushed %+v, want the change and not the repeat", msg)
	}
}

// TestStream_MaxStreamAgeShorterThanKeepAlive: the age ends the stream on time
// even when the keep-alive wait is far longer; it used to be checked only between
// keep-alive waits, so a stream outlived its age by up to keepAlive.
func TestStream_MaxStreamAgeShorterThanKeepAlive(t *testing.T) {
	s := newSite(t, live.New(live.Options{MaxStreamAge: collage.Duration(200 * time.Millisecond), KeepAlive: collage.Duration(time.Minute)}))
	e := s.open(t, "f=/live/footer", nil)
	e.next()
	done := make(chan struct{})
	go func() {
		for e.lines.Scan() {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the stream outlived MaxStreamAge while waiting a keep-alive")
	}
}

// MaxStreamAge ends a stream, so the browser reconnects with the cookies it holds
// now.
func TestStream_MaxStreamAge(t *testing.T) {
	s := newSite(t, live.New(live.Options{MaxStreamAge: collage.Duration(200 * time.Millisecond), KeepAlive: collage.Duration(50 * time.Millisecond)}))
	e := s.open(t, "f=/live/footer", nil)
	e.next()
	done := make(chan struct{})
	go func() {
		for e.lines.Scan() {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the stream outlived MaxStreamAge")
	}
}

// A Shared render is made once, but every reader it goes to is still asked
// about by the fragment path's guards: the fragment's own, and the page's. A
// reader whose access was revoked on the server is sent a stale message, as a
// render made for them alone would be refused, not the render made for the
// others. Before, the first reader's render went to every reader of the URL, and
// a revoked reader kept receiving a guarded fragment until MaxStreamAge.
//
// A Shared fragment no guard covers is still rendered once per change, however
// many read it. Each of four readers is revoked in turn: a revoked reader the
// hub renders for first is refused and sends everyone down the per-reader path,
// which would hide the leak were it always the same reader.
func TestStream_SharedPushAsksEachReadersGuard(t *testing.T) {
	var revoked sync.Map // user → true, the server's record of who lost access
	guard := func(_ context.Context, r *http.Request) (*collage.GuardDecision, error) {
		if c, err := r.Cookie("user"); err == nil {
			if _, out := revoked.Load(c.Value); out {
				return &collage.GuardDecision{Status: http.StatusForbidden}, nil
			}
		}
		return nil, nil
	}
	var n, boardRenders atomic.Int64
	shared := func(name string, count *atomic.Int64) *collage.FragmentBuilder {
		return collage.NewFragment(name, "v.html").WithData(collage.LoadTagged(func(context.Context, *collage.RenderContext) (string, []string, error) {
			if count != nil {
				count.Add(1)
			}
			return fmt.Sprintf("%s %d", name, n.Load()), []string{"news"}, nil
		})).Shared()
	}
	board := shared("board", &boardRenders).Build()
	news := shared("news", nil).WithGuard(guard).Build()
	feed := shared("feed", nil).Build()
	home := collage.NewFragment("home", "home.html").WithSlotFragment("board", board).WithSlotFragment("news", news).Build()
	private := collage.NewFragment("private", "private.html").WithSlotFragment("feed", feed).WithGuard(guard).Build()
	app := collagetest.NewSite(t,
		collagetest.WithoutHome(),
		collagetest.WithTemplates(map[string]string{
			"home.html":    `<html><head>{{liveClient}}</head><body>{{slot "board"}}{{slot "news"}}</body></html>`,
			"private.html": `<html><head>{{liveClient}}</head><body>{{slot "feed"}}</body></html>`,
			"v.html":       `<p>{{.}}</p>`,
		}),
		collagetest.WithRegister(
			collage.NewPage("home").WithContent(home).WithPath("en", "/").
				WithFragmentPath("en", "/live/board", board).
				WithFragmentPath("en", "/live/news", news).Build(),
			collage.NewPage("private").WithContent(private).WithPath("en", "/private").
				WithFragmentPath("en", "/private/feed", feed).Build(),
		),
		collagetest.WithPlugins(client.New(), live.New(live.Options{})),
	).App
	s := &site{app: app, server: httptest.NewServer(app.Handler())}
	t.Cleanup(s.server.Close)

	query := "f=/live/board&f=/live/news&f=/private/feed"
	users := []string{"ada", "bo", "cy", "di"}
	streams := make(map[string]*events, len(users))
	for _, user := range users {
		e := s.open(t, query, &http.Cookie{Name: "user", Value: user})
		for range 3 {
			if msg := e.next(); msg.Stale {
				t.Fatalf("%s's initial %s is stale", user, msg.URL)
			}
		}
		streams[user] = e
	}

	// Revoked on the server: the cookie a stream was opened with is the same, but
	// the guard now refuses it. Each reader takes a turn, so whichever the hub
	// happens to render for first, some round revokes another.
	for round := int64(1); round <= 8; round++ {
		out := users[round%int64(len(users))]
		revoked.Clear()
		revoked.Store(out, true)
		n.Store(round)
		before := boardRenders.Load()
		if err := app.InvalidateTags(context.Background(), "news"); err != nil {
			t.Fatal(err)
		}
		for _, user := range users {
			got := map[string]live.Message{}
			for range 3 {
				msg := streams[user].next()
				got[msg.URL] = msg
			}
			if want := fmt.Sprintf("<p>board %d</p>", round); got["/live/board"].HTML != want {
				t.Errorf("round %d: %s's board = %+v, want %q", round, user, got["/live/board"], want)
			}
			for _, url := range []string{"/live/news", "/private/feed"} {
				msg := got[url]
				if user == out {
					if !msg.Stale || msg.HTML != "" {
						t.Errorf("round %d: revoked %s was pushed %s = %+v, want a stale message", round, user, url, msg)
					}
					continue
				}
				if msg.Stale || !strings.Contains(msg.HTML, fmt.Sprintf(" %d<", round)) {
					t.Errorf("round %d: %s was pushed %s = %+v", round, user, url, msg)
				}
			}
		}
		if got := boardRenders.Load() - before; got != 1 {
			t.Errorf("round %d: the unguarded Shared board was rendered %d times for %d readers, want once", round, got, len(users))
		}
	}
}

// In an application where no guard covers a fragment path, a Shared render is
// made once per change whatever its path looks like — a placeholder pattern
// included — however many read it.
func TestStream_SharedPushRendersOnceWithoutGuards(t *testing.T) {
	var n, renders atomic.Int64
	item := collage.NewFragment("item", "item.html").WithData(collage.LoadTagged(func(_ context.Context, rc *collage.RenderContext) (string, []string, error) {
		renders.Add(1)
		return fmt.Sprintf("%s %d", rc.PathParams["id"], n.Load()), []string{"items"}, nil
	})).Shared().Build()
	page := collage.NewFragment("page", "page.html").WithSlotFragment("item", item).Build()
	app := collagetest.NewSite(t,
		collagetest.WithoutHome(),
		collagetest.WithTemplates(map[string]string{
			"page.html": `<html><head>{{liveClient}}</head><body>{{slot "item"}}</body></html>`,
			"item.html": `<p>item {{.}}</p>`,
		}),
		collagetest.WithRegister(collage.NewPage("item").WithContent(page).WithPath("en", "/items/{id}").
			WithFragmentPath("en", "/live/items/{id}", item).Build()),
		collagetest.WithPlugins(client.New(), live.New(live.Options{})),
	).App
	s := &site{app: app, server: httptest.NewServer(app.Handler())}
	t.Cleanup(s.server.Close)

	var streams []*events
	for range 4 {
		e := s.open(t, "f=/live/items/7", nil)
		if got := e.next().HTML; got != "<p>item 7 0</p>" {
			t.Fatalf("initial = %q", got)
		}
		streams = append(streams, e)
	}
	for round := int64(1); round <= 3; round++ {
		n.Store(round)
		before := renders.Load()
		if err := app.InvalidateTags(context.Background(), "items"); err != nil {
			t.Fatal(err)
		}
		want := fmt.Sprintf("<p>item 7 %d</p>", round)
		for _, e := range streams {
			if got := e.next().HTML; got != want {
				t.Errorf("round %d: pushed %q, want %q", round, got, want)
			}
		}
		if got := renders.Load() - before; got != 1 {
			t.Errorf("round %d: rendered %d times for %d readers, want once", round, got, len(streams))
		}
	}
}
