package live_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	live "github.com/Elagoht/collage-live"
	"github.com/Elagoht/collage/pkg/collage"
)

// site is a page with a CPU panel pushed on the "system:cpu" tag, a greeting that
// reads the reader's cookie, and a fixed footer.
type site struct {
	app    *collage.App
	plugin *live.Plugin
	cpu    atomic.Int64
	server *httptest.Server
}

func newSite(t *testing.T, plugin *live.Plugin) *site {
	t.Helper()
	s := &site{plugin: plugin}
	app, err := collage.New(&collage.Config{
		Server: collage.ServerConfig{Host: "localhost", Port: 3000},
		Template: collage.TemplateConfig{FS: fstest.MapFS{
			"t/page.html":   {Data: []byte(`<html><head>{{liveClient}}</head><body>{{slot "cpu"}}{{slot "hello"}}{{slot "footer"}}</body></html>`)},
			"t/cpu.html":    {Data: []byte(`<p>cpu {{.}}</p>`)},
			"t/hello.html":  {Data: []byte(`<p>hello {{.}}</p>`)},
			"t/footer.html": {Data: []byte(`<p>footer</p>`)},
		}, Root: "t"},
		Plugins: []collage.Plugin{plugin},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cpu := collage.NewFragment("cpu", "cpu.html").WithDataHandler(func(context.Context, *collage.RenderContext) (any, []string, error) {
		return s.cpu.Load(), []string{"system:cpu"}, nil
	}).Build()
	hello := collage.NewFragment("hello", "hello.html").WithDataHandler(func(_ context.Context, rc *collage.RenderContext) (any, []string, error) {
		name := "stranger"
		if c, err := rc.Request.Cookie("user"); err == nil {
			name = c.Value
		}
		return name, []string{"greeting"}, nil
	}).Build()
	footer := collage.NewFragment("footer", "footer.html").Build()
	page := collage.NewFragment("page", "page.html").
		WithSlotFragment("cpu", cpu).WithSlotFragment("hello", hello).WithSlotFragment("footer", footer).Build()
	if err := app.RegisterPage(collage.NewPage("home").WithContent(page).WithPath("en", "/").
		WithFragmentPath("en", "/live/cpu", cpu).
		WithFragmentPath("en", "/live/hello", hello).
		WithFragmentPath("en", "/live/footer", footer).Build()); err != nil {
		t.Fatal(err)
	}
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
	s := newSite(t, live.New())
	res, err := http.Get(s.server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	body := readAll(t, res)
	want := `<script src="/_live/client/client.js" data-collage-stream="/_live/stream/" data-collage-transport="sse" defer></script>`
	if !strings.Contains(body, want) {
		t.Errorf("page does not include the client:\n%s", body)
	}

	res, err = http.Get(s.server.URL + "/_live/client/client.js")
	if err != nil {
		t.Fatal(err)
	}
	if script := readAll(t, res); res.StatusCode != http.StatusOK || !strings.Contains(script, "data-collage-fragment") {
		t.Errorf("client.js = %d, %d bytes", res.StatusCode, len(script))
	}
}

func TestNoStream(t *testing.T) {
	s := newSite(t, live.NewWith(live.Config{NoStream: true}))
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

// The stream sends each fragment as it stands, then pushes it again when a tag it
// depends on is invalidated — and only then.
func TestStream_PushesOnInvalidation(t *testing.T) {
	s := newSite(t, live.New())
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
	s := newSite(t, live.New())
	ada := s.open(t, "f=/live/hello", &http.Cookie{Name: "user", Value: "ada"})
	bo := s.open(t, "f=/live/hello", &http.Cookie{Name: "user", Value: "bo"})
	if got := ada.next().HTML; got != "<p>hello ada</p>" {
		t.Fatalf("ada's initial = %q", got)
	}
	if got := bo.next().HTML; got != "<p>hello bo</p>" {
		t.Fatalf("bo's initial = %q", got)
	}
	_ = s.app.InvalidateTags(context.Background(), "greeting")
	if got := ada.next().HTML; got != "<p>hello ada</p>" {
		t.Errorf("ada was pushed %q", got)
	}
	if got := bo.next().HTML; got != "<p>hello bo</p>" {
		t.Errorf("bo was pushed %q", got)
	}
}

// Nothing is reachable over the stream that is not reachable over HTTP.
func TestStream_RefusesWhatIsNotAFragmentPath(t *testing.T) {
	s := newSite(t, live.New())
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
	s2 := newSite(t, live.NewWith(live.Config{MaxFragments: 1}))
	res, _ := http.Get(s2.server.URL + "/_live/stream/?f=/live/cpu&f=/live/footer")
	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("more than MaxFragments = %d, want 400", res.StatusCode)
	}
}

// Shutting down ends every stream first, so the server is not kept waiting.
func TestCloseStreams(t *testing.T) {
	plugin := live.New()
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
	plugin := live.New()
	s := newSite(t, plugin)
	sub, err := plugin.Subscribe(httptest.NewRequest(http.MethodGet, "/", nil), []string{"/live/cpu", "/live/cpu"})
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
