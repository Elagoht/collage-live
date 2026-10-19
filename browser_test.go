//go:build unix

package live_test

// The browser test: the served client in a real headless Chrome, against a real
// collage application with collage-client. The page runs its checks once loaded
// and POSTs what it observed; the Go side asserts on that, because the open
// event stream would keep a dumped DOM from ever finishing. Chrome is found at
// $CHROME (and only there, when it is set), else in the usual places; without it
// the test skips.

import (
	"bytes"
	"context"
	"encoding/json"
	"html"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	client "github.com/Elagoht/collage-client"
	live "github.com/Elagoht/collage-live"
	"github.com/Elagoht/collage/pkg/collage"
	"github.com/Elagoht/collage/pkg/collagetest"
)

// browserTimeout bounds the Chrome run, from launch to the result POST.
const browserTimeout = 30 * time.Second

const browserPage = `<!doctype html><html><head><title>t</title>{{liveClient}}</head><body>
<ul id="items" data-collage-fragment="{{fragmentURL "home" "items"}}" data-collage-push data-collage-swap="morph">{{slot "items"}}</ul>
<div id="count" data-collage-fragment="{{fragmentURL "home" "count"}}" data-collage-interval="200ms">{{slot "count"}}</div>
<form id="add" method="post" action="/_add" data-collage-target="#reply"><input name="item" value="milk"></form>
<div id="reply"></div>
<script>
document.addEventListener("DOMContentLoaded", async function () {
  var checks = {}, notes = {}, events = [];
  function check(name, ok) { checks[name] = !!ok; }
  function sleep(ms) { return new Promise(function (r) { setTimeout(r, ms); }); }
  async function until(cond, ms) {
    for (var end = Date.now() + ms; Date.now() < end; await sleep(25)) if (cond()) return true;
    return cond();
  }
  function of(id) { return events.filter(function (e) { return e.target.id === id; }); }
  document.addEventListener("collage:swap", function (e) {
    events.push({ target: e.target, detail: e.detail || {} });
  });
  try {
    check("loaded", !!window.collage && !!window.collageLive);
    var items = document.getElementById("items");
    // Settle: the stream opens, and the copy it sends first is what the page
    // already shows, which changes nothing.
    await sleep(500);
    check("firstCopyQuiet", of("items").length === 0);

    // A push puts an item at the top of the list while the reader types further
    // down: the morph matches what is there by id, so every kept node stays the
    // same node, and the field keeps what is typed, its focus and its caret.
    var note = document.getElementById("note"), b = document.getElementById("i-b");
    note.focus();
    note.value = "draft";
    note.setSelectionRange(1, 3);
    await fetch("/_push?item=a", { method: "POST" });
    var pushed = await until(function () { return items.firstElementChild.id === "i-a"; }, 5000);
    check("pushPrepends", pushed && items.children.length === 4);
    check("pushKeepsNodes", document.getElementById("i-b") === b && document.getElementById("note") === note);
    check("pushKeepsTyping", note.value === "draft" && document.activeElement === note &&
      note.selectionStart === 1 && note.selectionEnd === 3);
    var ev = of("items");
    check("pushOneEvent", ev.length === 1 && ev[0].detail.mode === "morph" && ev[0].detail.inner === true);
    check("pushFresh", !items.hasAttribute("data-collage-stale"));

    // The polled element is replaced inside, the element itself kept.
    var count = document.getElementById("count"), first = count.textContent;
    var polled = await until(function () { return count.textContent !== first; }, 3000);
    ev = of("count");
    check("pollReplaces", polled && document.getElementById("count") === count &&
      ev.length >= 1 && ev[0].detail.mode === "replace" && ev[0].detail.inner === true);

    // collage:before-swap hands the swap over; detail.swap() changes the page
    // before it returns.
    var held;
    items.addEventListener("collage:before-swap", function (e) {
      e.preventDefault();
      held = e.detail.swap;
    }, { once: true });
    var n = events.length;
    collageLive.put(items, '<li id="i-x">x</li>');
    check("beforeSwapHolds", typeof held === "function" && items.firstElementChild.id === "i-a" && events.length === n);
    held();
    check("beforeSwapApplies", items.children.length === 1 && items.firstElementChild.id === "i-x" && events.length === n + 1);

    // pause holds an element still; resume puts in the latest answer at once.
    collageLive.pause(items);
    collageLive.put(items, '<li id="i-y">y</li>');
    collageLive.put(items, '<li id="i-z">z</li>');
    check("pauseHolds", items.firstElementChild.id === "i-x" && items.hasAttribute("data-collage-paused"));
    n = events.length;
    collageLive.resume(items);
    check("resumeApplies", items.firstElementChild.id === "i-z" && !items.hasAttribute("data-collage-paused") &&
      events.length === n + 1);

    // A form marked data-collage-target posts with fetch and fills the target.
    var reply = document.getElementById("reply");
    document.getElementById("add").requestSubmit();
    var answered = await until(function () { return reply.textContent === "got milk"; }, 3000);
    ev = of("reply");
    check("formFills", answered && reply.firstElementChild.tagName === "P" &&
      ev.length === 1 && ev[0].detail.mode === "replace" && ev[0].detail.inner === true);
  } catch (e) {
    notes.error = String(e && e.stack || e);
  }
  await fetch("/_result", { method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ checks: checks, notes: notes }) });
});
</script>
</body></html>`

const browserItems = `{{range .}}<li id="i-{{.}}">{{.}}</li>{{end}}<li id="i-note"><textarea id="note"></textarea></li>`

// observed is what the page reports.
type observed struct {
	Checks map[string]bool   `json:"checks"`
	Notes  map[string]string `json:"notes"`
}

// browserSite serves the page: a pushed list of items, a polled counter, and a
// form whose answer fills a target.
func browserSite(t *testing.T) (string, <-chan []byte) {
	t.Helper()
	var mu sync.Mutex
	list := []string{"b", "c"}
	var renders atomic.Int64
	itemsFragment := collage.NewFragment("items", "items.html").WithData(collage.LoadTagged(func(context.Context, *collage.RenderContext) ([]string, []string, error) {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), list...), []string{"items"}, nil
	})).Build()
	countFragment := collage.NewFragment("count", "count.html").WithData(collage.Load(func(context.Context, *collage.RenderContext) (int64, error) {
		return renders.Add(1), nil
	})).Build()
	page := collage.NewFragment("page", "page.html").
		WithSlotFragment("items", itemsFragment).WithSlotFragment("count", countFragment).Build()
	results := make(chan []byte, 1)
	app := collagetest.NewSite(t,
		collagetest.WithoutHome(),
		collagetest.WithTemplates(map[string]string{
			"page.html":  browserPage,
			"items.html": browserItems,
			"count.html": `<b>{{.}}</b>`,
		}),
		collagetest.WithRegister(collage.NewPage("home").WithContent(page).WithPath("en", "/").
			WithFragmentPath("en", "/live/items", itemsFragment).
			WithFragmentPath("en", "/live/count", countFragment).Build()),
		collagetest.WithPlugins(client.New(), live.New(live.Options{})),
		collagetest.WithSetup(func(app *collage.App) error {
			if err := app.Handle("/_push", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				list = append([]string{r.URL.Query().Get("item")}, list...)
				mu.Unlock()
				if err := app.InvalidateTags(r.Context(), "items"); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				w.WriteHeader(http.StatusNoContent)
			})); err != nil {
				return err
			}
			if err := app.Handle("/_add", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				_, _ = io.WriteString(w, "<p>got "+html.EscapeString(r.FormValue("item"))+"</p>")
			})); err != nil {
				return err
			}
			return app.Handle("/_result", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				select {
				case results <- b:
				default:
				}
				w.WriteHeader(http.StatusNoContent)
			}))
		}),
	).App
	srv := httptest.NewServer(app.Handler())
	t.Cleanup(srv.Close)
	return srv.URL, results
}

// findChrome is $CHROME when it is set (an unusable value means no Chrome, so
// CHROME=/nonexistent skips), else the first browser found in the usual places.
func findChrome() string {
	if c, ok := os.LookupEnv("CHROME"); ok {
		if p, err := exec.LookPath(c); err == nil {
			return p
		}
		return ""
	}
	for _, c := range []string{
		"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
		"google-chrome",
		"chromium",
	} {
		if p, err := exec.LookPath(c); err == nil {
			return p
		}
	}
	return ""
}

// browserChrome is the Chrome to run, or why the browser test skips. On CI
// (CI=true) it runs only when CHROME is set explicitly.
func browserChrome() (path, skipReason string) {
	if _, set := os.LookupEnv("CHROME"); !set && os.Getenv("CI") == "true" {
		return "", "CI: set CHROME to run the browser test"
	}
	if c := findChrome(); c != "" {
		return c, ""
	}
	return "", "no Chrome: set CHROME or install Google Chrome to run the browser test"
}

// runChrome opens pageURL in a fresh headless Chrome profile and returns the
// result the page POSTs. Chrome is killed, with its whole process group, before
// the profile directory is removed.
func runChrome(t *testing.T, chrome, pageURL string, results <-chan []byte) []byte {
	t.Helper()
	profile := t.TempDir()
	var output bytes.Buffer
	cmd := exec.Command(chrome,
		"--headless=new", "--disable-gpu", "--no-first-run", "--no-default-browser-check",
		"--disable-extensions", "--disable-background-networking", "--disable-sync",
		"--user-data-dir="+profile, pageURL)
	cmd.Stdout, cmd.Stderr = &output, &output
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start chrome: %v", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	done := false
	stop := func() {
		if done {
			return
		}
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-exited
		done = true
	}
	defer stop()

	timer := time.NewTimer(browserTimeout)
	defer timer.Stop()
	select {
	case b := <-results:
		return b
	case err := <-exited:
		done = true
		t.Fatalf("chrome exited before the page reported: %v\n%s", err, output.String())
	case <-timer.C:
		t.Fatalf("no result within %s\n%s", browserTimeout, output.String())
	}
	return nil
}

func TestBrowser(t *testing.T) {
	chrome, why := browserChrome()
	if chrome == "" {
		t.Skip(why)
	}
	base, results := browserSite(t)
	raw := runChrome(t, chrome, base+"/", results)
	var o observed
	if err := json.Unmarshal(raw, &o); err != nil {
		t.Fatalf("result %s: %v", raw, err)
	}
	if o.Notes["error"] != "" {
		t.Fatalf("page error: %s", o.Notes["error"])
	}
	want := []string{
		"loaded", "firstCopyQuiet",
		"pushPrepends", "pushKeepsNodes", "pushKeepsTyping", "pushOneEvent", "pushFresh",
		"pollReplaces", "beforeSwapHolds", "beforeSwapApplies", "pauseHolds", "resumeApplies", "formFills",
	}
	for _, name := range want {
		if v, ok := o.Checks[name]; !ok || !v {
			t.Errorf("check %s = %v (reported: %v); note %q", name, v, ok, o.Notes[name])
		}
	}
	if len(o.Checks) != len(want) {
		t.Errorf("%d checks reported, want %d", len(o.Checks), len(want))
	}
	if t.Failed() {
		t.Logf("observed: %s", raw)
	}
}
