package live

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/Elagoht/collage/pkg/collage"
)

// writeTimeout bounds one write to a stream. The server's WriteTimeout is for
// responses that end, and would cut a stream every thirty seconds; the deadline is
// pushed forward before each write instead, so a reader that stopped reading is
// still let go.
const writeTimeout = 10 * time.Second

// sseHandler serves the event stream: one per page, whatever it watches.
//
//	GET /_live/stream/?f=/live/cpu&f=/posts/hello/comments
//
//	event: fragment
//	id: 3
//	data: {"url":"/live/cpu","html":"<p>…</p>","head":[…]}
//
// One stream per page rather than per fragment, because a browser opens at most
// six connections to an origin over HTTP/1.1, and a page of seven live panels
// would otherwise stop loading anything else. The data is JSON rather than the
// markup split over data: lines, so the URL and the hoisted items travel in the
// same message as the markup they belong to.
type sseHandler struct {
	plugin *Plugin
}

func (h *sseHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	sub, err := h.plugin.Subscribe(r, r.URL.Query()["f"])
	switch {
	case errors.Is(err, ErrClosed):
		http.Error(w, "shutting down", http.StatusServiceUnavailable)
		return
	case errors.Is(err, collage.ErrUnknownFragmentPath), errors.Is(err, ErrNoFragments), errors.Is(err, ErrTooManyFragments):
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	case err != nil:
		http.Error(w, "stream unavailable", http.StatusInternalServerError)
		return
	}
	defer sub.Close()

	header := w.Header()
	if cookie := sub.Cookie(); cookie != nil {
		http.SetCookie(w, cookie)
	}
	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-cache")
	// nginx buffers a proxied response by default, and a buffered stream arrives
	// all at once when it ends — which it does not.
	header.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	controller := http.NewResponseController(w)
	id := 0
	write := func(chunk string) error {
		_ = controller.SetWriteDeadline(time.Now().Add(writeTimeout))
		if _, err := fmt.Fprint(w, chunk); err != nil {
			return err
		}
		return controller.Flush()
	}
	send := func(msgs []Message) error {
		for _, msg := range msgs {
			data, err := json.Marshal(msg)
			if err != nil {
				return err
			}
			id++
			if err := write("event: fragment\nid: " + strconv.Itoa(id) + "\ndata: " + string(data) + "\n\n"); err != nil {
				return err
			}
		}
		return nil
	}

	// retry: how long the browser waits before reconnecting a dropped stream.
	if err := write("retry: 2000\n\n"); err != nil {
		return
	}
	if err := send(sub.Initial()); err != nil {
		return
	}

	keepAlive := time.Duration(h.plugin.cfg.KeepAlive)
	for {
		wait, cancel := context.WithTimeout(r.Context(), keepAlive)
		msgs, err := sub.Wait(wait)
		cancel()
		switch {
		case err == nil:
			if send(msgs) != nil {
				return
			}
		case errors.Is(err, context.DeadlineExceeded) && r.Context().Err() == nil:
			if write(": keep-alive\n\n") != nil {
				return
			}
		default:
			// The reader left, or the application is shutting down. Either way
			// the stream ends here; a client that is still there reconnects, and
			// is told current state when it does.
			return
		}
	}
}
