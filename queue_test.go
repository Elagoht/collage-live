package live

import (
	"sync"
	"testing"
)

// A close landing between queue releasing the lock and waking the worker sent on
// a closed channel, and the process went down with it — at shutdown, which is when
// an application invalidating every second is most likely to be invalidating.
func TestQueueDuringClose(t *testing.T) {
	p := &Plugin{}
	for range 2000 {
		h := newHub()
		h.queue(p, []string{"warm"})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); h.queue(p, []string{"system:cpu"}) }()
		go func() { defer wg.Done(); h.close() }()
		wg.Wait()
	}
}
