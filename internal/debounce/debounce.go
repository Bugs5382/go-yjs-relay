// Package debounce coalesces a burst of calls into one delayed callback.
package debounce

/*
MIT License

Copyright (c) 2026 Shane

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/

import (
	"sync"
	"time"
)

// Debouncer runs the most recent callback once no Trigger has arrived for the
// configured wait. The callback runs on the time.AfterFunc goroutine. It is
// safe for concurrent use.
type Debouncer struct {
	wait time.Duration

	mu    sync.Mutex
	timer *time.Timer
}

// New returns a Debouncer with the given quiet period.
func New(wait time.Duration) *Debouncer {
	return &Debouncer{wait: wait}
}

// Trigger arms or re-arms the debouncer with fn, replacing any callback still
// waiting.
func (d *Debouncer) Trigger(fn func()) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.timer != nil {
		d.timer.Stop()
	}
	d.timer = time.AfterFunc(d.wait, func() {
		d.mu.Lock()
		d.timer = nil
		d.mu.Unlock()
		fn()
	})
}

// Stop cancels any waiting callback. It is safe to call more than once.
func (d *Debouncer) Stop() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.timer != nil {
		d.timer.Stop()
		d.timer = nil
	}
}
