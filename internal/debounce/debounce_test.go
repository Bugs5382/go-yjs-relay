package debounce_test

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
	"sync/atomic"
	"testing"
	"time"

	"github.com/Bugs5382/go-yjs-relay/internal/debounce"
)

func TestFiresAfterQuietPeriod(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	d := debounce.New(20 * time.Millisecond)
	defer d.Stop()

	d.Trigger(func() { calls.Add(1) })

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if calls.Load() == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("debounced callback did not fire; calls=%d", calls.Load())
}

func TestCoalescesRapidCalls(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	d := debounce.New(40 * time.Millisecond)
	defer d.Stop()

	for range 5 {
		d.Trigger(func() { calls.Add(1) })
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(150 * time.Millisecond)

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected exactly 1 callback after coalescing; got %d", got)
	}
}

func TestStopCancelsPendingFire(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	d := debounce.New(50 * time.Millisecond)

	d.Trigger(func() { calls.Add(1) })
	d.Stop()
	time.Sleep(150 * time.Millisecond)

	if got := calls.Load(); got != 0 {
		t.Fatalf("expected 0 callbacks after Stop; got %d", got)
	}
}

func TestRefiresAfterFireCompletes(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	d := debounce.New(20 * time.Millisecond)
	defer d.Stop()

	d.Trigger(func() { calls.Add(1) })
	time.Sleep(100 * time.Millisecond)
	d.Trigger(func() { calls.Add(1) })
	time.Sleep(100 * time.Millisecond)

	if got := calls.Load(); got != 2 {
		t.Fatalf("expected 2 callbacks across two windows; got %d", got)
	}
}
