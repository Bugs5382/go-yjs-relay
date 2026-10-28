package yjsrelay_test

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
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Bugs5382/go-apperr"
	yjsrelay "github.com/Bugs5382/go-yjs-relay"
	"github.com/Bugs5382/go-yjs-relay/protocol"
)

func TestFlushPersistsTheNewestCheckpoint(t *testing.T) {
	sink := newMemSink()
	tb := newTestbed(t, sink)
	conn := tb.dial(t, "doc-1", "u-1", "")

	older := `{"root":{"type":"root","children":[{"type":"paragraph","text":"old"}]}}`
	write(t, conn, protocol.EncodeSnapshot([]byte(older), []byte{0x01}))
	sendSnapshotAndSettle(t, conn, testContent, []byte{0x02})

	res, err := tb.hub.Flush(t.Context(), "doc-1")
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if !res.RoomFound || !res.ContentFlushed {
		t.Fatalf("Flush with a checkpoint pending: got %+v", res)
	}
	got := sink.recorded()
	if len(got) != 1 || string(got[0].Content) != testContent {
		t.Fatalf("sink saves: got %+v, want only the newest checkpoint", got)
	}
}

// A debounced flush can already be talking to the sink when a publish asks
// for a flush. Reporting success then, with nothing left pending, would let
// the version be cut before the sink holds the content.
func TestFlushWaitsForAnInFlightCheckpoint(t *testing.T) {
	sink := newMemSink()
	release := sink.holdSaves()
	tb := newTestbed(t, sink, yjsrelay.WithDebounce(fastDebounce))
	conn := tb.dial(t, "doc-1", "u-1", "")

	write(t, conn, protocol.EncodeSnapshot([]byte(testContent), []byte{0x01}))
	select {
	case <-sink.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("debounced flush never reached the sink")
	}

	done := make(chan error, 1)
	go func() {
		_, err := tb.hub.Flush(t.Context(), "doc-1")
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("Flush returned (err=%v) while the in-flight checkpoint was unanswered", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Flush: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Flush never returned after the sink answered")
	}
	if got := sink.recorded(); len(got) != 1 {
		t.Fatalf("sink saves: got %d want 1", len(got))
	}
}

func TestFlushSurfacesARejection(t *testing.T) {
	sink := newMemSink()
	sink.setErr(&yjsrelay.RejectError{Reason: "invalid_content", Detail: "boilerplate edited"})
	tb := newTestbed(t, sink)
	conn := tb.dial(t, "doc-1", "u-1", "")

	sendSnapshotAndSettle(t, conn, testContent, []byte{0x01})
	_, err := tb.hub.Flush(t.Context(), "doc-1")
	var fe *yjsrelay.FlushError
	if !errors.As(err, &fe) || !errors.Is(err, yjsrelay.ErrRejected) {
		t.Fatalf("Flush error = %v, want a *FlushError matching ErrRejected", err)
	}
	if fe.Room != "doc-1" || fe.Reason != "invalid_content" || fe.Detail != "boilerplate edited" {
		t.Fatalf("FlushError: got %+v", fe)
	}

	// Nothing newer has arrived, so the sink still lacks the room's newest
	// content. A second flush must say so, without resending.
	if _, err := tb.hub.Flush(t.Context(), "doc-1"); !errors.Is(err, yjsrelay.ErrRejected) {
		t.Fatalf("second Flush error = %v, want the standing rejection", err)
	}
	if got := sink.recorded(); len(got) != 1 {
		t.Fatalf("sink saves: got %d want 1 (a rejected checkpoint is not resent)", len(got))
	}

	sink.setErr(nil)
	sendSnapshotAndSettle(t, conn, testContent, []byte{0x02})
	if _, err := tb.hub.Flush(t.Context(), "doc-1"); err != nil {
		t.Fatalf("Flush after an accepted checkpoint: %v", err)
	}
}

// An unreachable sink says nothing about the content, so the checkpoint has
// to survive for the caller's retry.
func TestFlushKeepsTheCheckpointAfterATransientFailure(t *testing.T) {
	sink := newMemSink()
	sink.setErr(fmt.Errorf("dial: %w", yjsrelay.ErrUnavailable))
	tb := newTestbed(t, sink)
	conn := tb.dial(t, "doc-1", "u-1", "")

	sendSnapshotAndSettle(t, conn, testContent, []byte{0x01})
	_, err := tb.hub.Flush(t.Context(), "doc-1")
	var fe *yjsrelay.FlushError
	if !errors.As(err, &fe) || !errors.Is(err, yjsrelay.ErrUnavailable) || fe.Reason != yjsrelay.ReasonUnavailable {
		t.Fatalf("Flush error = %v, want a *FlushError matching ErrUnavailable", err)
	}

	sink.setErr(nil)
	res, err := tb.hub.Flush(t.Context(), "doc-1")
	if err != nil {
		t.Fatalf("retry Flush: %v", err)
	}
	if !res.ContentFlushed {
		t.Fatal("retry Flush found nothing to send; the checkpoint was dropped")
	}
	got := sink.recorded()
	if len(got) != 2 || string(got[1].Content) != testContent {
		t.Fatalf("sink saves: got %+v, want the same checkpoint sent twice", got)
	}
}

func TestFlushGivesUpWhenTheCallerDoes(t *testing.T) {
	sink := newMemSink()
	release := sink.holdSaves()
	defer close(release)
	tb := newTestbed(t, sink)
	conn := tb.dial(t, "doc-1", "u-1", "")
	sendSnapshotAndSettle(t, conn, testContent, []byte{0x01})

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err := tb.hub.Flush(ctx, "doc-1")
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, yjsrelay.ErrUnavailable) {
		t.Fatalf("Flush error = %v, want DeadlineExceeded and ErrUnavailable", err)
	}
}

func TestFlushReportsWhatItFound(t *testing.T) {
	sink := newMemSink()
	tb := newTestbed(t, sink)

	res, err := tb.hub.Flush(t.Context(), "doc-absent")
	if err != nil || res.RoomFound || res.ContentFlushed {
		t.Fatalf("Flush with no room: got %+v, %v; want zero", res, err)
	}
	if tb.hub.RoomCount() != 0 {
		t.Fatalf("Flush created a room; RoomCount=%d", tb.hub.RoomCount())
	}

	conn := tb.dial(t, "doc-1", "u-1", "")
	tb.waitForEditors(t, "doc-1", 1)
	res, err = tb.hub.Flush(t.Context(), "doc-1")
	if err != nil || !res.RoomFound || res.ContentFlushed {
		t.Fatalf("Flush on an idle room: got %+v, %v; want found, nothing flushed", res, err)
	}

	sendSnapshotAndSettle(t, conn, testContent, []byte{0x01})
	res, err = tb.hub.Flush(t.Context(), "doc-1")
	if err != nil || !res.RoomFound || !res.ContentFlushed {
		t.Fatalf("Flush with a pending checkpoint: got %+v, %v", res, err)
	}
	// Synchronous by contract: the caller can publish straight after.
	if got := sink.recorded(); len(got) != 1 {
		t.Fatalf("sink saves after Flush: got %d want 1", len(got))
	}
	// Idempotent: nothing pending means nothing sent.
	if res, _ := tb.hub.Flush(t.Context(), "doc-1"); res.ContentFlushed {
		t.Fatal("a second Flush resent an already-flushed checkpoint")
	}
}

// TestFreezeFlushesAndGoesReadOnly: a publish is a hard boundary. Flush first,
// stop accepting edits, and tell every editor so it drops to read-only.
func TestFreezeFlushesAndGoesReadOnly(t *testing.T) {
	sink := newMemSink()
	tb := newTestbed(t, sink)
	a := tb.dial(t, "doc-1", "u-a", "")
	b := tb.dial(t, "doc-1", "u-b", "")
	tb.waitForEditors(t, "doc-1", 2)

	sendSnapshotAndSettle(t, a, testContent, []byte{0x01})

	if !tb.hub.Freeze(t.Context(), "doc-1", "7") {
		t.Fatal("Freeze did not find the live room")
	}
	if got := sink.recorded(); len(got) != 1 {
		t.Fatalf("sink saves: got %d want 1 (flush before freeze)", len(got))
	}
	for {
		ctrl := readControl(t, b)
		if ctrl.Type == protocol.ControlSnapshotAccepted {
			continue
		}
		if ctrl.Type != protocol.ControlRoomFrozen || ctrl.Version != "7" || ctrl.Room != "doc-1" {
			t.Fatalf("control: got %+v, want room.frozen version 7", ctrl)
		}
		break
	}

	// Frozen means immutable: later snapshots and updates are refused.
	write(t, a, protocol.EncodeUpdate([]byte{0x09}))
	sendSnapshotAndSettle(t, a, testContent, []byte{0x02})
	if res, _ := tb.hub.Flush(t.Context(), "doc-1"); res.ContentFlushed {
		t.Fatal("a frozen room accepted a snapshot")
	}
	if info, _ := tb.hub.Lookup("doc-1"); !info.Frozen {
		t.Fatalf("Lookup does not report the freeze: %+v", info)
	}
	if tb.hub.Freeze(t.Context(), "doc-absent", "1") {
		t.Fatal("Freeze reported a room nobody joined")
	}
}

func TestWithCodesAttachesTheCallersCodes(t *testing.T) {
	sink := newMemSink()
	sink.setErr(&yjsrelay.RejectError{Reason: "bad"})
	tb := newTestbed(t, sink, yjsrelay.WithCodes(yjsrelay.Codes{Rejected: 4221, Unavailable: 5031}))
	conn := tb.dial(t, "doc-1", "u-1", "")
	sendSnapshotAndSettle(t, conn, testContent, nil)

	_, err := tb.hub.Flush(t.Context(), "doc-1")
	if code, ok := apperr.Code(err); !ok || code != 4221 {
		t.Fatalf("code = %d, %v; want 4221", code, ok)
	}
	var fe *yjsrelay.FlushError
	if !errors.As(err, &fe) || !errors.Is(err, yjsrelay.ErrRejected) {
		t.Fatalf("coding hid the FlushError: %v", err)
	}

	sink.setErr(yjsrelay.ErrUnavailable)
	sendSnapshotAndSettle(t, conn, testContent, nil)
	_, err = tb.hub.Flush(t.Context(), "doc-1")
	if code, ok := apperr.Code(err); !ok || code != 5031 {
		t.Fatalf("code = %d, %v; want 5031", code, ok)
	}
}

func TestFlushErrorsAreUncodedByDefault(t *testing.T) {
	sink := newMemSink()
	sink.setErr(&yjsrelay.RejectError{Reason: "bad"})
	tb := newTestbed(t, sink)
	conn := tb.dial(t, "doc-1", "u-1", "")
	sendSnapshotAndSettle(t, conn, testContent, nil)

	_, err := tb.hub.Flush(t.Context(), "doc-1")
	if code, ok := apperr.Code(err); ok {
		t.Fatalf("a library-picked code leaked: %d", code)
	}
}
