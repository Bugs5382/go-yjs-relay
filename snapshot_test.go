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
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	yjsrelay "github.com/Bugs5382/go-yjs-relay"
	"github.com/Bugs5382/go-yjs-relay/protocol"
)

const fastDebounce = 20 * time.Millisecond

// TestSnapshotFrameReachesSink: the snapshot frame is the only path from live
// edits to durable storage.
func TestSnapshotFrameReachesSink(t *testing.T) {
	sink := newMemSink()
	tb := newTestbed(t, sink, yjsrelay.WithDebounce(fastDebounce))
	conn := tb.dialToken(t, "doc-1", "doc-1:u-7:Bob:tenant=t-1")
	readUntilSync(t, conn, protocol.SyncStep1)

	state := []byte{0x11, 0x22, 0x33}
	write(t, conn, protocol.EncodeSnapshot([]byte(testContent), state))

	got := sink.waitForSaves(t, 1)[0]
	if string(got.Content) != testContent || !bytes.Equal(got.State, state) {
		t.Errorf("snapshot: got content %q state %x", got.Content, got.State)
	}
	if got.Room != "doc-1" || got.RoomAttrs["tenant"] != "t-1" {
		t.Errorf("room: got %q attrs %v", got.Room, got.RoomAttrs)
	}
	// The author is the connection's verified identity, never something the
	// frame asserted: the frame has no author field at all.
	if got.Author.UserID != "u-7" || got.Author.Attrs["tenant"] != "t-1" {
		t.Errorf("author: got %+v", got.Author)
	}
}

// TestRejectedSnapshotEmitsControlFrame: a refusal used to be invisible, so
// the editor kept accepting keystrokes that would never persist.
func TestRejectedSnapshotEmitsControlFrame(t *testing.T) {
	sink := newMemSink()
	sink.setErr(&yjsrelay.RejectError{Reason: "invalid_content", Detail: "boilerplate edited"})
	tb := newTestbed(t, sink, yjsrelay.WithDebounce(fastDebounce))
	conn := tb.dial(t, "doc-1", "u-1", "")

	write(t, conn, protocol.EncodeSnapshot([]byte(testContent), []byte{0x01}))

	ctrl := readControl(t, conn)
	if ctrl.Type != protocol.ControlSnapshotRejected || ctrl.Room != "doc-1" {
		t.Fatalf("control: got %+v", ctrl)
	}
	if ctrl.Reason != "invalid_content" || ctrl.Detail != "boilerplate edited" {
		t.Fatalf("control reason/detail: got %q/%q", ctrl.Reason, ctrl.Detail)
	}
}

// TestPlainSinkErrorDoesNotLeakToEditors: only a RejectError chooses what
// editors see; any other error text stays server-side.
func TestPlainSinkErrorDoesNotLeakToEditors(t *testing.T) {
	sink := newMemSink()
	sink.setErr(errors.New("pq: connection to 10.0.0.5 refused"))
	tb := newTestbed(t, sink, yjsrelay.WithDebounce(fastDebounce))
	conn := tb.dial(t, "doc-1", "u-1", "")

	write(t, conn, protocol.EncodeSnapshot([]byte(testContent), nil))

	ctrl := readControl(t, conn)
	if ctrl.Type != protocol.ControlSnapshotRejected || ctrl.Reason != yjsrelay.ReasonSinkError {
		t.Fatalf("control: got %+v", ctrl)
	}
	if strings.Contains(ctrl.Detail, "10.0.0.5") {
		t.Fatalf("sink error text leaked to editors: %q", ctrl.Detail)
	}
}

func TestAcceptedSnapshotEmitsControlFrame(t *testing.T) {
	tb := newTestbed(t, newMemSink(), yjsrelay.WithDebounce(fastDebounce))
	conn := tb.dial(t, "doc-1", "u-1", "")

	write(t, conn, protocol.EncodeSnapshot([]byte(testContent), nil))

	if ctrl := readControl(t, conn); ctrl.Type != protocol.ControlSnapshotAccepted {
		t.Fatalf("control type: got %q want %q", ctrl.Type, protocol.ControlSnapshotAccepted)
	}
}

// TestMalformedSnapshotFrameIsIgnored: a bad frame must not take the room
// down or reach the sink.
func TestMalformedSnapshotFrameIsIgnored(t *testing.T) {
	sink := newMemSink()
	tb := newTestbed(t, sink, yjsrelay.WithDebounce(fastDebounce))
	conn := tb.dial(t, "doc-1", "u-1", "")

	// A length prefix that claims more bytes than the frame carries.
	write(t, conn, []byte{protocol.MsgSnapshot, 99, 0x01})
	// The room still answers afterwards.
	write(t, conn, protocol.EncodeSync1(protocol.EmptyStateVector()))
	readUntilSync(t, conn, protocol.SyncStep2)

	time.Sleep(100 * time.Millisecond)
	if got := sink.recorded(); len(got) != 0 {
		t.Fatalf("malformed snapshot reached the sink: %+v", got)
	}
}

// TestUpdatesNeitherDeriveNorPersistState: a concatenation of updates is not
// a valid Yjs update, and saving per update made storage grow without bound.
// State changes only on snapshot.
func TestUpdatesNeitherDeriveNorPersistState(t *testing.T) {
	sink := newMemSink()
	initial := []byte{0xAA, 0xBB}
	sink.states["doc-1"] = initial
	tb := newTestbed(t, sink, yjsrelay.WithDebounce(fastDebounce))
	conn := tb.dial(t, "doc-1", "u-1", "")

	for range 5 {
		write(t, conn, protocol.EncodeUpdate([]byte{0x01, 0x02, 0x03}))
	}
	time.Sleep(150 * time.Millisecond)

	if got := sink.recorded(); len(got) != 0 {
		t.Fatalf("relayed updates wrote to the sink %d time(s)", len(got))
	}
	write(t, conn, protocol.EncodeSync1(protocol.EmptyStateVector()))
	if got := readUntilSync(t, conn, protocol.SyncStep2); !bytes.Equal(got, initial) {
		t.Fatalf("relayed updates changed the room state: got %x want %x", got, initial)
	}
}

// TestSnapshotReplacesStateWithoutGrowing: each snapshot replaces the stored
// state, so a later, smaller compacted state shrinks it.
func TestSnapshotReplacesStateWithoutGrowing(t *testing.T) {
	sink := newMemSink()
	tb := newTestbed(t, sink)
	conn := tb.dial(t, "doc-1", "u-1", "")

	first := bytes.Repeat([]byte{0x01}, 64)
	sendSnapshotAndSettle(t, conn, "", first)
	if res, err := tb.hub.Flush(t.Context(), "doc-1"); err != nil || !res.ContentFlushed {
		t.Fatalf("first Flush: %+v, %v", res, err)
	}
	if got := sink.state("doc-1"); !bytes.Equal(got, first) {
		t.Fatalf("stored state after first snapshot: got %d bytes want %d", len(got), len(first))
	}

	second := bytes.Repeat([]byte{0x02}, 8)
	sendSnapshotAndSettle(t, conn, "", second)
	if res, err := tb.hub.Flush(t.Context(), "doc-1"); err != nil || !res.ContentFlushed {
		t.Fatalf("second Flush: %+v, %v", res, err)
	}
	if got := sink.state("doc-1"); !bytes.Equal(got, second) {
		t.Fatalf("stored state after second snapshot: got %x want %x", got, second)
	}
	if n := len(sink.recorded()); n != 2 {
		t.Fatalf("saves: got %d want 2 (one per flush)", n)
	}
}

// TestRehydrationServesLastCompactedState: a room re-created from the sink
// answers Sync Step-1 with exactly the state the previous session saved.
func TestRehydrationServesLastCompactedState(t *testing.T) {
	sink := newMemSink()
	tb := newTestbed(t, sink)
	compacted := []byte{0x0A, 0x0B, 0x0C, 0x0D}

	conn := tb.dial(t, "doc-1", "u-1", "")
	sendSnapshotAndSettle(t, conn, "", compacted)
	_ = conn.Close()
	tb.waitForRoomCount(t, 0)

	again := tb.dial(t, "doc-1", "u-1", "")
	write(t, again, protocol.EncodeSync1(protocol.EmptyStateVector()))
	if got := readUntilSync(t, again, protocol.SyncStep2); !bytes.Equal(got, compacted) {
		t.Fatalf("rehydrated Sync Step-2: got %x want %x", got, compacted)
	}
}

// TestSnapshotAdoptsStateEvenWhenRejected: the peers already hold the refused
// edits, so the room must still rehydrate to what everyone is editing. The
// refusal travels over the control channel instead.
func TestSnapshotAdoptsStateEvenWhenRejected(t *testing.T) {
	sink := newMemSink()
	sink.setErr(&yjsrelay.RejectError{Reason: "nope"})
	tb := newTestbed(t, sink)
	conn := tb.dial(t, "doc-1", "u-1", "")

	compacted := []byte{0x07, 0x08}
	sendSnapshotAndSettle(t, conn, testContent, compacted)
	if _, err := tb.hub.Flush(t.Context(), "doc-1"); err == nil {
		t.Fatal("Flush hid the rejection")
	}

	write(t, conn, protocol.EncodeSync1(protocol.EmptyStateVector()))
	if got := readUntilSync(t, conn, protocol.SyncStep2); !bytes.Equal(got, compacted) {
		t.Fatalf("room state after rejection: got %x want %x", got, compacted)
	}
}

// TestFlushOnLastCollaboratorLeaves: without it the final edits die with the
// room. The default debounce is kept on purpose, so a pass can only mean the
// disconnect triggered the flush.
func TestFlushOnLastCollaboratorLeaves(t *testing.T) {
	sink := newMemSink()
	tb := newTestbed(t, sink)
	conn := tb.dial(t, "doc-1", "u-1", "")

	compacted := []byte{0x42}
	sendSnapshotAndSettle(t, conn, testContent, compacted)
	time.Sleep(100 * time.Millisecond)
	if got := sink.recorded(); len(got) != 0 {
		t.Fatalf("snapshot flushed before the debounce elapsed: %+v", got)
	}

	_ = conn.Close()

	got := sink.waitForSaves(t, 1)[0]
	if string(got.Content) != testContent || !bytes.Equal(got.State, compacted) {
		t.Fatalf("final flush: got %+v", got)
	}
	tb.waitForRoomCount(t, 0)
}

// TestRoomSurvivesAJoinDuringTheFinalFlush: a client can join while the final
// flush is in flight. Dropping the room then would strand it, and the next
// joiner would open a second room for the same document.
func TestRoomSurvivesAJoinDuringTheFinalFlush(t *testing.T) {
	sink := newMemSink()
	release := sink.holdSaves()
	tb := newTestbed(t, sink)

	first := tb.dial(t, "doc-1", "u-1", "")
	sendSnapshotAndSettle(t, first, testContent, []byte{0x01})
	_ = first.Close()
	select {
	case <-sink.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("final flush never reached the sink")
	}

	tb.dial(t, "doc-1", "u-2", "")
	tb.waitForEditors(t, "doc-1", 1)
	close(release)
	sink.waitForSaves(t, 1)
	time.Sleep(100 * time.Millisecond)

	if tb.hub.RoomCount() != 1 {
		t.Fatalf("RoomCount = %d after a join during the final flush, want 1", tb.hub.RoomCount())
	}
	if info, ok := tb.hub.Lookup("doc-1"); !ok || info.Editors != 1 {
		t.Fatalf("the joiner was stranded: %+v, %v", info, ok)
	}
}
