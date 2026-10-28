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
	"context"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/Bugs5382/go-yjs-relay/protocol"
)

func TestRelayBroadcastsUpdateToOtherClients(t *testing.T) {
	tb := newTestbed(t, newMemSink())
	a := tb.dial(t, "doc-1", "u-a", "")
	b := tb.dial(t, "doc-1", "u-b", "")
	// Both must be registered before A types, or the fan-out can race B's
	// registration.
	tb.waitForEditors(t, "doc-1", 2)

	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	write(t, a, protocol.EncodeUpdate(payload))

	if got := readUntilSync(t, b, protocol.SyncUpdate); !bytes.Equal(got, payload) {
		t.Fatalf("broadcast payload mismatch: got %x, want %x", got, payload)
	}
}

func TestHubCreatesAndRemovesRoom(t *testing.T) {
	tb := newTestbed(t, newMemSink())
	if got := tb.hub.RoomCount(); got != 0 {
		t.Fatalf("initial RoomCount: got %d, want 0", got)
	}

	a := tb.dial(t, "doc-1", "u-1", "")
	tb.waitForEditors(t, "doc-1", 1)
	b := tb.dial(t, "doc-1", "u-2", "")
	tb.waitForEditors(t, "doc-1", 2)
	if got := tb.hub.RoomCount(); got != 1 {
		t.Fatalf("two joiners of one room: RoomCount %d, want 1", got)
	}

	_ = a.Close()
	_ = b.Close()
	tb.waitForRoomCount(t, 0)
	if _, ok := tb.hub.Lookup("doc-1"); ok {
		t.Fatal("Lookup still finds a room after the last client left")
	}
}

func TestLookupNeverCreatesARoom(t *testing.T) {
	tb := newTestbed(t, newMemSink())
	if _, ok := tb.hub.Lookup("doc-absent"); ok {
		t.Fatal("Lookup reported a room nobody joined")
	}
	if tb.hub.RoomCount() != 0 {
		t.Fatalf("Lookup created a room; RoomCount=%d", tb.hub.RoomCount())
	}
}

func TestLookupReportsRoomAttrsFromTheFirstJoiner(t *testing.T) {
	tb := newTestbed(t, newMemSink())
	tb.dialToken(t, "doc-1", "doc-1:u-1:Ann:tenant=t-1")
	tb.waitForEditors(t, "doc-1", 1)

	info, ok := tb.hub.Lookup("doc-1")
	if !ok {
		t.Fatal("Lookup did not find the live room")
	}
	if info.ID != "doc-1" || info.Attrs["tenant"] != "t-1" || info.Editors != 1 {
		t.Fatalf("RoomInfo: got %+v", info)
	}
}

// TestCloseDisconnectsClients replaces the old context-cancel test: shutting
// the hub down must close every socket rather than leave read pumps running.
func TestCloseDisconnectsClients(t *testing.T) {
	tb := newTestbed(t, newMemSink())
	conn := tb.dial(t, "doc-1", "u-1", "")
	tb.waitForEditors(t, "doc-1", 1)

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := tb.hub.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		_, _, err := conn.ReadMessage()
		if err == nil {
			continue
		}
		if strings.Contains(err.Error(), "i/o timeout") {
			t.Fatalf("read timed out instead of failing on a closed conn: %v", err)
		}
		return
	}
}

// TestJoinAsksPeersToReAdvertise: the relay holds only the last compacted
// state, so anything typed since lives only in the peers. A joiner has to
// make them re-advertise, or it starts from stale content.
func TestJoinAsksPeersToReAdvertise(t *testing.T) {
	tb := newTestbed(t, newMemSink())
	first := tb.dial(t, "doc-1", "u-1", "")
	tb.waitForEditors(t, "doc-1", 1)

	tb.dial(t, "doc-1", "u-2", "")

	if sv := readUntilSync(t, first, protocol.SyncStep1); !bytes.Equal(sv, protocol.EmptyStateVector()) {
		t.Fatalf("solicited state vector: got %x want %x", sv, protocol.EmptyStateVector())
	}
}

// TestNewRoomNeverSendsAZeroLengthUpdate: a room with no stored state still
// answers Sync Step-1 with a valid (empty-document) update.
func TestNewRoomNeverSendsAZeroLengthUpdate(t *testing.T) {
	tb := newTestbed(t, newMemSink())
	conn := tb.dial(t, "doc-new", "u-1", "")

	write(t, conn, protocol.EncodeSync1(protocol.EmptyStateVector()))
	if got := hex.EncodeToString(readUntilSync(t, conn, protocol.SyncStep2)); got != "0000" {
		t.Fatalf("new room Sync Step-2: got %s want 0000", got)
	}
}

// TestZeroLengthUpdateIsNotRelayed keeps an empty update from reaching peers,
// whose Y.applyUpdate would throw on it.
func TestZeroLengthUpdateIsNotRelayed(t *testing.T) {
	tb := newTestbed(t, newMemSink())
	a := tb.dial(t, "doc-1", "u-a", "")
	b := tb.dial(t, "doc-1", "u-b", "")
	tb.waitForEditors(t, "doc-1", 2)

	write(t, a, protocol.EncodeUpdate(nil))
	write(t, a, protocol.EncodeUpdate([]byte{0x05}))

	if got := readUntilSync(t, b, protocol.SyncUpdate); !bytes.Equal(got, []byte{0x05}) {
		t.Fatalf("first relayed update: got %x, want 05 (the empty one must be dropped)", got)
	}
}
