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
	"encoding/json"
	"slices"
	"sync"
	"testing"
	"time"

	yjsrelay "github.com/Bugs5382/go-yjs-relay"
	"github.com/Bugs5382/go-yjs-relay/protocol"
)

type boundUser struct {
	UID        string `json:"uid"`
	Name       string `json:"name"`
	Color      string `json:"color"`
	ColorIndex int    `json:"colorIndex"`
}

func userOf(t *testing.T, state []byte) boundUser {
	t.Helper()
	var s struct {
		User boundUser `json:"user"`
	}
	if err := json.Unmarshal(state, &s); err != nil {
		t.Fatalf("unmarshal state %s: %v", state, err)
	}
	return s.User
}

// TestAwarenessIdentityIsServerAuthoritative: a client asserting another
// user's uid, name or colour has the whole user block replaced with its own
// verified identity, while cursor and selection fields pass through.
func TestAwarenessIdentityIsServerAuthoritative(t *testing.T) {
	tb := newTestbed(t, newMemSink())
	a := tb.dial(t, "doc-1", "real-user-7", "")
	b := tb.dial(t, "doc-1", "u-b", "")
	tb.waitForEditors(t, "doc-1", 2)

	spoofed := `{"user":{"uid":"victim-9","name":"Victim","color":"#000000"},"anchorPos":{"key":"a","offset":3},"focusing":true}`
	write(t, a, awarenessFrame(42, 7, spoofed))

	entries := readAwareness(t, b)
	if len(entries) != 1 || entries[0].ClientID != 42 || entries[0].Clock != 7 {
		t.Fatalf("client id/clock not preserved: %+v", entries)
	}
	u := userOf(t, entries[0].State)
	if u.UID != "real-user-7" || u.Name != "real-user-7" {
		t.Fatalf("spoofed identity survived: %+v", u)
	}
	if !slices.Contains(yjsrelay.DefaultPalette, u.Color) || yjsrelay.DefaultPalette[u.ColorIndex] != u.Color {
		t.Fatalf("colour %q / index %d not from the palette", u.Color, u.ColorIndex)
	}
	var rest struct {
		AnchorPos json.RawMessage `json:"anchorPos"`
		Focusing  bool            `json:"focusing"`
	}
	if err := json.Unmarshal(entries[0].State, &rest); err != nil {
		t.Fatal(err)
	}
	if string(rest.AnchorPos) != `{"key":"a","offset":3}` || !rest.Focusing {
		t.Fatalf("cursor fields not preserved: %s", entries[0].State)
	}
}

func TestPresenceColourIsStablePerUser(t *testing.T) {
	tb := newTestbed(t, newMemSink())
	colours := make([]string, 0, 2)
	for range 2 {
		conn := tb.dial(t, "doc-1", "user-x", "")
		write(t, conn, awarenessFrame(1, uint32(len(colours)+1), `{}`))
		colours = append(colours, userOf(t, readAwareness(t, conn)[0].State).Color)
		_ = conn.Close()
		tb.waitForRoomCount(t, 0)
	}
	if colours[0] != colours[1] {
		t.Fatalf("colour changed between sessions: %v", colours)
	}
}

func TestWithPaletteOverridesTheColours(t *testing.T) {
	tb := newTestbed(t, newMemSink(), yjsrelay.WithPalette([]string{"#123456"}))
	conn := tb.dial(t, "doc-1", "u-1", "")
	write(t, conn, awarenessFrame(1, 1, `{}`))
	if u := userOf(t, readAwareness(t, conn)[0].State); u.Color != "#123456" || u.ColorIndex != 0 {
		t.Fatalf("palette ignored: %+v", u)
	}
}

func TestClearedAwarenessPassesThrough(t *testing.T) {
	tb := newTestbed(t, newMemSink())
	conn := tb.dial(t, "doc-1", "u-1", "")
	write(t, conn, awarenessFrame(42, 8, protocol.AwarenessStateCleared))
	if got := readAwareness(t, conn); len(got) != 1 || !got[0].Cleared() {
		t.Fatalf("cleared state not passed through: %+v", got)
	}
}

// TestQueryAwarenessAnswersWithRoomPresence: a y-websocket client queries on
// connect and must learn who is already present.
func TestQueryAwarenessAnswersWithRoomPresence(t *testing.T) {
	tb := newTestbed(t, newMemSink())
	alice := tb.dial(t, "doc-1", "alice", "Alice")
	write(t, alice, awarenessFrame(101, 1, `{"user":{"uid":"nope"},"cursor":{"anchor":0}}`))
	// The echo proves the room recorded the presence before Bob asks.
	readAwareness(t, alice)

	bob := tb.dial(t, "doc-1", "bob", "Bob")
	write(t, bob, protocol.EncodeQueryAwareness())

	entries := readAwareness(t, bob)
	if len(entries) != 1 || entries[0].ClientID != 101 || entries[0].Clock != 1 {
		t.Fatalf("query reply: %+v", entries)
	}
	if u := userOf(t, entries[0].State); u.UID != "alice" || u.Name != "Alice" {
		t.Fatalf("queried identity: %+v", u)
	}
}

// TestQueryAwarenessOnEmptyRoomIsSilent: nobody present means no frame, not a
// frame claiming zero clients.
func TestQueryAwarenessOnEmptyRoomIsSilent(t *testing.T) {
	tb := newTestbed(t, newMemSink())
	conn := tb.dial(t, "doc-1", "u-1", "")
	write(t, conn, protocol.EncodeQueryAwareness())
	write(t, conn, protocol.EncodeSync1(protocol.EmptyStateVector()))

	// The Sync Step-2 reply comes back first; an awareness frame before it
	// would be the empty presence the room must not send.
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		mt, payload, _ := protocol.DecodeMessage(msg)
		if mt == protocol.MsgAwareness {
			t.Fatalf("empty room answered the query with %x", payload)
		}
		if mt == protocol.MsgSync {
			if st, _, _ := protocol.DecodeSyncMessage(payload); st == protocol.SyncStep2 {
				return
			}
		}
	}
}

// TestDisconnectClearsPresence: peers drop a departed cursor at once instead
// of waiting out the client-side timeout, and the clock advances so the clear
// is not discarded as stale.
func TestDisconnectClearsPresence(t *testing.T) {
	tb := newTestbed(t, newMemSink())
	a := tb.dial(t, "doc-1", "u-a", "")
	b := tb.dial(t, "doc-1", "u-b", "")
	tb.waitForEditors(t, "doc-1", 2)

	write(t, a, awarenessFrame(9, 3, `{"a":1}`))
	readAwareness(t, b)
	_ = a.Close()

	got := readAwareness(t, b)
	if len(got) != 1 || got[0].ClientID != 9 || !got[0].Cleared() || got[0].Clock != 4 {
		t.Fatalf("cleared broadcast: %+v", got)
	}

	// The room forgot the peer too.
	write(t, b, protocol.EncodeQueryAwareness())
	write(t, b, protocol.EncodeSync1(protocol.EmptyStateVector()))
	_ = b.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		_, msg, err := b.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		mt, payload, _ := protocol.DecodeMessage(msg)
		if mt == protocol.MsgAwareness {
			t.Fatalf("departed peer still in the presence map: %x", payload)
		}
		if mt == protocol.MsgSync {
			if st, _, _ := protocol.DecodeSyncMessage(payload); st == protocol.SyncStep2 {
				return
			}
		}
	}
}

func TestPresenceFallsBackToUIDWithoutDisplayName(t *testing.T) {
	tb := newTestbed(t, newMemSink())
	conn := tb.dial(t, "doc-1", "u-42", "")
	write(t, conn, awarenessFrame(1, 1, `{}`))
	if u := userOf(t, readAwareness(t, conn)[0].State); u.Name != "u-42" {
		t.Fatalf("name fallback: got %q want u-42", u.Name)
	}
}

type recordingObserver struct {
	mu     sync.Mutex
	joined []yjsrelay.Session
	left   []yjsrelay.Session
}

func (r *recordingObserver) SessionJoined(_ context.Context, s yjsrelay.Session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.joined = append(r.joined, s)
}

func (r *recordingObserver) SessionLeft(_ context.Context, s yjsrelay.Session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.left = append(r.left, s)
}

func (r *recordingObserver) wait(t *testing.T, joined, left int) ([]yjsrelay.Session, []yjsrelay.Session) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		j, l := slices.Clone(r.joined), slices.Clone(r.left)
		r.mu.Unlock()
		if len(j) >= joined && len(l) >= left {
			return j, l
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("observer saw %d join(s) and %d leave(s), want %d and %d", len(r.joined), len(r.left), joined, left)
	return nil, nil
}

func TestSessionObserverSeesJoinAndLeave(t *testing.T) {
	obs := &recordingObserver{}
	tb := newTestbed(t, newMemSink(), yjsrelay.WithSessionObserver(obs))
	conn := tb.dialToken(t, "doc-9", "doc-9:u-audit:Ann:tenant=t-9")
	readUntilSync(t, conn, protocol.SyncStep1)

	joined, _ := obs.wait(t, 1, 0)
	if joined[0].Room != "doc-9" || joined[0].Identity.UserID != "u-audit" || joined[0].RoomAttrs["tenant"] != "t-9" {
		t.Fatalf("joined session: %+v", joined[0])
	}

	_ = conn.Close()
	_, left := obs.wait(t, 1, 1)
	if left[0].Room != "doc-9" || left[0].Identity.UserID != "u-audit" {
		t.Fatalf("left session: %+v", left[0])
	}
}
