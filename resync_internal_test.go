package yjsrelay

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
	"testing"

	"github.com/Bugs5382/go-yjs-relay/protocol"
)

// A full send buffer used to make the room drop a document frame silently,
// which desyncs that client for good: Yjs has no gap detection. The drop has
// to flag the client for resync instead. Filling a real socket's buffers is
// not deterministic, so these two run against a client with no write pump.
func TestDroppedSyncFrameSchedulesResync(t *testing.T) {
	c := &client{room: newRoom("doc-1", nil, New(nil)), send: make(chan []byte, 1), id: Identity{UserID: "u-1"}}
	c.send <- []byte("occupied")

	c.queueSync(protocol.EncodeUpdate([]byte{0x01}))

	if !c.resync.Load() {
		t.Fatal("dropped sync frame did not schedule a resync")
	}
}

// Presence is transient and re-advertised, so dropping it must not trigger a
// document resync.
func TestDroppedAwarenessFrameDoesNotScheduleResync(t *testing.T) {
	c := &client{room: newRoom("doc-1", nil, New(nil)), send: make(chan []byte, 1), id: Identity{UserID: "u-1"}}
	c.send <- []byte("occupied")

	c.queueAwareness(protocol.EncodeAwareness(nil))

	if c.resync.Load() {
		t.Fatal("dropped awareness frame scheduled a document resync")
	}
}

func TestPresenceMapRespectsClocksAndClears(t *testing.T) {
	r := newRoom("doc-1", nil, New(nil))

	r.recordAwareness([]protocol.AwarenessEntry{{ClientID: 7, Clock: 5, State: []byte(`{"a":1}`)}})
	r.recordAwareness([]protocol.AwarenessEntry{{ClientID: 7, Clock: 2, State: []byte(`{"a":"stale"}`)}})
	_, payload, _ := protocol.DecodeMessage(r.awarenessState())
	body, _ := protocol.DecodeAwarenessMessage(payload)
	entries, _ := protocol.DecodeAwarenessUpdate(body)
	if len(entries) != 1 || entries[0].Clock != 5 || string(entries[0].State) != `{"a":1}` {
		t.Fatalf("stale clock overwrote current presence: %+v", entries)
	}

	r.recordAwareness([]protocol.AwarenessEntry{{ClientID: 7, Clock: 6, State: []byte(protocol.AwarenessStateCleared)}})
	if frame := r.awarenessState(); frame != nil {
		t.Fatalf("cleared peer still present: %x", frame)
	}
}
