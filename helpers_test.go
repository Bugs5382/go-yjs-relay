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
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	yjsrelay "github.com/Bugs5382/go-yjs-relay"
	"github.com/Bugs5382/go-yjs-relay/protocol"
	"github.com/gorilla/websocket"
)

const testContent = `{"root":{"type":"root","children":[{"type":"paragraph"}]}}`

// fakeVerifier accepts tokens shaped "room:user[:name[:attrKey=attrValue]]"
// and refuses anything else, so tests can mint identities without a signer.
type fakeVerifier struct{}

func (fakeVerifier) Verify(_ context.Context, token string) (yjsrelay.Join, error) {
	parts := strings.Split(token, ":")
	if len(parts) < 2 {
		return yjsrelay.Join{}, errors.New("unparseable token")
	}
	j := yjsrelay.Join{Room: parts[0], Identity: yjsrelay.Identity{UserID: parts[1]}}
	if len(parts) > 2 {
		j.Identity.DisplayName = parts[2]
	}
	if len(parts) > 3 {
		k, v, _ := strings.Cut(parts[3], "=")
		j.Identity.Attrs = map[string]string{k: v}
		j.RoomAttrs = map[string]string{k: v}
	}
	return j, nil
}

// memSink is an in-memory SnapshotSink. It keeps the newest state per room,
// records every Save, can be told to fail, and can hold each Save open until
// release is closed so a test can watch a flush wait.
type memSink struct {
	mu      sync.Mutex
	states  map[string][]byte
	saves   []yjsrelay.Snapshot
	err     error
	hold    chan struct{}
	entered chan struct{}
}

func newMemSink() *memSink {
	return &memSink{states: make(map[string][]byte), entered: make(chan struct{}, 16)}
}

func (m *memSink) Load(_ context.Context, room string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]byte(nil), m.states[room]...), nil
}

func (m *memSink) Save(ctx context.Context, snap yjsrelay.Snapshot) error {
	m.entered <- struct{}{}
	m.mu.Lock()
	hold := m.hold
	m.mu.Unlock()
	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	snap.Content = append([]byte(nil), snap.Content...)
	snap.State = append([]byte(nil), snap.State...)
	m.saves = append(m.saves, snap)
	if len(snap.State) > 0 {
		m.states[snap.Room] = snap.State
	}
	return m.err
}

func (m *memSink) setErr(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
}

func (m *memSink) holdSaves() chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hold = make(chan struct{})
	return m.hold
}

func (m *memSink) recorded() []yjsrelay.Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]yjsrelay.Snapshot(nil), m.saves...)
}

func (m *memSink) state(room string) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]byte(nil), m.states[room]...)
}

// waitForSaves polls until the sink has seen at least n saves.
func (m *memSink) waitForSaves(t *testing.T, n int) []yjsrelay.Snapshot {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got := m.recorded(); len(got) >= n {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("sink received %d save(s), want %d", len(m.recorded()), n)
	return nil
}

type testbed struct {
	hub *yjsrelay.Hub
	srv *httptest.Server
}

// newTestbed serves a hub over a real TCP listener. A nil sink runs the hub
// as a pure relay.
func newTestbed(t *testing.T, sink *memSink, opts ...yjsrelay.Option) *testbed {
	t.Helper()
	var s yjsrelay.SnapshotSink
	if sink != nil {
		s = sink
	}
	return newTestbedWithHandler(t, yjsrelay.New(s, opts...))
}

func newTestbedWithHandler(t *testing.T, hub *yjsrelay.Hub, hopts ...yjsrelay.HandlerOption) *testbed {
	t.Helper()
	srv := httptest.NewServer(yjsrelay.NewHandler(hub, fakeVerifier{}, hopts...))
	t.Cleanup(func() {
		srv.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = hub.Close(ctx)
	})
	return &testbed{hub: hub, srv: srv}
}

func (tb *testbed) url(room, token string) string {
	return "ws" + strings.TrimPrefix(tb.srv.URL, "http") + "/ws/" + room + "?token=" + token
}

// dial joins room as user and drains the relay's opening Sync Step-1.
func (tb *testbed) dial(t *testing.T, room, user, name string) *websocket.Conn {
	t.Helper()
	token := room + ":" + user
	if name != "" {
		token += ":" + name
	}
	conn := tb.dialToken(t, room, token)
	readUntilSync(t, conn, protocol.SyncStep1)
	return conn
}

func (tb *testbed) dialToken(t *testing.T, room, token string) *websocket.Conn {
	t.Helper()
	conn, _, err := websocket.DefaultDialer.Dial(tb.url(room, token), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// waitForEditors blocks until the room has n registered clients. Reading a
// frame only proves a client's write pump started, not that the room has
// picked its registration up.
func (tb *testbed) waitForEditors(t *testing.T, room string, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if info, ok := tb.hub.Lookup(room); ok && info.Editors == n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	info, _ := tb.hub.Lookup(room)
	t.Fatalf("room %q has %d editor(s), want %d", room, info.Editors, n)
}

func (tb *testbed) waitForRoomCount(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if tb.hub.RoomCount() == n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("RoomCount = %d, want %d", tb.hub.RoomCount(), n)
}

func write(t *testing.T, conn *websocket.Conn, frame []byte) {
	t.Helper()
	if err := conn.WriteMessage(websocket.BinaryMessage, frame); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// readFrame reads until a frame of msgType arrives and returns its payload.
func readFrame(t *testing.T, conn *websocket.Conn, msgType uint32) []byte {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		got, payload, err := protocol.DecodeMessage(msg)
		if err != nil || got != msgType {
			continue
		}
		return payload
	}
}

// readUntilSync reads until a sync frame of syncType arrives and returns its data.
func readUntilSync(t *testing.T, conn *websocket.Conn, syncType uint32) []byte {
	t.Helper()
	for {
		st, data, err := protocol.DecodeSyncMessage(readFrame(t, conn, protocol.MsgSync))
		if err != nil {
			t.Fatalf("decode sync: %v", err)
		}
		if st == syncType {
			return data
		}
	}
}

func readControl(t *testing.T, conn *websocket.Conn) protocol.ControlMessage {
	t.Helper()
	msg, err := protocol.DecodeControlMessage(readFrame(t, conn, protocol.MsgControl))
	if err != nil {
		t.Fatalf("decode control: %v", err)
	}
	return msg
}

func readAwareness(t *testing.T, conn *websocket.Conn) []protocol.AwarenessEntry {
	t.Helper()
	body, err := protocol.DecodeAwarenessMessage(readFrame(t, conn, protocol.MsgAwareness))
	if err != nil {
		t.Fatalf("decode awareness message: %v", err)
	}
	entries, err := protocol.DecodeAwarenessUpdate(body)
	if err != nil {
		t.Fatalf("decode awareness update: %v", err)
	}
	return entries
}

func awarenessFrame(clientID, clock uint32, state string) []byte {
	return protocol.EncodeAwareness(protocol.EncodeAwarenessUpdate([]protocol.AwarenessEntry{
		{ClientID: clientID, Clock: clock, State: []byte(state)},
	}))
}

var settleClock atomic.Uint32

// sendSnapshotAndSettle writes a snapshot frame, then an awareness frame, and
// waits for the awareness echo. A client's frames are handled in order, so the
// echo proves the room has recorded the snapshot.
func sendSnapshotAndSettle(t *testing.T, conn *websocket.Conn, content string, state []byte) {
	t.Helper()
	write(t, conn, protocol.EncodeSnapshot([]byte(content), state))
	write(t, conn, awarenessFrame(9999, settleClock.Add(1), `{}`))
	readAwareness(t, conn)
}
