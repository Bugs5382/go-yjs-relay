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
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"

	yjsrelay "github.com/Bugs5382/go-yjs-relay"
	"github.com/Bugs5382/go-yjs-relay/protocol"
	"github.com/gorilla/websocket"
)

// mapSink keeps checkpoints in a map. A real sink writes to a database or
// forwards the content to the service that owns it.
type mapSink struct {
	mu      sync.Mutex
	content map[string]string
}

func (s *mapSink) Load(context.Context, string) ([]byte, error) { return nil, nil }

func (s *mapSink) Save(_ context.Context, snap yjsrelay.Snapshot) error {
	if err := yjsrelay.ValidateLexicalJSON(snap.Content); err != nil {
		return &yjsrelay.RejectError{Reason: "invalid_content", Detail: err.Error()}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.content[snap.Room] = string(snap.Content)
	fmt.Printf("saved %s for %s\n", snap.Room, snap.Author.UserID)
	return nil
}

func Example() {
	// A verifier that trusts "room/user" tokens. Use a signed token in
	// production.
	verifier := yjsrelay.TokenVerifierFunc(func(_ context.Context, token string) (yjsrelay.Join, error) {
		room, user, ok := strings.Cut(token, "/")
		if !ok {
			return yjsrelay.Join{}, errors.New("bad token")
		}
		return yjsrelay.Join{Room: room, Identity: yjsrelay.Identity{UserID: user}}, nil
	})

	sink := &mapSink{content: map[string]string{}}
	hub := yjsrelay.New(sink)
	defer func() { _ = hub.Close(context.Background()) }()

	mux := http.NewServeMux()
	mux.Handle("GET /rooms/{room}", yjsrelay.NewHandler(hub, verifier))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// A client joins doc-1 and sends a checkpoint, the frame a browser editor
	// sends after a burst of edits.
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/rooms/doc-1?token=doc-1/ann"
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = conn.Close() }()
	_, _, _ = conn.ReadMessage() // the relay's opening Sync Step-1

	content := []byte(`{"root":{"type":"root","children":[]}}`)
	_ = conn.WriteMessage(websocket.BinaryMessage, protocol.EncodeSnapshot(content, nil))
	// A client's frames are handled in order, so once a presence frame sent
	// after the checkpoint is echoed back, the room has recorded it.
	presence := protocol.EncodeAwarenessUpdate([]protocol.AwarenessEntry{{ClientID: 1, Clock: 1, State: []byte(`{}`)}})
	_ = conn.WriteMessage(websocket.BinaryMessage, protocol.EncodeAwareness(presence))
	for {
		_, msg, _ := conn.ReadMessage()
		if t, _, _ := protocol.DecodeMessage(msg); t == protocol.MsgAwareness {
			break
		}
	}

	// Before publishing: the sink must hold the room's newest content.
	res, err := hub.Flush(context.Background(), "doc-1")
	fmt.Println("flushed:", res.ContentFlushed, "error:", err)

	// After publishing: every editor drops to read-only.
	fmt.Println("frozen:", hub.Freeze(context.Background(), "doc-1", "v1"))

	// Output:
	// saved doc-1 for ann
	// flushed: true error: <nil>
	// frozen: true
}
