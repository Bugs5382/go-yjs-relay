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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	yjsrelay "github.com/Bugs5382/go-yjs-relay"
	"github.com/Bugs5382/go-yjs-relay/protocol"
	"github.com/gorilla/websocket"
)

func serve(t *testing.T, h http.Handler, target string) *httptest.ResponseRecorder {
	t.Helper()
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, target, nil))
	return rr
}

func assertUnauthorized(t *testing.T, rr *httptest.ResponseRecorder) {
	t.Helper()
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
	if got := rr.Header().Get("WWW-Authenticate"); !strings.HasPrefix(got, "Bearer ") {
		t.Fatalf("expected a Bearer challenge, got %q", got)
	}
}

func TestHandlerRejectsMissingToken(t *testing.T) {
	h := yjsrelay.NewHandler(yjsrelay.New(nil), fakeVerifier{})
	assertUnauthorized(t, serve(t, h, "/ws/doc-1"))
}

func TestHandlerRejectsATokenTheVerifierRefuses(t *testing.T) {
	h := yjsrelay.NewHandler(yjsrelay.New(nil), fakeVerifier{})
	assertUnauthorized(t, serve(t, h, "/ws/doc-1?token=not-a-token"))
}

// TestHandlerRejectsPathMismatch: a token for one room must not open another.
func TestHandlerRejectsPathMismatch(t *testing.T) {
	h := yjsrelay.NewHandler(yjsrelay.New(nil), fakeVerifier{})
	assertUnauthorized(t, serve(t, h, "/ws/doc-2?token=doc-1:u-1"))
}

func TestHandlerRejectsAJoinWithoutAUser(t *testing.T) {
	h := yjsrelay.NewHandler(yjsrelay.New(nil), fakeVerifier{})
	assertUnauthorized(t, serve(t, h, "/ws/doc-1?token=doc-1:"))
}

func TestHandlerAcceptsBearerHeader(t *testing.T) {
	tb := newTestbed(t, newMemSink())
	hdr := http.Header{}
	hdr.Set("Authorization", "Bearer doc-hdr:u-1")
	conn, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(tb.srv.URL, "http")+"/ws/doc-hdr", hdr)
	if err != nil {
		t.Fatalf("dial: %v (status=%v)", err, resp)
	}
	defer func() { _ = conn.Close() }()
	readUntilSync(t, conn, protocol.SyncStep1)
	tb.waitForEditors(t, "doc-hdr", 1)
}

func TestHandlerAcceptsAValidTokenAndJoinsTheRoom(t *testing.T) {
	tb := newTestbed(t, newMemSink())
	conn := tb.dialToken(t, "doc-42", "doc-42:u-42")
	// The relay opens with Sync Step-1 carrying the empty state vector.
	if sv := readUntilSync(t, conn, protocol.SyncStep1); string(sv) != string(protocol.EmptyStateVector()) {
		t.Fatalf("opening state vector: got %x", sv)
	}
	tb.waitForEditors(t, "doc-42", 1)
}

func TestWithRoomFuncPicksTheRoomFromTheRequest(t *testing.T) {
	hub := yjsrelay.New(nil)
	tb := newTestbedWithHandler(t, hub, yjsrelay.WithRoomFunc(func(r *http.Request) string {
		return r.URL.Query().Get("doc")
	}))
	conn, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(tb.srv.URL, "http")+"/collab?doc=doc-q&token=doc-q:u-1", nil)
	if err != nil {
		t.Fatalf("dial: %v (status=%v)", err, resp)
	}
	defer func() { _ = conn.Close() }()
	tb.waitForEditors(t, "doc-q", 1)
}

func TestHandlerUsesThePathValueWhenTheMuxSetsOne(t *testing.T) {
	hub := yjsrelay.New(nil)
	mux := http.NewServeMux()
	mux.Handle("GET /rooms/{room}/socket", yjsrelay.NewHandler(hub, fakeVerifier{}))
	srv := httptest.NewServer(mux)
	t.Cleanup(func() {
		srv.Close()
		_ = hub.Close(context.Background())
	})
	conn, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http")+"/rooms/doc-pv/socket?token=doc-pv:u-1", nil)
	if err != nil {
		t.Fatalf("dial: %v (status=%v)", err, resp)
	}
	defer func() { _ = conn.Close() }()
	readUntilSync(t, conn, protocol.SyncStep1)
	if _, ok := hub.Lookup("doc-pv"); !ok {
		t.Fatal("the {room} path value was not used")
	}
}

func dialWithOrigin(t *testing.T, tb *testbed, room, origin string) int {
	t.Helper()
	hdr := http.Header{}
	if origin != "" {
		hdr.Set("Origin", origin)
	}
	conn, resp, err := websocket.DefaultDialer.Dial(tb.url(room, room+":u-1"), hdr)
	if conn != nil {
		_ = conn.Close()
	}
	if err == nil {
		return http.StatusSwitchingProtocols
	}
	if resp == nil {
		t.Fatalf("dial failed with no response: %v", err)
	}
	return resp.StatusCode
}

func originTestbed(t *testing.T, allowed ...string) *testbed {
	t.Helper()
	return newTestbedWithHandler(t, yjsrelay.New(nil), yjsrelay.WithAllowedOrigins(allowed...))
}

// A server-side proxy dials with no Origin header; that path stays open with
// an empty allowlist.
func TestCheckOriginAllowsMissingOrigin(t *testing.T) {
	if got := dialWithOrigin(t, originTestbed(t), "doc-o1", ""); got != http.StatusSwitchingProtocols {
		t.Fatalf("no-Origin upgrade: got %d, want 101", got)
	}
}

// With no allowlist a browser Origin is refused even with a valid token, so a
// leaked token cannot be spent from any web page.
func TestCheckOriginRejectsBrowserOriginByDefault(t *testing.T) {
	if got := dialWithOrigin(t, originTestbed(t), "doc-o2", "https://evil.example.com"); got != http.StatusForbidden {
		t.Fatalf("cross-origin upgrade: got %d, want 403", got)
	}
}

func TestCheckOriginAllowsAllowlistedOrigin(t *testing.T) {
	tb := originTestbed(t, "https://docs.example.org")
	if got := dialWithOrigin(t, tb, "doc-o3", "https://docs.example.org"); got != http.StatusSwitchingProtocols {
		t.Fatalf("allowlisted upgrade: got %d, want 101", got)
	}
}

func TestCheckOriginAllowlistIsCaseInsensitiveAndExact(t *testing.T) {
	tb := originTestbed(t, "https://Docs.Example.ORG")
	if got := dialWithOrigin(t, tb, "doc-o4", "https://docs.example.org"); got != http.StatusSwitchingProtocols {
		t.Fatalf("case-insensitive match: got %d, want 101", got)
	}
	if got := dialWithOrigin(t, tb, "doc-o4", "https://evil.docs.example.org"); got != http.StatusForbidden {
		t.Fatalf("suffix impostor: got %d, want 403", got)
	}
}

// TestReadLimitClosesOversizedFrames: an oversized frame closes the socket
// instead of being buffered without bound.
func TestReadLimitClosesOversizedFrames(t *testing.T) {
	tb := newTestbed(t, newMemSink(), yjsrelay.WithReadLimit(64))
	conn := tb.dial(t, "doc-1", "u-1", "")
	tb.waitForEditors(t, "doc-1", 1)
	write(t, conn, protocol.EncodeUpdate(make([]byte, 128)))
	tb.waitForRoomCount(t, 0)
}

func TestDefaultReadLimitIsTwoMiB(t *testing.T) {
	if yjsrelay.DefaultReadLimit != 2<<20 {
		t.Fatalf("DefaultReadLimit = %d, want 2 MiB", yjsrelay.DefaultReadLimit)
	}
}
